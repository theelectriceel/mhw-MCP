"""EuroSea POST/CSV adapter. No assumptions about HTML download success."""
import csv
import hashlib
import io
import math
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .system import ToolError

BUTTONS = {"sst": "Download SST", "clim": "Download Climatology",
           "mhw": "Download MHWs", "cs": "Download Cold Spells"}


def coordinates(location):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([NS])-(\d+(?:\.\d+)?)([EW])", location)
    if not m:
        raise ToolError("Location must look like 51.675N-9.625W")
    lat = float(m[1]) * (1 if m[2] == "N" else -1)
    lon = float(m[3]) * (1 if m[4] == "E" else -1)
    if abs(lat) > 90 or abs(lon) > 180:
        raise ToolError("Coordinates outside geographic bounds")
    return lat, lon


def form(config):
    kind, location = config["dataset_type"], config["location"]
    coordinates(location)
    if kind not in BUTTONS:
        raise ToolError("EuroSea type must be sst, clim, mhw, or cs")
    return {f"/tmp/csv-{kind}-{location}.csv": BUTTONS[kind]}


def download(config, raw_dir):
    body = urlencode(form(config)).encode()
    for attempt in range(3):
        try:
            request = Request(config["url"], data=body, headers={"User-Agent": "MarineTools/2.0"})
            with urlopen(request, timeout=90) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise ToolError("EuroSea response exceeds 32 MiB")
            table = parse_csv(raw.decode("utf-8-sig"), config)
            folder = Path(raw_dir)
            folder.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256(raw).hexdigest()
            path = folder / (digest + ".csv")
            path.write_bytes(raw)
            return table, {"adapter": "eurosea", "request_method": "POST", "form": form(config),
                           "raw_sha256": digest, "raw_path": str(path.resolve()),
                           "coordinate_role": "requested_point", "temperature_unit": "degree_Celsius",
                           "climatology_baseline": "not supplied in CSV; source-defined"}
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise ToolError(f"EuroSea HTTP {exc.code}; the server may require generating this point on its website first") from exc
            if attempt == 2:
                raise ToolError(f"EuroSea download failed: {exc}") from exc
            time.sleep(2 ** attempt)


def parse_csv(text, config):
    reader = csv.DictReader(io.StringIO(text))
    kind = config["dataset_type"]
    expected = {"sst": ["Date", "SST (ºC)"],
                "clim": ["Day", "PCT.10 (ºC)", "mean (ºC)", "PCT.90 (ºC)"],
                "mhw": ["Event ID", "start", "end", "duration (days)", "intensity (ºC)", "category"],
                "cs": ["Event ID", "start", "end", "duration (days)", "intensity (ºC)", "category"]}[kind]
    if reader.fieldnames != expected:
        raise ToolError(f"Unexpected EuroSea {kind} CSV header: {reader.fieldnames}; HTML/error responses are not cached")
    lat, lon = coordinates(config["location"])
    names = {"sst": ["time", "sst_c"], "clim": ["calendar_day", "p10_c", "climatology_c", "p90_c"],
             "mhw": ["event_id", "time", "time_end", "duration_days", "intensity_c", "category"],
             "cs": ["event_id", "time", "time_end", "duration_days", "intensity_c", "category"]}[kind]
    numeric = {"sst_c", "p10_c", "climatology_c", "p90_c", "duration_days", "intensity_c"}
    rows = []
    seen = set()
    for number, source in enumerate(reader, 2):
        if None in source or any(v is None for v in source.values()):
            raise ToolError(f"Malformed CSV row {number}")
        row = []
        for name, header in zip(names, expected):
            value = source[header].strip()
            if name in numeric:
                try:
                    value = None if value.lower() in ("", "na", "nan", "null", "-999", "-9999") else float(value)
                    if value is not None and not math.isfinite(value):
                        value = None
                except ValueError as exc:
                    raise ToolError(f"Invalid number at CSV row {number}") from exc
            elif name in ("time", "time_end"):
                try:
                    value = datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
                except ValueError as exc:
                    raise ToolError(f"Invalid date at CSV row {number}") from exc
            elif name == "calendar_day":
                try:
                    value = datetime.strptime("2000-" + value, "%Y-%d-%b").strftime("%m-%d")
                except ValueError as exc:
                    raise ToolError(f"Invalid climatology day at CSV row {number}") from exc
            row.append(value)
        identity = row[0]
        if identity in seen:
            raise ToolError(f"Duplicate {kind} record identity: {identity}")
        seen.add(identity)
        if kind in ("mhw", "cs") and row[2] < row[1]:
            raise ToolError("Event end precedes start")
        rows.append(row + [lat, lon])
    names += ["latitude", "longitude"]
    numeric |= {"latitude", "longitude"}
    return {"table": {"columnNames": names, "columnTypes": ["double" if n in numeric else "String" for n in names],
                      "columnUnits": ["degree_Celsius" if n.endswith("_c") else "days" if n == "duration_days" else
                                      "degrees_north" if n == "latitude" else "degrees_east" if n == "longitude" else
                                      "UTC" if n in ("time", "time_end") else None for n in names], "rows": rows}}
