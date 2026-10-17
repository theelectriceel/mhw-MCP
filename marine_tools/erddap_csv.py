"""ERDDAP CSV transport, preserving the names + units header rows."""
import csv
import hashlib
import io
from pathlib import Path
from urllib.request import Request, urlopen

from .system import ToolError


def parse_csv(text, metadata):
    reader = csv.reader(io.StringIO(text))
    names, units = next(reader, None), next(reader, None)
    if not names or not units or len(names) != len(units):
        raise ToolError("Invalid ERDDAP CSV headers")
    types = {r[1]: r[3] for r in metadata["table"]["rows"] if r[0] in ("dimension", "variable")}
    kinds = ["String" if u == "UTC" else types.get(n, "String") for n, u in zip(names, units)]
    rows = []
    for row in reader:
        if not row:
            continue
        if len(row) != len(names):
            raise ToolError("ERDDAP CSV row width mismatch")
        converted = []
        for value, kind in zip(row, kinds):
            if kind.lower() in ("string", "char"):
                converted.append(value or None)
            else:
                try:
                    converted.append(None if value.lower() in ("", "nan", "null", "na") else float(value))
                except ValueError as exc:
                    raise ToolError("Invalid ERDDAP numeric CSV value") from exc
        rows.append(converted)
    return {"table": {"columnNames": names, "columnTypes": kinds, "columnUnits": units, "rows": rows}}


def download_csv(url, metadata, raw_dir):
    try:
        with urlopen(Request(url, headers={"User-Agent": "MarineTools/2.0"}), timeout=90) as response:
            raw = response.read(128*1024*1024+1)
        if len(raw)>128*1024*1024:
            raise ToolError("CSV exceeds 128 MiB; narrow the grid")
        payload = parse_csv(raw.decode("utf-8-sig"), metadata)
        folder = Path(raw_dir)
        folder.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(raw).hexdigest()
        path = folder / (digest + ".csv")
        path.write_bytes(raw)
        metadata["raw_sha256"], metadata["raw_path"] = digest, str(path.resolve())
        return payload
    except OSError as exc:
        raise ToolError(f"ERDDAP CSV request failed: {exc}") from exc
