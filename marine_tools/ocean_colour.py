"""Copernicus CDS ocean colour download and local chlorophyll-a extraction."""
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import tarfile
import tempfile
import threading
import zipfile

from filelock import FileLock

from .system import ToolError

DATASET = "satellite-ocean-colour"
TITLE = "Ocean colour from 1997 to present derived from satellite observations"
URL = "https://cds.climate.copernicus.eu/datasets/satellite-ocean-colour"
DEFAULT_REQUEST = {
    "variable": ["mass_concentration_of_chlorophyll_a"],
    "projection": "regular_latitude_longitude_grid",
    "temporal_resolution": "daily",
    "year": ["2026"], "month": ["03"], "day": ["31"], "version": "6_0",
}
_netcdf_lock = threading.Lock()  # The netCDF/HDF5 library is not thread-safe.


def request_for_date(year=2026, month=3, day=31):
    if any(type(v) is not int for v in (year, month, day)):
        raise ToolError("year, month and day must be integers")
    try:
        selected = date(year, month, day)
    except ValueError as exc:
        raise ToolError("Invalid ocean colour date") from exc
    if selected < date(1997, 9, 1) or selected > date.today():
        raise ToolError("Ocean colour date must be between September 1997 and today; recent data may not yet be published")
    return {**DEFAULT_REQUEST, "year": [f"{year:04d}"], "month": [f"{month:02d}"], "day": [f"{day:02d}"]}


def settings(config, cds_request=None, latitude=None, longitude=None):
    request = dict(config.get("request", DEFAULT_REQUEST) if cds_request is None else cds_request)
    if set(request) != set(DEFAULT_REQUEST):
        raise ToolError("Provide the seven supported CDS ocean colour request fields")
    for name in ("variable", "projection", "temporal_resolution", "version"):
        if request[name] != DEFAULT_REQUEST[name]:
            raise ToolError(f"Unsupported ocean colour {name}; this adapter supports daily regular-grid chlorophyll-a v6.0")
    try:
        parts = [request[key] for key in ("year", "month", "day")]
        if any(not isinstance(p, list) or len(p) != 1 or not isinstance(p[0], str) or not p[0].isdigit() for p in parts):
            raise ValueError()
        request = request_for_date(*(int(p[0]) for p in parts))
    except (TypeError, ValueError) as exc:
        raise ToolError("Choose one valid date per CDS request") from exc
    latitude = config.get("latitude", 51.675) if latitude is None else latitude
    longitude = config.get("longitude", -9.625) if longitude is None else longitude
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in (latitude, longitude)):
        raise ToolError("latitude/longitude must be finite numbers")
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ToolError("Invalid ocean colour latitude/longitude")
    return request, float(latitude), float(longitude)


def selection_url(config, **selection):
    from urllib.parse import urlencode
    request, lat, lon = settings(config, **selection)
    return URL + "?" + urlencode({"request": json.dumps(request, sort_keys=True), "latitude": lat, "longitude": lon})


def create_client():
    import cdsapi
    try:
        return cdsapi.Client(timeout=60, retry_max=2, sleep_max=30, quiet=True, progress=False)
    except Exception as exc:
        raise ToolError("Configure CDSAPI_URL/CDSAPI_KEY or your .cdsapirc personal access token; accept the dataset licences in the CDS portal") from exc


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _netcdf_files(artifact, directory):
    """Read archive members as streams, never trusting their names as paths."""
    limit = 2 * 1024 ** 3
    files, total = [], 0

    def save(name, size, stream):
        nonlocal total
        if not name.lower().endswith((".nc", ".nc4", ".cdf")):
            return
        total += size
        if size < 0 or total > limit or len(files) >= 32:
            raise ToolError("Ocean colour archive exceeds extraction limits")
        target = directory / f"part_{len(files):02d}.nc"
        with target.open("wb") as output:
            copied = 0
            while chunk := stream.read(1024 * 1024):
                copied += len(chunk)
                if copied > size:
                    raise ToolError("Archive member exceeds declared size")
                output.write(chunk)
        files.append(target)

    if zipfile.is_zipfile(artifact):
        with zipfile.ZipFile(artifact) as archive:
            for member in archive.infolist():
                if not member.is_dir():
                    with archive.open(member) as stream:
                        save(member.filename, member.file_size, stream)
    elif tarfile.is_tarfile(artifact):
        with tarfile.open(artifact) as archive:
            for member in archive:
                if member.isfile():
                    with archive.extractfile(member) as stream:
                        save(member.name, member.size, stream)
    else:
        files = [artifact]
    if not files:
        raise ToolError("CDS response contains no NetCDF files")
    return files


def parse_files(files, request, latitude, longitude):
    import numpy as np
    import xarray as xr
    rows, seen = [], set()
    expected_date = f"{request['year'][0]}-{request['month'][0]}-{request['day'][0]}"
    with _netcdf_lock:
        for path in files:
            with xr.open_dataset(path, engine="netcdf4", mask_and_scale=True, decode_times=True) as dataset:
                if "chlor_a" not in dataset:
                    raise ToolError("NetCDF is missing chlor_a")
                data = dataset["chlor_a"]
                if set(data.dims) != {"time", "lat", "lon"} or any(dataset[c].dims != (c,) for c in ("lat", "lon", "time")):
                    raise ToolError("Expected chlor_a on a regular (time, lat, lon) grid")
                original_unit = data.attrs.get("units", "")
                if original_unit not in ("milligram m-3", "mg m-3", "mg m^-3", "mg m**-3", "mg/m3"):
                    raise ToolError("Unrecognized chlorophyll-a units; refusing an implicit conversion")
                lats, lons = dataset.lat.values, dataset.lon.values
                if not len(lats) or not len(lons) or not np.isfinite(lats).all() or not np.isfinite(lons).all():
                    raise ToolError("Invalid NetCDF grid coordinates")
                if np.any(abs(lats) > 90) or np.any((lons < -180) | (lons > 360)):
                    raise ToolError("Invalid NetCDF coordinate range")
                normalized_lon = (lons + 180) % 360 - 180
                iy = int(np.argmin(abs(lats - latitude)))
                ix = int(np.argmin(abs((normalized_lon - longitude + 180) % 360 - 180)))
                if abs(float(lats[iy]) - latitude) > 0.05 or abs(float((normalized_lon[ix] - longitude + 180) % 360 - 180)) > 0.05:
                    raise ToolError("No ocean colour grid cell within 0.05 degrees of the requested point")
                # Slice before loading: never materialize the global grid in memory.
                point = data.isel(lat=iy, lon=ix)
                for index, instant in enumerate(dataset.time.values):
                    stamp = np.datetime_as_string(instant, unit="s")
                    if not stamp.startswith(expected_date + "T") or stamp in seen:
                        raise ToolError("Unexpected or duplicate observation date in CDS response")
                    seen.add(stamp)
                    value = float(point.isel(time=index).values)
                    value = value if math.isfinite(value) and value >= 0 else None
                    rows.append([stamp + "Z", float(lats[iy]), float(normalized_lon[ix]), value,
                                 latitude, longitude, original_unit])
    if not rows:
        raise ToolError("CDS response contains no observations for the requested date")
    return {"table": {"columnNames": ["time", "latitude", "longitude", "chlor_a", "requested_latitude", "requested_longitude", "source_unit"],
                      "columnTypes": ["String", "double", "double", "double", "double", "double", "String"],
                      "columnUnits": ["UTC", "degrees_north", "degrees_east", "mg m-3", "degrees_north", "degrees_east", ""],
                      "rows": rows}}


def download(config, raw_folder, cds_request=None, latitude=None, longitude=None, force=False, client_factory=None):
    request, lat, lon = settings(config, cds_request, latitude, longitude)
    folder = Path(raw_folder) / "ocean_colour"
    folder.mkdir(parents=True, exist_ok=True)
    request_key = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
    manifest_path = folder / f"{request_key}.json"
    with FileLock(str(folder / f"{request_key}.lock"), timeout=3600):
        with tempfile.TemporaryDirectory(prefix=".cds-", dir=folder) as temporary:
            stage = Path(temporary)
            artifact, digest = None, None
            if manifest_path.exists() and not force:
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    digest = manifest["sha256"]
                    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                        raise ValueError("Invalid digest")
                    candidate = folder / f"{digest}.download"
                    if candidate.exists() and _hash(candidate) == digest:
                        artifact = candidate
                except (ValueError, KeyError, OSError):
                    pass
            downloaded = artifact is None
            if downloaded:
                target = stage / "source.download"
                try:
                    client = (client_factory or create_client)()
                    client.retrieve(DATASET, request).download(str(target))
                except ToolError:
                    raise
                except Exception as exc:
                    # Do not leak tokens, authenticated URLs or response headers.
                    response = getattr(exc, "response", None)
                    code = getattr(response, "status_code", None)
                    raise ToolError(f"CDS ocean colour retrieval failed ({code or type(exc).__name__}). Check credentials, dataset licence acceptance and date availability; recent dates can lag 9–12 months.") from exc
                if not target.is_file() or target.stat().st_size == 0 or target.stat().st_size > 2 * 1024 ** 3:
                    raise ToolError("Invalid or oversized CDS download")
                artifact = target
            try:
                payload = parse_files(_netcdf_files(artifact, stage), request, lat, lon)
            except ToolError:
                raise
            except Exception as exc:
                raise ToolError("CDS returned invalid or unreadable chlorophyll NetCDF data") from exc
            if downloaded:
                digest = _hash(artifact)
                published = folder / f"{digest}.download"
                artifact.replace(published)
                artifact = published
                manifest = {"dataset": DATASET, "request": request, "sha256": digest}
                temporary_manifest = stage / "manifest.json"
                temporary_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                temporary_manifest.replace(manifest_path)
            metadata = {"title": TITLE, "dataset": DATASET, "source_url": URL,
                        "doi": "10.24381/cds.f85b319d", "request": request,
                        "raw_path": str(artifact.resolve()), "raw_sha256": digest,
                        "raw_downloaded_at": datetime.fromtimestamp(artifact.stat().st_mtime, timezone.utc).isoformat(),
                        "requested_point": {"latitude": lat, "longitude": lon},
                        "coordinate_role": "actual nearest satellite grid cell centre",
                        "spatial_selection": "nearest cell within 0.05 degrees on each axis; no interpolation or search for a nonmissing neighbour",
                        "data_role": "satellite-derived surface chlorophyll-a concentration",
                        "unit": "mg m-3", "raw_cache_hit": not downloaded,
                        "limitations": "Cloud, ice and land masking may yield missing values. Chlorophyll concentration is not a species count or a HAB diagnosis."}
            return payload, metadata
