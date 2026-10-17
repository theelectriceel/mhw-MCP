import copy
import json
from pathlib import Path
import zipfile

import numpy as np
import pytest
import xarray as xr

from marine_tools import MarineTools, ToolError
from marine_tools.api import call_tool, catalog
from marine_tools.ocean_colour import (
    DATASET, DEFAULT_REQUEST, TITLE, download, parse_files, request_for_date, settings,
)


@pytest.fixture
def netcdf(tmp_path):
    path = tmp_path / "sample.nc"
    dataset = xr.Dataset({"chlor_a": (("time", "lat", "lon"),
                                     np.array([[[1.25, 2.5], [np.nan, -1.0]]], dtype="float32"))},
                         coords={"time": [np.datetime64("2026-03-31")],
                                 "lat": [51.68, 51.64], "lon": [-9.62, -9.66]})
    dataset.chlor_a.attrs["units"] = "milligram m-3"
    dataset.to_netcdf(path, engine="netcdf4")
    dataset.close()
    return path


class FakeClient:
    def __init__(self, source):
        self.source = source
        self.calls = []

    def retrieve(self, dataset, request):
        self.calls.append((dataset, copy.deepcopy(request)))
        return self

    def download(self, target):
        Path(target).write_bytes(self.source.read_bytes())


def test_exact_request_and_name():
    assert request_for_date() == DEFAULT_REQUEST
    config = MarineTools().source("ocean_colour")
    assert config["name"] == TITLE
    assert config["request"] == DEFAULT_REQUEST
    assert config["normalization"]["variable"] == "chlorophyll_a_concentration"


@pytest.mark.parametrize("parts", [(1997, 8, 31), (2026, 2, 30), (2026, 0, 1), (2026, 3, 31.0), ("2026", 3, 31), (True, 3, 31), (9999, 1, 1)])
def test_bad_dates(parts):
    with pytest.raises(ToolError):
        request_for_date(*parts)


def test_historical_dates_leap_day_and_zero_padding():
    assert request_for_date(1997, 9, 1)["day"] == ["01"]
    assert request_for_date(2024, 2, 29)["month"] == ["02"]


@pytest.mark.parametrize("selection", [
    {"latitude": True}, {"latitude": float("nan")}, {"longitude": 181},
    {"latitude": "51.675"}, {"latitude": 91},
    {"cds_request": {**DEFAULT_REQUEST, "year": ["2025", "2026"]}},
    {"cds_request": {**DEFAULT_REQUEST, "version": "5_0"}},
    {"cds_request": {**DEFAULT_REQUEST, "projection": "sinusoidal_grid"}},
])
def test_bad_selections(selection):
    with pytest.raises(ToolError):
        settings({}, **selection)


def test_point_coordinates_values_and_units(netcdf):
    result = parse_files([netcdf], DEFAULT_REQUEST, 51.675, -9.625)["table"]
    assert result["rows"][0][:4] == ["2026-03-31T00:00:00Z", 51.68, -9.620000000000005, 1.25]
    assert result["columnUnits"][3] == "mg m-3"
    assert result["rows"][0][4:6] == [51.675, -9.625]
    # Do not replace a missing cell with a nearby valid ocean cell or with zero.
    assert parse_files([netcdf], DEFAULT_REQUEST, 51.64, -9.62)["table"]["rows"][0][3] is None
    assert parse_files([netcdf], DEFAULT_REQUEST, 51.64, -9.66)["table"]["rows"][0][3] is None


def test_reject_dates_units_and_distant_grid(netcdf, tmp_path):
    for request, lat, lon in [(request_for_date(2026, 3, 30), 51.675, -9.625),
                              (DEFAULT_REQUEST, 60, -20)]:
        with pytest.raises(ToolError):
            parse_files([netcdf], request, lat, lon)
    with xr.open_dataset(netcdf) as data:
        loaded = data.load()
    loaded.chlor_a.attrs["units"] = "kg m-3"
    bad = tmp_path / "bad.nc"
    loaded.to_netcdf(bad)
    loaded.close()
    with pytest.raises(ToolError, match="units"):
        parse_files([bad], DEFAULT_REQUEST, 51.675, -9.625)


def test_raw_download_cache_repair_and_failed_refresh_preservation(netcdf, tmp_path):
    client = FakeClient(netcdf)
    payload, meta = download({}, tmp_path / "raw", client_factory=lambda: client)
    assert client.calls == [(DATASET, DEFAULT_REQUEST)]
    assert Path(meta["raw_path"]).read_bytes() == netcdf.read_bytes()
    assert meta["title"] == TITLE and not meta["raw_cache_hit"]
    _, cached = download({}, tmp_path / "raw", latitude=51.64, client_factory=lambda: client)
    assert cached["raw_cache_hit"] and len(client.calls) == 1
    # A corrupt local raw artifact must not be reused.
    Path(meta["raw_path"]).write_bytes(b"corrupt")
    download({}, tmp_path / "raw", client_factory=lambda: client)
    assert len(client.calls) == 2
    invalid = tmp_path / "invalid"
    invalid.write_bytes(b"<html>not NetCDF</html>")
    with pytest.raises(ToolError):
        download({}, tmp_path / "raw", force=True, client_factory=lambda: FakeClient(invalid))
    assert Path(meta["raw_path"]).read_bytes() == netcdf.read_bytes()
    assert not list((tmp_path / "raw" / "ocean_colour").glob(".cds-*"))


def test_zip_archives_do_not_trust_member_paths(netcdf, tmp_path):
    archive = tmp_path / "result.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.write(netcdf, "../../escape.nc")
    payload, meta = download({}, tmp_path / "raw", client_factory=lambda: FakeClient(archive))
    assert payload["table"]["rows"][0][3] == 1.25
    assert not (tmp_path / "escape.nc").exists()
    assert Path(meta["raw_path"]).read_bytes() == archive.read_bytes()


def test_pipeline_http_catalog_cache_and_failure_preservation(netcdf, tmp_path, monkeypatch):
    import marine_tools.ocean_colour as adapter
    client = FakeClient(netcdf)
    monkeypatch.setattr(adapter, "create_client", lambda: client)
    system = MarineTools(tmp_path / "marine.sqlite", "sources.json")
    first = system.ingest_ocean_colour()
    assert first["errors"] == []
    result = system.pipeline_query("ocean_colour")
    row = result["rows"][0]
    assert row["variable"] == "chlorophyll_a_concentration"
    assert row["unit"] == "mg m-3" and row["value"] == 1.25
    assert row["latitude"] == 51.68
    assert "ingest_ocean_colour" in {t["name"] for t in catalog(system)}
    assert call_tool(system, "ingest_ocean_colour", {})["datasets"][0]["reused"]
    assert len(client.calls) == 1
    # A different selection explicitly fetches live data; fetch never reuses the raw-download cache.
    assert system.ingest_ocean_colour(latitude=51.64)["errors"] == []
    assert len(client.calls) == 2
    with system.connect() as con:
        metadata = json.loads(con.execute("SELECT metadata_json FROM snapshots LIMIT 1").fetchone()[0])
    assert metadata["request"] == DEFAULT_REQUEST
    assert metadata["raw_sha256"] and metadata["doi"]
    invalid = tmp_path / "invalid"
    invalid.write_text("html")
    monkeypatch.setattr(adapter, "create_client", lambda: FakeClient(invalid))
    failed = system.ingest_ocean_colour(force=True)
    assert failed["errors"]
    # Existing standardized data is still readable after the failed replacement.
    assert system.pipeline_query("ocean_colour")["matched_rows"] == 1


def test_transport_error_redacts_details(tmp_path):
    class BadClient:
        def retrieve(self, dataset, request):
            raise RuntimeError("private-token-should-never-appear")
    with pytest.raises(ToolError) as error:
        download({}, tmp_path, client_factory=BadClient)
    assert "private-token" not in str(error.value)
    assert "licence" in str(error.value) and "availability" in str(error.value)


def test_http_ingest_and_query(netcdf, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from marine_tools.api import create_app
    import marine_tools.ocean_colour as adapter
    monkeypatch.setattr(adapter, "create_client", lambda: FakeClient(netcdf))
    system = MarineTools(tmp_path / "marine.sqlite", "sources.json")
    with TestClient(create_app(system)) as client:
        response = client.post("/tools/ingest_ocean_colour", json={"year": 2026, "month": 3, "day": 31})
        assert response.status_code == 200 and not response.json()["errors"]
        query = client.post("/tools/pipeline_query", json={"source": "ocean_colour"})
        assert query.json()["rows"][0]["value"] == 1.25
        assert client.post("/tools/ingest_ocean_colour", json={"year": 2026, "month": 2, "day": 30}).status_code == 400
