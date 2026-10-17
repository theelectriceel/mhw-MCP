"""Behavioral guarantees: network fetches never publish, cache/read calls never fetch."""
import copy
import json
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
import httpx
import pymupdf
import pytest

from marine_tools import MarineTools, ToolError
from marine_tools.api import create_app
from marine_tools.hab import HABConfig, HABError, HABService


@pytest.fixture
def tools(tmp_path):
    sources = tmp_path / "sources.json"
    sources.write_text(Path("sources.json").read_text(), encoding="utf-8")
    service = MarineTools(tmp_path / "marine.sqlite", sources)
    payload = {"table": {"columnNames": ["time", "scientific_name", "count", "latitude", "longitude"],
                         "columnTypes": ["String", "String", "double", "double", "double"],
                         "columnUnits": ["UTC", None, "cells/L", "degrees_north", "degrees_east"],
                         "rows": [["2026-10-01T00:00:00Z", "A", 12, 51.6, -9.6]]}}
    service.fetcher = Mock(side_effect=lambda url: {"table": {"rows": []}} if "/info/" in url else copy.deepcopy(payload))
    yield service
    if service._hab_service:
        service.hab.close()


def test_live_fetch_never_publishes_and_always_contacts_source(tools):
    first = tools.fetch_data("bantry_phyto")
    second = tools.fetch_data("bantry_phyto")
    assert first["fetch_id"] != second["fetch_id"]
    assert first["cached"] is False and second["cached"] is False
    assert tools.fetcher.call_count == 4
    assert tools.cache_status() == []
    assert tools.pipeline_status()["datasets"] == []


def test_cache_and_reads_succeed_with_all_transports_disabled(tools, monkeypatch):
    receipt = tools.fetch_data("bantry_phyto")
    blocked = Mock(side_effect=AssertionError("Network must never be called by cache/read tools"))
    tools.fetcher = blocked
    for target in ("marine_tools.eurosea.download", "marine_tools.erddap_csv.download_csv", "marine_tools.ocean_colour.download"):
        monkeypatch.setattr(target, blocked)
    saved = tools.cache_store(receipt["fetch_id"])
    assert saved["publication"]["datasets"][0]["row_count"] == 1
    assert tools.pipeline_query("bantry_phyto")["rows"][0]["value"] == 12
    assert tools.infer_query(saved["key"])["rows"][0]["count"] == 12
    again = tools.cache_store(receipt["fetch_id"])
    assert again["cache_hit"]
    blocked.assert_not_called()


def test_read_and_cache_misses_never_attempt_download(tools):
    tools.fetcher = Mock(side_effect=AssertionError("Unexpected network call"))
    with pytest.raises(ToolError, match="Ingest"):
        tools.pipeline_query("bantry_phyto")
    with pytest.raises(ToolError, match="fetch_id"):
        tools.cache_store("invalid-id")
    with pytest.raises(ToolError, match="not found"):
        tools.cache_store("0" * 32)
    tools.fetcher.assert_not_called()


@pytest.mark.parametrize("source,target", [("eurosea_sst", "marine_tools.eurosea.download"),
                                          ("bantry_particles_csv", "marine_tools.erddap_csv.download_csv"),
                                          ("ocean_colour", "marine_tools.ocean_colour.download")])
def test_all_adapters_fetch_to_staging_without_populating_cache(tools, source, target):
    payload = {"table": {"columnNames": ["value"], "columnTypes": ["double"], "rows": [[1]]}}
    response = payload if "download_csv" in target else (payload, {})
    with patch(target, return_value=response) as transport:
        receipt = tools.fetch_data(source)
        assert receipt["cached"] is False
        assert "fetches" in str(transport.call_args.args[1 if source != "bantry_particles_csv" else 2])
        if source == "ocean_colour":
            assert transport.call_args.kwargs["force"] is True
    assert tools.cache_status() == []


def test_tampered_receipt_cannot_be_published(tools):
    receipt = tools.fetch_data("bantry_phyto")
    path = tools.db.parent / "fetches" / receipt["fetch_id"] / "receipt.json"
    path.write_text('{}')
    with pytest.raises(ToolError, match="integrity"):
        tools.cache_store(receipt["fetch_id"])
    assert tools.cache_status() == []


def test_hab_fetch_cache_and_get_are_separate(tools, tmp_path):
    with pymupdf.open() as pdf:
        for _ in range(13):
            pdf.new_page(width=100, height=80)
        raw = pdf.tobytes()
    calls = []
    def response(request):
        calls.append(request.url)
        return httpx.Response(200, content=raw)
    tools._hab_service = HABService(tools.db, HABConfig(root=tmp_path / "hab", interval=0, retries=0), httpx.MockTransport(response))
    with TestClient(create_app(tools)) as client:
        assert client.get('/api/hab/2026/7').status_code == 404
        assert client.get('/api/hab/2026/7/page/12').status_code == 404
        assert calls == []
        with patch('marine_tools.hab.render_pages', side_effect=AssertionError('Fetching must not render')):
            fetched = client.post('/tools/fetch_hab_bulletin', json={'year': 2026, 'week': 7})
            assert fetched.status_code == 200
        assert tools.hab._read(2026, 7) is None
        assert client.get('/api/hab/2026/7').status_code == 404
        assert len(calls) == 1
        with patch.object(tools.hab, '_download', side_effect=AssertionError('Cache and GET must be offline')):
            cached = client.post('/tools/cache_hab_bulletin', json={'fetch_id': fetched.json()['fetch_id']})
            assert cached.status_code == 200
            assert client.get('/api/hab/2026/7').status_code == 200
            for page in (12, 13):
                png = client.get(f'/api/hab/2026/7/page/{page}')
                assert png.headers['content-type'] == 'image/png'
            path, _, _ = tools.hab.page(2026, 7, 12)
            path.write_bytes(b'corrupt')
            assert client.get('/api/hab/2026/7/page/12').status_code == 404
            assert len(calls) == 1


def test_atomic_publication_retries_a_transient_windows_lock(tmp_path, monkeypatch):
    from marine_tools.hab import publish_directory
    stage, target = tmp_path / '.tmp-generation', tmp_path / 'generation'
    stage.mkdir()
    (stage / 'page_12.png').write_bytes(b'png')
    original = Path.replace
    attempts = []
    def replace(path, destination):
        attempts.append(path)
        if len(attempts) == 1:
            raise PermissionError('transient file lock')
        return original(path, destination)
    monkeypatch.setattr(Path, 'replace', replace)
    publish_directory(stage, target)
    assert len(attempts) == 2
    assert (target / 'page_12.png').read_bytes() == b'png'
