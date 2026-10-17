"""Offline acceptance coverage; PDFs and upstream responses are generated locally."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import threading
import time

from fastapi.testclient import TestClient
import httpx
import pymupdf
import pytest
from pydantic import ValidationError

from marine_tools import MarineTools
from marine_tools.api import create_app, call_tool
from marine_tools.hab import BackfillRequest, HABConfig, HABError, HABService
from marine_tools.hab_pdf import sha256


def sample_pdf(count=13):
    with pymupdf.open() as doc:
        for index in range(count):
            page = doc.new_page(width=144, height=108)
            page.draw_rect(page.rect, color=None, fill=(index / 20, 0.2, 0.3))
            page.insert_text((10, 40), f"PAGE {index + 1}", fontsize=12)
        return doc.tobytes()


@pytest.fixture
def setup(tmp_path):
    raw = sample_pdf()
    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=raw)

    config = HABConfig(root=tmp_path / "hab", interval=0, retries=0)
    service = HABService(tmp_path / "marine.sqlite", config, httpx.MockTransport(respond))
    system = MarineTools(service.db, tmp_path / "sources.json")
    system._hab_service = service
    yield service, system, calls, raw
    service.close()


@pytest.mark.parametrize("week", [2, 51])
def test_boundaries_accepted(setup, week):
    service, _, calls, _ = setup
    result = service.ingest(2026, week)
    assert result["status"] == "available"
    assert calls[0].endswith(f"2026_Week_{week:02d}_Irish_HAB_Bulletin.pdf")


@pytest.mark.parametrize("week", [0, 1, 52, 53, -1, 7.0, 7.5, "7", True, None])
def test_internal_strict_validation_before_network(setup, week):
    service, system, calls, _ = setup
    with pytest.raises(ValidationError):
        service.ingest(2026, week)
    with pytest.raises(ValidationError):
        call_tool(system, "get_hab_bulletin_pages", {"year": 2026, "week": week})
    with pytest.raises(ValidationError):
        system.backfill_hab_bulletins(2026, week, 51)
    assert not calls


def test_correct_pages_full_dimensions_orientation_and_original(setup):
    service, _, calls, raw = setup
    service.ingest(2026, 7)
    record = service._read(2026, 7)
    assert service.asset_path(record["pdf_path"]).read_bytes() == raw
    with pymupdf.open(stream=raw, filetype="pdf") as doc:
        for number in (12, 13):
            path, item, metadata = service.page(2026, 7, number)
            expected = doc[number - 1].get_pixmap(dpi=200, alpha=False)
            assert path.read_bytes() == expected.tobytes("png")
            assert (item["width"], item["height"]) == (expected.width, expected.height)
            assert item["image_sha256"] == sha256(path.read_bytes())
            assert metadata["forecast_validity"] is None
    assert len(calls) == 1


def test_cache_hit_regeneration_and_uniqueness(setup):
    service, _, calls, _ = setup
    first = service.ingest(2026, 7)
    assert service.ingest(2026, 7)["cache_hit"]
    path, _, _ = service.page(2026, 7, 12)
    path.write_bytes(b"corrupt")
    assert not service.ingest(2026, 7)["cache_hit"]
    path, _, _ = service.page(2026, 7, 13)
    path.unlink()
    service.ingest(2026, 7)
    assert len(calls) == 1
    assert service.ingest(2026, 7)["fetched_at"] == first["fetched_at"]
    with service.connect() as con:
        assert con.execute("SELECT count(*) FROM hab_bulletins").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM hab_bulletin_pages").fetchone()[0] == 2
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("INSERT INTO hab_bulletins(year,week,source_url,status) VALUES(2026,7,'x','failed')")


def test_corrupt_pdf_requires_download(setup):
    service, _, calls, _ = setup
    service.ingest(2026, 7)
    record = service._read(2026, 7)
    service.asset_path(record["pdf_path"]).write_bytes(b"broken")
    service.ingest(2026, 7)
    assert len(calls) == 2


@pytest.mark.parametrize("content", [b"<html>ok</html>", b"%PDF-invalid", sample_pdf(12)])
def test_bad_pdf_persisted_failure(setup, content):
    service, _, _, _ = setup
    service.transport = httpx.MockTransport(lambda request: httpx.Response(200, content=content))
    with pytest.raises(HABError):
        service.ingest(2026, 7)
    record = service._read(2026, 7)
    assert record["status"] == "failed"
    assert record["error_message"]
    assert not record["page_metadata"]
    assert not list(service.root.glob("2026/week_07/*/page*.png"))


def test_404_negative_cache(setup):
    service, _, calls, _ = setup

    def missing(request):
        calls.append(1)
        return httpx.Response(404)

    service.transport = httpx.MockTransport(missing)
    for _ in range(2):
        with pytest.raises(HABError) as error:
            service.ingest(2026, 7)
        assert error.value.status_code == 404
    assert len(calls) == 1
    assert service._read(2026, 7)["retry_after"] > time.time()


@pytest.mark.parametrize("status", [429, 500, 503])
def test_bounded_retries(setup, monkeypatch, status):
    service, _, calls, raw = setup
    monkeypatch.setattr("marine_tools.hab.time.sleep", lambda seconds: None)
    service.config.retries = 2

    def respond(request):
        calls.append(1)
        return httpx.Response(status if len(calls) < 3 else 200, content=raw)

    service.transport = httpx.MockTransport(respond)
    assert service.ingest(2026, 7)["status"] == "available"
    assert len(calls) == 3


def test_timeout_retry_failure(setup, monkeypatch):
    service, _, calls, _ = setup
    monkeypatch.setattr("marine_tools.hab.time.sleep", lambda seconds: None)
    service.config.retries = 1

    def respond(request):
        calls.append(1)
        raise httpx.ReadTimeout("timeout")

    service.transport = httpx.MockTransport(respond)
    with pytest.raises(HABError) as error:
        service.ingest(2026, 7)
    assert error.value.status_code == 503
    assert len(calls) == 2


def test_concurrent_requests_across_service_instances(setup):
    service, _, calls, _ = setup
    other = HABService(service.db, service.config, service.transport)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda i: (service if i % 2 else other).ingest(2026, 7), range(6)))
    assert len(calls) == 1
    assert sum(not r["cache_hit"] for r in results) == 1


def test_failed_refresh_keeps_generation_atomic(setup, monkeypatch):
    service, _, _, _ = setup
    first = service.ingest(2026, 7)
    before = service._read(2026, 7)

    def broken_render(raw, directory, dpi):
        (directory / "page_12.png").write_bytes(b"partial")
        raise ValueError("render failure after page 12")

    monkeypatch.setattr("marine_tools.hab.render_pages", broken_render)
    with pytest.raises(HABError):
        service.ingest(2026, 7, force=True)
    after = service._read(2026, 7)
    assert after["pdf_path"] == before["pdf_path"]
    assert after["page_metadata"] == before["page_metadata"]
    assert service.ingest(2026, 7)["pdf_sha256"] == first["pdf_sha256"]
    assert after["status"] == "available" and "render failure" in after["error_message"]
    assert not list(service.root.glob("2026/week_07/.tmp-*"))


def test_database_publish_failure_keeps_old_generation(setup):
    service, _, _, _ = setup
    service.ingest(2026, 7)
    before = service._read(2026, 7)
    with service.connect() as con:
        con.execute("CREATE TRIGGER fail_page BEFORE UPDATE ON hab_bulletin_pages BEGIN SELECT RAISE(ABORT,'test failure'); END")
    with pytest.raises(HABError):
        service.ingest(2026, 7, force=True)
    assert service._read(2026, 7)["pdf_path"] == before["pdf_path"]
    assert service._valid(service._read(2026, 7))
    assert len(list(service.root.glob("2026/week_07/*/bulletin.pdf"))) == 1


def test_successful_refresh_keeps_old_readers_safe(setup):
    service, _, calls, _ = setup
    service.ingest(2026, 7)
    old, _, _ = service.page(2026, 7, 12)
    old_bytes = old.read_bytes()
    service.ingest(2026, 7, force=True)
    new, _, _ = service.page(2026, 7, 12)
    assert old != new and old.read_bytes() == old_bytes
    assert len(calls) == 2


@pytest.mark.parametrize("start,end", [(1, 51), (2, 52), (-1, 20), (9, 8), (2.0, 51), (True, 51)])
def test_backfill_boundaries(start, end):
    with pytest.raises(ValidationError):
        BackfillRequest(start_week=start, end_week=end)
    assert BackfillRequest(start_week=2, end_week=51).end_week == 51


def wait_job(service, job_id):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = service.jobs.status(job_id)
        if result["status"] == "completed":
            return result
        time.sleep(0.02)
    pytest.fail("Backfill did not complete")


def test_backfill_progress_cached_missing_failed_and_resume(setup):
    service, _, calls, raw = setup
    service.ingest(2026, 2)

    def respond(request):
        calls.append(str(request.url))
        if "Week_04" in str(request.url):
            return httpx.Response(404)
        return httpx.Response(200, content=b"html" if "Week_05" in str(request.url) else raw)

    service.transport = httpx.MockTransport(respond)
    job = service.jobs.submit(2026, 2, 5)
    result = wait_job(service, job["job_id"])
    assert (result["total"], result["completed"], result["cached"], result["downloaded"], result["missing"], result["failed"]) == (4, 4, 1, 1, 1, 1)
    assert result["total_storage_bytes"] > 0
    assert result["extracted_images"] == 2 and len(result["errors"]) == 2
    service.close()
    # Simulate a process dying between marking a week processing and publishing its result.
    with service.connect() as con:
        con.execute("UPDATE hab_jobs SET status='running' WHERE id=?", (job["job_id"],))
        con.execute("UPDATE hab_job_weeks SET status='processing' WHERE job_id=? AND week=3", (job["job_id"],))
    service._jobs = None
    count = len(calls)
    resumed = wait_job(service, job["job_id"])
    assert resumed["completed"] == 4 and resumed["cached"] == 2
    assert len(calls) == count  # Published data is reused, even if its job result was lost.


def test_http_routes_validation_auth_content_type_and_cache(setup, monkeypatch):
    service, system, calls, raw = setup
    monkeypatch.setenv("HAB_ADMIN_TOKEN", "test-secret")
    with TestClient(create_app(system)) as client:
        for week in ("0", "1", "52", "53", "-1", "7.0", "banana"):
            assert client.get(f"/api/hab/2026/{week}").status_code == 422
            assert client.get(f"/api/hab/2026/{week}/page/12").status_code == 422
        assert not calls
        assert client.get("/api/hab/2026/7/page/11").status_code == 422
        assert client.get("/api/hab/2026/7/page/12.0").status_code == 422
        assert client.get("/api/hab/2026/7/page/12").status_code == 404
        assert not calls
        receipt = client.post("/tools/fetch_hab_bulletin", json={"year": 2026, "week": 7}).json()
        assert client.get("/api/hab/2026/7/page/12").status_code == 404
        assert client.post("/tools/cache_hab_bulletin", json={"fetch_id": receipt["fetch_id"]}).status_code == 200
        for page in (12, 13):
            response = client.get(f"/api/hab/2026/7/page/{page}")
            assert response.status_code == 200
            assert response.headers["content-type"] == "image/png"
            assert response.content.startswith(b"\x89PNG")
        assert len(calls) == 1
        assert client.get("/api/hab/available?year=2026").json()["bulletins"][0]["week"] == 7
        assert client.get("/api/hab/2026/7").json()["cache_hit"]
        assert client.post("/api/hab/2026/7/refresh").status_code == 401
        assert client.post("/tools/backfill_hab_bulletins", json={}).status_code == 401
        headers = {"Authorization": "Bearer test-secret"}
        assert client.post("/api/hab/backfill", json={"start_week": 1}, headers=headers).status_code == 422
        assert client.post("/api/hab/backfill", json={"start_week": 2.0}, headers=headers).status_code == 422
        assert client.post("/api/hab/2026/7/refresh", headers=headers).status_code == 200
        assert len(calls) == 2
        job = client.post("/api/hab/backfill", json={"start_week": 7, "end_week": 7}, headers=headers)
        assert job.status_code == 202
        result = wait_job(service, job.json()["job_id"])
        assert result["cached_skipped"] == 1
        assert client.get(job.json()["status_url"]).status_code == 200
        assert client.get("/api/hab/backfill/unknown").status_code == 404


def test_service_discovery_does_not_download(setup):
    _, system, calls, _ = setup
    with TestClient(create_app(system)) as client:
        response = client.get("/api/hab")
        assert response.status_code == 200
        info = response.json()
        assert info["pages"] == [12, 13]
        assert info["week_range"] == {"minimum": 2, "maximum": 51}
        assert info["render_dpi"] == 200
        assert info["endpoints"]["image"]["path"] == "/api/hab/{year}/{week}/page/{page}"
        assert info["endpoints"]["refresh"]["requires_admin_token"]
        assert "get_hab_page" in info["tools"]
        assert not calls


def test_admin_rate_limit_and_disabled_by_default(setup, monkeypatch):
    _, system, _, _ = setup
    monkeypatch.delenv("HAB_ADMIN_TOKEN", raising=False)
    with TestClient(create_app(system)) as client:
        assert client.post("/api/hab/2026/7/refresh").status_code == 503
        monkeypatch.setenv("HAB_ADMIN_TOKEN", "secret")
        headers = {"Authorization": "Bearer secret"}
        for _ in range(10):
            assert client.post("/api/hab/2026/1/refresh", headers=headers).status_code == 422
        assert client.post("/api/hab/2026/7/refresh", headers=headers).status_code == 429


def test_year_url_overrides_and_offline_listing_detects_corruption(setup):
    service, _, calls, _ = setup
    service.config.year_templates = {"2025": "https://example.org/archive/{year}/bulletin-{week:02d}.pdf"}
    assert service.source_url(2025, 2).endswith("2025/bulletin-02.pdf")
    service.ingest(2026, 7)
    path, _, _ = service.page(2026, 7, 12)
    path.unlink()
    assert service.available(2026) == []
    assert len(calls) == 1
