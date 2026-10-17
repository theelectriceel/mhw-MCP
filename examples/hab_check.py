"""Opt-in live acceptance check. Run: python -m examples.hab_check."""
import json
from pathlib import Path

from fastapi.testclient import TestClient

from marine_tools import MarineTools
from marine_tools.api import create_app
from marine_tools.hab_pdf import sha256


def main():
    system = MarineTools()
    report = {"year": 2026, "week": 7, "pages": {}}
    with TestClient(create_app(system)) as client:
        metadata = client.get("/api/hab/2026/7")
        if metadata.status_code == 404:
            fetched = client.post("/tools/fetch_hab_bulletin", json={"year": 2026, "week": 7})
            fetched.raise_for_status()
            saved = client.post("/tools/cache_hab_bulletin", json={"fetch_id": fetched.json()["fetch_id"]})
            saved.raise_for_status()
            metadata = client.get("/api/hab/2026/7")
        metadata.raise_for_status()
        report["bulletin"] = metadata.json()
        for page in (12, 13):
            path = f"/api/hab/2026/7/page/{page}"
            first = client.get(path)
            first.raise_for_status()
            second = client.get(path)
            second.raise_for_status()
            assert first.headers["content-type"] == "image/png"
            assert first.content == second.content
            report["pages"][str(page)] = {"bytes": len(first.content), "sha256": sha256(first.content)}
        # Prove the second metadata request uses the persistent cache without a transport.
        def no_download(url):
            raise AssertionError("Cache hit attempted a download")
        system.hab._download = no_download
        cached = client.get("/api/hab/2026/7")
        cached.raise_for_status()
        assert cached.json()["cache_hit"]
        for page in (12, 13):
            client.get(f"/api/hab/2026/7/page/{page}").raise_for_status()
        report["offline_cache_verified"] = True
    target = Path("data/hab_check.json")
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
