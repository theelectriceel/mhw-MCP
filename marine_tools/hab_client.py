"""Reusable HTTP client; presentation code never downloads or renders source PDFs."""
import os
import httpx

from .hab import BulletinRequest, PageRequest


class HABClient:
    def __init__(self, base_url=None, timeout=180):
        self.base_url = (base_url or os.getenv("HAB_API_URL", "http://127.0.0.1:8000")).rstrip("/")
        self.timeout = timeout

    def _get(self, path, image=False):
        response = httpx.get(self.base_url + path, timeout=self.timeout)
        response.raise_for_status()
        return response.content if image else response.json()

    def bulletin(self, year=2026, week=2):
        BulletinRequest(year=year, week=week)
        return self._get(f"/api/hab/{year}/{week}")

    def service(self):
        """Discover supported pages, parameter bounds, and API routes without ingestion."""
        return self._get("/api/hab")

    def fetch(self, year=2026, week=2):
        """Network phase only; returns a receipt without publishing to the cache."""
        BulletinRequest(year=year, week=week)
        response = httpx.post(self.base_url + "/tools/fetch_hab_bulletin",
                              json={"year": year, "week": week}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def cache(self, fetch_id):
        """Publish an existing receipt locally; never downloads the source PDF."""
        response = httpx.post(self.base_url + "/tools/cache_hab_bulletin",
                              json={"fetch_id": fetch_id}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def page(self, year, week, page):
        PageRequest(year=year, week=week, page=page)
        return self._get(f"/api/hab/{year}/{week}/page/{page}", image=True)

    def available(self, year=None):
        if year is not None:
            BulletinRequest(year=year, week=2)
        return self._get("/api/hab/available" + (f"?year={year}" if year is not None else ""))
