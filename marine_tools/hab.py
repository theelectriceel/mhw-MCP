"""Shared HAB service: persistent cache, provenance, bounded retrieval and rendering.

Immutable generations are published by one SQLite transaction. Readers can finish
serving an old generation during refresh; no partial or mixed generation is visible.
File locks also coordinate separate API and MCP processes on this machine.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import threading
import time
from typing import Annotated
from uuid import uuid4

from filelock import FileLock
import httpx
from pydantic import BaseModel, Field, StrictInt, model_validator

from .hab_pdf import render_pages, sha256
from .system import ToolError

log = logging.getLogger(__name__)
Week = Annotated[StrictInt, Field(ge=2, le=51)]
Year = Annotated[StrictInt, Field(ge=1, le=9999)]
Page = Annotated[StrictInt, Field(ge=12, le=13)]


class BulletinRequest(BaseModel):
    year: Year = 2026
    week: Week


class PageRequest(BulletinRequest):
    page: Page


class BackfillRequest(BaseModel):
    year: Year = 2026
    start_week: Week = 2
    end_week: Week = 51

    @model_validator(mode="after")
    def ordered(self):
        if self.start_week > self.end_week:
            raise ValueError("start_week must not exceed end_week")
        return self


class HABError(ToolError):
    def __init__(self, message, status_code=502, retry_after=None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass
class HABConfig:
    root: Path = field(default_factory=lambda: Path(os.getenv("HAB_CACHE_DIR", "data/hab")))
    template: str = field(default_factory=lambda: os.getenv("HAB_URL_TEMPLATE",
        "https://www.marine.ie/sites/default/files/MIFiles/Docs/SeafoodSafety/"
        "{year}_Week_{week:02d}_Irish_HAB_Bulletin.pdf"))
    year_templates: dict = field(default_factory=lambda: json.loads(os.getenv("HAB_YEAR_TEMPLATES", "{}")))
    concurrency: int = field(default_factory=lambda: int(os.getenv("HAB_CONCURRENCY", "2")))
    interval: float = field(default_factory=lambda: float(os.getenv("HAB_REQUEST_INTERVAL", "0.5")))
    timeout: float = field(default_factory=lambda: float(os.getenv("HAB_TIMEOUT", "30")))
    retries: int = field(default_factory=lambda: int(os.getenv("HAB_RETRIES", "2")))
    negative_ttl: int = field(default_factory=lambda: int(os.getenv("HAB_NEGATIVE_TTL", "21600")))
    max_bytes: int = 64 * 1024 * 1024
    dpi: int = 200

    def __post_init__(self):
        self.root = Path(self.root).resolve()
        if not 1 <= self.concurrency <= 8 or not 0 <= self.retries <= 5:
            raise ValueError("HAB_CONCURRENCY must be 1..8 and HAB_RETRIES 0..5")
        if self.timeout <= 0 or self.interval < 0 or self.negative_ttl <= 0:
            raise ValueError("HAB timing settings must be positive (interval may be zero)")


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def publish_directory(stage, generation):
    """Retry transient Windows/OneDrive file locks without exposing a partial generation."""
    for attempt in range(6):
        try:
            stage.replace(generation)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.05 * 2 ** attempt)


class HABService:
    def __init__(self, db="data/marine.sqlite", config=None, transport=None):
        self.db = Path(db)
        self.db.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or HABConfig()
        self.root = self.config.root
        (self.root / ".locks").mkdir(parents=True, exist_ok=True)
        self.transport = transport
        self._slots = threading.BoundedSemaphore(self.config.concurrency)
        self._rate_lock = threading.Lock()
        self._next_request = 0.0
        self._jobs = None
        self._jobs_lock = threading.Lock()
        with self.connect() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS hab_bulletins (
                    id INTEGER PRIMARY KEY, year INTEGER NOT NULL, week INTEGER NOT NULL CHECK(week BETWEEN 2 AND 51),
                    source_url TEXT NOT NULL, pdf_path TEXT, pdf_sha256 TEXT,
                    fetched_at TEXT, last_checked_at TEXT, status TEXT NOT NULL,
                    error_message TEXT, retry_after REAL, UNIQUE(year, week));
                CREATE TABLE IF NOT EXISTS hab_bulletin_pages (
                    id INTEGER PRIMARY KEY, bulletin_id INTEGER NOT NULL REFERENCES hab_bulletins(id),
                    page_number INTEGER NOT NULL CHECK(page_number IN (12,13)), image_path TEXT NOT NULL,
                    image_sha256 TEXT NOT NULL, image_size_bytes INTEGER NOT NULL,
                    width INTEGER NOT NULL, height INTEGER NOT NULL, rendered_at TEXT NOT NULL,
                    status TEXT NOT NULL, UNIQUE(bulletin_id, page_number));
            """)

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.db, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        try:
            with con:
                yield con
        finally:
            con.close()

    def source_url(self, year, week):
        request = BulletinRequest(year=year, week=week)
        template = self.config.year_templates.get(str(request.year), self.config.template)
        return template.format(year=request.year, week=request.week)

    def _read(self, year, week):
        with self.connect() as con:
            row = con.execute("SELECT * FROM hab_bulletins WHERE year=? AND week=?", (year, week)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["page_metadata"] = [dict(p) for p in con.execute(
                "SELECT * FROM hab_bulletin_pages WHERE bulletin_id=? ORDER BY page_number", (row["id"],))]
        return result

    def asset_path(self, stored):
        path = (self.root / stored).resolve()
        if not path.is_relative_to(self.root):
            raise HABError("Cache path is outside HAB storage")
        return path

    def _valid_file(self, path, digest):
        try:
            return bool(path and digest and sha256(self.asset_path(path).read_bytes()) == digest)
        except OSError:
            return False

    def _valid(self, record):
        return bool(record and record["status"] == "available"
                    and self._valid_file(record["pdf_path"], record["pdf_sha256"])
                    and {p["page_number"] for p in record["page_metadata"]} == {12, 13}
                    and all(p["status"] == "available" and self._valid_file(p["image_path"], p["image_sha256"])
                            for p in record["page_metadata"]))

    def _metadata(self, record, cache_hit=True):
        result = {k: record[k] for k in ("year", "week", "status", "source_url", "pdf_sha256",
                  "fetched_at", "last_checked_at", "error_message")}
        result.update(cache_hit=cache_hit, cache_status="valid", attribution="Marine Institute, Ireland",
                      data_role="modelled oceanographic bulletin imagery",
                      forecast_validity=None,
                      provenance_note="Issue week is not a forecast validity date or an observational measurement.")
        result["pages"] = {str(p["page_number"]): f"/api/hab/{record['year']}/{record['week']}/page/{p['page_number']}"
                           for p in record["page_metadata"]}
        result["page_metadata"] = [{k: v for k, v in p.items() if k not in ("id", "bulletin_id", "image_path")}
                                   for p in record["page_metadata"]]
        return result

    def _download(self, url):
        config = self.config
        with httpx.Client(timeout=config.timeout, transport=self.transport, follow_redirects=True,
                          headers={"User-Agent": "MarineTools-HAB/1.0", "Accept": "application/pdf"}) as client:
            for attempt in range(config.retries + 1):
                delay = 2 ** attempt
                with self._rate_lock:
                    time.sleep(max(0, self._next_request - time.monotonic()))
                    self._next_request = time.monotonic() + config.interval
                try:
                    with client.stream("GET", url) as response:
                        if response.status_code == 404:
                            raise HABError("Bulletin unavailable at the configured source", 404)
                        if response.status_code == 429 or response.status_code >= 500:
                            try:
                                delay = min(30, max(delay, float(response.headers.get("Retry-After", 0))))
                            except ValueError:
                                pass
                            raise httpx.HTTPStatusError("Upstream temporarily unavailable", request=response.request, response=response)
                        if response.status_code != 200:
                            raise HABError(f"Unexpected source HTTP {response.status_code}")
                        chunks, size = [], 0
                        for chunk in response.iter_bytes():
                            size += len(chunk)
                            if size > config.max_bytes:
                                raise HABError("PDF exceeds download size limit")
                            chunks.append(chunk)
                        raw = b"".join(chunks)
                        if not raw.lstrip().startswith(b"%PDF-"):
                            raise HABError("Source returned HTML or invalid PDF content")
                        return raw
                except httpx.HTTPError as exc:
                    if attempt == config.retries:
                        raise HABError(f"PDF retrieval failed after {attempt + 1} attempts: {type(exc).__name__}", 503) from exc
                    log.warning("HAB retry %s for %s: %s", attempt + 1, url, type(exc).__name__)
                    time.sleep(delay)

    def _failure(self, year, week, url, error, previous):
        retry = time.time() + (self.config.negative_ttl if error.status_code == 404 else 60)
        # Keep a complete previous generation available on refresh failure.
        status = "available" if self._valid(previous) else ("missing" if error.status_code == 404 else "failed")
        with self.connect() as con:
            con.execute("""INSERT INTO hab_bulletins(year,week,source_url,status,last_checked_at,error_message,retry_after)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(year,week) DO UPDATE SET
                status=excluded.status,last_checked_at=excluded.last_checked_at,
                error_message=excluded.error_message,retry_after=excluded.retry_after""",
                (year, week, url, status, utcnow(), str(error), retry))
        log.warning("HAB %s week %02d: %s", year, week, error)

    def fetch_bulletin(self, year=2026, week=2):
        """Fetch a PDF to transport staging only, without updating the bulletin cache."""
        from .staging import stage
        from uuid import uuid4
        import pymupdf
        url = self.source_url(year, week)
        raw = self._download(url)
        try:
            with pymupdf.open(stream=raw, filetype="pdf") as pdf:
                if pdf.needs_pass or pdf.page_count < 13:
                    raise ValueError("Bulletin requires at least 13 unencrypted physical pages")
        except Exception as exc:
            raise HABError(f"Invalid fetched PDF: {exc}") from exc
        fetch_id = uuid4().hex
        folder = self.db.parent / "fetches" / fetch_id
        folder.mkdir(parents=True)
        (folder / "bulletin.pdf").write_bytes(raw)
        data = {"year": year, "week": week, "source_url": url, "pdf_sha256": sha256(raw), "fetched_at": utcnow()}
        stage(self.db.parent / "fetches", "hab", data, fetch_id)
        return {**data, "fetch_id": fetch_id, "cached": False, "next_step": "cache_hab_bulletin"}

    def cache_bulletin(self, fetch_id):
        """Render/publish a fetched PDF offline; never download on a missing receipt."""
        from .staging import load
        receipt = load(self.db.parent / "fetches", fetch_id, "hab")
        BulletinRequest(year=receipt["year"], week=receipt["week"])
        try:
            raw = (self.db.parent / "fetches" / fetch_id / "bulletin.pdf").read_bytes()
        except OSError as exc:
            raise HABError("Fetched PDF is missing; call fetch_hab_bulletin first", 404) from exc
        if sha256(raw) != receipt["pdf_sha256"]:
            raise HABError("Fetched PDF hash mismatch; fetch it again", 409)
        return self.ingest(receipt["year"], receipt["week"], force=True, _fetched=(raw, receipt["fetched_at"], receipt["source_url"]))

    def cached(self, year=2026, week=2):
        """Read valid cache only. Missing/corrupt assets never trigger network or repair."""
        BulletinRequest(year=year, week=week)
        record = self._read(year, week)
        if not self._valid(record):
            raise HABError("Bulletin is not cached or its files are invalid. Call fetch_hab_bulletin, then cache_hab_bulletin explicitly.", 404)
        return self._metadata(record)

    def ingest(self, year=2026, week=2, force=False, _fetched=None):
        """Explicit legacy batch orchestration; read endpoints never call this method."""
        url = self.source_url(year, week)  # Strict validation before locks, cache or network.
        if type(force) is not bool:
            raise ValueError("force must be a boolean")
        with FileLock(str(self.root / ".locks" / f"{year}_{week}.lock"), timeout=180):
            previous = self._read(year, week)
            if _fetched is not None and self._valid(previous) and previous["fetched_at"] >= _fetched[1]:
                return self._metadata(previous)
            if not force and self._valid(previous):
                return self._metadata(previous)
            if not force and previous and previous["status"] != "available" and (previous["retry_after"] or 0) > time.time():
                raise HABError(previous["error_message"], 404 if previous["status"] == "missing" else 503,
                               max(1, int(previous["retry_after"] - time.time())))
            with self._slots:
                generation = self.root / str(year) / f"week_{week:02d}" / uuid4().hex
                stage = generation.with_name(".tmp-" + generation.name)
                published = False
                try:
                    reuse = not force and previous and self._valid_file(previous["pdf_path"], previous["pdf_sha256"])
                    if _fetched is not None:
                        raw, fetched, url = _fetched
                    else:
                        raw = self.asset_path(previous["pdf_path"]).read_bytes() if reuse else self._download(url)
                        fetched = previous["fetched_at"] if reuse else utcnow()
                    stage.mkdir(parents=True)
                    (stage / "bulletin.pdf").write_bytes(raw)
                    pages = render_pages(raw, stage, self.config.dpi)
                    publish_directory(stage, generation)
                    now = utcnow()
                    with self.connect() as con:
                        con.execute("""INSERT INTO hab_bulletins(year,week,source_url,pdf_path,pdf_sha256,fetched_at,
                            last_checked_at,status) VALUES(?,?,?,?,?,?,?,'available') ON CONFLICT(year,week)
                            DO UPDATE SET source_url=excluded.source_url,pdf_path=excluded.pdf_path,
                            pdf_sha256=excluded.pdf_sha256,fetched_at=excluded.fetched_at,
                            last_checked_at=excluded.last_checked_at,status='available',error_message=NULL,retry_after=NULL""",
                            (year, week, previous["source_url"] if reuse else url,
                             str((generation / "bulletin.pdf").relative_to(self.root)), sha256(raw), fetched, now))
                        bulletin_id = con.execute("SELECT id FROM hab_bulletins WHERE year=? AND week=?", (year, week)).fetchone()[0]
                        for page in pages:
                            con.execute("""INSERT INTO hab_bulletin_pages(bulletin_id,page_number,image_path,image_sha256,
                                image_size_bytes,width,height,rendered_at,status) VALUES(?,?,?,?,?,?,?,?,'available')
                                ON CONFLICT(bulletin_id,page_number) DO UPDATE SET image_path=excluded.image_path,
                                image_sha256=excluded.image_sha256,image_size_bytes=excluded.image_size_bytes,
                                width=excluded.width,height=excluded.height,rendered_at=excluded.rendered_at,status='available'""",
                                (bulletin_id, page["page_number"],
                                 str((generation / Path(page["image_path"]).name).relative_to(self.root)),
                                 page["image_sha256"], page["image_size_bytes"], page["width"], page["height"], now))
                    published = True
                    return self._metadata(self._read(year, week), cache_hit=False)
                except Exception as exc:
                    error = exc if isinstance(exc, HABError) else HABError(f"PDF processing failed: {exc}")
                    self._failure(year, week, url, error, previous)
                    raise error from exc
                finally:
                    if stage.exists():
                        shutil.rmtree(stage)
                    if not published and generation.exists():
                        shutil.rmtree(generation)

    def page(self, year, week, page):
        PageRequest(year=year, week=week, page=page)
        metadata = self.cached(year, week)
        record = self._read(year, week)
        item = next(p for p in record["page_metadata"] if p["page_number"] == page)
        return self.asset_path(item["image_path"]), item, metadata

    def available(self, year=None):
        if year is not None:
            BulletinRequest(year=year, week=2)
        with self.connect() as con:
            keys = con.execute("SELECT year,week FROM hab_bulletins WHERE status='available'"
                               + (" AND year=?" if year is not None else "") + " ORDER BY year,week",
                               (year,) if year is not None else ()).fetchall()
        return [self._metadata(r) for y, w in keys if self._valid(r := self._read(y, w))]

    @property
    def jobs(self):
        with self._jobs_lock:
            if self._jobs is None:
                from .hab_jobs import BackfillJobs
                self._jobs = BackfillJobs(self)
            return self._jobs

    def close(self):
        if self._jobs is not None:
            self._jobs.close()
