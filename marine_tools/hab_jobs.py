"""SQLite-backed, restartable backfills with one local scheduler owner."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import threading
import time
from uuid import uuid4

from filelock import FileLock, Timeout

from .hab import BackfillRequest, HABError, utcnow

log = logging.getLogger(__name__)


class BackfillJobs:
    def __init__(self, service):
        self.service = service
        with service.connect() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS hab_jobs (
                    id TEXT PRIMARY KEY, year INTEGER NOT NULL, start_week INTEGER NOT NULL,
                    end_week INTEGER NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
                    elapsed_seconds REAL NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS hab_job_weeks (
                    job_id TEXT NOT NULL REFERENCES hab_jobs(id), week INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', error TEXT,
                    PRIMARY KEY(job_id, week));
            """)
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.thread = threading.Thread(target=self._loop, name="hab-backfill", daemon=True)
        self.thread.start()

    def submit(self, year=2026, start_week=2, end_week=51):
        request = BackfillRequest(year=year, start_week=start_week, end_week=end_week)
        job_id = uuid4().hex
        with self.service.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if con.execute("SELECT count(*) FROM hab_jobs WHERE status IN ('queued','running')").fetchone()[0] >= 4:
                raise HABError("Backfill queue is full; retry after a job completes", 429)
            con.execute("INSERT INTO hab_jobs(id,year,start_week,end_week,status,created_at) VALUES(?,?,?,?,'queued',?)",
                        (job_id, request.year, request.start_week, request.end_week, utcnow()))
            con.executemany("INSERT INTO hab_job_weeks(job_id,week) VALUES(?,?)",
                            [(job_id, w) for w in range(start_week, end_week + 1)])
        self.wake.set()
        return {"job_id": job_id, "status": "queued", "status_url": f"/api/hab/backfill/{job_id}"}

    def status(self, job_id):
        with self.service.connect() as con:
            row = con.execute("SELECT * FROM hab_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise HABError("Unknown backfill job", 404)
            items = [dict(r) for r in con.execute("SELECT week,status,error FROM hab_job_weeks WHERE job_id=? ORDER BY week", (job_id,))]
        result = dict(row)
        counts = {state: sum(i["status"] == state for i in items)
                  for state in ("downloaded", "cached", "missing", "failed")}
        result.update(total=len(items), completed=sum(counts.values()), **counts)
        result["cached_skipped"] = counts["cached"]
        result["currently_processing"] = [i["week"] for i in items if i["status"] == "processing"]
        result["errors"] = [i for i in items if i["error"]]
        result["extracted_images"] = counts["downloaded"] * 2
        result["total_storage_bytes"] = 0
        result["active_storage_bytes"] = 0
        for item in items:
            directory = self.service.root / str(row["year"]) / f"week_{item['week']:02d}"
            if directory.exists():
                result["total_storage_bytes"] += sum(p.stat().st_size for p in directory.glob("*/*")
                                                     if p.is_file() and p.suffix in (".pdf", ".png"))
            if item["status"] in ("downloaded", "cached"):
                record = self.service._read(row["year"], item["week"])
                if record and self.service._valid(record):
                    result["active_storage_bytes"] += self.service.asset_path(record["pdf_path"]).stat().st_size
                    result["active_storage_bytes"] += sum(p["image_size_bytes"] for p in record["page_metadata"])
        return result

    def _week(self, job_id, year, week):
        with self.service.connect() as con:
            con.execute("UPDATE hab_job_weeks SET status='processing',error=NULL WHERE job_id=? AND week=?", (job_id, week))
        try:
            result = self.service.ingest(year, week)
            state, error = ("cached" if result["cache_hit"] else "downloaded"), None
        except Exception as exc:
            state = "missing" if isinstance(exc, HABError) and exc.status_code == 404 else "failed"
            error = str(exc)
        with self.service.connect() as con:
            con.execute("UPDATE hab_job_weeks SET status=?,error=? WHERE job_id=? AND week=?", (state, error, job_id, week))

    def _run(self, job):
        start = time.monotonic()
        job_id = job["id"]
        with self.service.connect() as con:
            con.execute("UPDATE hab_jobs SET status='running' WHERE id=?", (job_id,))
            con.execute("UPDATE hab_job_weeks SET status='pending' WHERE job_id=? AND status='processing'", (job_id,))
            weeks = [r[0] for r in con.execute("SELECT week FROM hab_job_weeks WHERE job_id=? AND status='pending'", (job_id,))]
        try:
            # Only one bounded chunk is scheduled at a time; shutdown leaves pending items resumable.
            with ThreadPoolExecutor(max_workers=self.service.config.concurrency) as pool:
                for offset in range(0, len(weeks), self.service.config.concurrency):
                    if self.stop.is_set():
                        break
                    futures = [pool.submit(self._week, job_id, job["year"], week)
                               for week in weeks[offset:offset + self.service.config.concurrency]]
                    for future in as_completed(futures):
                        future.result()
            with self.service.connect() as con:
                pending = con.execute("SELECT count(*) FROM hab_job_weeks WHERE job_id=? AND status IN ('pending','processing')", (job_id,)).fetchone()[0]
                con.execute("UPDATE hab_jobs SET status=?,elapsed_seconds=elapsed_seconds+? WHERE id=?",
                            ("queued" if pending else "completed", time.monotonic() - start, job_id))
            if not pending:
                log.info("HAB backfill summary: %s", json.dumps(self.status(job_id)))
        except Exception:
            log.exception("Backfill interrupted; unfinished weeks will resume: %s", job_id)

    def _loop(self):
        # A second MCP/API process may enqueue jobs, but cannot steal a running job.
        lock = FileLock(str(self.service.root / ".locks" / "scheduler.lock"))
        while not self.stop.is_set():
            try:
                with lock.acquire(timeout=0):
                    while not self.stop.is_set():
                        with self.service.connect() as con:
                            row = con.execute("SELECT * FROM hab_jobs WHERE status IN ('queued','running') ORDER BY created_at LIMIT 1").fetchone()
                        if row:
                            self._run(dict(row))
                        else:
                            self.wake.wait(1)
                            self.wake.clear()
            except Timeout:
                self.stop.wait(1)
            except Exception:
                log.exception("HAB scheduler failure")
                self.stop.wait(1)

    def close(self):
        self.stop.set()
        self.wake.set()
        self.thread.join()
