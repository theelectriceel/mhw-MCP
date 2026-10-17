"""Local JSON API and function-calling catalog. Bind only to loopback."""
import inspect
import os
import re
import secrets
import socket
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BeforeValidator, Field, ValidationError

from .system import ToolError


PARAMETERS = {
    "fetch_id": {"type": "string", "description": "Receipt returned by a fetch tool"},
    "publish": {"type": "boolean", "default": True, "description": "Also standardize the raw snapshot locally"},
    "month": {"type": "integer", "minimum": 1, "maximum": 12},
    "day": {"type": "integer", "minimum": 1, "maximum": 31},
    "latitude": {"type": "number", "minimum": -90, "maximum": 90},
    "longitude": {"type": "number", "minimum": -180, "maximum": 180},
    "cds_request": {"type": "object", "description": "One daily regular-grid chlorophyll-a v6.0 CDS request"},
    "year": {"type": "integer", "minimum": 1, "maximum": 9999, "default": 2026},
    "week": {"type": "integer", "minimum": 2, "maximum": 51},
    "page": {"type": "integer", "enum": [12, 13]},
    "start_week": {"type": "integer", "minimum": 2, "maximum": 51},
    "end_week": {"type": "integer", "minimum": 2, "maximum": 51},
    "job_id": {"type": "string"},
    "default_query": {"type": "string", "description": "Default selection for an ERDDAP link without a query"},
    "response_format": {"type": "string", "enum": ["json", "csv"]},
    "adapter": {"type": "string", "enum": ["erddap", "eurosea"]},
    "dataset_type": {"type": "string", "enum": ["sst", "clim", "mhw", "cs"]},
    "location": {"type": "string", "description": "Point token, e.g. 51.675N-9.625W"},
    "sources": {"type": "array", "items": {"type": "string"}},
    "selections": {"type": "object", "description": "Source name to ERDDAP selection arguments"},
    "start": {"type": "string", "description": "ISO UTC timestamp, inclusive"},
    "end": {"type": "string", "description": "ISO UTC timestamp, inclusive"},
    "window_days": {"type": "integer", "minimum": 1, "maximum": 366},
    "radius_km": {"type": "number", "exclusiveMinimum": 0, "maximum": 1000},
    "min_coverage": {"type": "number", "minimum": 0, "maximum": 1},
    **{n: {"type": "string"} for n in ["phyto_source", "sst_source", "clim_source", "mhw_source", "cs_source"]},
    "source": {"type": "string"}, "key": {"type": "string"},
    "name": {"type": "string"}, "url": {"type": "string"},
    "ttl_seconds": {"type": "integer", "minimum": 1},
    "force": {"type": "boolean"}, "allow_stale": {"type": "boolean"},
    "columns": {"type": "array", "items": {"type": "string"}},
    "query": {"type": "string", "description": "Raw ERDDAP selection, before URL encoding"},
    "constraints": {"type": "array", "items": {"type": "object", "properties": {
        "field": {"type": "string"}, "op": {"type": "string", "enum": ["=", "!=", ">", ">=", "<", "<=", "=~"]},
        "value": {}}, "required": ["field", "op", "value"], "additionalProperties": False}},
    "slices": {"type": "array", "items": {"type": "object", "properties": {
        "start": {"type": ["string", "number"]}, "stop": {"type": ["string", "number"]},
        "stride": {"type": "integer", "minimum": 1}}, "required": ["start", "stop"], "additionalProperties": False}},
    "filters": {"type": "array", "items": {"type": "object", "properties": {
        "field": {"type": "string"}, "op": {"type": "string", "enum": ["=", "!=", ">", ">=", "<", "<=", "in", "contains", "is_null"]},
        "value": {}}, "required": ["field", "op"], "additionalProperties": False}},
    "order_by": {"type": "string"}, "descending": {"type": "boolean"},
    "limit": {"type": "integer", "minimum": 1, "maximum": 10000},
    "offset": {"type": "integer", "minimum": 0}, "value": {"type": "string"},
    "group_by": {"type": "array", "items": {"type": "string"}},
}

TOOLS = {
    "fetch_data": ("Always fetch live data into transport staging; does not populate dataset caches.", ["source", "columns", "constraints", "slices", "query", "cds_request", "latitude", "longitude"]),
    "cache_store": ("Save a fetched response locally, optionally publishing Parquet. Offline; never downloads.", ["fetch_id", "publish"]),
    "pipeline_publish": ("Publish a raw cached snapshot into Parquet offline; never downloads.", ["key"]),
    "fetch_hab_bulletin": ("Fetch a fresh HAB PDF into staging without rendering or publishing.", ["year", "week"]),
    "cache_hab_bulletin": ("Render and publish a fetched HAB PDF offline; never downloads.", ["fetch_id"]),
    "ingest_ocean_colour": ("Ingest satellite-derived surface chlorophyll-a (mg m-3) for a date and nearest grid point. Requires CDS credentials and licence acceptance; recent dates may not be available.", ["year", "month", "day", "latitude", "longitude", "force"]),
    "get_hab_bulletin_pages": ("Read cached physical HAB pages 12/13 only; a cache miss never downloads.", ["year", "week"]),
    "get_hab_page": ("Return a retrievable cached HAB PNG URL; start the API to serve it.", ["year", "week", "page"]),
    "list_available_hab_bulletins": ("List validated cached HAB bulletins.", ["year"]),
    "backfill_hab_bulletins": ("Queue a resumable HAB backfill; weeks must be 2..51.", ["year", "start_week", "end_week"]),
    "hab_backfill_status": ("Read persistent HAB backfill progress and summary.", ["job_id"]),
    "list_sources": ("List configured ERDDAP sources.", []),
    "register_source": ("Register an ERDDAP link or EuroSea POST dataset/point.", ["name", "url", "ttl_seconds", "adapter", "dataset_type", "location", "default_query", "response_format"]),
    "build_url": ("Preview a source selection without fetching data; CDS URLs identify requests, not direct downloads.", ["source", "columns", "constraints", "slices", "query", "cds_request", "latitude", "longitude"]),
    "metadata": ("Fetch live ERDDAP schema, units and dimension ranges.", ["source"]),
    "cache_status": ("List cached snapshots, provenance and freshness; offline.", []),
    "cache_remove": ("Delete one cached snapshot.", ["key"]),
    "infer_query": ("Query a cached selection offline with filters, sorting and pagination.", ["key", "columns", "filters", "order_by", "descending", "limit", "offset", "allow_stale"]),
    "infer_summary": ("Compute grouped descriptive statistics offline, ignoring missing values.", ["key", "value", "group_by", "filters", "limit", "allow_stale"]),
    "pipeline_ingest": ("Explicit compatibility batch workflow combining fetch/cache. Prefer fetch_data then cache_store for separated operations.", ["sources", "force", "selections"]),
    "pipeline_status": ("Inspect standardized datasets, schema, freshness, and provenance offline.", []),
    "pipeline_query": ("Read standardized data by source name; offline, with dates and filters.", ["source", "start", "end", "filters", "limit", "offset", "allow_stale"]),
    "analyze_exposure": ("Match phytoplankton to nearest environmental point within radius and trailing daily exposure window; includes unmatched records and coverage.", ["phyto_source", "sst_source", "clim_source", "mhw_source", "cs_source", "window_days", "radius_km", "min_coverage", "start", "end", "limit", "allow_stale"]),
    "analyze_relationships": ("Exploratory per-species anomaly/count correlations with pairing counts; not predictive or causal.", ["phyto_source", "sst_source", "clim_source", "mhw_source", "cs_source", "window_days", "radius_km", "min_coverage", "start", "end", "allow_stale"]),
}


def catalog(system):
    result = []
    for name, (description, fields) in TOOLS.items():
        signature = inspect.signature(getattr(system, name))
        required = [p.name for p in signature.parameters.values()
                    if p.default is inspect.Parameter.empty and p.kind != p.VAR_KEYWORD]
        result.append({"type": "function", "name": name, "description": description,
                       "parameters": {"type": "object", "properties": {f: PARAMETERS[f] for f in fields},
                                      "required": required, "additionalProperties": False}})
    return result


def call_tool(system, name, arguments):
    if name not in TOOLS:
        raise ToolError(f"Unknown tool: {name}")
    if not isinstance(arguments, dict):
        raise ToolError("Tool arguments must be a JSON object")
    extra = set(arguments) - set(TOOLS[name][1])
    if extra:
        raise ToolError(f"Unknown arguments: {sorted(extra)}")
    try:
        return getattr(system, name)(**arguments)
    except (TypeError, KeyError, AttributeError) as exc:
        raise ToolError(f"Invalid tool arguments: {exc}") from exc


def _decimal(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    if type(value) is int:
        return value
    raise ValueError("Expected an integer")


HTTPYear = Annotated[int, BeforeValidator(_decimal), Field(ge=1, le=9999)]
HTTPWeek = Annotated[int, BeforeValidator(_decimal), Field(ge=2, le=51)]
HTTPPage = Annotated[int, BeforeValidator(_decimal), Field(ge=12, le=13)]


def create_app(system):
    from .hab import BackfillRequest, HABError

    @asynccontextmanager
    async def lifespan(app):
        system.hab.jobs  # Resume durable jobs on API restart.
        yield
        # Wait for active threads without blocking the event loop.
        from starlette.concurrency import run_in_threadpool
        await run_in_threadpool(system.hab.close)

    app = FastAPI(title="Marine data API", lifespan=lifespan)
    calls = deque()
    rate_lock = threading.Lock()

    def authorize(authorization: str | None = Header(default=None)):
        token = os.getenv("HAB_ADMIN_TOKEN")
        if not token:
            raise HTTPException(503, "Set HAB_ADMIN_TOKEN to enable HTTP backfill and refresh")
        if not authorization or not secrets.compare_digest(authorization, "Bearer " + token):
            raise HTTPException(401, "Invalid bearer token")
        with rate_lock:
            now = time.monotonic()
            while calls and calls[0] < now - 60:
                calls.popleft()
            if len(calls) >= 10:
                raise HTTPException(429, "Administrative request limit reached", headers={"Retry-After": "60"})
            calls.append(now)

    @app.middleware("http")
    async def no_cross_origin_mutations(request: Request, call_next):
        if request.method == "POST" and request.headers.get("Origin"):
            return JSONResponse(status_code=403, content={"error": "Browser cross-origin requests are disabled"})
        return await call_next(request)

    @app.exception_handler(HABError)
    async def hab_error(request, exc):
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc)},
                            headers={"Retry-After": str(exc.retry_after)} if exc.retry_after else None)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/tools")
    def tools():
        return catalog(system)

    @app.get("/sources")
    def sources():
        return system.list_sources()

    @app.get("/cache")
    def cache():
        return system.cache_status()

    @app.post("/tools/{name}")
    def invoke(name: str, arguments: dict, request: Request):
        if name == "backfill_hab_bulletins":
            authorize(request.headers.get("Authorization"))
        try:
            return call_tool(system, name, arguments)
        except ValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        except HABError:
            raise
        except ToolError as exc:
            raise HTTPException(400, str(exc)) from exc

    # Declare fixed routes before parameterized routes.
    @app.get("/api/hab", tags=["HAB bulletins"])
    def hab_service():
        """Discover the bulletin API without downloading PDFs or starting a backfill."""
        return {
            "service": "Marine Institute HAB bulletin imagery",
            "default_year": 2026,
            "week_range": {"minimum": 2, "maximum": 51},
            "pages": [12, 13],
            "image_format": "image/png",
            "render_dpi": system.hab.config.dpi,
            "source_url_template": system.hab.config.template,
            "year_source_overrides": system.hab.config.year_templates,
            "endpoints": {
                "bulletin": {"method": "GET", "path": "/api/hab/{year}/{week}"},
                "image": {"method": "GET", "path": "/api/hab/{year}/{week}/page/{page}"},
                "available": {"method": "GET", "path": "/api/hab/available"},
                "refresh": {"method": "POST", "path": "/api/hab/{year}/{week}/refresh", "requires_admin_token": True},
                "backfill": {"method": "POST", "path": "/api/hab/backfill", "requires_admin_token": True},
                "job_status": {"method": "GET", "path": "/api/hab/backfill/{job_id}"},
            },
            "read_policy": "cache_only; fetch and cache explicitly before reading",
            "tools": ["fetch_hab_bulletin", "cache_hab_bulletin", "get_hab_bulletin_pages", "get_hab_page", "list_available_hab_bulletins",
                      "backfill_hab_bulletins", "hab_backfill_status"],
            "data_role": "Modelled bulletin imagery; issue week does not establish forecast validity dates.",
            "docs_url": "/docs",
        }

    @app.get("/api/hab/available")
    def available(year: HTTPYear | None = None):
        return system.list_available_hab_bulletins(year)

    @app.post("/api/hab/backfill", status_code=202, dependencies=[Depends(authorize)])
    def backfill(body: BackfillRequest):
        return system.backfill_hab_bulletins(body.year, body.start_week, body.end_week)

    @app.get("/api/hab/backfill/{job_id}")
    def job_status(job_id: str):
        return system.hab_backfill_status(job_id)

    @app.get("/api/hab/{year}/{week}")
    def metadata(year: HTTPYear, week: HTTPWeek):
        return system.get_hab_bulletin_pages(year, week)

    @app.get("/api/hab/{year}/{week}/page/{page}")
    def page_image(year: HTTPYear, week: HTTPWeek, page: HTTPPage):
        path, item, _ = system.hab.page(year, week, page)
        return FileResponse(path, media_type="image/png", headers={
            "ETag": '"' + item["image_sha256"] + '"', "Cache-Control": "no-cache"})

    @app.post("/api/hab/{year}/{week}/refresh", dependencies=[Depends(authorize)])
    def refresh(year: HTTPYear, week: HTTPWeek):
        # Refresh now performs the network phase only. Cache the returned receipt explicitly.
        return system.fetch_hab_bulletin(year, week)

    return app


class LocalServer:
    """Keep the existing launcher/test interface while serving one FastAPI app."""
    def __init__(self, system, port):
        import uvicorn
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("127.0.0.1", port))
        self.server_port = self.socket.getsockname()[1]
        self.server = uvicorn.Server(uvicorn.Config(create_app(system), host="127.0.0.1",
                                                   port=self.server_port, log_level="warning"))
        self.done = threading.Event()

    def serve_forever(self):
        try:
            self.server.run(sockets=[self.socket])
        finally:
            self.done.set()

    def shutdown(self):
        self.server.should_exit = True
        self.done.wait()

    def server_close(self):
        self.server.should_exit = True
        self.socket.close()


def make_server(system, port=8000):
    return LocalServer(system, port)
