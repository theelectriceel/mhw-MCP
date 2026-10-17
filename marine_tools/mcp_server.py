"""MCP is only an interface; all ETL and analysis live in Pipeline."""
from mcp.server import MCPServer
from contextlib import asynccontextmanager
from functools import partial
import asyncio
from .hab import Week, Year, Page


@asynccontextmanager
async def mcp_lifespan(system, server):
    """All entry points resume durable jobs and close workers on shutdown."""
    system.hab.jobs
    try:
        yield {}
    finally:
        await asyncio.to_thread(system.hab.close)


def create_server(system):
    server = MCPServer("marine-data", lifespan=partial(mcp_lifespan, system),
                       instructions="Fetch and cache are separate: fetch_data returns a fetch_id without publishing; cache_store(fetch_id) publishes offline. All get/query/analysis tools are cache-only, including HAB. Never auto-fetch on a cache miss. pipeline_ingest and ingest_ocean_colour are explicit compatibility batch workflows. Report freshness, coverage and matching limits.")

    @server.tool()
    def list_sources() -> dict:
        """List configured data endpoints and adapter settings."""
        return system.list_sources()

    @server.tool()
    def pipeline_status() -> dict:
        """Inspect normalized Parquet datasets, freshness and common schema offline."""
        return system.pipeline_status()

    @server.tool()
    def fetch_data(source: str, columns: list[str] | None = None, constraints: list[dict] | None = None,
                   slices: list[dict] | None = None, query: str | None = None,
                   cds_request: dict | None = None, latitude: float | None = None, longitude: float | None = None) -> dict:
        """Always fetch live upstream data. Return a staging fetch_id; do not update raw cache or analytical datasets."""
        return system.fetch_data(source, columns, constraints, slices, query, cds_request, latitude, longitude)

    @server.tool()
    def cache_store(fetch_id: str, publish: bool = True) -> dict:
        """Save an already fetched response offline. By default also standardize to Parquet. Never downloads."""
        return system.cache_store(fetch_id, publish)

    @server.tool()
    def pipeline_publish(key: str) -> dict:
        """Publish an existing raw cache snapshot to Parquet offline. Never fetches missing data."""
        return system.pipeline_publish(key)

    @server.tool()
    def pipeline_ingest(sources: list[str] | None = None, force: bool = False, selections: dict | None = None) -> dict:
        """Explicit compatibility batch workflow combining fetch and cache. Prefer fetch_data then cache_store for separate control."""
        return system.pipeline_ingest(sources, force, selections)

    @server.tool()
    def pipeline_query(source: str, start: str | None = None, end: str | None = None,
                       filters: list[dict] | None = None, limit: int = 100, offset: int = 0, allow_stale: bool = False) -> dict:
        """Query standardized data offline by source. Dates are inclusive ISO timestamps; limits 1..10000."""
        return system.pipeline_query(source, start, end, filters, limit, offset, allow_stale)

    @server.tool()
    def analyze_exposure(window_days: int = 7, radius_km: float = 20, min_coverage: float = 0.7,
                         start: str | None = None, end: str | None = None, limit: int = 1000,
                         allow_stale: bool = False, phyto_source: str = "bantry_phyto", sst_source: str = "eurosea_sst",
                         clim_source: str = "eurosea_clim", mhw_source: str = "eurosea_mhw", cs_source: str = "eurosea_cs") -> dict:
        """Match each phyto observation to nearest point and trailing daily exposure. Returns coverage, unmatched rows and provenance."""
        options = {k: v for k, v in locals().items() if k != "system"}
        return system.analyze_exposure(**options)

    @server.tool()
    def analyze_relationships(window_days: int = 7, radius_km: float = 20, min_coverage: float = 0.7,
                              start: str | None = None, end: str | None = None, allow_stale: bool = False) -> dict:
        """Exploratory per-species anomaly/count Pearson correlations. No causal or forecasting interpretation."""
        options = {k: v for k, v in locals().items() if k != "system"}
        return system.analyze_relationships(**options)

    @server.tool()
    def register_source(name: str, url: str, adapter: str = "erddap", dataset_type: str | None = None,
                        location: str | None = None, ttl_seconds: int = 86400, default_query: str | None = None,
                        response_format: str = "json") -> dict:
        """Register another ERDDAP link or EuroSea dataset/point without modifying pipeline code."""
        return system.register_source(name, url, ttl_seconds, adapter, dataset_type, location, default_query, response_format)

    @server.tool()
    def ingest_ocean_colour(year: int = 2026, month: int = 3, day: int = 31,
                            latitude: float = 51.675, longitude: float = -9.625, force: bool = False) -> dict:
        """Ingest Ocean colour from 1997 to present derived from satellite observations.

        Daily chlorophyll-a in mg m-3 at the nearest grid cell. CDS credentials and
        dataset licence acceptance required. Recent dates may not yet be published.
        Query the resulting ocean_colour dataset using pipeline_query.
        """
        return system.ingest_ocean_colour(year, month, day, latitude, longitude, force)

    @server.tool()
    def get_hab_bulletin_pages(year: Year = 2026, week: Week = 2) -> dict:
        """Read cached physical HAB pages 12/13 and provenance. Cache miss returns an error; never downloads."""
        return system.get_hab_bulletin_pages(year, week)

    @server.tool()
    def get_hab_page(year: Year, week: Week, page: Page) -> dict:
        """Read cached PNG reference only. Never downloads on a miss. Run the local HTTP API to serve the URL."""
        return system.get_hab_page(year, week, page)

    @server.tool()
    def fetch_hab_bulletin(year: Year = 2026, week: Week = 2) -> dict:
        """Fetch a fresh PDF into staging and return fetch_id. Do not render or populate the bulletin cache."""
        return system.fetch_hab_bulletin(year, week)

    @server.tool()
    def cache_hab_bulletin(fetch_id: str) -> dict:
        """Render and cache an already fetched PDF offline. Never downloads on missing/corrupt receipt."""
        return system.cache_hab_bulletin(fetch_id)

    @server.tool()
    def list_available_hab_bulletins(year: Year | None = None) -> dict:
        """List cached HAB bulletins after verifying their files and hashes."""
        return system.list_available_hab_bulletins(year)

    @server.tool()
    def backfill_hab_bulletins(year: Year = 2026, start_week: Week = 2, end_week: Week = 51) -> dict:
        """Start a bounded, persistent background backfill; returns a job identifier."""
        return system.backfill_hab_bulletins(year, start_week, end_week)

    @server.tool()
    def hab_backfill_status(job_id: str) -> dict:
        """Return progress, errors, storage usage and elapsed time for a HAB job."""
        return system.hab_backfill_status(job_id)

    return server
