# Marine data pipeline

For the complete reference, see [Backend/MCP Documentation](backend/mcp%20documentation.md), including all 18 tools, the separate fetch/cache/read workflow, schemas, verification, and limitations.

**Fetch → cache → query or analyze.** These are separate operations across Python, HTTP, MCP, and Streamlit. Fetching returns a receipt without publishing a dataset; cache tools save it offline; reads never contact a source, including on a cache miss.

The three calls are **`fetch_data` → `cache_store` → `pipeline_query`**. Fetching alone does not update saved data; storing does not fetch; querying does neither.

```python
from marine_tools import MarineTools
marine = MarineTools()
fetched = marine.fetch_data("bantry_wave_buoy")  # Live source call; returns fetch_id
marine.cache_store(fetched["fetch_id"])          # Offline save + standardization
records = marine.pipeline_query("bantry_wave_buoy")  # Offline read
```

For HAB PDFs use `fetch_hab_bulletin(year, week)`, then `cache_hab_bulletin(fetch_id)`, then the GET/read tools. Streamlit provides separate Fetch, Save, and Load controls. A missing/corrupt cached image returns 404; it never downloads or repairs itself.

Explicit compatibility workflows (`pipeline_ingest`, `ingest_ocean_colour`, CLI `ingest`, and HAB backfills) still orchestrate multiple phases when deliberately called. They are not read calls. `/tools/cache_fetch` was removed; use the separate tools above.

## Quick start

The isolated project environment is installed. The following is the optional combined batch workflow; use the separate fetch/cache calls above for individual control:

```powershell
.\run.ps1 ingest
.\run.ps1 status
.\run.ps1 dashboard
```

For a fresh installation, create a Python 3.11+ environment and install `requirements.txt`. The launcher prefers `.venv`. Portable equivalent: `python -m marine_tools`.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Endpoints included

| Source | Transport | Data |
| --- | --- | --- |
| `eurosea_sst` | EuroSea POST | Daily SST, Celsius |
| `eurosea_clim` | EuroSea POST | Calendar-day mean, 10th/90th percentiles |
| `eurosea_mhw` | EuroSea POST | Heatwave intervals, duration, intensity, category |
| `eurosea_cs` | EuroSea POST | Cold-spell intervals, duration, intensity, category |
| `bantry_phyto` | ERDDAP tabledap JSON | Species observations and counts |
| `bantry_particles_csv` | ERDDAP griddap CSV GET | Particle hours and four grid dimensions |
| `bantry_wave_buoy` | ERDDAP tabledap CSV GET, station Bantry Bay | Sea temperature, significant wave height, peak period, peak direction |
| `bantry_particles` | Original griddap JSON link | Original fixed-date particle selection |
| `ocean_colour` | Copernicus CDS API, NetCDF | Satellite-derived surface chlorophyll-a concentration, mg m-3 |

The default workflow ingests all sources except the original `bantry_particles` JSON link and the credentialed, explicitly requested `ocean_colour` product. EuroSea uses requested point **51.675N, 9.625W**. Phytoplankton defaults to Bantry records from January 1, 2025 onward. Edit `pipeline.json` to change these selections.

### Ocean colour: chlorophyll-a concentration

Registered name: **Ocean colour from 1997 to present derived from satellite observations** (`ocean_colour`). This uses `cdsapi.Client().retrieve('satellite-ocean-colour', request).download(target)` with your exact default request: daily `mass_concentration_of_chlorophyll_a`, regular latitude/longitude grid, version `6_0`, March 31, 2026.

```powershell
.\run.ps1 call ingest_ocean_colour '{"year":2026,"month":3,"day":31}'
.\run.ps1 call pipeline_query '{"source":"ocean_colour"}'
```

```python
from marine_tools import MarineTools
marine = MarineTools()
result = marine.ingest_ocean_colour(year=2026, month=3, day=31,
                                    latitude=51.675, longitude=-9.625)
if result['errors']:
    raise RuntimeError(result['errors'])
chlorophyll = marine.pipeline_query('ocean_colour')
```

In Streamlit, choose **Ocean colour** to select a date and location and retrieve the product. The **Environmental data** view also displays an ingested `ocean_colour` dataset using its full registered name. HTTP exposes `POST /tools/ingest_ocean_colour` and the existing `pipeline_query` tool. MCP exposes `ingest_ocean_colour` with the same arguments, plus the existing query tools. This is an explicit ingestion operation: CDS can queue a request while preparing its output, so the call may take time. Cached requests avoid the CDS queue.

The raw download is preserved under `data/raw/ocean_colour/`, named by SHA-256, with a request manifest. Both direct NetCDF and ZIP/tar downloads are supported; archive paths are never trusted for extraction. One request selects one date, and additional dates must be requested separately. Dates from September 1997 through today can be submitted; this describes the catalogue coverage, not a download of the entire archive. Published availability is determined by CDS and recent dates may be unavailable: the catalogue lists quarterly updates with a 9–12 month delay. The service reports failure instead of substituting another date.

The downloaded global product remains intact. For this project's Bantry analysis, normalization reads only the nearest cell to the configured/requested point (default **51.675N, 9.625W**, within 0.05° on each axis) into SQLite and standardized Parquet/DuckDB. This avoids loading or inserting the entire global grid into the local analytical database. Each result retains its actual grid centre, observation date, requested point, source unit and raw-download provenance. A new explicit fetch always contacts CDS, even for another point on the same day. A previously cached exact selection is reused only by the optional compatibility batch workflow. No interpolation, coastal neighbour substitution or averaging is performed. Changing dates/points changes the snapshot key; `pipeline_query('ocean_colour')` selects the most recently ingested selection, following the existing pipeline contract. Earlier selections remain accessible using snapshot keys and `infer_query`.

The standardized variable is `chlorophyll_a_concentration`, in **mg m-3 (mg/m³)**. CF masking/scaling is applied; masked, nonfinite and negative concentrations remain null. Cloud, ice and land masks may leave no estimate. Chlorophyll is a satellite-derived pigment concentration, not a phytoplankton species count, direct water sample, or HAB diagnosis. It is available for querying and plotting; the existing thermal-exposure analysis does not automatically join it to species observations.

Credentials remain outside source control. Configure your personal access token using the official [CDS setup guide](https://cds.climate.copernicus.eu/how-to-api), and accept the licences on the [dataset page](https://cds.climate.copernicus.eu/datasets/satellite-ocean-colour) before downloading. The standard `.cdsapirc` file in your user home is supported, or set these environment variables before launching the app/API/MCP:

```powershell
$env:CDSAPI_URL = 'https://cds.climate.copernicus.eu/api'
$env:CDSAPI_KEY = 'your-personal-access-token'
```

There are no credentials in `sources.json`, cache keys or provenance. The requested date, version, raw file hash, download timestamp, dataset URL and DOI `10.24381/cds.f85b319d` are retained. `force=true` downloads a new copy; invalid downloads preserve the previous valid cache and standardized dataset. The opt-in live check is `python -m examples.ocean_colour_check`, with its result saved to `data/ocean_colour_check.json`.

### Bantry wave buoy

The registered CSV query retains `station_id="Bantry Bay"` and all four requested measurements, adding source latitude/longitude for spatial analysis. Each upstream observation becomes four standardized measurement rows with variables `sea_temperature`, `significant_wave_height`, `peak_period`, and `peak_direction`. Units are preserved: `degree_C`, `cm`, `s`, and `degrees_true`. In particular, wave heights have not been relabeled as metres. Station identity appears in `location_name` and `sample_id`; timestamp plus station identifies an observation.

```powershell
.\run.ps1 call pipeline_ingest '{"sources":["bantry_wave_buoy"]}'
# Small historical example:
.\run.ps1 call pipeline_ingest '@examples/cache_wave_buoy.json'
```

```python
waves = marine.pipeline_query('bantry_wave_buoy', filters=[
    {'field': 'variable', 'op': '=', 'value': 'significant_wave_height'}
])
```

The registered query selects all available Bantry Bay history by default; use time constraints for smaller downloads. On verification, its latest timestamp was **September 24, 2024 at 10:00 UTC**, so it currently does not overlap the default 2025-onward phytoplankton selection. Wave ingestion is available through the same MCP/HTTP tools; the thermal exposure analysis does not automatically combine wave variables.

The new endpoint is `https://erddap.marine.ie/erddap/griddap/BANTRY_PARTICLES.csv`. A grid URL needs a variable/dimension selection, so `sources.json` supplies a default: **latest available time**, six release locations, spatial stride 20. This is subsampling, not averaging. The request actually uses CSV, retaining its names and units header rows. The original fixed October 17, 2026 JSON source is preserved.

## Simple Python interface

```python
from marine_tools import MarineTools
marine = MarineTools()
result = marine.pipeline_ingest()
if result['errors']:
    raise RuntimeError(result['errors'])

sst = marine.pipeline_query('eurosea_sst', start='2025-01-01', limit=100)
particles = marine.pipeline_query('bantry_particles_csv', filters=[
    {'field': 'release_location', 'op': '=', 'value': 0}
])
exposure = marine.analyze_exposure(window_days=7, radius_km=20)
relationships = marine.analyze_relationships(start='2025-01-01')
```

High-level tools use **source names**, so callers do not manage snapshot hashes.

| Layer | Functions |
| --- | --- |
| Configuration | `list_sources`, `register_source`, `metadata`, `build_url` |
| Fetch only | `fetch_data`, `fetch_hab_bulletin` |
| Offline cache | `cache_store`, `pipeline_publish`, `cache_hab_bulletin`, `cache_status`, `cache_remove` |
| Explicit batch workflow | `pipeline_ingest`, `ingest_ocean_colour`, `backfill_hab_bulletins` |
| Offline analysis | `pipeline_status`, `pipeline_query`, `analyze_exposure`, `analyze_relationships` |
| Low-level offline analysis by snapshot key | `infer_query`, `infer_summary` |

To ingest just the new endpoint:

```powershell
.\run.ps1 call pipeline_ingest '{"sources":["bantry_particles_csv"]}'
```

Override a grid selection through `selections`, using coordinate slices in dimension order **time, release_location, latitude, longitude**:

```python
marine.pipeline_ingest(sources=['bantry_particles_csv'], selections={
    'bantry_particles_csv': {'slices': [
        {'start': 'last', 'stop': 'last'},
        {'start': 0, 'stop': 5},
        {'start': 51.5, 'stop': 51.8, 'stride': 10},
        {'start': -10.0, 'stop': -9.4, 'stride': 10},
    ]}
})
```

Tabledap supports `columns` and additional AND `constraints`. Raw `query` replaces the full upstream selection and cannot be mixed with structured options. See `examples/cache_phyto.json` and `examples/cache_particles.json`.

## MCP and HTTP

The MCP stdio server exposes 18 tools, including separate fetch/cache operations, read/analysis calls, and explicit compatibility batch workflows.

Use `mcp-config.example.json` in an MCP client supporting `mcpServers` configuration. Absolute paths are filled in for this machine; update them when moving the project. `mcp_entry.py` works independently of the client's working directory. This example has **not** been installed into client settings. Manual launch: `.\run.ps1 mcp`; it waits for protocol messages from a client.

MCP contains no ETL or analytical formulas: it calls the same functions as the other interfaces. No API key is required for MCP itself; CDS credentials are needed for live ocean-colour fetches. Diagnostics go to stderr, leaving stdout for protocol messages.

For HTTP, run `.\run.ps1 serve --port 8000`:

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/health` | Health check |
| GET | `/sources` | Endpoint registry |
| GET | `/cache` | Raw snapshot inventory |
| GET | `/tools` | Function-calling catalog and JSON argument schemas |
| POST | `/tools/{name}` | Execute any catalog function with a JSON argument object |

```powershell
Invoke-RestMethod 'http://127.0.0.1:8000/tools/pipeline_query' -Method Post `
    -ContentType 'application/json' -Body '{"source":"bantry_particles_csv","limit":10}'
```

The HTTP API binds only to loopback and is intended for trusted local development.

## Standard data contract

Every Parquet file has one common schema; irrelevant fields are null:

- Provenance: `source`, `snapshot_key`.
- Measurements: `time`, `time_end`, `latitude`, `longitude`, `release_location`, `variable`, `value`, `unit`.
- Monitoring: `sample_id`, `species`, `location_name`.
- Climatology: `calendar_day` (`MM-DD`), `climatology_c`, `p10_c`, `p90_c`.
- Events: `event_id`, `duration_days`, `category`.
- Traceability: `raw_json` preserves the original row and unmodeled fields.

Times use UTC; date-only records become midnight UTC. Coordinates are degrees north/east, with west negative. EuroSea coordinates are **requested points**, not independently verified satellite-cell centers. Missing numbers become null. EuroSea temperatures are Celsius; absent ERDDAP units stay `unknown`. The phytoplankton count metadata currently does not state a unit. Climatology retains recurring calendar keys rather than acquiring an artificial observation year.

`pipeline_query` returns the common fields except `raw_json`, plus provenance. It supports inclusive `start`/`end`, `filters`, `limit` (1–10,000), and `offset`. A date-only end is midnight at the beginning of that date. Local operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `contains`, `is_null`. Values are bound parameters; arbitrary SQL is not exposed through tools.

## Spatial and temporal analysis

`analyze_exposure` selects the nearest environmental point within a configurable radius, then aggregates daily data over the sample UTC date and preceding `window_days - 1` days. It computes mean SST, anomaly against the source climatology, and days overlapping event intervals (inclusive endpoints). One result is attached to each phytoplankton observation; daily rows never multiply species records.

Defaults: seven days, 20 km, 70% coverage. These are starting assumptions, not validated ecological thresholds. Outputs include distance, valid-day counts, anomaly-day counts, coverage, and match status. Out-of-radius records remain visible. Insufficient coverage produces null summaries/event counts, not false zeros. At least one valid day is required even if minimum coverage is zero. Event absence is interpreted within the downloaded catalog; completeness is not independently established.

Climatology matches calendar dates, including February 29, without shifting March dates. Missing climatology produces null anomalies; no implicit interpolation. Duplicate environmental time/coordinate keys are rejected. Spatial proximity alone does not establish coastal representativeness.

`analyze_relationships` reports per-species exploratory Pearson correlations between anomaly and raw counts, with at least three pairs. It does not control seasonality, repeated samples, autocorrelation, or multiple testing, and returns no significance or causal claims. Biodiversity indices and forecasting models are not implemented.

## Add more feeds

```python
marine.register_source('another_sst', 'https://eurosea.marine.ie/his',
    adapter='eurosea', dataset_type='sst', location='51.743N-10.212W')
marine.pipeline_ingest(sources=['another_sst'])
```

EuroSea POST downloads a server-side file; a new point may first need generation through the upstream map. HTML/error responses and malformed CSV are rejected. CSV schemas for SST, climatology, and event intervals are validated separately.

Register ERDDAP links with optional `response_format='csv'` and `default_query` for URLs without a selection. Phytoplankton and particle schemas normalize automatically. For other variables, configure a source's `normalization` block in `sources.json`:

```json
{
  "normalization": {
    "value_field": "temperature",
    "variable": "water_temperature",
    "unit": "degree_Celsius",
    "fields": {"time": "observation_time", "latitude": "lat", "longitude": "lon"}
  }
}
```

Unit overrides label data; they do not convert values. Only set them from verified source information. Other API protocols need an adapter. Add the source name to `pipeline.json` to include it in the default workflow. Existing names cannot be silently replaced.

## Fetch receipts and offline cache

Receipts and downloaded transport files live under `data/fetches/<fetch_id>/`. Fetch tools do not populate snapshot tables or the analytical manifest. Cache tools validate receipt hashes and never fetch missing receipts. Receipts persist for retries/provenance; there is no automatic staging cleanup. Do not delete files referenced by cached source metadata. Raw cache publication and Parquet publication can be split with `cache_store(fetch_id, publish=False)` followed by `pipeline_publish(key)`.

## Storage and cache behavior

- `sources.json`: transport settings, endpoints, defaults, TTL.
- `pipeline.json`: default ingestion plan and selections.
- `data/marine.sqlite`: validated raw snapshots and metadata; legacy tools remain compatible.
- `data/raw/`: original EuroSea and ERDDAP CSV files, named by SHA-256.
- `data/parquet/<source>/`: standardized compressed analytical files.
- `data/analytics.duckdb`: manifest selecting the current Parquet snapshot per source.

SQLite is the ingestion cache; **Parquet and DuckDB are the analysis layer**. Use the manifest-selected Parquet path for notebooks rather than globbing historical versions and duplicating rows. DuckDB can read these files directly with `read_parquet(?)` and the selected path as a bound parameter.

Fresh requests reuse cache; `force=true` refreshes. Offline analysis rejects expired data unless `allow_stale=true`. Fetch time and observation time are separate. Raw refresh is transactional; failed ingestion preserves the prior published analytical snapshot. Partial ingestion reports errors per source and keeps successful datasets available. The CLI exits nonzero on partial failure.

This is a single-user local pipeline. For analytical pipeline ingestion, run one ingestion process at a time and avoid overlapping DuckDB writers from separate MCP/dashboard/HTTP processes; DuckDB uses process-level file locks. HAB ingestion has separate cross-process locks and persistent background backfills. Responses have size limits and are parsed in memory; narrow very large selections. JSON ERDDAP and EuroSea implement transient retries; the grid CSV transport uses a bounded request timeout.

## HAB bulletin ingestion

The bulletin service is discoverable at **`GET /api/hab`**. It returns supported pages, strict week bounds, source URL configuration, REST routes, and MCP tool names without fetching a PDF. The reusable client also exposes `HABClient().service()`.

The existing `serve` command now runs one FastAPI application, preserving the original routes and adding HAB routes. SQLite tables `hab_bulletins`, `hab_bulletin_pages`, `hab_jobs`, and `hab_job_weeks` reuse `--db` (default `data/marine.sqlite`). HAB imagery is separate from observational Parquet datasets and is never automatically joined to observations or assigned forecast validity dates.

Start the API, then open the dashboard in a second terminal and select **HAB bulletins**:

```powershell
.\run.ps1 serve --port 8000
# In another terminal:
.\run.ps1 dashboard
```

The dashboard offers year/week selectors, both pages independently or side by side, cache information, missing-data warnings and the source PDF link. It uses the reusable `marine_tools.hab_client.HABClient` HTTP client. Set `HAB_API_URL` in both the dashboard and MCP environment when using a different API port. Interactive API documentation is available at `http://127.0.0.1:8000/docs`.

| Method | Route | Behavior |
| --- | --- | --- |
| GET | `/api/hab/2026/7` | Read cached metadata and both image URLs; 404 on a miss |
| GET | `/api/hab/2026/7/page/12` | Serve cached physical PDF page 12 as PNG |
| GET | `/api/hab/2026/7/page/13` | Serve cached physical PDF page 13 as PNG |
| GET | `/api/hab/available?year=2026` | List successfully cached bulletins whose files/hashes validate |
| POST | `/api/hab/2026/7/refresh` | Authenticated fresh fetch; explicitly cache its returned fetch_id |
| POST | `/api/hab/backfill` | Authenticated background ingestion; returns 202 and job ID |
| GET | `/api/hab/backfill/{job_id}` | Persistent progress, per-week errors and summary |

Weeks are strictly **2–51 inclusive** everywhere. JSON/Python/MCP reject strings, booleans and floats as weeks; HTTP paths accept integer text, including `02`, but reject `2.0`. Invalid week/page/range requests return 422. Pages are exactly physical indices 11 and 12, rendered separately at 200 DPI with full orientation and extent. There is no OCR. A short, encrypted, malformed or HTML response is a recorded failure. Unavailable PDFs return 404, are negatively cached for six hours, and can be retried after expiry or through authenticated refresh. Transient upstream/network failures use two retries with bounded exponential backoff and are briefly negatively cached after exhaustion.

Read calls check cached files and hashes without network access. Missing/corrupt assets return an error without repair or download. Explicitly re-cache a valid fetch receipt, or fetch a fresh PDF and then cache it. Per-bulletin filesystem locks coordinate API/MCP processes. Downloads are bounded to two concurrently per service by default; MuPDF rendering is serialized because its library is not thread-safe.

Assets live under `data/hab/{year}/week_{week:02d}/{generation}/`, containing one `bulletin.pdf`, `page_12.png`, and `page_13.png` per complete generation. Relative paths in SQLite select the active generation. This extra directory level allows all three assets to be staged and published in one database transaction. Failed staging is removed. Failed refresh records the error and retains the previous valid version. Earlier successful generations remain on disk so an in-flight image response can finish safely; `total_storage_bytes` includes these retained generations, while `active_storage_bytes` counts current assets. Back up the SQLite database and HAB directory together. Do not delete active generations; old unreferenced generations can be removed during maintenance while services are stopped.

### Backfill and restart

To ingest the 50 candidates for 2026 and print a summary:

```powershell
.\run.ps1 hab-backfill --year 2026 --start-week 2 --end-week 51
```

Valid cached weeks are skipped, not downloaded again. Missing weeks do not stop other weeks. The report includes downloaded/cached/missing/failed counts, extracted images, storage bytes and elapsed time. Interruptions retain results in SQLite. Restart the API or MCP server, or use `hab-backfill --job-id JOB_ID` to resume unfinished work. A new backfill also reuses already-published assets; to retry terminal failures/missing results after the negative-cache TTL, submit a new job. A completed job means all candidates were attempted; inspect `failed` and `missing` for availability.

The API and MCP server keep background workers alive. A one-shot `call backfill_hab_bulletins` only enqueues durable work; use the dedicated `hab-backfill` command or a running API/MCP server to execute it. One scheduler owns the queue across local processes; at most four jobs may be queued/running, and each schedules only a bounded chunk. Use one API worker on a local filesystem. There is no distributed scheduler or automatic calendar schedule; the CLI can be invoked by an existing task scheduler for periodic ingestion.

HTTP refresh and backfill are disabled until `HAB_ADMIN_TOKEN` is set, require its bearer token, and share a limit of ten administrative requests per minute per API process. The same protection applies to backfill through `/tools/backfill_hab_bulletins`. Local Python/CLI and stdio MCP are trusted interfaces and use the shared service directly. The API binds to loopback; do not expose it publicly without a properly configured authenticated gateway.

```powershell
# Set before starting the API; use your own secret:
$env:HAB_ADMIN_TOKEN = 'your-local-secret'
.\run.ps1 serve
# In another terminal, using the same secret:
$headers = @{ Authorization = 'Bearer your-local-secret' }
Invoke-RestMethod 'http://127.0.0.1:8000/api/hab/backfill' -Method Post `
  -Headers $headers -ContentType 'application/json' `
  -Body '{"year":2026,"start_week":2,"end_week":51}'
```

MCP tools `get_hab_bulletin_pages`, `get_hab_page`, `list_available_hab_bulletins`, `backfill_hab_bulletins` and `hab_backfill_status` call this same backend. Page references contain an absolute PNG URL and provenance; the API must be running against the same database/cache for the client to retrieve the image. MCP requests enforce the same bounds before network access.

```python
from marine_tools import MarineTools
from marine_tools.hab_client import HABClient

marine = MarineTools()
metadata = marine.get_hab_bulletin_pages(year=2026, week=7)
# UI clients use HTTP rather than rendering PDFs themselves:
client = HABClient()
png_bytes = client.page(2026, 7, 12)
```

### Configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `HAB_CACHE_DIR` | `hab/` beside the selected SQLite DB | Asset storage |
| `HAB_API_URL` | `http://127.0.0.1:8000` | Dashboard client and MCP image URL base |
| `HAB_ADMIN_TOKEN` | unset | Enable/authenticate HTTP refresh and backfill |
| `HAB_CONCURRENCY` | `2` | Concurrent ingestion operations (1–8) |
| `HAB_REQUEST_INTERVAL` | `0.5` | Minimum seconds between upstream attempts per service |
| `HAB_TIMEOUT` | `30` | HTTP timeout in seconds |
| `HAB_RETRIES` | `2` | Additional transient retries (0–5) |
| `HAB_NEGATIVE_TTL` | `21600` | Missing-bulletin retry delay in seconds |
| `HAB_URL_TEMPLATE` | Marine Institute template below | Default source pattern |
| `HAB_YEAR_TEMPLATES` | `{}` | JSON object mapping individual years to source templates |

Default source template: `https://www.marine.ie/sites/default/files/MIFiles/Docs/SeafoodSafety/{year}_Week_{week:02d}_Irish_HAB_Bulletin.pdf`. Only the 2026 example has been verified live. Historical conventions must be checked before backfilling other years; override `HAB_YEAR_TEMPLATES` for years using a different convention. The service never guesses alternative sources or forecast dates. Downloads are capped at 64 MiB.

## Verification

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m pytest -q --basetemp data/pytest-tmp
.\.venv\Scripts\python.exe -m examples.hab_check
.\.venv\Scripts\python.exe -m examples.pipeline_check
```

The original tests cover caching/rollback, CSV validation, Parquet standardization, multiple buoy measurements and their units, leap days, spatial radius, missing coverage, local HTTP calls, and a real MCP stdio handshake/tool call. HAB pytest coverage adds strict validation, correct physical pages, original PDF preservation, invalid/short PDFs, 404 negative caching, transient retries, cache repair, concurrent calls, transactional publication failures, protected HTTP routes, and restartable backfills. Tests mock upstream responses and generate local PDFs. `examples.hab_check` is an explicit live check of both week 07 endpoints and offline cache reuse, saving its report to `data/hab_check.json`. `examples.pipeline_check` saves the analytical live report to `data/pipeline_check.json`.

Live ingestion on October 17, 2026 verified six default feeds: **16,360 SST rows, 366 climatology rows, 98 heatwave events, 85 cold spells, 3,564 phytoplankton records, and 3,276 subsampled particle rows**. Counts describe the configured selections at fetch time.

## References

Data supplied by the Irish Marine Institute. ERDDAP license metadata is retained in snapshots. The EuroSea CSV does not itself specify the climatology baseline; it is recorded as unknown/source-defined.

- [EuroSea interface](https://eurosea.marine.ie/his)
- [Phytoplankton metadata](https://erddap3.marine.ie/erddap/info/habs_phyto/index.html)
- [Particle metadata](https://erddap.marine.ie/erddap/info/BANTRY_PARTICLES/index.html)
- [ERDDAP griddap documentation](https://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html)
- [MCP server guide](https://modelcontextprotocol.io/docs/develop/build-server)
