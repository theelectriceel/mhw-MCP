"""ETL and analytical functions shared by CLI, HTTP, dashboard and MCP."""
import json
import math
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .system import ToolError, identifier

FIELDS = {
    "source": "VARCHAR", "snapshot_key": "VARCHAR", "time": "TIMESTAMPTZ", "time_end": "TIMESTAMPTZ",
    "latitude": "DOUBLE", "longitude": "DOUBLE", "release_location": "DOUBLE", "variable": "VARCHAR", "value": "DOUBLE", "unit": "VARCHAR",
    "sample_id": "VARCHAR", "species": "VARCHAR", "location_name": "VARCHAR", "calendar_day": "VARCHAR",
    "climatology_c": "DOUBLE", "p10_c": "DOUBLE", "p90_c": "DOUBLE", "event_id": "VARCHAR",
    "duration_days": "DOUBLE", "category": "VARCHAR", "raw_json": "VARCHAR",
}


def timestamp(value):
    if value in (None, ""):
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise ToolError(f"Invalid timestamp: {value}") from exc


def number(value):
    if value in (None, "", "NaN", "nan", "NA"):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def normalize(row, config, source, key, schema):
    kind = config.get("dataset_type")
    fields = config.get("normalization", {}).get("fields", {})
    def get(name):
        return row.get(fields.get(name, name))
    result = {name: None for name in FIELDS}
    result.update(source=source, snapshot_key=key, time=timestamp(get("time")), time_end=timestamp(get("time_end")),
                  latitude=number(get("latitude")), longitude=number(get("longitude")),
                  release_location=number(get("release_location")),
                  sample_id=get("sample_id"), location_name=get("location_name"),
                  raw_json=json.dumps(row, allow_nan=False))
    for coord, bound in (("latitude", 90), ("longitude", 180)):
        if result[coord] is not None and abs(result[coord]) > bound:
            raise ToolError(f"Invalid {coord}")
    units = dict(zip(schema["columns"], schema.get("units", [])))
    if kind == "sst":
        result.update(variable="sst", value=number(row["sst_c"]), unit="degree_Celsius")
    elif kind == "clim":
        result.update(variable="climatology", value=number(row["climatology_c"]), unit="degree_Celsius",
                      calendar_day=row["calendar_day"], climatology_c=number(row["climatology_c"]),
                      p10_c=number(row["p10_c"]), p90_c=number(row["p90_c"]))
    elif kind in ("mhw", "cs"):
        result.update(variable="heatwave" if kind == "mhw" else "cold_spell", value=number(row["intensity_c"]),
                      unit="degree_Celsius", event_id=row["event_id"], duration_days=number(row["duration_days"]), category=row["category"])
    elif "scientific_name" in row and "count" in row:
        result.update(variable="phytoplankton_count", value=number(row["count"]), unit=units.get("count") or "unknown",
                      sample_id=str(row.get("samplecode", "")), species=row["scientific_name"], location_name=row.get("location_name"))
    else:
        mapping = config.get("normalization", {})
        value_field = mapping.get("value_field", "particle_hours" if "particle_hours" in row else None)
        if not value_field:
            raise ToolError("Configure normalization.value_field for this source")
        result.update(variable=mapping.get("variable", value_field), value=number(row[value_field]),
                      unit=mapping.get("unit", units.get(value_field) or "unknown"))
    return tuple(result[name] for name in FIELDS)


def normalize_records(row, config, source, key, schema):
    """Expand multi-measurement observations into the shared variable/value schema."""
    mapping = config.get("normalization", {})
    measurements = mapping.get("measurements")
    if measurements is None:
        return [normalize(row, config, source, key, schema)]
    if not measurements:
        raise ToolError("normalization.measurements cannot be empty")
    records = []
    for measurement in measurements:
        if measurement.get("value_field") not in row:
            raise ToolError(f"Missing measurement field: {measurement.get('value_field')}")
        specific = {**config, "normalization": {**mapping, **measurement}}
        records.append(normalize(row, specific, source, key, schema))
    return records


def serial_rows(cursor):
    names = [d[0] for d in cursor.description]
    return [{k: v.isoformat().replace("+00:00", "Z") if isinstance(v, datetime) else
             None if isinstance(v, float) and not math.isfinite(v) else v for k, v in zip(names, row)} for row in cursor.fetchall()]


class Pipeline:
    def __init__(self, tools):
        self.tools = tools
        self.root = tools.db.parent.resolve()
        self.db = self.root / "analytics.duckdb"

    @contextmanager
    def connect(self):
        try:
            import duckdb
        except ImportError as exc:
            raise ToolError("Install project requirements to use the analytical pipeline") from exc
        con = duckdb.connect(str(self.db))
        try:
            con.execute("SET TimeZone='UTC'")
            con.execute("""CREATE TABLE IF NOT EXISTS datasets (
                source VARCHAR PRIMARY KEY, snapshot_key VARCHAR, path VARCHAR,
                row_count BIGINT, ingested DOUBLE, fetched DOUBLE, expires DOUBLE)""")
            yield con
        finally:
            con.close()

    def status(self):
        with self.connect() as con:
            rows = serial_rows(con.execute("SELECT *, expires < epoch(current_timestamp) AS stale FROM datasets ORDER BY source"))
        return {"datasets": rows, "database": str(self.db), "schema_version": 1,
                "fields": FIELDS, "time_standard": "UTC", "coordinate_standard": "degrees north/east",
                "notes": "Unknown source units remain 'unknown'. EuroSea coordinates are requested points, not independently verified grid centers."}

    def ingest(self, sources=None, force=False, selections=None, cached_keys=None):
        plan = self.tools.sources.resolve().parent / "pipeline.json"
        defaults = json.loads(plan.read_text()) if plan.exists() else {}
        sources = sources or defaults.get("sources", list(self.tools.list_sources()))
        selections = {**defaults.get("selections", {}), **(selections or {})}
        results, errors = [], []
        for source in sources:
            try:
                identifier(source)
                snapshot = {"key": cached_keys[source]} if cached_keys is not None else self.tools.cache_fetch(source, force=force, **selections.get(source, {}))
                key = snapshot["key"]
                meta, schema = self.tools._snapshot(key, allow_stale=cached_keys is not None)
                with self.connect() as con:
                    existing = con.execute("SELECT * FROM datasets WHERE source=? AND snapshot_key=? AND fetched=?", [source, key, meta["fetched"]]).fetchone()
                    if existing and Path(existing[2]).exists() and "release_location" in [d[0] for d in con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [existing[2]]).description]:
                        results.append({"source": source, "key": key, "row_count": existing[3], "reused": True})
                        continue
                    con.execute("CREATE TEMP TABLE normalized (" + ",".join(identifier(k) + " " + v for k, v in FIELDS.items()) + ")")
                    config = self.tools.source(source)
                    con.execute("BEGIN TRANSACTION")
                    with self.tools.connect() as raw:
                        cursor = raw.execute(f'SELECT * FROM "data_{key}"')
                        while batch := cursor.fetchmany(5000):
                            normalized = [record for row in batch for record in normalize_records(dict(row), config, source, key, schema)]
                            if normalized:
                                import pyarrow as pa
                                con.register("normalization_batch", pa.Table.from_pylist([dict(zip(FIELDS, record)) for record in normalized]))
                                try:
                                    con.execute("INSERT INTO normalized SELECT * FROM normalization_batch")
                                finally:
                                    con.unregister("normalization_batch")
                    con.execute("COMMIT")
                    folder = self.root / "parquet" / source
                    folder.mkdir(parents=True, exist_ok=True)
                    path = folder / f"{key}-{int(meta['fetched'] * 1000000)}.parquet"
                    temp = path.with_suffix(".tmp.parquet")
                    literal = str(temp).replace("'", "''")
                    con.execute(f"COPY normalized TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                    temp.replace(path)
                    count = con.execute("SELECT count(*) FROM normalized").fetchone()[0]
                    con.execute("INSERT OR REPLACE INTO datasets VALUES (?,?,?,?,?,?,?)", [source, key, str(path), count, time.time(), meta["fetched"], meta["expires"]])
                    results.append({"source": source, "key": key, "row_count": count, "parquet": str(path), "reused": False})
            except Exception as exc:
                errors.append({"source": source, "error": str(exc)})
        return {"status": "partial_failure" if errors else "ok", "datasets": results, "errors": errors}

    def dataset(self, con, source, allow_stale=False):
        row = con.execute("SELECT path, expires FROM datasets WHERE source=?", [source]).fetchone()
        if not row:
            raise ToolError(f"Ingest {source} first")
        if row[1] <= time.time() and not allow_stale:
            raise ToolError(f"{source} expired; ingest again or use allow_stale=true")
        return "read_parquet('" + row[0].replace("'", "''") + "')"

    def query(self, source, start=None, end=None, filters=None, limit=100, offset=0, allow_stale=False):
        if type(limit) is not int or not 1 <= limit <= 10000 or type(offset) is not int or offset < 0:
            raise ToolError("limit must be 1..10000; offset must be nonnegative")
        filters = list(filters or [])
        if start:
            filters.append({"field": "time", "op": ">=", "value": timestamp(start).isoformat()})
        if end:
            filters.append({"field": "time", "op": "<=", "value": timestamp(end).isoformat()})
        where, params = self.tools._where(filters, {"columns": list(FIELDS)})
        with self.connect() as con:
            relation = self.dataset(con, source, allow_stale)
            total = con.execute("SELECT count(*) FROM " + relation + where, params).fetchone()[0]
            rows = serial_rows(con.execute("SELECT * EXCLUDE(raw_json) FROM " + relation + where + " ORDER BY time, sample_id, species, event_id, variable LIMIT ? OFFSET ?", params + [limit, offset]))
            provenance = serial_rows(con.execute("SELECT * FROM datasets WHERE source=?", [source]))[0]
        return {"source": source, "rows": rows, "matched_rows": total, "offset": offset, "limit": limit,
                "provenance": provenance, "stale": provenance["expires"] <= time.time()}

    def environment(self, con, sst_source, clim_source, mhw_source, cs_source, allow_stale):
        relations = [self.dataset(con, s, allow_stale) for s in (sst_source, clim_source, mhw_source, cs_source)]
        sst, clim, mhw, cs = relations
        for relation, columns in ((sst, "time,latitude,longitude"), (clim, "calendar_day,latitude,longitude")):
            if con.execute(f"SELECT 1 FROM {relation} GROUP BY {columns} HAVING count(*)>1 LIMIT 1").fetchone():
                raise ToolError("Environmental coordinates/time must be unique; duplicate records would multiply matches")
        # Calendar join handles leap day by its own calendar key, never shifts March.
        con.execute(f"""CREATE OR REPLACE TEMP VIEW environment AS
            SELECT s.time, s.latitude, s.longitude, s.value AS sst_c, c.value AS climatology_c,
                   s.value-c.value AS anomaly_c, c.p10_c, c.p90_c,
                   EXISTS(SELECT 1 FROM {mhw} h WHERE s.time BETWEEN h.time AND h.time_end
                          AND s.latitude=h.latitude AND s.longitude=h.longitude) AS heatwave,
                   EXISTS(SELECT 1 FROM {cs} h WHERE s.time BETWEEN h.time AND h.time_end
                          AND s.latitude=h.latitude AND s.longitude=h.longitude) AS cold_spell
            FROM {sst} s LEFT JOIN {clim} c ON strftime(s.time, '%m-%d')=c.calendar_day
                 AND s.latitude=c.latitude AND s.longitude=c.longitude""")

    def exposure(self, phyto_source="bantry_phyto", sst_source="eurosea_sst", clim_source="eurosea_clim",
                 mhw_source="eurosea_mhw", cs_source="eurosea_cs", window_days=7, radius_km=20,
                 min_coverage=0.7, start=None, end=None, limit=1000, allow_stale=False):
        if type(window_days) is not int or not 1 <= window_days <= 366:
            raise ToolError("window_days must be 1..366")
        if not 0 < radius_km <= 1000 or not 0 <= min_coverage <= 1 or not 1 <= limit <= 10000:
            raise ToolError("Invalid radius, coverage, or limit")
        with self.connect() as con:
            self.environment(con, sst_source, clim_source, mhw_source, cs_source, allow_stale)
            phyto = self.dataset(con, phyto_source, allow_stale)
            # Collapse daily environmental series before joining species rows.
            # A lateral aggregate always returns one result per observation, including unmatched rows.
            sql = f"""WITH points AS (SELECT DISTINCT latitude,longitude FROM environment),
                observations AS (SELECT * FROM {phyto} WHERE variable='phytoplankton_count'
                    AND (?::TIMESTAMPTZ IS NULL OR time>=?::TIMESTAMPTZ)
                    AND (?::TIMESTAMPTZ IS NULL OR time<=?::TIMESTAMPTZ))
                SELECT p.sample_id,p.species,p.time,p.latitude,p.longitude,p.value AS phyto_count,p.unit,
                       n.distance_km,x.valid_days,x.anomaly_days,x.sst_mean_c,x.anomaly_mean_c,
                       CASE WHEN x.valid_days >= ? THEN x.heatwave_days END AS heatwave_days,
                       CASE WHEN x.valid_days >= ? THEN x.cold_spell_days END AS cold_spell_days,
                       x.valid_days::DOUBLE / ? AS coverage,
                       CASE WHEN n.distance_km IS NULL THEN 'outside_radius_or_missing_coordinates'
                            WHEN x.valid_days < ? THEN 'insufficient_coverage' ELSE 'matched' END AS match_status
                FROM observations p
                LEFT JOIN LATERAL (
                    SELECT latitude,longitude, 6371*2*asin(sqrt(least(1.0,
                      pow(sin(radians(points.latitude-p.latitude)/2),2)+cos(radians(p.latitude))*cos(radians(points.latitude))*
                      pow(sin(radians(points.longitude-p.longitude)/2),2)))) AS distance_km
                    FROM points WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                    AND distance_km<=? ORDER BY distance_km,latitude,longitude LIMIT 1
                ) n ON true
                LEFT JOIN LATERAL (
                    SELECT count(sst_c) AS valid_days,count(anomaly_c) AS anomaly_days,
                           CASE WHEN count(sst_c)>=? THEN avg(sst_c) END AS sst_mean_c,
                           CASE WHEN count(anomaly_c)>=? THEN avg(anomaly_c) END AS anomaly_mean_c,
                           count(*) FILTER (WHERE heatwave AND sst_c IS NOT NULL) AS heatwave_days,
                           count(*) FILTER (WHERE cold_spell AND sst_c IS NOT NULL) AS cold_spell_days
                    FROM environment e WHERE e.latitude=n.latitude AND e.longitude=n.longitude
                      AND e.time::DATE BETWEEN p.time::DATE-?::INTEGER AND p.time::DATE
                ) x ON true ORDER BY p.time,p.sample_id,p.species LIMIT ?"""
            required = max(1, math.ceil(window_days * min_coverage))
            params = [start, start, end, end, required, required, window_days, required,
                      radius_km, required, required, window_days - 1, limit + 1]
            rows = serial_rows(con.execute(sql, params))
            provenance = serial_rows(con.execute("SELECT * FROM datasets WHERE source IN (?,?,?,?,?)", [phyto_source, sst_source, clim_source, mhw_source, cs_source]))
        return {"rows": rows[:limit], "truncated": len(rows) > limit, "provenance": provenance,
                "method": {"window_days": window_days, "window": "sample UTC date and preceding window_days-1 dates",
                           "radius_km": radius_km, "spatial": "nearest requested environmental point within radius",
                           "min_coverage": min_coverage, "event_end": "inclusive", "leap_day": "match 02-29 only; otherwise anomaly null"},
                "limitations": "Spatial proximity does not establish coastal representativeness. Missing coverage is not evidence of no event. Association is not causation."}

    def relationships(self, **options):
        result = self.exposure(**{**options, "limit": 10000})
        if result["truncated"]:
            raise ToolError("More than 10000 observations; narrow dates before relationship analysis")
        groups = {}
        for row in result["rows"]:
            if row["phyto_count"] is not None and row["anomaly_mean_c"] is not None and row["match_status"] == "matched":
                groups.setdefault(row["species"], []).append((row["anomaly_mean_c"], row["phyto_count"]))
        rows = []
        for species, pairs in groups.items():
            xs, ys = zip(*pairs)
            mx, my = sum(xs)/len(xs), sum(ys)/len(ys)
            variance = sum((x-mx)**2 for x in xs)*sum((y-my)**2 for y in ys)
            corr = sum((x-mx)*(y-my) for x,y in pairs)/math.sqrt(variance) if len(pairs)>=3 and variance>0 else None
            rows.append({"species": species, "paired_observations": len(pairs), "pearson_r": corr})
        return {"rows": rows, "method": result["method"], "provenance": result["provenance"],
                "limitations": "Exploratory Pearson correlation on raw counts, minimum 3 pairs. Seasonality, repeated samples, spatial dependence and multiple testing are not controlled; no p-values or causal claims."}
