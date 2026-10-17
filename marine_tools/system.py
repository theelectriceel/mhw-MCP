from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, urlopen


class ToolError(ValueError):
    pass


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ToolError(f"Invalid field name: {value!r}")
    return '"' + value + '"'


def parse_source(url):
    p = urlsplit(url.replace("\\&", "&"))
    match = re.fullmatch(r"(.*/erddap)/(tabledap|griddap)/([^/.]+)\.[A-Za-z0-9]+", p.path)
    if p.scheme != "https" or not p.hostname or p.username or not match:
        raise ToolError("Expected an HTTPS ERDDAP tabledap or griddap data URL")
    base, kind, dataset = match.groups()
    query = unquote(p.query)
    # A copied Markdown label may omit quotes around the regex.
    query = re.sub(r'(=~)(?!")([^&]+)', lambda m: m[1] + json.dumps(m[2]), query)
    return {"base": f"https://{p.netloc}{base}", "kind": kind,
            "dataset": dataset, "query": query, "url": url}


def request_json(url, timeout=60, max_bytes=128 * 1024 * 1024, retries=2):
    for attempt in range(retries + 1):
        try:
            with urlopen(Request(url, headers={"User-Agent": "MarineTools/1.0",
                                                 "Accept": "application/json"}), timeout=timeout) as response:
                raw = response.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ToolError("Response exceeds size limit; narrow time/bounds or increase grid stride")
            return json.loads(raw)
        except HTTPError as exc:
            detail = exc.read(4096).decode("utf-8", errors="replace")
            if exc.code == 404 and "no matching results" in detail.lower():
                raise ToolError("No matching results; narrow or correct the selection") from exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt == retries:
                raise ToolError(f"ERDDAP HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            if attempt == retries:
                raise ToolError(f"Cannot reach ERDDAP: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ToolError("ERDDAP returned invalid JSON") from exc
        time.sleep(2 ** attempt)


class MarineTools:
    """Fetch, cache publication, and reads have separate public operations.

    Each distinct request is an immutable snapshot. Refresh replaces that snapshot
    atomically, never appending duplicate rows. TTL never silently permits stale data.
    """

    def __init__(self, db="data/marine.sqlite", sources="sources.json", fetcher=None):
        self.db = Path(db)
        self.db.parent.mkdir(parents=True, exist_ok=True)
        self.sources = Path(sources)
        self.fetcher = fetcher or request_json
        self._hab_service = None
        self._hab_lock = threading.Lock()
        with self.connect() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS snapshots (
                key TEXT PRIMARY KEY, source TEXT NOT NULL, url TEXT NOT NULL,
                fetched REAL NOT NULL, expires REAL NOT NULL, row_count INTEGER NOT NULL,
                schema_json TEXT NOT NULL, metadata_json TEXT NOT NULL)""")

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.db, timeout=30)
        con.row_factory = sqlite3.Row
        try:
            with con:
                yield con
        finally:
            con.close()

    @property
    def hab(self):
        with self._hab_lock:
            if self._hab_service is None:
                import os
                from .hab import HABConfig, HABService
                self._hab_service = HABService(self.db, HABConfig(root=Path(os.getenv("HAB_CACHE_DIR", str(self.db.parent / "hab")))))
            return self._hab_service

    def get_hab_bulletin_pages(self, year=2026, week=2):
        return self.hab.cached(year, week)

    def fetch_hab_bulletin(self, year=2026, week=2):
        return self.hab.fetch_bulletin(year, week)

    def cache_hab_bulletin(self, fetch_id):
        return self.hab.cache_bulletin(fetch_id)

    def get_hab_page(self, year, week, page):
        _, item, metadata = self.hab.page(year, week, page)
        import os
        base = os.getenv("HAB_API_URL", "http://127.0.0.1:8000").rstrip("/")
        return {"year": year, "week": week, "page": page, "content_type": "image/png",
                "url": base + metadata["pages"][str(page)], "image_sha256": item["image_sha256"],
                "source_url": metadata["source_url"], "pdf_sha256": metadata["pdf_sha256"]}

    def list_available_hab_bulletins(self, year=None):
        return {"bulletins": self.hab.available(year)}

    def ingest_ocean_colour(self, year=2026, month=3, day=31, latitude=51.675, longitude=-9.625, force=False):
        from .ocean_colour import request_for_date, settings
        request = request_for_date(year, month, day)
        if type(force) is not bool:
            raise ToolError("force must be a boolean")
        settings({}, request, latitude, longitude)
        return self.pipeline_ingest(sources=["ocean_colour"], force=force, selections={
            "ocean_colour": {"cds_request": request, "latitude": latitude, "longitude": longitude}})

    def backfill_hab_bulletins(self, year=2026, start_week=2, end_week=51):
        from .hab import BackfillRequest
        BackfillRequest(year=year, start_week=start_week, end_week=end_week)
        return self.hab.jobs.submit(year, start_week, end_week)

    def hab_backfill_status(self, job_id):
        return self.hab.jobs.status(job_id)

    def list_sources(self):
        return json.loads(self.sources.read_text(encoding="utf-8"))

    def register_source(self, name, url, ttl_seconds=86400, adapter="erddap", dataset_type=None, location=None,
                        default_query=None, response_format="json"):
        identifier(name)
        if adapter == "erddap":
            parse_source(url)
        elif adapter == "eurosea":
            from .eurosea import form
            if url != "https://eurosea.marine.ie/his":
                raise ToolError("EuroSea adapter expects https://eurosea.marine.ie/his")
            form({"dataset_type": dataset_type, "location": location or ""})
        else:
            raise ToolError("Unknown adapter")
        if ttl_seconds <= 0:
            raise ToolError("ttl_seconds must be positive")
        sources = self.list_sources() if self.sources.exists() else {}
        if name in sources:
            raise ToolError(f"Source {name} already exists; edit sources.json to change it")
        sources[name] = {"url": url, "ttl_seconds": ttl_seconds}
        if adapter == "eurosea":
            sources[name].update(adapter=adapter, dataset_type=dataset_type, location=location)
        else:
            if response_format not in ("json", "csv"):
                raise ToolError("response_format must be json or csv")
            sources[name]["response_format"] = response_format
            if default_query:
                sources[name]["default_query"] = default_query
        self.sources.parent.mkdir(parents=True, exist_ok=True)
        temp = self.sources.with_suffix(".tmp")
        temp.write_text(json.dumps(sources, indent=2) + "\n", encoding="utf-8")
        temp.replace(self.sources)
        return sources[name]

    def source(self, name):
        try:
            config = self.list_sources()[name]
        except KeyError as exc:
            raise ToolError(f"Unknown source: {name}") from exc
        if config.get("adapter") in ("eurosea", "cds"):
            return config
        parsed = parse_source(config["url"])
        if not parsed["query"] and config.get("default_query"):
            parsed["query"] = config["default_query"]
        return {**parsed, **config}

    def metadata(self, source):
        config = self.source(source)
        if config.get("adapter") == "cds":
            from .ocean_colour import TITLE, URL, settings
            request, lat, lon = settings(config)
            return {"title": TITLE, "source_url": URL, "dataset": "satellite-ocean-colour",
                    "request": request, "latitude": lat, "longitude": lon,
                    "unit": "mg m-3", "data_role": "satellite-derived surface chlorophyll-a concentration"}
        if config.get("adapter") == "eurosea":
            from .eurosea import form
            return {**config, "method": "POST", "form": form(config), "coordinate_role": "requested_point"}
        return self.fetcher(f"{config['base']}/info/{config['dataset']}/index.json")

    def build_url(self, source, columns=None, constraints=None, slices=None, query=None,
                  cds_request=None, latitude=None, longitude=None):
        config = self.source(source)
        if config.get("adapter") == "cds":
            from .ocean_colour import selection_url
            if any(v is not None for v in (columns, constraints, slices, query)):
                raise ToolError("CDS uses cds_request, latitude and longitude selections")
            return selection_url(config, cds_request=cds_request, latitude=latitude, longitude=longitude)
        if any(v is not None for v in (cds_request, latitude, longitude)):
            raise ToolError("cds_request/latitude/longitude selections require a CDS source")
        if config.get("adapter") == "eurosea":
            from .eurosea import form
            from urllib.parse import urlencode
            if any(v is not None for v in (columns, constraints, slices, query)):
                raise ToolError("EuroSea downloads the registered point; filter locally after ingestion")
            return config["url"] + "?" + urlencode(form(config))
        if query is not None and any(x is not None for x in (columns, constraints, slices)):
            raise ToolError("Use raw query or structured options, not both")
        if query is None:
            query = config["query"]
            if config["kind"] == "tabledap":
                if slices is not None:
                    raise ToolError("slices are for griddap")
                parts = query.split("&")
                if columns is not None:
                    if not columns:
                        raise ToolError("Provide at least one column")
                    for col in columns:
                        identifier(col)
                    parts[0] = ",".join(columns)
                for item in constraints or []:
                    field, op, value = item["field"], item["op"], item["value"]
                    identifier(field)
                    if op not in ("=", "!=", ">", ">=", "<", "<=", "=~"):
                        raise ToolError("Unsupported ERDDAP operator")
                    literal = json.dumps(value, ensure_ascii=False, allow_nan=False)
                    parts.append(field + op + literal)
                query = "&".join(parts)
            else:
                if columns is not None or constraints is not None:
                    raise ToolError("For griddap use slices, in dimension order, or a raw query")
                if slices is not None:
                    # Match bracket groups without splitting timestamp colons.
                    variables = query.split(",")
                    built = []
                    for variable in variables:
                        name = variable.split("[")[0]
                        identifier(name)
                        original = re.findall(r"\[[^\]]+\]", variable)
                        if len(slices) != len(original):
                            raise ToolError(f"Expected {len(original)} dimension slices")
                        groups = []
                        for s in slices:
                            stride = s.get("stride", 1)
                            if type(stride) is not int or stride < 1:
                                raise ToolError("Grid stride must be a positive integer")
                            start, stop = str(s["start"]), str(s["stop"])
                            if any(c in start + stop for c in "[]()&\r\n"):
                                raise ToolError("Invalid grid coordinate")
                            groups.append(f"[({start}):{stride}:({stop})]")
                        built.append(name + "".join(groups))
                    query = ",".join(built)
        if not query:
            raise ToolError("Provide a data selection")
        extension = config.get("response_format", "json")
        if extension not in ("json", "csv"):
            raise ToolError("response_format must be json or csv")
        return f"{config['base']}/{config['kind']}/{config['dataset']}.{extension}?" + quote(query, safe=",[]():")

    def cache_fetch(self, source, force=False, **selection):
        """Legacy explicit ingest orchestrator; prefer fetch_data then cache_store.

        Retained for existing batch ingestion code, never called by read tools.
        """
        url = self.build_url(source, **selection)
        key = hashlib.sha256((source + "\n" + url).encode()).hexdigest()
        with self.connect() as con:
            hit = con.execute("SELECT * FROM snapshots WHERE key=?", (key,)).fetchone()
        if hit and hit["expires"] > time.time() and not force:
            return {**dict(hit), "cache_hit": True}
        receipt = self.fetch_data(source, **selection)
        return self.cache_store(receipt["fetch_id"], publish=False)

    def fetch_data(self, source, columns=None, constraints=None, slices=None, query=None,
                   cds_request=None, latitude=None, longitude=None):
        """Always contact upstream; stage a receipt, without populating any dataset cache."""
        from uuid import uuid4
        from .staging import stage
        selection = {k: v for k, v in {"columns": columns, "constraints": constraints,
                     "slices": slices, "query": query, "cds_request": cds_request,
                     "latitude": latitude, "longitude": longitude}.items() if v is not None}
        url = self.build_url(source, **selection)
        fetch_id = uuid4().hex
        raw_root = self.db.parent / "fetches" / fetch_id / "raw"
        config = self.source(source)
        if config.get("adapter") == "cds":
            from .ocean_colour import download
            payload, metadata = download(config, raw_root, force=True, **selection)
        elif config.get("adapter") == "eurosea":
            from .eurosea import download
            payload, metadata = download(config, raw_root)
        else:
            metadata = self.metadata(source)
            if config.get("response_format") == "csv":
                from .erddap_csv import download_csv
                payload = download_csv(url, metadata, raw_root)
            else:
                payload = self.fetcher(url)
        self._validate_payload(payload)
        fetched = time.time()
        stage(self.db.parent / "fetches", "table", {"source": source, "url": url, "payload": payload,
              "metadata": metadata, "fetched": fetched}, fetch_id)
        table = payload["table"]
        return {"fetch_id": fetch_id, "source": source, "url": url, "fetched": fetched,
                "row_count": len(table["rows"]), "cached": False,
                "next_step": "cache_store", "columns": table["columnNames"]}

    @staticmethod
    def _validate_payload(payload):
        try:
            table = payload["table"]
            names, types, rows = table["columnNames"], table["columnTypes"], table["rows"]
        except (KeyError, TypeError) as exc:
            raise ToolError("Invalid ERDDAP table response") from exc
        if not names or len(names) != len(types) or len(set(names)) != len(names):
            raise ToolError("Invalid ERDDAP columns")
        for name in names:
            identifier(name)
        if any(len(row) != len(names) for row in rows):
            raise ToolError("ERDDAP row width mismatch")
        return table

    def cache_store(self, fetch_id, publish=True):
        """Publish an already fetched response locally; never contacts an upstream service."""
        from .staging import load
        if type(publish) is not bool:
            raise ToolError("publish must be a boolean")
        receipt = load(self.db.parent / "fetches", fetch_id, "table")
        result = self._store_payload(**receipt)
        if publish:
            result["publication"] = self.pipeline_publish(result["key"])
        return result

    def pipeline_publish(self, key):
        """Normalize and publish a cached raw snapshot without fetching."""
        from .pipeline import Pipeline
        snapshot, _ = self._snapshot(key, allow_stale=True)
        result = Pipeline(self).ingest(sources=[snapshot["source"]], cached_keys={snapshot["source"]: key})
        if result["errors"]:
            raise ToolError("Local publication failed: " + result["errors"][0]["error"])
        return result

    def _store_payload(self, source, url, payload, metadata, fetched):
        table = self._validate_payload(payload)
        names, types, rows = table["columnNames"], table["columnTypes"], table["rows"]
        key = hashlib.sha256((source + "\n" + url).encode()).hexdigest()
        # Metadata provides fill values; JSON often already represents them as null.
        missing = {}
        for row in metadata.get("table", {}).get("rows", []):
            if row[0] == "attribute" and row[2] in ("_FillValue", "missing_value"):
                try:
                    missing.setdefault(row[1], set()).add(float(row[4]))
                except (TypeError, ValueError):
                    pass
        def clean(i, v):
            if isinstance(v, (int, float)) and (not math.isfinite(v) or v in missing.get(names[i], ())):
                return None
            return v
        sql_types = ["TEXT" if t.lower() in ("string", "char") else "REAL" if t.lower() in ("float", "double") else "INTEGER" for t in types]
        table_name = '"data_' + key + '"'
        now = fetched
        ttl = self.source(source).get("ttl_seconds", 86400)
        schema = {"columns": names, "types": types, "units": table.get("columnUnits", [])}
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute("SELECT * FROM snapshots WHERE key=?", (key,)).fetchone()
            if existing and existing["fetched"] >= fetched:
                return {**dict(existing), "cache_hit": True}
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} (" + ",".join(identifier(n) + " " + t for n, t in zip(names, sql_types)) + ")")
            con.executemany(f"INSERT INTO {table_name} VALUES (" + ",".join("?" for _ in names) + ")",
                            ([clean(i, v) for i, v in enumerate(row)] for row in rows))
            con.execute("INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                        (key, source, url, now, now + ttl, len(rows), json.dumps(schema), json.dumps(metadata)))
        return {"key": key, "source": source, "url": url, "row_count": len(rows),
                "fetched": now, "expires": now + ttl, "cache_hit": False, "schema": schema}

    def cache_status(self):
        with self.connect() as con:
            return [{**dict(r), "stale": r["expires"] <= time.time()} for r in con.execute(
                "SELECT key,source,url,fetched,expires,row_count,schema_json FROM snapshots ORDER BY fetched DESC")]

    def cache_remove(self, key):
        self._snapshot(key, True)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute(f'DROP TABLE "data_{key}"')
            con.execute("DELETE FROM snapshots WHERE key=?", (key,))
        return {"removed": key}

    def _snapshot(self, key, allow_stale=False):
        if not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ToolError("Invalid snapshot key")
        with self.connect() as con:
            row = con.execute("SELECT * FROM snapshots WHERE key=?", (key,)).fetchone()
        if row is None:
            raise ToolError("Snapshot not found; call cache_fetch first")
        if row["expires"] <= time.time() and not allow_stale:
            raise ToolError("Snapshot expired; refresh it or explicitly set allow_stale=true")
        return dict(row), json.loads(row["schema_json"])

    def _where(self, filters, schema):
        clauses, params = [], []
        for f in filters or []:
            field, op, value = f["field"], f["op"], f.get("value")
            if field not in schema["columns"]:
                raise ToolError(f"Unknown field: {field}")
            col = identifier(field)
            if op not in ("=", "!=", ">", ">=", "<", "<=", "in", "contains", "is_null"):
                raise ToolError("Unsupported local filter operator")
            if op == "is_null":
                clauses.append(f"{col} IS NULL")
            elif op == "in":
                if not isinstance(value, list) or not value:
                    raise ToolError("in requires a nonempty list")
                clauses.append(f"{col} IN (" + ",".join("?" for _ in value) + ")")
                params.extend(value)
            elif op == "contains":
                clauses.append(f"instr(lower({col}), lower(?)) > 0")
                params.append(str(value))
            elif value is None:
                if op not in ("=", "!="):
                    raise ToolError("Use = or != for null")
                clauses.append(f"{col} IS {'NOT ' if op == '!=' else ''}NULL")
            else:
                clauses.append(f"{col} {op} ?")
                params.append(value)
        return (" WHERE " + " AND ".join(clauses) if clauses else ""), params

    def infer_query(self, key, columns=None, filters=None, order_by=None, descending=False,
                    limit=100, offset=0, allow_stale=False):
        snapshot, schema = self._snapshot(key, allow_stale)
        if type(limit) is not int or not 1 <= limit <= 10000 or type(offset) is not int or offset < 0:
            raise ToolError("limit must be 1..10000 and offset must be nonnegative")
        columns = columns or schema["columns"]
        for field in columns + ([order_by] if order_by else []):
            if field not in schema["columns"]:
                raise ToolError(f"Unknown field: {field}")
        where, params = self._where(filters, schema)
        sql = "SELECT " + ",".join(identifier(c) for c in columns) + f' FROM "data_{key}"' + where
        if order_by:
            sql += " ORDER BY " + identifier(order_by) + (" DESC" if descending else " ASC")
        with self.connect() as con:
            total = con.execute(f'SELECT COUNT(*) FROM "data_{key}"' + where, params).fetchone()[0]
            rows = [dict(r) for r in con.execute(sql + " LIMIT ? OFFSET ?", params + [limit, offset])]
        return {"key": key, "source": snapshot["source"], "fetched": snapshot["fetched"],
                "stale": snapshot["expires"] <= time.time(), "matched_rows": total,
                "rows": rows, "limit": limit, "offset": offset, "schema": schema}

    def infer_summary(self, key, value, group_by=None, filters=None, limit=100, allow_stale=False):
        snapshot, schema = self._snapshot(key, allow_stale)
        groups = group_by or []
        for field in groups + [value]:
            if field not in schema["columns"]:
                raise ToolError(f"Unknown field: {field}")
        if schema["types"][schema["columns"].index(value)].lower() in ("string", "char"):
            raise ToolError("Summary value must be numeric")
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ToolError("limit must be 1..10000")
        reserved = {"row_count", "valid_count", "minimum", "maximum", "mean", "total"}
        if reserved.intersection(groups):
            raise ToolError("Group field conflicts with a summary output name")
        v = identifier(value)
        select = [identifier(g) for g in groups] + ["COUNT(*) AS row_count", f"COUNT({v}) AS valid_count",
                  f"MIN({v}) AS minimum", f"MAX({v}) AS maximum", f"AVG({v}) AS mean", f"SUM({v}) AS total"]
        where, params = self._where(filters, schema)
        sql = "SELECT " + ",".join(select) + f' FROM "data_{key}"' + where
        if groups:
            sql += " GROUP BY " + ",".join(identifier(g) for g in groups) + " ORDER BY row_count DESC," + ",".join(identifier(g) for g in groups)
        with self.connect() as con:
            rows = [dict(r) for r in con.execute(sql + " LIMIT ?", params + [limit + 1])]
        return {"key": key, "source": snapshot["source"], "value": value,
                "stale": snapshot["expires"] <= time.time(), "fetched": snapshot["fetched"],
                "rows": rows[:limit], "truncated": len(rows) > limit,
                "scope": "Only this cached selection; no predictive model or causal inference."}

    def pipeline_ingest(self, sources=None, force=False, selections=None):
        from .pipeline import Pipeline
        return Pipeline(self).ingest(sources, force, selections)

    def pipeline_status(self):
        from .pipeline import Pipeline
        return Pipeline(self).status()

    def pipeline_query(self, source, start=None, end=None, filters=None, limit=100, offset=0, allow_stale=False):
        from .pipeline import Pipeline
        return Pipeline(self).query(source, start, end, filters, limit, offset, allow_stale)

    def analyze_exposure(self, **options):
        from .pipeline import Pipeline
        return Pipeline(self).exposure(**options)

    def analyze_relationships(self, **options):
        from .pipeline import Pipeline
        return Pipeline(self).relationships(**options)
