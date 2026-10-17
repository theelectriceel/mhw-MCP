import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import unquote
from urllib.request import Request, urlopen

from marine_tools import MarineTools, ToolError
from marine_tools.api import TOOLS, catalog, make_server


class ToolsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.config = root / "sources.json"
        self.config.write_text(Path("sources.json").read_text(), encoding="utf-8")
        self.calls = []
        self.payload = {"table": {"columnNames": ["scientific_name", "count", "time"],
                                 "columnTypes": ["String", "double", "String"],
                                 "columnUnits": ["", "cells/L", "UTC"],
                                 "rows": [["A", 10, "2026-10-01T00:00:00Z"],
                                          ["A", -999, "2026-10-02T00:00:00Z"],
                                          ["B", 30, "2026-10-03T00:00:00Z"]]}}
        self.tools = MarineTools(root / "cache.sqlite", self.config, self.fetch)

    def fetch(self, url):
        self.calls.append(url)
        if "/info/" in url:
            return {"table": {"rows": [["attribute", "count", "_FillValue", "double", "-999"]]}}
        return copy.deepcopy(self.payload)

    def test_cache_hit_force_and_offline_summary(self):
        first = self.tools.cache_fetch("bantry_phyto")
        self.assertEqual(first["row_count"], 3)
        self.assertTrue(self.tools.cache_fetch("bantry_phyto")["cache_hit"])
        self.assertEqual(len(self.calls), 2)
        key = first["key"]
        result = self.tools.infer_summary(key, "count", ["scientific_name"])
        self.assertEqual(result["rows"][0]["valid_count"], 1)
        self.assertEqual(result["rows"][0]["mean"], 10)
        self.assertEqual(len(self.calls), 2)
        self.tools.cache_fetch("bantry_phyto", force=True)
        self.assertEqual(self.tools.cache_status()[0]["row_count"], 3)
        self.assertEqual(len(self.tools.cache_status()), 1)

    def test_filters_pagination_null_and_injection(self):
        key = self.tools.cache_fetch("bantry_phyto")["key"]
        result = self.tools.infer_query(key, filters=[{"field": "count", "op": ">", "value": 5}], order_by="count", descending=True, limit=1)
        self.assertEqual(result["matched_rows"], 2)
        self.assertEqual(result["rows"][0]["count"], 30)
        result = self.tools.infer_query(key, filters=[{"field": "scientific_name", "op": "=", "value": "' OR 1=1 --"}])
        self.assertEqual(result["matched_rows"], 0)
        self.assertEqual(self.tools.infer_query(key, filters=[{"field": "count", "op": "is_null"}])["matched_rows"], 1)
        with self.assertRaises(ToolError):
            self.tools.infer_query(key, columns=["x; DROP TABLE snapshots"])
        with self.assertRaises(ToolError):
            self.tools.infer_summary(key, "scientific_name")

    def test_expiry_requires_explicit_opt_in(self):
        key = self.tools.cache_fetch("bantry_phyto")["key"]
        with self.tools.connect() as con:
            con.execute("UPDATE snapshots SET expires=0")
        with self.assertRaises(ToolError):
            self.tools.infer_query(key)
        self.assertTrue(self.tools.infer_query(key, allow_stale=True)["stale"])
        self.assertTrue(self.tools.cache_status()[0]["stale"])

    def test_failed_refresh_keeps_previous_snapshot(self):
        key = self.tools.cache_fetch("bantry_phyto")["key"]
        self.payload["table"]["rows"] = [["A", {"bad": "type"}, "today"]]
        with self.assertRaises(Exception):
            self.tools.cache_fetch("bantry_phyto", force=True)
        self.assertEqual(self.tools.infer_query(key)["matched_rows"], 3)

    def test_url_building_and_new_source(self):
        url = unquote(self.tools.build_url("bantry_phyto", constraints=[{"field": "time", "op": ">=", "value": "2026-10-01T00:00:00Z"}]))
        self.assertIn('parent_area_name=~"(?i).*bantry.*"', url)
        self.assertIn('&time>="2026-10-01T00:00:00Z"', url)
        self.assertIn("habs_phyto.json?", url)
        args = json.loads(Path("examples/cache_particles.json").read_text())
        grid = unquote(self.tools.build_url(**args))
        self.assertIn("[(51.140912310003):20:(51.899102770785255)]", grid)
        with self.assertRaises(ToolError):
            self.tools.build_url("bantry_particles", slices=[{"start": 0, "stop": 1}])
        self.tools.register_source("new_source", "https://example.org/erddap/tabledap/new.htmlTable?time,x")
        self.assertIn("/new.json?time,x", self.tools.build_url("new_source"))
        self.assertEqual(len(catalog(self.tools)), len(TOOLS))

    def test_remove(self):
        key = self.tools.cache_fetch("bantry_phyto")["key"]
        self.tools.cache_remove(key)
        self.assertEqual(self.tools.cache_status(), [])
        with self.assertRaises(ToolError):
            self.tools.infer_query(key)

    def test_http_api(self):
        server = make_server(self.tools, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_port}"
        with urlopen(base + "/tools") as response:
            self.assertEqual(len(json.load(response)), len(TOOLS))
        body = json.dumps({"source": "bantry_phyto"}).encode()
        with urlopen(Request(base + "/tools/fetch_data", data=body, headers={"Content-Type": "application/json"})) as response:
            fetched = json.load(response)
        with urlopen(Request(base + "/tools/cache_store", data=json.dumps({"fetch_id": fetched["fetch_id"], "publish": False}).encode(), headers={"Content-Type": "application/json"})) as response:
            key = json.load(response)["key"]
        with urlopen(Request(base + "/tools/infer_summary", data=json.dumps({"key": key, "value": "count"}).encode(), headers={"Content-Type": "application/json"})) as response:
            self.assertEqual(json.load(response)["rows"][0]["mean"], 20)
        with self.assertRaises(HTTPError) as failure:
            urlopen(Request(base + "/tools/infer_query", data=b'{"key":"invalid"}', headers={"Content-Type": "application/json"}))
        self.assertEqual(failure.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
