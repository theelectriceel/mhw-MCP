"""Opt-in live fetch, followed by offline cache/read checks in an isolated database."""
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

from marine_tools import MarineTools


def main():
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(dir=root / "data") as folder:
        tools = MarineTools(Path(folder) / "marine.sqlite", root / "sources.json")
        fetched = tools.fetch_data("bantry_wave_buoy", constraints=[
            {"field": "time", "op": ">=", "value": "2024-09-24T00:00:00Z"},
            {"field": "time", "op": "<", "value": "2024-09-25T00:00:00Z"},
        ])
        assert tools.cache_status() == []
        assert tools.pipeline_status()["datasets"] == []
        with patch.object(tools, "fetch_data", side_effect=AssertionError("No fetch during cache/read")), \
             patch.object(tools, "fetcher", side_effect=AssertionError("No network during cache/read")):
            saved = tools.cache_store(fetched["fetch_id"])
            rows = tools.pipeline_query("bantry_wave_buoy")
        assert rows["matched_rows"] == fetched["row_count"] * 4
        result = {"source": fetched["source"], "fetched_observations": fetched["row_count"],
                  "cache_empty_after_fetch": True, "publication": "passed",
                  "cache_and_reads_with_fetch_disabled": "passed", "measurement_rows": rows["matched_rows"],
                  "measurements": [{"variable": r["variable"], "unit": r["unit"]} for r in rows["rows"][:4]],
                  "scope": "Isolated temporary database; existing project selections were unchanged."}
    path = root / "backend/separation-verification.json"
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
