"""Small real requests, followed by offline analysis. Run from project root."""
import json
from pathlib import Path

from marine_tools import MarineTools


def main():
    tools = MarineTools()
    requests = [json.loads(Path(f"examples/cache_{name}.json").read_text()) for name in ("phyto", "particles")]
    results = []
    for args in requests:
        snapshot = tools.cache_fetch(**args)
        value = "count" if args["source"] == "bantry_phyto" else "particle_hours"
        groups = ["scientific_name"] if value == "count" else ["release_location"]
        results.append({"snapshot": snapshot,
                        "summary": tools.infer_summary(snapshot["key"], value, group_by=groups)})
    Path("data/live_check.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    for result in results:
        s = result["snapshot"]
        print(f"{s['source']}: {s['row_count']} rows; snapshot {s['key']}")


if __name__ == "__main__":
    main()
