"""Opt-in live CDS retrieval for the user's March 31, 2026 request."""
import json
from pathlib import Path

from marine_tools import MarineTools


def main():
    tools = MarineTools()
    result = tools.ingest_ocean_colour(year=2026, month=3, day=31)
    if not result["errors"]:
        result["sample"] = tools.pipeline_query("ocean_colour")
    target = Path("data/ocean_colour_check.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
