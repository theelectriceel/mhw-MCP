"""Verify standardized live data without printing large tables."""
import json
from collections import Counter
from pathlib import Path

from marine_tools import MarineTools


def main():
    tools = MarineTools()
    exposure = tools.analyze_exposure(limit=10000)
    relationships = tools.analyze_relationships()
    report = {"status": tools.pipeline_status(),
              "observations": len(exposure["rows"]),
              "match_status": dict(Counter(r["match_status"] for r in exposure["rows"])),
              "method": exposure["method"], "relationships": relationships}
    path = Path("data/pipeline_check.json")
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"datasets": len(report["status"]["datasets"]), "observations": report["observations"],
                      "match_status": report["match_status"], "species_with_pairs": len(relationships["rows"]), "report": str(path)}))


if __name__ == "__main__":
    main()
