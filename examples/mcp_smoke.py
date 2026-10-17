"""Read-only checks of the configured MCP server and cached data; no bulk downloads."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile

from fastapi.testclient import TestClient
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from marine_tools import MarineTools
from marine_tools.api import create_app
from marine_tools.mcp_server import create_server

ROOT = Path(__file__).resolve().parents[1]


def body(result):
    if result.is_error:
        raise RuntimeError("MCP tool failed: " + str(result.content))
    return json.loads(next(item.text for item in result.content if item.type == "text"))


async def smoke():
    report = {"checked_at_utc": datetime.now(timezone.utc).isoformat(), "problems": []}
    with tempfile.TemporaryDirectory(dir=ROOT / "data") as folder:
        params = StdioServerParameters(command=sys.executable, args=[str(ROOT / "mcp_entry.py")], cwd=folder)
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                listing = await session.list_tools()
                report["server"] = initialized.server_info.name
                report["tool_names"] = sorted(tool.name for tool in listing.tools)
                sources = body(await session.call_tool("list_sources", {}))
                report["registered_sources"] = sorted(sources)
                status = body(await session.call_tool("pipeline_status", {}))
                report["cached_datasets"] = []
                for dataset in status["datasets"]:
                    result = body(await session.call_tool("pipeline_query", {
                        "source": dataset["source"], "limit": 2, "allow_stale": True}))
                    report["cached_datasets"].append({"source": dataset["source"], "rows": result["matched_rows"],
                                                      "returned_rows": len(result["rows"]), "stale": result["stale"],
                                                      "null_values_in_sample": sum(r["value"] is None for r in result["rows"])})
                for tool in ("analyze_exposure", "analyze_relationships"):
                    options = {"allow_stale": True}
                    if tool == "analyze_exposure":
                        options["limit"] = 2
                    result = body(await session.call_tool(tool, options))
                    report[tool] = {"returned_rows": len(result["rows"]), "limitations": result["limitations"]}
                available = body(await session.call_tool("list_available_hab_bulletins", {"year": 2026}))
                cached_week7 = any(b["week"] == 7 for b in available["bulletins"])
                report["hab_week7_cached"] = cached_week7
                if cached_week7:
                    report["hab_page_urls"] = [body(await session.call_tool("get_hab_page", {
                        "year": 2026, "week": 7, "page": page}))["url"] for page in (12, 13)]
                report["foreign_working_directory"] = "passed"
    system = MarineTools(ROOT / "data/marine.sqlite", ROOT / "sources.json")
    tools = await create_server(system).list_tools()
    target = ROOT / "backend/mcp-tools.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps([t.model_dump(by_alias=True, exclude_none=True) for t in tools], indent=2), encoding="utf-8")
    with TestClient(create_app(system)) as client:
        report["http"] = {}
        paths = ["/health", "/api/hab", "/docs", "/openapi.json"]
        if report["hab_week7_cached"]:
            paths += [f"/api/hab/2026/7/page/{page}" for page in (12, 13)]
        for path in paths:
            response = client.get(path)
            response.raise_for_status()
            report["http"][path] = {"status": response.status_code, "content_type": response.headers.get("content-type")}
    path = ROOT / "backend/verification.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"tools": len(report["tool_names"]), "datasets": len(report["cached_datasets"]),
                      "foreign_working_directory": report["foreign_working_directory"], "report": str(path)}))


if __name__ == "__main__":
    asyncio.run(smoke())
