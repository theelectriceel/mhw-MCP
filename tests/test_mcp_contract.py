"""Exercise every advertised MCP tool without touching upstream services."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from marine_tools import MarineTools, ToolError
from marine_tools.mcp_server import create_server, mcp_lifespan


CALLS = {
    "fetch_data": {"source": "bantry_phyto"},
    "cache_store": {"fetch_id": "receipt"},
    "pipeline_publish": {"key": "snapshot"},
    "fetch_hab_bulletin": {"year": 2026, "week": 7},
    "cache_hab_bulletin": {"fetch_id": "receipt"},
    "list_sources": {},
    "pipeline_status": {},
    "pipeline_ingest": {"sources": ["bantry_phyto"]},
    "pipeline_query": {"source": "bantry_wave_buoy", "limit": 5},
    "analyze_exposure": {"limit": 2, "radius_km": 10},
    "analyze_relationships": {"window_days": 3},
    "register_source": {"name": "example", "url": "https://example.org/erddap/tabledap/a.csv?time,value"},
    "ingest_ocean_colour": {"year": 2026, "month": 3, "day": 31},
    "get_hab_bulletin_pages": {"year": 2026, "week": 7},
    "get_hab_page": {"year": 2026, "week": 7, "page": 12},
    "list_available_hab_bulletins": {"year": 2026},
    "backfill_hab_bulletins": {"year": 2026, "start_week": 2, "end_week": 3},
    "hab_backfill_status": {"job_id": "example-job"},
}


@pytest.mark.parametrize("name,arguments", CALLS.items())
def test_each_tool_dispatches_to_shared_backend(name, arguments):
    method = Mock(return_value={"called": name})
    system = SimpleNamespace(**{name: method})
    result = asyncio.run(create_server(system).call_tool(name, arguments))
    assert not result.is_error
    assert json.loads(result.content[0].text) == {"called": name}
    method.assert_called_once()
    if name.startswith("analyze_"):
        assert "system" not in method.call_args.kwargs
        for key, value in arguments.items():
            assert method.call_args.kwargs[key] == value


def test_all_tools_are_documented_in_catalog():
    tools = asyncio.run(create_server(SimpleNamespace()).list_tools())
    assert {tool.name for tool in tools} == set(CALLS)


def test_mcp_lifecycle_resumes_jobs_and_closes_even_on_failure():
    class Jobs:
        def __init__(self):
            self.started = False
            self.closed = False

        @property
        def jobs(self):
            self.started = True

        def close(self):
            self.closed = True

    service = Jobs()
    async def check():
        with pytest.raises(RuntimeError):
            async with mcp_lifespan(SimpleNamespace(hab=service), None):
                assert service.started
                raise RuntimeError("client disconnected")
    asyncio.run(check())
    assert service.closed


def test_ingestion_plan_is_relative_to_source_registry(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    sources = root / "sources.json"
    sources.write_text(json.dumps({"selected": {}, "should_not_run": {}}))
    (root / "pipeline.json").write_text(json.dumps({"sources": ["selected"]}))
    foreign = tmp_path / "mcp-client"
    foreign.mkdir()
    monkeypatch.chdir(foreign)
    tools = MarineTools(root / "marine.sqlite", sources)
    tools.cache_fetch = Mock(side_effect=ToolError("mocked offline request"))
    result = tools.pipeline_ingest()
    assert [error["source"] for error in result["errors"]] == ["selected"]
    tools.cache_fetch.assert_called_once_with("selected", force=False)
