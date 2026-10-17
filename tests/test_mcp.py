import json
import sys
import tempfile
import unittest
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class MCPTest(unittest.IsolatedAsyncioTestCase):
    async def test_stdio_initialize_list_and_offline_call(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            params = StdioServerParameters(command=sys.executable, args=["-m", "marine_tools", "--db", str(Path(folder)/"marine.sqlite"), "mcp"], cwd=str(Path.cwd()))
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    names = {t.name for t in tools.tools}
                    self.assertIn("pipeline_ingest", names)
                    self.assertIn("analyze_exposure", names)
                    self.assertIn("ingest_ocean_colour", names)
                    self.assertIn("get_hab_bulletin_pages", names)
                    self.assertIn("get_hab_page", names)
                    self.assertIn("backfill_hab_bulletins", names)
                    hab_list = await session.call_tool("list_available_hab_bulletins", {"year": 2026})
                    self.assertFalse(hab_list.is_error)
                    for week in [1, 52, -1, 7.5, "7", True]:
                        invalid = await session.call_tool("get_hab_bulletin_pages", {"year": 2026, "week": week})
                        self.assertTrue(invalid.is_error)
                    result = await session.call_tool("pipeline_status", {})
                    self.assertFalse(result.is_error)
                    text = "".join(c.text for c in result.content if c.type == "text")
                    self.assertEqual(json.loads(text)["datasets"], [])


if __name__ == "__main__":
    unittest.main()
