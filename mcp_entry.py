"""Absolute-path entry point for MCP clients; independent of their working directory."""
from pathlib import Path

from marine_tools import MarineTools
from marine_tools.mcp_server import create_server

if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    create_server(MarineTools(root / "data/marine.sqlite", root / "sources.json")).run(transport="stdio")
