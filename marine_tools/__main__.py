import argparse
import json
import sys

from .api import call_tool, catalog, make_server
from .system import MarineTools, ToolError


def main():
    parser = argparse.ArgumentParser(description="ERDDAP caching and offline analysis")
    parser.add_argument("--db", default="data/marine.sqlite")
    parser.add_argument("--sources", default="sources.json")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("tools")
    sub.add_parser("ingest", help="Run the configured standard pipeline")
    sub.add_parser("status", help="Show standardized dataset inventory")
    sub.add_parser("mcp", help="Run the MCP interface over stdio")
    sub.add_parser("dashboard", help="Launch the Streamlit dashboard")
    backfill = sub.add_parser("hab-backfill", help="Run/resume a durable HAB backfill and print its summary")
    backfill.add_argument("--year", type=int, default=2026)
    backfill.add_argument("--start-week", type=int, default=2)
    backfill.add_argument("--end-week", type=int, default=51)
    backfill.add_argument("--job-id", help="Wait for an existing, possibly interrupted job")
    serve = sub.add_parser("serve")
    serve.add_argument("--port", type=int, default=8000)
    call = sub.add_parser("call")
    call.add_argument("name")
    call.add_argument("arguments", nargs="?", default="{}", help="JSON object, or @path/to/request.json")
    args = parser.parse_args()
    system = MarineTools(args.db, args.sources)
    try:
        if args.command == "hab-backfill":
            import time
            from .hab import BackfillRequest
            BackfillRequest(year=args.year, start_week=args.start_week, end_week=args.end_week)
            job_id = args.job_id or system.backfill_hab_bulletins(args.year, args.start_week, args.end_week)["job_id"]
            print(json.dumps({"job_id": job_id}), flush=True)
            try:
                while True:
                    result = system.hab_backfill_status(job_id)
                    if result["status"] == "completed":
                        print(json.dumps(result, indent=2))
                        if result["failed"]:
                            raise SystemExit(1)
                        return
                    time.sleep(0.5)
            finally:
                system.hab.close()
        if args.command == "mcp":
            from .mcp_server import create_server
            create_server(system).run(transport="stdio")
            return
        if args.command == "dashboard":
            import subprocess
            from pathlib import Path
            app = Path(__file__).resolve().parents[1] / "app.py"
            raise SystemExit(subprocess.call([sys.executable, "-m", "streamlit", "run", str(app),
                                             "--server.address", "127.0.0.1", "--", "--db", args.db, "--sources", args.sources]))
        if args.command == "ingest":
            result = system.pipeline_ingest()
            print(json.dumps(result, indent=2))
            if result["errors"]:
                raise SystemExit(1)
            return
        if args.command == "status":
            print(json.dumps(system.pipeline_status(), indent=2))
            return
        if args.command == "serve":
            server = make_server(system, args.port)
            print(f"Marine API listening at http://127.0.0.1:{server.server_port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return
        if args.command == "tools":
            result = catalog(system)
        else:
            from pathlib import Path
            raw = Path(args.arguments[1:]).read_text(encoding="utf-8") if args.arguments.startswith("@") else args.arguments
            result = call_tool(system, args.name, json.loads(raw))
        print(json.dumps(result, indent=2, allow_nan=False))
    except (ToolError, ValueError, OSError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
