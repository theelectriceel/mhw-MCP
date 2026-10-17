"""Transport receipts, separate from the application cache and analytical manifest."""
import hashlib
import json
from pathlib import Path
import re
import time
from uuid import uuid4

from .system import ToolError


def stage(root, kind, data, fetch_id=None):
    fetch_id = fetch_id or uuid4().hex
    folder = Path(root) / fetch_id
    folder.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps({"kind": kind, "data": data}, allow_nan=False).encode()
    temp = folder / "receipt.tmp"
    temp.write_bytes(encoded)
    temp.replace(folder / "receipt.json")
    (folder / "receipt.sha256").write_text(hashlib.sha256(encoded).hexdigest(), encoding="ascii")
    return fetch_id


def load(root, fetch_id, kind):
    if not isinstance(fetch_id, str) or not re.fullmatch(r"[0-9a-f]{32}", fetch_id):
        raise ToolError("Invalid fetch_id")
    folder = (Path(root) / fetch_id).resolve()
    if not folder.is_relative_to(Path(root).resolve()):
        raise ToolError("Fetch receipt is outside staging")
    try:
        raw = (folder / "receipt.json").read_bytes()
        expected = (folder / "receipt.sha256").read_text(encoding="ascii")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ToolError("Fetched response failed its integrity check; fetch again")
        receipt = json.loads(raw)
    except ToolError:
        raise
    except (OSError, ValueError) as exc:
        raise ToolError("Fetched response not found or invalid; call the matching fetch tool first") from exc
    if receipt["kind"] != kind:
        raise ToolError("Fetch receipt belongs to a different data type")
    return receipt["data"]
