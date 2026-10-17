"""Physical PDF page rendering, with no interpretation or OCR."""
from pathlib import Path
import hashlib
import threading

import pymupdf

_render_lock = threading.Lock()


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def render_pages(raw: bytes, directory: Path, dpi: int = 200) -> list[dict]:
    # MuPDF is not thread-safe. Downloads may overlap; rendering is serialized.
    with _render_lock:
        return _render_pages(raw, directory, dpi)


def _render_pages(raw: bytes, directory: Path, dpi: int) -> list[dict]:
    if not raw.lstrip().startswith(b"%PDF-"):
        raise ValueError("Source did not contain a PDF header")
    with pymupdf.open(stream=raw, filetype="pdf") as pdf:
        if pdf.needs_pass or pdf.page_count < 13:
            raise ValueError("Bulletin must be unencrypted and contain at least 13 physical pages")
        pages = []
        for number in (12, 13):
            pix = pdf[number - 1].get_pixmap(dpi=dpi, alpha=False)
            png = pix.tobytes("png")
            path = directory / f"page_{number}.png"
            path.write_bytes(png)
            pages.append(dict(page_number=number, image_path=str(path),
                              image_sha256=sha256(png), image_size_bytes=len(png),
                              width=pix.width, height=pix.height))
        return pages
