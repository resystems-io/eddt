#!/usr/bin/env python3
"""Mermaid-support smoke tests for markdown-pdf.py.

Exercises render_mermaid() and the wider pipeline against the
showcase document in this directory. Checks:

* R01: every ```mermaid fence is replaced with an SVG-bearing
  <div class="mermaid-diagram"> wrapper in the intermediate HTML.
* R05 / R19: no ```mermaid source leaks into a codehilite
  syntax-highlighted block (i.e. we don't accidentally render the
  source as code).
* R09: second invocation on the same source returns the cached
  SVG (file mtime unchanged).
* R07 --no-mermaid: when disabled, mermaid fences fall through to
  codehilite as plain code blocks.

Run from the repo root or this directory:

    python3 scripts/tests/test_mermaid.py

Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
SHOWCASE = HERE / "mermaid-showcase.md"
MD_PDF = SCRIPTS / "markdown-pdf.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("markdown_pdf", MD_PDF)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"ok   {msg}")


def test_render_mermaid_replaces_fences() -> None:
    """render_mermaid(md) replaces every ```mermaid fence with a
    <div class="mermaid-diagram"> block containing an <img> tag
    bearing a data: URI."""
    mod = _load_module()
    src = (
        "Before.\n\n"
        "```mermaid\n"
        "graph TD\n  A --> B\n"
        "```\n\n"
        "Between.\n\n"
        "```mermaid\n"
        "sequenceDiagram\n  A->>B: hi\n"
        "```\n\n"
        "After.\n"
    )
    out = mod.render_mermaid(src)
    if "```mermaid" in out:
        _fail("render_mermaid left a ```mermaid fence in the output")
    if out.count('<div class="mermaid-diagram">') != 2:
        _fail(
            "expected 2 mermaid-diagram wrappers; "
            f"got {out.count('<div class=\"mermaid-diagram\">')}"
        )
    if out.count('src="data:image/png;base64,') < 2:
        _fail("expected 2 inlined base64 PNG data URIs")
    _ok("render_mermaid replaces fences with PNG-bearing divs")


def test_cache_hit_on_second_render() -> None:
    """A second render of the same source returns the cached PNG
    without re-invoking mmdc (file mtime unchanged).

    Note: snap-confined mmdc can't see ``/tmp``; we place the
    per-test tempdir under the scripts directory so it's visible
    to the renderer. The ``tempfile.TemporaryDirectory(dir=...)``
    form keeps cleanup automatic.
    """
    mod = _load_module()
    with tempfile.TemporaryDirectory(dir=str(SCRIPTS)) as tmp:
        cache = Path(tmp)
        src = "```mermaid\ngraph TD\n  X --> Y\n```\n"
        mod.render_mermaid(src, cache_dir=cache)
        pngs = list(cache.glob("*.png"))
        if len(pngs) != 1:
            _fail(f"expected 1 cached PNG, got {len(pngs)}")
        first_mtime = pngs[0].stat().st_mtime_ns
        # Second render — should reuse the cache.
        mod.render_mermaid(src, cache_dir=cache)
        second_mtime = pngs[0].stat().st_mtime_ns
        if first_mtime != second_mtime:
            _fail("cache miss on second render (mtime changed)")
    _ok("cache is consumed on repeat render of identical source")


def test_showcase_builds() -> None:
    """The showcase document builds without error and produces a
    PDF, and the intermediate HTML carries the mermaid-diagram
    wrappers (not codehilite-highlighted code)."""
    if not SHOWCASE.exists():
        _fail(f"showcase not found at {SHOWCASE}")
    out_pdf = SHOWCASE.with_suffix(".pdf")
    # Fresh build — remove the output so we can assert it was
    # actually produced by this invocation.
    if out_pdf.exists():
        out_pdf.unlink()
    result = subprocess.run(
        [sys.executable, str(MD_PDF), str(SHOWCASE)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        _fail(
            "markdown-pdf.py failed on showcase:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    if not out_pdf.exists():
        _fail(f"expected PDF at {out_pdf}, not found")
    _ok(f"showcase built to {out_pdf.name}")


def test_showcase_intermediate_html() -> None:
    """Run the pipeline in-process against the showcase source and
    assert:

    * at least 11 mermaid-diagram wrappers in the body HTML;
    * no fenced 'graph TD' / 'sequenceDiagram' marker appears
      inside a codehilite block.
    """
    mod = _load_module()
    md_text = SHOWCASE.read_text()
    md_text = mod.render_mermaid(md_text)
    body_html, _, _refs = mod.md_to_html(md_text, math=True)

    count = body_html.count('<div class="mermaid-diagram">')
    if count < 11:
        _fail(
            f"expected ≥11 mermaid-diagram wrappers in the HTML, got {count}"
        )

    # R19: no mermaid source should appear inside a codehilite block.
    # We grep crudely: find every <div class="codehilite"> region and
    # make sure none contains a canonical Mermaid marker.
    import re
    codehilite_regions = re.findall(
        r'<div class="codehilite">.*?</div>\s*</div>',
        body_html, flags=re.DOTALL,
    )
    leaked = [
        region for region in codehilite_regions
        if any(marker in region for marker in (
            "graph TD", "sequenceDiagram", "stateDiagram",
            "classDiagram", "erDiagram", "gantt", "gitGraph",
            "pie ", "mindmap", "timeline", "sankey-beta",
        ))
    ]
    if leaked:
        _fail(
            f"{len(leaked)} mermaid source block(s) leaked into codehilite "
            "(R05/R19 violation)"
        )
    _ok("R05/R19: no mermaid source leaked into codehilite")
    _ok(f"R01: {count} mermaid-diagram wrappers present in HTML")


def test_no_mermaid_flag_falls_through() -> None:
    """With --no-mermaid, mermaid fences should render as plain
    code blocks (codehilite)."""
    # Build in a sibling directory to avoid clobbering the real
    # showcase PDF. Use SCRIPTS rather than /tmp so snap-confined
    # binaries (unused on this path, but symmetry keeps test
    # environments consistent) can reach it.
    with tempfile.TemporaryDirectory(dir=str(SCRIPTS)) as tmp:
        out_pdf = Path(tmp) / "showcase-nomermaid.pdf"
        result = subprocess.run(
            [
                sys.executable, str(MD_PDF),
                str(SHOWCASE),
                "--no-mermaid",
                "--output", str(out_pdf),
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            _fail(
                "build with --no-mermaid failed:\n"
                f"stderr:\n{result.stderr}"
            )
        if not out_pdf.exists():
            _fail("--no-mermaid run produced no PDF")
    _ok("--no-mermaid builds successfully (fall-through path)")


def main() -> int:
    test_render_mermaid_replaces_fences()
    test_cache_hit_on_second_render()
    test_showcase_intermediate_html()
    test_showcase_builds()
    test_no_mermaid_flag_falls_through()
    print("\nall tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
