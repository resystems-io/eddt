#!/usr/bin/env python3
"""Unit tests for markdown-table.py.

Exercises both the ``check`` and ``format`` paths of the merged
script against synthesised markdown fixtures.

Run from the repo root or this directory:

    python3 docs/scripts/tests/test_markdown_table.py

Exits 0 on success, non-zero on any failed assertion.

The internal helper functions are exercised directly via
``importlib`` (the script's filename contains a hyphen, which
the regular ``import`` statement cannot load); the CLI surface
is exercised via subprocess.
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
MD_TABLE = SCRIPTS / "markdown-table.py"


# Load the hyphenated script as a module so we can call its
# helpers directly.
_spec = importlib.util.spec_from_file_location(
    "markdown_table", str(MD_TABLE))
mt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mt)


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"ok   {msg}")


def _write(content: str) -> str:
    f = tempfile.NamedTemporaryFile(
        mode='w', suffix='.md', delete=False)
    f.write(content)
    f.close()
    return f.name


def _run(cmd: str, *args: str) -> subprocess.CompletedProcess:
    """Invoke ``markdown-table.py {cmd} {args}``."""
    return subprocess.run(
        ["python3", str(MD_TABLE), cmd, *args],
        capture_output=True,
        text=True,
    )


def _pipe_positions(lines: list[str]) -> list[list[int]]:
    result = []
    for line in lines:
        line = line.rstrip('\n')
        result.append(
            [i for i, c in enumerate(line) if c == '|'
             and (i == 0 or line[i - 1] != '\\')])
    return result


# ----------------------------------------------------------------
# find_pipe_positions
# ----------------------------------------------------------------

def test_pipe_positions_simple() -> None:
    if mt.find_pipe_positions("| a | b | c |") != [0, 4, 8, 12]:
        _fail("find_pipe_positions: simple row")


def test_pipe_positions_escaped() -> None:
    if mt.find_pipe_positions(r"| a\|b | c |") != [0, 7, 11]:
        _fail("find_pipe_positions: escaped pipe not skipped")


def test_pipe_positions_empty() -> None:
    if mt.find_pipe_positions("no pipes here") != []:
        _fail("find_pipe_positions: no pipes")


def test_pipe_positions_backticked_pipe() -> None:
    # Pipe inside an inline code span is literal content.
    actual = mt.find_pipe_positions("| a | `x|y` | b |")
    if actual != [0, 4, 12, 16]:
        _fail(f"find_pipe_positions: backticked pipe; got {actual}")


def test_pipe_positions_multiple_code_spans() -> None:
    actual = mt.find_pipe_positions("| `a|b` | `c|d` |")
    if actual != [0, 8, 16]:
        _fail(
            f"find_pipe_positions: two spans; got {actual}")


def test_pipe_positions_escaped_backtick_not_span() -> None:
    actual = mt.find_pipe_positions(r"| a\` | b|c |")
    if actual != [0, 6, 9, 12]:
        _fail(
            f"find_pipe_positions: escaped backtick should "
            f"not open a span; got {actual}")


# ----------------------------------------------------------------
# split_cells
# ----------------------------------------------------------------

def test_split_cells_simple() -> None:
    if mt.split_cells("| a | b | c |") != ["a", "b", "c"]:
        _fail("split_cells: simple row")


def test_split_cells_escaped_pipe() -> None:
    if mt.split_cells(r"| a\|b | c |") != [r"a\|b", "c"]:
        _fail("split_cells: escaped pipe leaks as delimiter")


def test_split_cells_whitespace_stripped() -> None:
    if mt.split_cells("|  foo  |  bar  |") != ["foo", "bar"]:
        _fail("split_cells: surrounding whitespace not stripped")


def test_split_cells_pipe_inside_backticks() -> None:
    if mt.split_cells(
            "| a | `x|y` | c |") != ["a", "`x|y`", "c"]:
        _fail("split_cells: pipe inside code span split as cell")


def test_split_cells_pipe_inside_backticks_with_adjacent_text(
) -> None:
    if mt.split_cells(
            "| key | `snap|delta` partition |") != [
                "key", "`snap|delta` partition"]:
        _fail("split_cells: code span with trailing text")


def test_split_cells_multiple_code_spans_per_cell() -> None:
    actual = mt.split_cells(
        "| a | `x|y` and `p|q` | b |")
    if actual != ["a", "`x|y` and `p|q`", "b"]:
        _fail(
            f"split_cells: two code spans in one cell; "
            f"got {actual}")


def test_split_cells_escaped_backtick_does_not_open_span(
) -> None:
    actual = mt.split_cells(r"| a\` | b|c | d |")
    if actual != [r"a\`", "b", "c", "d"]:
        _fail(
            f"split_cells: escaped backtick should not open "
            f"a span; got {actual}")


# ----------------------------------------------------------------
# check_file
# ----------------------------------------------------------------

def test_aligned_table_passes() -> None:
    path = _write(
        "| Name  | Age |\n"
        "|:------|:----|\n"
        "| Alice | 30  |\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(f"check: aligned table flagged: {errors}")
    finally:
        os.unlink(path)


def test_misaligned_table_fails() -> None:
    path = _write(
        "| Name | Age |\n"
        "|:---|:---|\n"
        "| Alice | 30 |\n"
    )
    try:
        errors = mt.check_file(path)
        if len(errors) != 1 or "misaligned" not in errors[0]:
            _fail(f"check: misaligned not flagged: {errors}")
    finally:
        os.unlink(path)


def test_multiple_tables_reports_only_bad() -> None:
    path = _write(
        "# Good table\n"
        "\n"
        "| A | B |\n"
        "|:--|:--|\n"
        "| 1 | 2 |\n"
        "\n"
        "# Bad table\n"
        "\n"
        "| Long Header | Short |\n"
        "|:---|:---|\n"
        "| x           | y     |\n"
    )
    try:
        errors = mt.check_file(path)
        if len(errors) != 1:
            _fail(
                f"check: expected exactly 1 error across two "
                f"tables; got {errors}")
    finally:
        os.unlink(path)


def test_escaped_pipe_in_content() -> None:
    path = _write(
        r"| Format       | Example    |" + "\n"
        r"|:-------------|:-----------|" + "\n"
        r"| json\|text   | either one |" + "\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(f"check: escaped pipe flagged: {errors}")
    finally:
        os.unlink(path)


def test_backticked_pipe_in_content() -> None:
    path = _write(
        "| Key | Value                |\n"
        "|:----|:---------------------|\n"
        "| nat | `kind=<snap|delta>`  |\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(
                f"check: backticked pipe flagged: {errors}")
    finally:
        os.unlink(path)


def test_fenced_code_block_skipped() -> None:
    path = _write(
        "```\n"
        "| not | a | table |\n"
        "|:----|:--|:------|\n"
        "| a | b | c |\n"
        "```\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(
                f"check: fenced code block parsed as table: "
                f"{errors}")
    finally:
        os.unlink(path)


def test_empty_file() -> None:
    path = _write("")
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(f"check: empty file flagged: {errors}")
    finally:
        os.unlink(path)


def test_header_and_separator_only() -> None:
    path = _write(
        "| Col A | Col B |\n"
        "|:------|:------|\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(
                f"check: header-only table flagged: {errors}")
    finally:
        os.unlink(path)


def test_separator_alignment_markers() -> None:
    path = _write(
        "| Left | Centre | Right |\n"
        "|:-----|:------:|------:|\n"
        "| a    | b      | c     |\n"
    )
    try:
        errors = mt.check_file(path)
        if errors != []:
            _fail(
                f"check: aligned tri-alignment flagged: "
                f"{errors}")
    finally:
        os.unlink(path)


# ----------------------------------------------------------------
# format_table
# ----------------------------------------------------------------

def test_pads_short_cells() -> None:
    lines = [
        "| Name | Age |\n",
        "|:---|:---|\n",
        "| Alice | 30 |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: pads_short_cells reported unsafe")
    pipes = _pipe_positions(result)
    if not all(p == pipes[0] for p in pipes):
        _fail(
            f"format_table: pipes not aligned after pad: "
            f"{pipes}")


def test_extends_separator_dashes() -> None:
    lines = [
        "| Long Header | Short |\n",
        "|:---|:---|\n",
        "| x           | y     |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: extend_separator reported unsafe")
    pipes = _pipe_positions(result)
    if not all(p == pipes[0] for p in pipes):
        _fail(
            f"format_table: pipes not aligned after extend: "
            f"{pipes}")


def test_preserves_left_align() -> None:
    lines = [
        "| Col |\n",
        "|:----|\n",
        "| val |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: left-align reported unsafe")
    sep = result[1].strip()
    if not sep.startswith('|:') or ':|' in sep:
        _fail(f"format_table: left-align not preserved: {sep}")


def test_preserves_right_align() -> None:
    lines = [
        "| Col |\n",
        "|----:|\n",
        "| val |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: right-align reported unsafe")
    sep = result[1].strip()
    if not sep.endswith(':|') or ':--' in sep:
        _fail(
            f"format_table: right-align not preserved: {sep}")


def test_preserves_centre_align() -> None:
    lines = [
        "| Col |\n",
        "|:---:|\n",
        "| val |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: centre-align reported unsafe")
    inner = result[1].strip().strip('|')
    if not (inner.startswith(':') and inner.endswith(':')):
        _fail(
            f"format_table: centre-align not preserved: "
            f"{inner}")


def test_preserves_no_align() -> None:
    lines = [
        "| Col |\n",
        "|-----|\n",
        "| val |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: no-align reported unsafe")
    inner = result[1].strip().strip('|')
    if inner.startswith(':') or inner.endswith(':'):
        _fail(
            f"format_table: no-align grew a colon: {inner}")


def test_escaped_pipe_untouched() -> None:
    lines = [
        r"| Format     | Example |" + "\n",
        r"|:-----------|:--------|" + "\n",
        r"| json\|text | either  |" + "\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: escaped pipe reported unsafe")
    if r"json\|text" not in result[2]:
        _fail(
            f"format_table: escaped pipe lost after format: "
            f"{result[2]}")


def test_code_span_pipe_preserved_through_format() -> None:
    lines = [
        "| key   | value                |\n",
        "|:------|:---------------------|\n",
        "| nat   | `kind=<snap|delta>`  |\n",
    ]
    result, ok = mt.format_table(lines)
    if not ok:
        _fail("format_table: code-span row reported unsafe")
    data_cells = mt.split_cells(result[2].strip())
    if len(data_cells) != 2 \
            or data_cells[1] != "`kind=<snap|delta>`":
        _fail(
            f"format_table: code-span content not preserved: "
            f"{data_cells}")


def test_content_verification_skips_unsafe() -> None:
    # Inconsistent column counts → unsafe.
    lines = [
        "| A | B | C |\n",
        "|:--|:--|:--|\n",
        "| 1 | 2 |\n",
    ]
    result, ok = mt.format_table(lines)
    if ok:
        _fail("format_table: unsafe row reported safe")
    if result != lines:
        _fail("format_table: unsafe row mutated input")


# ----------------------------------------------------------------
# format_file
# ----------------------------------------------------------------

def test_fenced_code_block_untouched_by_format() -> None:
    content = (
        "# Title\n"
        "\n"
        "```\n"
        "| not | a | table |\n"
        "|:----|:--|:------|\n"
        "| a | b | c |\n"
        "```\n"
        "\n"
        "Done.\n"
    )
    path = _write(content)
    try:
        changed, formatted, skipped = mt.format_file(path)
        if changed or formatted or skipped:
            _fail(
                f"format_file: fenced block touched: "
                f"changed={changed} formatted={formatted} "
                f"skipped={skipped}")
    finally:
        os.unlink(path)


def test_already_aligned_unchanged() -> None:
    content = (
        "| Name  | Age |\n"
        "|:------|:----|\n"
        "| Alice | 30  |\n"
    )
    path = _write(content)
    try:
        changed, _, _ = mt.format_file(path)
        if changed:
            _fail(
                "format_file: aligned table rewritten "
                "(should be no-op)")
    finally:
        os.unlink(path)


def test_multiple_tables_only_bad_reformatted() -> None:
    content = (
        "# Good\n"
        "\n"
        "| A | B |\n"
        "|:--|:--|\n"
        "| 1 | 2 |\n"
        "\n"
        "# Bad\n"
        "\n"
        "| Long Header | Short |\n"
        "|:---|:---|\n"
        "| x           | y     |\n"
    )
    path = _write(content)
    try:
        changed, formatted, _ = mt.format_file(path)
        if not changed or formatted < 1:
            _fail(
                "format_file: bad table not reformatted")
        with open(path) as f:
            result = f.read()
        if "| A | B |" not in result:
            _fail(
                "format_file: good table altered "
                "unnecessarily")
    finally:
        os.unlink(path)


# ----------------------------------------------------------------
# CLI entry-point smoke tests
# ----------------------------------------------------------------

def test_cli_check_ok() -> None:
    path = _write(
        "| Name  | Age |\n"
        "|:------|:----|\n"
        "| Alice | 30  |\n"
    )
    try:
        res = _run("check", path)
        if res.returncode != 0:
            _fail(
                f"cli check: aligned table exited "
                f"{res.returncode}: {res.stderr!r}")
    finally:
        os.unlink(path)


def test_cli_check_fails_on_misaligned() -> None:
    path = _write(
        "| Name | Age |\n"
        "|:---|:---|\n"
        "| Alice | 30 |\n"
    )
    try:
        res = _run("check", path)
        if res.returncode != 1:
            _fail(
                f"cli check: misaligned exit code "
                f"{res.returncode}, expected 1")
    finally:
        os.unlink(path)


def test_cli_format_then_check() -> None:
    """End-to-end: format makes check pass."""
    path = _write(
        "| Name | Age |\n"
        "|:---|:---|\n"
        "| Alice | 30 |\n"
    )
    try:
        fmt = _run("format", path)
        if fmt.returncode != 0:
            _fail(
                f"cli format: exited {fmt.returncode}: "
                f"{fmt.stderr!r}")
        chk = _run("check", path)
        if chk.returncode != 0:
            _fail(
                f"cli check after format: exited "
                f"{chk.returncode}: {chk.stdout!r}")
    finally:
        os.unlink(path)


def test_cli_usage_error_without_subcommand() -> None:
    res = subprocess.run(
        ["python3", str(MD_TABLE)],
        capture_output=True,
        text=True,
    )
    # argparse exits 2 on usage error.
    if res.returncode != 2:
        _fail(
            f"cli: missing subcommand should exit 2; got "
            f"{res.returncode}")


# ----------------------------------------------------------------
# Driver
# ----------------------------------------------------------------

def main() -> None:
    tests = [
        # find_pipe_positions
        ("find_pipe_positions: simple",
         test_pipe_positions_simple),
        ("find_pipe_positions: escaped pipe",
         test_pipe_positions_escaped),
        ("find_pipe_positions: empty line",
         test_pipe_positions_empty),
        ("find_pipe_positions: pipe inside code span",
         test_pipe_positions_backticked_pipe),
        ("find_pipe_positions: multiple code spans",
         test_pipe_positions_multiple_code_spans),
        ("find_pipe_positions: escaped backtick not span",
         test_pipe_positions_escaped_backtick_not_span),

        # split_cells
        ("split_cells: simple", test_split_cells_simple),
        ("split_cells: escaped pipe",
         test_split_cells_escaped_pipe),
        ("split_cells: whitespace stripped",
         test_split_cells_whitespace_stripped),
        ("split_cells: pipe inside code span",
         test_split_cells_pipe_inside_backticks),
        ("split_cells: code span with trailing text",
         test_split_cells_pipe_inside_backticks_with_adjacent_text),
        ("split_cells: multiple code spans per cell",
         test_split_cells_multiple_code_spans_per_cell),
        ("split_cells: escaped backtick not span",
         test_split_cells_escaped_backtick_does_not_open_span),

        # check_file
        ("check_file: aligned table passes",
         test_aligned_table_passes),
        ("check_file: misaligned table fails",
         test_misaligned_table_fails),
        ("check_file: multiple tables — only bad reported",
         test_multiple_tables_reports_only_bad),
        ("check_file: escaped pipe in content",
         test_escaped_pipe_in_content),
        ("check_file: backticked pipe in content",
         test_backticked_pipe_in_content),
        ("check_file: fenced code block skipped",
         test_fenced_code_block_skipped),
        ("check_file: empty file", test_empty_file),
        ("check_file: header + separator only",
         test_header_and_separator_only),
        ("check_file: separator alignment markers",
         test_separator_alignment_markers),

        # format_table
        ("format_table: pads short cells",
         test_pads_short_cells),
        ("format_table: extends separator dashes",
         test_extends_separator_dashes),
        ("format_table: preserves left align",
         test_preserves_left_align),
        ("format_table: preserves right align",
         test_preserves_right_align),
        ("format_table: preserves centre align",
         test_preserves_centre_align),
        ("format_table: preserves no align",
         test_preserves_no_align),
        ("format_table: escaped pipe untouched",
         test_escaped_pipe_untouched),
        ("format_table: code span pipe preserved",
         test_code_span_pipe_preserved_through_format),
        ("format_table: unsafe rows skipped",
         test_content_verification_skips_unsafe),

        # format_file
        ("format_file: fenced code block untouched",
         test_fenced_code_block_untouched_by_format),
        ("format_file: already aligned unchanged",
         test_already_aligned_unchanged),
        ("format_file: multiple tables — only bad reformatted",
         test_multiple_tables_only_bad_reformatted),

        # CLI
        ("cli check: aligned exits 0", test_cli_check_ok),
        ("cli check: misaligned exits 1",
         test_cli_check_fails_on_misaligned),
        ("cli format-then-check: end-to-end pass",
         test_cli_format_then_check),
        ("cli usage error: missing subcommand exits 2",
         test_cli_usage_error_without_subcommand),
    ]
    for label, fn in tests:
        fn()
        _ok(label)
    print(f"\nall {len(tests)} tests passed")


if __name__ == "__main__":
    main()
