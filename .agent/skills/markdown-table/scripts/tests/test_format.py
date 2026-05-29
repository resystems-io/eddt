"""Tests for the markdown table formatter."""

import os
import tempfile

import pytest

import sys
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from format import format_table, format_file, split_cells


# --- split_cells ---

def test_split_cells_simple():
    assert split_cells("| a | b | c |") == ["a", "b", "c"]


def test_split_cells_escaped_pipe():
    assert split_cells(r"| a\|b | c |") == [r"a\|b", "c"]


def test_split_cells_whitespace_stripped():
    assert split_cells("|  foo  |  bar  |") == ["foo", "bar"]


def test_split_cells_pipe_inside_backticks():
    # Pipes inside a backtick-delimited inline code span are
    # literal content, not cell delimiters — python-markdown
    # renders them correctly, so the formatter must not treat
    # them as column separators.
    assert split_cells("| a | `x|y` | c |") == ["a", "`x|y`", "c"]


def test_split_cells_pipe_inside_backticks_with_adjacent_text():
    assert split_cells("| key | `snap|delta` partition |") == [
        "key", "`snap|delta` partition"
    ]


def test_split_cells_multiple_code_spans_per_cell():
    # Two backtick-delimited spans in one cell; pipes inside
    # either span stay literal, pipes outside are delimiters.
    assert split_cells("| a | `x|y` and `p|q` | b |") == [
        "a", "`x|y` and `p|q`", "b"
    ]


def test_split_cells_escaped_backtick_does_not_open_span():
    # A \` is a literal backtick; it should not open a code
    # span, so a following pipe remains a delimiter.
    assert split_cells(r"| a\` | b|c | d |") == [r"a\`", "b", "c", "d"]


# --- format_table ---

def test_pads_short_cells():
    lines = [
        "| Name | Age |\n",
        "|:---|:---|\n",
        "| Alice | 30 |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    # All pipes should be at the same positions.
    pipes = _pipe_positions(result)
    assert all(p == pipes[0] for p in pipes)


def test_extends_separator_dashes():
    lines = [
        "| Long Header | Short |\n",
        "|:---|:---|\n",
        "| x           | y     |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    pipes = _pipe_positions(result)
    assert all(p == pipes[0] for p in pipes)


def test_preserves_left_align():
    lines = [
        "| Col |\n",
        "|:----|\n",
        "| val |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    sep = result[1].strip()
    assert sep.startswith('|:')
    assert ':|' not in sep  # no right-align


def test_preserves_right_align():
    lines = [
        "| Col |\n",
        "|----:|\n",
        "| val |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    sep = result[1].strip()
    assert sep.endswith(':|')
    assert ':--' not in sep  # no left-align


def test_preserves_centre_align():
    lines = [
        "| Col |\n",
        "|:---:|\n",
        "| val |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    sep = result[1].strip()
    # Should have colons on both sides.
    inner = sep.strip('|')
    assert inner.startswith(':')
    assert inner.endswith(':')


def test_preserves_no_align():
    lines = [
        "| Col |\n",
        "|-----|\n",
        "| val |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    sep = result[1].strip()
    inner = sep.strip('|')
    assert not inner.startswith(':')
    assert not inner.endswith(':')


def test_escaped_pipe_untouched():
    lines = [
        r"| Format     | Example |" + "\n",
        r"|:-----------|:--------|" + "\n",
        r"| json\|text | either  |" + "\n",
    ]
    result, ok = format_table(lines)
    assert ok
    # Content should still contain the escaped pipe.
    assert r"json\|text" in result[2]


def test_code_span_pipe_preserved_through_format():
    # A pipe inside a backtick-delimited inline code span is
    # literal; the formatter must not split the row on it, and
    # the content must round-trip verbatim through the reformat.
    lines = [
        "| key   | value                |\n",
        "|:------|:---------------------|\n",
        "| nat   | `kind=<snap|delta>`  |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    # The reformatted row has exactly 2 cells (not 3).
    data_cells = split_cells(result[2].strip())
    assert len(data_cells) == 2
    assert data_cells[1] == "`kind=<snap|delta>`"


def test_fenced_code_block_untouched():
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
        changed, formatted, skipped = format_file(path)
        assert not changed
        assert formatted == 0
        assert skipped == 0
    finally:
        os.unlink(path)


def test_content_verification_skips_unsafe():
    # A table with inconsistent column counts should be skipped.
    lines = [
        "| A | B | C |\n",
        "|:--|:--|:--|\n",
        "| 1 | 2 |\n",  # missing column
    ]
    result, ok = format_table(lines)
    assert not ok
    # Original lines returned unchanged.
    assert result == lines


def test_already_aligned_unchanged():
    content = (
        "| Name  | Age |\n"
        "|:------|:----|\n"
        "| Alice | 30  |\n"
    )
    path = _write(content)
    try:
        changed, formatted, skipped = format_file(path)
        assert not changed
    finally:
        os.unlink(path)


def test_multiple_tables_only_bad_reformatted():
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
        changed, formatted, skipped = format_file(path)
        assert changed
        assert formatted >= 1

        # Verify the good table is untouched by checking content.
        with open(path) as f:
            result = f.read()
        assert "| A | B |" in result
    finally:
        os.unlink(path)


def test_trailing_whitespace_normalised():
    lines = [
        "| Name   | Age   |\n",
        "|:-------|:------|\n",
        "| Alice  | 30    |\n",
    ]
    result, ok = format_table(lines)
    assert ok
    # No trailing spaces before the final pipe.
    for line in result:
        stripped = line.rstrip('\n')
        assert stripped.endswith('|')


# --- Helpers ---

def _pipe_positions(lines: list[str]) -> list[list[int]]:
    """Return pipe positions for each line."""
    result = []
    for line in lines:
        line = line.rstrip('\n')
        result.append(
            [i for i, c in enumerate(line) if c == '|'
             and (i == 0 or line[i - 1] != '\\')])
    return result


def _write(content: str) -> str:
    f = tempfile.NamedTemporaryFile(
        mode='w', suffix='.md', delete=False)
    f.write(content)
    f.close()
    return f.name
