"""Tests for the markdown table alignment checker."""

import os
import tempfile

import pytest

# Import from parent package.
import sys
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from check import check_file, find_pipe_positions


# --- find_pipe_positions ---

def test_pipe_positions_simple():
    assert find_pipe_positions("| a | b | c |") == [0, 4, 8, 12]


def test_pipe_positions_escaped():
    # The \| should not be counted as a pipe.
    assert find_pipe_positions(r"| a\|b | c |") == [0, 7, 11]


def test_pipe_positions_empty():
    assert find_pipe_positions("no pipes here") == []


def test_pipe_positions_backticked_pipe():
    # A pipe inside a backtick-delimited inline code span is
    # literal content and must not be counted as a cell
    # delimiter.
    assert find_pipe_positions("| a | `x|y` | b |") == [0, 4, 12, 16]


def test_pipe_positions_multiple_code_spans():
    # Two spans in the same line — pipes inside either span
    # are literal, the pipe between them is a delimiter.
    line = "| `a|b` | `c|d` |"
    # Delimiter pipes at column 0, 8 (between the spans), 16.
    assert find_pipe_positions(line) == [0, 8, 16]


def test_pipe_positions_escaped_backtick_not_span():
    # A \` is a literal backtick and must not open a span, so
    # the following pipes all remain cell delimiters.
    assert find_pipe_positions(r"| a\` | b|c |") == [0, 6, 9, 12]


# --- check_file ---

def _write(content: str) -> str:
    """Write content to a temp file and return the path."""
    f = tempfile.NamedTemporaryFile(
        mode='w', suffix='.md', delete=False)
    f.write(content)
    f.close()
    return f.name


def test_aligned_table_passes():
    path = _write(
        "| Name  | Age |\n"
        "|:------|:----|\n"
        "| Alice | 30  |\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_misaligned_table_fails():
    path = _write(
        "| Name | Age |\n"
        "|:---|:---|\n"
        "| Alice | 30 |\n"
    )
    try:
        errors = check_file(path)
        assert len(errors) == 1
        assert "misaligned" in errors[0]
    finally:
        os.unlink(path)


def test_multiple_tables_reports_only_bad():
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
        errors = check_file(path)
        assert len(errors) == 1
    finally:
        os.unlink(path)


def test_escaped_pipe_in_content():
    # The escaped pipe should not create an extra column.
    path = _write(
        r"| Format       | Example    |" + "\n"
        r"|:-------------|:-----------|" + "\n"
        r"| json\|text   | either one |" + "\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_backticked_pipe_in_content():
    # A pipe inside a backtick-delimited inline code span is
    # literal and must not be flagged as misaligned — the
    # corresponding cell is otherwise a single consistent
    # column.
    path = _write(
        "| Key | Value                |\n"
        "|:----|:---------------------|\n"
        "| nat | `kind=<snap|delta>`  |\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_fenced_code_block_skipped():
    path = _write(
        "```\n"
        "| not | a | table |\n"
        "|:----|:--|:------|\n"
        "| a | b | c |\n"
        "```\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_empty_file():
    path = _write("")
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_header_and_separator_only():
    path = _write(
        "| Col A | Col B |\n"
        "|:------|:------|\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)


def test_separator_alignment_markers():
    # All three alignment styles should be recognised.
    path = _write(
        "| Left | Centre | Right |\n"
        "|:-----|:------:|------:|\n"
        "| a    | b      | c     |\n"
    )
    try:
        errors = check_file(path)
        assert errors == []
    finally:
        os.unlink(path)
