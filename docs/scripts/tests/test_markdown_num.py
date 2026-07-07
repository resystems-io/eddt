#!/usr/bin/env python3
"""Unit tests for markdown-num.py.

Exercises the heading renumberer against synthesised markdown fixtures.
Checks:

* R01: zero-based sequence renumbered to 1-based (primary use case).
* R02: sequence with arbitrary gap numbers renumbered correctly from 1.
* R03: multi-level (## + ###) renumbering; ### numbering follows the new
  ## parent number.
* R04: --start 0 produces a 0-based top-level sequence.
* R05: --start N shifts the base to an arbitrary N.
* R06: headings inside fenced code blocks are not renumbered.
* R07: second run is byte-identical (idempotent); mtime is preserved.
* R08: --check exits non-zero when renumbering is needed.
* R09: --check exits zero when numbering is already correct.
* R10: --dry-run prints changed lines to stdout but does not write the file.
* R11: all non-heading content is preserved byte-for-byte.
* R12: unnumbered ## / ### / #### headings produce a warning on stderr.
* R13: warning includes the offending line number and heading text.
* R14: a numbered-looking heading inside an HTML comment is left
  byte-for-byte unchanged — not renumbered, and no warning emitted.
* R15: a same-line, self-closing comment does not block renumbering of a
  real heading that follows it.

Run from the repo root or this directory:

    python3 designs/scripts/tests/test_markdown_num.py

Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
MD_NUM = SCRIPTS / "markdown-num.py"


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"ok   {msg}")


def _run(path: Path, *extra_args: str) -> subprocess.CompletedProcess:
    """Invoke markdown-num.py against `path` and return the completed process."""
    return subprocess.run(
        ["python3", str(MD_NUM), str(path), *extra_args],
        capture_output=True,
        text=True,
    )


def _mktemp_md(suffix: str = ".md") -> Path:
    """Create a tempfile under SCRIPTS so snap-confined interpreters can see it."""
    fd, name = tempfile.mkstemp(suffix=suffix, dir=str(SCRIPTS))
    os.close(fd)
    return Path(name)


# ── Fixtures ─────────────────────────────────────────────────────────────────

DOC_ZERO_BASED = """\
# Title

Preamble paragraph.

## 0. Background

First section body.

## 1. Purpose and Scope

Second section body.

## 2. Definitions

Third section body.
"""

DOC_ONE_BASED = """\
# Title

Preamble paragraph.

## 1. Background

First section body.

## 2. Purpose and Scope

Second section body.

## 3. Definitions

Third section body.
"""

DOC_GAP_NUMBERS = """\
# Title

## 3. First Section

Body.

## 7. Second Section

More body.
"""

DOC_MULTI_LEVEL = """\
# Title

## 0. Introduction

Some intro text.

### 0.1 Background

Background text.

### 0.2 Scope

Scope text.

## 1. Requirements

Requirements intro.

### 1.1 Functional

Functional details.

### 1.2 Non-Functional

Non-functional details.
"""

DOC_MULTI_LEVEL_EXPECTED = """\
# Title

## 1. Introduction

Some intro text.

### 1.1 Background

Background text.

### 1.2 Scope

Scope text.

## 2. Requirements

Requirements intro.

### 2.1 Functional

Functional details.

### 2.2 Non-Functional

Non-functional details.
"""

# The fenced headings start at "3." — clearly different from the real
# headings — so the test can confirm the fence was respected.
DOC_WITH_FENCE = (
    "# Title\n\n"
    "## 0. Real Section\n\n"
    "Some body.\n\n"
    "```python\n"
    "## 3. Fake Heading (inside fence, must not be renumbered)\n"
    "### 3.1 Fake Sub (also inside fence)\n"
    "```\n\n"
    "## 1. Another Real Section\n\n"
    "End.\n"
)

# HTML comment containing numbered-looking headings — must not be
# renumbered or warned about. Covers: R14.
DOC_WITH_COMMENT = (
    "# Title\n\n"
    "## 0. Real Section\n\n"
    "Some body.\n\n"
    "<!--\n\n"
    "## 5. Fake Heading (inside comment, must not be renumbered)\n\n"
    "### 5.1 Fake Sub (also inside comment)\n\n"
    "-->\n\n"
    "## 1. Another Real Section\n\n"
    "End.\n"
)

# Same-line, self-closing comment immediately followed by a real numbered
# heading. Covers: R15.
DOC_WITH_SAME_LINE_COMMENT = (
    "# Title\n\n"
    "<!-- a short note -->\n\n"
    "## 0. Real Section\n\n"
    "End.\n"
)

# "### Overview" is deliberately unnumbered to trigger warnings.
DOC_WITH_UNNUMBERED = """\
# Title

## 1. Introduction

### Overview

Some text here.

### 1.1 Details

More text.

## 2. Conclusion

End.
"""


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_zero_based_renumbered_to_one_based() -> None:
    """Covers: R01 — 0-based sequence becomes 1-based with default --start 1."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R01: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if "## 1. Background" not in content:
            _fail("R01: '## 1. Background' not found after renumbering")
        if "## 2. Purpose and Scope" not in content:
            _fail("R01: '## 2. Purpose and Scope' not found after renumbering")
        if "## 3. Definitions" not in content:
            _fail("R01: '## 3. Definitions' not found after renumbering")
        if "## 0." in content:
            _fail("R01: original '## 0.' still present after renumbering")
    finally:
        path.unlink()


def test_gap_numbers_renumbered() -> None:
    """Covers: R02 — sequence with gaps (3, 7) is renumbered to (1, 2)."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_GAP_NUMBERS)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R02: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if "## 1. First Section" not in content:
            _fail("R02: '## 1. First Section' not found")
        if "## 2. Second Section" not in content:
            _fail("R02: '## 2. Second Section' not found")
        if "## 3." in content or "## 7." in content:
            _fail("R02: original gap numbers still present in output")
    finally:
        path.unlink()


def test_multi_level_renumbering() -> None:
    """Covers: R03 — ### numbers follow their new ## parent after renumbering."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_MULTI_LEVEL)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R03: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if content != DOC_MULTI_LEVEL_EXPECTED:
            _fail(
                "R03: output does not match expected.\n"
                f"  got:\n{content}\n"
                f"  expected:\n{DOC_MULTI_LEVEL_EXPECTED}"
            )
    finally:
        path.unlink()


def test_start_zero_flag() -> None:
    """Covers: R04 — --start 0 produces a 0-based top-level sequence."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ONE_BASED)
        res = _run(path, "--start", "0")
        if res.returncode != 0:
            _fail(f"R04: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if "## 0. Background" not in content:
            _fail("R04: '## 0. Background' not found with --start 0")
        if "## 1. Purpose and Scope" not in content:
            _fail("R04: '## 1. Purpose and Scope' not found with --start 0")
        if "## 2. Definitions" not in content:
            _fail("R04: '## 2. Definitions' not found with --start 0")
    finally:
        path.unlink()


def test_start_n_flag() -> None:
    """Covers: R05 — --start N shifts the top-level base to an arbitrary N."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        res = _run(path, "--start", "3")
        if res.returncode != 0:
            _fail(f"R05: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if "## 3. Background" not in content:
            _fail("R05: '## 3. Background' not found with --start 3")
        if "## 4. Purpose and Scope" not in content:
            _fail("R05: '## 4. Purpose and Scope' not found with --start 3")
        if "## 5. Definitions" not in content:
            _fail("R05: '## 5. Definitions' not found with --start 3")
    finally:
        path.unlink()


def test_fenced_headings_not_renumbered() -> None:
    """Covers: R06 — headings inside fenced code blocks are not touched."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_WITH_FENCE)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R06: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        # Fenced headings must remain at their original numbers.
        if "## 3. Fake Heading (inside fence, must not be renumbered)" not in content:
            _fail("R06: fenced ## heading was modified (must be left unchanged)")
        if "### 3.1 Fake Sub (also inside fence)" not in content:
            _fail("R06: fenced ### heading was modified (must be left unchanged)")
        # Real headings must have been renumbered from 0-based to 1-based.
        if "## 1. Real Section" not in content:
            _fail("R06: real '## 1. Real Section' missing after renumbering")
        if "## 2. Another Real Section" not in content:
            _fail("R06: real '## 2. Another Real Section' missing after renumbering")
    finally:
        path.unlink()


def test_idempotent_and_mtime_preserved() -> None:
    """Covers: R07 — second run is byte-identical; mtime preserved on no-op."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        _run(path)                # first run: renumber 0-based → 1-based

        after_first = path.read_text()

        # Anchor mtime well in the past so any rewrite is unambiguously visible.
        past = time.time() - 3600
        os.utime(path, (past, past))
        mtime_before = path.stat().st_mtime

        res = _run(path)          # second run: should be a no-op
        if res.returncode != 0:
            _fail(f"R07: no-op run exited {res.returncode}; stderr={res.stderr!r}")
        if "no changes" not in res.stdout:
            _fail(f"R07: expected 'no changes' on second run; got {res.stdout!r}")

        after_second = path.read_text()
        if after_first != after_second:
            _fail("R07: second run changed the file bytes (not idempotent)")

        mtime_after = path.stat().st_mtime
        if mtime_after != mtime_before:
            _fail(
                f"R07: mtime changed despite no-op "
                f"(before={mtime_before:.6f}, after={mtime_after:.6f})"
            )
    finally:
        path.unlink()


def test_check_exits_nonzero_when_needed() -> None:
    """Covers: R08 — --check exits non-zero when renumbering is needed."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        res = _run(path, "--check")
        if res.returncode == 0:
            _fail("R08: --check returned 0 but file needed renumbering")
        # --check must not modify the file.
        if path.read_text() != DOC_ZERO_BASED:
            _fail("R08: --check modified the file (must not write)")
    finally:
        path.unlink()


def test_check_exits_zero_when_correct() -> None:
    """Covers: R09 — --check exits zero when numbering is already correct."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ONE_BASED)
        res = _run(path, "--check")
        if res.returncode != 0:
            _fail(
                f"R09: --check exited {res.returncode} on an already-correct "
                f"file; stderr={res.stderr!r}"
            )
    finally:
        path.unlink()


def test_dry_run_prints_changes_not_write() -> None:
    """Covers: R10 — --dry-run prints changed lines to stdout; file unchanged."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        res = _run(path, "--dry-run")
        if res.returncode != 0:
            _fail(f"R10: --dry-run exited {res.returncode}; stderr={res.stderr!r}")
        # stdout must show both the old heading and the new heading.
        if "0. Background" not in res.stdout:
            _fail("R10: old '0. Background' not shown in --dry-run output")
        if "1. Background" not in res.stdout:
            _fail("R10: new '1. Background' not shown in --dry-run output")
        # The file itself must remain unchanged.
        if path.read_text() != DOC_ZERO_BASED:
            _fail("R10: --dry-run modified the file (must not write)")
    finally:
        path.unlink()


def test_non_heading_content_preserved() -> None:
    """Covers: R11 — all non-heading content is preserved byte-for-byte."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_ZERO_BASED)
        _run(path)
        content = path.read_text()
        for fragment in [
            "Preamble paragraph.",
            "First section body.",
            "Second section body.",
            "Third section body.",
        ]:
            if fragment not in content:
                _fail(f"R11: body text {fragment!r} missing after renumbering")
    finally:
        path.unlink()


def test_unnumbered_heading_warns() -> None:
    """Covers: R12, R13 — unnumbered ## / ### headings produce a warning on
    stderr that includes the line number and heading text."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_WITH_UNNUMBERED)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R12: script exited {res.returncode}; stderr={res.stderr!r}")

        # R12: warning must appear on stderr.
        if "warning" not in res.stderr.lower():
            _fail("R12: no warning emitted for unnumbered ### heading")

        # R13: warning must name the heading text.
        if "Overview" not in res.stderr:
            _fail("R13: warning does not include the offending heading text ('Overview')")

        # R13: warning must include a line number reference.
        if not re.search(r"line \d+", res.stderr):
            _fail("R13: warning does not include a line number (expected 'line N')")

        # R12: the unnumbered heading itself must be left unchanged.
        content = path.read_text()
        if "### Overview" not in content:
            _fail("R12: unnumbered heading was modified (must be left as-is)")

        # Numbered headings alongside the unnumbered one must still be renumbered.
        if "### 1.1 Details" not in content:
            _fail("R12: numbered sub-heading not correctly renumbered despite warning")
    finally:
        path.unlink()


def test_commented_headings_not_renumbered() -> None:
    """Covers: R14 — a numbered-looking heading inside an HTML comment
    is left byte-for-byte unchanged: not renumbered, and no warning
    emitted, even though it looks unnumbered-adjacent. Real headings
    before and after the comment are still correctly renumbered."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_WITH_COMMENT)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R14: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()

        # Headings inside the comment must remain at their original numbers.
        if "## 5. Fake Heading (inside comment, must not be renumbered)" not in content:
            _fail("R14: commented ## heading was modified (must be left unchanged)")
        if "### 5.1 Fake Sub (also inside comment)" not in content:
            _fail("R14: commented ### heading was modified (must be left unchanged)")

        # No warning should be emitted for the commented-out headings.
        if "warning" in res.stderr.lower():
            _fail(f"R14: unexpected warning for commented headings: {res.stderr!r}")

        # Real headings must still have been renumbered from 0-based to 1-based.
        if "## 1. Real Section" not in content:
            _fail("R14: real '## 1. Real Section' missing after renumbering")
        if "## 2. Another Real Section" not in content:
            _fail("R14: real '## 2. Another Real Section' missing after renumbering")
    finally:
        path.unlink()


def test_same_line_comment_does_not_block_renumbering() -> None:
    """Covers: R15 — a same-line, self-closing comment does not leave
    the scanner stuck 'inside a comment', so a real heading immediately
    after it is still renumbered."""
    path = _mktemp_md()
    try:
        path.write_text(DOC_WITH_SAME_LINE_COMMENT)
        res = _run(path)
        if res.returncode != 0:
            _fail(f"R15: script exited {res.returncode}; stderr={res.stderr!r}")
        content = path.read_text()
        if "## 1. Real Section" not in content:
            _fail("R15: heading after same-line comment was not renumbered")
    finally:
        path.unlink()


# ── Runner ────────────────────────────────────────────────────────────────────

def main() -> None:
    tests = [
        ("R01 — zero-based sequence renumbered to 1-based",
         test_zero_based_renumbered_to_one_based),
        ("R02 — gap numbers renumbered from 1",
         test_gap_numbers_renumbered),
        ("R03 — multi-level (## + ###) renumbering follows parent",
         test_multi_level_renumbering),
        ("R04 — --start 0 produces 0-based sequence",
         test_start_zero_flag),
        ("R05 — --start N shifts base to arbitrary N",
         test_start_n_flag),
        ("R06 — headings inside fenced code blocks not renumbered",
         test_fenced_headings_not_renumbered),
        ("R07 — second run idempotent; mtime preserved on no-op",
         test_idempotent_and_mtime_preserved),
        ("R08 — --check exits non-zero when renumbering needed",
         test_check_exits_nonzero_when_needed),
        ("R09 — --check exits zero when numbering already correct",
         test_check_exits_zero_when_correct),
        ("R10 — --dry-run prints changes but does not write",
         test_dry_run_prints_changes_not_write),
        ("R11 — non-heading content preserved byte-for-byte",
         test_non_heading_content_preserved),
        ("R12/R13 — unnumbered heading warns with line number and text",
         test_unnumbered_heading_warns),
        ("R14 — headings inside HTML comments not renumbered, no warning",
         test_commented_headings_not_renumbered),
        ("R15 — same-line comment does not block renumbering of following heading",
         test_same_line_comment_does_not_block_renumbering),
    ]
    for label, fn in tests:
        fn()
        _ok(label)
    print("\nall tests passed")


if __name__ == "__main__":
    main()
