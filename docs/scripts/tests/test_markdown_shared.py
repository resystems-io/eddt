#!/usr/bin/env python3
"""Unit tests for the shared _markdown_shared.py helper module.

Exercises real_line_flags(), find_first_real_match(), strip_marked_block(),
and write_if_changed() directly — the primitives that markdown-toc.py,
markdown-pdf.py, markdown-num.py, markdown-index.py, and refs-linkify.py
all depend on to correctly ignore fenced code blocks and HTML comments
when scanning for headings and identifiers, to locate generated blocks
bracketed by start/end markers, and to skip a write when the output is
unchanged. Checks:

* R01: a plain document with no fences or comments has every line flagged
  as real.
* R02: a fenced code block's delimiter and content lines are flagged as
  not-real; content immediately after the fence is real again.
* R03: a multi-line HTML comment (including internal blank lines) has
  every line, including the opening and closing delimiter lines, flagged
  as not-real; content after the closing delimiter is real again.
* R04: a same-line, self-closing comment marks only that one line as
  not-real; a real heading on the very next line is unaffected.
* R05: a comment and a fence interacting in sequence are each tracked
  independently (comment then fence, and fence then comment).
* R06: find_first_real_match returns None when no real line matches the
  given pattern.
* R07: find_first_real_match skips a match inside a comment and returns
  the first match on a real line instead.
* R08: find_first_real_match's returned Match object's span indexes
  correctly into the ORIGINAL text (not just the matched line), so
  text[m.start():m.end()] equals the matched line's real content.
* R09: find_first_real_match preserves capture groups from line_pattern.
* R10: strip_marked_block splits a well-formed block cleanly, consuming
  both marker lines.
* R11: strip_marked_block returns found=False, text unchanged, when the
  start marker is absent.
* R12: strip_marked_block returns found=False, text unchanged, when the
  start marker is present but no end marker follows it.
* R13: strip_marked_block requires a whole-line marker match — a line
  merely containing the marker text as a substring does not match.
* R14: write_if_changed writes and returns True when new_text differs
  from original_text.
* R15: write_if_changed does not write (and preserves mtime) and returns
  False when new_text equals original_text.

Run from the repo root or this directory:

    python3 designs/scripts/tests/test_markdown_shared.py

Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))

import _markdown_shared as mc  # noqa: E402


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"ok   {msg}")


# ----------------------------------------------------------------------
# real_line_flags
# ----------------------------------------------------------------------


def test_plain_lines_all_real() -> None:
    """Covers: R01 — no fences or comments: every line is real."""
    lines = ["# Title", "", "## Section", "", "Body text."]
    flags = mc.real_line_flags(lines)
    if flags != [True] * len(lines):
        _fail(f"R01: expected all-True, got {flags!r}")


def test_fenced_lines_not_real() -> None:
    """Covers: R02 — fence delimiter and content lines are not-real;
    content resumes being real after the closing fence."""
    lines = [
        "## Real",
        "```",
        "## Fake (inside fence)",
        "```",
        "## Real Again",
    ]
    flags = mc.real_line_flags(lines)
    expected = [True, False, False, False, True]
    if flags != expected:
        _fail(f"R02: expected {expected!r}, got {flags!r}")


def test_multiline_comment_not_real() -> None:
    """Covers: R03 — a multi-line comment, including internal blank
    lines and its own delimiter lines, is entirely not-real; content
    after the closing --> is real again."""
    lines = [
        "<!--",
        "",
        "## Fake Section",
        "",
        "More scratch content.",
        "-->",
        "## Real Section",
    ]
    flags = mc.real_line_flags(lines)
    expected = [False, False, False, False, False, False, True]
    if flags != expected:
        _fail(f"R03: expected {expected!r}, got {flags!r}")


def test_same_line_comment_marks_only_that_line() -> None:
    """Covers: R04 — a same-line self-closing comment (mirroring
    delimiter comments like `<!-- TOC generated ... -->`) marks only its
    own line; a real heading on the next line is unaffected."""
    lines = ["<!-- a short note -->", "## Real Section"]
    flags = mc.real_line_flags(lines)
    if flags != [False, True]:
        _fail(f"R04: expected [False, True], got {flags!r}")


def test_comment_and_fence_interaction() -> None:
    """Covers: R05 — a comment followed by a fence, and a fence followed
    by a comment, are each tracked independently and correctly."""
    lines = [
        "<!-- note -->",
        "## Real",
        "```",
        "<!-- this is just fenced text, not a real comment -->",
        "```",
        "## Real Again",
    ]
    flags = mc.real_line_flags(lines)
    expected = [False, True, False, False, False, True]
    if flags != expected:
        _fail(f"R05: expected {expected!r}, got {flags!r}")


# ----------------------------------------------------------------------
# find_first_real_match
# ----------------------------------------------------------------------


def test_no_real_match_returns_none() -> None:
    """Covers: R06 — no real line matches the pattern: returns None."""
    text = "<!--\n## Fake\n-->\n\nJust a paragraph.\n"
    m = mc.find_first_real_match(text, re.compile(r"## "))
    if m is not None:
        _fail(f"R06: expected None, got a match: {m.group(0)!r}")


def test_skips_commented_match() -> None:
    """Covers: R07 — a match inside a comment is skipped; the first real
    match is returned instead."""
    text = "<!--\n## Fake\n-->\n\n## Real\n"
    m = mc.find_first_real_match(text, re.compile(r"## (.+)"))
    if m is None:
        _fail("R07: expected a match, got None")
    if m.group(1) != "Real":
        _fail(f"R07: expected 'Real', got {m.group(1)!r}")


def test_match_span_indexes_original_text() -> None:
    """Covers: R08 — the returned Match's span indexes into the
    ORIGINAL text, not just the matched line."""
    text = "<!--\n## Fake\n-->\n\n## Real Section\n"
    m = mc.find_first_real_match(text, re.compile(r"## (.+)"))
    if m is None:
        _fail("R08: expected a match, got None")
    if text[m.start():m.end()] != "## Real Section":
        _fail(f"R08: span does not index correctly: {text[m.start():m.end()]!r}")


def test_capture_groups_preserved() -> None:
    """Covers: R09 — capture groups in line_pattern survive through to
    the returned Match (used by markdown-pdf.py to extract title text)."""
    text = "# My Document Title\n\n## Section\n"
    m = mc.find_first_real_match(text, re.compile(r"# (.+)"))
    if m is None:
        _fail("R09: expected a match, got None")
    if m.group(1) != "My Document Title":
        _fail(f"R09: expected 'My Document Title', got {m.group(1)!r}")


# ----------------------------------------------------------------------
# strip_marked_block
# ----------------------------------------------------------------------


def test_strip_marked_block_found() -> None:
    """Covers: R10 — a well-formed block is split cleanly: before/after
    are everything outside the marker lines, both marker lines are
    consumed."""
    text = "Before.\n\n<!-- START -->\nblock content\n<!-- END -->\n\nAfter.\n"
    before, after, found = mc.strip_marked_block(text, "<!-- START -->", "<!-- END -->")
    if not found:
        _fail("R10: expected found=True")
    if before != "Before.\n\n":
        _fail(f"R10: unexpected before: {before!r}")
    if after != "\nAfter.\n":
        _fail(f"R10: unexpected after: {after!r}")


def test_strip_marked_block_start_not_found() -> None:
    """Covers: R11 — start marker absent: found=False, text unchanged."""
    text = "No markers here.\n"
    before, after, found = mc.strip_marked_block(text, "<!-- START -->", "<!-- END -->")
    if found:
        _fail("R11: expected found=False")
    if before != text or after != "":
        _fail(f"R11: expected text unchanged; got before={before!r}, after={after!r}")


def test_strip_marked_block_end_not_found() -> None:
    """Covers: R12 — start marker present but no matching end marker:
    found=False, text unchanged (caller implements its own fallback)."""
    text = "<!-- START -->\nunterminated block\n"
    before, after, found = mc.strip_marked_block(text, "<!-- START -->", "<!-- END -->")
    if found:
        _fail("R12: expected found=False")
    if before != text or after != "":
        _fail(f"R12: expected text unchanged; got before={before!r}, after={after!r}")


def test_strip_marked_block_requires_whole_line_match() -> None:
    """Covers: R13 — the marker must be the entire (whitespace-trimmed)
    line content; a line merely containing the marker text as a
    substring does not match. Safer than a plain substring search,
    which could false-positive on marker text embedded in unrelated
    content (e.g. inside a fenced code example)."""
    text = "Some text mentioning <!-- START --> inline, not as its own line.\n"
    before, after, found = mc.strip_marked_block(text, "<!-- START -->", "<!-- END -->")
    if found:
        _fail("R13: expected found=False for a mid-line marker occurrence")


# ----------------------------------------------------------------------
# write_if_changed
# ----------------------------------------------------------------------


def _mktemp() -> Path:
    fd, name = tempfile.mkstemp(dir=str(SCRIPTS))
    os.close(fd)
    return Path(name)


def test_write_if_changed_writes_when_different() -> None:
    """Covers: R14 — writes and returns True when new_text differs from
    original_text."""
    path = _mktemp()
    try:
        path.write_text("old content\n")
        wrote = mc.write_if_changed(path, "new content\n", "old content\n")
        if not wrote:
            _fail("R14: expected True (a write occurred)")
        if path.read_text() != "new content\n":
            _fail(f"R14: file content not updated: {path.read_text()!r}")
    finally:
        path.unlink()


def test_write_if_changed_noop_preserves_mtime() -> None:
    """Covers: R15 — does not write (and preserves mtime) and returns
    False when new_text equals original_text — the no-op-if-unchanged
    guarantee that makes callers safe to wire into Makefile dependency
    chains."""
    path = _mktemp()
    try:
        path.write_text("same content\n")
        past = time.time() - 3600
        os.utime(path, (past, past))
        mtime_before = path.stat().st_mtime

        wrote = mc.write_if_changed(path, "same content\n", "same content\n")
        if wrote:
            _fail("R15: expected False (no write)")
        mtime_after = path.stat().st_mtime
        if mtime_after != mtime_before:
            _fail(f"R15: mtime changed despite no-op (before={mtime_before}, after={mtime_after})")
    finally:
        path.unlink()


def main() -> None:
    tests = [
        ("R01 — plain lines all flagged real",
         test_plain_lines_all_real),
        ("R02 — fenced lines flagged not-real",
         test_fenced_lines_not_real),
        ("R03 — multi-line comment entirely not-real",
         test_multiline_comment_not_real),
        ("R04 — same-line comment marks only that line",
         test_same_line_comment_marks_only_that_line),
        ("R05 — comment/fence interaction tracked independently",
         test_comment_and_fence_interaction),
        ("R06 — find_first_real_match returns None when no real match",
         test_no_real_match_returns_none),
        ("R07 — find_first_real_match skips a commented match",
         test_skips_commented_match),
        ("R08 — match span indexes into the original text",
         test_match_span_indexes_original_text),
        ("R09 — capture groups preserved through find_first_real_match",
         test_capture_groups_preserved),
        ("R10 — strip_marked_block splits a well-formed block cleanly",
         test_strip_marked_block_found),
        ("R11 — strip_marked_block: start marker absent leaves text unchanged",
         test_strip_marked_block_start_not_found),
        ("R12 — strip_marked_block: end marker absent leaves text unchanged",
         test_strip_marked_block_end_not_found),
        ("R13 — strip_marked_block requires a whole-line marker match",
         test_strip_marked_block_requires_whole_line_match),
        ("R14 — write_if_changed writes and returns True when different",
         test_write_if_changed_writes_when_different),
        ("R15 — write_if_changed no-ops and preserves mtime when unchanged",
         test_write_if_changed_noop_preserves_mtime),
    ]
    for label, fn in tests:
        fn()
        _ok(label)
    print("\nall tests passed")


if __name__ == "__main__":
    main()
