#!/usr/bin/env python3
"""Renumber section headings in a markdown document.

Usage:
    ./scripts/markdown-num.py docs/file.md
    ./scripts/markdown-num.py --start 1 docs/file.md
    ./scripts/markdown-num.py --check docs/file.md
    ./scripts/markdown-num.py --dry-run docs/file.md

Renumbers all numbered headings at ## and deeper levels in-place.
Heading number formats follow the project convention:

    ## N. Title           — top-level (##): single number with trailing period
    ### N.M Title         — second level (###): dotted pair, no trailing period
    #### N.M.O Title      — third level (####): dotted triple, no trailing period

A heading at ##, ###, or #### that does NOT carry a leading number is left
unchanged and a warning is emitted to stderr — this is likely an authoring
error (e.g. an appendix heading accidentally placed at ## level without a
number).

Headings inside fenced code blocks and HTML comments are skipped entirely
(see the shared ``_markdown_shared`` module, which must live alongside
this script).

When the resulting content is byte-identical to the input the file is not
rewritten and its mtime is preserved (safe to wire into Makefile dependency
chains; a no-op run prints "no changes" to stdout).

Flags:
    --start N   First top-level section number (default: 1).
    --check     Exit non-zero if the file needs renumbering; do not write.
    --dry-run   Print changed lines to stdout; do not write.
"""

import argparse
import re
import sys
from pathlib import Path

import _markdown_shared


# Matches a numbered heading at ##, ###, or ####.
# Number formats accepted:
#   "N."     — top-level (##), e.g. "1."
#   "N.M"    — second level (###), e.g. "1.2"
#   "N.M.O"  — third level (####), e.g. "1.2.3"
# Requires at least one period so bare tokens like "2024" do not match.
_NUMBERED_RE = re.compile(
    r"^(#{2,4})"                          # hash prefix: ##, ###, or ####
    r"\s+"
    r"(\d+\.(?:\d+(?:\.\d+)*)?)"          # number: N. or N.M or N.M.O
    r"\s+"
    r"(.+)$"                              # title text (non-empty)
)

# Detects any ## / ### / #### heading with non-empty content (for warning).
_HEADING_RE = re.compile(r"^(#{2,4})\s+\S")

_MAX_DEPTH = 3  # supports ## (depth 0), ### (depth 1), #### (depth 2)


def _warn(msg: str) -> None:
    print(f"warning: {msg}", file=sys.stderr)


def _build_prefix(stack: list[int], depth: int) -> str:
    """Return the formatted number prefix for a heading at `depth`.

    depth 0  →  "N."    (trailing period, matching ## convention)
    depth 1  →  "N.M"   (no trailing period)
    depth 2  →  "N.M.O" (no trailing period)
    """
    parts = stack[: depth + 1]
    if depth == 0:
        return f"{parts[0]}."
    return ".".join(str(p) for p in parts)


def renumber_lines(
    lines: list[str], start: int = 1
) -> tuple[list[str], list[str]]:
    """Return (new_lines, warnings).

    Renumbers all numbered headings outside fenced code blocks and HTML
    comments (see ``_markdown_shared.real_line_flags``) — a
    heading-shaped line inside either is left completely untouched: not
    renumbered, and not warned about even if it looks unnumbered.
    Unnumbered ## / ### / #### headings are left unchanged and produce a
    human-readable warning string in the returned warnings list.
    """
    stack: list[int] = [start - 1] + [0] * (_MAX_DEPTH - 1)
    result: list[str] = []
    warnings: list[str] = []

    stripped = [line.rstrip("\n") for line in lines]
    real = _markdown_shared.real_line_flags(stripped)

    for lineno, (line, raw, is_real) in enumerate(zip(lines, stripped, real), start=1):
        # Preserve the original line ending (handles files with/without
        # trailing newlines without introducing spurious diffs).
        ending = line[len(raw):]

        if not is_real:
            result.append(line)
            continue

        m = _NUMBERED_RE.match(raw)
        if m:
            hashes, _old_num, title = m.group(1), m.group(2), m.group(3)
            depth = len(hashes) - 2      # ## → 0, ### → 1, #### → 2
            stack[depth] += 1
            for i in range(depth + 1, _MAX_DEPTH):
                stack[i] = 0
            prefix = _build_prefix(stack, depth)
            result.append(f"{hashes} {prefix} {title}{ending}")
            continue

        if _HEADING_RE.match(raw):
            level = len(raw) - len(raw.lstrip("#"))
            level_str = "#" * level
            warnings.append(
                f"line {lineno}: unnumbered {level_str} heading: {raw!r}"
            )

        result.append(line)

    return result, warnings


def process(
    path: Path, *, start: int, check: bool, dry_run: bool
) -> int:
    """Process one file. Returns an exit code (0 = ok, 1 = needs renumbering)."""
    old_text = path.read_text()
    lines = old_text.splitlines(keepends=True)

    new_lines, warnings = renumber_lines(lines, start=start)
    new_text = "".join(new_lines)

    for w in warnings:
        _warn(w)

    if new_text == old_text:
        print(f"{path}: no changes")
        return 0

    if check:
        print(f"{path}: needs renumbering", file=sys.stderr)
        return 1

    if dry_run:
        for i, (old, new) in enumerate(
            zip(old_text.splitlines(keepends=True), new_lines)
        ):
            if old != new:
                print(f"line {i + 1}: {old.rstrip()!r} → {new.rstrip()!r}")
        return 0

    # The equality check above already gates --check/--dry-run before
    # this point is reached, so new_text is known to differ here —
    # write_if_changed is still used for consistency with the other
    # markdown-*.py scripts' write path.
    _markdown_shared.write_if_changed(path, new_text, old_text)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Renumber section headings in a markdown file."
    )
    ap.add_argument("file", type=Path, help="markdown file to process in place")
    ap.add_argument(
        "--start",
        type=int,
        default=1,
        metavar="N",
        help="first top-level section number (default: 1)",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if file needs renumbering; do not write",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="print changed lines to stdout; do not write",
    )
    args = ap.parse_args()

    if not args.file.exists():
        print(f"error: {args.file}: file not found", file=sys.stderr)
        sys.exit(1)

    sys.exit(process(args.file, start=args.start, check=args.check, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
