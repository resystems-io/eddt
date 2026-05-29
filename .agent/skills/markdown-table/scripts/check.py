#!/usr/bin/env python3
"""Check markdown tables for vertical pipe alignment.

Usage: python3 check.py <file>...

Exits 0 if all tables are aligned, 1 if any are misaligned.
Skips fenced code blocks and handles escaped pipes (\\|) in
cell content.
"""

import re
import sys


def find_pipe_positions(line: str) -> list[int]:
    """Return the column positions of pipe characters that act as
    cell delimiters.

    A pipe is a delimiter unless it is:

    * Preceded by a backslash (``\\|``) — the standard markdown
      escape for a literal pipe in a cell; or
    * Inside a backtick-delimited inline code span (``` `…` ```) —
      where pipes are literal content. Python-markdown renders
      such pipes correctly without an escape, so the alignment
      check must not treat them as column separators.

    Backticks themselves can be escaped with a backslash (``\\```)
    to be treated as literal content. Multi-backtick spans are not
    specifically recognised; each unescaped backtick toggles the
    in-code-span state.
    """
    positions = []
    in_code = False
    i = 0
    while i < len(line):
        c = line[i]
        prev = line[i - 1] if i > 0 else ''
        if c == '`' and prev != '\\':
            in_code = not in_code
        elif c == '|' and not in_code and prev != '\\':
            positions.append(i)
        i += 1
    return positions


def check_file(path: str) -> list[str]:
    """Check a single file. Returns a list of error messages."""
    with open(path) as f:
        lines = f.readlines()

    errors = []
    i = 0
    in_fence = False

    while i < len(lines):
        line = lines[i].rstrip('\n')

        # Track fenced code blocks.
        if re.match(r'^```', line):
            in_fence = not in_fence
            i += 1
            continue

        if in_fence:
            i += 1
            continue

        # Detect table block.
        if line.startswith('|'):
            start = i
            table_pipes = []
            while i < len(lines):
                row = lines[i].rstrip('\n')
                if not row.startswith('|'):
                    break
                table_pipes.append(
                    (i + 1, find_pipe_positions(row)))
                i += 1

            # Check alignment.
            ref = table_pipes[0][1]
            for line_num, pipes in table_pipes[1:]:
                if pipes != ref:
                    errors.append(
                        f"{path}:{start + 1}-{i}: "
                        f"misaligned (line {line_num} "
                        f"differs from header)")
                    break
        else:
            i += 1

    return errors


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: check.py <file>...", file=sys.stderr)
        return 2

    all_errors = []
    for path in sys.argv[1:]:
        all_errors.extend(check_file(path))

    for err in all_errors:
        print(err)

    return 1 if all_errors else 0


if __name__ == '__main__':
    sys.exit(main())
