#!/usr/bin/env python3
"""Check or format markdown tables for vertical pipe alignment.

Usage:
    ./markdown-table.py check  <file>...
    ./markdown-table.py format <file>...

Subcommands
-----------

``check``
    Report misaligned tables across the listed files. Exit status
    is 0 when every table is aligned, 1 when one or more files
    contain a misaligned table, and 2 on a usage error.

``format``
    Rewrite each file so every table's pipe characters align
    vertically. A table is rewritten only when the data cell
    contents would be preserved exactly; otherwise the original
    is left in place and a warning is printed. Exit status is 0
    when every reformatted table preserved content, 1 when one
    or more tables were skipped for content safety, and 2 on a
    usage error.

The two subcommands are paired: running ``format`` on a corpus
should always make ``check`` exit 0, unless one or more tables
were too unsafe to rewrite (in which case ``format`` prints a
warning and ``check`` continues to flag them).

Rules
-----

* All pipe characters in a table column align to the same column
  position so the table is readable in a plain text editor.
* Separator rows (``:---``, ``---:``, ``:---:``) are extended
  with dashes to match the column width; alignment markers
  (left ``:---``, right ``---:``, centre ``:---:``) are preserved.
* Data cells are padded with a single leading and trailing
  space, then left-justified to the column's maximum content
  width.

Edge cases handled
------------------

* **Fenced code blocks** (``` regions) are skipped entirely;
  pipe characters inside fences are content, not table syntax.
* **Escaped pipes** (``\\|``) in a cell are treated as literal
  content and do not act as cell delimiters.
* **Inline code spans** (`` `…` ``) inside a cell may contain
  literal pipes; these are recognised and not treated as cell
  delimiters. Backticks themselves can be escaped with a
  backslash. Multi-backtick spans are not specifically
  recognised — each unescaped backtick toggles the in-code-span
  state, which is correct for the dominant single-backtick
  case in table cells.
* **Tables whose cells cannot be parsed safely** are left in
  place with a warning rather than corrupted.

Origin
------

This script is the merged form of the ``check.py`` and
``format.py`` from the markdown-table skill. It is vendored
into ``docs/scripts/`` so the documentation build chain
(``docs/Makefile``) has no dependency on a user-installed
skill outside the repository.
"""

import argparse
import re
import sys


# ---------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------

def find_pipe_positions(line: str) -> list[int]:
    """Return the column positions of pipe characters that act as
    cell delimiters in ``line``.

    A pipe is a delimiter unless it is preceded by a backslash
    (``\\|``) or lies inside a backtick-delimited inline code
    span (`` `…` ``).
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


def split_cells(line: str) -> list[str]:
    """Split a table row into cells.

    Respects escaped pipes (``\\|``) and pipes inside
    backtick-delimited inline code spans. The leading and
    trailing pipes that bracket a row are stripped; cell content
    is returned with surrounding whitespace removed.
    """
    cells = []
    current = []
    i = 0
    chars = line.strip()
    in_code = False

    if chars.startswith('|'):
        i = 1

    while i < len(chars):
        c = chars[i]
        prev = chars[i - 1] if i > 0 else ''
        if c == '`' and prev != '\\':
            in_code = not in_code
            current.append(c)
        elif c == '|' and not in_code and prev != '\\':
            cells.append(''.join(current).strip())
            current = []
        else:
            current.append(c)
        i += 1

    # Anything after the last pipe is outside the table and is
    # discarded — markdown tables terminate at the last ``|``.
    return cells


def is_separator(cells: list[str]) -> bool:
    """Report whether ``cells`` is a separator row (every cell
    matches the ``:?-+:?`` pattern)."""
    return all(re.match(r'^:?-+:?$', c) for c in cells)


def detect_alignments(cells: list[str]) -> list[str]:
    """Return the per-column alignment from a separator row's
    cells: one of ``left``, ``right``, ``center``, ``none``."""
    alignments = []
    for c in cells:
        if c.startswith(':') and c.endswith(':'):
            alignments.append('center')
        elif c.startswith(':'):
            alignments.append('left')
        elif c.endswith(':'):
            alignments.append('right')
        else:
            alignments.append('none')
    return alignments


# ---------------------------------------------------------------
# Check
# ---------------------------------------------------------------

def check_file(path: str) -> list[str]:
    """Check every table in ``path``. Return a list of error
    messages (empty when every table is aligned)."""
    with open(path) as f:
        lines = f.readlines()

    errors = []
    i = 0
    in_fence = False

    while i < len(lines):
        line = lines[i].rstrip('\n')

        if re.match(r'^```', line):
            in_fence = not in_fence
            i += 1
            continue

        if in_fence:
            i += 1
            continue

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


def cmd_check(args: argparse.Namespace) -> int:
    all_errors = []
    for path in args.files:
        all_errors.extend(check_file(path))
    for err in all_errors:
        print(err)
    return 1 if all_errors else 0


# ---------------------------------------------------------------
# Format
# ---------------------------------------------------------------

def format_table(
        table_lines: list[str]) -> tuple[list[str], bool]:
    """Reformat a table so pipes align vertically.

    Returns ``(formatted_lines, success)``. When ``success`` is
    ``False`` the table could not be reformatted safely (most
    commonly because rows have different cell counts) and the
    caller should preserve ``table_lines`` unchanged.
    """
    rows = []
    separator_idx = None
    alignments = []

    for i, line in enumerate(table_lines):
        cells = split_cells(line.rstrip('\n'))
        if is_separator(cells):
            separator_idx = i
            alignments = detect_alignments(cells)
        rows.append(cells)

    if not rows or separator_idx is None:
        return table_lines, False

    ncols = len(rows[0])

    for i, cells in enumerate(rows):
        if i == separator_idx:
            continue
        if len(cells) != ncols:
            return table_lines, False

    # Maximum content width per column, excluding the separator.
    col_widths = [0] * ncols
    for i, cells in enumerate(rows):
        if i == separator_idx:
            continue
        for j, cell in enumerate(cells):
            col_widths[j] = max(col_widths[j], len(cell))

    # Rebuild rows. Data cells get one space of padding either
    # side; the separator's dash run matches that total width
    # (content + 2 spaces).
    result = []
    for i, cells in enumerate(rows):
        if i == separator_idx:
            sep_cells = []
            for j in range(ncols):
                w = col_widths[j]
                align = (alignments[j]
                         if j < len(alignments) else 'none')
                total = w + 2
                if align == 'left':
                    sep_cells.append(':' + '-' * (total - 1))
                elif align == 'right':
                    sep_cells.append('-' * (total - 1) + ':')
                elif align == 'center':
                    sep_cells.append(
                        ':' + '-' * (total - 2) + ':')
                else:
                    sep_cells.append('-' * total)
            result.append('|' + '|'.join(sep_cells) + '|\n')
        else:
            parts = []
            for j, cell in enumerate(cells):
                w = col_widths[j]
                parts.append(' ' + cell.ljust(w) + ' ')
            result.append('|' + '|'.join(parts) + '|\n')

    # Safety check: data cell content must be preserved exactly.
    orig_data = [
        cells for i, cells in enumerate(rows)
        if i != separator_idx
    ]
    new_data = []
    for line in result:
        cells = split_cells(line.rstrip('\n'))
        if not is_separator(cells):
            new_data.append(cells)

    if orig_data != new_data:
        return table_lines, False

    return result, True


def format_file(path: str) -> tuple[bool, int, int]:
    """Reformat every table in ``path`` in-place.

    Returns ``(changed, formatted_count, skipped_count)``. The
    file is rewritten only when ``changed`` is true.
    """
    with open(path) as f:
        lines = f.readlines()

    output = []
    i = 0
    in_fence = False
    formatted = 0
    skipped = 0

    while i < len(lines):
        line = lines[i]

        if re.match(r'^```', line):
            in_fence = not in_fence
            output.append(line)
            i += 1
            continue

        if in_fence:
            output.append(line)
            i += 1
            continue

        if line.startswith('|'):
            table_block = []
            while i < len(lines) and lines[i].startswith('|'):
                table_block.append(lines[i])
                i += 1
            new_block, ok = format_table(table_block)
            if ok:
                output.extend(new_block)
                formatted += 1
            else:
                output.extend(table_block)
                skipped += 1
        else:
            output.append(line)
            i += 1

    changed = output != lines
    if changed:
        with open(path, 'w') as f:
            f.writelines(output)

    return changed, formatted, skipped


def cmd_format(args: argparse.Namespace) -> int:
    any_error = False
    for path in args.files:
        changed, formatted, skipped = format_file(path)
        if changed:
            print(
                f"  FORMATTED {path} ({formatted} table(s))")
        if skipped:
            print(
                f"  WARNING {path}: "
                f"{skipped} table(s) skipped "
                f"(content safety check failed)")
            any_error = True

    return 1 if any_error else 0


# ---------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check or format markdown tables for vertical "
            "pipe alignment."),
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_check = sub.add_parser(
        'check',
        help="report misaligned tables; exit 1 if any found")
    p_check.add_argument('files', nargs='+')
    p_check.set_defaults(func=cmd_check)

    p_format = sub.add_parser(
        'format',
        help="reformat tables in place; exit 1 if any skipped")
    p_format.add_argument('files', nargs='+')
    p_format.set_defaults(func=cmd_format)

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
