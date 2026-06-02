#!/usr/bin/env python3
"""Format markdown tables so pipe characters align vertically.

Usage: python3 format.py <file>...

Reformats tables in-place. Verifies data cell content is
preserved before writing — tables that cannot be safely
reformatted are skipped with a warning. Fenced code blocks
are left untouched. Escaped pipes (\\|) in cell content are
handled correctly.
"""

import re
import sys


def split_cells(line: str) -> list[str]:
    """Split a table row into cells, respecting escaped pipes and
    pipes inside inline code spans.

    A pipe character is a cell delimiter unless it is:

    * Preceded by a backslash (``\\|``) — the standard markdown
      escape for a literal pipe in a cell; or
    * Inside a backtick-delimited inline code span (``` `…` ```) —
      where pipes are literal content. Python-markdown renders
      such pipes correctly without an escape, so authors expect
      them to round-trip through this tooling unchanged.

    Backticks themselves can be escaped with a backslash (``\\```)
    to be treated as literal content rather than code-span
    delimiters. Multi-backtick spans (``` `` `` ``` and longer)
    are not specifically recognised; the boolean toggle treats
    each unescaped backtick as a state flip, which is correct for
    the single-backtick case that dominates table cells.

    Returns the cell contents (stripped of leading/trailing
    whitespace) excluding the outer empty cells created by the
    leading and trailing pipe characters.
    """
    cells = []
    current = []
    i = 0
    chars = line.strip()
    in_code = False  # inside a backtick-delimited inline code span

    # Skip leading pipe.
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

    # Don't append trailing content after the last pipe — it's
    # outside the table.
    return cells


def is_separator(cells: list[str]) -> bool:
    """Check if a row of cells is a separator row."""
    return all(re.match(r'^:?-+:?$', c) for c in cells)


def detect_alignments(cells: list[str]) -> list[str]:
    """Detect alignment markers from a separator row."""
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


def format_table(table_lines: list[str]) -> tuple[list[str], bool]:
    """Reformat a table for aligned pipes.

    Returns (formatted_lines, success). If success is False the
    table could not be safely reformatted and the original lines
    should be kept.
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

    # All rows must have the same number of cells.
    for i, cells in enumerate(rows):
        if i == separator_idx:
            continue
        if len(cells) != ncols:
            return table_lines, False

    # Compute max content width per column (excluding separator).
    col_widths = [0] * ncols
    for i, cells in enumerate(rows):
        if i == separator_idx:
            continue
        for j, cell in enumerate(cells):
            col_widths[j] = max(col_widths[j], len(cell))

    # Rebuild rows.
    result = []
    for i, cells in enumerate(rows):
        if i == separator_idx:
            sep_cells = []
            for j in range(ncols):
                w = col_widths[j]
                align = alignments[j] if j < len(alignments) \
                    else 'none'
                # Total fill between pipes = w + 2 (matching the
                # space-padded data cells: " content ").
                total = w + 2
                if align == 'left':
                    sep_cells.append(':' + '-' * (total - 1))
                elif align == 'right':
                    sep_cells.append('-' * (total - 1) + ':')
                elif align == 'center':
                    sep_cells.append(':' + '-' * (total - 2) + ':')
                else:
                    sep_cells.append('-' * total)
            result.append('|' + '|'.join(sep_cells) + '|\n')
        else:
            parts = []
            for j, cell in enumerate(cells):
                w = col_widths[j]
                parts.append(' ' + cell.ljust(w) + ' ')
            result.append('|' + '|'.join(parts) + '|\n')

    # Verify data content is preserved.
    orig_data = [
        cells for i, cells in enumerate(rows)
        if i != separator_idx
    ]
    new_data = []
    for i, line in enumerate(result):
        cells = split_cells(line.rstrip('\n'))
        if not is_separator(cells):
            new_data.append(cells)

    if orig_data != new_data:
        return table_lines, False

    return result, True


def format_file(path: str) -> tuple[bool, int, int]:
    """Format all tables in a file.

    Returns (changed, formatted_count, skipped_count).
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

        # Track fenced code blocks.
        if re.match(r'^```', line):
            in_fence = not in_fence
            output.append(line)
            i += 1
            continue

        if in_fence:
            output.append(line)
            i += 1
            continue

        # Detect table block.
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


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: format.py <file>...", file=sys.stderr)
        return 2

    any_error = False
    for path in sys.argv[1:]:
        changed, formatted, skipped = format_file(path)
        if changed:
            print(f"  FORMATTED {path} "
                  f"({formatted} table(s))")
        if skipped:
            print(f"  WARNING {path}: "
                  f"{skipped} table(s) skipped "
                  f"(content safety check failed)")
            any_error = True

    return 1 if any_error else 0


if __name__ == '__main__':
    sys.exit(main())
