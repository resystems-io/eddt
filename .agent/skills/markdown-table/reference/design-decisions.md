# Design Decisions

## No per-table skip control

**Decision:** The formatter operates on all tables in a file.
There is no flag or marker to skip specific tables.

**Rationale:** The content verification guard already skips
tables that cannot be safely reformatted (e.g. cells containing
unescaped pipes that confuse the parser). For all other tables,
formatting is always desirable — there is no known case where a
safely parseable table should intentionally remain misaligned.

**Future option:** If a need arises, add a comment marker:

```markdown
<!-- markdown-table:skip -->
| This | table | stays | as-is |
```

Both `check.py` and `format.py` would look for this marker on
the line immediately preceding a table block and skip it. This
avoids adding CLI flags or line-number arguments that would be
fragile across file edits.

## Escaped pipes cause table skip

**Decision:** When `split_cells` encounters a table where the
parsed cell count differs between rows (because an escaped pipe
`\|` was mishandled or the table has genuinely inconsistent
columns), the formatter skips the table and prints a warning.

**Rationale:** Silent corruption is worse than a skipped table.
The warning alerts the operator to manually inspect the table.
The checker still reports the table as misaligned so it remains
visible.

## Separator width matches data cells

**Decision:** The separator row's content between pipes is
exactly `column_width + 2` characters (matching the
` content ` padding in data cells). Alignment markers (`:`)
consume one character of this budget.

**Rationale:** This ensures pipe positions are identical between
the separator and data rows. Earlier attempts that computed
separator width independently produced off-by-one misalignments
that were difficult to diagnose.
