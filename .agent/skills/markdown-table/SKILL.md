# Markdown Table Formatting

Check and fix vertical alignment of pipe characters (`|`) in
markdown tables. All pipes in a table must align vertically so
the table is readable in a plain text editor. Separator rows
(`:---`, `---:`, `:---:`) must be extended with dashes to meet
the column width.

## When to Use

- Before committing any markdown file that contains tables.
- After editing table content (adding rows, changing cell text).
- As a bulk check across all `*.md` files in a documentation
  directory.

## Workflow

(Note, the skill scripts are executable can be be run directly, rather than
 using `python3` to run this. This ensure that permissions are granted to the
 script and not liberally to python3.)

1. **Check** for misaligned tables:

   ```bash
   <skill-dir>/scripts/check.py docs/*.md
   ```

   Exit 0 means all tables are aligned. Exit 1 reports which
   files and line ranges have misaligned pipes.

2. **Format** misaligned tables in-place:

   ```bash
   <skill-dir>/scripts/format.py docs/*.md
   ```

   The formatter verifies that data cell content is preserved
   before writing. If a table cannot be safely reformatted
   (e.g. cells contain unescaped pipes), it is skipped with a
   warning.

3. **Verify** with check again:

   ```bash
   <skill-dir>/scripts/check.py docs/*.md
   ```

## Edge Cases Handled

- **Escaped pipes** (`\|`) in cell content are not treated as
  column boundaries.
- **Fenced code blocks** (``` regions) are skipped entirely.
- **Alignment markers** (`:---`, `---:`, `:---:`) are preserved
  in separator rows.
- **Tables with unparseable content** are skipped with a warning
  rather than corrupted.

## Scripts

| Script             | Purpose                            | Exit Code         |
|:-------------------|:-----------------------------------|:------------------|
| `scripts/check.py` | Report misaligned tables           | 0 = OK, 1 = found |
| `scripts/format.py`| Fix alignment in-place             | 0 = OK, 1 = error |

## Tests

```bash
python3 -m pytest <skill-dir>/scripts/tests/ -v
```
