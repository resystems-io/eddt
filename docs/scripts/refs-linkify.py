#!/usr/bin/env python3
"""refs-linkify.py — unified linkify for reify traceability identifiers.

Replaces ddrs-linkify.py, reqs-linkify.py, and needs-linkify.py.

Resolves definitions via five strategies (in detection order; multiple
matches for the same identifier across tiers is a fatal authoring
error):

  1. Checklist item       `- [ ] **X-nn: Title**` / `- [x] **X-nn: ...**`
                          The script auto-inserts `<a id="x-nn"></a>`
                          after the checkbox if absent.
  2. Bolded list entry    `* **X-nn: Title**` / `- **X-nn [Title]: ...**`
                          The script auto-inserts the anchor after the
                          bullet if absent.
  3. Section header       `#### X-nn: Title` / `#### X-nn — Title`
                          Anchor-targeted; `<a id>` auto-inserted.
                          Heading wins over same-file table (tier 4)
                          for the same identifier.
  4. Table column         `| X-nn | ... |`     (first column of a row)
                          The script auto-inserts the anchor at the
                          start of the cell if absent.
  5. Anchor (fallback)    `<a id="x-nn"></a>` anywhere on a line that
                          has not already matched a higher tier.
                          Author-written; not auto-inserted.

Identifier shape: `[A-Z]-(?:[A-Z]{2,4}-)?\\d{2,3}` — supports
`X-nn`, `X-nnn`, `X-AB-nn`, `X-ABC-nnn`, `X-ABCD-nnn` shapes.

Reference rewrites: every body-text occurrence of the identifier
(outside fenced code blocks, outside HTML comments, outside link defs,
and on lines that are not themselves definition sites) is rewritten to
`[X-nn][x-nn]`. The reference-link-definition block at the foot of
the file is regenerated each run, bracketed by start/end markers
so it composes with adjacent generated blocks (markdown-toc,
markdown-index).

Definition scanning, reference rewriting, and used-identifier collection
all skip fenced code blocks and HTML comments (see the shared
``_markdown_shared`` module, which must live alongside this script) —
a definition- or identifier-shaped line inside either is never treated
as real.

Idempotent. Run cleanly multiple times against the same file.

Usage:
    refs-linkify.py [--refinements PATH] FILE.md
    refs-linkify.py --self-test
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import _markdown_shared


# ---------- Identifier shape ------------------------------------------------

# Bare identifier in upper-case body text:
#   X-nn        e.g. D-22, A-01
#   X-nnn       e.g. D-100
#   X-AB-nn     e.g. C-DG-01, N-DG-003
#   X-ABC-nn    e.g. R-PKG-01, R-RES-04
#   X-ABCD-nnn  e.g. N-EDDT-001
ID = r"[A-Z]-(?:[A-Z]{2,4}-)?\d{2,3}"

# Anchor / link-def slug form (lowercase, hyphenated).
ID_LOWER = r"[a-z]-(?:[a-z]{2,4}-)?\d{2,3}"

ID_RE = re.compile(rf"\b({ID})\b")

# An anchor that the script may have inserted.
ANCHOR_RE = re.compile(rf'<a\s+id="({ID_LOWER})"\s*></a>')

# Tier 1 — checklist item.
TIER1_RE = re.compile(
    r"^\s*[-*+]\s+\[[ xX]\]\s+"
    rf"(?:<a\s+id=\"[a-z0-9-]+\"></a>\s*)?"
    r"(?:\*\*)?(?:\[)?"
    rf"({ID})\b"
)

# Tier 2 — bolded list entry (no checkbox).
# `**` is required so generic bullets do not accidentally match.
TIER2_RE = re.compile(
    r"^\s*[-*+]\s+"
    rf"(?:<a\s+id=\"[a-z0-9-]+\"></a>\s*)?"
    r"\*\*(?:\[)?"
    rf"({ID})\b"
)

# Tier 3 — section header (colon or em-dash separator).
# Matches:
#   #### ID: Title
#   #### ID — Title
#   #### <a id="x-nn"></a>ID — Title    (anchor already present)
#   #### <a id="x-nn"></a>`ID` — Title  (backtick-wrapped id, not-yet-cleaned)
# Em-dash (U+2014) with or without surrounding spaces is accepted.
TIER3_RE = re.compile(
    r"^#{1,6}\s+"
    r"(?:<a\s+id=\"[a-z0-9-]+\"></a>\s*)?"   # optional existing anchor
    r"(?:`)?(?:\[)?"                           # optional backtick or [
    rf"({ID})"
    r"(?:\])?(?:\[[a-z0-9-]+\])?(?:`)?"       # optional ], linkref, backtick
    r"(?:\s*:\s+|\s*—\s*)"               # colon separator OR em-dash
    r"(.+)$"                                   # title (not captured for logic, kept for completeness)
)

# Tier 4 — table cell, first column.
#
# A definition is one of:
#   (a) `| <a id="x-yy"></a>[X-NN]…`  (anchor present; post-script-run shape)
#   (b) `| X-NN …`                     (bare ID with no preceding bracket; pre-script shape)
#
# A cell whose first content is a linked reference like `[R-RES-01][r-res-01]`
# **without** a preceding anchor is a reference, not a definition — the
# author has put a cross-reference in the cell, not a new identifier.
TIER4_RE = re.compile(
    r"^\s*\|\s*"
    rf"(?:"
    rf"<a\s+id=\"[a-z0-9-]+\"></a>\s*(?:\[)?({ID})\b"   # anchor + (optional bracket) + ID
    rf"|"
    rf"({ID})\b"                                          # bare ID, no preceding `[`
    rf")"
)

# A reference-link-definition line (`[x-nn]: …`).
LINKDEF_RE = re.compile(rf"^\[{ID_LOWER}\]:\s")

# Top-level heading. Single `#` followed by whitespace then non-whitespace
# content. H1 lines are excluded from the linkify rewrite entirely —
# rewriting an identifier inside the document title would inject markdown
# link syntax into HTML <title> and PDF metadata extraction.
H1_RE = re.compile(r"^#\s+\S")

# Inline-code span. Matches double-backtick or single-backtick spans.
# Used to split a line into code/non-code segments so identifiers inside
# backtick spans are never rewritten or collected as references.
INLINE_CODE_RE = re.compile(r"(``[^\n]*?``|`[^`\n]*?`)")


# ---------- Generated-block markers -----------------------------------------

GENERATED_START = "<!-- Reference links generated by scripts/refs-linkify.py -->"
GENERATED_END = "<!-- /Reference links -->"

# Legacy markers from the predecessor scripts. Found-and-removed on the
# first run after the consolidation; the generated-block boundaries below
# subsume them.
LEGACY_BLOCK_PATTERNS = [
    (
        "<!-- DDR reference links generated by scripts/ddrs-linkify.py -->",
        "<!-- /DDR reference links -->",
    ),
    (
        "<!-- Requirement reference links generated by scripts/reqs-linkify.py -->",
        "<!-- /Requirement reference links -->",
    ),
    (
        "<!-- Need reference links generated by scripts/needs-linkify.py -->",
        "<!-- /Need reference links -->",
    ),
]


# ---------- Helpers ---------------------------------------------------------

def python_markdown_slug(heading_text: str) -> str:
    """Replicate Python-Markdown's TOC default slugifier.

    NFKD-normalise, strip non-ASCII, drop punctuation except whitespace
    and hyphens, lowercase, collapse runs of whitespace/hyphens.
    """
    value = unicodedata.normalize("NFKD", heading_text)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[-\s]+", "-", value)


def lower_anchor(id_str: str) -> str:
    """X-NN -> x-nn, X-ABC-nn -> x-abc-nn."""
    return id_str.lower()


def sort_key(id_str: str) -> tuple:
    """Sort by class prefix then by remaining components.

    Class order is alphabetical by upper-case prefix; within a class,
    shorter forms (X-nn) sort before X-ABC-nn forms; within a form,
    numeric-aware on the trailing digits.
    """
    parts = id_str.split("-")
    head = parts[0]
    if len(parts) == 2:
        return (head, "", int(parts[1]))
    # X-ABC-nnn or X-ABCD-nnn
    return (head, parts[1], int(parts[2]))


# ---------- Definition discovery --------------------------------------------

class Definition:
    """A located definition site for one identifier."""

    __slots__ = ("id", "tier", "target", "line_no", "needs_anchor", "index_path")

    def __init__(self, id_str: str, tier: int, target: str, line_no: int,
                 needs_anchor: bool, index_path: Path | None = None):
        self.id = id_str
        self.tier = tier
        self.target = target  # the slug fragment after '#' in a link def
        self.line_no = line_no
        self.needs_anchor = needs_anchor
        # The index file this definition was found in. None during
        # initial scanning; populated by `find_definitions_in_indexes`.
        self.index_path = index_path


def find_definitions(text: str) -> tuple[dict[str, Definition], list[tuple[str, list[Definition]]]]:
    """Scan `text` and return (definitions, duplicates).

    `definitions` maps each canonical identifier to the single
    Definition that the script will use.

    `duplicates` lists identifiers that matched more than once across
    any tiers — the caller should fail with a duplicate-definition
    error and report all locations.

    Skips fenced code blocks and HTML comments (see
    ``_markdown_shared.real_line_flags``) — otherwise a
    heading/checklist/table definition-shaped line inside either would be
    registered as a real definition.
    """
    found: dict[str, list[Definition]] = {}
    lines = text.splitlines()
    real = _markdown_shared.real_line_flags(lines)

    for line_no, (line, is_real) in enumerate(zip(lines, real), start=1):
        if not is_real:
            continue
        if LINKDEF_RE.match(line):
            continue

        # Tier 1 — checklist (highest priority structural marker).
        m = TIER1_RE.match(line)
        if m:
            id_str = m.group(1)
            d = Definition(id_str, 1, lower_anchor(id_str), line_no, needs_anchor=True)
            found.setdefault(id_str, []).append(d)
            continue

        # Tier 2 — bolded list entry (no checkbox).
        m = TIER2_RE.match(line)
        if m:
            id_str = m.group(1)
            d = Definition(id_str, 2, lower_anchor(id_str), line_no, needs_anchor=True)
            found.setdefault(id_str, []).append(d)
            continue

        # Tier 3 — section header (colon or em-dash).
        # Anchor-targeted: the link-def points at the auto-inserted
        # `<a id="x-nn"></a>` anchor, not a python-markdown slug.
        m = TIER3_RE.match(line)
        if m:
            id_str = m.group(1)
            d = Definition(id_str, 3, lower_anchor(id_str), line_no, needs_anchor=True)
            found.setdefault(id_str, []).append(d)
            continue

        # Tier 4 — table cell, first column.
        m = TIER4_RE.match(line)
        if m:
            id_str = m.group(1) or m.group(2)
            d = Definition(id_str, 4, lower_anchor(id_str), line_no, needs_anchor=True)
            found.setdefault(id_str, []).append(d)
            continue

        # Tier 5 — bare anchor not adjacent to a higher tier.
        for am in ANCHOR_RE.finditer(line):
            anchor = am.group(1)
            id_upper = anchor.upper()
            # Validate that the anchor's slug round-trips an ID exactly.
            if not re.fullmatch(ID, id_upper):
                continue
            d = Definition(id_upper, 5, anchor, line_no, needs_anchor=False)
            found.setdefault(id_upper, []).append(d)

    # Deduplicate within tier-5 hits on the same line: a single anchor
    # detected once is the canonical case.
    for id_str, defs in found.items():
        # Dedup tier-5 entries on the same line (the same anchor reflowed by re.finditer).
        seen = set()
        unique = []
        for d in defs:
            key = (d.tier, d.line_no)
            if key in seen and d.tier == 5:
                continue
            seen.add(key)
            unique.append(d)
        found[id_str] = unique

    duplicates: list[tuple[str, list[Definition]]] = []
    chosen: dict[str, Definition] = {}
    for id_str, defs in found.items():
        if len(defs) > 1:
            # Heading wins: if exactly one tier-3 definition and all
            # others are tier-4 (table first-column), the heading is the
            # canonical definition and the table occurrences are treated
            # as references — no duplicate error.
            tier3 = [d for d in defs if d.tier == 3]
            tier4 = [d for d in defs if d.tier == 4]
            if len(tier3) == 1 and len(tier3) + len(tier4) == len(defs):
                chosen[id_str] = tier3[0]
                continue
            duplicates.append((id_str, defs))
            continue
        chosen[id_str] = defs[0]
    return chosen, duplicates


def find_definitions_in_indexes(
    index_paths: list[Path],
) -> tuple[dict[str, Definition], list[tuple[str, list[Definition]]]]:
    """Scan each index doc and merge definitions across them.

    A duplicate identifier — within one index, or across multiple
    indexes — is reported as a fatal authoring error and the caller
    surfaces all source-line locations.

    Each Definition's `index_path` records the file it came from so
    the generated reference-link block can target the correct doc.
    """
    merged: dict[str, list[Definition]] = {}
    for path in index_paths:
        text = path.read_text()
        per_doc, per_doc_dups = find_definitions(text)
        # Carry forward intra-doc duplicates verbatim.
        for id_str, defs in per_doc_dups:
            for d in defs:
                d.index_path = path
            merged.setdefault(id_str, []).extend(defs)
        for id_str, d in per_doc.items():
            d.index_path = path
            merged.setdefault(id_str, []).append(d)

    duplicates: list[tuple[str, list[Definition]]] = []
    chosen: dict[str, Definition] = {}
    for id_str, defs in merged.items():
        if len(defs) > 1:
            duplicates.append((id_str, defs))
            continue
        chosen[id_str] = defs[0]
    return chosen, duplicates


# ---------- Auto-anchor insertion ------------------------------------------

INSERT_TIER1_RE = re.compile(
    r"^(?P<lead>\s*[-*+]\s+\[[ xX]\]\s+)"
    r"(?P<rest>.+)$"
)

INSERT_TIER2_RE = re.compile(
    r"^(?P<lead>\s*[-*+]\s+)"
    r"(?P<rest>\*\*.+)$"
)

INSERT_TIER3_RE = re.compile(
    r"^(?P<lead>#{1,6}\s+)"
    r"(?P<rest>.+)$"
)

INSERT_TIER4_RE = re.compile(
    r"^(?P<lead>\s*\|\s*)"
    r"(?P<rest>.+)$"
)


def insert_anchor_in_line(line: str, tier: int, id_str: str) -> str:
    """Insert `<a id="x-nn"></a>` into the appropriate slot on `line`.

    No-op if an anchor is already present at the slot.
    """
    anchor = f'<a id="{lower_anchor(id_str)}"></a>'
    if anchor in line:
        return line

    if tier == 1:
        m = INSERT_TIER1_RE.match(line)
        if not m:
            return line
        return m.group("lead") + anchor + m.group("rest")
    if tier == 2:
        m = INSERT_TIER2_RE.match(line)
        if not m:
            return line
        return m.group("lead") + anchor + m.group("rest")
    if tier == 3:
        m = INSERT_TIER3_RE.match(line)
        if not m:
            return line
        return m.group("lead") + anchor + m.group("rest")
    if tier == 4:
        m = INSERT_TIER4_RE.match(line)
        if not m:
            return line
        return m.group("lead") + anchor + m.group("rest")
    return line


# ---------- Linkify body-text references ------------------------------------

def linkify_text(text: str, definitions: dict[str, Definition]) -> str:
    """Rewrite bare identifiers to `[X-nn][x-nn]` form, except on
    definition lines (whose lines are passed through unchanged at the
    body-rewrite stage; auto-anchor insertion is applied separately).

    Skips fenced code blocks, HTML comments (see
    ``_markdown_shared.real_line_flags``), and link-definition lines. A
    bare identifier inside a comment is left bare — rewriting it would
    inject link syntax into text that's supposed to be completely inert.

    A heading line that is itself a tier-3 definition for an
    identifier is NOT rewritten for that identifier (the heading is
    the canonical anchor; rewriting would point it at itself and
    break Python-Markdown's slug derivation). However, any *other*
    identifier appearing in the same heading IS linkified.
    """
    lines = text.splitlines(keepends=True)
    stripped_lines = [line.rstrip("\n") for line in lines]
    real = _markdown_shared.real_line_flags(stripped_lines)
    out: list[str] = []

    for line, is_real in zip(lines, real):
        if not is_real:
            out.append(line)
            continue
        if LINKDEF_RE.match(line.rstrip("\n")):
            out.append(line)
            continue
        if H1_RE.match(line):
            # H1 is the document title — rewriting an identifier inside
            # it injects markdown link syntax into HTML <title> and PDF
            # title metadata extraction and should be skipped.
            out.append(line)
            continue

        # Identify whether this line is a tier-3 definition site, and
        # if so for which identifier.
        heading_def_id = None
        m = TIER3_RE.match(line.rstrip("\n"))
        if m:
            heading_def_id = m.group(1)

        def replace(m: re.Match) -> str:
            id_str = m.group(0)
            # Heading defining this very identifier — leave alone.
            if id_str == heading_def_id:
                return id_str
            # Reference to an unknown identifier — leave bare; the
            # caller emits a warning.
            if id_str not in definitions:
                return id_str
            return f"[{id_str}][{lower_anchor(id_str)}]"

        # Split on inline-code spans; only rewrite the non-code segments.
        # Odd-indexed segments (captured groups) are backtick spans and
        # are preserved verbatim; even-indexed segments are plain text.
        segments = INLINE_CODE_RE.split(line)
        rewritten = []
        for seg_i, seg in enumerate(segments):
            if seg_i % 2 == 1:  # captured backtick span — preserve verbatim
                rewritten.append(seg)
            else:
                rewritten.append(re.sub(
                    rf'(?<![\[\w])({ID})(?![\]\w])(?![^<>]*"></a>)',
                    replace,
                    seg,
                ))
        line = "".join(rewritten)
        out.append(line)
    return "".join(out)


# ---------- Auto-anchor insertion across the whole file ---------------------

def insert_definition_anchors(text: str, definitions: dict[str, Definition]) -> str:
    """Walk the text and ensure every tier-1/2/4 definition site
    carries the `<a id>` anchor.
    """
    by_line: dict[int, Definition] = {
        d.line_no: d for d in definitions.values() if d.needs_anchor
    }
    if not by_line:
        return text

    lines = text.splitlines(keepends=True)
    for i, line in enumerate(lines, start=1):
        d = by_line.get(i)
        if d is None:
            continue
        new = insert_anchor_in_line(line.rstrip("\n"), d.tier, d.id)
        # Preserve original trailing newline.
        if line.endswith("\n"):
            new += "\n"
        lines[i - 1] = new
    return "".join(lines)


# ---------- Reference block emission ----------------------------------------

def collect_used_identifiers(text: str) -> set[str]:
    """Return the set of identifiers referenced in body text.

    Skips fenced code blocks and HTML comments (see
    ``_markdown_shared.real_line_flags``) — an identifier appearing
    only inside a comment isn't a real reference and shouldn't cause a
    reference-link definition to be emitted for it.
    """
    used: set[str] = set()
    lines = text.splitlines()
    for line, is_real in zip(lines, _markdown_shared.real_line_flags(lines)):
        if not is_real:
            continue
        if LINKDEF_RE.match(line):
            continue
        # Skip identifiers inside inline-code spans; only collect from
        # plain-text segments (even-indexed after splitting on backticks).
        for seg in INLINE_CODE_RE.split(line)[::2]:
            for m in ID_RE.finditer(seg):
                used.add(m.group(1))
    return used


def emit_reference_block(
    used: set[str],
    definitions: dict[str, Definition],
    target_path: Path,
    fallback_index_name: str,
) -> str:
    """Build the generated reference block.

    Each ID's link-def points at the index file that defined it;
    when the target file is the same as the defining index, the
    same-file `#anchor` form is used.

    Identifiers without a corresponding definition produce a link-def
    with an empty fragment (`#`) so the warning is loud but the file
    remains parseable. The caller surfaces the warning on stderr.
    """
    lines: list[str] = [GENERATED_START, ""]
    for id_str in sorted(used, key=sort_key):
        d = definitions.get(id_str)
        if d is None:
            href = f"{fallback_index_name}#"
        else:
            same_file = (d.index_path is not None
                         and d.index_path.resolve() == target_path.resolve())
            anchor_or_slug = d.target
            if same_file:
                href = f"#{anchor_or_slug}"
            else:
                import os
                if d.index_path:
                    index_name = os.path.relpath(d.index_path, target_path.parent)
                else:
                    index_name = fallback_index_name
                href = f"{index_name}#{anchor_or_slug}"
        lines.append(f"[{lower_anchor(id_str)}]: {href}")
    lines.append("")
    lines.append(GENERATED_END)
    lines.append("")
    return "\n".join(lines)


REF_DEF_RE = re.compile(rf"^\s*\[{ID_LOWER}\]:\s")


def locate_existing_block(text: str) -> tuple[str, str, bool]:
    """Return (before, after, found).

    Recognises the unified GENERATED_START/END pair plus the three
    legacy per-class block forms left over from the predecessor
    scripts.  Multiple legacy blocks present at once are stripped on
    the same run.
    """
    # First sweep: legacy blocks.
    out = text
    found_legacy = False
    for start_marker, end_marker in LEGACY_BLOCK_PATTERNS:
        before, after, ok = _strip_block(out, start_marker, end_marker)
        if ok:
            out = before.rstrip() + ("\n\n" + after.lstrip() if after.lstrip() else "\n")
            found_legacy = True

    # Now the unified block.
    before, after, ok = _strip_block(out, GENERATED_START, GENERATED_END)
    if ok:
        return before, after, True
    if found_legacy:
        return out, "", True
    return out, "", False


def _strip_block(text: str, start: str, end: str) -> tuple[str, str, bool]:
    """Split `text` around a start/end-marked block.

    Delegates the well-formed (start-and-end-found) case to the shared
    ``_markdown_shared.strip_marked_block``. If the start marker is
    present but no end marker follows it, applies this script's own
    shape-based recovery (consume trailing blank + reference-definition
    lines) — a policy specific to refs-linkify's legacy block formats,
    not shared with the other markdown-*.py scripts.
    """
    before, after, found = _markdown_shared.strip_marked_block(text, start, end)
    if found:
        return before, after, True

    lines = text.splitlines(keepends=True)
    start_idx = next(
        (i for i, ln in enumerate(lines) if ln.rstrip() == start),
        None,
    )
    if start_idx is None:
        return text, "", False
    # Shape-based fallback: consume blank + ref-def lines.
    end_idx = start_idx + 1
    while end_idx < len(lines) and (
        lines[end_idx].strip() == "" or REF_DEF_RE.match(lines[end_idx])
    ):
        end_idx += 1
    return "".join(lines[:start_idx]), "".join(lines[end_idx:]), True


# ---------- main -----------------------------------------------------------

DEFAULT_REFINEMENTS = "reify-refinements.md"


def resolve_index_paths(target: Path, explicit: list[str] | None) -> list[Path]:
    """Return the list of index docs to scan for definitions.

    If `explicit` is non-empty, those paths are used (each made
    absolute). Otherwise auto-discover a single `reify-refinements.md`
    next to the target or one directory up.
    """
    if explicit:
        return [Path(p).resolve() for p in explicit]
    for candidate in (target.parent / DEFAULT_REFINEMENTS,
                      target.parent.parent / DEFAULT_REFINEMENTS):
        if candidate.exists():
            return [candidate.resolve()]
    return [target.parent / DEFAULT_REFINEMENTS]


def process_file(
    target: Path,
    indexes: list[Path],
) -> bool:
    """Process target file. Returns True on no change, False if rewritten.

    Raises SystemExit on fatal errors.
    """
    definitions, duplicates = find_definitions_in_indexes(indexes)
    if duplicates:
        print("refs-linkify: fatal — duplicate definitions:", file=sys.stderr)
        for id_str, defs in duplicates:
            for d in defs:
                where = d.index_path.name if d.index_path else "<unknown>"
                print(f"  {id_str}: tier {d.tier} at {where}:{d.line_no}", file=sys.stderr)
        sys.exit(1)

    text = target.read_text()
    target_resolved = target.resolve()

    # If the target IS one of the index docs, auto-insert anchors for
    # any tier-1/2/4 definition sites that this index defines (only
    # the ones whose `index_path` points at this target).
    is_index = any(p.resolve() == target_resolved for p in indexes)
    if is_index:
        own_defs = {
            id_str: d for id_str, d in definitions.items()
            if d.index_path is not None and d.index_path.resolve() == target_resolved
        }
        text_after_anchors = insert_definition_anchors(text, own_defs)
    else:
        text_after_anchors = text

    text_after_linkify = linkify_text(text_after_anchors, definitions)
    used = collect_used_identifiers(text_after_linkify)

    # Pick a fallback index name for unresolved references.
    fallback_index_name = indexes[0].name if indexes else DEFAULT_REFINEMENTS

    if not used:
        # Still strip any legacy block.
        before, after, ok = locate_existing_block(text_after_linkify)
        if ok:
            new_text = (before.rstrip() + "\n" + after.lstrip()).rstrip() + "\n"
            if _markdown_shared.write_if_changed(target, new_text, text):
                print(f"  REFS {target} (no references; legacy block stripped)")
                return False
        if _markdown_shared.write_if_changed(target, text_after_linkify, text):
            return False
        print(f"  REFS {target} (no references; no changes)")
        return True

    unknown = sorted(u for u in used if u not in definitions)
    for u in unknown:
        print(f"  WARN: {target.name} references unknown identifier {u}", file=sys.stderr)

    block = emit_reference_block(used, definitions, target, fallback_index_name)
    before, after, ok = locate_existing_block(text_after_linkify)

    if ok:
        new_text = before.rstrip() + "\n\n" + block
        trailing = after.lstrip()
        if trailing:
            new_text = new_text.rstrip() + "\n\n" + trailing
    else:
        new_text = text_after_linkify.rstrip() + "\n\n" + block

    new_text = new_text.rstrip() + "\n"

    n_def = sum(1 for u in used if u in definitions)
    n_unk = len(unknown)
    if _markdown_shared.write_if_changed(target, new_text, text):
        print(
            f"  REFS {target} ({n_def} resolved, {n_unk} unknown, "
            f"{len(definitions)} in index)"
        )
        return False
    print(
        f"  REFS {target} (no changes, {n_def} resolved, {n_unk} unknown, "
        f"{len(definitions)} in index)"
    )
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Linkify reify traceability identifiers (D / A / C / N / V / R-XXX-NN).",
    )
    parser.add_argument("input", help="Markdown file to process.")
    # `--refinements`, `--index`, `--indexes` are aliases. Repeatable;
    # each invocation appends one path. The Makefile is the place to
    # enumerate the project's index docs.
    parser.add_argument(
        "--refinements", "--index", "--indexes",
        action="append",
        dest="indexes",
        default=None,
        help=(
            "Path to an index doc that hosts canonical definitions. "
            "Repeatable. Default: auto-discover "
            f"{DEFAULT_REFINEMENTS} next to the target or one directory up."
        ),
    )
    args = parser.parse_args()

    target = Path(args.input).resolve()
    if not target.exists():
        print(f"refs-linkify: file not found: {target}", file=sys.stderr)
        sys.exit(1)

    indexes = resolve_index_paths(target, args.indexes)
    missing = [str(p) for p in indexes if not p.exists()]
    if missing:
        print(f"refs-linkify: index doc(s) not found: {', '.join(missing)}", file=sys.stderr)
        print("Specify with --refinements/--index/--indexes <path>", file=sys.stderr)
        sys.exit(1)

    process_file(target, indexes)


if __name__ == "__main__":
    main()
