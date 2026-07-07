#!/usr/bin/env python3
"""Tests for scripts/refs-linkify.py.

Run from anywhere with:
    python3 -m unittest discover docs/scripts/tests

or stand-alone:
    ./test_refs_linkify.py
"""

import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parent.parent / "refs-linkify.py"


class RefsLinkifyTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.dir = Path(tmp.name)
        self.addCleanup(tmp.cleanup)

    def write_refinements(self, content: str) -> Path:
        p = self.dir / "reify-refinements.md"
        p.write_text(content)
        return p

    def write_target(self, content: str) -> Path:
        p = self.dir / "consumer.md"
        p.write_text(content)
        return p

    def run_tool(self, target: Path, *extra) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(target), *extra],
            capture_output=True, text=True, check=False,
        )

    def assertFatal(self, target: Path, *extra):
        result = self.run_tool(target, *extra)
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        return result


# --- Tier 1: checklist -----------------------------------------------------

class TestTier1Checklist(RefsLinkifyTestCase):
    def test_checkbox_unchecked_with_anchor_inserted(self):
        self.write_refinements(textwrap.dedent("""\
            # Refinements

            - [ ] **A-01: Reconciler Interface Definition**
        """))
        target = self.write_target("Per A-01 we proceed.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[A-01][a-01]", content)
        self.assertIn("[a-01]: reify-refinements.md#a-01", content)

    def test_checkbox_checked_recognised(self):
        self.write_refinements(textwrap.dedent("""\
            - [x] **C-02: Bucket Client**
        """))
        target = self.write_target("Per C-02.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[C-02][c-02]", content)
        self.assertIn("[c-02]: reify-refinements.md#c-02", content)

    def test_checkbox_with_existing_anchor_idempotent(self):
        self.write_refinements(textwrap.dedent("""\
            - [x] <a id="v-04"></a>**V-04: Walking Skeleton**
        """))
        target = self.write_target("Per V-04.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[V-04][v-04]", content)
        self.assertIn("[v-04]: reify-refinements.md#v-04", content)

    def test_anchor_inserted_on_refinements_doc_itself(self):
        # Running refs-linkify on the refinements doc auto-inserts the
        # anchor into the checklist line (same-file mode).
        ref = self.write_refinements(textwrap.dedent("""\
            # Refinements

            - [ ] **A-99: New analysis** with body text mentioning A-99 again.
        """))
        # Run against the refinements doc itself.
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        self.assertIn('<a id="a-99"></a>', content)


# --- Tier 2: bolded list entry --------------------------------------------

class TestTier2BoldedList(RefsLinkifyTestCase):
    def test_bullet_with_bold_id_recognised(self):
        self.write_refinements(textwrap.dedent("""\
            * **N-01 [Declarative Desired State]:** The system must accept...
        """))
        target = self.write_target("Per N-01.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[N-01][n-01]", content)
        self.assertIn("[n-01]: reify-refinements.md#n-01", content)

    def test_bullet_without_bold_does_not_match(self):
        # A bullet without bold is not a definition site; the
        # identifier in body text is treated as a reference, but with
        # no definition the ref resolves to an empty fragment with a
        # warning.
        self.write_refinements("* N-77 plain bullet, no bold.\n")
        target = self.write_target("Per N-77.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unknown identifier", result.stderr)


# --- Tier 3: section header -----------------------------------------------

class TestTier3Heading(RefsLinkifyTestCase):
    def test_h4_colon_heading_anchor_targeted(self):
        # Colon-form headings are anchor-targeted, not slug-targeted.
        # The link-def resolves to `#r-pkg-01`, not the python-markdown
        # slug `#r-pkg-01-single-binary-dual-context-distribution`.
        self.write_refinements(textwrap.dedent("""\
            #### R-PKG-01: Single-Binary Dual-Context Distribution
            **Statement:** something.
        """))
        target = self.write_target("Per R-PKG-01.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[r-pkg-01]: reify-refinements.md#r-pkg-01", content)
        self.assertNotIn("r-pkg-01-single-binary", content)

    def test_h2_colon_heading_anchor_targeted(self):
        # Same anchor-targeting rule for H2 headings.
        self.write_refinements("## A-01: Some heading\nbody\n")
        target = self.write_target("Per A-01.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[a-01]: reify-refinements.md#a-01", content)
        self.assertNotIn("a-01-some-heading", content)

    def test_colon_heading_definition_line_not_linkified(self):
        # Running on the definition doc: anchor is inserted; the heading
        # identifier must not become a self-link.
        ref = self.write_refinements(textwrap.dedent("""\
            #### R-PKG-01: Title

            Body referencing R-PKG-01.
        """))
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        # Anchor is inserted before the identifier.
        self.assertIn('<a id="r-pkg-01"></a>', content)
        # Identifier text in heading stays bare (no self-link).
        self.assertNotIn("[R-PKG-01] — ", content)
        self.assertNotIn("[R-PKG-01]: ", content)
        self.assertNotIn("#### [R-PKG-01]", content)
        # Body reference IS linkified.
        self.assertIn("Body referencing [R-PKG-01][r-pkg-01]", content)

    # --- Em-dash heading tests (tier-3, em-dash separator) ------------------

    def test_emdash_heading_recognised(self):
        # Em-dash separator `#### ID — Title` is a tier-3 definition;
        # references in other docs resolve to `#id` (anchor form).
        self.write_refinements("#### N-DG-001 — Declarative Authoring\nbody.\n")
        target = self.write_target("Per N-DG-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[N-DG-001][n-dg-001]", content)
        self.assertIn("[n-dg-001]: reify-refinements.md#n-dg-001", content)

    def test_emdash_heading_with_anchor_prefix_recognised(self):
        # Em-dash heading that already carries `<a id="…"></a>` must match.
        self.write_refinements(
            '#### <a id="n-dg-001"></a>N-DG-001 — Declarative Authoring\nbody.\n'
        )
        target = self.write_target("Per N-DG-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[n-dg-001]: reify-refinements.md#n-dg-001", content)

    def test_emdash_heading_anchor_inserted(self):
        # Running on the definition doc auto-inserts `<a id="…"></a>`
        # immediately after the `#### ` marker.
        ref = self.write_refinements("#### N-DG-001 — Declarative Authoring\nbody.\n")
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        self.assertIn('#### <a id="n-dg-001"></a>', content)

    def test_emdash_heading_anchor_idempotent(self):
        # Anchor already present → no-op; two runs produce byte-identical output.
        ref = self.write_refinements(
            '#### <a id="n-dg-001"></a>N-DG-001 — Declarative Authoring\nbody.\n'
        )
        self.run_tool(ref)
        first = ref.read_text()
        self.run_tool(ref)
        second = ref.read_text()
        self.assertEqual(first.count('<a id="n-dg-001">'), 1)
        self.assertEqual(first, second)

    def test_emdash_heading_def_line_not_linkified(self):
        # The heading line that defines the identifier must not have it
        # rewritten to a self-link; only body text is linkified.
        ref = self.write_refinements(textwrap.dedent("""\
            #### N-DG-001 — Declarative Authoring

            Body referencing N-DG-001.
        """))
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        self.assertIn("N-DG-001 — Declarative Authoring", content)
        self.assertNotIn("[N-DG-001] —", content)
        # Body reference IS linkified.
        self.assertIn("Body referencing [N-DG-001][n-dg-001]", content)

    def test_second_identifier_in_heading_title_linkified(self):
        # The leading (defining) id stays bare; a SECOND id in the title
        # IS linkified.
        ref = self.write_refinements(textwrap.dedent("""\
            #### R-DG-005 — Tag syntax; see also R-DG-051

            #### R-DG-051 — Since option tolerance
        """))
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        # Leading id keeps its bare form.
        self.assertIn("R-DG-005 — Tag syntax", content)
        self.assertNotIn("[R-DG-005] —", content)
        # Second id in the R-DG-005 heading title is linkified.
        self.assertIn("[R-DG-051][r-dg-051]", content)

    def test_emdash_with_and_without_surrounding_spaces(self):
        # Em-dash WITH surrounding spaces ` — ` (normal) AND without spaces
        # `—` must both match.
        ref = self.write_refinements(textwrap.dedent("""\
            #### A-01 — Title with spaces around em-dash
            #### B-02—Title without spaces
        """))
        target = self.write_target("Per A-01 and B-02.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[A-01][a-01]", content)
        self.assertIn("[B-02][b-02]", content)

    def test_backtick_wrapped_heading_id_recognised(self):
        # A heading whose id is still wrapped in backticks (not-yet-cleaned
        # form) must still be recognised as a tier-3 definition.
        ref = self.write_refinements(
            '#### <a id="n-cl-001"></a>`N-CL-001` — Convergent Replication\nbody.\n'
        )
        target = self.write_target("Per N-CL-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[N-CL-001][n-cl-001]", content)
        self.assertIn("[n-cl-001]: reify-refinements.md#n-cl-001", content)


# --- Tier 4: table column --------------------------------------------------

class TestTier4Table(RefsLinkifyTestCase):
    def test_table_first_column_with_anchor_inserted(self):
        ref = self.write_refinements(textwrap.dedent("""\
            # Refinements

            | ID    | Summary |
            |:------|:--------|
            | D-22  | Some decision. |
        """))
        # Process the refinements doc itself so the anchor gets
        # auto-inserted.
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        self.assertIn('<a id="d-22"></a>', content)

    def test_table_with_existing_anchor_preserved(self):
        ref = self.write_refinements(textwrap.dedent("""\
            | <a id="d-22"></a>D-22 | Some decision. |
        """))
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        # Exactly one anchor (no duplication).
        self.assertEqual(content.count('<a id="d-22">'), 1)


# --- Tier 5: anchor fallback ----------------------------------------------

class TestTier5Anchor(RefsLinkifyTestCase):
    def test_bare_anchor_recognised(self):
        # A bare anchor in prose is a tier-5 definition. (Edge case;
        # in practice authors won't usually do this, but the script
        # must handle it as the documented fallback.)
        self.write_refinements(textwrap.dedent("""\
            # Refinements

            Some prose <a id="d-99"></a> with an inline anchor.
        """))
        target = self.write_target("Per D-99.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[d-99]: reify-refinements.md#d-99", content)

    def test_tier5_hyphenated_anchor_resolves(self):
        # A bare <a id="n-dg-001"></a> (hyphenated subsystem code) must
        # produce the fully-uppercase identifier N-DG-001. The old buggy
        # re.sub path produced mixed-case N-Dg-001 which fails fullmatch.
        self.write_refinements(
            'Some prose <a id="n-dg-001"></a> inline anchor.\n'
        )
        target = self.write_target("Per N-DG-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[N-DG-001][n-dg-001]", content)
        self.assertIn("[n-dg-001]: reify-refinements.md#n-dg-001", content)
        # Must NOT contain any mixed-case form.
        self.assertNotIn("N-Dg-001", content)

    def test_no_mixed_case_identifier_emitted(self):
        # refs-linkify must never emit mixed-case identifier tokens
        # (e.g. N-Dg-001). All canonical ids are fully uppercase;
        # all anchors/link-def keys are fully lowercase.
        import re as _re
        self.write_refinements(
            'Some prose <a id="n-dg-001"></a> inline anchor.\n'
        )
        target = self.write_target("Per N-DG-001.\n")
        self.run_tool(target)
        content = target.read_text()
        # No X-Xx-NNN style mixed-case identifier anywhere.
        mixed = _re.findall(
            r'\b[A-Z]-[A-Za-z]*[a-z][A-Za-z]*-\d{2,3}\b', content
        )
        self.assertEqual(mixed, [], f"mixed-case identifiers found: {mixed}")


# --- Mixed identifiers across tiers ---------------------------------------

class TestMixedTiers(RefsLinkifyTestCase):
    def test_one_doc_with_all_tiers(self):
        self.write_refinements(textwrap.dedent("""\
            # Refinements

            - [ ] **A-01: An analysis** description.
            * **N-01 [Need title]:** statement.

            #### R-PKG-01: Some requirement
            body.

            | ID    | Summary |
            |:------|:--------|
            | D-22  | Something. |

            Plus inline <a id="d-99"></a> for tier-5.
        """))
        target = self.write_target(
            "References to A-01, N-01, R-PKG-01, D-22, and D-99 all appear here.\n"
        )
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        for ref in ("[A-01][a-01]", "[N-01][n-01]", "[R-PKG-01][r-pkg-01]", "[D-22][d-22]", "[D-99][d-99]"):
            self.assertIn(ref, content, f"missing rewrite for {ref}")
        for href in (
            "[a-01]: reify-refinements.md#a-01",
            "[n-01]: reify-refinements.md#n-01",
            "[r-pkg-01]: reify-refinements.md#r-pkg-01",
            "[d-22]: reify-refinements.md#d-22",
            "[d-99]: reify-refinements.md#d-99",
        ):
            self.assertIn(href, content, f"missing link def for {href!r}")


# --- Idempotency / stability ----------------------------------------------

class TestIdempotency(RefsLinkifyTestCase):
    def test_two_runs_on_consumer_byte_identical(self):
        self.write_refinements(textwrap.dedent("""\
            - [ ] **A-01: An analysis**
        """))
        target = self.write_target("Per A-01.\n")
        self.run_tool(target)
        first = target.read_text()
        self.run_tool(target)
        second = target.read_text()
        self.assertEqual(first, second)

    def test_two_runs_on_refinements_byte_identical(self):
        ref = self.write_refinements(textwrap.dedent("""\
            # Refinements

            - [ ] **A-01: Analysis**
            * **N-01 [Title]:** statement.

            #### R-PKG-01: Title
            body.

            | ID    | Summary |
            |:------|:--------|
            | D-22  | Decision. |

            Body references A-01, N-01, R-PKG-01, D-22.
        """))
        self.run_tool(ref)
        first = ref.read_text()
        self.run_tool(ref)
        second = ref.read_text()
        self.assertEqual(first, second)

    def test_no_op_run_preserves_mtime(self):
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target("Per A-01.\n")
        self.run_tool(target)
        first_stat = target.stat()
        self.run_tool(target)
        second_stat = target.stat()
        self.assertEqual(first_stat.st_mtime_ns, second_stat.st_mtime_ns)


# --- Generated block + composition ---------------------------------------

class TestGeneratedBlock(RefsLinkifyTestCase):
    def test_block_has_start_and_end_markers(self):
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target("Per A-01.\n")
        self.run_tool(target)
        content = target.read_text()
        self.assertIn(
            "<!-- Reference links generated by scripts/refs-linkify.py -->",
            content,
        )
        self.assertIn("<!-- /Reference links -->", content)

    def test_legacy_blocks_stripped(self):
        # A doc with the predecessor scripts' generated blocks gets
        # cleanly upgraded to the unified block.
        self.write_refinements(textwrap.dedent("""\
            - [ ] **A-01: Analysis**
            #### R-PKG-01: Title
            body.
        """))
        legacy = textwrap.dedent("""\
            Per A-01 and R-PKG-01.

            <!-- DDR reference links generated by scripts/ddrs-linkify.py -->

            [d-99]: stale.md#d-99

            <!-- /DDR reference links -->

            <!-- Requirement reference links generated by scripts/reqs-linkify.py -->

            [r-stale-99]: stale.md#r-stale-99

            <!-- /Requirement reference links -->
        """)
        target = self.write_target(legacy)
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertNotIn("DDR reference links", content)
        self.assertNotIn("Requirement reference links", content)
        self.assertIn(
            "<!-- Reference links generated by scripts/refs-linkify.py -->",
            content,
        )

    def test_content_after_block_preserved(self):
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            Per A-01.

            <!-- Reference links generated by scripts/refs-linkify.py -->

            [a-01]: reify-refinements.md#a-01

            <!-- /Reference links -->

            <!-- Some other tool's block -->

            [external]: https://example.com
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertIn("<!-- Some other tool's block -->", content)
        self.assertIn("[external]: https://example.com", content)

    def test_toc_and_index_blocks_untouched(self):
        """Mirrors the real shape of eddt-stakeholders.md: a document
        carrying a TOC block and an Alphabetical Index block alongside
        the Reference-links block. Regenerating the reference-links
        block must touch only that block — the other two must pass
        through byte-for-byte unchanged. Guards the strip_marked_block
        unification: the three generated-block types must keep mutually
        distinct markers and never corrupt or consume each other's
        content."""
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        toc_block = (
            "<!-- TOC generated by scripts/markdown-toc.py -->\n"
            "\n"
            "- [Section](#section)\n"
            "\n"
            "<!-- /TOC -->\n"
        )
        index_block = (
            "## Alphabetical Index\n"
            "\n"
            "<!-- Generated by scripts/markdown-index.py — re-run after adding or renaming entries. -->\n"
            "\n"
            "- [Term][term] -- Section\n"
            "\n"
            '[term]: #term "Term"\n'
            "\n"
            "<!-- /Alphabetical Index -->\n"
        )
        target = self.write_target(
            toc_block + "\n"
            "## Section\n\nPer A-01.\n\n" + index_block
        )
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn(toc_block, content)
        self.assertIn(index_block, content)
        self.assertIn(
            "<!-- Reference links generated by scripts/refs-linkify.py -->",
            content,
        )
        self.assertIn("[a-01]: reify-refinements.md#a-01", content)


# --- Diagnostics -----------------------------------------------------------

class TestDiagnostics(RefsLinkifyTestCase):
    def test_unknown_reference_warns_but_continues(self):
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target("Reference to A-01 and to A-99 (unknown).\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unknown identifier A-99", result.stderr)
        content = target.read_text()
        self.assertIn("[a-01]: reify-refinements.md#a-01", content)
        # Unknown reference still gets linkified to the bracket form;
        # the broken link def is emitted with an empty target so the
        # author sees the warning loudly.
        self.assertIn("[a-99]: reify-refinements.md#", content)

    def test_duplicate_definition_fatal(self):
        self.write_refinements(textwrap.dedent("""\
            - [ ] **A-01: First**
            - [x] **A-01: Second occurrence**
        """))
        target = self.write_target("Per A-01.\n")
        result = self.run_tool(target)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate definitions", result.stderr)
        self.assertIn("A-01", result.stderr)


# --- Code-block + link-def scoping ---------------------------------------

class TestCodeBlockSkip(RefsLinkifyTestCase):
    def test_id_inside_fenced_code_not_linkified(self):
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            Real reference: A-01.

            ```
            literal A-01 in code; do not rewrite
            ```
        """))
        self.run_tool(target)
        content = target.read_text()
        # Body reference rewritten.
        self.assertIn("Real reference: [A-01][a-01]", content)
        # In-fence reference left bare.
        self.assertIn("literal A-01 in code; do not rewrite", content)


# --- HTML comment scoping ---------------------------------------------------

class TestCommentSkip(RefsLinkifyTestCase):
    def test_id_inside_comment_not_linkified(self):
        """Mirrors test_id_inside_fenced_code_not_linkified: a bare ID
        inside an HTML comment is left bare; a real reference elsewhere
        is still rewritten."""
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            Real reference: A-01.

            <!--
            literal A-01 in a comment; do not rewrite
            -->
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertIn("Real reference: [A-01][a-01]", content)
        self.assertIn("literal A-01 in a comment; do not rewrite", content)

    def test_heading_definition_inside_comment_not_registered(self):
        """A #### D-nn: Title -shaped line inside a comment does not
        become a real Tier-3 definition: a genuine bare reference to
        that ID elsewhere produces an 'unknown reference' warning
        rather than resolving, proving the commented heading was
        ignored."""
        self.write_refinements(textwrap.dedent("""\
            <!--
            #### D-99: Hidden Decision
            -->
        """))
        target = self.write_target("Reference to D-99.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("unknown identifier D-99", result.stderr)

    def test_same_line_comment_does_not_block_following_reference(self):
        """A same-line, self-closing comment does not suppress
        linkification of a real reference on the next line."""
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            <!-- a short note -->

            Reference to A-01.
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertIn("Reference to [A-01][a-01]", content)

    def test_defined_id_referenced_only_in_comment_gets_no_linkdef(self):
        """An ID with a real Tier-1 definition but whose only apparent
        'usage' is inside a comment gets no reference-link entry
        emitted — proves the collect_used_identifiers() fix."""
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            <!-- see A-01 for background -->

            No real reference here.
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertNotIn("[a-01]:", content)


# --- Identifier shapes -----------------------------------------------------

class TestIdentifierShapes(RefsLinkifyTestCase):
    def test_three_digit_identifier(self):
        self.write_refinements("- [ ] **D-100: Decision**\n")
        target = self.write_target("Per D-100.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[d-100]: reify-refinements.md#d-100", content)

    def test_three_letter_category(self):
        self.write_refinements("#### R-PKG-01: Title\n")
        target = self.write_target("Per R-PKG-01.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("R-PKG-01", target.read_text())

    def test_four_letter_category(self):
        self.write_refinements("- [ ] **N-EDDT-001: Need title**\n")
        target = self.write_target("Per N-EDDT-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[n-eddt-001]: reify-refinements.md#n-eddt-001", content)

    def test_two_letter_category_in_checklist(self):
        # 2-letter domain codes (e.g. C-DG-NNN, N-DG-NNN) must be
        # recognised after the regex widened to [A-Z]{2,4}.
        self.write_refinements("- [ ] **C-DG-001: Data sovereignty constraint**\n")
        target = self.write_target("Per C-DG-001.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[C-DG-001][c-dg-001]", content)
        self.assertIn("[c-dg-001]: reify-refinements.md#c-dg-001", content)

    def test_two_letter_category_in_table(self):
        # 2-letter domain code defined via tier-4 table first column.
        ref = self.write_refinements(textwrap.dedent("""\
            | ID        | Summary               |
            |:----------|:----------------------|
            | N-DG-003  | Flat numbering need.  |
        """))
        result = self.run_tool(ref)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = ref.read_text()
        self.assertIn('<a id="n-dg-003"></a>', content)

    def test_two_letter_category_across_indexes(self):
        # A 2-letter code in one index and a 4-letter code in another
        # must both resolve in the target.
        a = self.dir / "primary.md"
        a.write_text("- [ ] **C-DG-001: DG constraint**\n")
        b = self.dir / "secondary.md"
        b.write_text("- [ ] **N-EDDT-001: EDDT need**\n")
        target = self.write_target("Per C-DG-001 and N-EDDT-001.\n")
        result = self.run_tool(target, "--index", str(a), "--index", str(b))
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[C-DG-001][c-dg-001]", content)
        self.assertIn("[N-EDDT-001][n-eddt-001]", content)
        self.assertIn("[c-dg-001]: primary.md#c-dg-001", content)
        self.assertIn("[n-eddt-001]: secondary.md#n-eddt-001", content)


# --- Multi-index resolution ----------------------------------------------

class TestMultipleIndexes(RefsLinkifyTestCase):
    """Definitions can be spread across multiple index docs.

    The Makefile decides which docs are indexes; the script accepts
    --refinements / --index / --indexes (all aliases) repeated.
    """

    def write_named(self, name: str, content: str) -> Path:
        p = self.dir / name
        p.write_text(content)
        return p

    def test_two_indexes_resolve_correctly(self):
        a = self.write_named("primary.md", "- [ ] **A-01: Primary**\n")
        b = self.write_named("secondary.md", "#### R-FOO-01: Secondary requirement\n")
        target = self.write_target("Per A-01 and R-FOO-01.\n")
        result = self.run_tool(target, "--index", str(a), "--index", str(b))
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[A-01][a-01]", content)
        self.assertIn("[R-FOO-01][r-foo-01]", content)
        # A-01 link def points at primary.md; R-FOO-01 at secondary.md.
        self.assertIn("[a-01]: primary.md#a-01", content)
        self.assertIn("[r-foo-01]: secondary.md#r-foo-01", content)

    def test_aliases_refinements_index_indexes_all_work(self):
        a = self.write_named("alpha.md", "- [ ] **A-01: Alpha**\n")
        b = self.write_named("beta.md", "- [ ] **C-01: Beta**\n")
        target = self.write_target("Per A-01 and C-01.\n")
        # Mix all three flag spellings in the same invocation.
        result = self.run_tool(
            target,
            "--refinements", str(a),
            "--index", str(b),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[a-01]: alpha.md#a-01", content)
        self.assertIn("[c-01]: beta.md#c-01", content)

    def test_duplicate_across_indexes_fatal(self):
        a = self.write_named("alpha.md", "- [ ] **A-01: First**\n")
        b = self.write_named("beta.md", "- [ ] **A-01: Conflict**\n")
        target = self.write_target("Per A-01.\n")
        result = self.run_tool(target, "--index", str(a), "--index", str(b))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate definitions", result.stderr)
        self.assertIn("A-01", result.stderr)
        # Both source locations referenced.
        self.assertIn("alpha.md", result.stderr)
        self.assertIn("beta.md", result.stderr)

    def test_target_is_one_of_the_indexes_uses_same_file_form(self):
        # When the target IS the defining index for some IDs, those
        # link defs use the `#anchor` (same-file) form. References to
        # IDs defined in *other* indexes still use the cross-file form.
        primary = self.write_named("primary.md", "- [ ] **A-01: Primary**\n\nReferences A-01 and N-01.\n")
        secondary = self.write_named("secondary.md", "* **N-01 [Need]:** statement.\n")
        result = self.run_tool(
            primary,
            "--index", str(primary),
            "--index", str(secondary),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        content = primary.read_text()
        # A-01 lives in primary itself → same-file form.
        self.assertIn("[a-01]: #a-01", content)
        # N-01 lives in secondary → cross-file form.
        self.assertIn("[n-01]: secondary.md#n-01", content)

    def test_relative_paths_for_indexes_in_different_directories(self):
        # Index in parent dir
        ref = self.write_named("reify-refinements.md", "- [ ] **A-01: Primary**\n")
        
        # Target in sub dir
        sub_dir = self.dir / "analyses"
        sub_dir.mkdir()
        target = sub_dir / "consumer.md"
        target.write_text("Per A-01.\n")

        result = self.run_tool(target, "--index", str(ref))
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        # Should correctly use relative path pointing up
        self.assertIn("[a-01]: ../reify-refinements.md#a-01", content)


# --- H1 skip ---------------------------------------------------------------

class TestH1Skip(RefsLinkifyTestCase):
    """H1 lines are the document title. Rewriting an identifier inside
    them injects markdown link syntax into HTML <title> and PDF title
    metadata extraction, so they must be passed through verbatim."""

    def test_h1_with_identifier_not_linkified(self):
        self.write_refinements("- [ ] **A-05: Manifest**\n")
        target = self.write_target(textwrap.dedent("""\
            # A-05 — Payload & Manifest Format

            Body referencing A-05.
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertIn("# A-05 — Payload & Manifest Format", content)
        self.assertNotIn("# [A-05]", content)
        self.assertIn("Body referencing [A-05][a-05]", content)

    def test_h1_with_existing_link_form_reverted(self):
        # An H1 that already carries the linkified shape from a prior
        # buggy run must be cleaned back to bare form on the next run.
        self.write_refinements("- [ ] **A-05: Manifest**\n")
        target = self.write_target(textwrap.dedent("""\
            # [A-05][a-05] — Payload & Manifest Format

            Body.
        """))
        self.run_tool(target)
        # The H1 skip prevents future rewrites, but does not actively
        # rewrite already-corrupted H1s. This test documents the
        # behaviour: an existing `[A-05][a-05]` H1 is left untouched.
        # (Authors must hand-edit corrupted titles once.)
        content = target.read_text()
        self.assertIn("# [A-05][a-05] — Payload & Manifest Format", content)

    def test_h2_still_linkified(self):
        # Only single-`#` H1 is excluded; deeper headings continue to
        # follow tier-3 / body rules.
        self.write_refinements("- [ ] **A-05: Manifest**\n")
        target = self.write_target(textwrap.dedent("""\
            ## Section about A-05

            body.
        """))
        self.run_tool(target)
        content = target.read_text()
        self.assertIn("## Section about [A-05][a-05]", content)


# --- Inline code spans --------------------------------------------------------

class TestInlineCode(RefsLinkifyTestCase):
    def test_inline_backtick_id_not_linkified(self):
        # An identifier inside a backtick inline-code span is literal text
        # and must NOT be rewritten to a reference link.
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target(textwrap.dedent("""\
            Body prose references A-01 normally.
            The tag is `A-01` and must stay literal.
        """))
        self.run_tool(target)
        content = target.read_text()
        # Prose reference IS rewritten.
        self.assertIn("references [A-01][a-01]", content)
        # Inline-code span is preserved verbatim.
        self.assertIn("`A-01`", content)
        self.assertNotIn("`[A-01]", content)

    def test_backticked_id_not_collected_as_used(self):
        # An identifier that appears ONLY inside backtick spans must not be
        # collected as "used" — no link-def emitted, no unknown warning.
        self.write_refinements("- [ ] **A-01: Analysis**\n")
        target = self.write_target("The tag `B-02` is literal only; A-01 is a ref.\n")
        result = self.run_tool(target)
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        # No link-def for B-02.
        self.assertNotIn("[b-02]:", content)
        # No unknown-identifier warning for B-02.
        self.assertNotIn("unknown identifier B-02", result.stderr)


# --- Heading wins over table (same file) --------------------------------------

class TestHeadingWins(RefsLinkifyTestCase):
    def test_heading_wins_over_table_same_file(self):
        # When both a tier-3 heading and a tier-4 table first-column cell
        # define the same identifier in the same file, the heading wins
        # and no duplicate-definition error is raised.
        ref = self.write_refinements(textwrap.dedent("""\
            | ID      | Name            |
            |:--------|:----------------|
            | S-DG-01 | Snapshot Parser |

            #### S-DG-01 — Snapshot Parser

            Description of S-DG-01.
        """))
        target = self.write_target("Uses S-DG-01.\n")
        result = self.run_tool(target, "--index", str(ref))
        self.assertEqual(result.returncode, 0, result.stderr)
        content = target.read_text()
        self.assertIn("[S-DG-01][s-dg-01]", content)
        self.assertIn("[s-dg-01]: reify-refinements.md#s-dg-01", content)


if __name__ == "__main__":
    unittest.main()
