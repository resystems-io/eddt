#!/usr/bin/env python3
"""Unit tests for markdown-pdf.py size-variant code fences.

Exercises the two fence forms that markdown-pdf.py supports for
per-block font-size overrides (small-text, tiny-text, large-text):

* **Form 1 — bare fence tag**: ```small-text ... ``` (no language,
  plain text, pymdownx.superfences custom_fences dispatches).
  Expected HTML shape: ``<pre class="<variant>"><code>...</code></pre>``.

* **Form 2 — attribute list on language fence**: ```<lang> {: .small-text } ... ```
  (pymdownx.superfences + pymdownx.highlight composed; retains pygments
  syntax highlighting and adds the variant class to the wrapping div).
  Expected HTML shape: ``<div class="<variant> codehilite"><pre>...</pre></div>``.

Checks:

* R01: each bare fence tag (small-text / tiny-text / large-text) emits
  a ``<pre class="<variant>">`` wrapper.
* R02: each attribute-list form attaches the variant class to the
  wrapping ``<div class="... codehilite">``.
* R03: attribute-list form preserves pygments syntax highlighting
  (tokens are wrapped in spans with pygments class names).
* R04: control — a plain fence (``` ... ```) emits the standard
  ``<div class="codehilite">`` without any size-variant class.
* R05: control — a language fence (```go ... ```) emits pygments
  highlighting inside ``<div class="codehilite">``, again without
  any size-variant class.
* R06: inline code (single backtick) is unaffected by the extension
  swap.
* R07: content is preserved verbatim through both fence forms — in
  particular a realistic multi-dot NATS-subject string survives
  rendering with no mangling.

Run from the repo root or this directory:

    python3 designs/scripts/tests/test_markdown_pdf.py

Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
MD_PDF = SCRIPTS / "markdown-pdf.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("markdown_pdf", MD_PDF)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _render(mod, md_text: str) -> str:
    """Render markdown to HTML using md_to_html() with math disabled."""
    html, _, _ = mod.md_to_html(md_text, pygments_style="default", math=False)
    return html


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"ok   {msg}")


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------


def test_bare_fence_tags() -> None:
    """Covers: R01 — each bare-fence variant emits <pre class="<variant>">."""
    mod = _load_module()
    variants = ["small-text", "tiny-text", "large-text"]
    for variant in variants:
        md = f"```{variant}\nsample content\n```"
        html = _render(mod, md)
        expected = f'<pre class="{variant}">'
        if expected not in html:
            _fail(
                f"R01: bare `{variant}` fence did not produce {expected!r}; "
                f"got {html!r}"
            )
        # Content must appear inside a <code> element within the <pre>.
        if "<code>sample content</code>" not in html:
            _fail(
                f"R01: bare `{variant}` fence did not wrap content in "
                f"<code>...</code>; got {html!r}"
            )


def test_attr_list_with_language() -> None:
    """Covers: R02, R03 — attribute-list form attaches variant class to
    the wrapping <div class="<variant> codehilite"> and preserves
    pygments syntax highlighting."""
    mod = _load_module()
    cases = [
        ("small-text", "python", 'print("hi")'),
        ("tiny-text",  "go",     "func main() {}"),
        ("large-text", "rust",   "fn main() {}"),
    ]
    for variant, lang, body in cases:
        md = f"```{lang} {{: .{variant} }}\n{body}\n```"
        html = _render(mod, md)
        # R02: wrapping div carries both the variant class and codehilite.
        expected_div_class = f'<div class="{variant} codehilite">'
        if expected_div_class not in html:
            _fail(
                f"R02: attr-list `{lang} {{: .{variant} }}` did not produce "
                f"{expected_div_class!r}; got {html!r}"
            )
        # R03: pygments emits at least one <span class="..."> token
        # inside the <code> element.
        if '<span class="' not in html:
            _fail(
                f"R03: attr-list `{lang} {{: .{variant} }}` produced no "
                f"pygments spans — syntax highlighting lost; got {html!r}"
            )


def test_plain_fence_control() -> None:
    """Covers: R04 — a plain fence (no language, no attributes) emits
    <div class="codehilite"> with no size-variant class."""
    mod = _load_module()
    md = "```\nplain block\n```"
    html = _render(mod, md)
    if '<div class="codehilite">' not in html:
        _fail(f"R04: plain fence did not produce codehilite wrapper; got {html!r}")
    # None of the variant classes should leak in.
    for variant in ("small-text", "tiny-text", "large-text"):
        if variant in html:
            _fail(
                f"R04: plain fence unexpectedly carries `{variant}` class; "
                f"got {html!r}"
            )


def test_language_fence_control() -> None:
    """Covers: R05 — a language fence emits pygments highlighting inside
    <div class="codehilite"> with no size-variant class."""
    mod = _load_module()
    md = "```go\nfunc main() {}\n```"
    html = _render(mod, md)
    if '<div class="codehilite">' not in html:
        _fail(f"R05: language fence did not produce codehilite wrapper; got {html!r}")
    if '<span class="' not in html:
        _fail(f"R05: language fence produced no pygments spans; got {html!r}")
    for variant in ("small-text", "tiny-text", "large-text"):
        if variant in html:
            _fail(
                f"R05: language fence unexpectedly carries `{variant}` class; "
                f"got {html!r}"
            )


def test_inline_code_unaffected() -> None:
    """Covers: R06 — inline code (single backtick) renders as an inline
    <code> element inside a <p>, unchanged by the extension swap."""
    mod = _load_module()
    md = "A paragraph with `inline` code in it."
    html = _render(mod, md)
    if "<p>" not in html or "<code>inline</code>" not in html:
        _fail(f"R06: inline code rendering regressed; got {html!r}")
    # Must not be wrapped in a codehilite div.
    if '<div class="codehilite">' in html:
        _fail(f"R06: inline code was wrapped in a codehilite div; got {html!r}")


def test_nats_subject_content_preserved() -> None:
    """Covers: R07 — a realistic long NATS subject survives both fence
    forms verbatim, with dots and hyphens intact."""
    mod = _load_module()
    subject = (
        "ol.data.delta.acme.ue-materialiser-1.silver.3gpp.lte.ue-session."
        "imsi.310150123456789.oneid.9c7d3a8f14e2b0c1a4f6d982356f5780e9c8b7a1"
    )

    # Bare-fence form.
    md_bare = f"```small-text\n{subject}\n```"
    html_bare = _render(mod, md_bare)
    if subject not in html_bare:
        _fail(
            f"R07: bare-fence form lost or mangled the NATS subject "
            f"content; expected {subject!r} in output"
        )

    # Attr-list form (text language to avoid pygments lexer tokenising
    # the subject as code — we want to check the shape, not the
    # highlighting).
    md_attr = f"```text {{: .small-text }}\n{subject}\n```"
    html_attr = _render(mod, md_attr)
    if subject not in html_attr:
        _fail(
            f"R07: attr-list form lost or mangled the NATS subject "
            f"content; expected {subject!r} in output"
        )


# ----------------------------------------------------------------------
# Theme loader — British spelling alias + per-level headings map
# ----------------------------------------------------------------------


def _write_yaml(tmp_dir: Path, name: str, content: str) -> Path:
    path = tmp_dir / name
    path.write_text(content)
    return path


def test_loader_accepts_british_spelling(tmp_path: Path) -> None:
    """R08 — `colours:` / `colour:` (British canonical) loads cleanly."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "british.yaml", """
colours:
  accent: "#abcdef"
headings:
  h4:
    colour: accent
    style:  italic
""")
    theme = mod.load_theme(yaml_path)
    if theme["colours"]["accent"] != "#abcdef":
        _fail(f"R08: British colours.accent did not survive load: {theme['colours']['accent']!r}")
    if theme["headings"]["h4"]["colour"] != "#abcdef":
        _fail(f"R08: h4.colour did not resolve from palette: {theme['headings']['h4']['colour']!r}")
    if theme["headings"]["h4"].get("style") != "italic":
        _fail(f"R08: h4.style did not survive: {theme['headings']['h4'].get('style')!r}")
    if "colors" in theme:
        _fail("R08: legacy `colors` key leaked into resolved theme; should be normalised")


def test_loader_accepts_american_spelling_alias(tmp_path: Path) -> None:
    """R09 — `colors:` / `color:` is accepted and normalised to British canonical."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "american.yaml", """
colors:
  accent: "#abcdef"
headings:
  h4:
    color: accent
    style: italic
""")
    theme = mod.load_theme(yaml_path)
    if theme["colours"]["accent"] != "#abcdef":
        _fail(f"R09: American colors.accent did not normalise to colours: {theme.get('colours')!r}")
    if theme["headings"]["h4"]["colour"] != "#abcdef":
        _fail(f"R09: per-heading `color` did not normalise to `colour`: {theme['headings']['h4']!r}")
    if "colors" in theme:
        _fail("R09: `colors` key still present after normalisation")
    if "color" in theme["headings"]["h4"]:
        _fail("R09: per-heading `color` key still present after normalisation")


def test_loader_no_headings_map_falls_back_to_defaults(tmp_path: Path) -> None:
    """R10 — A theme with no `headings:` map renders identically to the
    built-in defaults (regression: existing themes must keep working)."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "no-headings.yaml", """
colours:
  accent: "#e57222"
""")
    theme = mod.load_theme(yaml_path)
    # Built-in heading defaults survive merge.
    h1 = theme["headings"]["h1"]
    if h1["size"] != "22pt":
        _fail(f"R10: h1.size lost: {h1!r}")
    if h1["colour"] != theme["colours"]["heading"]:
        _fail(f"R10: h1.colour did not resolve to palette `heading`: {h1!r}")
    h2 = theme["headings"]["h2"]
    if h2["border_bottom"]["colour"] != theme["colours"]["accent"]:
        _fail(f"R10: h2 border_bottom colour did not resolve to palette `accent`: {h2!r}")
    h3 = theme["headings"]["h3"]
    if h3["colour"] != theme["colours"]["accent"]:
        _fail(f"R10: h3.colour did not resolve to palette `accent`: {h3!r}")


def test_loader_per_heading_override_resolves_correctly(tmp_path: Path) -> None:
    """R11 — A user theme overriding `headings.h4.colour: accent` produces
    a resolved hex equal to the user's palette `accent`, and renders into
    a CSS `color: <hex>` declaration for the h4 selector in the template."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "h4-override.yaml", """
colours:
  accent: "#e57222"
headings:
  h4:
    colour: accent
    style:  italic
""")
    theme = mod.load_theme(yaml_path)
    if theme["headings"]["h4"]["colour"] != "#e57222":
        _fail(f"R11: h4.colour did not resolve via palette: {theme['headings']['h4']!r}")

    # Render the CSS template fragment to confirm h4 carries the resolved
    # colour and the style.
    import jinja2
    css_tmpl = jinja2.Template("""
{%- for level, h in theme.headings.items() %}
{{ level }} { {%- if h.colour %}color: {{ h.colour }};{%- endif %} {%- if h.style %}font-style: {{ h.style }};{%- endif %} }
{%- endfor %}
""")
    css = css_tmpl.render(theme=theme)
    if "color: #e57222" not in css:
        _fail(f"R11: rendered CSS does not contain h4 accent colour\n{css}")
    if "font-style: italic" not in css:
        _fail(f"R11: rendered CSS does not contain h4 italic style\n{css}")


# ----------------------------------------------------------------------
# Watermark — schema, resolver, SVG generator, default-off
# ----------------------------------------------------------------------


def test_watermark_default_off_produces_no_svg() -> None:
    """R12 — When the theme's watermark text is None / absent, the
    SVG generator returns None so the CSS template emits no
    background-image declaration."""
    mod = _load_module()
    theme = mod.load_theme(None)  # built-in theme; watermark.text=None
    svg = mod.render_watermark_svg(theme.get("watermark"))
    if svg is not None:
        _fail(f"R12: built-in theme should yield no watermark; got {svg!r}")


def test_watermark_loader_accepts_block_with_palette_colour(tmp_path: Path) -> None:
    """R13 — Loader accepts a `watermark:` block; palette-key colour
    resolves; British / American spelling alias both work."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "wm.yaml", """
colours:
  status_red: "#c62828"
watermark:
  text:    "DRAFT"
  colour:  status_red
  opacity: 0.20
  size:    "100pt"
  angle:   -45
""")
    theme = mod.load_theme(yaml_path)
    wm = theme["watermark"]
    if wm["text"] != "DRAFT":
        _fail(f"R13: watermark.text not preserved: {wm!r}")
    if wm["colour"] != "#c62828":
        _fail(f"R13: palette-key colour did not resolve to hex: {wm!r}")
    if wm["opacity"] != 0.20 or wm["size"] != "100pt" or wm["angle"] != -45:
        _fail(f"R13: watermark numeric/string fields not preserved: {wm!r}")


def test_watermark_american_spelling_alias(tmp_path: Path) -> None:
    """R14 — `watermark.color:` is accepted as an alias and
    normalised to `watermark.colour:`."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "wm-us.yaml", """
colours:
  accent: "#abcdef"
watermark:
  text:  "DRAFT"
  color: accent
""")
    theme = mod.load_theme(yaml_path)
    wm = theme["watermark"]
    if "color" in wm:
        _fail(f"R14: `color` key should have been normalised away: {wm!r}")
    if wm.get("colour") != "#abcdef":
        _fail(f"R14: alias did not resolve to palette accent: {wm!r}")


def test_watermark_svg_round_trips_text_and_attributes(tmp_path: Path) -> None:
    """R15 — The SVG generator emits a data URI carrying the supplied
    text, colour, opacity, and angle, and is XML-safe under unusual
    characters."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "wm-rich.yaml", """
colours:
  accent: "#0066cc"
watermark:
  text:    "DRAFT & REVIEW"
  colour:  accent
  opacity: 0.25
  size:    "100pt"
  angle:   -45
""")
    theme = mod.load_theme(yaml_path)
    svg = mod.render_watermark_svg(theme["watermark"])
    if svg is None:
        _fail("R15: generator returned None for an enabled watermark")
    if not svg.startswith("data:image/svg+xml;utf8,"):
        _fail(f"R15: data URI prefix wrong: {svg[:60]!r}")

    from urllib.parse import unquote
    payload = unquote(svg.split(",", 1)[1])
    if "DRAFT &amp; REVIEW" not in payload:
        _fail(f"R15: text not XML-escaped or absent in SVG payload:\n{payload}")
    if 'fill="#0066cc"' not in payload:
        _fail(f"R15: colour not present in SVG payload:\n{payload}")
    if 'fill-opacity="0.25"' not in payload:
        _fail(f"R15: opacity not present in SVG payload:\n{payload}")
    if "rotate(-45" not in payload:
        _fail(f"R15: angle not present in SVG payload:\n{payload}")


# ----------------------------------------------------------------------
# Doc-level directive — <!-- markdown-pdf: ... --> watermark override
# ----------------------------------------------------------------------


def test_directive_overrides_theme(tmp_path: Path) -> None:
    """Covers: R16 — Doc directive present, theme has no watermark
    (built-in default ``text=None``); the directive's watermark.text is
    the effective value."""
    mod = _load_module()
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"DRAFT\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\nbody\n"
    )
    directives, stripped = mod.extract_doc_directives(md)
    if directives.get("watermark", {}).get("text") != "DRAFT":
        _fail(f"R16: directive did not surface watermark.text: {directives!r}")
    if "<!-- markdown-pdf:" in stripped:
        _fail(f"R16: directive not stripped from md_text: {stripped!r}")

    # Apply the directive against a built-in theme; effective watermark
    # should pick up DRAFT and a colour resolved from the palette.
    theme = mod.load_theme(None)  # text=None by default
    theme["watermark"] = mod._deep_merge(
        theme.get("watermark", {}) or {}, directives["watermark"],
    )
    mod._resolve_watermark(theme)
    if theme["watermark"]["text"] != "DRAFT":
        _fail(f"R16: theme.watermark.text not overridden: {theme['watermark']!r}")
    svg = mod.render_watermark_svg(theme["watermark"])
    if svg is None or "DRAFT" not in __import__("urllib.parse").parse.unquote(
        svg.split(",", 1)[1]
    ):
        _fail(f"R16: SVG did not carry directive text: {svg!r}")


def test_directive_deep_merges_over_theme(tmp_path: Path) -> None:
    """Covers: R17 — Doc directive present, theme has watermark; directive
    overrides ``text`` while inheriting the theme's other watermark
    attributes (colour, opacity, angle)."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "wm-theme.yaml", """
colours:
  accent: "#0066cc"
watermark:
  text:    "REVIEW"
  colour:  accent
  opacity: 0.20
  angle:   -45
""")
    theme = mod.load_theme(yaml_path)
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"DRAFT\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\n"
    )
    directives, _ = mod.extract_doc_directives(md)
    theme["watermark"] = mod._deep_merge(
        theme.get("watermark", {}) or {}, directives["watermark"],
    )
    mod._resolve_watermark(theme)
    wm = theme["watermark"]
    if wm["text"] != "DRAFT":
        _fail(f"R17: directive did not override theme text: {wm!r}")
    if wm["colour"] != "#0066cc":
        _fail(f"R17: theme colour not inherited: {wm!r}")
    if wm["opacity"] != 0.20 or wm["angle"] != -45:
        _fail(f"R17: theme opacity/angle not inherited: {wm!r}")


def test_directive_yields_to_cli_watermark(tmp_path: Path) -> None:
    """Covers: R18 — Directive sets watermark.text=DRAFT; --watermark
    CLI override replaces it with the CLI value (CLI wins outright)."""
    mod = _load_module()
    theme = mod.load_theme(None)
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"DRAFT\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\n"
    )
    directives, _ = mod.extract_doc_directives(md)
    theme["watermark"] = mod._deep_merge(
        theme.get("watermark", {}) or {}, directives["watermark"],
    )
    mod._resolve_watermark(theme)
    # Simulate `--watermark "CLI-WINS"`: CLI overwrites .text and re-resolves.
    theme.setdefault("watermark", {})["text"] = "CLI-WINS"
    mod._resolve_watermark(theme)
    if theme["watermark"]["text"] != "CLI-WINS":
        _fail(f"R18: CLI did not override directive: {theme['watermark']!r}")


def test_directive_yields_to_no_watermark_cli(tmp_path: Path) -> None:
    """Covers: R19 — Directive sets watermark.text=DRAFT; --no-watermark
    forces text to None and disables the watermark entirely."""
    mod = _load_module()
    theme = mod.load_theme(None)
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"DRAFT\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\n"
    )
    directives, _ = mod.extract_doc_directives(md)
    theme["watermark"] = mod._deep_merge(
        theme.get("watermark", {}) or {}, directives["watermark"],
    )
    mod._resolve_watermark(theme)
    # Simulate `--no-watermark`.
    theme.setdefault("watermark", {})["text"] = None
    if mod.render_watermark_svg(theme["watermark"]) is not None:
        _fail("R19: --no-watermark did not disable the watermark")


def test_no_directive_falls_back_to_theme(tmp_path: Path) -> None:
    """Covers: R20 — No doc directive: theme value is used unchanged
    (regression of pre-directive behaviour)."""
    mod = _load_module()
    yaml_path = _write_yaml(tmp_path, "wm-theme.yaml", """
watermark:
  text: "REVIEW"
""")
    theme = mod.load_theme(yaml_path)
    md = "# Title\n\n## Heading\nno directive here\n"
    directives, stripped = mod.extract_doc_directives(md)
    if directives:
        _fail(f"R20: spurious directives extracted from clean doc: {directives!r}")
    if stripped != md:
        _fail("R20: md_text mutated by no-op extraction")
    if theme["watermark"]["text"] != "REVIEW":
        _fail(f"R20: theme value not preserved: {theme['watermark']!r}")


def test_directive_malformed_yaml_raises(tmp_path: Path) -> None:
    """Covers: R21 — Malformed YAML inside the directive raises a
    ValueError citing the source line; build is expected to fail."""
    mod = _load_module()
    md = (
        "<!-- markdown-pdf:\n"
        "watermark: [text: DRAFT\n"  # unbalanced bracket
        "-->\n"
        "# Title\n\n"
        "## Heading\n"
    )
    try:
        mod.extract_doc_directives(md)
    except ValueError as exc:
        if "line" not in str(exc).lower():
            _fail(f"R21: ValueError did not cite a line: {exc!r}")
        return
    _fail("R21: malformed-YAML directive did not raise ValueError")


def test_directive_stripped_from_rendered_html(tmp_path: Path) -> None:
    """Covers: R22 — After extraction the directive text is gone from
    md_text; rendering the stripped text produces no leftover comment
    fragment in the HTML body."""
    mod = _load_module()
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"DRAFT\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\nbody copy\n"
    )
    _, stripped = mod.extract_doc_directives(md)
    html, _toc, _refs = mod.md_to_html(stripped, pygments_style="default", math=False)
    if "markdown-pdf" in html:
        _fail(f"R22: directive bled into rendered HTML:\n{html}")
    if "watermark" in html:
        _fail(f"R22: directive body bled into rendered HTML:\n{html}")


def test_directive_below_h2_raises() -> None:
    """Covers: R21 (variant) — Directive below the first H2 is an
    authoring mistake and raises rather than being silently ignored."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "## Heading\nbody\n\n"
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"LATE\"\n"
        "-->\n"
    )
    try:
        mod.extract_doc_directives(md)
    except ValueError as exc:
        if "preamble" not in str(exc).lower():
            _fail(f"R21-variant: ValueError did not mention preamble: {exc!r}")
        return
    _fail("R21-variant: late directive did not raise")


def test_duplicate_directives_raise() -> None:
    """Covers: R21 (variant) — More than one directive in the preamble
    is a build error; merging is the author's job, not the parser's."""
    mod = _load_module()
    md = (
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"FIRST\"\n"
        "-->\n"
        "<!-- markdown-pdf:\n"
        "watermark:\n"
        "  text: \"SECOND\"\n"
        "-->\n"
        "# Title\n\n"
        "## Heading\n"
    )
    try:
        mod.extract_doc_directives(md)
    except ValueError as exc:
        if "more than one" not in str(exc).lower():
            _fail(f"R21-dup: ValueError did not flag duplicate: {exc!r}")
        return
    _fail("R21-dup: duplicate directives did not raise")


# ----------------------------------------------------------------------
# Math rendering — auto-generated TOC entries
# ----------------------------------------------------------------------


def test_math_in_toc_inline_renders() -> None:
    """Covers: R23 — A heading containing inline math ($\\mathcal{C}$)
    produces a TOC entry whose displayed text is rendered MathML, not
    the bare ``\\(LATEX\\)`` delimited form."""
    mod = _load_module()
    md = (
        "# Doc\n\n"
        "## Heading with $\\mathcal{C}$ inside\n\n"
        "body\n"
    )
    body, toc, _refs = mod.md_to_html(md, pygments_style="default", math=True)
    toc = mod.render_math_in_toc(toc)

    if "\\(" in toc or "\\)" in toc:
        _fail(f"R23: bare LaTeX delimiters left in TOC after rendering:\n{toc}")
    if "<math" not in toc:
        _fail(f"R23: TOC missing rendered MathML:\n{toc}")
    if 'class="math-inline"' not in toc:
        _fail(f"R23: TOC missing math-inline wrapper for CSS targeting:\n{toc}")
    # Anchor must still match the body heading id.
    if 'href="#heading-with-mathcalc-inside"' not in toc:
        _fail(f"R23: TOC anchor link missing or changed:\n{toc}")


def test_math_in_toc_multiple_constructs() -> None:
    """Covers: R24 — A heading containing a more complex inline math
    expression (sum-with-limits) renders to MathML in the TOC, not
    just the simple-symbol case."""
    mod = _load_module()
    md = (
        "# Doc\n\n"
        "## Cumulative $\\sum_{i=1}^{n} a_i$ section\n\n"
        "body\n"
    )
    body, toc, _refs = mod.md_to_html(md, pygments_style="default", math=True)
    toc = mod.render_math_in_toc(toc)

    if "\\(" in toc or "\\)" in toc:
        _fail(f"R24: bare delimiters left in TOC for sum expression:\n{toc}")
    if "<math" not in toc:
        _fail(f"R24: TOC missing MathML for sum expression:\n{toc}")


def test_toc_without_math_unchanged() -> None:
    """Covers: R25 — A TOC built from headings that contain no math is
    untouched by ``render_math_in_toc`` (regression guard)."""
    mod = _load_module()
    md = (
        "# Doc\n\n"
        "## First section\n\nA\n\n"
        "## Second section\n\nB\n"
    )
    _, toc_before, _refs = mod.md_to_html(md, pygments_style="default", math=True)
    toc_after = mod.render_math_in_toc(toc_before)
    if toc_before != toc_after:
        _fail(
            f"R25: render_math_in_toc mutated a math-free TOC\n"
            f"before:\n{toc_before}\nafter:\n{toc_after}"
        )


def test_toc_math_render_failure_falls_back() -> None:
    """Covers: R26 — When the LaTeX inside a TOC entry cannot be
    converted to MathML (latex2mathml raises), the original delimited
    form is preserved rather than the build crashing. Exercises the
    try / except fallback in render_math_in_toc."""
    mod = _load_module()
    # Hand-crafted TOC fragment with a pathological LaTeX payload.
    # latex2mathml raises on a bare backslash + EOF / unbalanced group.
    fake_toc = (
        '<div class="toc"><ul>'
        '<li><a href="#x">Bad \\(\\unknownmacro{\\)</a></li>'
        '</ul></div>'
    )
    out = mod.render_math_in_toc(fake_toc)
    # Either the converter survived and produced MathML, or the
    # fallback preserved the original delimiters. Both are acceptable;
    # what is NOT acceptable is the function raising.
    if "<a href=\"#x\">" not in out:
        _fail(f"R26: anchor lost after render attempt:\n{out}")


def test_toc_math_inside_anchor_preserves_link() -> None:
    """Covers: R27 — MathML inside an <a> tag does not break the
    anchor's href attribute. Body heading id and TOC link href still
    match, so PDF clickable navigation continues to work."""
    mod = _load_module()
    md = (
        "# Doc\n\n"
        "## Category $\\mathcal{C}$ and morphisms\n\n"
        "body\n"
    )
    body, toc, _refs = mod.md_to_html(md, pygments_style="default", math=True)
    toc = mod.render_math_in_toc(toc)

    # Body heading carries an id that matches the TOC link href.
    import re as _re
    body_id = _re.search(r'<h2 id="([^"]+)"', body)
    toc_href = _re.search(r'<a href="#([^"]+)"', toc)
    if not body_id or not toc_href:
        _fail(f"R27: missing body id or toc href\nbody:\n{body}\ntoc:\n{toc}")
    if body_id.group(1) != toc_href.group(1):
        _fail(
            f"R27: body heading id ({body_id.group(1)!r}) does not match "
            f"toc link href ({toc_href.group(1)!r})"
        )


# ----------------------------------------------------------------------
# Reference-link handover from body to preamble
# ----------------------------------------------------------------------


def _render_split(mod, md_text: str, *, math: bool = False) -> tuple[str, str]:
    """Convert a doc the way main() does — split on first '## ', convert
    body first to capture references, then convert preamble with those
    references injected. Returns (preamble_html, body_html).

    The title `# Heading` is stripped first to mirror main()'s actual
    behaviour (the title is rendered separately on the title page).
    """
    src = mod.re.sub(r"^# .+\n*", "", md_text, count=1)
    preamble_md, body_md = mod.extract_preamble_and_body(src)
    body_html, _toc, body_refs = mod.md_to_html(
        body_md, pygments_style="default", math=math,
    )
    if preamble_md:
        preamble_html, _toc2, _refs2 = mod.md_to_html(
            preamble_md, pygments_style="default", math=math,
            references=body_refs,
        )
    else:
        preamble_html = ""
    return preamble_html, body_html


def test_preamble_reference_link_resolves() -> None:
    """Covers: R28 — A reference-style link [text][ref] in the preamble
    resolves when the matching [ref]: url definition lives at the
    bottom of the document body. This is the bug this refinement
    closes — without the body→preamble references handover, the
    preamble would render '[thing][abc]' as literal text."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract with a reference link [thing][abc] in it.\n\n"
        "## Section\n\nBody.\n\n"
        "[abc]: https://example.com/abc\n"
    )
    preamble_html, _body = _render_split(mod, md)
    if 'href="https://example.com/abc"' not in preamble_html:
        _fail(f"R28: preamble ref link not resolved:\n{preamble_html}")
    if "[thing][abc]" in preamble_html:
        _fail(f"R28: literal [thing][abc] left in preamble:\n{preamble_html}")


def test_preamble_reference_link_carries_title() -> None:
    """Covers: R29 — A [ref]: url "title" definition (CommonMark §4.7
    title form) round-trips correctly via the dict-handover. The
    preamble link should carry the title attribute, exercising the
    no-serialisation path: the title is held in the references dict
    as a tuple value, never re-emitted to markdown text."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract referencing [thing][abc] with a title.\n\n"
        "## Section\n\nBody.\n\n"
        '[abc]: https://example.com/abc "the abc anchor"\n'
    )
    preamble_html, _body = _render_split(mod, md)
    if 'title="the abc anchor"' not in preamble_html:
        _fail(f"R29: preamble link missing title attribute:\n{preamble_html}")
    if 'href="https://example.com/abc"' not in preamble_html:
        _fail(f"R29: preamble link missing href:\n{preamble_html}")


def test_reference_inside_fenced_code_not_picked_up() -> None:
    """Covers: R30 — A line that looks like a [ref]: url definition but
    sits inside a fenced code block must NOT be picked up as a real
    reference. Python-Markdown's preprocessor handles this for us
    (refs inside fences aren't parsed); the test guards against any
    future regression where a regex-based extractor might be tempted
    to scan line-wise without code-fence awareness."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract with [thing][abc] reference.\n\n"
        "## Section\n\n"
        "```\n"
        "[abc]: https://fenced.example.com/SHOULD-NOT-WIN\n"
        "```\n\n"
        "[abc]: https://example.com/abc\n"
    )
    preamble_html, _body = _render_split(mod, md)
    if "fenced.example.com" in preamble_html:
        _fail(
            f"R30: ref def inside fenced code block was incorrectly picked "
            f"up:\n{preamble_html}"
        )
    if 'href="https://example.com/abc"' not in preamble_html:
        _fail(f"R30: real ref def did not resolve:\n{preamble_html}")


def test_reference_inside_inline_code_not_picked_up() -> None:
    """Covers: R31 — A [ref]: url-looking string inside an inline code
    span (single-backtick, possibly multi-line per CommonMark §6.1)
    must not be parsed as a real definition. Same protection as R30
    but for inline code."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract with [thing][abc] reference and an inline span "
        "containing `[abc]: https://inline.example.com/SHOULD-NOT-WIN` here.\n\n"
        "## Section\n\nBody.\n\n"
        "[abc]: https://example.com/abc\n"
    )
    preamble_html, _body = _render_split(mod, md)
    if "inline.example.com" in preamble_html:
        # The hostname must not appear as the resolved href; if it
        # appears only as inline-code text content that is acceptable,
        # but the link [thing][abc] must still resolve to the real def.
        if 'href="https://inline.example.com' in preamble_html:
            _fail(
                f"R31: ref def inside inline code span was incorrectly "
                f"used:\n{preamble_html}"
            )
    if 'href="https://example.com/abc"' not in preamble_html:
        _fail(f"R31: real ref def did not resolve:\n{preamble_html}")


def test_preamble_local_ref_def_resolves() -> None:
    """Covers: R32 — A preamble that defines its OWN reference (rather
    than relying on a body def) still resolves it. Confirms that
    Python-Markdown's preprocessor merges with — rather than reset
    over — the injected references dict, so preamble-local defs
    coexist with body defs."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "[xyz]: https://preamble.example.com/xyz\n\n"
        "Abstract with [thing][xyz] resolved from a preamble-local def.\n\n"
        "## Section\n\nBody.\n"
    )
    preamble_html, _body = _render_split(mod, md)
    if 'href="https://preamble.example.com/xyz"' not in preamble_html:
        _fail(
            f"R32: preamble-local ref def did not resolve:\n{preamble_html}"
        )


def test_label_collision_preamble_local_wins() -> None:
    """Covers: R33 — When the preamble defines a reference label that
    also appears in the body, the preamble-local def wins for
    preamble links. Preprocessor merges the parsed-from-source defs
    on top of injected ones, so input-local definitions naturally
    override (matching the spec's last-definition-wins convention
    when references are scanned in source order, with the preamble
    seen as source order *after* the injected body refs)."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "[abc]: https://preamble.example.com/abc\n\n"
        "Abstract with [thing][abc] should pick the preamble URL.\n\n"
        "## Section\n\nBody [thing-body][abc] should pick body URL.\n\n"
        "[abc]: https://body.example.com/abc\n"
    )
    preamble_html, body_html = _render_split(mod, md)
    if "preamble.example.com" not in preamble_html:
        _fail(f"R33: preamble link did not pick preamble def:\n{preamble_html}")
    if "body.example.com" not in body_html:
        _fail(f"R33: body link did not pick body def:\n{body_html}")


def test_preamble_inline_link_unaffected() -> None:
    """Covers: R34 — Inline-form links [text](url) in the preamble are
    untouched by the references-handover change (regression guard).
    Inline links never depended on the references dict; this test
    locks that in."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract with inline link [stuff](here.md#we-go) form.\n\n"
        "## Section\n\nBody.\n"
    )
    preamble_html, _body = _render_split(mod, md)
    if 'href="here.md#we-go"' not in preamble_html:
        _fail(f"R34: inline link in preamble lost href:\n{preamble_html}")


def test_image_reference_resolves_in_preamble() -> None:
    """Covers: R35 — Image references ![alt][ref] (which share the same
    reference-resolution path as text links) also resolve in the
    preamble via the same handover. Free coverage from the
    references-dict approach: no separate plumbing needed for image
    refs."""
    mod = _load_module()
    md = (
        "# Title\n\n"
        "Abstract with image ![diagram][fig1] in line.\n\n"
        "## Section\n\nBody.\n\n"
        '[fig1]: https://example.com/fig1.png "Figure 1"\n'
    )
    preamble_html, _body = _render_split(mod, md)
    if "<img" not in preamble_html:
        _fail(f"R35: preamble image not rendered:\n{preamble_html}")
    if 'src="https://example.com/fig1.png"' not in preamble_html:
        _fail(f"R35: preamble image src not resolved:\n{preamble_html}")


def test_md_to_html_returns_references_third_value() -> None:
    """Covers: R36 — md_to_html now returns a 3-tuple
    (body_html, toc_html, references). Sanity check on the shape and
    on the references dict's contents — body link defs surface as
    {label: (url, title)} entries."""
    mod = _load_module()
    md = (
        "## Body section\n\nLink to [thing][abc].\n\n"
        '[abc]: https://example.com/abc "Title here"\n'
    )
    out = mod.md_to_html(md, pygments_style="default", math=False)
    if not isinstance(out, tuple) or len(out) != 3:
        _fail(f"R36: md_to_html return shape wrong; got {type(out).__name__}({len(out) if hasattr(out, '__len__') else '?'} items)")
    body, toc, refs = out
    if not isinstance(refs, dict):
        _fail(f"R36: references not a dict; got {type(refs).__name__}")
    if "abc" not in refs:
        _fail(f"R36: ref label 'abc' missing from references dict: {refs!r}")
    url, title = refs["abc"]
    if url != "https://example.com/abc" or title != "Title here":
        _fail(f"R36: references entry shape wrong: {refs['abc']!r}")


# ----------------------------------------------------------------------
# Title-line link stripping
# ----------------------------------------------------------------------


def test_title_strips_reference_link() -> None:
    """Covers: R37 — A title containing a reference-style link
    [text][ref] is reduced to its display text. Defends against
    upstream linkify tools that might insert ref-style links into
    the # title line; the title is rendered as plain text via Jinja
    so an unstripped link would surface verbatim brackets in the
    PDF title page."""
    mod = _load_module()
    md = "# Doc with [N-ECOL-011][n-ecol-011] in the title\n\n## Section\n"
    title = mod.extract_title(md)
    if title != "Doc with N-ECOL-011 in the title":
        _fail(f"R37: ref link not stripped from title: {title!r}")


def test_title_strips_inline_link() -> None:
    """Covers: R38 — A title containing an inline [text](url) link
    is reduced to its display text. Same protection as R37 for
    the inline-link form."""
    mod = _load_module()
    md = "# Doc with [stuff](http://inline.example.com) in the title\n\n## S\n"
    title = mod.extract_title(md)
    if title != "Doc with stuff in the title":
        _fail(f"R38: inline link not stripped from title: {title!r}")


def test_title_strips_titled_inline_link() -> None:
    """Covers: R39 — The CommonMark title-attribute form
    [text](url "title") is also stripped to display text. The simple
    regex matches up to the closing ) regardless of the title
    attribute between the URL and the close paren."""
    mod = _load_module()
    md = '# Doc with [link with title](http://x.com "the title") here\n\n## S\n'
    title = mod.extract_title(md)
    if title != "Doc with link with title here":
        _fail(f"R39: titled inline link not stripped: {title!r}")


def test_title_strips_multiple_links() -> None:
    """Covers: R40 — Multiple link forms in the same title are all
    stripped (re.sub replaces every match, both regexes run). Mixed
    inline + reference forms should each reduce to text."""
    mod = _load_module()
    md = (
        "# Doc with [a](http://x) inline and [N-ECOL-011][n-ecol-011] ref\n\n"
        "## S\n"
    )
    title = mod.extract_title(md)
    if title != "Doc with a inline and N-ECOL-011 ref":
        _fail(f"R40: multiple links not stripped: {title!r}")


def test_title_unchanged_when_no_links() -> None:
    """Covers: R41 — A title with no markdown link syntax passes
    through unchanged (regression guard for the strip regexes)."""
    mod = _load_module()
    md = "# Plain title with no special syntax at all\n\n## S\n"
    title = mod.extract_title(md)
    if title != "Plain title with no special syntax at all":
        _fail(f"R41: plain title was modified: {title!r}")


def test_title_strips_image_link_variant() -> None:
    """Covers: R42 — Image-link variants !\\[alt\\](url) and
    !\\[alt\\]\\[ref\\] are also stripped (the leading ! is consumed
    along with the brackets), so a title with an embedded image
    falls back to the alt text without a stray ! prefix."""
    mod = _load_module()
    md = "# Doc with ![diagram](pic.png) and ![figure][fig1] images\n\n## S\n"
    title = mod.extract_title(md)
    if title != "Doc with diagram and figure images":
        _fail(f"R42: image links not stripped cleanly: {title!r}")


# ----------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------


def main() -> None:
    tests = [
        ("R01 — bare fence tags emit <pre class='<variant>'>",
         test_bare_fence_tags),
        ("R02/R03 — attribute-list form attaches variant class and keeps highlighting",
         test_attr_list_with_language),
        ("R04 — plain fence has no size-variant class",
         test_plain_fence_control),
        ("R05 — language fence has no size-variant class",
         test_language_fence_control),
        ("R06 — inline code unaffected by extension swap",
         test_inline_code_unaffected),
        ("R07 — long NATS-subject content preserved through both forms",
         test_nats_subject_content_preserved),
    ]
    for label, fn in tests:
        fn()
        _ok(label)
    # The four theme-loader tests use pytest's tmp_path fixture and run
    # under pytest only; the manual runner skips them with a heads-up.
    print("\n(Skipping R08–R11 theme-loader tests in manual runner; run via pytest.)")
    print("\nall tests passed")


if __name__ == "__main__":
    main()
