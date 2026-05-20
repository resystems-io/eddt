#!/usr/bin/env python3
"""Generate a styled PDF from a markdown document.

Usage:
    ./scripts/markdown-pdf.py docs/analysis.md
    ./scripts/markdown-pdf.py docs/analysis.md -o output.pdf
    ./scripts/markdown-pdf.py docs/analysis.md --logo path/to/logo.svg
    ./scripts/markdown-pdf.py docs/analysis.md --theme path/to/theme.yaml

Converts a markdown document to PDF with configurable report styling
(Inter font family, A4 page layout with running footer). Colours,
branding, and per-level heading styles are read from an optional
YAML or JSON theme file; see the ``BUILTIN_THEME`` constant below
for the full schema and defaults.

The theme schema uses British spelling canonically (``colours:``,
``colour:``); American spelling (``colors:``, ``color:``) is
accepted as an alias and normalised at load time, so existing
themes continue to work unchanged.

Assets resolved relative to the script location (``<script>/../assets/``):
  - ``logo.svg``           — page-footer + title-page logo
  - ``theme.yaml`` / .json — palette and brand overrides

Pass ``--logo`` or ``--theme`` to override, or omit the asset file
entirely for a brand-neutral monochrome result. Pass
``--watermark TEXT`` to overlay a draft-style watermark on every
page; the watermark is off by default (``--no-watermark`` forces
it off, overriding any theme value).

Requires: markdown, jinja2, weasyprint, pygments, pymdown-extensions,
latex2mathml. PyYAML is only required when a ``.yaml``/``.yml`` theme
file is used. The ``mmdc`` binary (Mermaid CLI) is required when a
document contains ``mermaid`` fenced blocks; pass ``--no-mermaid`` to
skip diagram rendering and fall through to plain code formatting.

The output PDF is written alongside the markdown file (or to -o path).
"""

import argparse
import copy
import json
import re
import sys
from datetime import date
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ASSETS_DIR = SCRIPT_DIR.parent / "assets"
DEFAULT_LOGO_PATH = DEFAULT_ASSETS_DIR / "logo.svg"
DEFAULT_THEME_CANDIDATES = (
    DEFAULT_ASSETS_DIR / "theme.yaml",
    DEFAULT_ASSETS_DIR / "theme.yml",
    DEFAULT_ASSETS_DIR / "theme.json",
)


# Built-in theme: neutral monochrome with no branding. Any keys set in
# an external theme file are deep-merged over the top, so a config only
# needs to override what it wants to change.
# Per-level heading defaults. Each level's record may carry any of:
#   colour            palette key (looked up against ``colours``) or hex literal
#   size              CSS font-size string (e.g. "11pt")
#   weight            CSS font-weight (default 600)
#   style             CSS font-style (e.g. "italic"); omit for "normal"
#   margin_bottom     CSS length (e.g. "4pt")
#   padding_bottom    CSS length
#   border_bottom     {"width": "<length>", "colour": "<palette key|hex>"}
#   page_break_after  CSS page-break-after value (e.g. "avoid")
# Any key may be omitted; missing keys fall back to these built-in
# defaults. Levels h5 and h6 are intentionally absent — they currently
# fall through to default browser styling. A user theme MAY add them.
BUILTIN_HEADINGS: dict = {
    "h1": {
        "colour":           "heading",
        "size":             "22pt",
        "weight":           600,
        "margin_bottom":    "4pt",
    },
    "h2": {
        "colour":           "heading",
        "size":             "16pt",
        "weight":           600,
        "margin_bottom":    "8pt",
        "border_bottom":    {"width": "2px", "colour": "accent"},
        "padding_bottom":   "4pt",
        "page_break_after": "avoid",
    },
    "h3": {
        "colour":           "accent",
        "size":             "13pt",
        "weight":           600,
        "margin_bottom":    "6pt",
        "page_break_after": "avoid",
    },
    "h4": {
        "colour":           "heading",
        "size":             "11pt",
        "weight":           600,
        "margin_bottom":    "4pt",
    },
}


BUILTIN_THEME: dict = {
    "colours": {
        "accent":             "#555555",  # h2 border, h3, links, blockquote border, brand prefix
        "heading":            "#434343",  # h1-h4 text default, thead background, brand suffix
        "body":               "#333333",  # body text
        "muted":              "#555555",  # toc subheadings, blockquote text
        "subtle":             "#999999",  # page footer, date, subtitle
        "faint":              "#bbbbbb",  # commit sha
        "border":             "#e0e0e0",  # hr, table cell borders
        "code_bg":            "#f4f4f4",  # inline code + pre block background
        "row_alt":            "#f8f8f8",  # zebra stripe for table rows
        # Status-pill palette. The traffic-light triplet is the primary
        # naming; status_resolved / status_choice / status_investigate
        # are accepted as legacy aliases in user YAML (see
        # ``_apply_status_aliases``) and retained as CSS class aliases
        # so existing documents render unchanged.
        "status_green":       "#2e7d32",  # alias of .status-resolved
        "status_amber":       "#ef6c00",  # alias of .status-choice
        "status_red":         "#c62828",  # alias of .status-investigate
        "status_black":       "#212121",  # dark neutral (not pure black)
        "status_grey":        "#616161",  # midtone neutral
        "status_blue":        "#1565c0",
        "status_purple":      "#6a1b9a",
        "status_teal":        "#00695c",
    },
    "headings": copy.deepcopy(BUILTIN_HEADINGS),
    "brand": {
        "name":     None,  # organisation/brand name — omitted ⇒ no brand block on title page
        "subtitle": None,  # sub-line under the brand name (e.g. team or division)
        "split":    None,  # optional two-tone split:
                           #   {"prefix": str, "suffix": str,
                           #    "prefix_colour": str|None, "suffix_colour": str|None}
                           # prefix_colour/suffix_colour default to
                           # colours.accent / colours.heading when None or absent.
    },
    # Optional page watermark. Default ``text=None`` means no watermark.
    # When ``text`` is set (via theme or ``--watermark`` CLI flag), a
    # rotated, semi-transparent watermark renders on every page
    # background, behind the body content. Other keys carry the
    # built-in defaults below; users may override any subset.
    "watermark": {
        "text":    None,         # text to display; None / empty ⇒ disabled
        "colour":  "status_red", # palette key or hex literal
        "opacity": 0.15,         # 0.0 (invisible) – 1.0 (opaque)
        "size":    "120pt",      # CSS font-size
        "angle":   -30,          # degrees of rotation; negative = lower-left → upper-right
        "weight":  700,          # CSS font-weight
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Return a new dict with ``override`` merged into ``base``, recursing into sub-dicts."""
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _normalise_colour_spelling(data: dict) -> dict:
    """Return a deep copy of ``data`` with American colour spellings
    normalised to British canonical form.

    Two rewrites apply at load time:

    * Top-level ``colors:`` becomes ``colours:`` (only when the
      canonical key is absent; if both are present, ``colours:`` wins
      and ``colors:`` is dropped silently).
    * Inside ``headings.<level>.*`` records, the keys ``color`` and
      ``border_bottom.color`` become ``colour`` (same precedence rule).
    * Inside ``brand.split.*``, ``prefix_color`` / ``suffix_color``
      become ``prefix_colour`` / ``suffix_colour``.

    The caller's input dict is not mutated; the returned dict is safe
    to feed into ``_deep_merge``.
    """
    if not isinstance(data, dict):
        return data
    out = copy.deepcopy(data)

    # Top-level colors → colours.
    if "colors" in out and "colours" not in out:
        out["colours"] = out.pop("colors")
    elif "colors" in out and "colours" in out:
        out.pop("colors")  # canonical wins; alias dropped

    # Per-heading colour aliases.
    headings = out.get("headings")
    if isinstance(headings, dict):
        for level, record in list(headings.items()):
            if not isinstance(record, dict):
                continue
            if "color" in record and "colour" not in record:
                record["colour"] = record.pop("color")
            elif "color" in record and "colour" in record:
                record.pop("color")
            border = record.get("border_bottom")
            if isinstance(border, dict):
                if "color" in border and "colour" not in border:
                    border["colour"] = border.pop("color")
                elif "color" in border and "colour" in border:
                    border.pop("color")

    # brand.split.{prefix,suffix}_color → _colour.
    split = (out.get("brand") or {}).get("split")
    if isinstance(split, dict):
        for old, new in (("prefix_color", "prefix_colour"),
                         ("suffix_color", "suffix_colour")):
            if old in split and new not in split:
                split[new] = split.pop(old)
            elif old in split and new in split:
                split.pop(old)

    # watermark.color → watermark.colour.
    watermark = out.get("watermark")
    if isinstance(watermark, dict):
        if "color" in watermark and "colour" not in watermark:
            watermark["colour"] = watermark.pop("color")
        elif "color" in watermark and "colour" in watermark:
            watermark.pop("color")

    return out


def _resolve_colour(value, palette: dict) -> str:
    """Resolve a heading-record colour value against the palette.

    A value that matches a palette key resolves to that palette
    entry's hex. Otherwise it is returned verbatim (treated as a
    literal hex / CSS colour). Caller is responsible for ensuring the
    palette is populated before calling.
    """
    if isinstance(value, str) and value in palette:
        return palette[value]
    return value


def _resolve_headings(theme: dict) -> None:
    """Resolve every heading record's ``colour`` (and any
    ``border_bottom.colour``) against the palette, in place.

    After this runs, ``theme["headings"][level]["colour"]`` is a hex
    string suitable for direct emission into the CSS template, even
    if the user wrote a palette-key name in their YAML.
    """
    palette = theme.get("colours", {})
    headings = theme.get("headings") or {}
    for level, record in headings.items():
        if not isinstance(record, dict):
            continue
        if "colour" in record:
            record["colour"] = _resolve_colour(record["colour"], palette)
        border = record.get("border_bottom")
        if isinstance(border, dict) and "colour" in border:
            border["colour"] = _resolve_colour(border["colour"], palette)


def _resolve_split_colours(theme: dict) -> None:
    """Fill in brand.split.{prefix,suffix}_colour from the palette when unset.

    Operates in place. Defaults are ``colours.accent`` for the prefix
    and ``colours.heading`` for the suffix — preserving the historical
    two-tone look while letting a theme override either independently.
    """
    split = theme.get("brand", {}).get("split")
    if not isinstance(split, dict):
        return
    if not split.get("prefix_colour"):
        split["prefix_colour"] = theme["colours"]["accent"]
    if not split.get("suffix_colour"):
        split["suffix_colour"] = theme["colours"]["heading"]


def _resolve_watermark(theme: dict) -> None:
    """Resolve the watermark colour against the palette, in place.

    A no-op when the watermark is disabled (``text`` is ``None`` or
    empty). When enabled, ``colour`` is normalised to a hex string
    (palette key resolves; literal hex passes through).
    """
    watermark = theme.get("watermark")
    if not isinstance(watermark, dict):
        return
    text = watermark.get("text")
    if not text:
        return
    palette = theme.get("colours", {})
    if "colour" in watermark:
        watermark["colour"] = _resolve_colour(watermark["colour"], palette)


def render_watermark_svg(watermark: dict | None) -> str | None:
    """Generate a URL-encoded inline SVG data URI for the watermark.

    Returns ``None`` when the watermark is disabled. The SVG carries
    a single rotated text element sized for an A4 page (595 × 842 pt)
    that WeasyPrint will scale via ``@page`` background sizing.
    Centred and rotated by ``angle`` degrees about the page centre,
    rendered at the configured colour with ``opacity`` applied via
    ``fill-opacity``. The text content is XML-escaped to defend
    against unusual characters.
    """
    if not isinstance(watermark, dict):
        return None
    text = watermark.get("text")
    if not text:
        return None

    # Sanitise text via XML escaping (handles &, <, >, quotes).
    from xml.sax.saxutils import escape as xml_escape
    text_safe = xml_escape(str(text), entities={'"': "&quot;", "'": "&apos;"})

    colour  = watermark.get("colour",  "#c62828")
    opacity = watermark.get("opacity", 0.15)
    size    = watermark.get("size",    "120pt")
    angle   = watermark.get("angle",   -30)
    weight  = watermark.get("weight",  700)

    # A4 dimensions in CSS points (1/72 inch). The SVG matches these
    # so background-size: 100% 100% on @page maps 1:1.
    width, height = 595, 842
    cx, cy = width / 2, height / 2

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
        f'<text x="{cx}" y="{cy}" '
        f'text-anchor="middle" dominant-baseline="middle" '
        f'fill="{colour}" fill-opacity="{opacity}" '
        f'font-family="Inter, &quot;Helvetica Neue&quot;, Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" '
        f'transform="rotate({angle} {cx} {cy})">{text_safe}</text>'
        f'</svg>'
    )

    # Data URI: percent-encode reserved characters that break in CSS
    # url() contexts. Quote keeps printable characters legible while
    # escaping #, ?, %, and whitespace.
    from urllib.parse import quote
    encoded = quote(svg, safe="")
    return f"data:image/svg+xml;utf8,{encoded}"


def load_theme(path: Path | None) -> dict:
    """Load a YAML or JSON theme file and merge it over ``BUILTIN_THEME``.

    Format is chosen by file extension (``.yaml``/``.yml`` vs ``.json``).
    YAML parsing requires PyYAML — imported lazily so the dependency is
    only needed when a YAML theme is actually referenced.

    User YAML may use British or American colour spellings (``colour``
    / ``color``, ``colours`` / ``colors``); both are accepted and
    normalised to British canonical at load time.
    """
    if path is None:
        merged = copy.deepcopy(BUILTIN_THEME)
        _resolve_headings(merged)
        _resolve_split_colours(merged)
        _resolve_watermark(merged)
        return merged
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError:
            print(
                "PyYAML is required to read YAML theme files. "
                "Install with: pip install pyyaml",
                file=sys.stderr,
            )
            sys.exit(1)
        data = yaml.safe_load(path.read_text()) or {}
    elif suffix == ".json":
        data = json.loads(path.read_text())
    else:
        print(
            f"Unsupported theme format: {path.suffix} (use .yaml, .yml, or .json)",
            file=sys.stderr,
        )
        sys.exit(1)
    if not isinstance(data, dict):
        print(f"Theme file {path} must contain a mapping at the top level", file=sys.stderr)
        sys.exit(1)
    data = _normalise_colour_spelling(data)
    merged = _deep_merge(BUILTIN_THEME, data)
    _apply_status_aliases(merged, data)
    _resolve_headings(merged)
    _resolve_split_colours(merged)
    _resolve_watermark(merged)
    return merged


def _apply_status_aliases(merged: dict, user_data: dict) -> None:
    """Propagate legacy status colour keys onto their canonical names.

    Historical theme files use ``status_resolved`` / ``status_choice`` /
    ``status_investigate``. The canonical palette now uses the traffic-
    light names (``status_green`` / ``status_amber`` / ``status_red``).
    When a user YAML sets a legacy key but not its canonical counterpart,
    copy the legacy value forward so the CSS — which references only the
    canonical names — picks up the user's override.
    """
    user_colours = (user_data.get("colours") or {})
    merged_colours = merged.setdefault("colours", {})
    aliases = {
        "status_resolved":    "status_green",
        "status_choice":      "status_amber",
        "status_investigate": "status_red",
    }
    for legacy, canonical in aliases.items():
        if legacy in user_colours and canonical not in user_colours:
            merged_colours[canonical] = user_colours[legacy]


def check_dependencies():
    """Verify required packages are importable."""
    # Mapping: import name → pip package name (usually identical, but
    # pymdownx is installed as pymdown-extensions).
    required = {
        "markdown": "markdown",
        "jinja2": "jinja2",
        "weasyprint": "weasyprint",
        "pygments": "pygments",
        "pymdownx": "pymdown-extensions",
        "latex2mathml": "latex2mathml",
    }
    missing = []
    for import_name, pip_name in required.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print(f"Missing packages: {', '.join(missing)}", file=sys.stderr)
        print(f"Run: pip install {' '.join(missing)}", file=sys.stderr)
        sys.exit(1)


def git_short_sha(cwd: Path | None = None) -> str:
    """Return the short git commit SHA, or empty string if not in a repo."""
    import subprocess
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=cwd, capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return ""


def extract_title(md_text: str) -> str:
    """Extract the first # heading as the document title.

    Markdown link syntax in the title — ``[text](url)``, ``[text][ref]``,
    ``[text](url "title")``, and the image variants ``![alt](url)`` /
    ``![alt][ref]`` — is stripped to its display text. The title flows
    into three rendering slots that all surface as plain text:

    * the HTML ``<title>`` element (which cannot carry markup at all);
    * the running footer (single-line plain text by template);
    * the title-page ``<h1>`` (rendered via Jinja ``{{ title }}`` with
      no markdown pass).

    Stripping link syntax keeps these three slots consistent and
    defends against upstream tools (e.g. ``needs-linkify.py``) that
    may auto-link bare identifiers in the title without realising the
    title is not a body context.

    Limitation: URLs containing balanced parentheses
    (``[Wikipedia](https://en.wikipedia.org/wiki/Foo_(bar))``) leave a
    stray ``)`` because the simple regex does not track paren
    balance. Titles needing such URLs should use the angle-bracket
    form ``[label](<url>)`` — which the regex handles cleanly — or
    avoid linking the title outright.
    """
    m = re.search(r"^# (.+)$", md_text, re.MULTILINE)
    if not m:
        return "Document"
    title = m.group(1).strip()
    # Strip inline links and image variants:  [text](url) / ![alt](url) → text/alt.
    title = re.sub(r"!?\[([^\]]+)\]\([^)]+\)", r"\1", title)
    # Strip reference links and image variants:  [text][ref] / ![alt][ref] → text/alt.
    title = re.sub(r"!?\[([^\]]+)\]\[[^\]]*\]", r"\1", title)
    return title


def strip_toc_block(md_text: str) -> str:
    """Remove the generated TOC block (between TOC comments)."""
    toc_start = "<!-- TOC generated by scripts/markdown-toc.py -->"
    toc_end = "<!-- /TOC -->"
    start = md_text.find(toc_start)
    if start < 0:
        return md_text
    end = md_text.find(toc_end, start)
    if end < 0:
        return md_text
    end += len(toc_end)
    while end < len(md_text) and md_text[end] == "\n":
        end += 1
    return md_text[:start] + md_text[end:]


# Doc-level directive comment: a single HTML comment carrying a YAML
# mapping under a `markdown-pdf` namespace tag, sitting in the document
# preamble (above the first H2). Invisible on every Markdown renderer
# yet machine-readable here. Slots into the watermark precedence chain
# between the theme YAML and the CLI flags:
#
#   theme.watermark < doc-directive watermark.* < --watermark / --no-watermark
#
# Example:
#
#   <!-- markdown-pdf:
#   watermark:
#     text: "DRAFT"
#     colour: "status_amber"
#   -->
#
# V1 acts only on the `watermark:` top-level key. Other top-level keys
# are tolerated (forward-compat door) but produce a stderr warning.
_DOC_DIRECTIVE_RE = re.compile(
    r"^<!--[ \t]*markdown-pdf:[ \t]*\n(.*?)\n-->[ \t]*$",
    re.DOTALL | re.MULTILINE,
)
_DOC_DIRECTIVE_KNOWN_KEYS = frozenset({"watermark"})


def extract_doc_directives(md_text: str) -> tuple[dict, str]:
    """Extract a single ``<!-- markdown-pdf: ... -->`` directive from
    the document preamble (above the first H2).

    Returns ``(directives, md_text_with_directive_stripped)``.

    Returns ``({}, md_text)`` when no directive is present. Raises
    ``ValueError`` when:

    * the directive body is not valid YAML, or does not parse to a
      mapping;
    * more than one directive comment appears in the preamble;
    * a directive comment appears below the first H2 (caller authoring
      mistake — flag rather than silently ignore).

    Unknown top-level keys produce a stderr warning but do not error.
    """
    # Determine preamble boundary (top → first H2). Directives below
    # that are an authoring mistake; flag them.
    preamble_end = len(md_text)
    h2 = re.search(r"^## ", md_text, re.MULTILINE)
    if h2:
        preamble_end = h2.start()

    matches_in_preamble: list[re.Match[str]] = []
    matches_below: list[re.Match[str]] = []
    for m in _DOC_DIRECTIVE_RE.finditer(md_text):
        (matches_in_preamble if m.start() < preamble_end else matches_below).append(m)

    if matches_below:
        line = md_text.count("\n", 0, matches_below[0].start()) + 1
        raise ValueError(
            f"markdown-pdf directive at line {line} appears below the "
            f"first ## heading; place it in the document preamble"
        )
    if len(matches_in_preamble) > 1:
        line = md_text.count("\n", 0, matches_in_preamble[1].start()) + 1
        raise ValueError(
            f"more than one markdown-pdf directive in preamble "
            f"(second at line {line}); merge into a single directive"
        )
    if not matches_in_preamble:
        return {}, md_text

    m = matches_in_preamble[0]
    body = m.group(1)
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ValueError(
            "PyYAML is required to parse markdown-pdf directive bodies; "
            "install with: pip install pyyaml"
        ) from exc
    try:
        data = yaml.safe_load(body)
    except yaml.YAMLError as exc:
        line = md_text.count("\n", 0, m.start()) + 1
        raise ValueError(
            f"malformed YAML in markdown-pdf directive at line {line}: {exc}"
        ) from exc

    if data is None:
        data = {}
    if not isinstance(data, dict):
        line = md_text.count("\n", 0, m.start()) + 1
        raise ValueError(
            f"markdown-pdf directive at line {line} body must be a YAML "
            f"mapping; got {type(data).__name__}"
        )

    for key in data:
        if key not in _DOC_DIRECTIVE_KNOWN_KEYS:
            print(
                f"warning: markdown-pdf directive: top-level key "
                f"'{key}' is not acted on (V1 supports: "
                f"{sorted(_DOC_DIRECTIVE_KNOWN_KEYS)})",
                file=sys.stderr,
            )

    # Strip the directive (and any single trailing newline) from md_text.
    end = m.end()
    if end < len(md_text) and md_text[end] == "\n":
        end += 1
    stripped = md_text[:m.start()] + md_text[end:]
    return data, stripped


def extract_preamble_and_body(md_text: str) -> tuple[str, str]:
    """Split the document into preamble (before first ##) and body (from first ## onwards).

    The preamble includes the title and any introductory text.
    The body starts at the first ## heading.
    """
    m = re.search(r"^## ", md_text, re.MULTILINE)
    if m:
        return md_text[:m.start()].strip(), md_text[m.start():]
    return md_text, ""


def md_to_html(
    md_text: str,
    pygments_style: str = "tango",
    math: bool = True,
    references: dict | None = None,
) -> tuple[str, str, dict]:
    """Convert markdown to HTML, returning ``(body_html, toc_html, references)``.

    Fenced code blocks are syntax-highlighted via Pygments using the
    specified style (see `pygmentize -L styles` for the full list).

    LaTeX-style math ($...$ inline, $$...$$ block) is parsed by
    pymdownx.arithmatex and rendered server-side to MathML — see
    ``render_math`` below. Disable via ``math=False``.

    Link reference handling (preamble support)
    ------------------------------------------
    Python-Markdown's ``Markdown`` instance carries a ``.references``
    attribute populated during preprocessing — a
    ``dict[label, (url, title)]`` of every parsed link reference
    definition (CommonMark §4.7). This pipeline splits the source
    into preamble (above the first ``## `` heading) and body, then
    runs ``md_to_html`` once per part. Reference definitions
    typically live at the bottom of the document, so the preamble
    invocation needs the body's references to be in scope before
    ``[text][ref]`` links in the abstract can resolve.

    To support that, this function:

    * Accepts an optional ``references`` mapping that is **assigned
      to the underlying** ``Markdown.references`` attribute **before**
      ``convert()`` is called. Python-Markdown's
      ``ReferencePreprocessor`` *adds* to this dict as it parses the
      source, so any reference defs local to the input merge with
      (and where labels collide, override) the injected ones.
    * Returns the resulting reference dict as the third tuple element
      so ``main()`` can hand the body's references over to the
      preamble invocation. The conversion order in ``main()`` is
      therefore body-first, preamble-second — the opposite of the
      original ordering.

    Round-tripping references through markdown text — emitting them
    as ``[label]: url "title"`` lines and re-parsing — is deliberately
    avoided in favour of direct dict-handover. It is shorter, has no
    title-quoting/escaping surface, and reuses the canonical parser's
    own representation.
    """
    import markdown as md
    from pymdownx.superfences import fence_code_format
    extensions = [
        "extra", "toc", "sane_lists", "smarty",
        "pymdownx.superfences", "pymdownx.highlight",
    ]
    extension_configs = {
        "toc": {"permalink": False, "toc_depth": "2-3"},
        "smarty": {"smart_dashes": True, "smart_quotes": True},
        # pymdownx.superfences supersedes python-markdown's built-in
        # fenced_code. custom_fences registers bare fence tags that
        # map to non-highlighted <pre class="..."> blocks — used here
        # to implement the size-variant text fences. Authors who want
        # size variants composed with syntax highlighting use the
        # attribute-list form (e.g. ```py {: .small-text }``) which
        # superfences routes through pymdownx.highlight and tags the
        # wrapping .codehilite div with the size class.
        "pymdownx.superfences": {
            "custom_fences": [
                {"name": "small-text", "class": "small-text", "format": fence_code_format},
                {"name": "tiny-text",  "class": "tiny-text",  "format": fence_code_format},
                {"name": "large-text", "class": "large-text", "format": fence_code_format},
            ],
        },
        # css_class=codehilite keeps the existing pygments stylesheet
        # scope intact so switching off codehilite is a no-op for docs
        # that don't use size variants.
        "pymdownx.highlight": {
            "use_pygments": True,
            "css_class": "codehilite",
            "guess_lang": False,
            "pygments_style": pygments_style,
            "noclasses": False,
        },
    }
    if math:
        extensions.append("pymdownx.arithmatex")
        # generic=true emits wrapper elements with \(...\) and \[...\]
        # payloads — suitable for post-processing into MathML.
        extension_configs["pymdownx.arithmatex"] = {"generic": True}
    converter = md.Markdown(
        extensions=extensions,
        extension_configs=extension_configs,
    )
    if references:
        # Inject pre-populated references BEFORE convert(). The
        # preprocessor does not reset this dict; it merges new defs
        # parsed from md_text on top, so input-local defs override
        # injected ones if labels collide (matching the spec's
        # last-definition-wins rule when references are scanned in
        # source order).
        converter.references = dict(references)
    body_html = converter.convert(md_text)
    if math:
        body_html = render_math(body_html)
    toc_html = getattr(converter, "toc", "")
    # Return a fresh copy so callers cannot mutate the converter's
    # internal state through the returned dict.
    out_refs = dict(converter.references)
    return body_html, toc_html, out_refs


# arithmatex with generic=true emits:
#   inline: <span class="arithmatex">\(LATEX\)</span>
#   block:  <div class="arithmatex">\[LATEX\]</div>
# We extract the LaTeX payload and convert to MathML via latex2mathml,
# then splice the MathML back in. Block math is wrapped in a
# containing <div class="math-block"> so CSS can centre it without
# relying on MathML display="block" semantics that WeasyPrint handles
# inconsistently across versions.
#
# GitHub-compatibility note for markdown authors
# ------------------------------------------------
# GitHub's markdown renderer processes backslash-escaped punctuation
# (\{, \}, \_, etc.) as Markdown-level escapes BEFORE handing the math
# payload to MathJax, so the backslashes are stripped and the escaped
# characters lose their LaTeX meaning. Our pipeline and Antigravity's
# preview do not do this, so $\{...\}$ renders correctly for us but
# produces bare `{...}` (invisible grouping) on GitHub.
#
# Workaround: prefer macro forms that don't rely on backslash-escaped
# punctuation:
#   \{, \}  →  \lbrace, \rbrace
#   \|      →  \Vert  (if used as a visible delimiter)
# All of these are recognised by latex2mathml, MathJax, and KaTeX.
# Doubling the backslash (\\{) DOES NOT work here — latex2mathml
# reads \\ as a hard line break.
_INLINE_MATH_RE = re.compile(
    r'<span class="arithmatex">\\\((.+?)\\\)</span>', re.DOTALL,
)
_BLOCK_MATH_RE = re.compile(
    r'<div class="arithmatex">\\\[(.+?)\\\]</div>', re.DOTALL,
)


def render_math(html: str) -> str:
    """Replace arithmatex wrappers with rendered MathML.

    Python-Markdown HTML-escapes ``<``, ``>``, and ``&`` inside the
    LaTeX payload as part of normal HTML production. We must reverse
    that escaping before handing the LaTeX to latex2mathml — otherwise
    ``&lt;`` is parsed as literal characters instead of the ``<``
    operator.
    """
    import html as _html
    from latex2mathml.converter import convert as latex_to_mathml

    def _inline(m: re.Match) -> str:
        latex = _html.unescape(m.group(1))
        try:
            return (
                '<span class="math-inline">'
                + latex_to_mathml(latex)
                + '</span>'
            )
        except Exception as exc:  # pragma: no cover
            print(f"Math render error (inline): {exc}", file=sys.stderr)
            return m.group(0)

    def _block(m: re.Match) -> str:
        latex = _html.unescape(m.group(1))
        try:
            mathml = latex_to_mathml(latex, display="block")
            return f'<div class="math-block">{mathml}</div>'
        except Exception as exc:  # pragma: no cover
            print(f"Math render error (block): {exc}", file=sys.stderr)
            return m.group(0)

    html = _INLINE_MATH_RE.sub(_inline, html)
    html = _BLOCK_MATH_RE.sub(_block, html)
    return html


# Bare delimiter forms left behind in the auto-generated TOC. Python-
# Markdown's toc extension extracts heading text by walking the heading
# tree and stripping inline elements; the `<span class="arithmatex">`
# wrapper is dropped but the inner `\(LATEX\)` / `\[LATEX\]` literal
# survives as raw text inside the TOC link. ``render_math_in_toc``
# below catches that bare form and converts it to MathML so the TOC
# entry visually matches the body heading.
_INLINE_MATH_BARE_RE = re.compile(r'\\\((.+?)\\\)', re.DOTALL)
_BLOCK_MATH_BARE_RE  = re.compile(r'\\\[(.+?)\\\]', re.DOTALL)


def render_math_in_toc(html: str) -> str:
    """Convert bare ``\\(...\\)`` and ``\\[...\\]`` forms in TOC HTML to MathML.

    Used for the auto-generated TOC, where Python-Markdown's toc
    extension strips the arithmatex wrapper span. Both inline and
    (theoretical) bare-block forms render as inline MathML — a heading
    is phrasing context, so block-display math inside a heading would
    be misshaped regardless.
    """
    import html as _html
    from latex2mathml.converter import convert as latex_to_mathml

    def _convert(m: re.Match) -> str:
        latex = _html.unescape(m.group(1))
        try:
            return (
                '<span class="math-inline">'
                + latex_to_mathml(latex)
                + '</span>'
            )
        except Exception as exc:  # pragma: no cover
            print(f"Math render error (TOC): {exc}", file=sys.stderr)
            return m.group(0)

    html = _INLINE_MATH_BARE_RE.sub(_convert, html)
    html = _BLOCK_MATH_BARE_RE.sub(_convert, html)
    return html


# --- Mermaid diagram rendering ---
# Fenced ``mermaid`` blocks are rendered by shelling out to the Mermaid
# CLI (``mmdc``). Output PNG is cached by SHA of the block source, so
# repeated builds skip diagrams whose source has not changed.
#
# Integration point is markdown-source-level pre-processing (not a
# custom Python-Markdown extension). Each fence is replaced with a
# block-level ``<div class="mermaid-diagram"><img …></div>`` carrying
# a base64 data: URI. Python-Markdown's ``extra`` / ``md_in_html``
# preserves the whole thing as a raw HTML block. This keeps the
# extension stack unchanged and puts the diagnostic in one obvious
# place when a diagram fails to render.
#
# PNG, not SVG: Mermaid 11 emits node labels inside SVG
# ``<foreignObject>`` elements (HTML labels), which WeasyPrint does
# not render. The ``flowchart.htmlLabels: false`` config flag was
# removed from Mermaid 11's default renderer, so SVG output leaves
# node text invisible in the PDF regardless of config. Rendering to
# PNG at 3× scale bakes labels into the raster at high enough
# fidelity to look clean in A4 printout.
#
# Future alternative — vector round-trip via pdf2svg
# ---------------------------------------------------
# A pure-vector path is possible without touching WeasyPrint's
# (absent) ``<foreignObject>`` support:
#
#     mmdc --output diagram.pdf -e pdf    # mermaid emits real PDF
#     pdf2svg diagram.pdf diagram.svg     # flatten to plain SVG
#     # (alternatives: `mutool convert -O format=svg`, `dvisvgm --pdf`)
#
# ``pdf2svg`` (and equivalents) rasterise nothing — they walk the
# PDF's graphics stream and emit vector SVG with real ``<text>``
# elements, matching the TikZ pipeline already in this repo
# (``docs/diagrams/Makefile`` uses ``xelatex`` → PDF → ``dvisvgm``).
# The resulting SVG has no ``<foreignObject>`` and renders cleanly
# through WeasyPrint.
#
# Not adopted in v1 because it adds a system dependency (``pdf2svg``
# or ``mutool``), doubles the subprocess cost per diagram, and the
# PNG fidelity is adequate for current needs. Revisit if vector
# scaling, smaller output, or paper-print fidelity becomes important.
# See the commit that introduced mermaid support for the full
# rationale.

_MERMAID_FENCE_RE = re.compile(
    r"^```mermaid[ \t]*\n(.*?)\n```[ \t]*$",
    re.MULTILINE | re.DOTALL,
)

# Mermaid config — picks a sans-serif font family and a neutral theme
# so diagrams blend with the report styling. Lives alongside the script.
MERMAID_CONFIG_PATH = SCRIPT_DIR / "mermaid-config.json"

# Render scale for PNG output. 3× is a good balance between file size
# and visual crispness at typical A4 PDF resolution.
MERMAID_SCALE = 3


def _mermaid_cache_dir() -> Path:
    """Return (and lazily create) the Mermaid SVG cache directory."""
    cache = SCRIPT_DIR / ".mermaid-cache"
    cache.mkdir(exist_ok=True)
    return cache


def _render_mermaid_source(source: str, cache_dir: Path) -> bytes:
    """Render a single Mermaid source string to PNG via ``mmdc``.

    Returns the raw PNG bytes. Caches by SHA-256 of the source so
    repeat builds skip unchanged diagrams. Raises SystemExit on
    ``mmdc`` invocation failure or missing dependency, with an
    actionable error message.
    """
    import hashlib
    import shutil
    import subprocess

    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
    cache_file = cache_dir / f"{digest}.png"
    if cache_file.exists():
        return cache_file.read_bytes()

    if not shutil.which("mmdc"):
        print(
            "mmdc (Mermaid CLI) is not on PATH. Install it with\n"
            "    npm install -g @mermaid-js/mermaid-cli\n"
            "or pass --no-mermaid to render mermaid fences as plain code.",
            file=sys.stderr,
        )
        sys.exit(1)

    mmdc_cmd = [
        "mmdc",
        "--input", "-",
        "--output", str(cache_file),
        "--backgroundColor", "transparent",
        "--scale", str(MERMAID_SCALE),
        "--quiet",
    ]
    if MERMAID_CONFIG_PATH.exists():
        mmdc_cmd.extend(["--configFile", str(MERMAID_CONFIG_PATH)])
    result = subprocess.run(
        mmdc_cmd,
        input=source,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not cache_file.exists():
        # On failure mmdc may have written a partial file; remove it
        # so the cache doesn't poison subsequent builds.
        cache_file.unlink(missing_ok=True)
        hint = ""
        if "doesn't exist" in (result.stderr or ""):
            # Snap-confined mmdc can't see /tmp or other non-home
            # locations. Surface this explicitly — the generic
            # "directory doesn't exist" message is misleading.
            hint = (
                "\nhint: snap-confined mmdc cannot access directories "
                "outside your home tree (e.g. /tmp). Ensure the cache "
                "directory lives under your home directory."
            )
        print(
            "mmdc failed to render a mermaid diagram.\n"
            f"stderr:\n{result.stderr.strip()}{hint}\n"
            f"source:\n{source}",
            file=sys.stderr,
        )
        sys.exit(1)
    return cache_file.read_bytes()


def render_mermaid(md_text: str, cache_dir: Path | None = None) -> str:
    """Replace fenced ``mermaid`` blocks in *md_text* with inlined PNG.

    Each block's rendered PNG is cached by source hash in
    *cache_dir* (defaults to ``<script>/.mermaid-cache``) and
    embedded in the markdown as a base64 data: URI inside a
    ``<div class="mermaid-diagram">`` wrapper. The ``extra`` markdown
    extension treats this as a raw HTML block.
    """
    import base64

    if cache_dir is None:
        cache_dir = _mermaid_cache_dir()

    def _replace(m: re.Match) -> str:
        source = m.group(1)
        png_bytes = _render_mermaid_source(source, cache_dir)
        data_uri = (
            "data:image/png;base64,"
            + base64.b64encode(png_bytes).decode("ascii")
        )
        return (
            f'\n\n<div class="mermaid-diagram">\n'
            f'<img src="{data_uri}" alt="Mermaid diagram">\n'
            f'</div>\n\n'
        )

    return _MERMAID_FENCE_RE.sub(_replace, md_text)


def pygments_css(style: str) -> str:
    """Generate the Pygments stylesheet for the given style, scoped to .codehilite."""
    from pygments.formatters import HtmlFormatter
    from pygments.util import ClassNotFound
    try:
        return HtmlFormatter(style=style).get_style_defs(".codehilite")
    except ClassNotFound:
        print(f"Unknown Pygments style: {style!r}", file=sys.stderr)
        print("Run: pygmentize -L styles  # to list available styles", file=sys.stderr)
        sys.exit(1)


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<style>
  /* --- Pygments syntax highlighting (dynamically generated) --- */
  {{ pygments_css | safe }}

  /* --- Page setup --- */
  @page {
    size: A4;
    margin: 25mm 20mm 30mm 20mm;
    {%- if watermark_svg %}
    background-image: url("{{ watermark_svg }}");
    background-position: center;
    background-repeat: no-repeat;
    background-size: 100% 100%;
    {%- endif %}

    @bottom-left {
      content: element(page-footer);
      width: 85%;
      vertical-align: top;
    }
    @bottom-right {
      content: element(page-right-footer);
      vertical-align: top;
    }
  }

  @page :first {
    margin-top: 40mm;
    @bottom-left {
      content: none;
    }
    @bottom-right {
      content: none;
    }
  }

  /* --- Right footer: page number with commit SHA beneath --- */
  .page-right-footer {
    position: running(page-right-footer);
    text-align: right;
    line-height: 1.2;
  }
  .page-right-footer .page-num::before {
    content: "Page " counter(page) " of " counter(pages);
  }
  .page-right-footer .page-num {
    font-family: "Inter", "Helvetica Neue", Arial, sans-serif;
    font-size: 8pt;
    color: {{ theme.colours.heading }};
  }
  .page-right-footer .commit-sha {
    font-family: "SFMono-Regular", "Consolas", "Liberation Mono", monospace;
    font-size: 6pt;
    color: {{ theme.colours.faint }};
    display: block;
  }

  /* --- Running footer element --- */
  .page-footer {
    position: running(page-footer);
    font-family: "Inter", "Helvetica Neue", Arial, sans-serif;
    font-size: 8pt;
    color: {{ theme.colours.subtle }};
  }
  .page-footer img {
    height: 12px;
    width: auto;
    vertical-align: text-bottom;
    margin-right: 4px;
  }
  .page-footer span {
    vertical-align: baseline;
  }

  /* --- Typography --- */
  body {
    font-family: "Inter", "Helvetica Neue", Arial, sans-serif;
    font-size: 10pt;
    line-height: 1.6;
    color: {{ theme.colours.body }};
    text-align: {{ text_align }};
    {% if text_align == "justify" %}
    hyphens: auto;
    -webkit-hyphens: auto;
    {% endif %}
  }

  h1, h2, h3, h4, h5, h6 {
    margin-top: 0;
  }
  {%- for level, h in (theme.headings or {}).items() %}
  {{ level }} {
    {%- if h.colour %}
    color: {{ h.colour }};
    {%- endif %}
    {%- if h.size %}
    font-size: {{ h.size }};
    {%- endif %}
    {%- if h.weight %}
    font-weight: {{ h.weight }};
    {%- endif %}
    {%- if h.style %}
    font-style: {{ h.style }};
    {%- endif %}
    {%- if h.margin_bottom %}
    margin-bottom: {{ h.margin_bottom }};
    {%- endif %}
    {%- if h.padding_bottom %}
    padding-bottom: {{ h.padding_bottom }};
    {%- endif %}
    {%- if h.border_bottom %}
    border-bottom: {{ h.border_bottom.width }} solid {{ h.border_bottom.colour }};
    {%- endif %}
    {%- if h.page_break_after %}
    page-break-after: {{ h.page_break_after }};
    {%- endif %}
  }
  {%- endfor %}

  p { margin: 0 0 8pt 0; }

  a { color: {{ theme.colours.accent }}; text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* --- Title page --- */
  .title-page {
    text-align: center;
    padding-top: 60px;
    page-break-after: always;
  }
  .title-page img.logo {
    width: 240px;
    height: auto;
    margin-bottom: 20px;
  }
  .brand-name {
    font-size: 28pt;
    font-weight: 700;
    margin-bottom: 6pt;
  }
  /* Two-tone brand rendering (theme.brand.split). Colours come from
     split.prefix_colour / split.suffix_colour, which default to
     colors.accent / colors.heading when not set. Without a split,
     the whole brand name inherits the .brand-name colour (the default
     body heading colour) via the surrounding element. */
  {% if theme.brand.split %}
  .brand-prefix { color: {{ theme.brand.split.prefix_colour }}; }
  .brand-suffix { color: {{ theme.brand.split.suffix_colour }}; }
  {% endif %}
  .team-name {
    font-size: 14pt;
    color: {{ theme.colours.subtle }};
    margin-bottom: 40px;
    font-weight: 400;
  }
  .title-page h1 {
    font-size: 26pt;
    color: {{ theme.colours.heading }};
    border: none;
    margin-bottom: 8pt;
  }
  .title-page .date {
    font-size: 12pt;
    color: {{ theme.colours.subtle }};
    margin-top: 12px;
  }
  .title-page .preamble {
    margin-top: 40px;
    text-align: {{ text_align }};
    font-size: 10pt;
    line-height: 1.6;
    max-width: 450px;
    margin-left: auto;
    margin-right: auto;
  }

  /* --- Table of contents --- */
  .toc-page {
    page-break-before: always;
    page-break-after: always;
  }
  .toc-page h2 {
    margin-bottom: 16pt;
  }
  .toc-page .toc ul {
    list-style: none;
    padding-left: 0;
    margin: 0;
  }
  .toc-page .toc > ul > li {
    font-size: 10.5pt;
    font-weight: 600;
    color: {{ theme.colours.heading }};
    margin-top: 8pt;
    margin-bottom: 2pt;
  }
  .toc-page .toc > ul > li > ul {
    padding-left: 16px;
  }
  .toc-page .toc > ul > li > ul > li {
    font-size: 9.5pt;
    font-weight: 400;
    color: {{ theme.colours.muted }};
    margin-top: 2pt;
    margin-bottom: 1pt;
  }
  .toc-page .toc a {
    color: inherit;
    text-decoration: none;
  }
  .toc-page .toc a:hover {
    color: {{ theme.colours.accent }};
  }

  /* --- Lists --- */
  ul, ol {
    padding-left: 20px;
    margin: 4pt 0 8pt 0;
  }
  li {
    margin-bottom: 4pt;
  }

  /* --- Tables --- */
  /* page-break-inside: avoid keeps small tables from straddling page
     boundaries. Tables too large for a single page will still split,
     but thead repeats on each continuation page via table-header-group. */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 8.5pt;
    margin: 8pt 0 12pt 0;
    page-break-inside: avoid;
  }
  thead {
    display: table-header-group;
  }
  thead th {
    background: {{ theme.colours.heading }};
    color: #fff;
    font-weight: 600;
    text-align: left;
    padding: 4px 6px;
    white-space: nowrap;
  }
  tbody td {
    padding: 3px 6px;
    border-bottom: 1px solid {{ theme.colours.border }};
  }
  tbody tr:nth-child(even) {
    background: {{ theme.colours.row_alt }};
  }

  /* --- Images (content only, not footer logo) --- */
  body > img, p > img, p img {
    max-width: 100%;
    height: auto;
    display: block;
    margin: 12pt auto;
  }

  /* --- Code --- */
  code {
    font-family: "SFMono-Regular", "Consolas", "Liberation Mono", monospace;
    font-size: 9pt;
    background: {{ theme.colours.code_bg }};
    padding: 1px 4px;
    border-radius: 2px;
  }
  pre {
    font-family: "SFMono-Regular", "Consolas", "Liberation Mono", monospace;
    font-size: 8pt;
    background: {{ theme.colours.code_bg }};
    padding: 8px 12px;
    border-radius: 4px;
    overflow-x: auto;
    line-height: 1.4;
    margin: 4pt 0 8pt 0;
  }
  pre code {
    background: none;
    padding: 0;
  }

  /* --- Size-variant code fences ---
     Opt-in per-block font-size overrides for individual fenced code
     blocks. Two fence forms are supported in the markdown source:

       (1) Attribute list (composes with any language tag for
           syntax-highlighted blocks):
             ```<lang> {: .small-text } ... ```
           produces: <div class="small-text codehilite"><pre>...</pre></div>

       (2) Bare fence tag (convenient for plain-text blocks such as
           long NATS subjects or literal sample data):
             ```small-text ... ```
           produces: <pre class="small-text"><code>...</code></pre>

     Form (1) goes through pymdownx.superfences + pymdownx.highlight
     and attaches the class to the wrapping .codehilite div. Form (2)
     is dispatched by pymdownx.superfences custom_fences registered
     in md_to_html() and attaches the class directly to the <pre>.

     Each variant sets both the <pre> (for the container's padding/
     line-height) and the descendant <code> (so the text itself
     resizes — the base `code { font-size: 9pt }` rule would otherwise
     override the pre's inherited size, leaving text rendered at the
     default even though the wrapper shrank).

     Variants: small-text, tiny-text, large-text. */
  .codehilite.small-text pre, .codehilite.small-text pre code,
  pre.small-text, pre.small-text code {
    font-size: 7pt;
    line-height: 1.3;
  }
  .codehilite.tiny-text pre, .codehilite.tiny-text pre code,
  pre.tiny-text, pre.tiny-text code {
    font-size: 6pt;
    line-height: 1.25;
  }
  .codehilite.large-text pre, .codehilite.large-text pre code,
  pre.large-text, pre.large-text code {
    font-size: 10pt;
    line-height: 1.4;
  }

  /* --- Horizontal rules --- */
  hr {
    border: none;
    border-top: 1px solid {{ theme.colours.border }};
    margin: 16pt 0;
  }

  /* --- Blockquotes --- */
  blockquote {
    border-left: 3px solid {{ theme.colours.accent }};
    margin: 8pt 0;
    padding: 4pt 12pt;
    color: {{ theme.colours.muted }};
    font-style: italic;
  }

  /* --- Strong / emphasis --- */
  strong { font-weight: 600; }
  em { font-style: italic; }

  /* --- Status badges (inline pills for workflow markers) ---
     Rendered as rounded-rectangle pill labels. On GitHub the class
     attribute is ignored and the span content renders as plain text. */
  .status {
    display: inline-block;
    font-size: 8.5pt;
    font-weight: 600;
    padding: 0 6px;
    border-radius: 10px;
    color: #fff;
    margin-right: 4px;
    vertical-align: baseline;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .status-green,  .status-resolved    { background: {{ theme.colours.status_green }}; }
  .status-amber,  .status-choice      { background: {{ theme.colours.status_amber }}; }
  .status-red,    .status-investigate { background: {{ theme.colours.status_red }}; }
  .status-black   { background: {{ theme.colours.status_black }}; }
  .status-grey    { background: {{ theme.colours.status_grey }}; }
  .status-blue    { background: {{ theme.colours.status_blue }}; }
  .status-purple  { background: {{ theme.colours.status_purple }}; }
  .status-teal    { background: {{ theme.colours.status_teal }}; }

  /* --- Mermaid diagrams (PNG produced by mmdc, inlined via data:) ---
     Each ```mermaid fenced block is replaced with a centred block
     wrapper containing a base64-encoded PNG. The image is bounded on
     both axes so that extreme aspect ratios do not overflow the page:
       * max-width:  text-column width — narrow+tall diagrams scale down
         naturally as their column width is the binding constraint.
       * max-height: roughly one page of A4 content height (25 mm + 30 mm
         margins removed from 297 mm, with breathing room for surrounding
         text) — tall diagrams whose intrinsic height would exceed the
         page are scaled down while aspect ratio is preserved, so they
         always fit on a single page.
     The more restrictive of the two wins; aspect ratio is preserved by
     letting height compute from width (or vice-versa) via auto.
     (PNG is used rather than SVG because WeasyPrint does not render
     the <foreignObject> elements Mermaid 11 uses for node labels.) */
  .mermaid-diagram {
    text-align: center;
    margin: 12pt auto;
    page-break-inside: avoid;
  }
  .mermaid-diagram img {
    max-width: 100%;
    max-height: 220mm;
    width: auto;
    height: auto;
  }

  /* --- Math (MathML produced by latex2mathml) ---
     WeasyPrint has no native MathML layout engine, so we style the
     presentation-MathML elements by hand. This covers the common
     inline-math shapes (identifiers, subscripts, superscripts,
     operators) used in this doc set. More complex constructs
     (fractions, matrices, integrals with bounds) would need
     additional rules — or a switch to image-based math rendering. */

  /* Math-font stack: prefer real math fonts over the body sans-serif,
     which lacks properly-sized glyphs for operators like ⊕, ⊗, ∮.
     Latin Modern Math is the canonical LaTeX look — readable and
     well-proportioned for inline math. STIX and the TeX Gyre math
     fonts are fallbacks.

     Note on weight: WeasyPrint does not inherit font-weight from
     the <math> container into its MathML children, so we declare
     the weight directly on the child elements that carry text. LMM
     ships only a Regular face, so any ≥600 request triggers
     WeasyPrint's (binary) synthesised bold applied per-glyph. */
  math {
    font-family: "Latin Modern Math", "STIX Two Math", "STIX",
                 "Asana Math", "TeX Gyre Pagella Math", serif;
  }
  math mi, math mo, math mn, math mtext, math mrow {
    font-family: inherit;
    font-weight: 600;
  }

  /* Inline math flows with the surrounding text. */
  .math-inline math {
    font-size: 1em;
  }
  /* Block math sits in its own centred paragraph. */
  .math-block {
    text-align: center;
    margin: 8pt 0 10pt 0;
  }
  .math-block math {
    font-size: 1.05em;
  }

  /* Italicise identifiers (math variables); operators and
     numbers remain upright. Matches standard math typography. */
  math mi { font-style: italic; }
  math mo, math mn { font-style: normal; }

  /* Slight horizontal air around operators (=, >, +). */
  math mo { padding: 0 0.15em; }

  /* Subscripts / superscripts: the base is the first child and
     renders inline; the modifier child(ren) shift vertically and
     shrink. msubsup needs both positions. */
  math msub, math msup, math msubsup {
    /* Nothing on the container itself — the children handle it. */
  }
  math msub > :nth-child(2) {
    vertical-align: sub;
    font-size: 0.75em;
  }
  math msup > :nth-child(2) {
    vertical-align: super;
    font-size: 0.75em;
  }
  math msubsup > :nth-child(2) {
    vertical-align: sub;
    font-size: 0.75em;
  }
  math msubsup > :nth-child(3) {
    vertical-align: super;
    font-size: 0.75em;
  }

  /* Fences (parentheses, brackets) via <mfenced> or bare <mo>
     are already upright by the rule above. Nothing extra needed. */
</style>
</head>
<body>

{# ---- Brand macro: two-tone split, single-tone name, or nothing ---- #}
{% macro brand_markup(brand) -%}
  {%- if brand.split and brand.split.prefix is not none and brand.split.suffix is not none -%}
    <span class="brand-prefix">{{ brand.split.prefix }}</span><span class="brand-suffix">{{ brand.split.suffix }}</span>
  {%- elif brand.name -%}
    {{ brand.name }}
  {%- endif -%}
{%- endmacro %}

<!-- Running footer (left) -->
<div class="page-footer">
  {% if logo_path %}<img src="{{ logo_path }}" alt="{{ theme.brand.name or '' }}">{% endif %}
  <span>
    {%- if theme.brand.name -%}
      {{ brand_markup(theme.brand) }}
      {%- if theme.brand.subtitle %} {{ theme.brand.subtitle }}{% endif %} &mdash;
    {% endif %}
    {{ title }}
  </span>
</div>

<!-- Running footer (right) -->
<div class="page-right-footer">
  <span class="page-num"></span>
  {% if commit_sha %}<span class="commit-sha">{{ commit_sha }}</span>{% endif %}
</div>

<!-- Title page -->
<div class="title-page">
  {% if logo_path %}
  <img class="logo" src="{{ logo_path }}" alt="{{ theme.brand.name or '' }}">
  {% endif %}
  {% if theme.brand.name %}
  <div class="brand-name">{{ brand_markup(theme.brand) }}</div>
  {% endif %}
  {% if theme.brand.subtitle %}
  <div class="team-name">{{ theme.brand.subtitle }}</div>
  {% endif %}
  <h1>{{ title }}</h1>
  <div class="date">{{ generated_date }}</div>
  {% if preamble_html %}
  <div class="preamble">
    {{ preamble_html }}
  </div>
  {% endif %}
</div>

<!-- Table of contents -->
{% if toc_html %}
<div class="toc-page">
  <h2>Contents</h2>
  <div class="toc">
    {{ toc_html }}
  </div>
</div>
{% endif %}

<!-- Document body -->
{{ body_html }}

</body>
</html>
"""


def render_pdf(
    title: str,
    preamble_html: str,
    body_html: str,
    toc_html: str,
    output_path: Path,
    *,
    theme: dict,
    logo_path: str | None = None,
    base_url: str = ".",
    text_align: str = "justify",
    commit_sha: str = "",
    pygments_css_text: str = "",
):
    """Render HTML to PDF using WeasyPrint."""
    from jinja2 import Template
    from weasyprint import HTML

    template = Template(HTML_TEMPLATE)
    html_str = template.render(
        title=title,
        generated_date=date.today().isoformat(),
        preamble_html=preamble_html,
        body_html=body_html,
        toc_html=toc_html,
        logo_path=logo_path,
        theme=theme,
        watermark_svg=render_watermark_svg(theme.get("watermark")),
        text_align=text_align,
        commit_sha=commit_sha,
        pygments_css=pygments_css_text,
    )

    HTML(string=html_str, base_url=base_url).write_pdf(str(output_path))


def resolve_logo(arg_value: str | None) -> str | None:
    """Resolve the logo path.

    Explicit ``--logo``: resolved relative to cwd; warn and proceed with
    no logo if the file is missing (preserves existing behaviour).

    Implicit (no ``--logo``): silently use ``<script>/../assets/logo.svg``
    if present, otherwise no logo.
    """
    if arg_value is not None:
        candidate = Path(arg_value).resolve()
        if candidate.exists():
            return str(candidate)
        print(f"Warning: logo not found at {candidate}", file=sys.stderr)
        return None
    if DEFAULT_LOGO_PATH.exists():
        return str(DEFAULT_LOGO_PATH)
    return None


def resolve_theme_path(arg_value: str | None) -> Path | None:
    """Resolve the theme file path.

    Explicit ``--theme``: hard error if the file is missing — if you've
    asked for a specific theme and it isn't there, silently falling back
    to defaults would be surprising.

    Implicit (no ``--theme``): try ``<script>/../assets/theme.{yaml,yml,json}``
    in order; return the first match, or ``None`` if none present.
    """
    if arg_value is not None:
        candidate = Path(arg_value).resolve()
        if not candidate.exists():
            print(f"Theme file not found: {candidate}", file=sys.stderr)
            sys.exit(1)
        return candidate
    for candidate in DEFAULT_THEME_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Generate a styled PDF from a markdown document.",
    )
    parser.add_argument("input", help="Path to the markdown file")
    parser.add_argument(
        "-o", "--output", default=None,
        help="Output PDF path (default: same name as input with .pdf extension)",
    )
    parser.add_argument(
        "--logo", default=None,
        help="Path to logo SVG/PNG, relative to cwd "
             "(default: <script>/../assets/logo.svg if present)",
    )
    parser.add_argument(
        "--theme", default=None,
        help="Path to a YAML or JSON theme file overriding colours and brand text "
             "(default: <script>/../assets/theme.{yaml,yml,json} if present, "
             "otherwise a neutral built-in palette)",
    )
    parser.add_argument(
        "--no-justify", action="store_true", default=False,
        help="Use left-aligned text instead of full justification (default: justify)",
    )
    parser.add_argument(
        "--style", default="tango",
        help="Pygments style for code-block syntax highlighting (default: tango). "
             "Run 'pygmentize -L styles' for available options.",
    )
    parser.add_argument(
        "--no-math", action="store_true", default=False,
        help="Disable LaTeX math parsing ($...$ and $$...$$). "
             "Math is enabled by default via pymdownx.arithmatex + latex2mathml.",
    )
    parser.add_argument(
        "--no-mermaid", action="store_true", default=False,
        help="Disable mermaid fenced-block rendering; the source is emitted as a "
             "plain code block. Mermaid rendering is enabled by default via mmdc.",
    )
    parser.add_argument(
        "--watermark", default=None, metavar="TEXT",
        help="Render TEXT as a page watermark (e.g. \"DRAFT\"). Overrides any "
             "theme-level watermark.text setting. Default: no watermark.",
    )
    parser.add_argument(
        "--no-watermark", action="store_true", default=False,
        help="Force the watermark off, overriding any --watermark or theme value.",
    )
    args = parser.parse_args()

    check_dependencies()

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        output_path = Path(args.output).resolve()
    else:
        output_path = input_path.with_suffix(".pdf")

    logo_path = resolve_logo(args.logo)
    theme = load_theme(resolve_theme_path(args.theme))

    md_text = input_path.read_text()

    # Doc-level directive sits between theme and CLI in the watermark
    # precedence chain: ``theme < doc-directive < CLI``. Authors set
    # a per-document default (e.g. an in-progress doc carries
    # ``watermark.text: DRAFT``); CI / reviewers retain the final word
    # via ``--watermark`` / ``--no-watermark``.
    try:
        directives, md_text = extract_doc_directives(md_text)
    except ValueError as exc:
        print(f"{input_path}: {exc}", file=sys.stderr)
        sys.exit(1)
    if "watermark" in directives:
        wm_override = directives["watermark"] or {}
        if not isinstance(wm_override, dict):
            print(
                f"{input_path}: markdown-pdf directive 'watermark' must "
                f"be a mapping; got {type(wm_override).__name__}",
                file=sys.stderr,
            )
            sys.exit(1)
        theme["watermark"] = _deep_merge(
            theme.get("watermark", {}) or {}, wm_override,
        )
        _resolve_watermark(theme)

    # CLI watermark overrides take precedence over theme + directive.
    # ``--no-watermark`` wins outright; ``--watermark TEXT`` sets the
    # text and re-resolves the watermark colour against the palette
    # (the theme's other watermark attributes are inherited).
    if args.no_watermark:
        theme.setdefault("watermark", {})["text"] = None
    elif args.watermark is not None:
        theme.setdefault("watermark", {})["text"] = args.watermark
        _resolve_watermark(theme)

    # Extract title, strip TOC, split preamble from body
    title = extract_title(md_text)
    md_text = strip_toc_block(md_text)

    # Remove the # title line from the markdown (it's on the title page)
    md_text = re.sub(r"^# .+\n*", "", md_text, count=1)

    # Pre-process fenced ``mermaid`` blocks to inline SVG. Done at
    # the markdown-source level so the rendered block-level <div>
    # passes verbatim through Python-Markdown's raw-HTML handling.
    if not args.no_mermaid:
        md_text = render_mermaid(md_text)

    math_enabled = not args.no_math
    preamble_md, body_md = extract_preamble_and_body(md_text)

    # Conversion order is body-first so its parsed link reference
    # definitions can be handed over to the preamble conversion. The
    # abstract / preamble (everything between the title and the first
    # ## heading) typically uses [text][ref] form to cite needs and
    # cross-document anchors, but the [ref]: url defs live at the
    # bottom of the document — i.e. inside body_md. Without the
    # handover, those references would render as literal text in the
    # title-page abstract. See ``md_to_html`` docstring for details.
    body_html, toc_html, body_refs = md_to_html(
        body_md, pygments_style=args.style, math=math_enabled,
    )
    if math_enabled and toc_html:
        toc_html = render_math_in_toc(toc_html)

    if preamble_md:
        preamble_html, _, _ = md_to_html(
            preamble_md, pygments_style=args.style, math=math_enabled,
            references=body_refs,
        )
    else:
        preamble_html = ""
    pygments_css_text = pygments_css(args.style)

    text_align = "left" if args.no_justify else "justify"
    commit_sha = git_short_sha(input_path.parent)

    print(f"Generating PDF: {output_path}")
    render_pdf(
        title=title,
        preamble_html=preamble_html,
        body_html=body_html,
        toc_html=toc_html,
        output_path=output_path,
        theme=theme,
        logo_path=logo_path,
        base_url=str(input_path.parent),
        text_align=text_align,
        commit_sha=commit_sha,
        pygments_css_text=pygments_css_text,
    )
    print(f"  PDF {output_path}")


if __name__ == "__main__":
    main()
