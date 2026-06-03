# lib/make/svg.mk — Shared PDF → SVG conversion rules
#
# Provides:
#   DVISVGM — dvisvgm binary (from TeX Live)
#   %.svg: %.pdf — pattern rule for PDF → SVG conversion
#
# Usage:
#   Include from a Makefile:
#     include ../../lib/make/svg.mk   (adjust path as needed)

DVISVGM ?= dvisvgm

# Optional post-processing hook invoked after dvisvgm to canonicalise the
# SVG output. Override in the including Makefile when the dvisvgm version
# in use produces non-deterministic output (see docs/scripts/svg_canonicalise.py).
# Default: the shell no-op ':'.
SVG_CANONICALISE ?= :

# Pattern rule: .pdf → .svg via dvisvgm
# --font-format=woff2 embeds fonts; --no-fonts converts text to paths
# (paths ensure correct rendering in all SVG consumers including WeasyPrint)
%.svg: %.pdf
	$(DVISVGM) --pdf --no-fonts $< -o $@
	$(SVG_CANONICALISE) $@
