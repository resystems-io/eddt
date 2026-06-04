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

# Pattern rule: .pdf → .svg via dvisvgm
# --no-fonts converts text to paths (ensures correct rendering in all SVG
# consumers including WeasyPrint)
# --optimize canonicalises the SVG structure, which also eliminates the
# non-deterministic font-subset glyph ordering that dvisvgm 3.2.1 produces
# even from identical PDF input — keeping git diff clean on unchanged sources.
%.svg: %.pdf
	$(DVISVGM) --pdf --no-fonts --optimize $< -o $@
