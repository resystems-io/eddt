# lib/make/latex.mk — Shared LaTeX compilation rules
#
# Provides:
#   LATEX, LATEXFLAGS — XeLaTeX compiler and flags
#   LATEXMK, LATEXMK_FLAGS — latexmk wrapper and flags
#   %.pdf: %.tex — pattern rule for standalone .tex → .pdf
#   latex-clean — remove LaTeX build artifacts
#
# Usage:
#   Include from a Makefile:
#     include ../../lib/make/latex.mk   (adjust path as needed)

LATEX       ?= xelatex
LATEXFLAGS  ?= -interaction=nonstopmode -halt-on-error

LATEXMK       ?= latexmk
LATEXMK_FLAGS ?= -pdfxe -shell-escape -interaction=nonstopmode

# Pattern rule: standalone .tex → .pdf via xelatex
#
# SOURCE_DATE_EPOCH=0 pins the build timestamp so xdvipdfmx produces
# byte-identical PDFs on every run. Without it, xdvipdfmx embeds a
# time-dependent CreationDate and /ID that cause font-subset glyph
# definitions to be emitted in a different order each run; dvisvgm
# faithfully reproduces that ordering in the SVG, making git see a
# changed file after every rebuild even when the .tex is unchanged.
# Source version is tracked by the git commit hash, not the build date.
# (Reproducible Builds spec: reproducible-builds.org/specs/source-date-epoch)
%.pdf: %.tex
	SOURCE_DATE_EPOCH=0 $(LATEX) $(LATEXFLAGS) $<

.PHONY: latex-clean

latex-clean:
	rm -f *.aux *.log *.fls *.fdb_latexmk *.synctex.gz *.xdv
	rm -f *.out *.toc *.bbl *.blg *.idx *.ilg *.ind
