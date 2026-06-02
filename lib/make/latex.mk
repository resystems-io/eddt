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
%.pdf: %.tex
	$(LATEX) $(LATEXFLAGS) $<

.PHONY: latex-clean

latex-clean:
	rm -f *.aux *.log *.fls *.fdb_latexmk *.synctex.gz *.xdv
	rm -f *.out *.toc *.bbl *.blg *.idx *.ilg *.ind
