# lib/make/markdown.mk — Shared Markdown → PDF conversion rules
#
# Provides:
#   MD_PDF, MD_TOC, MD_INDEX, NEEDS_LINK — paths to the helper scripts
#   %.pdf: %.md — pattern rule for markdown → PDF via markdown-pdf.py
#
# The includer may override SCRIPTS if its scripts directory sits
# somewhere other than ../scripts relative to the Makefile. The
# markdown-pdf.py script auto-discovers its logo and theme from
# <script>/../assets/, so no --logo/--theme flags are passed here —
# drop files into that directory to customise the output.
#
# Usage:
#   include ../lib/make/markdown.mk

SCRIPTS ?= ../scripts

MD_PDF     = $(SCRIPTS)/markdown-pdf.py
MD_TOC     = $(SCRIPTS)/markdown-toc.py
MD_INDEX   = $(SCRIPTS)/markdown-index.py
NEEDS_LINK = $(SCRIPTS)/needs-linkify.py

# Pattern rule: markdown → PDF. Rebuilds when the .md source or the
# markdown-pdf.py script itself changes.
%.pdf: %.md $(MD_PDF)
	$(MD_PDF) $<

.PHONY: markdown-clean

markdown-clean:
	rm -f *.pdf
