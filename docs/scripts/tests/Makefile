# designs/scripts/tests/Makefile — smoke tests for the shared markdown tooling
#
# Usage:
#   make                   — run all tests (default)
#   make test-mermaid      — run the mermaid pipeline smoke test
#   make test-toc          — run the markdown-toc.py unit tests
#   make test-markdown-pdf — run the markdown-pdf.py size-variant tests
#   make clean             — remove generated PDFs and the shared mermaid cache
#
# test_mermaid.py invokes markdown-pdf.py against mermaid-showcase.md
# and asserts on the intermediate HTML; mermaid-showcase.pdf is
# produced as a side-effect.
# test_markdown_toc.py exercises markdown-toc.py against synthesised
# fixtures under this directory.
# test_markdown_pdf.py exercises the size-variant code fences
# (small-text / tiny-text / large-text) supported by markdown-pdf.py
# via pymdownx.superfences + pymdownx.highlight.

.DEFAULT_GOAL := test

include ../../../lib/make/markdown.mk

.PHONY: test test-mermaid test-toc test-markdown-pdf clean

test: test-mermaid test-toc test-markdown-pdf

test-mermaid:
	python3 test_mermaid.py

test-toc:
	python3 test_markdown_toc.py

test-markdown-pdf:
	python3 test_markdown_pdf.py

clean: mermaid-clean
	rm -f *.pdf
