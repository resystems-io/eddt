# designs/scripts/tests/Makefile — smoke tests for the shared markdown tooling
#
# Usage:
#   make                        — run all tests (default)
#   make test-mermaid           — run the mermaid pipeline smoke test
#   make test-markdown-shared   — run the shared _markdown_shared.py unit tests
#   make test-toc               — run the markdown-toc.py unit tests
#   make test-markdown-pdf      — run the markdown-pdf.py size-variant tests
#   make test-markdown-num      — run the markdown-num.py unit tests
#   make test-markdown-index    — run the markdown-index.py unit tests
#   make test-markdown-table    — run the markdown-table.py unit tests
#   make clean                  — remove generated PDFs and the shared mermaid cache
#
# test_mermaid.py invokes markdown-pdf.py against mermaid-showcase.md
# and asserts on the intermediate HTML; mermaid-showcase.pdf is
# produced as a side-effect.
# test_markdown_shared.py exercises the shared _markdown_shared.py
# module (comment/fence-aware line scanning, and other recurring
# patterns as they're added) that markdown-toc.py, markdown-pdf.py,
# markdown-num.py, markdown-index.py, and refs-linkify.py all depend on.
# test_markdown_toc.py exercises markdown-toc.py against synthesised
# fixtures under this directory.
# test_markdown_pdf.py exercises the size-variant code fences
# (small-text / tiny-text / large-text) supported by markdown-pdf.py
# via pymdownx.superfences + pymdownx.highlight.
# test_markdown_num.py exercises markdown-num.py's heading renumbering
# against synthesised fixtures under this directory.
# test_markdown_index.py exercises markdown-index.py's alphabetical-index
# regeneration against synthesised fixtures under this directory.

.DEFAULT_GOAL := test

include ../../../lib/make/markdown.mk

.PHONY: test test-mermaid test-markdown-shared test-toc test-markdown-pdf test-markdown-num test-markdown-index test-refs-linkify test-markdown-table clean

test: test-mermaid test-markdown-shared test-toc test-markdown-pdf test-markdown-num test-markdown-index test-refs-linkify test-markdown-table

test-mermaid:
	python3 test_mermaid.py

test-markdown-shared:
	python3 test_markdown_shared.py

test-toc:
	python3 test_markdown_toc.py

test-markdown-pdf:
	python3 test_markdown_pdf.py

test-markdown-num:
	python3 test_markdown_num.py

test-markdown-index:
	python3 test_markdown_index.py

test-refs-linkify:
	python3 test_refs_linkify.py

test-markdown-table:
	python3 test_markdown_table.py

clean: mermaid-clean
	rm -f *.pdf
