# Makefile for eddt design documents → PDF
#
# Usage:
#   make            — build diagrams, refresh cross-reference link blocks,
#                     then generate PDFs from current markdown sources
#   make diagrams   — build SVG diagrams from .tex sources (delegates to diagrams/)
#   make linkify    — refresh cross-reference link blocks only
#   make clean      — remove generated PDFs and build artefacts

.DEFAULT_GOAL := all

# Scripts live inside docs/ in this project; override the default
# used by markdown.mk before including it.
SCRIPTS = scripts

include ../lib/make/parallel.mk
include ../lib/make/markdown.mk

SOURCES = arrow-reader-gen.md \
          arrow-reader-gen-refinements.md \
          arrow-writer-gen.md \
          arrow-writer-gen-multipkg.md \
          arrow-writer-gen-nesting.md \
          arrow-writer-gen-refinements.md \
          delta-gen-refinements.md \
          eddt-context.md \
          eddt-glossary.md \
          eddt-needs.md \
          future-plans.md \
          parquet-dataflow.md \
          refinement.md \
          release-notes.md

PDFS = $(SOURCES:.md=.pdf)

# Index docs that host canonical traceability-identifier definitions in
# the X-XXX-NN format expected by refs-linkify. The script scans these
# for definitions and rewrites references in every source.
#
# Currently restricted to eddt-needs.md (N-EDDT-NN / N-ECOL-NN).
# The arrow-*-refinements.md docs use a different identifier scheme
# (R1, R2, B1, …) that refs-linkify does not recognise.
# The delta-gen-refinements.md doc uses flat R-NN identifiers that
# produce duplicate-definition errors and will be superseded by the
# forthcoming delta-gen-spec.md with R-DG-NN / N-DG-NN identifiers.
INDEXES = eddt-needs.md

# refs-linkify maintains the unified cross-reference block at the
# foot of each source. Idempotent: repeated runs produce no change
# when references already resolve. Run as part of the default build
# so unresolved references and parser regressions surface early
# (rather than waiting for a missing-link to bite at PDF render time
# or in a downstream PR review).
#
# delta-gen-refinements.md is excluded from LINKIFY_SOURCES because it
# uses a flat R-NN / C-NN / D-NN / E-NN identifier scheme that does
# not match the X-XXX-NN format refs-linkify recognises; including it
# generates spurious empty-anchor link targets. It will be re-included
# once superseded by the forthcoming delta-gen-spec.md with R-DG-NN /
# N-DG-NN / E-DG-NN identifiers.
REFS_LINKIFY    = $(SCRIPTS)/refs-linkify.py
INDEX_FLAGS     = $(foreach idx,$(INDEXES),--index $(idx))
LINKIFY_SOURCES = $(filter-out delta-gen-refinements.md,$(SOURCES))

all: diagrams linkify $(PDFS)

.PHONY: diagrams
diagrams:
	$(MAKE) -C diagrams

.PHONY: linkify
linkify: build/linkify

build/linkify: $(LINKIFY_SOURCES) $(REFS_LINKIFY) | build
	@for src in $(LINKIFY_SOURCES); do \
	    python3 $(REFS_LINKIFY) $(INDEX_FLAGS) $$src || exit $$?; \
	done
	@touch $@

build:
	@mkdir $@

clean:
	rm -rf build/
	rm -f $(PDFS)
	$(MAKE) -C diagrams clean

.PHONY: all clean
