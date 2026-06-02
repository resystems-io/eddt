# Makefile for TikZ Diagrams → PDF → SVG
#
# Usage:
#   make          — compile all diagrams to PDF and SVG
#   make clean    — remove generated files
#
# Requires: xelatex (TeX Live), dvisvgm (TeX Live)

.DEFAULT_GOAL := all

LIB = ../../lib/make
include $(LIB)/latex.mk
include $(LIB)/svg.mk

SOURCES = $(wildcard *.tex)
PDFS    = $(SOURCES:.tex=.pdf)
SVGS    = $(SOURCES:.tex=.svg)

# Keep intermediate PDFs (make would otherwise delete them)
.PRECIOUS: $(PDFS)

.PHONY: all clean

all: $(SVGS)

clean: latex-clean
	rm -f $(PDFS) $(SVGS)
