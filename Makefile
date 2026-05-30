GOSRC=$(shell find . -name '*.go')

# Disable VCS stamping when built inside a git submodule — Go would
# record the *parent* repo's commit, not this module's.
BUILDVCS := $(if $(shell git rev-parse --show-superproject-working-tree 2>/dev/null),-buildvcs=false,)

all: docs build/eddt build/parquet-annotator build/arrow-writer-gen build/arrow-reader-gen build/delta-gen

build/eddt: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/eddt

build/parquet-annotator: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/parquet-annotator

build/arrow-writer-gen: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/arrow-writer-gen

build/arrow-reader-gen: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/arrow-reader-gen

build/delta-gen: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/delta-gen

build:
	mkdir -p build

docs:
	$(MAKE) -C docs

docs-clean:
	$(MAKE) -C docs clean

clean: docs-clean
	@rm -rf build

test:
	go test ./...

.PHONY: test clean test docs docs-clean
