GOSRC=$(shell find . -name '*.go')

# Disable VCS stamping when built inside a git submodule — Go would
# record the *parent* repo's commit, not this module's.
BUILDVCS := $(if $(shell git rev-parse --show-superproject-working-tree 2>/dev/null),-buildvcs=false,)

all: build/eddt build/parquet-annotator build/arrow-writer-gen

build/eddt: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/eddt

build/parquet-annotator: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/parquet-annotator

build/arrow-writer-gen: $(GOSRC) | build
	go build $(BUILDVCS) -o $@ ./cmd/arrow-writer-gen

build:
	mkdir -p build

clean:
	@rm -rf build

test:
	go test ./...

.PHONY: test
