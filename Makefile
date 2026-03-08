GOSRC=$(shell find . -name '*.go')

all: build/eddt build/parquet-annotator build/arrow-writer-gen

build/eddt: $(GOSRC) | build
	go build -o $@ cmd/eddt/*.go

build/parquet-annotator: $(GOSRC) | build
	go build -o $@ cmd/parquet-annotator/*.go

build/arrow-writer-gen: $(GOSRC) | build
	go build -o $@ cmd/arrow-writer-gen/*.go

build:
	mkdir -p build

clean:
	@rm -rf build

test:
	go test ./...

.PHONY: test
