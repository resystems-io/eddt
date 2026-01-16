GOSRC=$(shell find . -name '*.go')

build/eddt: $(GOSRC) | build
	go build -o $@ cmd/eddt/*.go

build:
	mkdir -p build

clean:
	@rm -rf build

test:
	go test ./...

.PHONY: test
