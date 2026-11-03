VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BIN     ?= bin/queuesim
GO      ?= go

.PHONY: build run list test test-short vet fmt fmt-check check clean

build:
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN) ./cmd/queuesim

run:
	$(GO) run ./cmd/queuesim serve -open

list:
	$(GO) run ./cmd/queuesim list

test:
	$(GO) test -count=1 -timeout 300s ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w cmd internal model models godes

fmt-check:
	@out=$$(gofmt -l cmd internal model models godes | grep -v '^$$' || true); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt-check vet test

clean:
	rm -rf bin
