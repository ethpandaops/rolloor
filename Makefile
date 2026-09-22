.PHONY: build test cover lint words run example

build:
	CGO_ENABLED=0 go build -trimpath -o bin/rolloor ./cmd/rolloor

test:
	go test -race -timeout 5m ./...

# Full coverage on the packages that decide things; see scripts/coverage.sh.
cover:
	./scripts/coverage.sh

lint: words
	golangci-lint run --timeout=10m

# The binary must not know the workload. See tasks/prd.md §1.
words:
	./scripts/lint-words.sh

run:
	go run ./cmd/rolloor serve --config examples/generic/config.yaml

example:
	./examples/generic/run.sh
