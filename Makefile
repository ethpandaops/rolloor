.PHONY: build test cover lint words run example simulate fuzz

build:
	CGO_ENABLED=0 go build -trimpath -o bin/rolloor ./cmd/rolloor

test:
	go test -race -timeout 5m ./...

# Full coverage on the packages that decide things; see scripts/coverage.sh.
cover:
	./scripts/coverage.sh

lint: words
	golangci-lint run --timeout=10m

# Nothing in the repository may be workload-specific. See tasks/prd.md §1.
words:
	./scripts/lint-words.sh

run:
	go run ./cmd/rolloor serve --config examples/generic/config.yaml

example:
	./examples/generic/run.sh

# The controller simulation at 10,000 seeds; see internal/reconcile/property_test.go.
simulate:
	ROLLOOR_SIM_SEEDS=10000 go test -count=1 -timeout 30m -run TestPropertyControllerInvariants ./internal/reconcile

FUZZTIME ?= 1m

# Every fuzz target for FUZZTIME each.
fuzz:
	./scripts/fuzz.sh $(FUZZTIME)
