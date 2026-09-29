#!/usr/bin/env bash
# Runs every fuzz target in the repository for a while each (default 1m).
# A failing input is saved under the package's testdata/fuzz directory;
# committing it makes it part of every go test run.
set -euo pipefail

cd "$(dirname "$0")/.."

fuzztime=${1:-1m}

git grep -E '^func Fuzz[A-Za-z0-9_]+\(' -- '*_test.go' | while IFS=: read -r file decl; do
  pkg=./$(dirname "$file")
  name=$(sed -E 's/^func (Fuzz[A-Za-z0-9_]+)\(.*/\1/' <<<"$decl")

  echo "== $pkg $name for $fuzztime"
  go test -run '^$' -fuzz "^${name}\$" -fuzztime "$fuzztime" "$pkg"
done
