#!/usr/bin/env bash
# Runs the tests with coverage and enforces a floor per package.
#
# Packages that make decisions must be fully covered. Packages that are thin
# wiring over I/O (main, the HTTP mux, the SQLite driver) are exercised by the
# generic end-to-end run instead and carry a lower floor here.
set -euo pipefail

cd "$(dirname "$0")/.."

declare -A floor=(
  [internal/config]=100
  [internal/targets]=100
  [internal/hooks]=100
  [internal/registry]=100
  [internal/reconcile]=100
  [internal/auth]=100
  [internal/store]=90
  [internal/api]=90
  [internal/ui]=80
  [internal/observability]=0
  [cmd/rolloor]=0
)

profile=$(mktemp)
trap 'rm -f "$profile"' EXIT

go test -race -timeout 5m -coverprofile="$profile" -covermode=atomic ./... >/dev/null

fail=0

for pkg in "${!floor[@]}"; do
  [ -d "$pkg" ] || continue

  pct=$(go tool cover -func="$profile" | awk -v p="github.com/ethpandaops/rolloor/${pkg}/" '
    index($1, p) == 1 { split($3, a, "%"); sum += a[1]; n++ }
    END { if (n == 0) print "0.0"; else printf "%.1f", sum / n }')

  want=${floor[$pkg]}

  if awk -v a="$pct" -v b="$want" 'BEGIN { exit !(a < b) }'; then
    echo "FAIL  ${pkg}  ${pct}% < ${want}%"
    fail=1
  else
    echo "ok    ${pkg}  ${pct}%"
  fi
done

if [ "$fail" -ne 0 ]; then
  echo
  echo "uncovered statements in packages below their floor:"

  for pkg in "${!floor[@]}"; do
    [ -d "$pkg" ] || continue
    go tool cover -func="$profile" | awk -v p="github.com/ethpandaops/rolloor/${pkg}/" -v want="${floor[$pkg]}" '
      index($1, p) == 1 { split($3, a, "%"); if (a[1] + 0 < want + 0) print "  " $1 " " $2 " " $3 }'
  done

  exit 1
fi
