#!/usr/bin/env bash
# Runs the tests with coverage and enforces a statement-coverage floor per package.
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
output=$(mktemp)
trap 'rm -f "$profile" "$output"' EXIT

if ! go test -race -timeout 5m -coverprofile="$profile" -covermode=atomic ./... >"$output" 2>&1; then
  grep -vE '^ok|no test files' "$output" >&2
  exit 1
fi

module=github.com/ethpandaops/rolloor
fail=0

# Statement coverage per package: sum of statements in blocks hit at least once
# over all statements. The profile format is file:start,end numStmts hitCount.
pct_for() {
  awk -v p="${module}/$1/" '
    NR > 1 && index($1, p) == 1 {
      rest = substr($1, length(p) + 1)
      if (index(rest, "/") == 0) { total += $2; if ($3 > 0) hit += $2 }
    }
    END { if (total == 0) print "0.0"; else printf "%.1f", 100 * hit / total }' "$profile"
}

for pkg in $(printf '%s\n' "${!floor[@]}" | sort); do
  [ -d "$pkg" ] || continue

  pct=$(pct_for "$pkg")
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
  echo "functions below 100% in failing packages:"

  for pkg in $(printf '%s\n' "${!floor[@]}" | sort); do
    [ -d "$pkg" ] || continue

    pct=$(pct_for "$pkg")
    if awk -v a="$pct" -v b="${floor[$pkg]}" 'BEGIN { exit !(a < b) }'; then
      go tool cover -func="$profile" | awk -v p="${module}/${pkg}/" '
        index($1, p) == 1 && $3 != "100.0%" { print "  " $1 " " $2 " " $3 }'
    fi
  done

  exit 1
fi
