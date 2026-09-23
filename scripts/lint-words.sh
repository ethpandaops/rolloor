#!/usr/bin/env bash
# Fails if a word from the first workload appears in any tracked file. Nothing
# in this repository is workload-specific; deployments bring their own hooks.
set -euo pipefail

cd "$(dirname "$0")/.."

words='beacon|validators?|slots?|epochs?|finality|watchtower|ethereum|cartographoor|coredevs|authentik|devnets?|lighthouse|prysm|teku|nimbus|lodestar|grandine|besu|nethermind|erigon|geth'

if hits=$(git grep -nIiwE "(${words})" -- . ':!scripts/lint-words.sh' ':!go.sum' ':!internal/ui/static/htmx.min.js'); then
  echo "workload words found:" >&2
  echo "$hits" >&2
  exit 1
fi

echo "words: clean"
