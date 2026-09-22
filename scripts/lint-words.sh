#!/usr/bin/env bash
# Fails if a workload word appears in the Go tree. The controller is generic;
# these words belong in contrib/ and examples/ only.
set -euo pipefail

cd "$(dirname "$0")/.."

words='beacon|validator|slot|epoch|finality|watchtower|ethereum|cartographoor|coredevs|authentik'

if hits=$(grep -rniE --include='*.go' --include='*.html' --include='*.sql' "\b(${words})\b" cmd internal 2>/dev/null); then
  echo "workload words found in the Go tree:" >&2
  echo "$hits" >&2
  exit 1
fi

echo "words: clean"
