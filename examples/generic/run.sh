#!/usr/bin/env bash
# The generic end-to-end run. Six nginx containers on a local registry move
# from build A to build B in waves and batches under the disruption budget; a broken
# build C halts and quarantines; build D supersedes and converges with the
# quarantined targets first. Needs docker, curl and jq.
set -euo pipefail

cd "$(dirname "$0")/../.."

compose=examples/generic/docker-compose.yaml
api=http://127.0.0.1:8080/api/v1
export ROLLOOR_EXAMPLE_COMPOSE=$compose

log() { printf '\n== %s\n' "$*"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ] && [ -f /tmp/rolloor-example/rolloor.log ]; then
    echo "== controller log (last 40 lines)" >&2
    tail -n 40 /tmp/rolloor-example/rolloor.log >&2
  fi
  [ -n "${rolloor_pid:-}" ] && kill "$rolloor_pid" 2>/dev/null || true
  docker compose -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf /tmp/rolloor-example
}
trap cleanup EXIT

# build pushes one variant of the app as localhost:5050/app:latest.
build() {
  local variant=$1 body=$2 status=${3:-200}
  local dir
  dir=$(mktemp -d)
  cat >"$dir/default.conf" <<EOF
server { listen 80; location / { return $status '$body\n'; add_header Content-Type text/plain; } }
EOF
  cat >"$dir/Dockerfile" <<'EOF'
FROM nginx:alpine
COPY default.conf /etc/nginx/conf.d/default.conf
LABEL org.opencontainers.image.revision=REV
EOF
  sed -i.bak "s/REV/$variant/" "$dir/Dockerfile"
  docker build -q -t localhost:5050/app:latest "$dir" >/dev/null
  docker push -q localhost:5050/app:latest >/dev/null
  rm -rf "$dir"
}

# wait_state polls a rollout's state until it matches or the deadline passes.
wait_state() {
  local rollout=$1 want=$2 deadline=$((SECONDS + ${3:-240}))
  while [ $SECONDS -lt $deadline ]; do
    state=$(curl -fsS "$api/rollouts/$rollout" | jq -r .state)
    if [ "$state" = "$want" ]; then return 0; fi
    sleep 2
  done
  echo "rollout $rollout is $state, wanted $want" >&2
  curl -fsS "$api/rollouts/$rollout" | jq . >&2
  return 1
}

active_rollout() {
  curl -fsS "$api/rollouts" | jq -r '[.[] | select(.state != "Complete" and .state != "Aborted" and .state != "Superseded")][0].id // empty'
}

log "registry and build A"
docker compose -f "$compose" up -d registry >/dev/null
for _ in $(seq 1 30); do curl -fsS http://localhost:5050/v2/ >/dev/null 2>&1 && break; sleep 1; done
build A "build A"

log "six containers on build A"
docker compose -f "$compose" up -d --pull always >/dev/null
for port in 18081 18082 18083 18084 18085 18086; do
  for _ in $(seq 1 30); do curl -fsS "http://127.0.0.1:$port/" >/dev/null 2>&1 && break; sleep 1; done
done

log "rolloor"
mkdir -p /tmp/rolloor-example
go build -o /tmp/rolloor-example/rolloor ./cmd/rolloor
/tmp/rolloor-example/rolloor validate --config examples/generic/config.yaml
/tmp/rolloor-example/rolloor serve --config examples/generic/config.yaml >/tmp/rolloor-example/rolloor.log 2>&1 &
rolloor_pid=$!
for _ in $(seq 1 30); do curl -fsS "$api/fleet" >/dev/null 2>&1 && break; sleep 1; done

# Everything on A reads Synced before anything happens.
for _ in $(seq 1 30); do
  synced=$(curl -fsS "$api/targets" | jq '[.[] | select(.sync == "Synced")] | length')
  [ "$synced" = "6" ] && break
  sleep 1
done
[ "$synced" = "6" ] || { echo "expected 6 synced targets, got $synced" >&2; curl -fsS "$api/targets" | jq . >&2; exit 1; }
[ "$(curl -fsS "$api/rollouts" | jq length)" = "0" ]

log "build B: waves and batches under the disruption budget"
build B "build B"
for _ in $(seq 1 30); do r1=$(active_rollout); [ -n "$r1" ] && break; sleep 1; done
[ -n "$r1" ] || { echo "no rollout opened for B" >&2; exit 1; }
wait_state "$r1" Complete 300
curl -fsS "$api/rollouts/$r1" | jq -r '.batches[] | "batch \(.number) wave \(.wave): \(.targets | join(", "))"'

first_wave=$(curl -fsS "$api/rollouts/$r1" | jq -r '.batches[0].wave')
[ "$first_wave" = "0" ] || { echo "wave 0 did not go first" >&2; exit 1; }
weighted_per_batch=$(curl -fsS "$api/rollouts/$r1" | jq '[.batches[] | select(.wave == 1) | .targets | length] | max')
[ "$weighted_per_batch" = "1" ] || { echo "the disruption budget allowed $weighted_per_batch weighted nodes at once, wanted 1" >&2; exit 1; }
[ "$(curl -fsS "$api/rollouts/$r1" | jq -r .revision)" = "B" ]

log "build C is broken: halt and quarantine"
build C "build C" 500
for _ in $(seq 1 30); do r2=$(active_rollout); [ -n "$r2" ] && [ "$r2" != "$r1" ] && break; sleep 1; done
wait_state "$r2" Halted 120
curl -fsS "$api/rollouts/$r2" | jq -r .reason
degraded=$(curl -fsS "$api/targets" | jq '[.[] | select(.health == "Degraded")] | length')
untouched=$(curl -fsS "$api/targets" | jq '[.[] | select(.live | endswith("'"$(curl -fsS "$api/rollouts/$r1" | jq -r '.desired[]' | sed 's/.*://')"'"))] | length')
echo "degraded: $degraded, still on B: $untouched"
[ "$degraded" -ge 1 ] && [ "$untouched" -ge 1 ]

log "build D supersedes and converges, quarantined targets first"
build D "build D"
for _ in $(seq 1 30); do r3=$(active_rollout); [ -n "$r3" ] && [ "$r3" != "$r2" ] && break; sleep 1; done
wait_state "$r2" Superseded 30
first_target=$(curl -fsS "$api/rollouts/$r3" | jq -r '.targets[0].degradedBefore')
[ "$first_target" = "true" ] || { echo "degraded targets did not go first" >&2; exit 1; }
wait_state "$r3" Complete 300
[ "$(curl -fsS "$api/targets" | jq '[.[] | select(.health == "Healthy" and .sync == "Synced")] | length')" = "6" ]

log "history"
curl -fsS "$api/history?limit=200" | jq -r '.[] | "\(.at | .[11:19]) \(.actor) \(.action) \(.reason)"' | tail -n 30

log "ok"
