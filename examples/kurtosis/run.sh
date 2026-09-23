#!/usr/bin/env bash
# Runs rolloor against a real four-node devnet in Kurtosis and checks that it
# rolls lighthouse from build A to B under the disruption budget, halts on a
# broken build C, and converges past it when D arrives.
#
# What stands in for what on a real devnet:
#   Kurtosis enclave          the devnet's hosts
#   one watchtower            each host's updater (wt-<host>)
#   enclave IPs               each host's bn- and rpc- endpoints
#   the rolloor container     the devnet's rolloor instance
#   render_targets below      the ansible template that writes targets.yaml
#
# Needs docker, kurtosis, curl, jq and openssl.
set -euo pipefail

cd "$(dirname "$0")/../.."

enclave=rolloor-example
package=github.com/ethpandaops/ethereum-package@6.1.0
base=sigp/lighthouse:v8.2.2
registry=localhost:5055
image=$registry/lighthouse:example
work=/tmp/rolloor-kurtosis
api=http://127.0.0.1:8090/api/v1
export UPDATER_TOKEN=example-token

log() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  if [ -d "$work" ]; then
    docker logs rolloor-example-controller >"$work/rolloor.log" 2>&1 || true
  fi
  docker rm -f rolloor-example-controller rolloor-example-updater rolloor-example-registry >/dev/null 2>&1 || true
  kurtosis enclave rm -f "$enclave" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
rm -rf "$work" && mkdir -p "$work/targets.d" "$work/builds"

# --- the registry: TLS with its own certificate, because the updater speaks
# only HTTPS to registries.
log "registry"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$work/registry.key" -out "$work/registry.crt" 2>/dev/null
docker run -d --name rolloor-example-registry -p 5055:5000 -v "$work:/certs:ro" \
  -e REGISTRY_HTTP_TLS_CERTIFICATE=/certs/registry.crt -e REGISTRY_HTTP_TLS_KEY=/certs/registry.key \
  registry:2 >/dev/null
for _ in $(seq 1 30); do curl -sfk "https://$registry/v2/" >/dev/null && break; sleep 1; done

# publish NAME DOCKERFILE-BODY: builds a lighthouse variant and pushes it to
# the tag rolloor follows; prints its digest.
publish() {
  mkdir -p "$work/builds/$1"
  printf 'FROM %s\n%s\n' "$base" "$2" >"$work/builds/$1/Dockerfile"
  docker build -q -t "$image" "$work/builds/$1" >/dev/null
  docker push -q "$image" >/dev/null
  curl -sfkI -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "https://$registry/v2/lighthouse/manifests/example" | awk -F': ' 'tolower($1) == "docker-content-digest" { print $2 }' | tr -d '\r'
}

log "build A"
A=$(publish a 'LABEL rolloor.example.build=a')
echo "A = $A"

# --- the devnet.
log "devnet"
kurtosis run --enclave "$enclave" "$package" --args-file examples/kurtosis/network.yaml >"$work/kurtosis.log" 2>&1 ||
  { tail -30 "$work/kurtosis.log"; fail "the devnet did not start"; }

container() { docker ps -a --format '{{.Names}}' | grep "^$1--"; }
ip_of() { docker inspect "$(container "$1")" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'; }

# --- the updater: one watchtower for the whole docker host, polling off,
# API on. A devnet runs one per host. Its API allows 60 authenticated calls a
# minute per caller by default; here it answers every inspect of all twelve
# containers, so the limit is raised. --revive-stopped starts a container
# that a broken build left stopped once a good build replaces it.
log "updater"
mapfile -t watched < <(docker ps --format '{{.Names}}' | grep -E '^(el|cl|vc)-[0-9]+-')
docker run -d --name rolloor-example-updater --network host \
  -v /var/run/docker.sock:/var/run/docker.sock -v "$work/registry.crt:/registry.crt:ro" \
  -e SSL_CERT_FILE=/registry.crt -e WATCHTOWER_HTTP_API_TOKEN="$UPDATER_TOKEN" \
  ghcr.io/nicholas-fedor/watchtower:latest \
  --http-api-endpoints=update,containers,check --http-api-port=8089 \
  --http-api-rate-limit=6000 \
  --include-stopped --revive-stopped --include-restarting --stop-timeout=30s "${watched[@]}" >/dev/null

# --- the targets file, as ansible would write it from an inventory: one node
# per participant, three targets on each.
log "targets"
weight=$(awk '/validator_count:/ { print $2 }' examples/kurtosis/network.yaml)
render_targets() {
  local i el cl vc
  echo "# Written by run.sh from the running enclave; one node per participant."
  for i in 1 2 3 4; do
    el=el-$i-geth-lighthouse cl=cl-$i-lighthouse-geth vc=vc-$i-geth-lighthouse
    cat <<TARGETS

- id: node-$i/execution
  node: node-$i
  address: $(ip_of "$el")
  weight: $weight
  image: ethereum/client-go:v1.17.6
  labels:
    client: geth
    owner: geth
    role: el
  hooks:
    ready: ready-execution
  extra:
    container: $(container "$el")
    updater: http://127.0.0.1:8089
    rpc: http://$(ip_of "$el"):8545

- id: node-$i/beacon
  node: node-$i
  address: $(ip_of "$cl")
  weight: $weight
  image: $image
  labels:
    client: lighthouse
    owner: lighthouse
    role: cl
  hooks:
    ready: ready-beacon
    soak: soak-beacon
  extra:
    container: $(container "$cl")
    updater: http://127.0.0.1:8089
    beacon: http://$(ip_of "$cl"):4000

- id: node-$i/validator
  node: node-$i
  address: $(ip_of "$vc")
  weight: $weight
  image: $image
  labels:
    client: lighthouse
    owner: lighthouse
    role: vc
  hooks:
    ready: ready-running
  extra:
    container: $(container "$vc")
    updater: http://127.0.0.1:8089
TARGETS
  done
}
render_targets >"$work/targets.d/targets.yaml"

# --- rolloor, from this repository's image, laid out as a deployment.
log "rolloor"
docker build -q -t rolloor:example . >/dev/null
docker run -d --name rolloor-example-controller --network host \
  -e UPDATER_TOKEN \
  -v "$PWD/examples/kurtosis/config.yaml:/etc/rolloor/config.yaml:ro" \
  -v "$PWD/examples/kurtosis/hooks:/etc/rolloor/hooks:ro" \
  -v "$work/targets.d:/etc/rolloor/targets.d:ro" \
  -v "$work/registry.crt:/etc/rolloor/registry.crt:ro" \
  rolloor:example >/dev/null
for _ in $(seq 1 30); do curl -sf "$api/fleet" >/dev/null && break; sleep 1; done

# wait_until DESCRIPTION SECONDS JQ-FILTER URL: polls until the filter is true.
wait_until() {
  local deadline=$((SECONDS + $2))
  until curl -sf "$4" | jq -e "$3" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || { curl -s "$4" | jq . | tail -40; fail "$1"; }
    sleep 5
  done
}

rollout_of() { curl -sf "$api/rollouts" | jq -r --arg d "${1#sha256:}" '[.[] | select(.desired[] | ltrimstr("sha256:") == $d)][0].id // empty'; }

# all_on DIGEST: every lighthouse target runs DIGEST, and every target is
# healthy and in sync.
all_on() {
  printf '[.[] | select(.sync != "Synced" or .health != "Healthy" or (.group == "lighthouse" and .live != "%s"))] | length == 0' "$1"
}

log "every target synced and healthy on A"
wait_until "targets did not all sync on A" 300 "$(all_on "$A")" "$api/targets"

log "build B: one node first, then at most two at once"
B=$(publish b 'LABEL rolloor.example.build=b')
echo "B = $B"
for _ in $(seq 1 60); do rB=$(rollout_of "$B"); [ -n "$rB" ] && break; sleep 5; done
[ -n "$rB" ] || fail "no rollout for B"
wait_until "B did not complete" 1200 '.state == "Complete"' "$api/rollouts/$rB"
curl -sf "$api/rollouts/$rB" | jq -e '[.targets[] | select(.phase != "passed")] | length == 0' >/dev/null ||
  fail "B completed without moving every target"
curl -sf "$api/rollouts/$rB" | jq -e '
  [.batches[] | [.targets[] | split("/")[0]] | unique | length] as $nodes
  | $nodes[0] == 1 and ($nodes | max) <= 2 and ([.batches[].targets | length] | add) == 8' >/dev/null ||
  fail "batches broke the strategy or the budget"

log "build C is broken: the rollout halts on its first node"
C=$(publish c 'RUN printf "#!/bin/sh\necho broken build >&2\nexit 1\n" > /usr/local/bin/lighthouse')
echo "C = $C"
for _ in $(seq 1 60); do rC=$(rollout_of "$C"); [ -n "$rC" ] && break; sleep 5; done
[ -n "$rC" ] || fail "no rollout for C"
wait_until "C did not halt" 600 '.state == "Halted"' "$api/rollouts/$rC"
curl -sf "$api/rollouts/$rC" | jq -e '[.batches[0].targets[] | split("/")[0]] | unique | length == 1' >/dev/null ||
  fail "the halted batch was not one node"

log "build D supersedes C and converges, the quarantined node first"
D=$(publish d 'LABEL rolloor.example.build=d')
echo "D = $D"
wait_until "C was not superseded" 120 '.state == "Superseded"' "$api/rollouts/$rC"
for _ in $(seq 1 60); do rD=$(rollout_of "$D"); [ -n "$rD" ] && break; sleep 5; done
[ -n "$rD" ] || fail "no rollout for D"
wait_until "D did not complete" 1200 '.state == "Complete"' "$api/rollouts/$rD"
curl -sf "$api/rollouts/$rD" | jq -e '[.targets[] | select(.phase != "passed")] | length == 0' >/dev/null ||
  fail "D completed without moving every target"
curl -sf "$api/rollouts/$rD" | jq -e '.targets[0].degradedBefore == true' >/dev/null || fail "the quarantined node did not go first"

log "every target synced and healthy on D"
wait_until "targets did not all end healthy on D" 300 "$(all_on "$D")" "$api/targets"

log "history"
curl -sf "$api/history?limit=60" | jq -r 'reverse[] | "\(.at[11:19]) \(.actor) \(.action) \(.reason // "")"'

log "ok"
