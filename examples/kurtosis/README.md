# Kurtosis devnet example

rolloor rolling a real devnet: four nodes of geth, a lighthouse beacon node and a lighthouse validator client, started by [ethereum-package](https://github.com/ethpandaops/ethereum-package) in Kurtosis. CI runs it on every change.

```sh
./examples/kurtosis/run.sh
```

Needs docker, kurtosis, curl, jq and openssl. It takes about half an hour.

## What happens

1. A local registry serves `localhost:5055/lighthouse:example`, build A.
2. ethereum-package starts the devnet from `network.yaml`.
3. A watchtower starts with its API on and polling off: the updater rolloor asks to restart containers.
4. `run.sh` writes the targets file from the running enclave, then starts rolloor from this repository's image.
5. Build B is pushed. rolloor updates one node (beacon and validator), soaks it against the three it hasn't reached, then updates the rest two nodes at a time, never more than `maxUnavailable` allows.
6. Build C, whose lighthouse exits at once, is pushed. The first node fails to become ready and the rollout halts with that node quarantined; the other three stay on B.
7. Build D is pushed. The halted rollout is superseded and D converges, the quarantined node first.

## How it maps to a real devnet

| here | on a devnet |
|---|---|
| the Kurtosis enclave | the devnet's hosts |
| one watchtower on `127.0.0.1:8089` | a watchtower on each host, reached at `https://wt-<host>...` |
| container IPs on the enclave network | each host's `bn-` and `rpc-` endpoints |
| `render_targets` in `run.sh` | the ansible template that writes `targets.yaml` from the inventory |
| the `rolloor-example-controller` container | the devnet's rolloor instance |
| `persistent: true` in `network.yaml` | data directories on the host, which survive a container being recreated |

## Files

| file | what it is |
|---|---|
| `config.yaml` | rolloor's config, at `/etc/rolloor/config.yaml` in the container |
| `hooks/` | the programs rolloor runs, at `/etc/rolloor/hooks` |
| `network.yaml` | ethereum-package's arguments |
| `run.sh` | everything above, with the checks CI relies on |

Each hook reads one target (or, for `soak-beacon`, the soak document) as JSON on stdin and finds what it needs under the target's `extra`: `container` and `updater` for watchtower, `beacon` or `rpc` for the node's APIs.

| hook | used for | passes when |
|---|---|---|
| `inspect` | every target | prints the digest watchtower says the container runs |
| `update` | every target | watchtower has taken an update to the desired digest, or the container already runs it; it refuses any other digest, since watchtower can only deploy the tag's head |
| `ready-beacon` | beacon nodes | synced, execution client online |
| `ready-execution` | execution clients | finished syncing |
| `ready-running` | validator clients | the container is running the desired digest |
| `soak-beacon` | beacon nodes | the updated nodes are no further behind than the rest (or 2 slots) and keep at least half their median peer count |

This directory is the one place in the repository allowed workload words; `scripts/lint-words.sh` skips it.
