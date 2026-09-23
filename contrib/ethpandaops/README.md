# rolloor for ethpandaops devnets

The hook programs, targets template and teams refresh for running one rolloor per devnet. Everything devnet-specific lives here; the binary under `cmd/` and `internal/` knows none of it.

## Programs

`rolloor-ethpandaops` is one binary. The image links it under each program name in `/usr/local/share/rolloor/ethpandaops`, which is `hooks.dir` in `config.yaml.example`.

| program | hook | passes when |
|---|---|---|
| `inspect` | inspect | prints the registry digest the container runs (`none` if the node has no such container) |
| `update` | update | the node's updater has taken an update to the desired digest, or the container already runs it |
| `ready-beacon` | ready | the beacon node is synced and its execution client is online |
| `ready-execution` | ready | the execution client has finished syncing and has peers |
| `ready-running` | ready | the container is running the desired digest (validator clients, sidecars) |
| `soak-beacon` | soak | updated beacon nodes are no more slots behind than the remaining ones (or 2) and keep at least half their median peer count |
| `soak-execution` | soak | the same for execution clients, in blocks behind the highest head |
| `soak-validator` | soak | updated validators hit the attestation target, two epochs back, at a rate within the tolerance of the remaining ones' (or above the floor when none remain) |
| `environment` | environment | finality trails the head by no more than the limit, and the head is not within the margin of a scheduled fork or a quiet epoch |

`rolloor-ethpandaops teams --team <owner>=<coredevs team> --member <owner>=<handle> --out /etc/rolloor/teams.yaml` writes the teams file from the coredevs registry. A team that cannot be read leaves the file as it was. Run it from cron.

## Settings

Environment variables on the rolloor container; hooks inherit them.

| variable | default | |
|---|---|---|
| `ETHPANDAOPS_UPDATER_TOKEN` | | the updaters' HTTP API token |
| `ETHPANDAOPS_NODE_AUTH` | | `user:password` for the nodes' `bn-` and `rpc-` endpoints |
| `ETHPANDAOPS_BEACON` | | a beacon API for the network (environment check, validator soak) |
| `ETHPANDAOPS_FINALITY_LAG` | `4` | epochs finality may trail the head |
| `ETHPANDAOPS_FORK_MARGIN` | `4` | epochs either side of a fork or quiet epoch kept quiet |
| `ETHPANDAOPS_QUIET_EPOCHS` | | further epochs to keep quiet, comma-separated (gas limit steps) |
| `ETHPANDAOPS_TOLERANCE` | `0.05` | how much lower the updated validators' target rate may be |
| `ETHPANDAOPS_FLOOR` | `0.8` | the target rate needed when nothing remains to compare against |
| `ETHPANDAOPS_SAMPLE` | `256` | validators asked about per target |
| `ETHPANDAOPS_UPDATE_WAIT` | `45s` | how long `update` waits for a busy updater |

## Targets

`targets.yaml.j2` renders the targets file from the devnet inventory: every host with `ethereum_node_el` or `ethereum_node_cl` becomes a node, weighted by its validator count, with `execution`, `beacon`, a separate `validator` when `<cl>_validator_container_name` is set, and `xatu-sentry` when enabled. Waves: hosts without validators 0, with validators 1, bootnodes 2. The programs read these `extra` fields:

| field | read by |
|---|---|
| `container` | inspect, update, ready-running |
| `updater` | inspect, update, ready-running |
| `beacon` | ready-beacon, soak-beacon |
| `rpc` | ready-execution, soak-execution |
| `validators` (`start-end`, end exclusive) | soak-validator |

## Node side

Each host runs the updater (`ghcr.io/nicholas-fedor/watchtower`) with polling off and its API on:

```
--http-api-endpoints=update,containers,check
WATCHTOWER_HTTP_API_TOKEN=<token>
```

reachable at `https://wt-<host>.<srv subdomain>` through the host's nginx proxy (`VIRTUAL_HOST`, `VIRTUAL_PORT=8080`, `LETSENCRYPT_HOST`), with a `wt-<host>` record in the devnet's `srv` zone. That virtual host must not add basic auth: the updater takes its token in the same `Authorization` header.

## Limits

- The updater deploys only a tag's current head. `update` refuses to deploy anything else: if the head has moved past the desired digest it leaves the container alone, and rolloor replaces the rollout on its next registry poll. A pinned older digest therefore never lands and the batch halts at the update deadline; pins need a different updater.
- The validator soak reads rewards two epochs back, so soak durations should cover at least three epochs.
