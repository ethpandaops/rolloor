# rolloor

Staged, health-gated rollouts for a fleet of containers. One instance per environment.

rolloor watches the image tags a set of containers run. When a tag moves, it converges those containers toward the new digest a batch at a time, keeping unavailable weight under a disruption budget (`maxUnavailable`), and after each batch compares the updated containers against the ones it hasn't reached yet. Independent probes assess readiness even when no rollout exists. When a batch does worse, it halts and leaves that batch on the new build for debugging.

It knows nothing about what the containers are. A deployment supplies:

- a **targets file**: every container, its node, weight, image tag and labels;
- **hook programs**: how to see what a container runs, update it, tell it is ready, and compare it with the rest;
- optionally a **teams file**: who may act on which containers.

The spec is `tasks/prd.md`.

## How a rollout runs

1. The registry is polled; a tag now points at a new digest.
2. `inspect` reports what each container runs. Those behind form a rollout per group (the value of a configured label).
3. Targets are sorted (not ready at rollout creation first, then by `wave` label, node, id) and cut into batches of nodes (the `strategy`: `firstBatch`, `batchSize`) that fit the disruption budget; a batch takes all of a node's targets in the group. An already-unavailable node adds no unavailable weight. When unavailable weight and the batch's cost are both zero, one weighted node may go however heavy it is, as `maxUnavailable` rounds up to one pod on a Kubernetes DaemonSet; weightless nodes never consume the budget. `maxUnavailable: 0` admits only nodes already unavailable or weightless.
4. `update` starts each update. The independent inspector observes the new digest, then the rollout waits for a successful readiness probe that began after that digest was first seen. The rollout does not run `ready`.
5. `soak` compares the batch against the containers not yet reached, every `interval` for `duration`. Passing moves on; more than `failureLimit` failed checks halts and quarantines the batch.
6. A newer digest supersedes a halted or running rollout. Currently not-ready targets go first in the next one. Quarantine is rollout metadata, not observed health or digest drift: it ends on abort or supersession, or when a retried target passes its batch. A quarantined target can be Healthy and Synced, and does not need an update solely to clear its quarantine.

People can sync, pause, promote, abort, retry, suspend targets with an expiry, and set a group's policy (automated or manual, a named strategy, pinned digests).

## Hooks

Programs in `hooks.dir`, run with a JSON document on stdin. Exit 0 means yes; the first line of stdout is the reason people see.

| hook | stdin | exit 0 means |
|---|---|---|
| `inspect` | target | stdout is the running digest (`sha256:…`) or `none` |
| `update` | target, with `desired` and `desiredRef` | the update to exactly that digest has started |
| `ready` | target | the container is currently available, regardless of its desired digest |
| `soak` | `{updated, remaining, rollout}` | the updated targets are no worse than the remaining ones |

Each target may name its own programs under `hooks`; the rest use `hooks.defaults`. `rolloor hook <name> --target <id>` runs one by hand with the same document. Programs must be idempotent: an update interrupted by a restart runs again.

Readiness programs must not compare the running digest with `desired`. Desired-build convergence belongs to `inspect`; `ready` answers the same availability question inside and outside a rollout.

## Observed state and disruption budget

Sync is digest comparison only: `Synced`, `OutOfSync`, or `Unknown`. Health is independent: suspension takes precedence, then an active batch awaiting its new build is `Progressing`, a known not-ready target is `Degraded`, failed inspection or unobserved readiness is `Unknown`, otherwise it is `Healthy`. Quarantine is exposed separately with its rollout and reason.

`readinessProbe` defaults to:

```yaml
readinessProbe:
  period: 30s
  failureThreshold: 3
  successThreshold: 1
```

Consecutive failures and successes must reach their threshold before changing readiness. Probes cover every target with the inspector's concurrency limit. Readiness, transition times, reasons and counters survive restarts; removing a target forgets its observations.

Unavailable weight is the union of nodes with a not-ready or unprobed target, nodes admitted into an update that has not yet been observed ready on its new build, and nodes still held after an ended operation while an update may land. A node is counted once at its configured weight, including outages unrelated to rollouts. Admission charges only nodes newly added to that union.

## Run

```sh
rolloor validate --config config.yaml   # config, targets and teams files
rolloor serve --config config.yaml      # the loop, web UI, API and metrics
```

The Docker image runs `serve` with `/etc/rolloor/config.yaml` and keeps its SQLite database in `/var/lib/rolloor`. It includes bash, curl and jq for shell hooks.

- **Web UI** at `/`: the fleet, groups, rollouts, nodes and history, with every control a viewer may not use disabled and the reason shown.
- **API** under `/api/v1`: the same reads, `/actions/*` for every verb, `/events` as a server-sent event stream.
- **Sign-in**: `auth.mode: oidc` against any OpenID Connect issuer (PKCE; a public client needs no secret), or `none`. `publicReads` opens the pages and reads to everyone; `trustedTokens` accepts bearer tokens minted for other clients, such as a CLI's. A person may act on targets whose owner label is listed for them in the teams file; a selection spanning several owners needs confirming.
- **Metrics** at `/metrics` for Prometheus.

## Example

```sh
./examples/generic/run.sh
```

Needs docker, curl and jq. Six nginx containers on a local registry: rolloor moves them between builds in waves and batches, is killed mid-batch and restarted, halts on a broken build, and converges past it. CI runs it on every change.

## Develop

```sh
make test     # go test -race
make cover    # coverage floors per package (100% where decisions are made)
make lint     # golangci-lint and the word lint
```

`internal/reconcile/property_test.go` runs the controller through random fleets, failures, verbs, restarts, crashes and store outages, checking the budget and the other rules after every step and that everything converges once the failures stop. A failing seed reproduces with `ROLLOOR_SIM_SEED=<n>`.

Go 1.25. Nothing in this repository is specific to a workload; deployments bring their own hooks and targets. `scripts/lint-words.sh` fails the build if the first workload's words appear in any tracked file.
