# rolloor

rolloor rolls container builds across a fleet in health-gated batches under a disruption budget. Deployments supply targets, hook programs and optional ownership rules; rolloor knows nothing about the workload.

## How a rollout runs

1. Poll image tags and inspect running digests.
2. Create one rollout per group with targets behind.
3. Admit node batches under the disruption budget.
4. Update, then wait for the new digest and observed readiness.
5. Compare each batch against unreached targets during soak; a worse batch halts.
6. Any group image changing supersedes its rollout.

## Hooks

Programs in `hooks.dir` read JSON; exit 0 means yes, stdout's first line gives the reason. Targets may override defaults.

| hook | stdin | exit 0 means |
|---|---|---|
| `inspect` | target | prints the running digest or `none` |
| `update` | target with `desired`, `desiredRef` | an update to exactly that digest started |
| `ready` | target | the container is available now |
| `soak` | `{updated, remaining, rollout}` | updated targets are no worse than the rest |

`ready` ignores desired digests and runs outside rollouts too. Hooks must be idempotent: interrupted updates may repeat, observed landed updates do not.

## Guarantees

- [`TestGuaranteeDisruptionBudget`](internal/reconcile/guarantees_test.go): admission respects `maxUnavailable`, counting unrelated outages once per node; unavailable nodes cost nothing. With zero unavailable weight and a positive budget, one weighted node may go alone.
- [`TestGuaranteeOneRolloutPerGroup`](internal/reconcile/guarantees_test.go): one active rollout per group; any of its images changing supersedes it.
- [`TestGuaranteeDurableUpdates`](internal/reconcile/guarantees_test.go): admission is stored before updates; restart/crash recovery preserves the budget without repeating landed updates.
- [`TestGuaranteeHaltStopsOpenBatch`](internal/reconcile/guarantees_test.go): a halt stops the open batch and the rest until a newer build or a retry.
- [`TestGuaranteeSuspendedTargetsNeverUpdate`](internal/reconcile/guarantees_test.go): a suspended target is never updated.
- [`TestGuaranteeObservedHealth`](internal/reconcile/guarantees_test.go): health follows observed readiness, including recovery outside rollouts.
- [`TestGuaranteeTransientFailuresRecover`](internal/reconcile/guarantees_test.go): failed update attempts and unexecuted soak checks do not halt progress within configured limits.
- [`TestGuaranteeAutonomousConvergence`](internal/reconcile/guarantees_test.go): once failures heal, every target converges unattended, except as below.

## When a person is needed

| state | cause | resolved by |
|---|---|---|
| `Paused` | `pauseAfterFirstBatch` or a person | Promote |
| `WaitingForSync` | manual policy | Sync |
| `Halted` | failed batch, no newer build | newer build or Retry |
| `Aborted` | a person stopped; the group holds that build | newer build or Sync |
| suspended target | a person | expiry or Resume |

Retry keeps the failed batch in history and admits its failed targets before untried targets, as many at a time as the budget permits.

## Config

```yaml
# UI name, passed to hooks.
environment: production
# Labels rolloor reads.
labels:
  # One rollout per value.
  group: app
  # Ownership label.
  owner: owner
# Shared by every rollout.
disruptionBudget:
  # Unavailable weight, absolute or percentage.
  maxUnavailable: 10%
# Programs rolloor runs.
hooks:
  # Default programs; inspect/update/ready use their hook names.
  defaults:
    # Compare updated and remaining targets.
    soak: soak
# How groups roll out.
strategy:
  # Nodes per batch, count or share.
  batchSize: 25%
  # Time allowed from update to readiness.
  progressDeadline: 10m
  # Watching each ready batch.
  soak:
    # Watch duration.
    duration: 10m
    # Failed comparisons tolerated.
    failureLimit: 1
# Policy for groups without an override.
defaultPolicy:
  # automated, or manual to require Sync.
  mode: automated
```

More settings: [`examples/generic/config.yaml`](examples/generic/config.yaml).

## Run

```sh
rolloor validate --config config.yaml
rolloor serve --config config.yaml
```

UI: `/`, API: `/api/v1`, metrics: `/metrics`. Sign-in: `auth.mode: oidc` or `none`. The Docker image includes bash, curl and jq.

## Example

```sh
./examples/generic/run.sh
```

Requires Docker, curl and jq. Six nginx containers demonstrate batches, crash recovery, a bad-build halt and convergence. [`examples/kurtosis`](examples/kurtosis) shows deployment-specific hooks.

## Develop

```sh
make test
make cover
make lint
```

Go 1.25; use `GOTOOLCHAIN=go1.25.13`.
