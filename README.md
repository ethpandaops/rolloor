# rolloor

rolloor rolls container builds across a fleet in health-gated batches under a disruption budget. Deployments supply targets, hook programs and optional ownership rules; rolloor knows nothing about the workload.

## How a rollout runs

1. Poll image tags and inspect running digests.
2. Create one rollout per group with targets behind.
3. Admit node batches under the disruption budget, which counts every node not ready (probed continuously, rollout or not) as unavailable.
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
- [`TestGuaranteeDurableUpdates`](internal/reconcile/guarantees_test.go): admission and each dispatched attempt are stored with full SQLite durability before updates; restart/crash recovery preserves the budget without repeating landed updates. Dispatched updates keep their reservation after a skip or end until observed ready on the new build at the admitted node and image, or `progressDeadline` after leaving the batch. Held nodes count at no less than their admitted weight, including after removal from the target files.
- [`TestGuaranteeHistorySurvivesStoreFailures`](internal/reconcile/history_recovery_test.go): owed events are stored in order before new decisions, then delivered to history listeners.
- [`TestGuaranteeHaltStopsOpenBatch`](internal/reconcile/guarantees_test.go): a halt stops the open batch and the rest until a newer build or a retry.
- [`TestGuaranteeSuspendedTargetsNeverUpdate`](internal/reconcile/guarantees_test.go): no update is dispatched to a suspended target; an already-dispatched update may still land and keeps its budget reservation.
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
# Runs every target's ready program, rollout or not; a node with a target
# not ready counts against the budget.
readinessProbe:
  # How often each target is probed.
  period: 30s
  # Consecutive failures before a target reads not ready.
  failureThreshold: 3
  # Consecutive successes before it reads ready again.
  successThreshold: 1
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

Cookie-authenticated API mutations require same-origin requests and `Content-Type: application/json`; bearer-token mutations are exempt. OIDC login URLs contain only opaque random state; PKCE verifiers remain in signed HttpOnly cookies. Cookie signatures are bound to their names, so login state cannot authenticate a session.

Behind a TLS-terminating proxy, preserve `Host` and set `X-Forwarded-Proto: https`.

### Metrics

| metric | meaning |
|---|---|
| `rolloor_fleet_unavailable_weight_ratio` | unavailable node weight / configured fleet weight, even with no rollout; 0 for an empty fleet |
| `rolloor_disruption_budget_ratio` | unavailable node weight / allowed unavailable weight, even with no rollout; 0 for a zero budget |
| `rolloor_store_writes_owed` | 1 while decisions or ordered history await durable storage |
| `rolloor_targets_load_failed` | 1 while targets files fail to load; the last valid set remains observed |
| `rolloor_last_inspect_timestamp_seconds` | last completed inspect pass, or 0 before the first |
| `rolloor_last_probe_timestamp_seconds` | last completed readiness probe pass, or 0 before the first |
| `rolloor_registry_resolve_failed{image}` | 1 for images whose most recent registry resolution failed |
| `rolloor_last_tick_timestamp_seconds` | last completed reconcile pass, or 0 before the first |
| `rolloor_target_info`, `rolloor_target_sync`, `rolloor_target_health` | target digests and observed status |
| `rolloor_rollout_state` | active rollout states |
| `rolloor_hook_runs_total`, `rolloor_hook_failures_total`, `rolloor_hook_duration_seconds` | hook outcomes and runtime |

Admitted node weights remain charged while held, even if their targets move or disappear. Ratios can exceed 1 after the configured fleet shrinks; they are not clamped.

History writes that fail are retried in order before new decisions; live history listeners receive events only once they are stored. Background inspection, background readiness probes and early batch observations share ten-second spacing per target and hook. An update that returns success earns one immediate inspect and readiness observation.

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
