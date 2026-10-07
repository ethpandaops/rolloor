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

Programs in `hooks.dir` read JSON. Exit 0 means yes, exit 1 means no, and exit 3 means **could not check**; stdout's first line gives the reason. Exec failures, timeouts and killed programs are indeterminate like exit 3. Targets may override defaults.

| hook | stdin | exit 0 | exit 1 | exit 3 or runner error |
|---|---|---|---|---|
| `inspect` | target | prints the running digest or `none` | failed observation | failed observation, reason says could not check |
| `update` | target with `desired`, `desiredRef` | update accepted | failed attempt, retried | failed attempt, retried |
| `ready` | target with `digestSince` (when it was first seen on its current digest) | target is available now | negative readiness observation | failed readiness observation, reason says could not check |
| `soak` | `{updated, remaining, rollout}` | updated targets are no worse than the rest | negative comparison, counts toward `failureLimit` | neutral error; wait and retry, never halt on its own |

`ready` ignores desired digests and runs outside rollouts too. Hooks must be idempotent: interrupted updates may repeat, observed landed updates do not.

Each `updated` soak entry includes `updatedAt` in RFC 3339, when inspect first observed its new digest. Programs can exclude warm-up using that time; `remaining` entries are unchanged. The diagnostic `rolloor hook soak` command has no rollout observation and uses its invocation time instead.

## Guarantees

- [`TestGuaranteeDisruptionBudget`](internal/reconcile/guarantees_test.go): admission respects `maxUnavailable`, counting unrelated outages once per node; unavailable nodes cost nothing. With zero unavailable weight and a positive budget, one weighted node may go alone.
- [`TestGuaranteeOneRolloutPerGroup`](internal/reconcile/guarantees_test.go): one active rollout per group; any of its images changing supersedes it.
- [`TestGuaranteeDurableUpdates`](internal/reconcile/guarantees_test.go): admission and each dispatched attempt are stored with full SQLite durability before updates; restart/crash recovery preserves the budget without repeating landed updates. Dispatched updates keep their reservation after a skip or end until observed ready on the new build at the admitted node and image, or `progressDeadline` after leaving the batch. Held nodes count at no less than their admitted weight, including after removal from the target files.
- [`TestGuaranteeHistorySurvivesStoreFailures`](internal/reconcile/history_recovery_test.go): owed events are stored in order before new decisions, then delivered to history listeners.
- [`TestGuaranteeHaltStopsOpenBatch`](internal/reconcile/guarantees_test.go): a halt stops the open batch and the rest until a newer build or a retry.
- [`TestGuaranteeSuspendedTargetsNeverUpdate`](internal/reconcile/guarantees_test.go): no update is dispatched to a suspended target; an already-dispatched update may still land and keeps its budget reservation.
- [`TestGuaranteeObservedHealth`](internal/reconcile/guarantees_test.go): health follows observed readiness, including recovery outside rollouts.
- [`TestGuaranteeTransientFailuresRecover`](internal/reconcile/guarantees_test.go): failed update attempts retry with backoff; indeterminate observations are not evidence of a bad build.
- [`TestGuaranteeUpdaterExhaustionSkips`](internal/reconcile/guarantees_test.go): exhausted updater retries or a digest that never arrives skip that target, retaining its dispatched reservation for the progress deadline; the rest proceeds and a later automatic rollout retries drift.
- [`TestGuaranteeSoakErrorsRecover`](internal/reconcile/guarantees_test.go): indeterminate soak checks never halt or pass a batch; fresh successful checks resume it automatically, however long the outage lasted.
- [`TestGuaranteeConfigPause`](internal/reconcile/guarantees_test.go): top-level and group config pauses block every admission and update, including forced sync, without stopping observation or releasing open reservations; clearing config resumes automatically.
- [`TestGuaranteeAutonomousConvergence`](internal/reconcile/guarantees_test.go): once failures heal, every target converges unattended, except as below.

## When a person is needed

Only a bad build or an explicitly chosen hold can need intervention. Operator pauses and suspensions expire after 24 hours by default, even across restarts.

| state | cause | resolved by | guarantee |
|---|---|---|---|
| `Halted` | conclusive bad readiness on the new build after the progress deadline, or negative soak comparisons; no newer build | newer build automatically, or Retry | `TestGuaranteeHaltStopsOpenBatch` |
| `Aborted` | a person stopped the operation; the group holds that build | newer build automatically, or Sync | `TestGuaranteeAbortKeepsBuild` |
| `Paused` | a person explicitly paused the operation | expiry automatically, or Promote | `TestGuaranteeOperatorPauseExpires` |
| suspended target | a person explicitly suspended it | expiry automatically, or Resume | `TestGuaranteeSuspensionExpires` |

Retry keeps the failed batch in history and admits its failed targets before untried targets, as many at a time as the budget permits.

`pause` and `suspend` accept an optional `expiresIn` duration, such as `"2h"`; absent or nonpositive values use 24 hours. Pause expiry starts when requested, not after its current batch finishes. Promote only clears an operator pause; it never overrides config.

An expired operator pause does not release a bad-build halt. Ended operations cancel pending pauses, so expiry cannot reopen a completed, aborted or superseded rollout.

There is no manual policy, first-batch pause, digest pin or runtime policy store. `GET` and `PUT /api/v1/policies/{group}` have been removed; nothing is deployed, and panda's CLI and proxy never call them. The `pause`, `promote`, `abort`, `retry`, `suspend`, `resume`, `sync` and `refresh` actions remain compatible.

## Config

```yaml
# UI name, passed to hooks.
environment: production
# Observe-only first stage or fleet kill switch.
paused: false
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
# Optional group overrides; strategy is a name from strategies.
groups:
  frontend:
    paused: false
    strategy: ""
```

More settings: [`examples/generic/config.yaml`](examples/generic/config.yaml).

`paused` and `groups` are reloaded from the config file every five seconds; invalid changes keep the previous settings. All other settings require a restart. An active rollout keeps its selected strategy unless Sync explicitly changes it; a group strategy change applies to later rollouts. A config pause prevents new batches and queued updates, while open reservations remain charged, observations continue and an already-soaking batch can finish.

The progress deadline starts at the first durable update dispatch, not while waiting for a worker. A queued target whose build lands another way starts that clock from the first observation of the new digest.

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
| `rolloor_soak_check_blocked{group}` | 1 while an active soak cannot check; 0 after checks recover, absent without an active rollout |
| `rolloor_last_tick_timestamp_seconds` | last completed reconcile pass, or 0 before the first |
| `rolloor_target_info`, `rolloor_target_sync`, `rolloor_target_health` | target digests and observed status |
| `rolloor_rollout_state` | active rollout states |
| `rolloor_hook_runs_total`, `rolloor_hook_failures_total`, `rolloor_hook_duration_seconds` | hook outcomes and runtime |

Admitted node weights remain charged while held, even if their targets move or disappear. Ratios can exceed 1 after the configured fleet shrinks; they are not clamped.

`GET /api/v1/history` returns the newest events first; with `after=<id>` it returns the events after that id oldest first, and `after=0` reads a history from its first event, which is how a follower starts. History writes that fail are retried in order before new decisions; live history listeners receive events only once they are stored. Background inspection, background readiness probes and early batch observations share ten-second spacing per target and hook. An update that returns success earns one immediate inspect and readiness observation.

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
