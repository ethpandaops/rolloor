# rolloor — build spec

One instance per environment. It converges a set of containers toward the current digest of their image tags, a batch at a time, under a disruption budget, comparing each batch against the containers it hasn't reached yet, and stopping when they do worse. It knows nothing about what the containers are. Everything domain-specific arrives as a file it reads or a program it runs.

This document is the spec to build from; when the milestones below are merged it is finished.

## 1. Scope

In the binary:

- targets and teams files, reloaded on change
- registry polling: tag → digest, revision label
- live state via the `inspect` hook
- the reconcile loop: sort, batch under the disruption budget, update, ready, soak, advance or halt
- policies per group, suspensions, pause points
- HTTP API, server-rendered UI, SQLite, history, events, metrics
- OIDC login and owner-based authorisation

Not in this repository, ever: anything specific to a workload. Hooks, targets templates and team lists for a real deployment live with that deployment. If a deployment cannot express what it needs through the files and programs described here, the binary is missing a generic feature; the fix goes there, not into workload code here. `scripts/lint-words.sh` fails the build if words from the first workload appear in any tracked file.

Scope guards: five hooks, no plugin registry, no step language, no high availability, no policy editor in the UI, no cross-environment knowledge.

## 2. Inputs

### 2.1 `config.yaml`

```yaml
# Shown in the page header; the only place the environment's name appears.
environment: prod-eu
listen: ":8080"
# rolloor.db lives here.
dataDir: /var/lib/rolloor

# Every *.yaml file here is a list of targets; merged, reloaded on change.
targetsDir: /etc/rolloor/targets.d
# Optional: owner value -> identities allowed to act on it.
teamsFile: /etc/rolloor/teams.yaml

labels:
  # Tiles, policies and rollouts are per value of this label.
  group: app
  # Authorisation is per value of this label.
  owner: owner
  # Optional: fleet page headings.
  section: role
  # Group values folded away on the fleet page by default.
  hiddenGroups: []

registry:
  # How often every tag is resolved to a digest.
  poll: 60s
  # Optional; docker config.json shape.
  authFile: /etc/rolloor/registry-auth.json

disruptionBudget:
  # Share of total weight allowed to be mid-update at once, across every
  # rollout ("10%"), or an absolute weight.
  maxUnavailable: 10%

hooks:
  # Program names are resolved under this directory.
  dir: /etc/rolloor/hooks
  # Each run is killed after this long.
  timeout: 60s
  # Programs for targets that name none of their own.
  defaults:
    inspect: inspect
    update: update
    ready: ready
    # Empty: no soak unless a target names one.
    soak: ""
  environment:
    # Empty: no environment check.
    program: ""
    interval: 30s

inspect:
  # How often every target's running digest is read.
  interval: 30s
  concurrency: 16
  # Inspections in a row that may fail before a target shows Unknown.
  failureThreshold: 3

strategy:
  # Nodes in batch 1; omitted means batchSize.
  firstBatch: 1
  # Nodes in every later batch, as a share of the rollout's nodes ("10%") or
  # a count. A batch takes every target the group has on each node it picks.
  batchSize: 10%
  # Wait for a promote once batch 1 has passed.
  pauseAfterFirstBatch: true
  # false: ignore the wave label and treat the rollout as one wave.
  waves: true
  soak:
    # How long each batch is watched before the next starts; 0s skips it.
    duration: 12m
    # How often the soak programs run.
    interval: 1m
    # Failed checks tolerated before the rollout halts.
    failureLimit: 1

# Optional named strategies, chosen by a group's policy or by sync. Each
# starts from `strategy` and overrides only what it sets.
strategies:
  fast:
    batchSize: 50%
    pauseAfterFirstBatch: false
    soak:
      duration: 0s

defaultPolicy:
  # automated: rollouts start by themselves; manual: they wait for a sync.
  mode: automated
  # Empty: `strategy`.
  strategy: ""

auth:
  # oidc or none.
  mode: oidc
  issuer: https://auth.example/application/o/rolloor/
  clientId: rolloor
  # Env var holding the client secret; empty makes rolloor a public client.
  # Every login uses PKCE.
  clientSecretEnv: ROLLOOR_OIDC_CLIENT_SECRET
  # One client can serve many instances when the issuer allows a pattern
  # of redirect URLs.
  redirectUrl: https://rolloor.prod-eu.example/auth/callback
  identityClaim: preferred_username
  # Members of this owner value may act on anything.
  adminOwner: operators
  # Anyone may read pages and the API without signing in; acting always
  # needs an identity.
  publicReads: true
  # Bearer tokens from other clients the API also accepts, such as a CLI's.
  trustedTokens:
    - issuer: https://auth.example/application/o/cli/
      audience: cli
      # The claim naming the person in those tokens.
      identityClaim: sub
```

Unknown fields are an error. `maxUnavailable` and batch sizes accept `N%` or an absolute number. The file is the only source of configuration; defaults fill what it leaves out. There are no flag or environment overrides.

### 2.2 Targets file

One or more YAML files in `targets_dir`. A list of targets:

```yaml
# Unique across every file.
- id: web-4/frontend
  # The machine it runs on; its weight is counted once however many targets it has.
  node: web-4
  # For people and hooks; rolloor does not connect to it.
  address: web-4.prod-eu.example
  # The node's share of what the disruption budget protects.
  weight: 1200
  # repository:tag; rolloor follows the tag's digest.
  image: registry.example/shop/frontend:stable
  labels:
    app: frontend
    role: web
    owner: shop
    # Lower waves update first.
    wave: "1"
  # Programs for this target; the rest come from hooks.defaults.
  hooks:
    ready: http-ok
    soak: error-rate
  # Opaque; passed through to every hook.
  extra:
    container: frontend
```

Rules:

- `id` unique across files. `node` required. `weight` belongs to the node; every target on a node must state the same weight, or the file is rejected.
- `image` is `repository:tag` with an optional registry host. Digests are not allowed here; pinning is a policy, not a file edit.
- `labels` are string → string. The configured group and owner labels must be present.
- `wave` is a label. Sort order is numeric ascending; missing means `1`.
- `hooks` names programs under `hooks.dir` for any of `inspect`, `update`, `ready`, `soak`. Unnamed falls back to `hooks.defaults`. A named program that doesn't exist fails validation.
- A reload that fails validation keeps the previous set and raises an event.
- Targets that disappear from the files are removed from the fleet along with their live state; their history stays.

### 2.3 Teams file

```yaml
shop: [alice, bob]
operators: [carol]
```

Owner value → list of identity-claim values. Without the file, a claim value equal to an owner value grants. With `auth.mode: none`, everyone is `admin_owner` and history records the remote address.

## 3. Model

- **Target**: a row from the file plus live state. `desired` (the tag's current digest, or a pinned one), `live` (from `inspect`), `sync` = `Synced | OutOfSync | Unknown`, `health` = `Healthy | Progressing | Degraded | Suspended`. JSON field names are camelCase throughout the API.
- **Node**: the set of targets sharing `node`. Weight counted once.
- **Group**: the set of targets sharing the group label value. One policy, at most one active rollout.
- **Policy** (per group, stored): `mode: automated | manual`, `strategy: <name>` (empty is the default strategy), `pins: {image: digest}` optional.
- **Rollout**: the convergence of one group's OutOfSync targets toward their desired digests. Created by the controller, never by a person. Identified by a random 12-hex-character id; displayed by the short form of the digest it moves to (first changed image if several).
- **Suspension**: selector, reason, actor, expires_at. Matching targets are `Suspended`: skipped by rollouts, excluded from the disruption budget and from the comparison group.
- **Event**: who (identity or `controller`), what (verb or transition), on what (selector, rollout, target), why (reason), when. Append-only. This is history.

## 4. Reconcile loop

The loop ticks every few seconds (a fixed cadence in `main`), and at once on `sync`, `refresh` or any verb that changes a rollout. Registry resolution inside a tick happens only when `registry.poll` has elapsed or a refresh was asked for; a refresh asked for while one is running stays pending. Steps, in order:

1. **Resolve desired.** For each distinct image ref, resolve tag → digest against the registry (HEAD manifest, `Docker-Content-Digest`; the manifest-list digest when the tag is multi-arch). Fetch the config blob once per new digest for `org.opencontainers.image.revision`. Pinned images use the pin. The resolved digests are persisted so a restart with the registry down still knows what is desired. Registry failure keeps the last known digest and raises one `registry.error` event per failure episode and one `registry.recovered` when it ends.
2. **Observe live.** `inspect` runs for every target on `inspect.interval` with `inspect.concurrency`, independent of this tick; the tick reads the latest. A target with `unknown_after` consecutive failures is `Unknown` and is neither updated nor counted anywhere.
3. **Environment check.** If configured, the latest result. Failing pauses *automated* progress: no new batch starts for a rollout the controller began on its own. In-flight soaks finish. Rollouts started by `sync` are unaffected.
4. **Groups.** For each group with OutOfSync, non-Suspended, non-Unknown targets and no active rollout: `automated` → create a rollout and start it; `manual` → create it in `WaitingForSync`.
5. **Advance rollouts.** Each active rollout runs its state machine (section 5) once.
6. **Expire.** Suspensions past `expires_at` are lifted with an event.

### 4.1 Sort and batch

Rollout targets are the group's OutOfSync targets at creation, minus Suspended and Unknown. Order: `Degraded` first, then by `wave` ascending, then by node name, then by `id`. Targets on the same node are adjacent and always land in the same batch.

A wave is the run of targets sharing a `wave` value. Batches are cut inside a wave. If the strategy has `waves: false`, the whole rollout is one wave.

Batch size: `firstBatch` nodes for batch 1 (if set), `batchSize` nodes for every batch after, as a count or a share of the rollout's nodes; a batch takes every rollout target on each node it picks. The batch is then trimmed to the disruption budget: walk the sorted list, adding a target if its node is already in the batch or if adding the node's weight keeps unavailable weight ≤ `maxUnavailable`. A batch is never empty when the budget allows any node; if nothing fits, the rollout waits in `WaitingForBudget` with the numbers.

Unavailable weight = sum over nodes with any target `Progressing`, across all rollouts in the environment, excluding nodes that were `Degraded` before their update began. Weight-zero nodes never use the budget. Total weight = sum of all nodes' weights, including Suspended and Unknown.

### 4.2 Update, ready

For each target in the batch: mark `Progressing`; run `update` with the target plus `desired` and `desiredRef` (see section 6), so the program deploys exactly that digest. Exit 0 = started. An `update` whose result never came back (the process stopped) runs again after restart; programs must be idempotent. Then poll `inspect` until `live_digest == desired_digest` or `hooks.timeout × 5`; then run `ready` (target's or default) until exit 0 or the same deadline. Any deadline or non-zero `update` → target `Degraded`, rollout `Halted` with the hook's reason. A target suspended, removed, moved to another group or node, or given another image while its batch is open is marked skipped and takes no further part, whether that is noticed while planning, while soaking, or when its program's result comes back; such a result neither advances nor halts the batch.

### 4.3 Soak

If the effective soak duration is 0, the batch passes when all its targets are ready. Otherwise, for `duration`, every `interval`: group the batch's targets by soak program (target's `hooks.soak`, else default; targets with none pass automatically) and run each program once with:

```json
{"updated": [targets in this batch using this program],
 "remaining": [rollout targets not yet updated, same program, not Suspended or Unknown],
 "rollout": {"id": "...", "group": "frontend", "batch": 1, "wave": 1}}
```

Exit 0 is a pass. Stdout's first line is shown as the reason; a second line of the form `updated=<num> remaining=<num> unit=<text>` is shown as the two numbers. The batch passes once `duration` has elapsed and the last check passed; failing checks beyond `failureLimit` halt it, and a soak that has run its duration keeps checking until one passes. A pass on stored evidence needs the last completed check to be no older than two intervals, so a process that was away re-checks before passing; a check that was dispatched but never answered is not evidence. A batch whose targets have no soak programs left passes at once. Each batch keeps its own soak record. `remaining` may be empty on the last batch; the program decides what that means.

### 4.4 Advance, pause, halt

After a pass: if `pauseAfterFirstBatch` and this was batch 1, or a `pause` is pending, the rollout is `Paused` with the reason. `promote` or `resume` continues. Otherwise the next batch starts. After the last batch the rollout is `Complete`.

`Halted`: the failed batch's targets stay `Degraded` on the new digest. Nothing else moves. The halt belongs to the desired digests at the time; when any of them changes, the rollout closes as `Superseded` and a new one is created with the Degraded targets sorted first. `retry` (reason required) reopens the halted batch: a failed target already running the digest is inspected again before readiness and the soak, one that is not gets its `update` again, and the failed soak stays on the batch's record. `abort` closes the rollout; the group stays OutOfSync and the controller creates no new rollout for it until `sync` or a new digest. The abort is persisted with the digests it applies to and survives a restart; the marker is written before the rollout, and on restart the marker wins over a rollout the store still shows active. When a rollout ends (abort or supersede) while an `update` has been dispatched and its digest has not shown, that target's node stays counted against the disruption budget for the update deadline (`hooks.timeout × 5`), so the next rollout cannot admit another node on top of it.

Pause never carries across digests: a new digest supersedes a paused rollout too.

`sync --force` skips ready and soak for that rollout. It never skips the disruption budget. It is recorded.

## 5. Rollout states

`WaitingForSync → Running → Soaking → Running … → Complete`, with `Paused`, `WaitingForBudget`, `WaitingForEnvironment`, `Halted`, `Aborted`, `Superseded` reachable as the rules above say. Every non-terminal state carries a one-line `reason` string built by the controller, e.g. `Waiting for the disruption budget: 9.5% of 10% unavailable; the next batch needs 1.2%.`. The same string is served by the API, the UI, and the metrics label-free `reason` event.

## 6. Hooks

Programs under `hooks.dir`, run with the target document (or the soak/environment document) as JSON on stdin, `hooks.timeout`, and these environment variables: `ROLLOOR_HOOK` (name), `ROLLOOR_ENVIRONMENT`, `ROLLOOR_TARGET_ID` (when applicable). Exit 0 is yes. Stdout: first line is the reason shown to people; `inspect` prints the live digest instead. Stderr goes to the log at debug. The last run's exit code, reason and duration per (target, hook) are stored and shown.

| hook | stdin | success |
|---|---|---|
| `inspect` | target document | stdout = digest currently running (`sha256:…`), or `none` |
| `update` | target document | update has started |
| `ready` | target document | the target is doing its job |
| `soak` | `{updated, remaining, rollout}` | updated are no worse than remaining |
| `environment` | `{environment}` | automated progress may continue |

The target document is the target's row from the file plus `desired` (the digest it should run) and `desiredRef` (`<image repository>@<digest>`, ready to hand to a container runtime). Stdout and stderr are captured up to 64 KiB each; the rest is discarded. `rolloor hook <name> --target <id>` runs a program by hand with the same document, resolving `desired` from the registry unless `--desired` is given.

## 7. API

JSON under `/api/v1`. Reads are open to any signed-in identity, or to anyone with `auth.publicReads`. Writes check the owner rule for every target the selector matches; a selector spanning more than one owner value requires `confirm: true` in the body.

```
GET  /fleet                         groups with counts, sync/health roll-up, reason line, disruption budget, environment check
GET  /groups/{label}/{value}         the group: policy, roll-up, current rollout, targets
GET  /nodes/{node}                   the node: weight, labels, targets
GET  /targets?selector=k=v,k=v       targets matching
GET  /rollouts                       all rollouts, newest first
GET  /rollouts/{id}                  the rollout: waves, batches, soak results, reason
GET  /history?selector=&node=&limit=&after= events, filtered to targets the selector or node matches; with after, only events with a greater id, oldest first
GET  /policies/{group}               PUT replaces mode, strategy, pins (a PUT without pins unpins)
GET  /events                         SSE: every event as it happens; a client that falls behind gets `event: gap` and reconnects with `Last-Event-ID` to have the rest replayed from the store
POST /actions/sync                   {selector, force?, strategy?, confirm?}
POST /actions/refresh                re-resolve every tag now
POST /actions/suspend                {selector, reason, expiresIn | expiresAt, confirm?}
POST /actions/resume                 {selector, confirm?}            lifts suspensions
POST /actions/pause                  {rollout}
POST /actions/promote                {rollout}                       also resumes a manual pause
POST /actions/abort                  {rollout}
POST /actions/retry                  {rollout, reason}
GET  /me                             identity, owner values it may act on
```

Errors: `403` with `{error, ownersRequired, ownersHeld}` so the UI can show why a control is disabled. `sync` authorizes over the whole groups the selector touches, since a rollout moves the group. Configuration comes from the file only; there are no environment or flag overrides.

Auth: OIDC authorization-code flow with PKCE at `/auth/login` → session cookie; the API also accepts `Authorization: Bearer <id or access token>` verified against the issuer's JWKS, or against any `trustedTokens` issuer and audience. `auth.mode: none` skips all of it.

## 8. UI

Go `html/template` plus htmx for polling fragments every 5 s. No build step. Routes and states are those in the brief's "Surfaces" and "States and screens that must exist":

`/` fleet, `/groups/{label}/{value}`, `/rollouts`, `/rollouts/{id}`, `/nodes/{node}`, `/history`.

Form posts must come from this site (`Sec-Fetch-Site`, else `Origin`, else `Referer` must match the host); anything else is refused. Pages stop polling while a dialog is open or a field has focus. Rules the templates enforce: every waiting or stopped state shows its `reason`; a control the viewer may not use is rendered disabled with the `403` reason; a target the rollout hasn't reached is styled as ordinary, never as an error; fleet tiles with a rollout in flight show `N of M on <digest>`.

## 9. Storage

SQLite via `modernc.org/sqlite` (no cgo), WAL mode, one file in `dataDir`. Tables: `live`, `degraded`, `desired`, `aborted`, `policies`, `rollouts` (the whole rollout as JSON, batches and soak included), `suspensions`, `events`. Migrations embedded and applied on start. Targets themselves are not stored; the file is the source. The last result per target and hook is stored (`hook_runs`) so it survives a restart.

## 10. Metrics

Prometheus at `/metrics`:

- `rolloor_target_info{id,node,group,owner,image,desired,live}` = 1
- `rolloor_target_sync{id,group}` 0 synced, 1 out of sync, 2 unknown
- `rolloor_target_health{id,group}` 0 healthy, 1 progressing, 2 degraded, 3 suspended
- `rolloor_rollout_state{rollout,group,state}` 1 for the state a rollout is in
- `rolloor_disruption_budget_ratio` unavailable weight / what `maxUnavailable` allows
- `rolloor_hook_duration_seconds{hook}` histogram, `rolloor_hook_runs_total{hook,outcome}`, `rolloor_hook_failures_total{hook}`
- `rolloor_last_tick_timestamp_seconds`, `rolloor_environment_check_passing`, `rolloor_environment_checked_at_seconds`

`id` is bounded by the targets file, so it is acceptable as a label here.

## 11. Repository layout

```
cmd/rolloor/            main.go: cobra `serve`, `validate` (config + targets + teams), `hook` (run one hook against one target, for script authors)
internal/config/
internal/targets/       file loading, validation, watching, selectors
internal/registry/      OCI tag resolution, revision label
internal/hooks/         runner
internal/reconcile/     the loop, sort/batch/disruption budget, rollout state machine   ← the tests that matter
internal/store/         sqlite, migrations, events
internal/auth/          oidc, teams file, owner checks
internal/api/           handlers, SSE
internal/ui/            templates, static, handlers
internal/observability/ logger, metrics
examples/generic/       compose file, targets.yaml, hooks that use docker + curl, ci script
scripts/lint-words.sh
```

Logging: logrus, JSON by default, contextual logger per component as the standards say. Lint: Xatu's `.golangci.yml`. Go 1.24. Module `github.com/ethpandaops/rolloor`.

## 12. Generic example and CI

`examples/generic/` runs in CI on every PR with no workload words:

1. Start a local `registry:2` and six `nginx`-based containers from image `localhost:5000/app:latest` at digest A, three of them "weighted" (weight 10) and three weight 0, on three "nodes" (compose service names).
2. Start rolloor with `maxUnavailable: 40%`, a 50% `batchSize` and a short soak, hooks: `inspect` = `docker inspect` RepoDigest, `update` = `docker pull && docker compose up -d <svc>`, `ready` = `curl -f localhost:<port>/`, `soak` = `updated` respond in under twice the median latency of `remaining`.
3. Push digest B to the tag. Assert: wave 0 (weight 0) updates first; weighted targets update in batches that never exceed 40% of weight; a rollout reaches `Complete`; history has the transitions.
4. Break B (a build that 500s), push as C. Assert: `Halted`, the batch is `Degraded`, the rest untouched, and pushing D supersedes and converges with the Degraded targets first.

## 13. Milestones

Each is one or a few PRs and ends with something running.

- **M1 core.** config, targets, registry, hooks, reconcile, store, API (reads + actions), metrics, `validate`, the generic example green in CI, word lint. No UI, no auth.
- **M2 UI + auth.** templates for every route and state in the brief, OIDC, teams file, disabled-with-reason.
- **M3 first deployment.** Its hooks, targets template and teams refresh live with the deployment, outside this repository; anything it cannot express through configuration becomes a generic feature here. Trial on a throwaway environment.
- **M4 panda.** `panda rollout` command group against the API; panda-pulse message per rollout and halt alert.
