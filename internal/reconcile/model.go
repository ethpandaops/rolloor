// Package reconcile converges targets toward their desired digests: it sorts
// them, cuts batches under the weight budget, runs the hooks, and halts when a
// batch does worse than the targets not yet reached.
package reconcile

import (
	"time"

	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// SyncState says whether a target runs what it should.
type SyncState string

// Sync states.
const (
	Synced    SyncState = "Synced"
	OutOfSync SyncState = "OutOfSync"
	Unknown   SyncState = "Unknown"
)

// Health says how a target is doing, independent of sync.
type Health string

// Health states.
const (
	Healthy     Health = "Healthy"
	Progressing Health = "Progressing"
	Degraded    Health = "Degraded"
	Suspended   Health = "Suspended"
)

// RolloutState is where a rollout is.
type RolloutState string

// Rollout states. Terminal ones are Complete, Halted-then-Superseded, Aborted
// and Superseded; Halted itself waits for a retry or a new digest.
const (
	WaitingForSync        RolloutState = "WaitingForSync"
	Running               RolloutState = "Running"
	Soaking               RolloutState = "Soaking"
	Paused                RolloutState = "Paused"
	WaitingForBudget      RolloutState = "WaitingForBudget"
	WaitingForEnvironment RolloutState = "WaitingForEnvironment"
	Halted                RolloutState = "Halted"
	Aborted               RolloutState = "Aborted"
	Superseded            RolloutState = "Superseded"
	Complete              RolloutState = "Complete"
)

// Active reports whether the controller still has work to do on this state.
func (s RolloutState) Active() bool {
	switch s {
	case WaitingForSync, Running, Soaking, Paused, WaitingForBudget, WaitingForEnvironment, Halted:
		return true
	case Aborted, Superseded, Complete:
		return false
	}

	return false
}

// Policy modes.
const (
	ModeAutomated = "automated"
	ModeManual    = "manual"
)

// Policy is what a group has chosen.
type Policy struct {
	Mode string `json:"mode"`
	// Strategy names a configured strategy; empty is the default.
	Strategy string `json:"strategy,omitempty"`
	// Pins hold an image at a digest regardless of the tag.
	Pins map[string]string `json:"pins,omitempty"`
}

// Suspension keeps matching targets out of every rollout until it expires.
type Suspension struct {
	ID        string           `json:"id"`
	Selector  targets.Selector `json:"selector"`
	Reason    string           `json:"reason"`
	Actor     string           `json:"actor"`
	CreatedAt time.Time        `json:"createdAt"`
	ExpiresAt time.Time        `json:"expiresAt"`
}

// Live is what inspect last reported for a target.
type Live struct {
	Digest   string    `json:"digest"`
	Failures int       `json:"failures"`
	SeenAt   time.Time `json:"seenAt"`
	Reason   string    `json:"reason,omitempty"`
	// ObservedAt is when the observation that produced this began, so a slow
	// inspect that returns after a newer one cannot overwrite it.
	ObservedAt time.Time `json:"observedAt,omitzero"`
}

// Desired is what a tag currently points at.
type Desired struct {
	Digest     string    `json:"digest"`
	Revision   string    `json:"revision,omitempty"`
	ResolvedAt time.Time `json:"resolvedAt"`
}

// TargetPhase is where a target is inside its rollout batch.
type TargetPhase string

// Target phases within a batch.
const (
	PhasePending  TargetPhase = "pending"
	PhaseUpdating TargetPhase = "updating"
	PhaseReady    TargetPhase = "ready"
	PhasePassed   TargetPhase = "passed"
	PhaseFailed   TargetPhase = "failed"
	PhaseSkipped  TargetPhase = "skipped"
)

// RolloutTarget is one target's progress in a rollout.
type RolloutTarget struct {
	ID     string      `json:"id"`
	Node   string      `json:"node"`
	Wave   int         `json:"wave"`
	Batch  int         `json:"batch"` // 0 = not yet batched
	Phase  TargetPhase `json:"phase"`
	Reason string      `json:"reason,omitempty"`
	// UpdateDone is set once the update hook has returned success; Updated
	// once inspect has seen the desired digest running.
	UpdateDone bool      `json:"updateDone"`
	Updated    bool      `json:"updated"`
	UpdatedAt  time.Time `json:"updatedAt,omitzero"`
	// DegradedBefore marks a target that was already Degraded when the
	// rollout began, so its node costs nothing against the budget.
	DegradedBefore bool `json:"degradedBefore,omitempty"`
	// HoldUntil keeps the node counted against the budget after the rollout
	// ended while this target's update may still be landing.
	HoldUntil time.Time `json:"holdUntil,omitzero"`
}

// Batch is one step of a rollout.
type Batch struct {
	Number    int       `json:"number"`
	Wave      int       `json:"wave"`
	Targets   []string  `json:"targets"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt,omitzero"`
	Passed    bool      `json:"passed"`
	// Soak is the batch's soak once it has ended; the open batch's lives on
	// the rollout. PriorSoaks are the attempts a retry replaced.
	Soak       *SoakProgress   `json:"soak,omitempty"`
	PriorSoaks []*SoakProgress `json:"priorSoaks,omitempty"`
}

// SoakCheck is one run of one soak program.
type SoakCheck struct {
	At        time.Time `json:"at"`
	Program   string    `json:"program"`
	OK        bool      `json:"ok"`
	Reason    string    `json:"reason"`
	Updated   string    `json:"updated,omitempty"`
	Remaining string    `json:"remaining,omitempty"`
	Unit      string    `json:"unit,omitempty"`
}

// SoakProgress tracks the current batch's soak.
type SoakProgress struct {
	StartedAt time.Time `json:"startedAt,omitzero"`
	// LastCheckAt is when the last check completed; LastCheckStartedAt is
	// when the last one was dispatched, which paces checks but is no evidence.
	LastCheckAt        time.Time   `json:"lastCheckAt,omitzero"`
	LastCheckStartedAt time.Time   `json:"lastCheckStartedAt,omitzero"`
	Streak             int         `json:"streak"`
	Failures           int         `json:"failures"`
	Checks             []SoakCheck `json:"checks,omitempty"`
}

// Rollout is one group's convergence toward a set of digests.
type Rollout struct {
	ID       string `json:"id"`
	Group    string `json:"group"`
	Strategy string `json:"strategy,omitempty"`
	// Desired is image → digest at creation; the rollout moves targets here.
	Desired   map[string]string `json:"desired"`
	Revisions map[string]string `json:"revisions,omitempty"`
	// From is image → the digest most targets ran before, for display.
	From map[string]string `json:"from,omitempty"`

	State  RolloutState `json:"state"`
	Reason string       `json:"reason"`

	// Human is true when a person started or resumed it; the environment
	// check never pauses a human's rollout.
	Human bool `json:"human"`
	Force bool `json:"force"`

	PausePending bool `json:"pausePending,omitempty"`
	RetryPending bool `json:"retryPending,omitempty"`

	Targets []RolloutTarget `json:"targets"`
	Batches []Batch         `json:"batches"`
	Soak    SoakProgress    `json:"soak"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	EndedAt   time.Time `json:"endedAt,omitzero"`
}

// CurrentBatch is the batch in progress, or nil.
func (r *Rollout) CurrentBatch() *Batch {
	if len(r.Batches) == 0 {
		return nil
	}

	b := &r.Batches[len(r.Batches)-1]
	if b.EndedAt.IsZero() {
		return b
	}

	return nil
}

// target finds a target's progress by id.
func (r *Rollout) target(id string) *RolloutTarget {
	for i := range r.Targets {
		if r.Targets[i].ID == id {
			return &r.Targets[i]
		}
	}

	return nil
}

// DigestShort is the display form of the digest the rollout moves to.
func (r *Rollout) DigestShort() string {
	for _, d := range sortedValues(r.Desired) {
		return shortDigest(d)
	}

	return ""
}

// Event is one line of history.
type Event struct {
	ID       int64     `json:"id"`
	At       time.Time `json:"at"`
	Actor    string    `json:"actor"`
	Action   string    `json:"action"`
	Group    string    `json:"group,omitempty"`
	Rollout  string    `json:"rollout,omitempty"`
	Target   string    `json:"target,omitempty"`
	Selector string    `json:"selector,omitempty"`
	Reason   string    `json:"reason,omitempty"`
}

// ControllerActor is the actor recorded for the controller's own decisions.
const ControllerActor = "controller"

// HookRun is the last result of one hook for one target, kept for display.
type HookRun struct {
	Hook   string       `json:"hook"`
	Result hooks.Result `json:"result"`
}
