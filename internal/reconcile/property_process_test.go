package reconcile

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
)

// errDiskFull is what the store returns during an outage.
var errDiskFull = errors.New("disk full")

// process is one life of the controller process. Its store writes reach the
// harness's MemoryStore, which plays the disk, and its programs reach the
// world. Once dead, nothing it does lands anywhere, as if it had stopped at
// that moment; the next process starts from what the disk holds.
type process struct {
	*MemoryStore

	sim *sim

	mu sync.Mutex
	// writes is how many more store writes land before the process dies;
	// negative is no limit.
	writes int
	dead   bool
}

var _ Store = (*process)(nil)

// lands reports whether a store write reaches the disk.
func (p *process) lands() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.writes == 0 {
		p.dead = true
	}

	if p.dead {
		return false
	}

	if p.writes > 0 {
		p.writes--
	}

	return true
}

func (p *process) Run(ctx context.Context, program, hook, targetID string, input any) (hooks.Result, error) {
	p.mu.Lock()
	dead := p.dead
	p.mu.Unlock()

	if dead {
		return hooks.Result{}, errors.New("process stopped")
	}

	if hook == config.HookUpdate {
		in, _ := input.(HookInput)
		p.sim.dispatched(p.MemoryStore, targetID, in.Desired)
	}

	return p.sim.h.world.Run(ctx, program, hook, targetID, input)
}

func (p *process) SaveRollout(ctx context.Context, r *Rollout) error {
	if !p.lands() {
		return nil
	}

	p.MemoryStore.mu.Lock()
	before := p.rollouts[r.ID]
	p.MemoryStore.mu.Unlock()

	problem := p.sim.admissionProblem(r, before)
	if err := p.MemoryStore.SaveRollout(ctx, r); err != nil {
		return err
	}

	if problem != nil {
		p.sim.violation("%v", problem)
	}

	return nil
}

func (p *process) SavePolicy(ctx context.Context, group string, pol Policy) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SavePolicy(ctx, group, pol)
}

func (p *process) SaveSuspension(ctx context.Context, s *Suspension) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveSuspension(ctx, s)
}

func (p *process) DeleteSuspension(ctx context.Context, id string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.DeleteSuspension(ctx, id)
}

func (p *process) SaveLive(ctx context.Context, id string, l *Live) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveLive(ctx, id, l)
}

func (p *process) DeleteLive(ctx context.Context, id string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.DeleteLive(ctx, id)
}

func (p *process) SaveDegraded(ctx context.Context, id, reason string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveDegraded(ctx, id, reason)
}

func (p *process) ClearDegraded(ctx context.Context, id string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.ClearDegraded(ctx, id)
}

func (p *process) SaveDesired(ctx context.Context, image string, d Desired) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveDesired(ctx, image, d)
}

func (p *process) SaveAborted(ctx context.Context, group, key string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveAborted(ctx, group, key)
}

func (p *process) ClearAborted(ctx context.Context, group string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.ClearAborted(ctx, group)
}

func (p *process) SaveHookRun(ctx context.Context, id string, run *HookRun) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.SaveHookRun(ctx, id, run)
}

func (p *process) DeleteHookRuns(ctx context.Context, id string) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.DeleteHookRuns(ctx, id)
}

func (p *process) ReplaceDecisions(ctx context.Context, snap *Snapshot) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.ReplaceDecisions(ctx, snap)
}

func (p *process) AppendEvent(ctx context.Context, e *Event) error {
	if !p.lands() {
		return nil
	}

	return p.MemoryStore.AppendEvent(ctx, e)
}

// onDisk reports whether the store holds an active rollout whose open batch
// includes the target, moving it to this digest.
func onDisk(disk *MemoryStore, id, digest string) bool {
	disk.mu.Lock()
	defer disk.mu.Unlock()

	for _, r := range disk.rollouts {
		b := r.CurrentBatch()
		if r.State.Active() && b != nil && slices.Contains(b.Targets, id) && slices.Contains(sortedValues(r.Desired), digest) {
			return true
		}
	}

	return false
}

// durableState is the state the store holds for a rollout.
func durableState(disk *MemoryStore, id string) RolloutState {
	disk.mu.Lock()
	defer disk.mu.Unlock()

	if r, ok := disk.rollouts[id]; ok {
		return r.State
	}

	return ""
}
