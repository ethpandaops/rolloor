package reconcile

import (
	"context"
	"maps"
	"sort"
	"sync"
)

// Snapshot is everything the controller needs back after a restart.
type Snapshot struct {
	Rollouts    []*Rollout
	Policies    map[string]Policy
	Suspensions []Suspension
	Live        map[string]Live
	Degraded    map[string]string
	Desired     map[string]Desired
	Aborted     map[string]string
	HookRuns    map[string]map[string]HookRun
	NextEventID int64
}

// Store persists the controller's decisions. The controller is the only
// writer; reads for the UI go through the controller, not the store.
type Store interface {
	Load(ctx context.Context) (*Snapshot, error)
	SaveRollout(ctx context.Context, r *Rollout) error
	SavePolicy(ctx context.Context, group string, p Policy) error
	SaveSuspension(ctx context.Context, s *Suspension) error
	DeleteSuspension(ctx context.Context, id string) error
	SaveLive(ctx context.Context, id string, l *Live) error
	DeleteLive(ctx context.Context, id string) error
	SaveDegraded(ctx context.Context, id, reason string) error
	ClearDegraded(ctx context.Context, id string) error
	SaveDesired(ctx context.Context, image string, d Desired) error
	SaveAborted(ctx context.Context, group, key string) error
	ClearAborted(ctx context.Context, group string) error
	SaveHookRun(ctx context.Context, id string, run *HookRun) error
	DeleteHookRuns(ctx context.Context, id string) error
	AppendEvent(ctx context.Context, e *Event) error
	Events(ctx context.Context, q EventQuery) ([]Event, error)
}

// EventQuery filters history.
type EventQuery struct {
	Group   string
	Rollout string
	Target  string
	// After returns only events with a greater id, oldest first, for replay.
	After int64
	Limit int
}

// Notifier receives every event as it happens, for live streams.
type Notifier interface {
	Publish(e *Event)
}

// MemoryStore keeps everything in memory. It backs tests and is the default
// when no data directory is configured.
type MemoryStore struct {
	mu          sync.Mutex
	rollouts    map[string]*Rollout
	policies    map[string]Policy
	suspensions map[string]Suspension
	live        map[string]Live
	degraded    map[string]string
	desired     map[string]Desired
	aborted     map[string]string
	events      []Event
	// Fail, when set, is returned by every write; FailRollouts by rollout
	// saves only. Tests use them.
	Fail         error
	FailRollouts error
	hookRuns     map[string]map[string]HookRun
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		rollouts:    map[string]*Rollout{},
		policies:    map[string]Policy{},
		suspensions: map[string]Suspension{},
		live:        map[string]Live{},
		degraded:    map[string]string{},
		desired:     map[string]Desired{},
		aborted:     map[string]string{},
		hookRuns:    map[string]map[string]HookRun{},
	}
}

var _ Store = (*MemoryStore)(nil)

// Load returns a copy of everything.
func (m *MemoryStore) Load(context.Context) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return nil, m.Fail
	}

	s := &Snapshot{
		Policies:    map[string]Policy{},
		Live:        map[string]Live{},
		Degraded:    map[string]string{},
		Desired:     map[string]Desired{},
		Aborted:     map[string]string{},
		HookRuns:    map[string]map[string]HookRun{},
		NextEventID: int64(len(m.events)) + 1,
	}

	for id, runs := range m.hookRuns {
		s.HookRuns[id] = maps.Clone(runs)
	}

	for _, r := range m.rollouts {
		s.Rollouts = append(s.Rollouts, cloneRollout(r))
	}

	sort.Slice(s.Rollouts, func(i, j int) bool { return s.Rollouts[i].ID < s.Rollouts[j].ID })

	for k, v := range m.policies {
		s.Policies[k] = v
	}

	for _, v := range m.suspensions {
		s.Suspensions = append(s.Suspensions, v)
	}

	sort.Slice(s.Suspensions, func(i, j int) bool { return s.Suspensions[i].ID < s.Suspensions[j].ID })

	for k, v := range m.live {
		s.Live[k] = v
	}

	for k, v := range m.degraded {
		s.Degraded[k] = v
	}

	for k, v := range m.desired {
		s.Desired[k] = v
	}

	for k, v := range m.aborted {
		s.Aborted[k] = v
	}

	return s, nil
}

// cloneRollout copies a rollout deeply enough that later mutation of the
// original does not show through the snapshot.
func cloneRollout(r *Rollout) *Rollout {
	cp := *r
	cp.Targets = append([]RolloutTarget(nil), r.Targets...)
	cp.Batches = append([]Batch(nil), r.Batches...)
	cp.Soak.Checks = append([]SoakCheck(nil), r.Soak.Checks...)
	cp.Desired = maps.Clone(r.Desired)
	cp.Revisions = maps.Clone(r.Revisions)
	cp.From = maps.Clone(r.From)

	for i := range cp.Batches {
		if r.Batches[i].Soak != nil {
			s := *r.Batches[i].Soak
			s.Checks = append([]SoakCheck(nil), r.Batches[i].Soak.Checks...)
			cp.Batches[i].Soak = &s
		}
	}

	return &cp
}

// SaveRollout stores a copy.
func (m *MemoryStore) SaveRollout(_ context.Context, r *Rollout) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	if m.FailRollouts != nil {
		return m.FailRollouts
	}

	m.rollouts[r.ID] = cloneRollout(r)

	return nil
}

// SavePolicy stores a policy.
func (m *MemoryStore) SavePolicy(_ context.Context, group string, p Policy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.policies[group] = p

	return nil
}

// SaveSuspension stores a suspension.
func (m *MemoryStore) SaveSuspension(_ context.Context, s *Suspension) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.suspensions[s.ID] = *s

	return nil
}

// DeleteSuspension removes one.
func (m *MemoryStore) DeleteSuspension(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	delete(m.suspensions, id)

	return nil
}

// SaveLive stores live state.
func (m *MemoryStore) SaveLive(_ context.Context, id string, l *Live) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.live[id] = *l

	return nil
}

// DeleteLive forgets a target.
func (m *MemoryStore) DeleteLive(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	delete(m.live, id)

	return nil
}

// SaveDegraded records a quarantined target.
func (m *MemoryStore) SaveDegraded(_ context.Context, id, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.degraded[id] = reason

	return nil
}

// ClearDegraded forgets a quarantine.
func (m *MemoryStore) ClearDegraded(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	delete(m.degraded, id)

	return nil
}

// SaveDesired remembers a tag's last known digest.
func (m *MemoryStore) SaveDesired(_ context.Context, image string, d Desired) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.desired[image] = d

	return nil
}

// SaveHookRun stores the last result of one hook for one target.
func (m *MemoryStore) SaveHookRun(_ context.Context, id string, run *HookRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	if m.hookRuns[id] == nil {
		m.hookRuns[id] = map[string]HookRun{}
	}

	m.hookRuns[id][run.Hook] = *run

	return nil
}

// DeleteHookRuns forgets a target's hook results.
func (m *MemoryStore) DeleteHookRuns(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	delete(m.hookRuns, id)

	return nil
}

// SaveAborted records that a group's rollout to these digests was aborted.
func (m *MemoryStore) SaveAborted(_ context.Context, group, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.aborted[group] = key

	return nil
}

// ClearAborted forgets an abort.
func (m *MemoryStore) ClearAborted(_ context.Context, group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	delete(m.aborted, group)

	return nil
}

// AppendEvent adds to history.
func (m *MemoryStore) AppendEvent(_ context.Context, e *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.events = append(m.events, *e)

	return nil
}

// Events returns history newest first.
func (m *MemoryStore) Events(_ context.Context, q EventQuery) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return nil, m.Fail
	}

	var out []Event

	if q.After > 0 {
		for i := range m.events {
			if m.events[i].ID > q.After {
				out = append(out, m.events[i])
			}
		}

		return out, nil
	}

	for i := len(m.events) - 1; i >= 0; i-- {
		e := m.events[i]
		if q.Group != "" && e.Group != q.Group {
			continue
		}

		if q.Rollout != "" && e.Rollout != q.Rollout {
			continue
		}

		if q.Target != "" && e.Target != q.Target {
			continue
		}

		out = append(out, e)

		if q.Limit > 0 && len(out) >= q.Limit {
			break
		}
	}

	return out, nil
}
