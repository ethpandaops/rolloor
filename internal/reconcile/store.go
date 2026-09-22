package reconcile

import (
	"context"
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
	SaveLive(ctx context.Context, id string, l Live) error
	DeleteLive(ctx context.Context, id string) error
	SaveDegraded(ctx context.Context, id, reason string) error
	ClearDegraded(ctx context.Context, id string) error
	AppendEvent(ctx context.Context, e *Event) error
	Events(ctx context.Context, q EventQuery) ([]Event, error)
}

// EventQuery filters history.
type EventQuery struct {
	Group   string
	Rollout string
	Target  string
	Limit   int
}

// Notifier receives every event as it happens, for live streams.
type Notifier interface {
	Publish(e Event)
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
	events      []Event
	// Fail, when set, is returned by every write; tests use it.
	Fail error
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		rollouts:    map[string]*Rollout{},
		policies:    map[string]Policy{},
		suspensions: map[string]Suspension{},
		live:        map[string]Live{},
		degraded:    map[string]string{},
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
		NextEventID: int64(len(m.events)) + 1,
	}

	for _, r := range m.rollouts {
		cp := *r
		s.Rollouts = append(s.Rollouts, &cp)
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

	return s, nil
}

// SaveRollout stores a copy.
func (m *MemoryStore) SaveRollout(_ context.Context, r *Rollout) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	cp := *r
	m.rollouts[r.ID] = &cp

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
func (m *MemoryStore) SaveLive(_ context.Context, id string, l Live) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Fail != nil {
		return m.Fail
	}

	m.live[id] = l

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
