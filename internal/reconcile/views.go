package reconcile

import (
	"sort"
	"strconv"
	"time"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// TargetView is a target with everything the controller knows about it.
type TargetView struct {
	targets.Target

	Group      string             `json:"group"`
	Owner      string             `json:"owner"`
	Wave       int                `json:"wave"`
	Desired    string             `json:"desired"`
	Revision   string             `json:"revision,omitempty"`
	Live       string             `json:"live"`
	Sync       SyncState          `json:"sync"`
	Health     Health             `json:"health"`
	Reason     string             `json:"reason"`
	Rollout    string             `json:"rollout,omitempty"`
	Suspension *Suspension        `json:"suspension,omitempty"`
	Hooks      map[string]HookRun `json:"hookRuns,omitempty"`
}

// GroupView is the roll-up for one group.
type GroupView struct {
	Name      string            `json:"name"`
	Owner     string            `json:"owner"`
	Section   string            `json:"section,omitempty"`
	Hidden    bool              `json:"hidden"`
	Policy    Policy            `json:"policy"`
	Targets   int               `json:"targets"`
	Nodes     int               `json:"nodes"`
	Weight    float64           `json:"weight"`
	Sync      SyncState         `json:"sync"`
	Health    Health            `json:"health"`
	OnDesired int               `json:"onDesired"`
	Reason    string            `json:"reason"`
	Rollout   *RolloutView      `json:"rollout,omitempty"`
	Images    []string          `json:"images"`
	Desired   map[string]string `json:"desired"`
	Revisions map[string]string `json:"revisions,omitempty"`
}

// RolloutView is a rollout plus derived numbers.
type RolloutView struct {
	Rollout

	Digest     string `json:"digest"`
	Revision   string `json:"revision,omitempty"`
	OnNewBuild int    `json:"onNewBuild"`
	NotReached int    `json:"notReached"`
	Total      int    `json:"total"`
	// UnavailableWeight is the weight mid-update now, across every rollout;
	// MaxUnavailableWeight is what the disruption budget allows.
	UnavailableWeight    float64       `json:"unavailableWeight"`
	MaxUnavailableWeight float64       `json:"maxUnavailableWeight"`
	Unavailable          string        `json:"unavailable"`
	Running              time.Duration `json:"running"`
}

// FleetView is the whole environment.
type FleetView struct {
	Environment string      `json:"environment"`
	Groups      []GroupView `json:"groups"`
	Targets     int         `json:"targets"`
	Nodes       int         `json:"nodes"`
	Weight      float64     `json:"weight"`
	// MaxUnavailable is the disruption budget as configured; Unavailable
	// is the share of weight mid-update now.
	MaxUnavailable string            `json:"maxUnavailable"`
	Unavailable    string            `json:"unavailable"`
	EnvOK          bool              `json:"environmentOk"`
	EnvReason      string            `json:"environmentReason"`
	EnvCheck       string            `json:"environmentCheck,omitempty"`
	Resolve        map[string]string `json:"registryErrors,omitempty"`
	At             time.Time         `json:"at"`
}

// NodeView is one machine.
type NodeView struct {
	Name    string            `json:"name"`
	Weight  float64           `json:"weight"`
	Share   string            `json:"share"`
	Labels  map[string]string `json:"labels"`
	Targets []TargetView      `json:"targets"`
}

// Targets returns views for every target the selector matches, or all.
func (c *Controller) Targets(sel targets.Selector) []TargetView {
	set := c.targets()

	c.mu.RLock()
	defer c.mu.RUnlock()

	var out []TargetView

	for i := range set.Targets {
		t := &set.Targets[i]
		if sel != nil && !sel.Match(t) {
			continue
		}

		out = append(out, c.viewLocked(set, t))
	}

	return out
}

// Target returns one target's view.
func (c *Controller) Target(id string) (TargetView, bool) {
	set := c.targets()

	t, ok := set.Get(id)
	if !ok {
		return TargetView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.viewLocked(set, &t), true
}

func (c *Controller) viewLocked(set *targets.Set, t *targets.Target) TargetView {
	group := set.Group(t)
	v := TargetView{Target: *t, Group: group, Owner: set.Owner(t), Wave: set.Wave(t), Health: Healthy, Hooks: c.hookRunsFor(t.ID)}

	if d, ok := c.desiredFor(group, t); ok {
		v.Desired, v.Revision = d.Digest, d.Revision
	}

	live, known := c.liveKnown(t.ID)
	v.Live = live

	switch {
	case !known || v.Desired == "":
		v.Sync = Unknown
		v.Reason = "not reachable"

		if l, ok := c.live[t.ID]; ok && l.Reason != "" {
			v.Reason = "not reachable: " + l.Reason
		}

		if v.Desired == "" && known {
			v.Reason = "tag not resolved yet"
			if msg, failed := c.resolveErrors[t.Image]; failed {
				v.Reason = "registry: " + msg
			}
		}
	case live == v.Desired:
		v.Sync = Synced
		v.Reason = "on " + shortDigest(live)
	default:
		v.Sync = OutOfSync
		v.Reason = "on " + shortDigest(live) + ", desired " + shortDigest(v.Desired)
	}

	if s := c.suspensionFor(t); s != nil {
		v.Health, v.Suspension = Suspended, s
		v.Reason = "suspended by " + s.Actor + " until " + s.ExpiresAt.UTC().Format("Mon 15:04") + ": " + s.Reason
	}

	if reason, ok := c.degraded[t.ID]; ok && v.Health != Suspended {
		v.Health, v.Reason = Degraded, reason
	}

	if r := c.activeRollout(group); r != nil {
		if rt := r.target(t.ID); rt != nil {
			v.Rollout = r.ID

			switch rt.Phase {
			case PhasePending:
				if v.Health == Healthy {
					v.Reason = "not reached yet"
				}
			case PhaseUpdating, PhaseReady:
				if v.Health != Suspended {
					v.Health, v.Reason = Progressing, rt.Reason
				}

				if rt.Phase == PhaseReady && r.State == Soaking {
					v.Reason = "in soak for batch " + itoa(rt.Batch)
				}
			case PhasePassed:
				if v.Health == Healthy {
					v.Reason = "updated in batch " + itoa(rt.Batch)
				}
			case PhaseFailed:
				if v.Health != Suspended {
					v.Health, v.Reason = Degraded, rt.Reason
				}
			case PhaseSkipped:
			}
		}
	}

	return v
}

// Group returns one group's roll-up and its targets.
func (c *Controller) Group(name string) (GroupView, []TargetView, bool) {
	set := c.targets()

	members := set.InGroup(name)
	if len(members) == 0 {
		return GroupView{}, nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	views := make([]TargetView, 0, len(members))
	for i := range members {
		views = append(views, c.viewLocked(set, &members[i]))
	}

	return c.groupLocked(set, name, views), views, true
}

func (c *Controller) groupLocked(set *targets.Set, name string, views []TargetView) GroupView {
	g := GroupView{Name: name, Policy: c.policyFor(name), Sync: Synced, Health: Healthy, Desired: map[string]string{}, Revisions: map[string]string{}}
	nodes := map[string]struct{}{}
	images := map[string]struct{}{}

	for _, h := range c.cfg.Labels.HiddenGroups {
		if h == name {
			g.Hidden = true
		}
	}

	for i := range views {
		v := &views[i]
		g.Targets++
		nodes[v.Node] = struct{}{}
		images[v.Image] = struct{}{}

		if i == 0 {
			g.Owner = v.Owner
			if c.cfg.Labels.Section != "" {
				g.Section = v.Labels[c.cfg.Labels.Section]
			}
		}

		if v.Desired != "" {
			g.Desired[v.Image] = v.Desired
			if v.Revision != "" {
				g.Revisions[v.Image] = v.Revision
			}
		}

		if v.Sync == Synced {
			g.OnDesired++
		}

		g.Sync = worseSync(g.Sync, v.Sync)
		g.Health = worseHealth(g.Health, v.Health)
	}

	g.Nodes = len(nodes)
	for n := range nodes {
		g.Weight += set.NodeWeight(n)
	}

	for img := range images {
		g.Images = append(g.Images, img)
	}

	sort.Strings(g.Images)

	if r := c.activeRollout(name); r != nil {
		rv := c.rolloutViewLocked(set, r)
		g.Rollout = &rv
		g.Reason = r.Reason
	} else {
		g.Reason = groupReason(&g, views)
	}

	return g
}

func groupReason(g *GroupView, views []TargetView) string {
	switch {
	case g.Sync == Synced && len(g.Desired) > 0:
		return "On " + shortDigest(firstValue(g.Desired))
	case g.Sync == Unknown:
		return itoa(g.Targets-g.OnDesired) + " targets unreachable or unresolved"
	}

	for i := range views {
		if views[i].Health == Degraded {
			return views[i].Reason
		}
	}

	return itoa(g.Targets-g.OnDesired) + " of " + itoa(g.Targets) + " not on " + shortDigest(firstValue(g.Desired))
}

// Fleet returns every group's roll-up.
func (c *Controller) Fleet() FleetView {
	set := c.targets()
	now := c.clock.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	f := FleetView{Environment: c.cfg.Environment, Targets: set.Len(), Nodes: len(set.Nodes()), Weight: set.TotalWeight(),
		MaxUnavailable: c.cfg.DisruptionBudget.MaxUnavailable.String(), EnvOK: c.envOK, EnvReason: c.envReason, EnvCheck: c.cfg.Hooks.Environment.Program, At: now}
	f.Unavailable = pct(c.inFlightWeight(set), set.TotalWeight())

	if len(c.resolveErrors) > 0 {
		f.Resolve = map[string]string{}
		for k, v := range c.resolveErrors {
			f.Resolve[k] = v
		}
	}

	for _, name := range set.Groups() {
		members := set.InGroup(name)

		views := make([]TargetView, 0, len(members))
		for i := range members {
			views = append(views, c.viewLocked(set, &members[i]))
		}

		f.Groups = append(f.Groups, c.groupLocked(set, name, views))
	}

	return f
}

// Node returns one machine's targets.
func (c *Controller) Node(name string) (NodeView, bool) {
	set := c.targets()

	members := set.Node(name)
	if len(members) == 0 {
		return NodeView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	n := NodeView{Name: name, Weight: set.NodeWeight(name), Share: pct(set.NodeWeight(name), set.TotalWeight()), Labels: map[string]string{}}

	for i := range members {
		n.Targets = append(n.Targets, c.viewLocked(set, &members[i]))

		for k, v := range members[i].Labels {
			if k == c.cfg.Labels.Group || k == c.cfg.Labels.Owner {
				continue
			}

			n.Labels[k] = v
		}
	}

	return n, true
}

// Rollouts lists every rollout, newest first.
func (c *Controller) Rollouts() []RolloutView {
	set := c.targets()

	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]RolloutView, 0, len(c.rollouts))
	for _, r := range c.rollouts {
		out = append(out, c.rolloutViewLocked(set, r))
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}

		return out[i].ID > out[j].ID
	})

	return out
}

// Rollout returns one rollout.
func (c *Controller) Rollout(id string) (RolloutView, bool) {
	set := c.targets()

	c.mu.RLock()
	defer c.mu.RUnlock()

	r, ok := c.rollouts[id]
	if !ok {
		return RolloutView{}, false
	}

	return c.rolloutViewLocked(set, r), true
}

func (c *Controller) rolloutViewLocked(set *targets.Set, r *Rollout) RolloutView {
	cp := *r
	cp.Targets = append([]RolloutTarget(nil), r.Targets...)
	cp.Batches = append([]Batch(nil), r.Batches...)

	v := RolloutView{Rollout: cp, Digest: r.DigestShort(), Total: len(r.Targets)}

	for _, rev := range sortedValues(r.Revisions) {
		v.Revision = rev

		break
	}

	// Only an observed digest counts as being on the new build, whatever
	// phase the target is in.
	for i := range r.Targets {
		rt := &r.Targets[i]

		switch rt.Phase {
		case PhasePassed, PhaseReady, PhaseUpdating, PhaseFailed:
			if rt.Updated {
				v.OnNewBuild++
			}
		case PhasePending:
			v.NotReached++
		case PhaseSkipped:
		}
	}

	v.MaxUnavailableWeight = c.cfg.DisruptionBudget.MaxUnavailable.OfWeight(set.TotalWeight())
	v.UnavailableWeight = c.inFlightWeight(set)
	v.Unavailable = pct(v.UnavailableWeight, set.TotalWeight()) + " of " + c.cfg.DisruptionBudget.MaxUnavailable.String()

	end := r.EndedAt
	if end.IsZero() {
		end = c.clock.Now()
	}

	v.Running = end.Sub(r.CreatedAt)

	return v
}

// EnvironmentStatus reports the last environment check.
func (c *Controller) EnvironmentStatus() (ok bool, reason string, checkedAt time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.envOK, c.envReason, c.envCheckedAt
}

// Suspensions lists active suspensions sorted by id.
func (c *Controller) Suspensions() []Suspension {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]Suspension, 0, len(c.suspensions))
	for _, s := range c.suspensions {
		out = append(out, s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func worseSync(a, b SyncState) SyncState {
	rank := map[SyncState]int{Synced: 0, OutOfSync: 1, Unknown: 2}
	if rank[b] > rank[a] {
		return b
	}

	return a
}

func worseHealth(a, b Health) Health {
	rank := map[Health]int{Healthy: 0, Suspended: 1, Progressing: 2, Degraded: 3}
	if rank[b] > rank[a] {
		return b
	}

	return a
}

func firstValue(m map[string]string) string {
	for _, v := range sortedValues(m) {
		return v
	}

	return ""
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
