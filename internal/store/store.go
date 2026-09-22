// Package store persists the controller's decisions in one SQLite file.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // database/sql driver

	"github.com/ethpandaops/rolloor/internal/reconcile"
)

const schema = `
CREATE TABLE IF NOT EXISTS rollouts (
	id TEXT PRIMARY KEY,
	group_name TEXT NOT NULL,
	state TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS rollouts_group ON rollouts(group_name, created_at);
CREATE TABLE IF NOT EXISTS policies (group_name TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS suspensions (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS live (target_id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS degraded (target_id TEXT PRIMARY KEY, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS desired (image TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS aborted (group_name TEXT PRIMARY KEY, key TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS hook_runs (target_id TEXT NOT NULL, hook TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY (target_id, hook));
CREATE TABLE IF NOT EXISTS events (
	id INTEGER PRIMARY KEY,
	at TEXT NOT NULL,
	actor TEXT NOT NULL,
	action TEXT NOT NULL,
	group_name TEXT NOT NULL DEFAULT '',
	rollout TEXT NOT NULL DEFAULT '',
	target TEXT NOT NULL DEFAULT '',
	selector TEXT NOT NULL DEFAULT '',
	reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS events_group ON events(group_name, id);
CREATE INDEX IF NOT EXISTS events_rollout ON events(rollout, id);
CREATE INDEX IF NOT EXISTS events_target ON events(target, id);
`

// SQLite is the store. One process writes it.
type SQLite struct {
	db *sql.DB
}

var _ reconcile.Store = (*SQLite)(nil)

// Open creates the directory and file if needed and applies the schema.
func Open(ctx context.Context, dir string) (*SQLite, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("store: create %s: %w", dir, err)
	}

	path := filepath.Join(dir, "rolloor.db")

	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()

		return nil, fmt.Errorf("store: apply schema: %w", err)
	}

	return &SQLite{db: db}, nil
}

// Close releases the file.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// Load reads everything back.
func (s *SQLite) Load(ctx context.Context) (*reconcile.Snapshot, error) {
	snap := &reconcile.Snapshot{
		Policies: map[string]reconcile.Policy{},
		Live:     map[string]reconcile.Live{},
		Degraded: map[string]string{},
		Desired:  map[string]reconcile.Desired{},
		Aborted:  map[string]string{},
		HookRuns: map[string]map[string]reconcile.HookRun{},
	}

	if err := s.loadKeyed(ctx, `SELECT target_id, data FROM hook_runs`, func(key string, raw []byte) error {
		var run reconcile.HookRun
		if err := json.Unmarshal(raw, &run); err != nil {
			return err
		}

		if snap.HookRuns[key] == nil {
			snap.HookRuns[key] = map[string]reconcile.HookRun{}
		}

		snap.HookRuns[key][run.Hook] = run

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load hook runs: %w", err)
	}

	if err := s.loadJSON(ctx, `SELECT data FROM rollouts ORDER BY id`, func(raw []byte) error {
		var r reconcile.Rollout
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}

		snap.Rollouts = append(snap.Rollouts, &r)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load rollouts: %w", err)
	}

	if err := s.loadKeyed(ctx, `SELECT group_name, data FROM policies`, func(key string, raw []byte) error {
		var p reconcile.Policy
		if err := json.Unmarshal(raw, &p); err != nil {
			return err
		}

		snap.Policies[key] = p

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load policies: %w", err)
	}

	if err := s.loadJSON(ctx, `SELECT data FROM suspensions ORDER BY id`, func(raw []byte) error {
		var sp reconcile.Suspension
		if err := json.Unmarshal(raw, &sp); err != nil {
			return err
		}

		snap.Suspensions = append(snap.Suspensions, sp)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load suspensions: %w", err)
	}

	if err := s.loadKeyed(ctx, `SELECT target_id, data FROM live`, func(key string, raw []byte) error {
		var l reconcile.Live
		if err := json.Unmarshal(raw, &l); err != nil {
			return err
		}

		snap.Live[key] = l

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load live: %w", err)
	}

	if err := s.loadKeyed(ctx, `SELECT target_id, reason FROM degraded`, func(key string, raw []byte) error {
		snap.Degraded[key] = string(raw)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load degraded: %w", err)
	}

	if err := s.loadKeyed(ctx, `SELECT image, data FROM desired`, func(key string, raw []byte) error {
		var d reconcile.Desired
		if err := json.Unmarshal(raw, &d); err != nil {
			return err
		}

		snap.Desired[key] = d

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load desired: %w", err)
	}

	if err := s.loadKeyed(ctx, `SELECT group_name, key FROM aborted`, func(key string, raw []byte) error {
		snap.Aborted[key] = string(raw)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("store: load aborted: %w", err)
	}

	var maxID sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM events`).Scan(&maxID); err != nil {
		return nil, fmt.Errorf("store: load events: %w", err)
	}

	snap.NextEventID = maxID.Int64 + 1

	return snap, nil
}

func (s *SQLite) loadJSON(ctx context.Context, query string, each func([]byte) error) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}

		if err := each(raw); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (s *SQLite) loadKeyed(ctx context.Context, query string, each func(string, []byte) error) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			key string
			raw []byte
		)

		if err := rows.Scan(&key, &raw); err != nil {
			return err
		}

		if err := each(key, raw); err != nil {
			return err
		}
	}

	return rows.Err()
}

// SaveRollout upserts the whole rollout as JSON with a few indexed columns.
func (s *SQLite) SaveRollout(ctx context.Context, r *reconcile.Rollout) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("store: encode rollout: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `INSERT INTO rollouts (id, group_name, state, created_at, data) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET group_name = excluded.group_name, state = excluded.state, data = excluded.data`,
		r.ID, r.Group, string(r.State), r.CreatedAt.UTC().Format(time.RFC3339Nano), raw)
	if err != nil {
		return fmt.Errorf("store: save rollout %s: %w", r.ID, err)
	}

	return nil
}

// SavePolicy upserts a group's policy.
func (s *SQLite) SavePolicy(ctx context.Context, group string, p reconcile.Policy) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("store: encode policy: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO policies (group_name, data) VALUES (?, ?) ON CONFLICT(group_name) DO UPDATE SET data = excluded.data`, group, raw); err != nil {
		return fmt.Errorf("store: save policy %s: %w", group, err)
	}

	return nil
}

// SaveSuspension upserts a suspension.
func (s *SQLite) SaveSuspension(ctx context.Context, sp *reconcile.Suspension) error {
	raw, err := json.Marshal(sp)
	if err != nil {
		return fmt.Errorf("store: encode suspension: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO suspensions (id, data) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`, sp.ID, raw); err != nil {
		return fmt.Errorf("store: save suspension %s: %w", sp.ID, err)
	}

	return nil
}

// DeleteSuspension removes one.
func (s *SQLite) DeleteSuspension(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM suspensions WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: delete suspension %s: %w", id, err)
	}

	return nil
}

// SaveLive upserts a target's live state.
func (s *SQLite) SaveLive(ctx context.Context, id string, l *reconcile.Live) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return fmt.Errorf("store: encode live: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO live (target_id, data) VALUES (?, ?) ON CONFLICT(target_id) DO UPDATE SET data = excluded.data`, id, raw); err != nil {
		return fmt.Errorf("store: save live %s: %w", id, err)
	}

	return nil
}

// DeleteLive forgets a target.
func (s *SQLite) DeleteLive(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM live WHERE target_id = ?`, id); err != nil {
		return fmt.Errorf("store: delete live %s: %w", id, err)
	}

	return nil
}

// SaveDegraded records a quarantine.
func (s *SQLite) SaveDegraded(ctx context.Context, id, reason string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO degraded (target_id, reason) VALUES (?, ?) ON CONFLICT(target_id) DO UPDATE SET reason = excluded.reason`, id, reason); err != nil {
		return fmt.Errorf("store: save degraded %s: %w", id, err)
	}

	return nil
}

// ClearDegraded lifts a quarantine.
func (s *SQLite) ClearDegraded(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM degraded WHERE target_id = ?`, id); err != nil {
		return fmt.Errorf("store: clear degraded %s: %w", id, err)
	}

	return nil
}

// SaveHookRun upserts the last result of one hook for one target.
func (s *SQLite) SaveHookRun(ctx context.Context, id string, run *reconcile.HookRun) error {
	raw, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("store: encode hook run: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO hook_runs (target_id, hook, data) VALUES (?, ?, ?) ON CONFLICT(target_id, hook) DO UPDATE SET data = excluded.data`, id, run.Hook, raw); err != nil {
		return fmt.Errorf("store: save hook run %s/%s: %w", id, run.Hook, err)
	}

	return nil
}

// DeleteHookRuns removes a target's hook results.
func (s *SQLite) DeleteHookRuns(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM hook_runs WHERE target_id = ?`, id); err != nil {
		return fmt.Errorf("store: delete hook runs %s: %w", id, err)
	}

	return nil
}

// SaveDesired remembers a tag's last known digest across restarts.
func (s *SQLite) SaveDesired(ctx context.Context, image string, d reconcile.Desired) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("store: encode desired: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO desired (image, data) VALUES (?, ?) ON CONFLICT(image) DO UPDATE SET data = excluded.data`, image, raw); err != nil {
		return fmt.Errorf("store: save desired %s: %w", image, err)
	}

	return nil
}

// SaveAborted records an abort that must hold across restarts.
func (s *SQLite) SaveAborted(ctx context.Context, group, key string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO aborted (group_name, key) VALUES (?, ?) ON CONFLICT(group_name) DO UPDATE SET key = excluded.key`, group, key); err != nil {
		return fmt.Errorf("store: save aborted %s: %w", group, err)
	}

	return nil
}

// ClearAborted lifts an abort.
func (s *SQLite) ClearAborted(ctx context.Context, group string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM aborted WHERE group_name = ?`, group); err != nil {
		return fmt.Errorf("store: clear aborted %s: %w", group, err)
	}

	return nil
}

// AppendEvent adds one line of history.
func (s *SQLite) AppendEvent(ctx context.Context, e *reconcile.Event) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO events (id, at, actor, action, group_name, rollout, target, selector, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.At.UTC().Format(time.RFC3339Nano), e.Actor, e.Action, e.Group, e.Rollout, e.Target, e.Selector, e.Reason); err != nil {
		return fmt.Errorf("store: append event %d: %w", e.ID, err)
	}

	return nil
}

// Events returns history newest first, filtered.
func (s *SQLite) Events(ctx context.Context, q reconcile.EventQuery) ([]reconcile.Event, error) {
	var (
		where []string
		args  []any
	)

	if q.Group != "" {
		where = append(where, "group_name = ?")
		args = append(args, q.Group)
	}

	if q.Rollout != "" {
		where = append(where, "rollout = ?")
		args = append(args, q.Rollout)
	}

	if q.Target != "" {
		where = append(where, "target = ?")
		args = append(args, q.Target)
	}

	order := "DESC"

	if q.After > 0 {
		where = append(where, "id > ?")
		args = append(args, q.After)
		order = "ASC"
	}

	query := `SELECT id, at, actor, action, group_name, rollout, target, selector, reason FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ") //nolint:gosec // fixed fragments; every value is bound
	}

	query += " ORDER BY id " + order + " LIMIT ?"

	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query events: %w", err)
	}
	defer rows.Close()

	var out []reconcile.Event

	for rows.Next() {
		var (
			e  reconcile.Event
			at string
		)

		if err := rows.Scan(&e.ID, &at, &e.Actor, &e.Action, &e.Group, &e.Rollout, &e.Target, &e.Selector, &e.Reason); err != nil {
			return nil, fmt.Errorf("store: scan event: %w", err)
		}

		parsed, perr := time.Parse(time.RFC3339Nano, at)
		if perr != nil {
			return nil, fmt.Errorf("store: event %d has a bad timestamp %q: %w", e.ID, at, perr)
		}

		e.At = parsed

		out = append(out, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read events: %w", err)
	}

	return out, nil
}
