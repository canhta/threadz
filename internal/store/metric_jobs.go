package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// MetricWindowMinutes are the minutes after publication at which an action is
// measured; schema.sql constrains metric_jobs.window_minutes to this list.
var MetricWindowMinutes = []int{15, 60, 180, 360, 720, 1440, 2880, 5760, 10080}

// CohortWindowMinutes are the windows whose samples are episodes: actions are
// compared with each other only at the same age.
var CohortWindowMinutes = []int{60, 360, 1440}

// settleAfterMinutes is the first window at which a flat action stops being measured.
const settleAfterMinutes = 1440

// settleViewsPercent is the largest growth in views, in percent of the newest
// count, between the last two samples of a settled action; likes and replies
// must not have changed at all.
const settleViewsPercent = 2

// MetricValues contains counters measured together for one published action.
// A nil Quotes, Shares, Followers or AuthorReplied is unavailable, never zero.
type MetricValues struct {
	Views, Likes, Replies, Reposts int64
	Quotes, Shares, Followers      *int64
	AuthorReplied                  *bool
}

// MetricJob identifies an immutable measurement target and its current fence.
type MetricJob struct {
	AgentID       string
	ActionID      int64
	WindowMinutes int
	DueAt         time.Time
	PostedAt      time.Time
	ThreadURL     string
	ClaimVersion  int64
	// Attempts counts the deferrals that blamed the read, not the source.
	Attempts int
}

// metricSeedWindow is shorter than the 30 days a completed job is kept.
const metricSeedWindow = 29 * 24 * time.Hour

// ErrMetricClaim identifies an invalid, stale, foreign or expired metric claim.
var ErrMetricClaim = errors.New("metric claim unavailable")

// SeedMetricJobs creates fixed targets only from confirmed non-dry-run actions.
// Actions published more than metricSeedWindow before now are not seeded: their
// completed jobs are pruned, and a seed must not recreate them.
// Invalid publish times fail the entire seed operation without guessing a clock.
func (s *Store) SeedMetricJobs(ctx context.Context, agentID string, now time.Time) (int, error) {
	if agentID == "" {
		return 0, errors.New("seeding metrics: missing agent")
	}
	count := 0
	err := s.write(ctx, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `SELECT a.id,a.posted_at FROM actions a JOIN agents g ON g.id=a.agent_id WHERE a.agent_id=? AND a.dry_run=0 AND trim(a.thread_url)<>'' AND a.media_id<>'' ORDER BY a.id`, agentID)
		if err != nil {
			return fmt.Errorf("reading metric actions: %w", err)
		}
		type action struct {
			id int64
			at time.Time
		}
		actions := []action{}
		for rows.Next() {
			var a action
			var stamp string
			if err = rows.Scan(&a.id, &stamp); err != nil {
				rows.Close()
				return fmt.Errorf("reading metric action: %w", err)
			}
			a.at, err = parseTime(stamp)
			if err != nil || !validTime(a.at) || !validTime(a.at.Add(24*time.Hour)) {
				rows.Close()
				return errors.New("seeding metrics: invalid published timestamp")
			}
			if a.at.Before(now.Add(-metricSeedWindow)) {
				continue
			}
			actions = append(actions, a)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("reading metric actions: %w", err)
		}
		for _, a := range actions {
			for _, minutes := range MetricWindowMinutes {
				due := formatTime(a.at.Add(time.Duration(minutes) * time.Minute))
				r, err := conn.ExecContext(ctx, `INSERT INTO metric_jobs(agent_id,action_id,window_minutes,due_at,not_before) VALUES(?,?,?,?,?) ON CONFLICT(agent_id,action_id,window_minutes) DO NOTHING`, agentID, a.id, minutes, due, due)
				if err != nil {
					return fmt.Errorf("seeding metric target: %w", err)
				}
				n, err := r.RowsAffected()
				if err != nil {
					return fmt.Errorf("counting metric targets: %w", err)
				}
				count += int(n)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// ClaimMetricJobs leases up to limit action groups, coalescing their ready
// targets. A live lease on any target excludes the entire action group.
func (s *Store) ClaimMetricJobs(ctx context.Context, agentID string, now time.Time, lease time.Duration, limit int) ([]MetricJob, error) {
	if agentID == "" || !validTime(now) || lease <= 0 || !validTime(now.Add(lease)) || limit < 1 || limit > 1000 {
		return nil, errors.New("claiming metrics: invalid options")
	}
	jobs := []MetricJob{}
	err := s.write(ctx, func(conn *sql.Conn) error {
		stamp := formatTime(now)
		rows, err := conn.QueryContext(ctx, `SELECT j.action_id FROM metric_jobs j JOIN actions a ON a.id=j.action_id AND a.agent_id=j.agent_id JOIN agents g ON g.id=j.agent_id WHERE j.agent_id=? AND j.completed_at IS NULL AND g.status='running' AND a.dry_run=0 AND trim(a.thread_url)<>'' GROUP BY j.action_id HAVING MIN(MAX(j.due_at,j.not_before))<=? AND (MAX(j.lease_until) IS NULL OR MAX(j.lease_until)<=?) ORDER BY MIN(MAX(j.due_at,j.not_before)),j.action_id LIMIT ?`, agentID, stamp, stamp, limit)
		if err != nil {
			return fmt.Errorf("selecting metric claims: %w", err)
		}
		ids := []int64{}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("reading metric claim group: %w", err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("reading metric claim groups: %w", err)
		}
		for _, id := range ids {
			var version int64
			if err = conn.QueryRowContext(ctx, `SELECT MAX(claim_version)+1 FROM metric_jobs WHERE agent_id=? AND action_id=?`, agentID, id).Scan(&version); err != nil {
				return fmt.Errorf("reading metric fence: %w", err)
			}
			if _, err = conn.ExecContext(ctx, `UPDATE metric_jobs SET claim_version=?,claimed_at=?,lease_until=?,reason='' WHERE agent_id=? AND action_id=? AND completed_at IS NULL AND due_at<=? AND not_before<=?`, version, stamp, formatTime(now.Add(lease)), agentID, id, stamp, stamp); err != nil {
				return fmt.Errorf("leasing metric targets: %w", err)
			}
			rows, err = conn.QueryContext(ctx, `SELECT j.window_minutes,j.due_at,a.posted_at,a.thread_url,j.attempts FROM metric_jobs j JOIN actions a ON a.id=j.action_id AND a.agent_id=j.agent_id WHERE j.agent_id=? AND j.action_id=? AND j.claim_version=? AND j.completed_at IS NULL ORDER BY j.window_minutes`, agentID, id, version)
			if err != nil {
				return fmt.Errorf("reading leased metrics: %w", err)
			}
			for rows.Next() {
				job := MetricJob{AgentID: agentID, ActionID: id, ClaimVersion: version}
				var due, posted string
				if err = rows.Scan(&job.WindowMinutes, &due, &posted, &job.ThreadURL, &job.Attempts); err != nil {
					rows.Close()
					return fmt.Errorf("reading leased metric: %w", err)
				}
				job.DueAt, err = parseTime(due)
				if err != nil {
					rows.Close()
					return errors.New("claiming metrics: invalid target timestamp")
				}
				job.PostedAt, err = parseTime(posted)
				if err != nil || !validTime(job.PostedAt) || !job.DueAt.Equal(job.PostedAt.Add(time.Duration(job.WindowMinutes)*time.Minute)) {
					rows.Close()
					return errors.New("claiming metrics: invalid published timestamp")
				}
				jobs = append(jobs, job)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return fmt.Errorf("reading leased metrics: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// NextMetricDue includes retry not-before times and action-wide lease fences.
func (s *Store) NextMetricDue(ctx context.Context, agentID string) (time.Time, bool, error) {
	var stamp sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT MIN(CASE WHEN last_lease IS NOT NULL AND earliest<last_lease THEN last_lease ELSE earliest END) FROM (SELECT MIN(COALESCE(j.lease_until,MAX(j.due_at,j.not_before))) AS earliest,MAX(j.lease_until) AS last_lease FROM metric_jobs j JOIN agents g ON g.id=j.agent_id JOIN actions a ON a.id=j.action_id AND a.agent_id=j.agent_id WHERE j.agent_id=? AND g.status='running' AND a.dry_run=0 AND trim(a.thread_url)<>'' AND j.completed_at IS NULL GROUP BY j.action_id)`, agentID).Scan(&stamp)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading next metric target: %w", err)
	}
	if !stamp.Valid {
		return time.Time{}, false, nil
	}
	at, err := parseTime(stamp.String)
	if err != nil || !validTime(at) {
		return time.Time{}, false, errors.New("reading next metric target: invalid timestamp")
	}
	return at, true, nil
}

// metricClaims checks the full claim set and immutable action binding while the
// writer lock is held. It returns true only for the same already-completed set.
func metricClaims(ctx context.Context, conn *sql.Conn, jobs []MetricJob, at time.Time, requireRunning bool) (bool, error) {
	if len(jobs) < 1 || len(jobs) > len(MetricWindowMinutes) || !validTime(at) {
		return false, ErrMetricClaim
	}
	first := jobs[0]
	if first.AgentID == "" || first.ActionID <= 0 || first.ClaimVersion <= 0 {
		return false, ErrMetricClaim
	}
	wanted := make(map[int]MetricJob, len(jobs))
	for _, job := range jobs {
		if job.AgentID != first.AgentID || job.ActionID != first.ActionID || job.ClaimVersion != first.ClaimVersion || !slices.Contains(MetricWindowMinutes, job.WindowMinutes) {
			return false, ErrMetricClaim
		}
		if _, ok := wanted[job.WindowMinutes]; ok {
			return false, ErrMetricClaim
		}
		wanted[job.WindowMinutes] = job
	}
	var posted, url, status string
	var dry int
	err := conn.QueryRowContext(ctx, `SELECT a.posted_at,a.thread_url,a.dry_run,g.status FROM actions a JOIN agents g ON g.id=a.agent_id WHERE a.id=? AND a.agent_id=?`, first.ActionID, first.AgentID).Scan(&posted, &url, &dry, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrMetricClaim
	}
	if err != nil {
		return false, fmt.Errorf("reading metric claim action: %w", err)
	}
	postedAt, err := parseTime(posted)
	if err != nil || !validTime(postedAt) || dry != 0 || url == "" {
		return false, ErrMetricClaim
	}
	rows, err := conn.QueryContext(ctx, `SELECT window_minutes,due_at,claim_version,claimed_at,lease_until,completed_at FROM metric_jobs WHERE agent_id=? AND action_id=?`, first.AgentID, first.ActionID)
	if err != nil {
		return false, fmt.Errorf("reading metric fences: %w", err)
	}
	defer rows.Close()
	found, completed, leased := 0, 0, 0
	for rows.Next() {
		var minutes int
		var due string
		var version int64
		var start, lease, done sql.NullString
		if err = rows.Scan(&minutes, &due, &version, &start, &lease, &done); err != nil {
			return false, fmt.Errorf("reading metric fence: %w", err)
		}
		if !done.Valid && lease.Valid {
			leased++
		}
		job, ok := wanted[minutes]
		if !ok {
			continue
		}
		found++
		if version != job.ClaimVersion || !job.PostedAt.Equal(postedAt) || job.ThreadURL != url || !job.DueAt.Equal(postedAt.Add(time.Duration(minutes)*time.Minute)) || formatTime(job.DueAt) != due {
			return false, ErrMetricClaim
		}
		if done.Valid {
			completed++
			continue
		}
		if !start.Valid || !lease.Valid || formatTime(at) < start.String || formatTime(at) >= lease.String || formatTime(at) < due {
			return false, ErrMetricClaim
		}
	}
	if err = rows.Err(); err != nil {
		return false, fmt.Errorf("reading metric fences: %w", err)
	}
	if found != len(jobs) {
		return false, ErrMetricClaim
	}
	if completed == len(jobs) {
		return true, nil
	}
	if completed > 0 || leased != len(jobs) || (requireRunning && status != "running") {
		return false, ErrMetricClaim
	}
	return false, nil
}

// CompleteMetricJobs commits one current sample and all claimed checkpoints in
// one transaction. An identical completed claim is an idempotent no-op.
func (s *Store) CompleteMetricJobs(ctx context.Context, jobs []MetricJob, at time.Time, values MetricValues) (bool, error) {
	counters := []int64{values.Views, values.Likes, values.Replies, values.Reposts}
	for _, p := range []*int64{values.Quotes, values.Shares, values.Followers} {
		if p != nil {
			counters = append(counters, *p)
		}
	}
	for _, value := range counters {
		if value < 0 || value > 1<<53 {
			return false, errors.New("completing metrics: invalid counters")
		}
	}
	committed := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		done, err := metricClaims(ctx, conn, jobs, at, true)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		first := jobs[0]
		measured := formatStoreTimestamp(at)
		if _, err = conn.ExecContext(ctx, `INSERT INTO metrics(agent_id,action_id,measured_at,views,likes,replies,reposts,quotes,shares,followers,author_replied) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, first.AgentID, first.ActionID, measured, values.Views, values.Likes, values.Replies, values.Reposts, values.Quotes, values.Shares, values.Followers, values.AuthorReplied); err != nil {
			return fmt.Errorf("inserting metric sample: %w", err)
		}
		for _, job := range jobs {
			r, err := conn.ExecContext(ctx, `UPDATE metric_jobs SET completed_at=?,claimed_at=NULL,lease_until=NULL,reason='' WHERE agent_id=? AND action_id=? AND window_minutes=? AND claim_version=? AND completed_at IS NULL`, measured, job.AgentID, job.ActionID, job.WindowMinutes, job.ClaimVersion)
			if err != nil {
				return fmt.Errorf("completing metric checkpoint: %w", err)
			}
			n, err := r.RowsAffected()
			if err != nil {
				return fmt.Errorf("checking metric checkpoint: %w", err)
			}
			if n != 1 {
				return ErrMetricClaim
			}
		}
		if err = settleMetricJobs(ctx, conn, jobs, measured, values); err != nil {
			return err
		}
		committed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return committed, nil
}

// settleMetricJobs ends the remaining jobs of the action with reason settled
// when a sample at or after the settle window differs from the one before it by
// less than settleViewsPercent of its views and by nothing in likes and replies.
func settleMetricJobs(ctx context.Context, conn *sql.Conn, jobs []MetricJob, measured string, values MetricValues) error {
	if !slices.ContainsFunc(jobs, func(j MetricJob) bool { return j.WindowMinutes >= settleAfterMinutes }) {
		return nil
	}
	first := jobs[0]
	var views, likes, replies int64
	err := conn.QueryRowContext(ctx, `SELECT views,likes,replies FROM metrics WHERE action_id=? AND measured_at<? ORDER BY measured_at DESC LIMIT 1`, first.ActionID, measured).Scan(&views, &likes, &replies)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading previous metric sample: %w", err)
	}
	grown := max(values.Views-views, views-values.Views)
	if likes != values.Likes || replies != values.Replies || (grown != 0 && grown*100 >= settleViewsPercent*values.Views) {
		return nil
	}
	if _, err = conn.ExecContext(ctx, `UPDATE metric_jobs SET completed_at=?,claimed_at=NULL,lease_until=NULL,reason='settled' WHERE agent_id=? AND action_id=? AND completed_at IS NULL`, measured, first.AgentID, first.ActionID); err != nil {
		return fmt.Errorf("settling metric targets: %w", err)
	}
	return nil
}

// YoungOwnActions returns those of ids that are confirmed actions of the agent
// posted at or after since, in id order. No ids means every such action.
func (s *Store) YoungOwnActions(ctx context.Context, agentID string, ids []int64, since time.Time) ([]int64, error) {
	list, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("reading young actions: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM actions WHERE agent_id=? AND dry_run=0 AND trim(thread_url)<>'' AND media_id<>'' AND julianday(posted_at)>=julianday(?) AND (?=0 OR id IN (SELECT value FROM json_each(?))) ORDER BY id`, agentID, formatTime(since), len(ids), string(list))
	if err != nil {
		return nil, fmt.Errorf("reading young actions: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("reading young action: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// RecordMetricSample stores a sample taken outside the schedule for a
// confirmed own action and leaves its jobs as they are.
func (s *Store) RecordMetricSample(ctx context.Context, agentID string, actionID int64, at time.Time, values MetricValues) error {
	if agentID == "" || actionID <= 0 || !validTime(at) {
		return errors.New("recording metric sample: invalid options")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		r, err := conn.ExecContext(ctx, `INSERT INTO metrics(agent_id,action_id,measured_at,views,likes,replies,reposts,quotes,shares,followers,author_replied) SELECT a.agent_id,a.id,?,?,?,?,?,?,?,?,? FROM actions a WHERE a.id=? AND a.agent_id=? AND a.dry_run=0 AND trim(a.thread_url)<>''`, formatStoreTimestamp(at), values.Views, values.Likes, values.Replies, values.Reposts, values.Quotes, values.Shares, values.Followers, values.AuthorReplied, actionID, agentID)
		if err != nil {
			return fmt.Errorf("inserting metric sample: %w", err)
		}
		if n, err := r.RowsAffected(); err != nil || n != 1 {
			return errors.New("recording metric sample: unknown action")
		}
		return nil
	})
}

// MaxMetricAttempts is how many counted deferrals a job takes before it ends
// with the reason of the last one, so a post that cannot be read stops retrying.
const MaxMetricAttempts = 8

// MetricReasonCode reports whether reason is a stored metric job reason.
func MetricReasonCode(reason string) bool {
	switch reason {
	case "metrics_unavailable", "source_unavailable", "read_failed", "binding_mismatch", "auth_required", "cancelled", "persistence_failed", "measurement_failed", "media_gone", "settled":
		return true
	}
	return false
}

// DeferMetricJobs releases only the current complete claim set. Reason codes
// cannot retain arbitrary backend errors, URLs, usernames or external content.
// A deferral that is not the source's or the owner's fault counts as an attempt;
// the MaxMetricAttempts-th one ends the job.
func (s *Store) DeferMetricJobs(ctx context.Context, jobs []MetricJob, now, next time.Time, reason string) error {
	if !MetricReasonCode(reason) || reason == "media_gone" || reason == "settled" {
		return errors.New("deferring metrics: invalid reason code")
	}
	if !validTime(now) || !validTime(next) || !next.After(now) {
		return errors.New("deferring metrics: future retry required")
	}
	counted := 1
	if reason == "cancelled" || reason == "auth_required" || reason == "source_unavailable" {
		counted = 0
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		done, err := metricClaims(ctx, conn, jobs, now, false)
		if err != nil {
			return err
		}
		if done {
			return ErrMetricClaim
		}
		for _, job := range jobs {
			r, err := conn.ExecContext(ctx, `UPDATE metric_jobs SET not_before=?,claimed_at=NULL,lease_until=NULL,reason=?,attempts=attempts+?,completed_at=CASE WHEN attempts+?>=? THEN ? END WHERE agent_id=? AND action_id=? AND window_minutes=? AND claim_version=? AND completed_at IS NULL`, formatTime(next), reason, counted, counted, MaxMetricAttempts, formatStoreTimestamp(now), job.AgentID, job.ActionID, job.WindowMinutes, job.ClaimVersion)
			if err != nil {
				return fmt.Errorf("deferring metric target: %w", err)
			}
			n, err := r.RowsAffected()
			if err != nil {
				return fmt.Errorf("checking metric deferral: %w", err)
			}
			if n != 1 {
				return ErrMetricClaim
			}
		}
		return nil
	})
}

// EndGoneMetricJobs completes every open job of the claimed action, due or not,
// with reason media_gone: the post was deleted, so nothing is left to measure.
func (s *Store) EndGoneMetricJobs(ctx context.Context, jobs []MetricJob, at time.Time) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		done, err := metricClaims(ctx, conn, jobs, at, true)
		if err != nil {
			return err
		}
		if done {
			return ErrMetricClaim
		}
		if _, err = conn.ExecContext(ctx, `UPDATE metric_jobs SET completed_at=?,claimed_at=NULL,lease_until=NULL,reason='media_gone' WHERE agent_id=? AND action_id=? AND completed_at IS NULL`, formatStoreTimestamp(at), jobs[0].AgentID, jobs[0].ActionID); err != nil {
			return fmt.Errorf("ending gone metric targets: %w", err)
		}
		return nil
	})
}

// BeginMetricWake opens an audit scope without touching the agent's schedule or
// self-note. Agent and maintenance wakes cannot overlap for the same agent.
func (s *Store) BeginMetricWake(ctx context.Context, agentID string, at time.Time) (int64, error) {
	if agentID == "" || !validTime(at) {
		return 0, errors.New("starting metric wake: invalid agent or time")
	}
	var id int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		r, err := conn.ExecContext(ctx, `INSERT INTO wakes(agent_id,started_at,kind) SELECT ?,?,'metrics' WHERE EXISTS(SELECT 1 FROM agents WHERE id=? AND status='running') AND NOT EXISTS(SELECT 1 FROM wakes WHERE agent_id=? AND ended_at IS NULL)`, agentID, formatStoreTimestamp(at), agentID, agentID)
		if err != nil {
			return fmt.Errorf("starting metric wake: %w", err)
		}
		n, err := r.RowsAffected()
		if err != nil {
			return fmt.Errorf("checking metric wake: %w", err)
		}
		if n != 1 {
			return errors.New("starting metric wake: agent unavailable")
		}
		id, err = r.LastInsertId()
		if err != nil {
			return fmt.Errorf("identifying metric wake: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// PendingMetricJobs counts the measurement targets of confirmed actions that are not complete.
func (s *Store) PendingMetricJobs(ctx context.Context, agentID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM metric_jobs j JOIN actions a ON a.id=j.action_id AND a.agent_id=j.agent_id WHERE j.agent_id=? AND a.dry_run=0 AND trim(a.thread_url)<>'' AND j.completed_at IS NULL`, agentID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("counting pending metric targets: %w", err)
	}
	return n, nil
}
