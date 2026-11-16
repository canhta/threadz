package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AccountDay is one Pacific day of an account metric. Value is nil when the
// API returned no bucket for the day: a missing day is never stored as 0.
type AccountDay struct {
	Day              string
	StartsAt, EndsAt time.Time
	Value            *int64
}

// UpsertAccountDays stores the days one account insights read returned. A
// later read of a day replaces its value; a missing value never replaces a
// stored one.
func (s *Store) UpsertAccountDays(ctx context.Context, agentID, metric string, days []AccountDay, at time.Time) error {
	if agentID == "" || metric == "" || at.IsZero() {
		return errors.New("storing account days: invalid agent, metric or time")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		for _, d := range days {
			if _, err := conn.ExecContext(ctx, `INSERT INTO account_daily(agent_id,day,metric,value,fetched_at,starts_at,ends_at) VALUES(?,?,?,?,?,?,?)
				ON CONFLICT(agent_id,day,metric) DO UPDATE SET value=COALESCE(excluded.value,account_daily.value),fetched_at=excluded.fetched_at`,
				agentID, d.Day, metric, d.Value, formatStoreTimestamp(at), formatStoreTimestamp(d.StartsAt), formatStoreTimestamp(d.EndsAt)); err != nil {
				return fmt.Errorf("storing account day %s: %w", d.Day, err)
			}
		}
		return nil
	})
}

// AccountDailyCount is the number of stored account days; zero means the
// series has never been read.
func (s *Store) AccountDailyCount(ctx context.Context, agentID string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_daily WHERE agent_id=?`, agentID).Scan(&n); err != nil {
		return 0, fmt.Errorf("counting account days: %w", err)
	}
	return n, nil
}

// RecordAccountSample appends one reading of an account total over since..until
// and reports whether it did: a value equal to the previous reading of the same
// metric and window is skipped.
func (s *Store) RecordAccountSample(ctx context.Context, agentID, metric string, since, until time.Time, value int64, source string, at time.Time) (bool, error) {
	if agentID == "" || metric == "" || source == "" || value < 0 || at.IsZero() {
		return false, errors.New("recording account sample: invalid agent, metric, value or time")
	}
	recorded := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		var last int64
		err := conn.QueryRowContext(ctx, `SELECT value FROM account_samples WHERE agent_id=? AND metric=? AND since=? AND until=? ORDER BY measured_at DESC LIMIT 1`,
			agentID, metric, formatStoreTimestamp(since), formatStoreTimestamp(until)).Scan(&last)
		if err == nil && last == value {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("reading last account sample: %w", err)
		}
		if _, err = conn.ExecContext(ctx, `INSERT INTO account_samples(agent_id,measured_at,metric,since,until,value,source) VALUES(?,?,?,?,?,?,?)`,
			agentID, formatStoreTimestamp(at), metric, formatStoreTimestamp(since), formatStoreTimestamp(until), value, source); err != nil {
			return fmt.Errorf("recording account sample: %w", err)
		}
		recorded = true
		return nil
	})
	return recorded, err
}
