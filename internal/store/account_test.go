package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/store"
)

func v(n int64) *int64 { return &n }

type dailyRow struct {
	value   *int64
	settled bool
	fetched string
}

func readDaily(t *testing.T, s *store.Store, day string) dailyRow {
	t.Helper()
	var r dailyRow
	var settled int
	if err := s.DB().QueryRow(`SELECT value,settled,fetched_at FROM account_daily WHERE agent_id='a' AND day=? AND metric='views'`, day).Scan(&r.value, &settled, &r.fetched); err != nil {
		t.Fatalf("reading %s: %v", day, err)
	}
	r.settled = settled == 1
	return r
}

func TestAccountDailyUpsertSettledAndNull(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ends := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	day := func(val *int64) store.AccountDay {
		return store.AccountDay{Day: "2026-11-14", StartsAt: ends.Add(-24 * time.Hour), EndsAt: ends, Value: val}
	}

	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(nil)}, ends.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := readDaily(t, s, "2026-11-14"); got.value != nil || got.settled {
		t.Fatalf("missing day = %+v, want NULL and unsettled, never 0", got)
	}
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(v(100))}, ends.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := readDaily(t, s, "2026-11-14"); got.value == nil || *got.value != 100 || got.settled {
		t.Fatalf("first value = %+v", got)
	}
	// A revision inside the trailing window replaces the value; a missing read keeps it.
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(v(120))}, ends.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(nil)}, ends.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got := readDaily(t, s, "2026-11-14")
	if got.value == nil || *got.value != 120 || got.settled {
		t.Fatalf("after revision and a missing read = %+v, want 120 unsettled", got)
	}
	// Read 3 days after the day closed: it settles, and later reads leave it alone.
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(v(125))}, ends.Add(72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got = readDaily(t, s, "2026-11-14"); *got.value != 125 || !got.settled {
		t.Fatalf("after 3 days = %+v, want 125 settled", got)
	}
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{day(v(999))}, ends.Add(96*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got = readDaily(t, s, "2026-11-14"); *got.value != 125 || got.fetched != "2026-11-18T08:00:00Z" {
		t.Fatalf("settled day rewritten: %+v", got)
	}
	if n, err := s.AccountDailyCount(ctx, "a"); err != nil || n != 1 {
		t.Fatalf("count = %d, %v", n, err)
	}
	if n, err := s.AccountDailyCount(ctx, "b"); err != nil || n != 0 {
		t.Fatalf("other agent count = %d, %v", n, err)
	}
	if err = s.UpsertAccountDays(ctx, "", "views", nil, ends); err == nil {
		t.Fatal("empty agent accepted")
	}
}

func TestAccountSampleSkipsEqualPreviousReading(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	since := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	until := since.Add(72 * time.Hour)
	at := until.Add(time.Hour)
	record := func(metric string, from, to time.Time, value int64, at time.Time) bool {
		t.Helper()
		ok, err := s.RecordAccountSample(ctx, "a", metric, from, to, value, "api", at)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !record("likes", since, until, 5, at) {
		t.Fatal("first reading skipped")
	}
	if record("likes", since, until, 5, at.Add(time.Hour)) {
		t.Fatal("equal reading recorded")
	}
	if !record("likes", since, until, 6, at.Add(2*time.Hour)) {
		t.Fatal("changed reading skipped")
	}
	if !record("likes", since, until, 5, at.Add(3*time.Hour)) {
		t.Fatal("a value equal to an older, not the previous, reading was skipped")
	}
	if !record("replies", since, until, 5, at) || !record("likes", since, until.Add(24*time.Hour), 5, at) {
		t.Fatal("another metric or window shares the previous reading")
	}
	var n int
	if err = s.DB().QueryRow(`SELECT COUNT(*) FROM account_samples WHERE metric='likes'`).Scan(&n); err != nil || n != 4 {
		t.Fatalf("likes rows = %d, %v; want 4", n, err)
	}
	if _, err = s.RecordAccountSample(ctx, "a", "likes", since, until, -1, "api", at); err == nil {
		t.Fatal("negative value accepted")
	}
}

func TestAccountReconciliationAttributesOwnActionGrowth(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ends := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC) // closes Pacific day 2026-11-14
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{
		{Day: "2026-11-13", StartsAt: ends.Add(-48 * time.Hour), EndsAt: ends.Add(-24 * time.Hour), Value: v(1000)},
		{Day: "2026-11-14", StartsAt: ends.Add(-24 * time.Hour), EndsAt: ends, Value: v(2000)},
		{Day: "2026-11-15", StartsAt: ends, EndsAt: ends.Add(24 * time.Hour)},
	}, ends.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	exec(t, s, `INSERT INTO actions(id,agent_id,kind,text,dry_run,posted_at,thread_url) VALUES(1,'a','post','x',0,'2026-11-13T09:00:00Z','u1'),(2,'a','reply','y',0,'2026-11-14T10:00:00Z','u2'),(3,'a','post','z',1,'2026-11-14T10:00:00Z','u3')`)
	for _, m := range []struct {
		action int
		at     string
		views  int
	}{
		{1, "2026-11-13T10:00:00Z", 30}, {1, "2026-11-14T09:00:00Z", 80}, {1, "2026-11-16T07:00:00Z", 500},
		{2, "2026-11-14T11:00:00Z", 40}, {2, "2026-11-14T20:00:00Z", 70},
		{3, "2026-11-14T11:00:00Z", 9999},
	} {
		exec(t, s, `INSERT INTO metrics(agent_id,action_id,measured_at,views) VALUES('a',?,?,?)`, m.action, m.at, m.views)
	}
	type row struct {
		day        string
		views      *int64
		attributed int64
		residual   *int64
	}
	rows, err := s.DB().Query(`SELECT day,account_views,attributed,residual FROM account_reconciliation WHERE agent_id='a' ORDER BY day`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []row
	for rows.Next() {
		var r row
		if err = rows.Scan(&r.day, &r.views, &r.attributed, &r.residual); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	// Day 13: action 1 grew 0 to 30. Day 14: action 1 30 to 80, action 2 0 to 70; the dry run counts for
	// nothing. Day 15 has no account value, so no residual; the later sample of action 1 lands there.
	if len(got) != 3 || got[0].attributed != 30 || *got[0].residual != 970 || got[1].attributed != 120 || *got[1].residual != 1880 || got[2].views != nil || got[2].residual != nil || got[2].attributed != 420 {
		t.Fatalf("reconciliation = %+v", got)
	}
}

func TestDashboardSnapshotReadsAccountDaysNewestFirst(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.EnsureAgent(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	ends := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	if err = s.UpsertAccountDays(ctx, "a", "views", []store.AccountDay{
		{Day: "2026-11-13", StartsAt: ends.Add(-48 * time.Hour), EndsAt: ends.Add(-24 * time.Hour), Value: v(7)},
		{Day: "2026-11-14", StartsAt: ends.Add(-24 * time.Hour), EndsAt: ends},
	}, ends); err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertAccountDays(ctx, "b", "views", []store.AccountDay{{Day: "2026-11-14", StartsAt: ends.Add(-24 * time.Hour), EndsAt: ends, Value: v(9)}}, ends); err != nil {
		t.Fatal(err)
	}
	snap, err := s.DashboardSnapshot(ctx, store.DashboardQuery{AgentID: "a", From: ends.Add(-72 * time.Hour), Now: ends})
	if err != nil {
		t.Fatal(err)
	}
	got := snap.AccountDays
	if len(got) != 2 || got[0].Day != "2026-11-14" || got[0].Views != nil || got[1].Day != "2026-11-13" || got[1].Views == nil || *got[1].Views != 7 {
		t.Fatalf("account days = %+v", got)
	}
}

func TestRecordFollowersKeepsSource(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	for _, source := range []string{"followers_api_fallback", "followers_browser"} {
		if err = s.RecordFollowers(ctx, "a", 7, source, at); err != nil {
			t.Fatal(err)
		}
		var got string
		if err = s.DB().QueryRow(`SELECT source FROM account_followers WHERE agent_id='a'`).Scan(&got); err != nil || got != source {
			t.Fatalf("source = %q, %v; want %q", got, err, source)
		}
	}
}
