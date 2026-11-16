package metrics_test

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/config"
	"github.com/canhta/threadz/internal/metrics"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
)

type accountPoll struct {
	t      *testing.T
	st     *store.Store
	source *threads.Memory
	now    time.Time
	p      *metrics.Processor
}

// accountSetup is an api-backend agent on a clock set to 2026-11-16 04:18 UTC,
// which is Pacific 11-15 20:18: the closed days end at 2026-11-15 08:00 UTC.
func accountSetup(t *testing.T) *accountPoll {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "account.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	tmpl := config.Template{ID: "own", DryRun: true, Limits: config.LimitsConfig{MinMinutesBetweenWakes: 15}, Threads: config.ThreadsConfig{Backend: "api"}}
	if err = st.EnsureAgent(ctx, tmpl.ID); err != nil {
		t.Fatal(err)
	}
	a := &accountPoll{t: t, st: st, source: threads.NewMemory(), now: time.Date(2026, 11, 16, 4, 18, 0, 0, time.UTC)}
	a.source.Account = threads.AccountInsights{
		Views: []threads.DayViews{
			{EndsAt: time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC), Value: 35285},
			{EndsAt: time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC), Value: 36925},
		},
		Clicks: []threads.LinkClicks{{URL: "https://threadz.canhta.com/", Value: 4}, {URL: "https://example.com/", Value: 1}},
	}
	a.p, err = metrics.New(metrics.Options{Store: st, Template: tmpl, Now: func() time.Time { return a.now }, FetchTrends: metricTrends,
		ThreadsFactory: func(context.Context, config.Template) (threads.Backend, error) { return a.source, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (a *accountPoll) run() metrics.Result {
	a.t.Helper()
	got, err := a.p.RunDue(context.Background())
	if err != nil {
		a.t.Fatal(err)
	}
	return got
}

func (a *accountPoll) due() time.Time {
	a.t.Helper()
	at, ok, err := a.p.NextDue(context.Background())
	if err != nil || !ok {
		a.t.Fatalf("NextDue = %v %v %v", at, ok, err)
	}
	return at
}

func (a *accountPoll) count(query string) int {
	a.t.Helper()
	var n int
	if err := a.st.DB().QueryRow(query).Scan(&n); err != nil {
		a.t.Fatal(err)
	}
	return n
}

func TestAccountPollBackfillsThenReadsTrailingDaysHourly(t *testing.T) {
	a := accountSetup(t)
	start := a.now
	if at := a.due(); !at.Equal(start) {
		t.Fatalf("first read due %s, want now", at)
	}
	if got := a.run(); got.AccountReads != 1 || got.ClaimedJobs != 0 {
		t.Fatalf("first run = %+v", got)
	}
	// The first read is the backfill: one call covers 700 days, a day the API
	// gave no bucket for is NULL, and the whole call is one wake with the
	// guarded read in its audit.
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE metric='views'`); n != threads.AccountBackfillDays {
		t.Fatalf("backfilled days = %d", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE value IS NOT NULL`); n != 2 {
		t.Fatalf("days with a value = %d, want 2", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE day='2026-11-14' AND value=36925`); n != 1 {
		t.Fatal("the bucket ending 2026-11-15 08:00 UTC is Pacific day 2026-11-14")
	}
	if n := a.count(`SELECT COUNT(*) FROM tool_calls WHERE tool='read_account_insights'`); n != 1 {
		t.Fatalf("audited reads = %d", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_samples WHERE julianday(until)-julianday(since)=700`); n != 1 {
		t.Fatalf("samples of the 700 day window = %d", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_samples`); n != 1 {
		t.Fatalf("samples = %d, want clicks (5 over both links)", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_samples WHERE metric='clicks' AND value=5 AND source='api'`); n != 1 {
		t.Fatal("clicks sample is not the sum of the links")
	}

	// Not due before an hour has passed; due exactly then.
	if at := a.due(); !at.Equal(start.Add(time.Hour)) {
		t.Fatalf("next read due %s, want one hour after the last", at)
	}
	a.now = start.Add(time.Hour - time.Second)
	if got := a.run(); got.AccountReads != 0 || a.count(`SELECT COUNT(*) FROM wakes WHERE kind='metrics'`) != 1 {
		t.Fatalf("early run = %+v", got)
	}
	a.now = start.Add(time.Hour)
	if got := a.run(); got.AccountReads != 1 {
		t.Fatalf("hourly run = %+v", got)
	}
	// Later reads cover the trailing 3 days only and the unchanged totals of
	// an identical window are not stored again.
	if n := a.count(`SELECT COUNT(*) FROM account_samples WHERE julianday(until)-julianday(since)=3`); n != 1 {
		t.Fatalf("samples of the trailing 3 day window = %d", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_samples`); n != 2 {
		t.Fatalf("samples after the trailing read = %d, want 1 per window", n)
	}
	a.now = start.Add(2 * time.Hour)
	a.run()
	if n := a.count(`SELECT COUNT(*) FROM account_samples`); n != 2 {
		t.Fatalf("an unchanged window was stored again: %d samples", n)
	}
	a.source.Account.Clicks[0].Value = 5
	a.now = start.Add(3 * time.Hour)
	a.run()
	if n := a.count(`SELECT COUNT(*) FROM account_samples WHERE metric='clicks' AND value=6`); n != 1 {
		t.Fatal("a changed total was not stored")
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily`); n != threads.AccountBackfillDays {
		t.Fatalf("trailing reads changed the number of days: %d", n)
	}
}

func TestAccountPollReadsJustAfterPacificMidnight(t *testing.T) {
	a := accountSetup(t)
	a.run()
	// Last read at 07:58 UTC; Pacific midnight is 08:00 UTC in November, so the
	// closing read is due at 08:05, before the hourly one at 08:58.
	a.now = time.Date(2026, 11, 16, 7, 58, 0, 0, time.UTC)
	if got := a.run(); got.AccountReads != 1 {
		t.Fatalf("hourly run = %+v", got)
	}
	if at := a.due(); !at.Equal(time.Date(2026, 11, 16, 8, 5, 0, 0, time.UTC)) {
		t.Fatalf("closing read due %s, want 08:05", at)
	}
	a.now = time.Date(2026, 11, 16, 8, 5, 0, 0, time.UTC)
	if got := a.run(); got.AccountReads != 1 {
		t.Fatalf("closing run = %+v", got)
	}
	if at := a.due(); !at.Equal(a.now.Add(time.Hour)) {
		t.Fatalf("after the closing read due %s, want an hour later", at)
	}
}

func TestAccountPollBacksOffAfterFailuresAndUsageWaits(t *testing.T) {
	a := accountSetup(t)
	a.source.Errors[threads.OpReadAccountInsights] = errors.New("synthetic failure")
	// Each failure doubles the 5 minute retry delay; the wake keeps no sample.
	for i, wait := range []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute} {
		if got := a.run(); got.AccountReads != 0 {
			t.Fatalf("failing run %d = %+v", i, got)
		}
		if at := a.due(); !at.Equal(a.now.Add(wait)) {
			t.Fatalf("after failure %d due %s, want %s later", i+1, at, wait)
		}
		if got := a.run(); got.AccountReads != 0 || a.source.Count(threads.OpReadAccountInsights) != i+1 {
			t.Fatalf("a run during backoff %d read again: %+v", i+1, got)
		}
		a.now = a.now.Add(wait)
	}
	// The wait the API asks for is kept when it is longer than the backoff.
	a.source.Errors[threads.OpReadAccountInsights] = &threads.RateLimitedError{Wait: 50 * time.Minute, Err: threads.ErrAPI}
	a.run()
	if at := a.due(); !at.Equal(a.now.Add(50 * time.Minute)) {
		t.Fatalf("after a rate limit due %s, want 50 minutes later", at)
	}
	a.now = a.now.Add(50 * time.Minute)
	delete(a.source.Errors, threads.OpReadAccountInsights)
	a.source.Account.RegainAfter = 90 * time.Minute
	if got := a.run(); got.AccountReads != 1 {
		t.Fatalf("run after the wait = %+v", got)
	}
	// A success under a usage wait waits for it, and resets the doubling.
	if at := a.due(); !at.Equal(a.now.Add(90 * time.Minute)) {
		t.Fatalf("after a read that asked for 90 minutes due %s", at)
	}
	a.now = a.now.Add(90 * time.Minute)
	a.source.Account.RegainAfter = 0
	a.run()
	a.source.Errors[threads.OpReadAccountInsights] = errors.New("synthetic failure")
	a.now = a.now.Add(time.Hour)
	a.run()
	if at := a.due(); !at.Equal(a.now.Add(5 * time.Minute)) {
		t.Fatalf("first failure after a success due %s, want 5 minutes later", at)
	}
}

func TestAccountPollSourceUnavailableBacksOff(t *testing.T) {
	a := accountSetup(t)
	tmpl := config.Template{ID: "own", DryRun: true, Limits: config.LimitsConfig{MinMinutesBetweenWakes: 15}, Threads: config.ThreadsConfig{Backend: "api"}}
	p, err := metrics.New(metrics.Options{Store: a.st, Template: tmpl, Now: func() time.Time { return a.now }, FetchTrends: metricTrends,
		ThreadsFactory: func(context.Context, config.Template) (threads.Backend, error) {
			return nil, errors.New("synthetic open failure")
		}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.RunDue(context.Background())
	if err != nil || got.AccountReads != 0 {
		t.Fatalf("run = %+v, %v", got, err)
	}
	if at, ok, err := p.NextDue(context.Background()); err != nil || !ok || !at.Equal(a.now.Add(5*time.Minute)) {
		t.Fatalf("next due %s %v %v, want 5 minutes later", at, ok, err)
	}
}

func TestAccountPollIsOffWithoutTheAPIBackend(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "stub.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tmpl := config.Template{ID: "own", DryRun: true, Limits: config.LimitsConfig{MinMinutesBetweenWakes: 15}}
	if err = st.EnsureAgent(ctx, tmpl.ID); err != nil {
		t.Fatal(err)
	}
	source := threads.NewMemory()
	p, err := metrics.New(metrics.Options{Store: st, Template: tmpl, Now: time.Now, FetchTrends: metricTrends,
		ThreadsFactory: func(context.Context, config.Template) (threads.Backend, error) { return source, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := p.NextDue(ctx); err != nil || ok {
		t.Fatalf("NextDue ok=%v err=%v, want nothing due", ok, err)
	}
	if source.Count(threads.OpReadAccountInsights) != 0 {
		t.Fatal("account insights read on a stub backend")
	}
}

func TestAccountPollStoreFailureBacksOffAndIsReported(t *testing.T) {
	a := accountSetup(t)
	if _, err := a.st.DB().Exec(`DROP TABLE account_daily`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.p.RunDue(context.Background()); err == nil {
		t.Fatal("a failed store write was not reported")
	}
}

func TestAccountPollWithoutDueActionsReadsNoActionMetrics(t *testing.T) {
	a := accountSetup(t)
	if got := a.run(); got.AccountReads != 1 || got.ClaimedJobs != 0 {
		t.Fatalf("run = %+v", got)
	}
	if n, f := a.source.Count(threads.OpReadActionMetrics), a.source.Count(threads.OpReadOwnMetrics); n != 0 || f != 0 {
		t.Fatalf("a run with no action due made %d action reads and %d follower reads", n, f)
	}
}

// totalsSeries is views for the newest n closed Pacific days and one-day
// totals for each of them except the day gap newest days back (1 is the newest).
func (a *accountPoll) totalsSeries(n, gap int) []threads.PacificDay {
	a.t.Helper()
	since, until := threads.AccountWindow(a.now, n)
	days := threads.PacificDays(since, until)
	a.source.Account.Views = nil
	a.source.AccountTotals = map[string]map[string]int64{}
	for i, d := range days {
		a.source.Account.Views = append(a.source.Account.Views, threads.DayViews{EndsAt: d.EndsAt, Value: 1000 + int64(i)})
		if len(days)-i != gap {
			a.source.AccountTotals[d.Day] = map[string]int64{"likes": int64(i), "replies": 1, "reposts": 0, "quotes": 0}
		}
	}
	return days
}

// dayReads records the Pacific date of every one-day totals read.
func (a *accountPoll) dayReads() *[]string {
	var days []string
	a.source.Hook = func(_ context.Context, c threads.Call) error {
		if c.Op == threads.OpReadAccountTotals {
			days = append(days, c.Arg)
		}
		return nil
	}
	return &days
}

func TestAccountTotalsPollReadsTenDaysAPassAndBackfillsSixtyDays(t *testing.T) {
	a := accountSetup(t)
	// Pacific noon, so the hourly passes of the backfill stay inside one day.
	a.now = time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	days := a.totalsSeries(70, 0)
	newest := days[len(days)-1]
	reads := a.dayReads()

	a.run()
	if len(*reads) != 10 {
		t.Fatalf("first pass made %d day reads, want the cap of 10", len(*reads))
	}
	// The 3 trailing days are read again by every pass; the rest of the cap
	// takes the newest days still without totals, newest first.
	if got, want := (*reads)[3], days[len(days)-4].Day; got != want {
		t.Fatalf("fourth read = %s, want %s", got, want)
	}
	for pass := 2; pass <= 20; pass++ {
		*reads = nil
		a.now = a.now.Add(time.Hour)
		a.run()
		if len(*reads) > 10 {
			t.Fatalf("pass %d made %d day reads", pass, len(*reads))
		}
		if len(*reads) < 10 {
			break
		}
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE metric='likes'`); n != 60 {
		t.Fatalf("days with likes = %d, want the newest 60", n)
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE day='` + days[9].Day + `' AND metric!='views'`); n != 0 {
		t.Fatal("a day beyond the newest 60 was read")
	}

	// Once backfilled, a pass re-reads the 3 trailing days and nothing else.
	*reads = nil
	a.source.AccountTotals[newest.Day]["likes"] = 99
	a.now = a.now.Add(time.Hour)
	a.run()
	if len(*reads) != 3 {
		t.Fatalf("settled pass made %d day reads, want the 3 trailing", len(*reads))
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE day='` + newest.Day + `' AND metric='likes' AND value=99`); n != 1 {
		t.Fatal("a trailing day was not replaced")
	}
}

func TestAccountTotalsPassRunsBeforeTheActionJobsAreClaimed(t *testing.T) {
	a := accountSetup(t)
	a.totalsSeries(3, 0)
	posted := a.now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := a.st.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,dry_run,posted_at,thread_url,media_id) VALUES(1,'own','post','',0,?,'https://fixture.test/own/1','m1')`, posted); err != nil {
		t.Fatal(err)
	}
	a.source.Actions["m1"] = threads.Metrics{Views: 10, Likes: 1}
	claimed := func() int { return a.count(`SELECT COUNT(*) FROM metric_jobs WHERE claimed_at IS NOT NULL`) }
	var claimedAtDayRead, claimedAtInsights, claimedAtActionRead []int
	a.source.Hook = func(_ context.Context, c threads.Call) error {
		switch c.Op {
		case threads.OpReadAccountInsights:
			claimedAtInsights = append(claimedAtInsights, claimed())
		case threads.OpReadAccountTotals:
			claimedAtDayRead = append(claimedAtDayRead, claimed())
		case threads.OpReadActionMetrics:
			claimedAtActionRead = append(claimedAtActionRead, claimed())
		}
		return nil
	}
	got := a.run()
	if got.ClaimedJobs == 0 || got.AccountReads != 1 || got.Samples == 0 {
		t.Fatalf("run = %+v", got)
	}
	if len(claimedAtInsights) != 1 || len(claimedAtDayRead) != 3 || len(claimedAtActionRead) != 1 {
		t.Fatalf("reads: insights %v, days %v, actions %v", claimedAtInsights, claimedAtDayRead, claimedAtActionRead)
	}
	for _, n := range append(claimedAtInsights, claimedAtDayRead...) {
		if n != 0 {
			t.Fatalf("an account read ran with %d jobs already claimed", n)
		}
	}
	if claimedAtActionRead[0] == 0 {
		t.Fatal("the action read ran before its jobs were claimed")
	}
}

func TestAccountTotalsPassReadsTheDaysItListedAcrossPacificMidnight(t *testing.T) {
	a := accountSetup(t)
	days := a.totalsSeries(3, 0)
	var reads []string
	a.source.Hook = func(_ context.Context, c threads.Call) error {
		if c.Op == threads.OpReadAccountTotals {
			reads = append(reads, c.Arg)
			a.now = time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
		}
		return nil
	}
	a.run()
	want := []string{days[2].Day, days[1].Day, days[0].Day}
	if !slices.Equal(reads, want) {
		t.Fatalf("day reads = %v, want %v", reads, want)
	}
}

func TestAccountTotalsRefusedDayIsTriedThreeTimesThenWaitsADay(t *testing.T) {
	a := accountSetup(t)
	days := a.totalsSeries(5, 5)
	gap := days[0].Day
	gapReads := func() int {
		n := 0
		for _, c := range a.source.Calls() {
			if c.Op == threads.OpReadAccountTotals && c.Arg == gap {
				n++
			}
		}
		return n
	}
	for pass := 1; pass <= 3; pass++ {
		if got := a.run(); got.AccountReads != 1 {
			t.Fatalf("pass %d = %+v: a refused day is not a failure", pass, got)
		}
		if gapReads() != pass {
			t.Fatalf("after pass %d the refused day was read %d times", pass, gapReads())
		}
		a.now = a.now.Add(time.Hour)
	}
	refusedAt := a.now.Add(-time.Hour)
	a.run()
	if gapReads() != 3 {
		t.Fatalf("a day refused 3 times was read again after an hour: %d reads", gapReads())
	}
	a.now = refusedAt.Add(23 * time.Hour)
	a.run()
	if gapReads() != 3 {
		t.Fatalf("a day refused 3 times was read again before 24 hours: %d reads", gapReads())
	}
	a.now = refusedAt.Add(24 * time.Hour)
	a.source.AccountTotals[gap] = map[string]int64{"likes": 7, "replies": 0, "reposts": 0, "quotes": 0}
	a.run()
	if gapReads() != 4 {
		t.Fatalf("the refused day was not retried after 24 hours: %d reads", gapReads())
	}
	if n := a.count(`SELECT COUNT(*) FROM account_daily WHERE day='` + gap + `' AND metric='likes' AND value=7`); n != 1 {
		t.Fatal("the retried day was not stored")
	}
	a.now = a.now.Add(time.Hour)
	a.run()
	if gapReads() != 4 {
		t.Fatalf("a stored day was read again: %d reads", gapReads())
	}
}

func TestAccountTotalsUsageWaitOfAnyReadEndsThePass(t *testing.T) {
	// The views read asks for a wait: no day is read, and the wait is kept.
	a := accountSetup(t)
	a.totalsSeries(5, 0)
	a.source.Account.RegainAfter = 90 * time.Minute
	reads := a.dayReads()
	if got := a.run(); got.AccountReads != 1 || len(*reads) != 0 {
		t.Fatalf("under a views usage wait: %+v, %d day reads", got, len(*reads))
	}
	if at := a.due(); !at.Equal(a.now.Add(90 * time.Minute)) {
		t.Fatalf("next read due %s, want the usage wait", at)
	}

	// A day read asks for a wait: that read is the last of the pass.
	a = accountSetup(t)
	a.totalsSeries(5, 0)
	reads = a.dayReads()
	hook := a.source.Hook
	a.source.Hook = func(ctx context.Context, c threads.Call) error {
		if c.Op == threads.OpReadAccountTotals {
			a.source.Account.RegainAfter = 90 * time.Minute
		}
		return hook(ctx, c)
	}
	if got := a.run(); got.AccountReads != 1 || len(*reads) != 1 {
		t.Fatalf("under a day usage wait: %+v, %d day reads", got, len(*reads))
	}
	if at := a.due(); !at.Equal(a.now.Add(90 * time.Minute)) {
		t.Fatalf("next read due %s, want the usage wait", at)
	}

	// A refused day read asks for a wait: the pass ends there too.
	a = accountSetup(t)
	days := a.totalsSeries(5, 1)
	reads = a.dayReads()
	hook = a.source.Hook
	a.source.Hook = func(ctx context.Context, c threads.Call) error {
		if c.Op == threads.OpReadAccountTotals {
			a.source.Account.RegainAfter = 90 * time.Minute
		}
		return hook(ctx, c)
	}
	if got := a.run(); got.AccountReads != 1 || len(*reads) != 1 || (*reads)[0] != days[4].Day {
		t.Fatalf("under a usage wait of a refused day: %+v, reads %v", got, *reads)
	}
	if at := a.due(); !at.Equal(a.now.Add(90 * time.Minute)) {
		t.Fatalf("next read due %s, want the usage wait", at)
	}
}

func TestAccountTotalsFailedDayReadBacksTheNextReadOff(t *testing.T) {
	a := accountSetup(t)
	a.totalsSeries(5, 0)
	a.source.Errors[threads.OpReadAccountTotals] = errors.New("synthetic failure")
	before := a.source.Count(threads.OpReadAccountTotals)
	if got := a.run(); got.AccountReads != 0 || a.source.Count(threads.OpReadAccountTotals)-before != 1 {
		t.Fatalf("failing day read: %+v", got)
	}
	if at := a.due(); !at.Equal(a.now.Add(5 * time.Minute)) {
		t.Fatalf("after a failed day read due %s, want 5 minutes later", at)
	}
}
