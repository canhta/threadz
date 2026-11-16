package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/agent"
	"github.com/canhta/threadz/internal/config"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
)

func TestSchedulerPollsMetricsBeforeAgentChosen24HourWake(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	tmpl := agent.OfflineTemplate()
	fixture := agent.NewOfflineFixture(clock)
	fixture.Calls = nil
	// Preexisting publication is synthetic setup, never a real post.
	if err = st.EnsureAgent(ctx, tmpl.ID); err != nil {
		t.Fatal(err)
	}
	if err = st.SetNextWake(ctx, tmpl.ID, start.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = st.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,dry_run,posted_at,thread_url,media_id) VALUES(1,?,'post','',0,?,'https://fixture.test/own','m')`, tmpl.ID, start.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	// Missing source is deferred, and must still be scheduled independently.
	r, err := agent.New(agent.Options{Store: st, Template: tmpl, Now: clock, HTTPClient: fixture.Client(), FetchTrends: fixture.Trends})
	if err != nil {
		t.Fatal(err)
	}
	waited := time.Duration(0)
	err = (agent.Scheduler{Agents: []*agent.Runtime{r}, Now: clock, Wait: func(ctx context.Context, d time.Duration) error { now = now.Add(d); waited += d; return ctx.Err() }}).Run(ctx, true)
	if err != nil || waited != 15*time.Minute {
		t.Fatalf("metrics missed first target: waited=%v err=%v", waited, err)
	}
	state, err := st.AgentState(ctx, tmpl.ID)
	if err != nil || !state.NextWake.Equal(start.Add(24*time.Hour)) {
		t.Fatalf("agent schedule changed %+v %v", state, err)
	}
	var maintenance, model int
	if err = st.DB().QueryRow(`SELECT COUNT(*) FROM wakes WHERE kind='metrics'`).Scan(&maintenance); err != nil {
		t.Fatal(err)
	}
	if err = st.DB().QueryRow(`SELECT COUNT(*) FROM wakes WHERE kind='agent'`).Scan(&model); err != nil || maintenance != 1 || model != 0 {
		t.Fatalf("maintenance=%d model=%d err=%v", maintenance, model, err)
	}
}

func TestRuntimeMetricsRefreshSamplesYoungActionsThroughTheProcessor(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	tmpl := agent.OfflineTemplate()
	if err = st.EnsureAgent(ctx, tmpl.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = st.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,dry_run,posted_at,thread_url,media_id) VALUES(1,?,'post','',0,?,'https://fixture.test/own','m1')`, tmpl.ID, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	source := threads.NewMemory()
	source.Actions["m1"] = threads.Metrics{Views: 7}
	fixture := agent.NewOfflineFixture(clock)
	r, err := agent.New(agent.Options{Store: st, Template: tmpl, Now: clock, HTTPClient: fixture.Client(), FetchTrends: fixture.Trends, ThreadsFactory: func(context.Context, config.Template) (threads.Backend, error) { return source, nil }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.RefreshMetrics(ctx, []int64{1})
	if err != nil || got.Refreshed != 1 || got.Skipped != 0 || source.Count(threads.OpReadActionMetrics) != 1 {
		t.Fatalf("refresh = %+v, %v, reads %d", got, err, source.Count(threads.OpReadActionMetrics))
	}
	if got, err = r.RefreshMetrics(ctx, nil); err != nil || got.Refreshed != 0 || got.Skipped != 1 {
		t.Fatalf("second refresh = %+v, %v", got, err)
	}
}
