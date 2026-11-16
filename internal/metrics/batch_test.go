package metrics_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/config"
	"github.com/canhta/threadz/internal/metrics"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
)

type batchRig struct {
	t      *testing.T
	st     *store.Store
	source *threads.Memory
	start  time.Time
	now    time.Time
	p      *metrics.Processor
}

// newBatchRig has `actions` own posts, ids 1..n and media ids m1..mn, all
// published at start, each reading 1000 views.
func newBatchRig(t *testing.T, actions int) *batchRig {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r := &batchRig{t: t, st: st, source: threads.NewMemory(), start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	r.now = r.start
	tmpl := config.Template{ID: "own", Limits: config.LimitsConfig{MinMinutesBetweenWakes: 15}}
	if err = st.EnsureAgent(ctx, tmpl.ID); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= actions; i++ {
		if _, err = st.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,dry_run,posted_at,thread_url,media_id) VALUES(?,'own','post','',0,?,?,?)`, i, r.start.Format(time.RFC3339Nano), fmt.Sprintf("https://fixture.test/own/%d", i), fmt.Sprintf("m%d", i)); err != nil {
			t.Fatal(err)
		}
		r.source.Actions[fmt.Sprintf("m%d", i)] = threads.Metrics{Views: 1000, Likes: 10, Replies: 2}
	}
	r.p, err = metrics.New(metrics.Options{Store: st, Template: tmpl, Now: func() time.Time { return r.now }, FetchTrends: metricTrends, ThreadsFactory: func(context.Context, config.Template) (threads.Backend, error) { return r.source, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *batchRig) at(d time.Duration) { r.now = r.start.Add(d) }

func (r *batchRig) runDue() metrics.Result {
	r.t.Helper()
	got, err := r.p.RunDue(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return got
}

func (r *batchRig) count(query string, args ...any) int {
	r.t.Helper()
	var n int
	if err := r.st.DB().QueryRow(query, args...).Scan(&n); err != nil {
		r.t.Fatal(err)
	}
	return n
}

func TestBatchReadIsOneSourceCallForAllDueActions(t *testing.T) {
	r := newBatchRig(t, 3)
	r.at(15 * time.Minute)
	got := r.runDue()
	if got.ClaimedJobs != 3 || got.Samples != 3 || got.DeferredJobs != 0 {
		t.Fatalf("result = %+v", got)
	}
	if n := r.source.Count(threads.OpReadActionMetrics); n != 1 {
		t.Fatalf("source reads = %d, want one batched read", n)
	}
	if ids := r.source.Last(threads.OpReadActionMetrics).Arg; ids != "m1,m2,m3" {
		t.Fatalf("batched ids = %q", ids)
	}
	if n := r.count(`SELECT COUNT(*) FROM tool_calls WHERE tool='read_own_metrics'`); n != 1 {
		t.Fatalf("guard audit rows = %d, want the one batched call", n)
	}
	if again := r.runDue(); again.ClaimedJobs != 0 || r.source.Count(threads.OpReadActionMetrics) != 1 {
		t.Fatalf("completed targets were read again: %+v", again)
	}
}

func TestBatchReadOneFailedActionDoesNotFailTheOthers(t *testing.T) {
	r := newBatchRig(t, 4)
	r.source.ActionErrors = map[string]error{"m2": threads.ErrMediaNotFound}
	r.source.Pages[threads.OpReadOwnPosts] = threads.Page{Kind: "own_posts", Posts: []threads.Post{{ID: "m2"}}}
	delete(r.source.Actions, "m3")
	r.source.ActionErrors["m4"] = errors.New("private source detail")
	r.at(15 * time.Minute)
	got := r.runDue()
	if got.ClaimedJobs != 4 || got.Samples != 1 || got.DeferredJobs != 3 {
		t.Fatalf("result = %+v, want one sample and three deferrals", got)
	}
	if n := r.source.Count(threads.OpReadActionMetrics); n != 1 {
		t.Fatalf("source reads = %d, want one", n)
	}
	for id, want := range map[int]string{2: "read_failed", 3: "metrics_unavailable", 4: "read_failed"} {
		var reason string
		if err := r.st.DB().QueryRow(`SELECT reason FROM metric_jobs WHERE action_id=? AND window_minutes=15`, id).Scan(&reason); err != nil || reason != want {
			t.Errorf("action %d reason = %q, %v; want %q", id, reason, err, want)
		}
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id=1 AND views=1000`); n != 1 {
		t.Fatalf("the healthy action has %d samples", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id<>1`); n != 0 {
		t.Fatalf("failed actions were sampled: %d", n)
	}
}

func TestBatchReadDeletedActionEndsWhileTheOthersAreMeasured(t *testing.T) {
	r := newBatchRig(t, 2)
	r.source.ActionErrors = map[string]error{"m2": threads.ErrMediaNotFound}
	r.at(15 * time.Minute)
	got := r.runDue()
	if got.Samples != 1 || got.DeferredJobs != 0 {
		t.Fatalf("result = %+v", got)
	}
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE action_id=2 AND reason='media_gone' AND completed_at IS NOT NULL`); n != len(store.MetricWindowMinutes) {
		t.Fatalf("ended jobs of the deleted post = %d", n)
	}
}

func TestBatchReadRefreshCooldownOnTheInjectedClock(t *testing.T) {
	ctx := context.Background()
	r := newBatchRig(t, 2)
	r.at(10 * time.Minute)
	refresh := func(ids ...int64) metrics.Result {
		t.Helper()
		got, err := r.p.Refresh(ctx, ids)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := refresh(1); got.Refreshed != 1 || got.ClaimedJobs != 0 {
		t.Fatalf("first refresh = %+v", got)
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id=1`); n != 1 {
		t.Fatalf("samples = %d, want 1", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NOT NULL`); n != 0 {
		t.Fatalf("a refresh completed %d jobs", n)
	}

	r.now = r.now.Add(metrics.RefreshCooldown - time.Second)
	if got := refresh(1); got.Refreshed != 0 || r.source.Count(threads.OpReadActionMetrics) != 1 {
		t.Fatalf("refresh inside the cooldown = %+v, reads %d", got, r.source.Count(threads.OpReadActionMetrics))
	}
	if got := refresh(1, 2); got.Refreshed != 1 || r.source.Last(threads.OpReadActionMetrics).Arg != "m2" {
		t.Fatalf("another action is refreshed on its own: %+v, read %q", got, r.source.Last(threads.OpReadActionMetrics).Arg)
	}

	r.now = r.now.Add(time.Second)
	if got := refresh(1); got.Refreshed != 1 || r.source.Count(threads.OpReadActionMetrics) != 3 {
		t.Fatalf("refresh after the cooldown = %+v, reads %d", got, r.source.Count(threads.OpReadActionMetrics))
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id=1`); n != 2 {
		t.Fatalf("samples of action 1 = %d, want 2", n)
	}
}

func TestBatchReadRefreshIgnoresOldAndUnknownActions(t *testing.T) {
	r := newBatchRig(t, 1)
	r.at(24*time.Hour + time.Minute)
	got, err := r.p.Refresh(context.Background(), []int64{1, 99})
	if err != nil || got.Refreshed != 0 || r.source.Count(threads.OpReadActionMetrics) != 0 {
		t.Fatalf("refresh = %+v, %v, reads %d", got, err, r.source.Count(threads.OpReadActionMetrics))
	}
}

func TestBatchReadRefreshRidesTheDueBatch(t *testing.T) {
	r := newBatchRig(t, 2)
	r.at(15 * time.Minute)
	got, err := r.p.Refresh(context.Background(), []int64{1})
	if err != nil || got.Refreshed != 0 || got.Samples != 2 || r.source.Count(threads.OpReadActionMetrics) != 1 {
		t.Fatalf("refresh with due jobs = %+v, %v, reads %d", got, err, r.source.Count(threads.OpReadActionMetrics))
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id=1`); n != 1 {
		t.Fatalf("an action both due and refreshed has %d samples, want 1", n)
	}
}

func TestPlateauStopSettledActionStops(t *testing.T) {
	r := newBatchRig(t, 1)
	r.at(15 * time.Minute)
	r.runDue()
	r.source.Actions["m1"] = threads.Metrics{Views: 1010, Likes: 10, Replies: 2}
	r.at(25 * time.Hour)
	got := r.runDue()
	if got.ClaimedJobs != 5 || got.Samples != 1 {
		t.Fatalf("catch-up = %+v", got)
	}
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE reason='settled' AND completed_at IS NOT NULL`); n != 3 {
		t.Fatalf("settled jobs = %d, want the three after 24h", n)
	}
	if _, ok, err := r.p.NextDue(context.Background()); err != nil || ok {
		t.Fatalf("a settled action is still due: %v %v", ok, err)
	}
	reads := r.source.Count(threads.OpReadActionMetrics)
	r.at(30 * 24 * time.Hour)
	if again := r.runDue(); again.ClaimedJobs != 0 || r.source.Count(threads.OpReadActionMetrics) != reads {
		t.Fatalf("a settled action was read again: %+v", again)
	}
}

func TestPlateauStopGrowingActionContinues(t *testing.T) {
	r := newBatchRig(t, 1)
	r.at(15 * time.Minute)
	r.runDue()
	views := int64(1000)
	for _, d := range []time.Duration{25 * time.Hour, 48 * time.Hour, 96 * time.Hour, 168 * time.Hour} {
		views *= 2
		r.source.Actions["m1"] = threads.Metrics{Views: views, Likes: 10, Replies: 2}
		r.at(d)
		if got := r.runDue(); got.Samples != 1 {
			t.Fatalf("at %v: %+v", d, got)
		}
	}
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE reason='settled'`); n != 0 {
		t.Fatalf("a growing action settled %d jobs", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NOT NULL AND reason=''`); n != len(store.MetricWindowMinutes) {
		t.Fatalf("measured jobs = %d, want every offset", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM metrics WHERE action_id=1`); n != 5 {
		t.Fatalf("samples = %d, want 5", n)
	}
}

func TestPlateauStopLikeOrReplyKeepsAnActionGoing(t *testing.T) {
	r := newBatchRig(t, 1)
	r.at(15 * time.Minute)
	r.runDue()
	r.source.Actions["m1"] = threads.Metrics{Views: 1000, Likes: 11, Replies: 2}
	r.at(25 * time.Hour)
	r.runDue()
	if n := r.count(`SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NULL`); n != 3 {
		t.Fatalf("open jobs = %d, want the three after 24h", n)
	}
}
