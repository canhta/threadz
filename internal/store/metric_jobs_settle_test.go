package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/store"
)

func TestMetricJobsSettleEndsFlatActions(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	earlier := store.MetricValues{Views: 1000, Likes: 10, Replies: 2}
	cases := []struct {
		name     string
		previous *store.MetricValues
		latest   store.MetricValues
		claimAt  time.Duration
		wantOpen int
	}{
		{name: "flat after 24h settles", previous: &earlier, latest: store.MetricValues{Views: 1010, Likes: 10, Replies: 2}, claimAt: 24 * time.Hour, wantOpen: 0},
		{name: "identical counts settle", previous: &earlier, latest: earlier, claimAt: 24 * time.Hour, wantOpen: 0},
		{name: "zero views flat settles", previous: &store.MetricValues{}, latest: store.MetricValues{}, claimAt: 24 * time.Hour, wantOpen: 0},
		{name: "views still growing", previous: &earlier, latest: store.MetricValues{Views: 1100, Likes: 10, Replies: 2}, claimAt: 24 * time.Hour, wantOpen: 3},
		{name: "exactly two percent continues", previous: &store.MetricValues{Views: 98, Likes: 1}, latest: store.MetricValues{Views: 100, Likes: 1}, claimAt: 24 * time.Hour, wantOpen: 3},
		{name: "a like continues", previous: &earlier, latest: store.MetricValues{Views: 1000, Likes: 11, Replies: 2}, claimAt: 24 * time.Hour, wantOpen: 3},
		{name: "a reply continues", previous: &earlier, latest: store.MetricValues{Views: 1000, Likes: 10, Replies: 3}, claimAt: 24 * time.Hour, wantOpen: 3},
		{name: "no earlier sample continues", latest: earlier, claimAt: 24 * time.Hour, wantOpen: 3},
		{name: "flat before 24h continues", previous: &earlier, latest: earlier, claimAt: 12 * time.Hour, wantOpen: 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			s := metricCoreStore(t)
			metricCoreAction(t, s, "a", base.Format(time.RFC3339Nano), "https://example.test/own", false)
			if _, err := s.SeedMetricJobs(ctx, "a", base); err != nil {
				t.Fatal(err)
			}
			if c.previous != nil {
				earlier, err := s.ClaimMetricJobs(ctx, "a", base.Add(c.claimAt-time.Hour), 10*time.Minute, 10)
				if err != nil || len(earlier) == 0 {
					t.Fatalf("earlier claim = %+v, %v", earlier, err)
				}
				if ok, err := s.CompleteMetricJobs(ctx, earlier, base.Add(c.claimAt-time.Hour), *c.previous); err != nil || !ok {
					t.Fatalf("earlier complete = %v, %v", ok, err)
				}
			}
			jobs, err := s.ClaimMetricJobs(ctx, "a", base.Add(c.claimAt), 10*time.Minute, 10)
			if err != nil || len(jobs) == 0 {
				t.Fatalf("claim = %+v, %v", jobs, err)
			}
			if ok, err := s.CompleteMetricJobs(ctx, jobs, base.Add(c.claimAt+time.Minute), c.latest); err != nil || !ok {
				t.Fatalf("complete = %v, %v", ok, err)
			}
			if open := countRows(t, s, `SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NULL`); open != c.wantOpen {
				t.Errorf("open jobs = %d, want %d", open, c.wantOpen)
			}
			settled := countRows(t, s, `SELECT COUNT(*) FROM metric_jobs WHERE reason='settled' AND completed_at IS NOT NULL AND lease_until IS NULL`)
			measured := countRows(t, s, `SELECT COUNT(*) FROM metric_jobs WHERE reason='' AND completed_at IS NOT NULL`)
			if want := len(store.MetricWindowMinutes) - measured - c.wantOpen; settled != want {
				t.Errorf("settled jobs = %d, want %d", settled, want)
			}
		})
	}
}

func TestMetricJobsSettleComparesWithTheScheduledSampleNotARefresh(t *testing.T) {
	ctx := context.Background()
	s := metricCoreStore(t)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	id := metricCoreAction(t, s, "a", base.Format(time.RFC3339Nano), "https://example.test/own", false)
	if _, err := s.SeedMetricJobs(ctx, "a", base); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimMetricJobs(ctx, "a", base.Add(12*time.Hour), 10*time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteMetricJobs(ctx, first, base.Add(12*time.Hour), store.MetricValues{Views: 1000, Likes: 10}); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordMetricSample(ctx, "a", id, base.Add(23*time.Hour+59*time.Minute), store.MetricValues{Views: 9000, Likes: 10}); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimMetricJobs(ctx, "a", base.Add(24*time.Hour), 10*time.Minute, 10)
	if err != nil || len(next) != 1 {
		t.Fatalf("claim = %+v, %v", next, err)
	}
	if _, err = s.CompleteMetricJobs(ctx, next, base.Add(24*time.Hour), store.MetricValues{Views: 9000, Likes: 10}); err != nil {
		t.Fatal(err)
	}
	if open := countRows(t, s, `SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NULL`); open != 3 {
		t.Fatalf("open jobs = %d, want 3: a refresh sample at 23 h 59 m settled the action", open)
	}
}

func TestMetricJobsSettledJobsAreNotEpisodesAndCannotBeDeferred(t *testing.T) {
	ctx := context.Background()
	s := metricCoreStore(t)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	id := metricCoreAction(t, s, "a", base.Format(time.RFC3339Nano), "https://example.test/own", false)
	if _, err := s.SeedMetricJobs(ctx, "a", base); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordMetricSample(ctx, "a", id, base.Add(23*time.Hour), store.MetricValues{Views: 10}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.ClaimMetricJobs(ctx, "a", base.Add(24*time.Hour), 10*time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteMetricJobs(ctx, jobs, base.Add(24*time.Hour), store.MetricValues{Views: 10}); err != nil {
		t.Fatal(err)
	}
	episodes, err := s.Episodes(ctx, "a", store.EpisodeQuery{})
	if err != nil || len(episodes) != 1 || episodes[0].Window != 24 {
		t.Fatalf("episodes = %+v, %v; want one at 24h", episodes, err)
	}
	if err = s.DeferMetricJobs(ctx, jobs, base.Add(24*time.Hour), base.Add(25*time.Hour), "settled"); err == nil {
		t.Error("a job was deferred with reason settled")
	}
	if !store.MetricReasonCode("settled") {
		t.Error("settled is not a stored reason")
	}
}

func TestMetricJobsRecordSampleKeepsJobsAndRefusesUnknownActions(t *testing.T) {
	ctx := context.Background()
	s := metricCoreStore(t)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	id := metricCoreAction(t, s, "a", base.Format(time.RFC3339Nano), "https://example.test/own", false)
	dry := metricCoreAction(t, s, "a", base.Format(time.RFC3339Nano), "https://example.test/dry", true)
	if _, err := s.SeedMetricJobs(ctx, "a", base); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordMetricSample(ctx, "a", id, base.Add(time.Hour), store.MetricValues{Views: 5}); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, s, `SELECT COUNT(*) FROM metrics WHERE action_id=? AND views=5`, id); n != 1 {
		t.Errorf("samples = %d, want 1", n)
	}
	if n := countRows(t, s, `SELECT COUNT(*) FROM metric_jobs WHERE completed_at IS NOT NULL`); n != 0 {
		t.Errorf("a refresh sample completed %d jobs", n)
	}
	for name, call := range map[string]func() error{
		"dry run":       func() error { return s.RecordMetricSample(ctx, "a", dry, base, store.MetricValues{}) },
		"foreign agent": func() error { return s.RecordMetricSample(ctx, "b", id, base, store.MetricValues{}) },
		"no agent":      func() error { return s.RecordMetricSample(ctx, "", id, base, store.MetricValues{}) },
		"zero time":     func() error { return s.RecordMetricSample(ctx, "a", id, time.Time{}, store.MetricValues{}) },
	} {
		if err := call(); err == nil {
			t.Errorf("%s: sample accepted", name)
		}
	}
}
