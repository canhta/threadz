package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var windowCheck = regexp.MustCompile(`window_minutes INTEGER NOT NULL CHECK\(window_minutes IN \(([0-9,]+)\)\)`)

func checkList(t *testing.T, text string) []int {
	t.Helper()
	m := windowCheck.FindStringSubmatch(text)
	if m == nil {
		t.Fatalf("no window_minutes CHECK in:\n%s", text)
	}
	var out []int
	for _, f := range strings.Split(m[1], ",") {
		n, err := strconv.Atoi(f)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func openScheduleStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "schedule.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMetricJobsScheduleOffsetsMatchSchema(t *testing.T) {
	want := []int{15, 60, 180, 360, 720, 1440, 2880, 5760, 10080}
	if !slices.Equal(MetricWindowMinutes, want) {
		t.Fatalf("MetricWindowMinutes = %v, want %v", MetricWindowMinutes, want)
	}
	if got := checkList(t, schemaSQL); !slices.Equal(got, MetricWindowMinutes) {
		t.Fatalf("schema.sql CHECK = %v, MetricWindowMinutes = %v", got, MetricWindowMinutes)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "deploy", "migrate-15.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if got := checkList(t, string(migration)); !slices.Equal(got, MetricWindowMinutes) {
		t.Fatalf("migrate-15.sql CHECK = %v, MetricWindowMinutes = %v", got, MetricWindowMinutes)
	}
	for _, c := range CohortWindowMinutes {
		if !slices.Contains(MetricWindowMinutes, c) {
			t.Errorf("cohort window %d is not a schedule offset", c)
		}
	}
	if !slices.Contains(MetricWindowMinutes, settleAfterMinutes) {
		t.Errorf("settle window %d is not a schedule offset", settleAfterMinutes)
	}
	s := openScheduleStore(t)
	for _, m := range MetricWindowMinutes {
		if _, err := s.DB().Exec(`INSERT INTO metric_jobs(agent_id,action_id,window_minutes,due_at,not_before) VALUES('a',1,?,'t','t')`, m); err != nil {
			t.Errorf("offset %d rejected by the schema: %v", m, err)
		}
	}
	if _, err := s.DB().Exec(`INSERT INTO metric_jobs(agent_id,action_id,window_minutes,due_at,not_before) VALUES('a',1,30,'t','t')`); err == nil {
		t.Error("an offset outside the table was accepted")
	}
}

func TestMetricJobsScheduleMigrationKeepsOldSamples(t *testing.T) {
	migration, err := os.ReadFile(filepath.Join("..", "..", "deploy", "migrate-15.sql"))
	if err != nil {
		t.Fatal(err)
	}
	old := buildSchema(t, "schema-v13.sql", 13)
	defer old.Close()
	if _, err = old.Exec(`
		INSERT INTO agents(id,status) VALUES('a','running');
		INSERT INTO actions(id,agent_id,kind,text,posted_at,thread_url,media_id) VALUES(1,'a','post','hello','2026-11-01T00:00:00Z','https://www.threads.net/@a/post/1','m1');
		INSERT INTO metrics(agent_id,action_id,measured_at,views,likes,replies,reposts) VALUES('a',1,'2026-11-01T06:00:00.000000000Z',90,5,2,1);
		INSERT INTO metric_jobs(agent_id,action_id,window_hours,due_at,not_before,completed_at) VALUES('a',1,6,'2026-11-01T06:00:00Z','2026-11-01T06:00:00Z','2026-11-01T06:00:00.000000000Z');
		INSERT INTO metric_jobs(agent_id,action_id,window_hours,due_at,not_before,attempts,reason) VALUES('a',1,24,'2026-11-02T00:00:00Z','2026-11-02T01:00:00Z',3,'read_failed');`); err != nil {
		t.Fatalf("seeding v13: %v", err)
	}
	if _, err = old.Exec(string(migration)); err != nil {
		t.Fatalf("applying migrate-15.sql: %v", err)
	}
	rows, err := old.Query(`SELECT window_minutes,completed_at IS NOT NULL,attempts,reason FROM metric_jobs ORDER BY window_minutes`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var minutes, done, attempts int
		var reason string
		if err = rows.Scan(&minutes, &done, &attempts, &reason); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d/%d/%d/%s", minutes, done, attempts, reason))
	}
	if want := []string{"360/1/0/", "1440/0/3/read_failed"}; !slices.Equal(got, want) {
		t.Fatalf("migrated jobs = %v, want %v", got, want)
	}
	var views int64
	if err = old.QueryRow(`SELECT views FROM metrics WHERE action_id=1`).Scan(&views); err != nil || views != 90 {
		t.Fatalf("old sample = %d, %v; want 90", views, err)
	}
}

func TestMetricJobsScheduleSeedsEveryOffset(t *testing.T) {
	ctx := context.Background()
	s := openScheduleStore(t)
	posted := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.DB().Exec(`INSERT INTO agents(id,status) VALUES('a','running')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,posted_at,thread_url,media_id,dry_run) VALUES(1,'a','post','x',?,'https://www.threads.net/@a/post/1','m1',0)`, posted.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	n, err := s.SeedMetricJobs(ctx, "a", posted)
	if err != nil || n != len(MetricWindowMinutes) {
		t.Fatalf("seeded %d, %v; want %d", n, err, len(MetricWindowMinutes))
	}
	var due string
	if err = s.DB().QueryRow(`SELECT due_at FROM metric_jobs WHERE window_minutes=15`).Scan(&due); err != nil || due != "2026-11-01T00:15:00.000000000Z" {
		t.Fatalf("15 minute target due %q, %v", due, err)
	}
	if err = s.DB().QueryRow(`SELECT due_at FROM metric_jobs WHERE window_minutes=10080`).Scan(&due); err != nil || due != "2026-11-08T00:00:00.000000000Z" {
		t.Fatalf("168 hour target due %q, %v", due, err)
	}
}
