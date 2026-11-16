package store

import (
	"context"
	"strings"
	"testing"
)

func TestAccountDaysQueryReadsMetricsOnceWithoutCorrelatedScans(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+accountDaysSQL, "2026-01-01", "a", snapshotAccountDays)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	metrics := 0
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
		if strings.HasPrefix(detail, "SEARCH m USING INDEX dashboard_metrics_agent (agent_id=? AND measured_at>?)") {
			metrics++
		}
	}
	t.Log("query plan:\n" + strings.Join(plan, "\n"))
	if metrics != 1 {
		t.Errorf("the plan searches metrics by the date bound %d times, want once", metrics)
	}
	for _, line := range plan {
		if strings.Contains(line, "CORRELATED") || strings.HasPrefix(line, "SCAN m") {
			t.Errorf("plan line %q: a correlated or full scan of metrics", line)
		}
	}
}
