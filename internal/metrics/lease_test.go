package metrics

import (
	"testing"

	"github.com/canhta/threadz/internal/tools"
)

func TestDefaultBatchReadWorstCaseStaysInsideTheLease(t *testing.T) {
	if worst := tools.MetricReadBudget(DefaultBatchSize) + leaseOverhead; worst >= DefaultLeaseDuration {
		t.Fatalf("a batch of %d takes up to %v, past the %v lease: the finished read would be discarded", DefaultBatchSize, worst, DefaultLeaseDuration)
	}
	if worst := tools.MetricReadBudget(DefaultBatchSize+1) + leaseOverhead; worst < DefaultLeaseDuration {
		t.Fatalf("a batch of %d still fits the lease (%v): raise DefaultBatchSize", DefaultBatchSize+1, worst)
	}
}
