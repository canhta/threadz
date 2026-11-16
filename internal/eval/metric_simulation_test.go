package eval_test

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/canhta/threadz/internal/eval"
	"github.com/canhta/threadz/internal/logging"
)

func TestMultiDaySimulationUsesActualCodeAndDatabaseRestart(t *testing.T) {
	var logs bytes.Buffer
	logger, _ := logging.New(&logs, "debug", "json")
	got, err := eval.RunMetricSimulation(context.Background(), filepath.Join(t.TempDir(), "simulation.db"), 72, logger)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Synthetic || got.TimingKind != "logical_time" || got.Restarts != 1 || got.ConfirmedActions != 7 || got.Samples != 32 || got.CompletedTargets != 63 || got.SourceReads != 31 || got.ExpectedSamples != 32 {
		t.Fatalf("%+v", got)
	}
	if measured, targets := strings.Count(logs.String(), `"event":"metrics.measured"`), strings.Count(logs.String(), `"event":"metrics.target.completed"`); measured != 32 || targets != 43 {
		t.Fatalf("measurement events=%d target events=%d", measured, targets)
	}
	for _, s := range []string{`"event":"metrics.measured"`, `"reason":"sample_committed"`, `"lateness_ms":92700000`, `"event":"guard.checked"`} {
		if !strings.Contains(logs.String(), s) {
			t.Errorf("missing evidence %s", s)
		}
	}
	for _, private := range []string{"https://fixture.invalid", "SYNTHETIC PRIVATE BODY"} {
		if strings.Contains(logs.String(), private) {
			t.Fatalf("log leak %s", private)
		}
	}
}
