package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/canhta/threadz/internal/guard"
	"github.com/canhta/threadz/internal/llm"
	"github.com/canhta/threadz/internal/logging"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
	"github.com/canhta/threadz/internal/tools"
)

// accountDue is when the account insights are next read and whether they are
// read at all (only the API backend serves them). The first read is due at
// once; later ones an hour after the previous or just after the next Pacific
// midnight, whichever is first, and never before a backoff ends.
func (p *Processor) accountDue(now time.Time) (time.Time, bool) {
	if p.opts.Template.Threads.Backend != "api" {
		return time.Time{}, false
	}
	p.accountMu.Lock()
	defer p.accountMu.Unlock()
	due := now
	if !p.accountLast.IsZero() {
		due = p.accountLast.Add(AccountPollEvery)
		if closed := threads.NextPacificMidnight(p.accountLast).Add(accountCloseDelay); closed.Before(due) {
			due = closed
		}
	}
	if p.accountBackoff.After(due) {
		due = p.accountBackoff
	}
	return due, true
}

// accountFailed backs the next read off: RetryDelay doubled for each earlier
// failure up to the poll period, or the wait the API asked for when longer.
func (p *Processor) accountFailed(now time.Time, err error) {
	p.accountMu.Lock()
	defer p.accountMu.Unlock()
	wait := min(p.opts.RetryDelay<<min(p.accountFailures, accountBackoffSteps), AccountPollEvery)
	p.accountFailures++
	var limited *threads.RateLimitedError
	if errors.As(err, &limited) {
		wait = max(wait, limited.Wait)
	}
	p.accountBackoff = now.Add(wait)
}

// readAccount reads the account insights through the guard and stores them:
// the daily views, and each window total unless it equals the previous reading.
// A failed read only backs the next one off; cancellation and audit failures
// are returned.
func (p *Processor) readAccount(ctx context.Context, dispatcher *tools.Dispatcher, wakeID int64, result *Result) error {
	id := p.opts.Template.ID
	days := threads.AccountTrailingDays
	if n, err := p.opts.Store.AccountDailyCount(ctx, id); err != nil {
		return metricError("counting account days", err)
	} else if n == 0 {
		days = threads.AccountBackfillDays
	}
	call := llm.ToolCall{ID: fmt.Sprintf("account-%d", wakeID), Type: "function", Function: llm.CallFunction{Name: "read_account_insights", Arguments: fmt.Sprintf(`{"days":%d}`, days)}}
	output, readErr := dispatcher.DispatchHarness(ctx, call)
	if readErr != nil {
		if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) || errors.Is(readErr, guard.ErrAudit) {
			return metricError("reading guarded account insights", readErr)
		}
		reason := "read_failed"
		if errors.Is(readErr, threads.ErrMetricsUnavailable) {
			reason = "metrics_unavailable"
		}
		p.accountDeferred(ctx, wakeID, readErr, reason)
		return nil
	}
	var envelope struct {
		Data threads.AccountInsights `json:"data"`
	}
	if err := json.Unmarshal([]byte(output), &envelope); err != nil {
		return metricError("decoding account insights", err)
	}
	now := p.opts.Now()
	in := envelope.Data
	viewsAt := make(map[int64]int64, len(in.Views))
	for _, v := range in.Views {
		viewsAt[v.EndsAt.Unix()] = v.Value
	}
	var rows []store.AccountDay
	for _, day := range threads.PacificDays(in.Since, in.Until) {
		row := store.AccountDay{Day: day.Day, StartsAt: day.StartsAt, EndsAt: day.EndsAt}
		if v, ok := viewsAt[day.EndsAt.Unix()]; ok {
			row.Value = &v
		}
		rows = append(rows, row)
	}
	totals := map[string]int64{}
	maps.Copy(totals, in.Totals)
	for _, l := range in.Clicks {
		totals["clicks"] += l.Value
	}
	err := p.opts.Store.UpsertAccountDays(ctx, id, "views", rows, now)
	for name, value := range totals {
		if err != nil {
			break
		}
		_, err = p.opts.Store.RecordAccountSample(ctx, id, name, in.Since, in.Until, value, accountSource, now)
	}
	if err != nil {
		p.accountFailed(now, err)
		return metricError("storing account insights", err)
	}
	p.accountMu.Lock()
	p.accountLast, p.accountFailures = now, 0
	p.accountBackoff = time.Time{}
	if in.RegainAfter > 0 {
		p.accountBackoff = now.Add(in.RegainAfter)
	}
	p.accountMu.Unlock()
	result.AccountReads++
	p.emit(ctx, logging.Record{Event: logging.MetricsMeasured, WakeID: wakeID, MeasuredAt: now, Outcome: "success", Reason: reasonAccountRead, Samples: len(rows)})
	return nil
}

func (p *Processor) accountDeferred(ctx context.Context, wakeID int64, err error, reason string) {
	p.accountFailed(p.opts.Now(), err)
	p.emit(ctx, logging.Record{Event: logging.MetricsDeferred, WakeID: wakeID, Outcome: "deferred", Reason: reason})
}
