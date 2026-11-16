package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	output, readErr := p.dispatchAccount(ctx, dispatcher, fmt.Sprintf("account-%d", wakeID), "read_account_insights", fmt.Sprintf(`{"days":%d}`, days))
	if readErr != nil {
		return p.accountReadFailed(ctx, wakeID, readErr)
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
	var clicks int64
	for _, l := range in.Clicks {
		clicks += l.Value
	}
	err := p.opts.Store.UpsertAccountDays(ctx, id, "views", rows, now)
	if err == nil {
		_, err = p.opts.Store.RecordAccountSample(ctx, id, "clicks", in.Since, in.Until, clicks, accountSource, now)
	}
	if err != nil {
		p.accountFailed(now, err)
		return metricError("storing account insights", err)
	}
	regain, ok, err := p.readAccountTotals(ctx, dispatcher, wakeID, now)
	if err != nil || !ok {
		return err
	}
	p.accountMu.Lock()
	p.accountLast, p.accountFailures = now, 0
	p.accountBackoff = time.Time{}
	if wait := max(in.RegainAfter, regain); wait > 0 {
		p.accountBackoff = now.Add(wait)
	}
	p.accountMu.Unlock()
	result.AccountReads++
	p.emit(ctx, logging.Record{Event: logging.MetricsMeasured, WakeID: wakeID, MeasuredAt: now, Outcome: "success", Reason: reasonAccountRead, Samples: len(rows)})
	return nil
}

// readAccountTotals reads, newest first, each Pacific day due its likes,
// replies, reposts and quotes (store.AccountTotalsDue) and stores them. A day
// the API has no totals for stays without a value and is retried by the next
// read. It stops at the first failure, which it reports as accountReadFailed
// does, or when a usage wait is asked for; it returns that wait;
// ok is false when the read failed.
func (p *Processor) readAccountTotals(ctx context.Context, dispatcher *tools.Dispatcher, wakeID int64, now time.Time) (regain time.Duration, ok bool, err error) {
	id := p.opts.Template.ID
	since, until := threads.AccountWindow(now, 1)
	closed, _ := time.Parse(time.DateOnly, threads.PacificDays(since, until)[0].Day)
	since, until = threads.AccountWindow(now, threads.AccountTrailingDays)
	due, err := p.opts.Store.AccountTotalsDue(ctx, id, threads.AccountTotalMetrics, threads.PacificDays(since, until)[0].Day, accountTotalsDays)
	if err != nil {
		return 0, false, metricError("listing account days due for totals", err)
	}
	for i, day := range due {
		if regain > 0 {
			break
		}
		date, err := time.Parse(time.DateOnly, day)
		if err != nil {
			return 0, false, metricError("reading a stored account day", err)
		}
		daysAgo := int(closed.Sub(date).Hours()/24) + 1
		output, err := p.dispatchAccount(ctx, dispatcher, fmt.Sprintf("account-%d-%d", wakeID, i), "read_account_totals_day", fmt.Sprintf(`{"days_ago":%d}`, daysAgo))
		if errors.Is(err, threads.ErrMetricsUnavailable) {
			continue
		}
		if err != nil {
			return 0, false, p.accountReadFailed(ctx, wakeID, err)
		}
		var envelope struct {
			Data threads.DayTotals `json:"data"`
		}
		if err = json.Unmarshal([]byte(output), &envelope); err != nil {
			return 0, false, metricError("decoding account day totals", err)
		}
		d := envelope.Data
		for _, metric := range threads.AccountTotalMetrics {
			value := d.Totals[metric]
			if err = p.opts.Store.UpsertAccountDays(ctx, id, metric, []store.AccountDay{{Day: d.Day, StartsAt: d.StartsAt, EndsAt: d.EndsAt, Value: &value}}, now); err != nil {
				p.accountFailed(now, err)
				return 0, false, metricError("storing account day totals", err)
			}
		}
		regain = d.RegainAfter
	}
	return regain, true, nil
}

func (p *Processor) dispatchAccount(ctx context.Context, dispatcher *tools.Dispatcher, id, tool, args string) (string, error) {
	return dispatcher.DispatchHarness(ctx, llm.ToolCall{ID: id, Type: "function", Function: llm.CallFunction{Name: tool, Arguments: args}})
}

// accountReadFailed backs the next read off after a failed guarded read and
// returns nil; cancellation and audit failures are returned.
func (p *Processor) accountReadFailed(ctx context.Context, wakeID int64, readErr error) error {
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

func (p *Processor) accountDeferred(ctx context.Context, wakeID int64, err error, reason string) {
	p.accountFailed(p.opts.Now(), err)
	p.emit(ctx, logging.Record{Event: logging.MetricsDeferred, WakeID: wakeID, Outcome: "deferred", Reason: reason})
}
