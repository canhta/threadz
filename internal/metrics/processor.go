// Package metrics owns fixed-window remeasurement of confirmed own actions.
// It never calls a model, submits content or changes the agent's chosen schedule.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/canhta/threadz/internal/config"
	"github.com/canhta/threadz/internal/guard"
	"github.com/canhta/threadz/internal/llm"
	"github.com/canhta/threadz/internal/logging"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
	"github.com/canhta/threadz/internal/tools"
	"github.com/canhta/threadz/internal/trends"
)

const (
	DefaultLeaseDuration = 5 * time.Minute
	DefaultRetryDelay    = 5 * time.Minute
	// DefaultBatchSize is the most action groups one run reads so that the
	// worst case stays inside DefaultLeaseDuration: the read takes at most
	// tools.MetricReadBudget(n) (one 30 s insights call per 25 actions, then a
	// 30 s replies read per action) after leaseOverhead. For n = 7 that is
	// (1+7)*30 s + 45 s = 285 s < 300 s; n = 8 would need 315 s.
	DefaultBatchSize = 7
	// leaseOverhead is what runs between the claim and the read: opening the
	// backend (30 s startup) and the account read (one 15 s request).
	leaseOverhead = 45 * time.Second

	// RefreshCooldown is the shortest time between two refreshes of one action.
	RefreshCooldown = time.Minute
	// refreshAge is how young an action is to be refreshed on request.
	refreshAge = 24 * time.Hour

	// AccountPollEvery is the period of the account insights read.
	AccountPollEvery = time.Hour
	// accountCloseDelay is how long after a Pacific midnight the closing read is made.
	accountCloseDelay = 5 * time.Minute
	// accountBackoffSteps caps the doubling of RetryDelay after failed account reads.
	accountBackoffSteps = 4

	accountSource     = "api"
	reasonAccountRead = "account_insights_stored"
)

type Options struct {
	Store          *store.Store
	Template       config.Template
	Now            func() time.Time
	Logger         *slog.Logger
	ThreadsFactory threads.Factory
	Owner          tools.Owner
	Redactor       *config.Redactor
	FetchTrends    func(context.Context) ([]trends.Trend, error)
	SearchWeb      trends.WebSearcher
	LeaseDuration  time.Duration
	RetryDelay     time.Duration
	BatchSize      int
}
type Result struct {
	ClaimedJobs  int `json:"claimed_jobs"`
	Samples      int `json:"samples"`
	DeferredJobs int `json:"deferred_jobs"`
	// AccountReads is 1 when this run stored an account insights read.
	AccountReads int `json:"account_reads"`
	// Refreshed counts the samples stored for refresh requests.
	Refreshed int `json:"refreshed"`
	// Skipped counts the actions a refresh request left out because they were refreshed less than RefreshCooldown ago.
	Skipped int `json:"skipped"`
}
type Processor struct {
	mu   sync.Mutex
	opts Options

	accountMu       sync.Mutex
	accountLast     time.Time
	accountBackoff  time.Time
	accountFailures int

	// refreshLast is when each action was last refreshed; mu guards it.
	refreshLast map[int64]time.Time
}

func New(opts Options) (*Processor, error) {
	if opts.Store == nil || opts.Template.ID == "" || opts.Template.Limits.MinMinutesBetweenWakes < 1 || opts.Template.Limits.MinMinutesBetweenWakes > 1440 {
		return nil, errors.New("configuring metrics: store, agent and valid wake interval required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = DefaultLeaseDuration
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = DefaultRetryDelay
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.LeaseDuration < 0 || opts.RetryDelay < 0 || opts.BatchSize < 1 || opts.BatchSize > 1000 {
		return nil, errors.New("configuring metrics: positive lease, retry and bounded batch required")
	}
	if opts.ThreadsFactory == nil {
		opts.ThreadsFactory, _ = threads.NewBackend(opts.Template, opts.Now, nil)
	}
	return &Processor{opts: opts}, nil
}

// NextDue reconciles confirmed actions before returning the next eligible target
// or retry/lease expiry. Paused and killed agents have no eligible due time.
func (p *Processor) NextDue(ctx context.Context) (time.Time, bool, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	state, err := p.opts.Store.AgentState(ctx, p.opts.Template.ID)
	if err != nil {
		return time.Time{}, false, metricError("reading metric agent", err)
	}
	if state.Status != "running" {
		return time.Time{}, false, nil
	}
	count, err := p.opts.Store.SeedMetricJobs(ctx, p.opts.Template.ID, p.opts.Now())
	if err != nil {
		return time.Time{}, false, metricError("seeding metric targets", err)
	}
	if count > 0 {
		p.emit(ctx, logging.Record{Event: logging.MetricsScheduled, Outcome: "success", Reason: "targets_seeded", Jobs: count})
	}
	at, ok, err := p.opts.Store.NextMetricDue(ctx, p.opts.Template.ID)
	if err != nil {
		return at, ok, metricError("reading next metric target", err)
	}
	if due, enabled := p.accountDue(p.opts.Now()); enabled && (!ok || due.Before(at)) {
		at, ok = due, true
	}
	return at, ok, nil
}

// RunDue leases at most BatchSize action groups. Each group has one guarded
// current read, one actual-timestamp sample and all its ready targets completed
// atomically. Missing/failed reads are deferred without manufacturing zeros.
func (p *Processor) RunDue(ctx context.Context) (Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result, _, err := p.runDue(ctx, nil)
	return result, err
}

// runDue is RunDue with the refresh requests to serve; the caller holds p.mu.
// ran reports whether a metric wake opened, so a read was attempted.
func (p *Processor) runDue(ctx context.Context, refresh []int64) (result Result, ran bool, runErr error) {
	due, ok, err := p.NextDue(ctx)
	if err != nil {
		return result, false, err
	}
	if len(refresh) == 0 && (!ok || due.After(p.opts.Now())) {
		return result, false, nil
	}
	available, err := p.available(ctx)
	if err != nil {
		return result, false, metricError("checking metric wake", err)
	}
	if !available {
		return result, false, nil
	}
	wakeID, err := p.opts.Store.BeginMetricWake(ctx, p.opts.Template.ID, p.opts.Now())
	if err != nil {
		if ready, e := p.available(ctx); e == nil && !ready {
			return result, false, nil
		}
		return result, false, metricError("starting metric wake", err)
	}
	ran = true
	endReason := "completed"
	accountDue := false
	defer func() {
		finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if runErr == nil && result.ClaimedJobs == 0 && !accountDue && len(refresh) == 0 {
			// Nothing was due after all: leave no wake row behind.
			runErr = metricError("dropping empty metric wake", p.opts.Store.DeleteMetricWake(finalCtx, wakeID))
			return
		}
		if runErr != nil {
			endReason = "measurement_failed"
		}
		if err := p.opts.Store.FinishWake(finalCtx, wakeID, p.opts.Now(), 0, endReason); err != nil {
			runErr = errors.Join(runErr, metricError("finishing metric wake", err))
		}
		outcome := "success"
		if runErr != nil {
			outcome = "error"
		}
		p.emit(finalCtx, logging.Record{Event: logging.MetricsFinished, WakeID: wakeID, Outcome: outcome, Reason: endReason, Jobs: result.ClaimedJobs, Samples: result.Samples})
	}()
	jobs, err := p.opts.Store.ClaimMetricJobs(ctx, p.opts.Template.ID, p.opts.Now(), p.opts.LeaseDuration, p.opts.BatchSize)
	if err != nil {
		return result, true, metricError("claiming metric targets", err)
	}
	result.ClaimedJobs = len(jobs)
	if due, enabled := p.accountDue(p.opts.Now()); enabled && !due.After(p.opts.Now()) {
		accountDue = true
	}
	if len(jobs) == 0 && !accountDue && len(refresh) == 0 {
		return result, true, nil
	}
	groups := groupJobs(jobs)
	// Every early exit releases unprocessed claims under a bounded cleanup context.
	pending := groups
	defer func() {
		if len(pending) == 0 {
			return
		}
		finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		reason := "measurement_failed"
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			reason = "cancelled"
		}
		for _, group := range pending {
			if err := p.deferJobs(finalCtx, wakeID, group, reason, 0); err != nil {
				runErr = errors.Join(runErr, err)
			} else {
				result.DeferredJobs += len(group)
			}
		}
	}()
	// The source may bind its lifetime to this context. Keep it alive through
	// cleanup; its own startup and each guarded read have tighter timeouts.
	openCtx, cancel := context.WithTimeout(ctx, p.opts.LeaseDuration)
	defer cancel()
	backend, err := p.opts.ThreadsFactory(openCtx, p.opts.Template)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, true, metricError("opening metric source", safeContextError(err))
		}
		if accountDue {
			p.accountFailed(p.opts.Now(), err)
		}
		for _, group := range groups {
			if e := p.deferJobs(ctx, wakeID, group, "source_unavailable", 0); e != nil {
				return result, true, e
			}
			result.DeferredJobs += len(group)
			pending = pending[1:]
		}
		endReason = "source_unavailable"
		return result, true, nil
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		// Cleanup happens after the maintenance scope decided its samples.
		// The backend logs its own close failure; it must not rewrite a
		// completed scope or stop the scheduler.
		_ = backend.Close(closeCtx)
	}()
	dispatcher, err := tools.NewDispatcher(tools.Options{Scope: guard.Scope{Store: p.opts.Store, Template: p.opts.Template, WakeID: wakeID, Now: p.opts.Now, Logger: p.opts.Logger, AllowLive: true}, Threads: backend, Owner: p.opts.Owner, Redactor: p.opts.Redactor, FetchTrends: p.opts.FetchTrends, SearchWeb: p.opts.SearchWeb})
	if err != nil {
		return result, true, metricError("configuring metric tools", err)
	}
	if accountDue {
		if err := p.readAccount(ctx, dispatcher, wakeID, &result); err != nil {
			return result, true, err
		}
	}
	// Agent wakes read the follower count from the profile page and record it;
	// a batch stamps the newest one rather than opening the browser again.
	var followers *int64
	if n, ok, err := p.opts.Store.LatestFollowers(ctx, p.opts.Template.ID); err != nil {
		return result, true, metricError("reading recorded followers", err)
	} else if ok {
		followers = &n
	}
	ids := make([]int64, 0, len(groups)+len(refresh))
	grouped := map[int64]bool{}
	for _, group := range groups {
		ids = append(ids, group[0].ActionID)
		grouped[group[0].ActionID] = true
	}
	var sampleOnly []int64
	for _, id := range refresh {
		if !grouped[id] {
			ids = append(ids, id)
			sampleOnly = append(sampleOnly, id)
		}
	}
	var items map[int64]tools.ActionMetrics
	var batchErr error
	if len(ids) > 0 {
		items, batchErr = p.readBatch(ctx, dispatcher, wakeID, ids)
	}
	if errors.Is(batchErr, context.Canceled) || errors.Is(batchErr, context.DeadlineExceeded) || errors.Is(batchErr, guard.ErrAudit) {
		return result, true, metricError("reading guarded metrics", batchErr)
	}
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return result, true, err
		}
		first := group[0]
		item, found := items[first.ActionID]
		reason := item.Failure
		switch {
		case errors.Is(batchErr, errBinding) || (batchErr == nil && !found):
			reason = "binding_mismatch"
		case errors.Is(batchErr, threads.ErrLoggedOut) || errors.Is(batchErr, threads.ErrCaptcha):
			reason = tools.FailureAuth
		case batchErr != nil:
			reason = tools.FailureRead
		case reason == tools.FailureNotFound:
			if p.gone(ctx, backend, first.ActionID) {
				if e := p.opts.Store.EndGoneMetricJobs(ctx, group, p.opts.Now()); e != nil {
					return result, true, metricError("ending metric targets", e)
				}
				pending = pending[1:]
				continue
			}
			reason = tools.FailureRead
		}
		if reason != "" {
			if e := p.deferJobs(ctx, wakeID, group, reason, time.Duration(item.RetryAfterSeconds)*time.Second); e != nil {
				return result, true, e
			}
			result.DeferredJobs += len(group)
			pending = pending[1:]
			endReason = reason
			continue
		}
		at := p.opts.Now()
		values := item.Metrics
		committed, e := p.opts.Store.CompleteMetricJobs(ctx, group, at, store.MetricValues{Views: values.Views, Likes: values.Likes, Replies: values.Replies, Reposts: values.Reposts, Quotes: values.Quotes, Shares: values.Shares, Followers: followers, AuthorReplied: item.AuthorReplied})
		if e != nil {
			// A stale/expired fence cannot write or release a newer claim. Leave it
			// recoverable at lease expiry; other DB errors get a future retry if possible.
			if errors.Is(e, store.ErrMetricClaim) {
				pending = pending[1:]
				return result, true, metricError("committing metric sample", e)
			}
			if deferErr := p.deferJobs(context.WithoutCancel(ctx), wakeID, group, "persistence_failed", 0); deferErr != nil {
				return result, true, errors.Join(metricError("committing metric sample", e), deferErr)
			}
			result.DeferredJobs += len(group)
			pending = pending[1:]
			return result, true, metricError("committing metric sample", e)
		}
		pending = pending[1:]
		if committed {
			result.Samples++
			earliest := group[0].DueAt
			for _, job := range group {
				if job.DueAt.Before(earliest) {
					earliest = job.DueAt
				}
			}
			p.emit(ctx, logging.Record{Event: logging.MetricsMeasured, WakeID: wakeID, ActionID: first.ActionID, TargetAt: earliest, MeasuredAt: at, LatenessMS: max(0, at.Sub(earliest).Milliseconds()), Outcome: "success", Reason: "sample_committed", Jobs: len(group), Samples: 1})
			for _, job := range group {
				p.emit(ctx, logging.Record{Event: logging.MetricsTargetCompleted, WakeID: wakeID, ActionID: job.ActionID, WindowMinutes: job.WindowMinutes, TargetAt: job.DueAt, MeasuredAt: at, LatenessMS: max(0, at.Sub(job.DueAt).Milliseconds()), Outcome: "success", Reason: "target_completed", Jobs: 1})
			}
		}
	}
	for _, id := range sampleOnly {
		item, found := items[id]
		if batchErr != nil || !found || item.Failure != "" {
			continue
		}
		values := item.Metrics
		if e := p.opts.Store.RecordMetricSample(ctx, p.opts.Template.ID, id, p.opts.Now(), store.MetricValues{Views: values.Views, Likes: values.Likes, Replies: values.Replies, Reposts: values.Reposts, Quotes: values.Quotes, Shares: values.Shares, Followers: followers, AuthorReplied: item.AuthorReplied}); e != nil {
			return result, true, metricError("recording refreshed sample", e)
		}
		result.Refreshed++
	}
	return result, true, nil
}

// errBinding means the guarded read answered with something other than the
// untrusted metrics of the requested actions.
var errBinding = errors.New("metric read did not match its request")

// readBatch reads the actions' metrics in one guarded call and indexes them by
// action id. A returned error that is not errBinding is the call's own: a
// cancellation, an audit failure or a store failure.
func (p *Processor) readBatch(ctx context.Context, dispatcher *tools.Dispatcher, wakeID int64, ids []int64) (map[int64]tools.ActionMetrics, error) {
	args, err := json.Marshal(map[string][]int64{"action_ids": ids})
	if err != nil {
		return nil, err
	}
	call := llm.ToolCall{ID: fmt.Sprintf("metric-%d", wakeID), Type: "function", Function: llm.CallFunction{Name: "read_own_metrics", Arguments: string(args)}}
	output, err := dispatcher.DispatchHarness(ctx, call)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Untrusted bool                  `json:"untrusted"`
		Source    string                `json:"source"`
		Data      []tools.ActionMetrics `json:"data"`
	}
	if json.Unmarshal([]byte(output), &envelope) != nil || !envelope.Untrusted || envelope.Source != "read_own_metrics" || len(envelope.Data) != len(ids) {
		return nil, errBinding
	}
	items := make(map[int64]tools.ActionMetrics, len(ids))
	for i, item := range envelope.Data {
		if item.ActionID != ids[i] {
			return nil, errBinding
		}
		items[item.ActionID] = item
	}
	return items, nil
}

// Refresh samples the given own actions that are younger than a day now, apart
// from the schedule, at most once per RefreshCooldown per action. It is the one
// entry point for an operator or the dashboard asking for fresher numbers. The
// result counts the samples stored and the actions the cooldown skipped. With
// no ids it covers every own action younger than a day.
func (p *Processor) Refresh(ctx context.Context, actionIDs []int64) (Result, error) {
	now := p.opts.Now()
	young, err := p.opts.Store.YoungOwnActions(ctx, p.opts.Template.ID, actionIDs, now.Add(-refreshAge))
	if err != nil {
		return Result{}, metricError("reading young actions", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refreshLast == nil {
		p.refreshLast = map[int64]time.Time{}
	}
	for id, at := range p.refreshLast {
		if now.Sub(at) >= RefreshCooldown {
			delete(p.refreshLast, id)
		}
	}
	var ids []int64
	skipped := 0
	for _, id := range young {
		if _, cooling := p.refreshLast[id]; cooling {
			skipped++
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return Result{Skipped: skipped}, nil
	}
	result, ran, err := p.runDue(ctx, ids)
	if ran {
		for _, id := range ids {
			p.refreshLast[id] = now
		}
	}
	result.Skipped = skipped
	return result, err
}

func (p *Processor) available(ctx context.Context) (bool, error) {
	return p.opts.Store.MetricsWakeReady(ctx, p.opts.Template.ID)
}
func groupJobs(jobs []store.MetricJob) [][]store.MetricJob {
	groups := [][]store.MetricJob{}
	positions := map[int64]int{}
	for _, job := range jobs {
		i, ok := positions[job.ActionID]
		if !ok {
			i = len(groups)
			positions[job.ActionID] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], job)
	}
	return groups
}

// gone reports whether the account's own post listing no longer holds the
// action's media: the post was deleted on Threads. A failed listing proves nothing.
// Limit: the listing holds the newest 100 threads; an older post would read as gone.
func (p *Processor) gone(ctx context.Context, backend threads.Backend, actionID int64) bool {
	action, ok, err := p.opts.Store.ConfirmedAction(ctx, p.opts.Template.ID, actionID)
	if err != nil || !ok || action.MediaID == "" {
		return false
	}
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	page, err := backend.ReadOwnPosts(listCtx)
	if err != nil {
		return false
	}
	for _, post := range page.Posts {
		if post.ID == action.MediaID {
			return false
		}
	}
	return true
}

// deferJobs retries after RetryDelay doubled for each earlier counted attempt,
// or after wait when the source asked for longer.
func (p *Processor) deferJobs(ctx context.Context, wakeID int64, jobs []store.MetricJob, reason string, wait time.Duration) error {
	finalCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	now := p.opts.Now()
	attempts := 0
	for _, job := range jobs {
		attempts = max(attempts, job.Attempts)
	}
	if err := p.opts.Store.DeferMetricJobs(finalCtx, jobs, now, now.Add(max(wait, p.opts.RetryDelay<<min(attempts, store.MaxMetricAttempts))), reason); err != nil {
		return metricError("deferring metric targets", err)
	}
	for _, job := range jobs {
		p.emit(finalCtx, logging.Record{Event: logging.MetricsDeferred, WakeID: wakeID, ActionID: job.ActionID, WindowMinutes: job.WindowMinutes, TargetAt: job.DueAt, Outcome: "deferred", Reason: reason, Jobs: len(jobs)})
	}
	return nil
}
func (p *Processor) emit(ctx context.Context, r logging.Record) {
	r.AgentID = p.opts.Template.ID
	r.Operation = "metrics"
	logging.Emit(ctx, p.opts.Logger, slog.LevelInfo, r)
}

type operationError struct {
	operation string
	cause     error
}

func (e *operationError) Error() string { return e.operation + ": failed" }
func (e *operationError) Unwrap() error { return e.cause }
func metricError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &operationError{operation, err}
}
func safeContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("source_failed")
}
