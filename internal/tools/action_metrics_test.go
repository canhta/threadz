package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/guard"
	"github.com/canhta/threadz/internal/llm"
	"github.com/canhta/threadz/internal/threads"
	"github.com/canhta/threadz/internal/tools"
)

// mediaOf is the media id the Memory backend derives from a confirmed URL.
func mediaOf(url string) string {
	if url == "" {
		return ""
	}
	return path.Base(url)
}

func insertPipelineAction(t *testing.T, h *browserHarness, id int64, agent string, dryRun bool, threadURL string) {
	t.Helper()
	dry := 0
	if dryRun {
		dry = 1
	}
	_, err := h.store.DB().Exec(`INSERT INTO actions(id,agent_id,kind,text,target_username,dry_run,posted_at,thread_url,media_id) VALUES(?,?,'reply','',?,?,?,?,?)`,
		id, agent, "foreign_person", dry, "2026-11-11T01:00:00Z", threadURL, mediaOf(threadURL))
	if err != nil {
		t.Fatalf("inserting action %d: %v", id, err)
	}
}

func decodeActionMetricsBatch(t *testing.T, out string) []tools.ActionMetrics {
	t.Helper()
	envelope := decodeBrowserEnvelope(t, out)
	if envelope.Source != "read_own_metrics" {
		t.Fatalf("action metrics source = %q", envelope.Source)
	}
	var got []tools.ActionMetrics
	if err := json.Unmarshal(envelope.Data, &got); err != nil {
		t.Fatalf("decoding action metrics %s: %v", envelope.Data, err)
	}
	return got
}

func decodeActionMetrics(t *testing.T, out string) tools.ActionMetrics {
	t.Helper()
	got := decodeActionMetricsBatch(t, out)
	if len(got) != 1 {
		t.Fatalf("action metrics = %+v, want one entry", got)
	}
	return got[0]
}

func TestReadOwnMetricsIsAHarnessReadTheModelCannotCall(t *testing.T) {
	h := newBrowserHarness(t, true)
	for _, tool := range h.dispatch.Definitions() {
		if tool.Function.Name == "read_own_metrics" {
			t.Fatal("read_own_metrics is offered to the model")
		}
	}
	if _, err := h.dispatch.Dispatch(context.Background(), llm.ToolCall{ID: "model-call", Type: "function", Function: llm.CallFunction{Name: "read_own_metrics", Arguments: `{}`}}); !errors.Is(err, guard.ErrBlocked) {
		t.Fatalf("a model call to a harness read: %v", err)
	}

	followers := int64(31)
	for _, tc := range []struct {
		name string
		own  threads.OwnFollowers
		want string
	}{
		{"known", threads.OwnFollowers{Followers: &followers}, `{"followers_count":31}`},
		{"unavailable", threads.OwnFollowers{}, `{"followers_count":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBrowserHarness(t, true)
			h.backend.Own = tc.own
			out, err := h.call("read_own_metrics", `{}`)
			if err != nil {
				t.Fatalf("aggregate read: %v", err)
			}
			if got := string(decodeBrowserEnvelope(t, out).Data); got != tc.want {
				t.Fatalf("data = %s, want %s", got, tc.want)
			}
			if aggregateReads(h.backend) != 1 {
				t.Fatalf("aggregate backend calls = %d", aggregateReads(h.backend))
			}
		})
	}
}

func TestReadAccountInsightsIsAHarnessReadOfTheClosedPacificDays(t *testing.T) {
	h := newBrowserHarness(t, true)
	for _, tool := range h.dispatch.Definitions() {
		if tool.Function.Name == "read_account_insights" {
			t.Fatal("read_account_insights is offered to the model")
		}
	}
	if _, err := h.dispatch.Dispatch(context.Background(), llm.ToolCall{ID: "model-call", Type: "function", Function: llm.CallFunction{Name: "read_account_insights", Arguments: `{"days":3}`}}); !errors.Is(err, guard.ErrBlocked) {
		t.Fatalf("a model call to a harness read: %v", err)
	}
	if _, err := h.call("read_account_insights", `{}`); err == nil {
		t.Fatal("a read without days was served")
	}

	h.backend.Account = threads.AccountInsights{Totals: map[string]int64{"likes": 5}}
	h.next()
	out, err := h.call("read_account_insights", `{"days":3}`)
	if err != nil {
		t.Fatal(err)
	}
	var got threads.AccountInsights
	if err := json.Unmarshal(decodeBrowserEnvelope(t, out).Data, &got); err != nil {
		t.Fatal(err)
	}
	since, until := threads.AccountWindow(h.now, 3)
	if !got.Since.Equal(since) || !got.Until.Equal(until) || got.Totals["likes"] != 5 {
		t.Fatalf("account insights = %+v, want window %s..%s", got, since, until)
	}
	if n := h.backend.Count(threads.OpReadAccountInsights); n != 1 {
		t.Fatalf("backend reads = %d", n)
	}
}

func TestActionScopedReadBindsOwnURLAndReturnsActionIdentity(t *testing.T) {
	const (
		agentID   = "agent"
		actionID  = int64(711)
		threadURL = "https://threads.example/own/path?reply=one"
	)
	want := threads.Metrics{Views: 412, Likes: 9, Replies: 4, Reposts: 2}
	h := newBrowserHarness(t, true)
	h.backend.Actions[mediaOf(threadURL)] = want
	unbounded := false
	onOp(h.backend, threads.OpReadActionMetrics, func(ctx context.Context) {
		if _, ok := ctx.Deadline(); !ok {
			unbounded = true
		}
	})
	insertPipelineAction(t, h, actionID, agentID, false, threadURL)

	out, err := h.call("read_own_metrics", fmt.Sprintf(`{"action_ids":[%d]}`, actionID))
	if err != nil {
		t.Fatalf("action read: %v", err)
	}
	got := decodeActionMetrics(t, out)
	if got.ActionID != actionID || got.Metrics != want {
		t.Fatalf("action metrics = %+v", got)
	}
	if urls := actionReads(h.backend); len(urls) != 1 || urls[0] != mediaOf(threadURL) {
		t.Fatalf("reader URLs = %v", urls)
	}
	if unbounded {
		t.Fatal("reader context had no deadline")
	}
	if aggregateReads(h.backend) != 0 {
		t.Fatal("action read fell back to the aggregate method")
	}
	if h.backend.Count("post") != 0 || h.backend.Count("reply") != 0 {
		t.Fatalf("metrics performed a write: %v", h.backend.Calls())
	}
	var stored string
	if err := h.store.DB().QueryRow(`SELECT thread_url FROM actions WHERE id=?`, actionID).Scan(&stored); err != nil || stored != threadURL {
		t.Fatalf("action URL changed: %q err=%v", stored, err)
	}

	// The no-argument aggregate call remains available on the same dispatcher.
	out, err = h.call("read_own_metrics", `{}`)
	if err != nil {
		t.Fatalf("aggregate after action read: %v", err)
	}
	envelope := decodeBrowserEnvelope(t, out)
	var aggregate threads.Metrics
	if err := json.Unmarshal(envelope.Data, &aggregate); err != nil {
		t.Fatal(err)
	}
	if aggregateReads(h.backend) != 1 || len(actionReads(h.backend)) != 1 {
		t.Fatal("aggregate and action reads shared a backend path")
	}
}

func TestActionScopedReadRefusesUnboundActionsBeforeReader(t *testing.T) {
	const threadURL = "https://threads.example/own/kept"
	for _, tc := range []struct {
		name    string
		wantID  int64
		agent   string
		dryRun  bool
		binding string
	}{
		{name: "foreign_agent", wantID: 801, agent: "foreign", binding: threadURL},
		{name: "dry_run", wantID: 802, agent: "agent", dryRun: true, binding: threadURL},
		{name: "unconfirmed", wantID: 803, agent: "agent", binding: ""},
		{name: "unknown", wantID: 899, agent: "agent", binding: threadURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBrowserHarness(t, true)
			h.backend.Actions[mediaOf(threadURL)] = threads.Metrics{Views: 1}
			if tc.name != "unknown" {
				insertPipelineAction(t, h, tc.wantID, tc.agent, tc.dryRun, tc.binding)
			}
			out, err := h.call("read_own_metrics", fmt.Sprintf(`{"action_ids":[%d]}`, tc.wantID))
			if err != nil {
				t.Fatal(err)
			}
			if got := decodeActionMetrics(t, out); got.ActionID != tc.wantID || got.Failure != tools.FailureUnavailable || got.Views != 0 {
				t.Fatalf("unbound action = %+v, want an unavailable entry", got)
			}
			if calls := len(actionReads(h.backend)); calls != 0 {
				t.Fatalf("reader called %d times for %s", calls, tc.name)
			}
			if aggregateReads(h.backend) != 0 {
				t.Fatal("refused action fell back to aggregate metrics")
			}
			if h.backend.Count("post") != 0 || h.backend.Count("reply") != 0 {
				t.Fatal("refused action performed a write")
			}
		})
	}
}

func TestActionScopedReadRejectsInvalidArgumentsBeforeReader(t *testing.T) {
	for _, args := range []string{
		`{"action_ids":[0]}`,
		`{"action_ids":[-1]}`,
		`{"action_ids":[1.5]}`,
		`{"action_ids":["1"]}`,
		`{"action_ids":[9223372036854775808]}`,
		`{"action_ids":true}`,
		`{"action_ids":1}`,
		`{"action_id":1}`,
	} {
		t.Run(strings.NewReplacer(`{`, "", `}`, "", `"`, "", `:`, "_").Replace(args), func(t *testing.T) {
			h := newBrowserHarness(t, true)
			h.backend.Actions[mediaOf("https://threads.example/own")] = threads.Metrics{Views: 1}
			insertPipelineAction(t, h, 901, "agent", false, "https://threads.example/own")
			out, err := h.call("read_own_metrics", args)
			if err == nil {
				t.Fatalf("invalid action_ids accepted: %s", out)
			}
			if len(actionReads(h.backend)) != 0 {
				t.Fatal("invalid argument reached the optional reader")
			}
		})
	}
}

func TestActionScopedReadRejectsUnsafeSourceResults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  threads.Metrics
		err     error
		failure string
	}{
		{name: "negative", values: threads.Metrics{Views: -1}, failure: tools.FailureUnavailable},
		{name: "reply_negative", values: threads.Metrics{Replies: -2}, failure: tools.FailureUnavailable},
		{name: "too_large", values: threads.Metrics{Views: 1<<53 + 1}, failure: tools.FailureUnavailable},
		{name: "source_error", err: errors.New("private transport detail"), failure: tools.FailureRead},
		{name: "not_found", err: threads.ErrMediaNotFound, failure: tools.FailureNotFound},
		{name: "unavailable", err: threads.ErrMetricsUnavailable, failure: tools.FailureUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBrowserHarness(t, true)
			h.backend.Actions[mediaOf("https://threads.example/own")] = tc.values
			h.backend.Errors[threads.OpReadActionMetrics] = tc.err
			insertPipelineAction(t, h, 904, "agent", false, "https://threads.example/own")
			out, err := h.call("read_own_metrics", `{"action_ids":[904]}`)
			if err != nil {
				t.Fatal(err)
			}
			if got := decodeActionMetrics(t, out); got.Failure != tc.failure || got.Views != 0 || got.Replies != 0 {
				t.Fatalf("unsafe source result = %+v, want failure %q and no counters", got, tc.failure)
			}
			if strings.Contains(out, "private transport detail") {
				t.Fatalf("raw source error leaked: %s", out)
			}
			if aggregateReads(h.backend) != 0 {
				t.Fatal("unsafe source result fell back to aggregate metrics")
			}
		})
	}
}

func TestActionMetricsRecordWhetherTheTargetAuthorReplied(t *testing.T) {
	const threadURL = "https://threads.example/own/reply-1"
	for _, tc := range []struct {
		name string
		page threads.Page
		err  error
		want *bool
	}{
		{"author answered", threads.Page{Kind: "replies", Posts: []threads.Post{{Username: "someone"}, {Username: "@Foreign_Person", IsReply: true}}}, nil, ptr(true)},
		{"author silent", threads.Page{Kind: "replies", Posts: []threads.Post{{Username: "someone"}}}, nil, ptr(false)},
		{"no replies", threads.Page{Kind: "replies"}, nil, ptr(false)},
		{"replies unreadable", threads.Page{}, errors.New("boom"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBrowserHarness(t, true)
			h.backend.Actions[mediaOf(threadURL)] = threads.Metrics{Views: 10, Likes: 1}
			h.backend.Pages[threads.OpReadReplies] = tc.page
			h.backend.Errors[threads.OpReadReplies] = tc.err
			insertPipelineAction(t, h, 950, "agent", false, threadURL)
			out, err := h.call("read_own_metrics", `{"action_ids":[950]}`)
			if err != nil {
				t.Fatal(err)
			}
			got := decodeActionMetrics(t, out)
			if (got.AuthorReplied == nil) != (tc.want == nil) || (tc.want != nil && *got.AuthorReplied != *tc.want) {
				t.Fatalf("author replied=%v want %v", got.AuthorReplied, tc.want)
			}
			if last := h.backend.Last(threads.OpReadReplies); last.Arg != mediaOf(threadURL) {
				t.Fatalf("replies read for %q", last.Arg)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestActionMetricsBatchIsOneSourceCallWithPerActionOutcomes(t *testing.T) {
	h := newBrowserHarness(t, true)
	const good, bad, ok = "https://threads.example/own/good", "https://threads.example/own/bad", "https://threads.example/own/ok"
	for i, url := range []string{good, bad, ok} {
		insertPipelineAction(t, h, int64(i+1), "agent", false, url)
	}
	h.backend.Actions[mediaOf(good)] = threads.Metrics{Views: 7, Likes: 2}
	h.backend.Actions[mediaOf(ok)] = threads.Metrics{Views: 9}
	h.backend.ActionErrors = map[string]error{mediaOf(bad): threads.ErrMediaNotFound}
	out, err := h.call("read_own_metrics", `{"action_ids":[1,2,3,99]}`)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeActionMetricsBatch(t, out)
	if len(got) != 4 || got[0].Views != 7 || got[1].Failure != tools.FailureNotFound || got[2].Views != 9 || got[3].ActionID != 99 || got[3].Failure != tools.FailureUnavailable {
		t.Fatalf("batch = %+v", got)
	}
	if reads := actionReads(h.backend); len(reads) != 1 || reads[0] != mediaOf(good)+","+mediaOf(bad)+","+mediaOf(ok) {
		t.Fatalf("source calls = %v, want one batched call of the three bound media", reads)
	}
}

func TestActionMetricsBatchLoggedOutFailsTheCall(t *testing.T) {
	h := newBrowserHarness(t, true)
	insertPipelineAction(t, h, 1, "agent", false, "https://threads.example/own/one")
	h.backend.Errors[threads.OpReadActionMetrics] = threads.ErrLoggedOut
	if _, err := h.call("read_own_metrics", `{"action_ids":[1]}`); !errors.Is(err, threads.ErrLoggedOut) {
		t.Fatalf("logged-out batch = %v, want ErrLoggedOut", err)
	}
}

func TestActionMetricsBatchCancellationFailsTheCall(t *testing.T) {
	h := newBrowserHarness(t, true)
	insertPipelineAction(t, h, 1, "agent", false, "https://threads.example/own/one")
	ctx, cancel := context.WithCancel(context.Background())
	onOp(h.backend, threads.OpReadActionMetrics, func(context.Context) { cancel() })
	h.backend.Errors[threads.OpReadActionMetrics] = context.Canceled
	call := llm.ToolCall{ID: "cancel", Type: "function", Function: llm.CallFunction{Name: "read_own_metrics", Arguments: `{"action_ids":[1]}`}}
	if _, err := h.dispatch.DispatchHarness(ctx, call); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled batch = %v, want context.Canceled", err)
	}
}

func TestActionMetricsRequestTimeoutFailsOnlyItsActions(t *testing.T) {
	h := newBrowserHarness(t, true)
	const slow, fast = "https://threads.example/own/slow", "https://threads.example/own/fast"
	insertPipelineAction(t, h, 1, "agent", false, slow)
	insertPipelineAction(t, h, 2, "agent", false, fast)
	h.backend.Actions[mediaOf(fast)] = threads.Metrics{Views: 9}
	h.backend.ActionErrors = map[string]error{mediaOf(slow): context.DeadlineExceeded}
	out, err := h.call("read_own_metrics", `{"action_ids":[1,2]}`)
	if err != nil {
		t.Fatalf("a request timeout failed the whole batch: %v", err)
	}
	got := decodeActionMetricsBatch(t, out)
	if len(got) != 2 || got[0].Failure != tools.FailureRead || got[1].Views != 9 || got[1].Failure != "" {
		t.Fatalf("batch = %+v, want the slow action read_failed and the other measured", got)
	}
}

func TestActionMetricsReportsTheWaitTheAPIAskedFor(t *testing.T) {
	h := newBrowserHarness(t, true)
	const limited, fine = "https://threads.example/own/limited", "https://threads.example/own/fine"
	insertPipelineAction(t, h, 1, "agent", false, limited)
	insertPipelineAction(t, h, 2, "agent", false, fine)
	h.backend.Actions[mediaOf(fine)] = threads.Metrics{Views: 9}
	h.backend.ActionErrors = map[string]error{mediaOf(limited): &threads.RateLimitedError{Wait: 12 * time.Minute, Err: threads.ErrAPI}}
	out, err := h.call("read_own_metrics", `{"action_ids":[1,2]}`)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeActionMetricsBatch(t, out)
	if got[0].Failure != tools.FailureSource || got[0].RetryAfterSeconds != 720 || got[1].Views != 9 || got[1].RetryAfterSeconds != 0 {
		t.Fatalf("batch = %+v, want the first source_unavailable with a 720 s wait", got)
	}
}
