package threads

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

const accountPath = "/1000000000000001/threads_insights"

func TestAccountInsightsParsesSavedReads(t *testing.T) {
	f, api, _ := newFake(t)
	ctx := context.Background()
	since, until := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)

	f.route("GET", accountPath, "account_multi_default.json")
	got, err := api.ReadAccountInsights(ctx, since, until)
	if err != nil {
		t.Fatal(err)
	}
	want := AccountInsights{
		Since: since, Until: until,
		Views: []DayViews{
			{EndsAt: time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC), Value: 35285},
			{EndsAt: time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC), Value: 36925},
		},
		Totals: map[string]int64{"likes": 133, "replies": 27, "reposts": 0, "quotes": 0},
		Clicks: []LinkClicks{{URL: "https://threadz.canhta.com/", Value: 4}, {URL: "https://example.com/", Value: 0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("account insights = %+v\nwant %+v", got, want)
	}
	q := f.queries["GET "+accountPath]
	if q["metric"] != "views,likes,replies,reposts,quotes,clicks" || q["since"] != "1794556800" || q["until"] != "1794729600" {
		t.Fatalf("query = %v", q)
	}

	f.route("GET", accountPath, "account_views_7d.json")
	got, err = api.ReadAccountInsights(ctx, since, until)
	if err != nil || len(got.Views) != 8 || got.Views[4].Value != 1374 || len(got.Totals) != 0 || got.Clicks != nil {
		t.Fatalf("views-only read = %+v, %v", got, err)
	}
	var sum int64
	for _, v := range got.Views[2:] {
		sum += v.Value
	}
	if sum != 110402 {
		t.Fatalf("sum of the 6 closed days = %d, want 110402", sum)
	}
}

func TestAccountInsightsRefusals(t *testing.T) {
	ctx := context.Background()
	since, until := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	for name, body := range map[string]string{
		"no metrics":     `{"data":[]}`,
		"totals only":    `{"data":[{"name":"likes","total_value":{"value":5}}]}`,
		"clicks only":    `{"data":[{"name":"clicks","link_total_values":[{"value":1,"link_url":"u"}]}]}`,
		"no view days":   `{"data":[{"name":"views","values":[]},{"name":"likes","total_value":{"value":5}}]}`,
		"bad end_time":   `{"data":[{"name":"views","values":[{"value":1,"end_time":"yesterday"}]}]}`,
		"negative views": `{"data":[{"name":"views","values":[{"value":-1,"end_time":"2026-11-14T08:00:00+0000"}]}]}`,
		"total missing":  `{"data":[{"name":"likes"}]}`,
		"negative total": `{"data":[{"name":"likes","total_value":{"value":-2}}]}`,
		"negative click": `{"data":[{"name":"clicks","link_total_values":[{"value":-1,"link_url":"u"}]}]}`,
	} {
		f, api, _ := newFake(t)
		f.route("GET", accountPath, body)
		if _, err := api.ReadAccountInsights(ctx, since, until); !errors.Is(err, ErrMetricsUnavailable) {
			t.Errorf("%s: err = %v, want ErrMetricsUnavailable", name, err)
		}
	}
	f, api, _ := newFake(t)
	f.route("GET", accountPath, "error_since_too_old.json")
	if _, err := api.ReadAccountInsights(ctx, since, until); !errors.Is(err, ErrAPI) {
		t.Errorf("out-of-range since: err = %v, want ErrAPI", err)
	}
}

func TestAccountInsightsUsageHeader(t *testing.T) {
	ctx := context.Background()
	since, until := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	const used = `{"1000000000000001":[{"type":"threads","call_count":1,"total_cputime":1,"total_time":1,"estimated_time_to_regain_access":0}]}`
	const blocked = `{"1000000000000001":[{"type":"threads","call_count":100,"total_cputime":100,"total_time":100,"estimated_time_to_regain_access":12},{"type":"other","estimated_time_to_regain_access":30}]}`

	f, api, _ := newFake(t)
	f.route("GET", accountPath, "account_views_7d.json")
	f.headers = map[string]string{"GET " + accountPath: used}
	if got, err := api.ReadAccountInsights(ctx, since, until); err != nil || got.RegainAfter != 0 {
		t.Fatalf("idle usage: %+v, %v", got, err)
	}
	f.headers["GET "+accountPath] = blocked
	if got, err := api.ReadAccountInsights(ctx, since, until); err != nil || got.RegainAfter != 30*time.Minute {
		t.Fatalf("a success under a wait: %+v, %v", got, err)
	}
	f.headers["GET "+accountPath] = "not json"
	if got, err := api.ReadAccountInsights(ctx, since, until); err != nil || got.RegainAfter != 0 {
		t.Fatalf("unparsable usage: %+v, %v", got, err)
	}

	f.route("GET", accountPath, "error.json")
	f.headers["GET "+accountPath] = blocked
	_, err := api.ReadAccountInsights(ctx, since, until)
	var limited *RateLimitedError
	if !errors.As(err, &limited) || limited.Wait != 30*time.Minute || !errors.Is(err, ErrAPI) {
		t.Fatalf("failed read under a wait: %v", err)
	}
	f.headers["GET "+accountPath] = used
	if _, err = api.ReadAccountInsights(ctx, since, until); err == nil || errors.As(err, &limited) {
		t.Fatalf("failed read without a wait: %v", err)
	}
}

func TestBatchReadUsageHeaderWaitReachesEveryID(t *testing.T) {
	const blocked = `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":12}]}`
	f, api, _ := newFake(t)
	f.route("GET", "/insights", "error.json")
	f.headers = map[string]string{"GET /insights": blocked}
	got := api.ReadActionMetricsBatch(context.Background(), []string{"1", "2"})
	for id, r := range got {
		var limited *RateLimitedError
		if !errors.As(r.Err, &limited) || limited.Wait != 12*time.Minute || !errors.Is(r.Err, ErrAPI) {
			t.Errorf("%s: err = %v, want a RateLimitedError of 12 minutes", id, r.Err)
		}
	}
	f.headers["GET /insights"] = "{}"
	for id, r := range api.ReadActionMetricsBatch(context.Background(), []string{"1"}) {
		var limited *RateLimitedError
		if r.Err == nil || errors.As(r.Err, &limited) {
			t.Errorf("%s: a failure without a wait = %v", id, r.Err)
		}
	}
}

func TestAccountWindowAndPacificDays(t *testing.T) {
	cases := []struct {
		now          time.Time
		days         int
		since, until string
	}{
		// November: PST, midnight is 08:00 UTC.
		{time.Date(2026, 11, 16, 4, 18, 0, 0, time.UTC), 3, "2026-11-12T08:00:00Z", "2026-11-15T08:00:00Z"},
		// August: PDT, midnight is 07:00 UTC.
		{time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC), 1, "2026-08-09T07:00:00Z", "2026-08-10T07:00:00Z"},
		// The window crosses the end of DST on 2026-11-01: that Pacific day has 25 hours.
		{time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC), 3, "2026-10-31T07:00:00Z", "2026-11-03T08:00:00Z"},
	}
	for _, c := range cases {
		since, until := AccountWindow(c.now, c.days)
		if since.Format(time.RFC3339) != c.since || until.Format(time.RFC3339) != c.until {
			t.Errorf("window at %s = %s..%s, want %s..%s", c.now, since.Format(time.RFC3339), until.Format(time.RFC3339), c.since, c.until)
		}
		days := PacificDays(since, until)
		if len(days) != c.days || !days[0].StartsAt.Equal(since) || !days[len(days)-1].EndsAt.Equal(until) {
			t.Errorf("days at %s = %+v", c.now, days)
		}
	}
	since, until := AccountWindow(cases[2].now, 3)
	days := PacificDays(since, until)
	if days[1].Day != "2026-11-01" || days[1].EndsAt.Sub(days[1].StartsAt) != 25*time.Hour || days[0].EndsAt != days[1].StartsAt {
		t.Errorf("DST day = %+v", days[1])
	}
	if got := PacificDays(until, since); got != nil {
		t.Errorf("days of an empty window = %+v", got)
	}
}
