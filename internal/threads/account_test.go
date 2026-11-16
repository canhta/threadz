package threads

import (
	"context"
	"errors"
	"reflect"
	"strconv"
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
		Clicks: []LinkClicks{{URL: "https://threadz.canhta.com/", Value: 4}, {URL: "https://example.com/", Value: 0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("account insights = %+v\nwant %+v", got, want)
	}
	q := f.queries["GET "+accountPath]
	if q["metric"] != "views,clicks" || q["since"] != "1794556800" || q["until"] != "1794729600" {
		t.Fatalf("query = %v", q)
	}

	f.route("GET", accountPath, "account_views_7d.json")
	got, err = api.ReadAccountInsights(ctx, since, until)
	if err != nil || len(got.Views) != 8 || got.Views[4].Value != 1374 || got.Clicks != nil {
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

func TestAccountTotalsDayParsesSavedRead(t *testing.T) {
	f, api, _ := newFake(t)
	day := mustPacificDay(t, "2026-11-14")
	f.route("GET", accountPath, "account_totals_day.json")
	f.headers = map[string]string{"GET " + accountPath: `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":7}]}`}
	got, err := api.ReadAccountTotalsDay(context.Background(), day)
	if err != nil {
		t.Fatal(err)
	}
	want := DayTotals{PacificDay: day, Totals: map[string]int64{"likes": 168, "replies": 40, "reposts": 0, "quotes": 0}, RegainAfter: 7 * time.Minute}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("day totals = %+v\nwant %+v", got, want)
	}
	q := f.queries["GET "+accountPath]
	if q["metric"] != "likes,replies,reposts,quotes" || q["since"] != "1794729600" || q["until"] != "1794733200" {
		t.Fatalf("query = %v", q)
	}
}

func mustPacificDay(t *testing.T, date string) PacificDay {
	t.Helper()
	day, err := ParsePacificDay(date)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

// The API totals every bucket whose end_time lies in [since, until], both ends
// inclusive; the bucket of a Pacific day ends at the midnight that closes it,
// so one hour after that midnight selects that day alone.
func TestAccountTotalsDayWindowIsTheClosingMidnightAndOneHour(t *testing.T) {
	for _, c := range []struct {
		date, starts, since string
	}{
		{"2026-11-14", "2026-11-14T08:00:00Z", "2026-11-15T08:00:00Z"},
		{"2026-03-08", "2026-03-08T08:00:00Z", "2026-03-09T07:00:00Z"},
		{"2026-11-01", "2026-11-01T07:00:00Z", "2026-11-02T08:00:00Z"},
	} {
		day := mustPacificDay(t, c.date)
		if day.Day != c.date || day.StartsAt.Format(time.RFC3339) != c.starts || day.EndsAt.Format(time.RFC3339) != c.since {
			t.Errorf("Pacific day %s = %+v", c.date, day)
		}
		f, api, _ := newFake(t)
		f.route("GET", accountPath, "account_totals_day.json")
		if _, err := api.ReadAccountTotalsDay(context.Background(), day); err != nil {
			t.Fatal(err)
		}
		since := day.EndsAt
		q := f.queries["GET "+accountPath]
		if q["since"] != strconv.FormatInt(since.Unix(), 10) || q["until"] != strconv.FormatInt(since.Add(time.Hour).Unix(), 10) {
			t.Errorf("%s: query = %v, want since %d and until one hour later", c.date, q, since.Unix())
		}
	}
	for _, bad := range []string{"", "2026-13-01", "11-14-2026", "2026-11-31"} {
		if _, err := ParsePacificDay(bad); err == nil {
			t.Errorf("ParsePacificDay(%q) accepted", bad)
		}
	}
}

func TestAccountTotalsDayRefusals(t *testing.T) {
	day := mustPacificDay(t, "2026-11-14")
	for name, body := range map[string]string{
		"no metrics":      `{"data":[]}`,
		"a metric absent": `{"data":[{"name":"likes","total_value":{"value":5}},{"name":"replies","total_value":{"value":1}},{"name":"reposts","total_value":{"value":0}}]}`,
		"no total":        `{"data":[{"name":"likes"},{"name":"replies","total_value":{"value":1}},{"name":"reposts","total_value":{"value":0}},{"name":"quotes","total_value":{"value":0}}]}`,
		"negative total":  `{"data":[{"name":"likes","total_value":{"value":-2}},{"name":"replies","total_value":{"value":1}},{"name":"reposts","total_value":{"value":0}},{"name":"quotes","total_value":{"value":0}}]}`,
	} {
		f, api, _ := newFake(t)
		f.route("GET", accountPath, body)
		if _, err := api.ReadAccountTotalsDay(context.Background(), day); !errors.Is(err, ErrMetricsUnavailable) {
			t.Errorf("%s: err = %v, want ErrMetricsUnavailable", name, err)
		}
	}
	f, api, _ := newFake(t)
	f.route("GET", accountPath, "error.json")
	f.headers = map[string]string{"GET " + accountPath: `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":12}]}`}
	var limited *RateLimitedError
	if _, err := api.ReadAccountTotalsDay(context.Background(), day); !errors.As(err, &limited) || limited.Wait != 12*time.Minute {
		t.Errorf("failed read under a wait: %v", err)
	}
	f, api, _ = newFake(t)
	f.route("GET", accountPath, `{"data":[]}`)
	f.headers = map[string]string{"GET " + accountPath: `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":9}]}`}
	if _, err := api.ReadAccountTotalsDay(context.Background(), day); !errors.Is(err, ErrMetricsUnavailable) || !errors.As(err, &limited) || limited.Wait != 9*time.Minute {
		t.Errorf("refused read under a wait: %v", err)
	}
	f, api, _ = newFake(t)
	f.route("GET", accountPath, `{"data":[{"name":"likes","total_value":{"value":-1}}]}`)
	f.headers = map[string]string{"GET " + accountPath: `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":4}]}`}
	if _, err := api.ReadAccountTotalsDay(context.Background(), day); !errors.As(err, &limited) || limited.Wait != 4*time.Minute {
		t.Errorf("invalid total under a wait: %v", err)
	}
	f, api, _ = newFake(t)
	f.route("GET", accountPath, `{"data":[]}`)
	f.headers = map[string]string{"GET " + accountPath: `{"1000000000000001":[{"type":"threads","estimated_time_to_regain_access":6}]}`}
	if _, err := api.ReadAccountInsights(context.Background(), day.StartsAt, day.EndsAt); !errors.As(err, &limited) || limited.Wait != 6*time.Minute {
		t.Errorf("refused views read under a wait: %v", err)
	}
}

func TestAccountInsightsRefusals(t *testing.T) {
	ctx := context.Background()
	since, until := time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	for name, body := range map[string]string{
		"no metrics":     `{"data":[]}`,
		"clicks only":    `{"data":[{"name":"clicks","link_total_values":[{"value":1,"link_url":"u"}]}]}`,
		"no view days":   `{"data":[{"name":"views","values":[]}]}`,
		"bad end_time":   `{"data":[{"name":"views","values":[{"value":1,"end_time":"yesterday"}]}]}`,
		"negative views": `{"data":[{"name":"views","values":[{"value":-1,"end_time":"2026-11-14T08:00:00+0000"}]}]}`,
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
