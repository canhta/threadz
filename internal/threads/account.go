package threads

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	// The account series is cut at midnight America/Los_Angeles; the zone must
	// load on a host without zoneinfo.
	_ "time/tzdata"
)

const (
	// AccountTrailingDays is how many closed Pacific days each account read
	// after the backfill covers.
	AccountTrailingDays = 3
	// AccountBackfillDays is the window of the first read. The API serves up to
	// 2 years and one call returned 700 daily buckets.
	AccountBackfillDays = 700

	accountInsightMetrics = "views,clicks"
)

// AccountTotalMetrics are the metrics a one-day read returns as `total_value`.
// `views` is not among them: its buckets are not totals.
var AccountTotalMetrics = []string{"likes", "replies", "reposts", "quotes"}

// pacific cannot fail to load: time/tzdata embeds the zone.
var pacific, _ = time.LoadLocation("America/Los_Angeles")

// AccountWindow is the since..until of a read of the last days closed Pacific
// days at now. The unfinished current day is not in it.
func AccountWindow(now time.Time, days int) (since, until time.Time) {
	n := now.In(pacific)
	end := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, pacific)
	return end.AddDate(0, 0, -days).UTC(), end.UTC()
}

// NextPacificMidnight is the first Pacific midnight after t: the moment a
// Pacific day closes and the account series gains a bucket.
func NextPacificMidnight(t time.Time) time.Time {
	y, m, d := t.In(pacific).Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, pacific).UTC()
}

// PacificDay is one calendar day of America/Los_Angeles as the account series
// counts it: Day is its date and the day runs from StartsAt to EndsAt.
type PacificDay struct {
	Day              string
	StartsAt, EndsAt time.Time
}

// PacificDays lists the Pacific days from since to until, both at midnight
// Pacific.
func PacificDays(since, until time.Time) []PacificDay {
	var out []PacificDay
	for start := since.In(pacific); start.Before(until); {
		y, m, d := start.Date()
		end := time.Date(y, m, d+1, 0, 0, 0, 0, pacific)
		out = append(out, PacificDay{Day: start.Format("2006-01-02"), StartsAt: start.UTC(), EndsAt: end.UTC()})
		start = end
	}
	return out
}

// ParsePacificDay is the Pacific day of a YYYY-MM-DD date.
func ParsePacificDay(date string) (PacificDay, error) {
	start, err := time.ParseInLocation(time.DateOnly, date, pacific)
	if err != nil {
		return PacificDay{}, fmt.Errorf("parsing Pacific day: %w", err)
	}
	return PacificDays(start, start.AddDate(0, 0, 1))[0], nil
}

// DayViews is the views of the Pacific day that closed at EndsAt.
type DayViews struct {
	EndsAt time.Time
	Value  int64
}

// LinkClicks is the clicks one link received.
type LinkClicks struct {
	URL   string
	Value int64
}

// AccountInsights is one read of the account's `threads_insights` over
// Since..Until. Views is a daily series; Clicks are per-link totals.
// RegainAfter is the wait the API's usage header asks for; zero when it asks
// for none.
type AccountInsights struct {
	Since, Until time.Time
	Views        []DayViews
	Clicks       []LinkClicks
	RegainAfter  time.Duration
}

// DayTotals is the likes, replies, reposts and quotes of one Pacific day.
// RegainAfter is as in AccountInsights.
type DayTotals struct {
	PacificDay
	Totals      map[string]int64
	RegainAfter time.Duration
}

// RateLimitedError is a failed read made while the usage header asked for a
// wait.
type RateLimitedError struct {
	Wait time.Duration
	Err  error
}

func (e *RateLimitedError) Error() string { return "threads rate limited: " + e.Err.Error() }
func (e *RateLimitedError) Unwrap() error { return e.Err }

// ReadAccountInsights reads the account's views and clicks over since..until in
// one request.
func (a *API) ReadAccountInsights(ctx context.Context, since, until time.Time) (AccountInsights, error) {
	var in insights
	q := url.Values{"metric": {accountInsightMetrics}, "since": {strconv.FormatInt(since.Unix(), 10)}, "until": {strconv.FormatInt(until.Unix(), 10)}}
	header, err := a.doHeader(ctx, http.MethodGet, "/"+a.o.UserID+"/threads_insights", q, &in)
	if err != nil {
		return AccountInsights{}, err
	}
	out := AccountInsights{Since: since, Until: until, RegainAfter: usageWait(header)}
	for _, d := range in.Data {
		switch d.Name {
		case "views":
			for _, p := range d.Values {
				end, err := parseTimestamp(p.EndTime)
				if err != nil || end.IsZero() || p.Value < 0 || p.Value > snapshotMaxMetric {
					return AccountInsights{}, refusal(out.RegainAfter)
				}
				out.Views = append(out.Views, DayViews{EndsAt: end, Value: p.Value})
			}
		case "clicks":
			for _, l := range d.LinkTotals {
				if l.Value < 0 || l.Value > snapshotMaxMetric {
					return AccountInsights{}, refusal(out.RegainAfter)
				}
				out.Clicks = append(out.Clicks, LinkClicks{URL: l.URL, Value: l.Value})
			}
		}
	}
	if len(out.Views) == 0 {
		return AccountInsights{}, refusal(out.RegainAfter)
	}
	return out, nil
}

// ReadAccountTotalsDay reads the account's likes, replies, reposts and quotes
// of the one Pacific day. The API totals every bucket whose end_time lies in
// [since, until], both ends inclusive, and the bucket of a day ends at the
// midnight that closes it, so the window is that midnight plus one hour. A
// response without all four is refused.
func (a *API) ReadAccountTotalsDay(ctx context.Context, day PacificDay) (DayTotals, error) {
	var in insights
	q := url.Values{"metric": {strings.Join(AccountTotalMetrics, ",")}, "since": {strconv.FormatInt(day.EndsAt.Unix(), 10)}, "until": {strconv.FormatInt(day.EndsAt.Add(time.Hour).Unix(), 10)}}
	header, err := a.doHeader(ctx, http.MethodGet, "/"+a.o.UserID+"/threads_insights", q, &in)
	if err != nil {
		return DayTotals{}, err
	}
	out := DayTotals{PacificDay: day, Totals: map[string]int64{}, RegainAfter: usageWait(header)}
	for _, d := range in.Data {
		if !slices.Contains(AccountTotalMetrics, d.Name) {
			continue
		}
		if d.TotalValue == nil || d.TotalValue.Value < 0 || d.TotalValue.Value > snapshotMaxMetric {
			return DayTotals{}, refusal(out.RegainAfter)
		}
		out.Totals[d.Name] = d.TotalValue.Value
	}
	if len(out.Totals) != len(AccountTotalMetrics) {
		return DayTotals{}, refusal(out.RegainAfter)
	}
	return out, nil
}

// refusal is ErrMetricsUnavailable for a response the client cannot use. When
// the usage header asked for a wait, it is a RateLimitedError carrying it.
func refusal(wait time.Duration) error {
	if wait > 0 {
		return &RateLimitedError{Wait: wait, Err: ErrMetricsUnavailable}
	}
	return ErrMetricsUnavailable
}

// usageWait is the longest estimated_time_to_regain_access, in minutes, in the
// x-business-use-case-usage header.
func usageWait(h http.Header) time.Duration {
	var usage map[string][]struct {
		Regain float64 `json:"estimated_time_to_regain_access"`
	}
	if json.Unmarshal([]byte(h.Get("X-Business-Use-Case-Usage")), &usage) != nil {
		return 0
	}
	var longest float64
	for _, entries := range usage {
		for _, e := range entries {
			longest = max(longest, e.Regain)
		}
	}
	return time.Duration(longest * float64(time.Minute))
}
