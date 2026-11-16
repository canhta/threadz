package dashboard

import (
	"fmt"
	"github.com/canhta/threadz/internal/config"
	"github.com/canhta/threadz/internal/store"
	"github.com/canhta/threadz/internal/threads"
	"math"
	"sort"
	"time"
)

// accountSeries is the account's daily views by Pacific date and, for each of
// those days, the views the agent's own actions gained, and the day's
// interactions (likes, replies, reposts and quotes). A day the account has no
// value for is absent, never 0. A chart day is matched to the Pacific date
// with the same calendar date, so a schedule zone far from Pacific time shifts
// a chart point by up to a day; the headline range is in Pacific days.
type accountSeries struct {
	views, attributed, interactions map[string]int64
	// days holds the dates that have a value, newest first.
	days []string
}

func accountFrom(days []store.DashboardAccountDay) accountSeries {
	a := accountSeries{views: map[string]int64{}, attributed: map[string]int64{}, interactions: map[string]int64{}}
	for _, d := range days {
		if d.Interactions != nil {
			a.interactions[d.Day] = *d.Interactions
		}
		if d.Views != nil {
			a.views[d.Day], a.attributed[d.Day] = *d.Views, d.Attributed
			a.days = append(a.days, d.Day)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(a.days)))
	return a
}

// rangeStart is the first of the n Pacific days that end the headline range:
// the newest day with a value that has closed at now. Today's Pacific day and
// a closed day not read yet are not in it.
func (a accountSeries) rangeStart(n int, now time.Time) (time.Time, bool) {
	since, until := threads.AccountWindow(now, 1)
	closed := threads.PacificDays(since, until)[0].Day
	for _, day := range a.days {
		if day <= closed {
			end, err := time.Parse("2006-01-02", day)
			return end.AddDate(0, 0, 1-n), err == nil
		}
	}
	return time.Time{}, false
}

// total sums the days from from up to but excluding to; ok is false when none
// of them has a value.
func (a accountSeries) total(from, to time.Time) (views, attributed int64, ok bool) {
	for d := from; d.Before(to); d = d.AddDate(0, 0, 1) {
		key := d.Format("2006-01-02")
		if v, has := a.views[key]; has {
			views, attributed, ok = views+v, attributed+a.attributed[key], true
		}
	}
	return views, attributed, ok
}

// aligned reports whether the day has views and all four interaction totals.
func (a accountSeries) aligned(day string) bool {
	_, hasViews := a.views[day]
	_, hasInteractions := a.interactions[day]
	return hasViews && hasInteractions
}

// totalInteractions sums the interactions of the aligned days from from up to
// but excluding to and counts them.
func (a accountSeries) totalInteractions(from, to time.Time) (n int64, counted int) {
	for d := from; d.Before(to); d = d.AddDate(0, 0, 1) {
		if key := d.Format("2006-01-02"); a.aligned(key) {
			n += a.interactions[key]
			counted++
		}
	}
	return n, counted
}

// interactionsPairs sums the interactions of the days that are aligned in both
// the days-day period starting at start and the one before it, matching the two
// periods day by day, and counts the pairs. Both sums cover the same days, so
// their change compares like with like.
func (a accountSeries) interactionsPairs(start time.Time, days int) (cur, prior int64, pairs int) {
	for i := range days {
		c, p := start.AddDate(0, 0, i), start.AddDate(0, 0, i-days)
		ck, pk := c.Format("2006-01-02"), p.Format("2006-01-02")
		if a.aligned(ck) && a.aligned(pk) {
			cur, prior, pairs = cur+a.interactions[ck], prior+a.interactions[pk], pairs+1
		}
	}
	return cur, prior, pairs
}

// chartSum sums m over the days from from up to but excluding to; ok is false
// when any of them has no value, so a chart point over several days with a
// missing one is a gap and never a smaller value.
func chartSum(m map[string]int64, from, to time.Time) (n int64, ok bool) {
	for d := from; d.Before(to); d = d.AddDate(0, 0, 1) {
		v, has := m[d.Format("2006-01-02")]
		if !has {
			return 0, false
		}
		n += v
	}
	return n, true
}

// metricSample is one cumulative measurement of an own publication. Views and
// engagement counters only grow, so a window is the difference between the
// latest sample at each end. When an action predates the window but has no
// sample before it, the first sample inside the window is used as the baseline
// rather than zero, which would overstate growth after a collection gap.
type metricSample struct {
	ActionID   int64
	PostedAt   time.Time
	MeasuredAt time.Time
	Views      int64
	Likes      int64
	Replies    int64
	Reposts    int64
	Quotes     int64
	Followers  *int64
}

type counts struct {
	Views, Likes, Replies, Reposts int64
}

func (c counts) add(o counts) counts {
	return counts{c.Views + o.Views, c.Likes + o.Likes, c.Replies + o.Replies, c.Reposts + o.Reposts}
}

func (c counts) subClamped(o counts) counts {
	return counts{clamp0(c.Views - o.Views), clamp0(c.Likes - o.Likes), clamp0(c.Replies - o.Replies), clamp0(c.Reposts - o.Reposts)}
}

func clamp0(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

func latestAt(samples []metricSample, from, to time.Time) (counts, bool) {
	var (
		end   *metricSample
		base  *metricSample
		first *metricSample
	)
	for i := range samples {
		s := &samples[i]
		if !s.MeasuredAt.After(to) {
			end = s
		}
		if s.MeasuredAt.Before(from) {
			base = s
		} else if first == nil && s.MeasuredAt.Before(to) {
			first = s
		}
	}
	if end == nil {
		return counts{}, false
	}
	if base == nil && samples[0].PostedAt.Before(from) && first != nil {
		base = first
	}
	out := endCounts(*end)
	if base != nil {
		out = out.subClamped(endCounts(*base))
	}
	return out, true
}

func endCounts(s metricSample) counts { return counts{s.Views, s.Likes, s.Replies, s.Reposts} }

// earnedBetween sums the growth of each action inside [from,to].
func earnedBetween(samples []metricSample, from, to time.Time) counts {
	byAction := map[int64][]metricSample{}
	order := []int64{}
	for _, s := range samples {
		if _, ok := byAction[s.ActionID]; !ok {
			order = append(order, s.ActionID)
		}
		byAction[s.ActionID] = append(byAction[s.ActionID], s)
	}
	var total counts
	for _, id := range order {
		group := byAction[id]
		sort.Slice(group, func(i, j int) bool { return group[i].MeasuredAt.Before(group[j].MeasuredAt) })
		if c, ok := latestAt(group, from, to); ok {
			total = total.add(c)
		}
	}
	return total
}

func dayStarts(l config.LimitsConfig, now time.Time, days int) []time.Time {
	day, _, _ := l.DayBounds(now)
	out := make([]time.Time, days)
	for i := range out {
		out[i] = day.AddDate(0, 0, -(days - 1 - i))
	}
	return out
}

// countDelta describes the change between two period totals in words.
func countDelta(cur, prior int64, period string) (string, bool) {
	if prior <= 0 {
		return "no earlier period to compare", false
	}
	if cur == prior {
		return "same as the previous " + period, true
	}
	change := (float64(cur) - float64(prior)) / float64(prior) * 100
	if change > 0 {
		return "up " + formatSignedPercent(change) + " on the previous " + period, true
	}
	return "down " + formatSignedPercent(-change) + " on the previous " + period, true
}

// countDeltaShort is the compact form used in the four secondary stat cells;
// the full "on the previous period" phrase appears once on the views total.
func countDeltaShort(cur, prior int64) string {
	if prior <= 0 {
		return ""
	}
	if cur == prior {
		return "no change"
	}
	change := (float64(cur) - float64(prior)) / float64(prior) * 100
	if change > 0 {
		return "+" + formatSignedPercent(change)
	}
	return "-" + formatSignedPercent(-change)
}

func formatSignedPercent(v float64) string {
	if v >= 10 || v == math.Trunc(v) {
		return fmt.Sprintf("%.0f%%", v)
	}
	return fmt.Sprintf("%.1f%%", v)
}

// niceMax rounds a value up to a readable 1/2/5 x 10^n axis maximum.
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	exp := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if v <= m*exp {
			return m * exp
		}
	}
	return 10 * exp
}

const (
	chartW = 600.0
	chartH = 160.0
)

// lineGeometry converts daily values into an SVG path. A nil entry breaks the
// line so a day with no denominator is a gap, never a fake zero.
func lineGeometry(values []*float64, max float64) (path string, axisMax float64) {
	if len(values) == 0 {
		return "", 1
	}
	peak := 0.0
	for _, v := range values {
		if v != nil && *v > peak {
			peak = *v
		}
	}
	axisMax = niceMax(peak)
	step := chartW / float64(maxInt(1, len(values)-1))
	open := false
	var b []byte
	for i, v := range values {
		if v == nil {
			open = false
			continue
		}
		x := float64(i) * step
		y := chartH - (*v/axisMax)*chartH
		if y < 0 {
			y = 0
		}
		if y > chartH {
			y = chartH
		}
		if !open {
			b = append(b, fmt.Sprintf("M%.1f,%.1f", x, y)...)
			open = true
		} else {
			b = append(b, fmt.Sprintf("L%.1f,%.1f", x, y)...)
		}
	}
	return string(b), axisMax
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// barGeometry stacks daily phase values into rects. It returns the axis max so
// the template can show a readable top tick and a budget line.
func barGeometry(rows [][]float64, max float64) (rects []ChartRect, axisMax float64) {
	peak := max
	for _, row := range rows {
		total := 0.0
		for _, v := range row {
			total += v
		}
		if total > peak {
			peak = total
		}
	}
	axisMax = niceMax(peak)
	if len(rows) == 0 {
		return nil, axisMax
	}
	step := chartW / float64(len(rows))
	width := step * 0.62
	classes := []string{"wake", "dream"}
	for i, row := range rows {
		base := chartH
		x := float64(i)*step + (step-width)/2
		for j, v := range row {
			if v <= 0 {
				continue
			}
			h := (v / axisMax) * chartH
			base -= h
			rects = append(rects, ChartRect{X: math.Round(x*10) / 10, Y: math.Round(base*10) / 10, W: math.Round(width*10) / 10, H: math.Round(h*10) / 10, Class: classes[j]})
		}
	}
	return rects, axisMax
}

// formatValue prints an axis tick without trailing noise.
func formatValue(v float64) string {
	if v == 0 {
		return "0"
	}
	switch {
	case v >= 1000:
		return formatCount(int64(math.Round(v)))
	case v >= 10:
		return fmt.Sprintf("%.0f", v)
	case v >= 1:
		if math.Abs(v-math.Round(v)) < 0.05 {
			return fmt.Sprintf("%.0f", v)
		}
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}
