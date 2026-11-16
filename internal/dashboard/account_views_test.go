package dashboard_test

import (
	"strings"
	"testing"
	"time"

	"github.com/canhta/threadz/internal/store"
)

func accountDay(day string, views *int64, attributed int64) store.DashboardAccountDay {
	return store.DashboardAccountDay{Day: day, Views: views, Attributed: attributed}
}

func n64(n int64) *int64 { return &n }

// accountSnapshot is measured() with the account series of the 7 days to
// 2026-11-12 (today, whose Pacific day has not closed) and the 7 before.
func accountSnapshot() store.DashboardSnapshot {
	s := measured()
	s.AccountDays = []store.DashboardAccountDay{
		accountDay("2026-11-11", n64(600), 60),
		accountDay("2026-11-10", n64(500), 50),
		accountDay("2026-11-09", n64(400), 40),
		accountDay("2026-11-08", nil, 0),
		accountDay("2026-11-07", n64(200), 20),
		accountDay("2026-11-06", n64(100), 10),
		accountDay("2026-11-04", n64(50), 5),
		accountDay("2026-11-03", n64(50), 5),
		accountDay("2026-11-02", n64(50), 5),
	}
	return s
}

func TestAccountViewsHeadlineIsTheSumOfTheAccountDays(t *testing.T) {
	page := mustPage(t, newHandler(newSource(accountSnapshot())))
	g := page.Growth
	// 100+200+400+500+600: the NULL day and today add nothing, and the own
	// actions' samples (300 views gained) are not the headline.
	if !g.HasViews || g.Views != 1800 {
		t.Fatalf("headline views = %d (has %v), want the sum 1800 of the account days", g.Views, g.HasViews)
	}
	if page.KPIs[0].Label != "Views in 7 days" || page.KPIs[0].Value != "1,800" {
		t.Fatalf("headline KPI = %+v", page.KPIs[0])
	}
	if g.ViewsDelta != "up 1100% on the previous 7 days" {
		t.Fatalf("delta = %q, want against the prior 7 days (150)", g.ViewsDelta)
	}
	if want := "180 of the 1,800 views came from your own posts and replies (10.0%)"; g.Reconciliation != want {
		t.Fatalf("reconciliation = %q, want %q", g.Reconciliation, want)
	}
	// The chart has a point per day; a NULL day is a gap, never a zero.
	var cells []string
	for _, row := range g.ViewsChart.Rows {
		cells = append(cells, row[1])
	}
	if got := strings.Join(cells, ","); got != "100,200,—,400,500,600,—" {
		t.Fatalf("chart cells = %s", got)
	}
	if g.Likes != 30 || len(g.Posts) != 1 || g.Posts[0].Views != "400" {
		t.Fatalf("per-post rows and engagement still come from the own actions: %+v", g)
	}
}

func TestAccountViewsWithoutAPriorPeriodOrAnyViews(t *testing.T) {
	s := accountSnapshot()
	s.AccountDays = s.AccountDays[:6]
	page := mustPage(t, newHandler(newSource(s)))
	if page.Growth.Views != 1800 || page.Growth.ViewsDelta != "" {
		t.Fatalf("without a prior period: views %d delta %q", page.Growth.Views, page.Growth.ViewsDelta)
	}

	s.AccountDays = []store.DashboardAccountDay{accountDay("2026-11-11", n64(0), 0), accountDay("2026-11-10", nil, 0)}
	page = mustPage(t, newHandler(newSource(s)))
	if !page.Growth.HasViews || page.Growth.Views != 0 || page.Growth.Reconciliation != "" || page.KPIs[0].Value != "0" {
		t.Fatalf("a day with 0 views: %+v", page.Growth)
	}

	s.AccountDays = []store.DashboardAccountDay{accountDay("2026-11-11", nil, 0)}
	page = mustPage(t, newHandler(newSource(s)))
	if page.Growth.HasViews || page.KPIs[0].Value != "—" {
		t.Fatalf("only missing days must show no figure: %+v", page.KPIs[0])
	}
}

// flatAccountDays is 100 views a day from newest back through 14 days.
func flatAccountDays(newest string) []store.DashboardAccountDay {
	end, _ := time.Parse("2006-01-02", newest)
	var days []store.DashboardAccountDay
	for i := range 14 {
		days = append(days, accountDay(end.AddDate(0, 0, -i).Format("2006-01-02"), n64(100), 10))
	}
	return days
}

func TestAccountViewsRangeIsTheLastCompletedPacificDays(t *testing.T) {
	s := measured()
	// Today is 2026-11-12 and has no bucket: a flat series must read flat, 7 days against 7.
	s.AccountDays = flatAccountDays("2026-11-11")
	g := mustPage(t, newHandler(newSource(s))).Growth
	if g.Views != 700 || g.ViewsDelta != "same as the previous 7 days" {
		t.Fatalf("flat series: views %d delta %q, want 700 and the same as the previous 7 days", g.Views, g.ViewsDelta)
	}
	// The newest closed day is not read yet: the range ends at the newest day that has a bucket.
	s.AccountDays = flatAccountDays("2026-11-10")
	g = mustPage(t, newHandler(newSource(s))).Growth
	if g.Views != 700 || g.ViewsDelta != "same as the previous 7 days" {
		t.Fatalf("series one day behind: views %d delta %q", g.Views, g.ViewsDelta)
	}
	// A bucket for a day that has not closed in Pacific time is not counted.
	s.AccountDays = append([]store.DashboardAccountDay{accountDay("2026-11-12", n64(5000), 0)}, flatAccountDays("2026-11-11")...)
	if g = mustPage(t, newHandler(newSource(s))).Growth; g.Views != 700 {
		t.Fatalf("today's bucket counted: views %d", g.Views)
	}
}

func withInteractions(days []store.DashboardAccountDay, by map[string]int64) []store.DashboardAccountDay {
	for i := range days {
		if n, ok := by[days[i].Day]; ok {
			days[i].Interactions = n64(n)
		}
	}
	return days
}

func TestInteractionsKPIChangeChartAndGapDay(t *testing.T) {
	s := accountSnapshot()
	// 2026-11-09 has views but no interactions: a gap, never a zero. 2026-11-08
	// has neither. The prior 7 days hold 3 interactions.
	s.AccountDays = withInteractions(s.AccountDays, map[string]int64{
		"2026-11-11": 6, "2026-11-10": 5, "2026-11-07": 2, "2026-11-06": 1,
		"2026-11-04": 1, "2026-11-03": 1, "2026-11-02": 1,
	})
	g := mustPage(t, newHandler(newSource(s))).Growth
	if !g.HasInteractions || g.Interactions != 14 || g.InteractionsDelta != "up 367% on the previous 7 days" {
		t.Fatalf("interactions = %d (has %v) delta %q, want 6+5+2+1 against 3", g.Interactions, g.HasInteractions, g.InteractionsDelta)
	}
	kpis := mustPage(t, newHandler(newSource(s))).KPIs
	if kpis[2].Label != "Interactions in 7 days" || kpis[2].Value != "14" || kpis[2].Note != g.InteractionsDelta || kpis[2].Tone != "up" {
		t.Fatalf("KPI = %+v", kpis[2])
	}
	if g.InteractionsChart.Title != "Interactions per day" {
		t.Fatalf("chart title = %q", g.InteractionsChart.Title)
	}
	var cells []string
	for _, row := range g.InteractionsChart.Rows {
		cells = append(cells, row[1])
	}
	if got := strings.Join(cells, ","); got != "1,2,—,—,5,6,—" {
		t.Fatalf("chart cells = %s", got)
	}

	s.AccountDays = accountSnapshot().AccountDays
	page := mustPage(t, newHandler(newSource(s)))
	if page.Growth.HasInteractions || page.KPIs[2].Value != "—" || page.KPIs[2].Note != "" {
		t.Fatalf("without any interactions: %+v", page.KPIs[2])
	}
}
