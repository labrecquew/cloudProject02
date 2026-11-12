package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func formRequest(body url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://example.com/request", strings.NewReader(body.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func TestWebRequestPendingThenCompletedWithoutResubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var service *jobService
	provider := providerFunc(func(ctx context.Context, input ForecastRequest) (Forecast, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return Forecast{}, ctx.Err()
		}
		f := testForecast(input)
		f.Location = `<script>alert("location")</script>`
		f.WindowStart = time.Date(2026, time.July, 2, 0, 0, 0, 0, time.UTC)
		f.WindowEnd = f.WindowStart.Add(24 * time.Hour)
		f.Points[0].Time = f.WindowStart.Add(time.Hour)
		f.Points[0].TemperatureCelsius = 40
		f.Points[0].WindMetersPerSec = 18
		f.Points = append(f.Points, ForecastPoint{
			Time:               f.WindowStart.Add(4 * time.Hour),
			TemperatureCelsius: 21,
			WindMetersPerSec:   5,
		})
		return f, nil
	})
	publisher := publisherFunc(func(_ context.Context, event forecastEvent) error {
		return service.evaluateEvent(event, kafkaReceipt{"test", 0, 1})
	})
	service = newJobService(ctx, provider, 1, 2, publisher, alertRules{defaultHighTempC, defaultLowTempC, defaultHighWindMPS})
	defer func() { cancel(); service.workers.Wait() }()
	handler := routes(service, true)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, formRequest(url.Values{"zip": {"02108"}, "days": {"1"}}))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("form returned %d: %s", w.Code, w.Body.String())
	}
	location := w.Header().Get("Location")
	if !strings.HasPrefix(location, "/requests/") {
		t.Fatal("form did not redirect to HTML result")
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, location, nil))
	if !strings.Contains(w.Body.String(), `http-equiv="refresh"`) || !strings.Contains(w.Body.String(), "Retrieving your weather forecast") {
		t.Fatal("pending page did not refresh or show status")
	}
	close(release)
	id := strings.TrimPrefix(location, "/requests/")
	if job := waitJob(t, service, id); job.Status != "completed" {
		t.Fatal(job.Error)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://example.com"+location, nil))
	body := w.Body.String()
	for _, text := range []string{"104.0", "40.3", "100.0", "20.0", "40.0", "High temperature", "High wind", `value="02108"`, "var-zip=02108", `title="Temperature forecast"`, `title="Wind forecast"`, "panelId=8", "panelId=9", "var-forecast_request=" + id, "Eastern time", "Jul 1 – Jul 2", "timezone=America%2FNew_York", "<summary>Wednesday, July 1</summary>", "<summary>Thursday, July 2</summary>", "12:00 AM EDT", "Wed, Jul 1 · 9:00 PM EDT"} {
		if !strings.Contains(body, text) {
			t.Fatalf("result missing %q", text)
		}
	}
	if strings.Contains(body, `http-equiv="refresh"`) {
		t.Fatal("completed page still refreshes")
	}
	if strings.Contains(body, "<script>") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatal("provider location was not escaped")
	}
	service.mu.Lock()
	count := len(service.jobs)
	service.mu.Unlock()
	if count != 1 {
		t.Fatal("status refresh submitted another job")
	}
}

func TestWebValidationMissingKeyAndCrossOrigin(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job), rules: alertRules{defaultHighTempC, defaultLowTempC, defaultHighWindMPS}}
	for _, input := range []url.Values{{"zip": {"1234"}, "days": {"1"}}, {"zip": {"10001"}, "days": {"6"}}, {"zip": {"10001"}, "days": {"wrong"}}, {"zip": {"<script>alert(1)</script>"}, "days": {"1"}}} {
		w := httptest.NewRecorder()
		routes(service, true).ServeHTTP(w, formRequest(input))
		if w.Code != 400 || strings.Contains(w.Body.String(), "<script>") {
			t.Fatalf("unsafe/invalid validation response %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	routes(service, false).ServeHTTP(w, formRequest(url.Values{"zip": {"10001"}, "days": {"1"}}))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "temporarily unavailable") {
		t.Fatal("missing key was not handled")
	}
	r := formRequest(url.Values{"zip": {"10001"}, "days": {"1"}})
	r.Header.Set("Origin", "https://unrelated.example")
	w = httptest.NewRecorder()
	routes(service, true).ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("cross-origin submission returned %d", w.Code)
	}
	if len(service.jobs) != 0 {
		t.Fatal("invalid requests submitted jobs")
	}
}

func TestWebAssetsAboutAndMissingResults(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job), rules: alertRules{defaultHighTempC, defaultLowTempC, defaultHighWindMPS}}
	handler := routes(service, false)
	for _, tc := range []struct {
		path, content string
		status        int
	}{
		{"/", "Request a forecast", 200},
		{"/static/style.css", "grid-template-columns", 200},
		{"/about", "Topaloglu", 200},
		{"/requests/missing", "no longer available", 404},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.content) {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
}

func TestDashboardLinksUsePublicHostAndConfiguredSubpath(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://cloud.example:8080/", nil)
	if got := dashboardLink(r, "weather-forecasts", "02108"); got != "http://cloud.example:3000/d/weather-forecasts/weather-forecasts?var-zip=02108" {
		t.Fatal(got)
	}
	t.Setenv("GRAFANA_PUBLIC_URL", "https://charts.example/grafana/")
	if got := dashboardLink(r, "weather-forecasts", "02108"); got != "https://charts.example/grafana/d/weather-forecasts/weather-forecasts?var-zip=02108" {
		t.Fatal(got)
	}
	t.Setenv("GRAFANA_PUBLIC_URL", "javascript:alert(1)")
	if got := dashboardLink(r, "weather-pipeline", ""); strings.HasPrefix(got, "javascript:") {
		t.Fatal("unsafe dashboard link")
	}
}

func TestWebRouteMetricsDoNotLabelRequestIDs(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job), metrics: newAppMetrics(nil, alertRules{defaultHighTempC, defaultLowTempC, defaultHighWindMPS})}
	handler := routes(service, false)
	for _, id := range []string{"private-A", "private-B"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/requests/"+id, nil))
	}
	if value, _ := metricValue(t, service.metrics, "weather_http_requests_total", map[string]string{"route": "web_result", "status": "404"}); value != 2 {
		t.Fatal("web lookups were not grouped under a fixed route")
	}
}
