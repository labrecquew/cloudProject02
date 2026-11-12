package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func metricValue(t *testing.T, m *appMetrics, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.Metric {
			matched := true
			for key, value := range labels {
				found := false
				for _, label := range sample.Label {
					if label.GetName() == key && label.GetValue() == value {
						found = true
					}
				}
				matched = matched && found
			}
			if !matched {
				continue
			}
			if sample.Counter != nil {
				return sample.Counter.GetValue(), true
			}
			if sample.Gauge != nil {
				return sample.Gauge.GetValue(), true
			}
			if sample.Histogram != nil {
				return float64(sample.Histogram.GetSampleCount()), true
			}
		}
	}
	return 0, false
}

func TestMetricsCountCompletionOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := newTestJobService(ctx, providerFunc(func(_ context.Context, input ForecastRequest) (Forecast, error) {
		f := testForecast(input)
		f.Points[0].TemperatureCelsius = 35
		f.Points[0].WindMetersPerSec = 15
		return f, nil
	}), 1, 2)
	defer func() { cancel(); service.workers.Wait() }()
	job, err := service.submit(ForecastRequest{"02108", 1})
	if err != nil {
		t.Fatal(err)
	}
	finished := waitJob(t, service, job.ID)
	if finished.Status != "completed" {
		t.Fatal(finished.Error)
	}
	if err := service.evaluateEvent(forecastEvent{1, job.ID, *finished.Forecast}, *finished.Kafka); err != nil {
		t.Fatal(err)
	}
	service.fail(job.ID, errors.New("late publication error"))
	m := service.metrics
	for _, check := range []struct{ got, want float64 }{
		{testutil.ToFloat64(m.accepted), 1},
		{testutil.ToFloat64(m.finished.WithLabelValues("completed")), 1},
		{testutil.ToFloat64(m.finished.WithLabelValues("failed")), 0},
		{testutil.ToFloat64(m.alerts.WithLabelValues("high_temperature")), 1},
		{testutil.ToFloat64(m.alerts.WithLabelValues("high_wind")), 1},
		{testutil.ToFloat64(m.ignored.WithLabelValues("duplicate")), 1},
	} {
		if check.got != check.want {
			t.Fatalf("got %v want %v", check.got, check.want)
		}
	}
	if count, _ := metricValue(t, m, "weather_request_duration_seconds", map[string]string{"status": "completed"}); count != 1 {
		t.Fatal("completion latency counted more than once")
	}
	if count, _ := metricValue(t, m, "weather_queue_wait_duration_seconds", nil); count != 1 {
		t.Fatal("queue wait not recorded")
	}
	if value, _ := metricValue(t, m, "weather_forecast_temperature_celsius", map[string]string{"zip": "02108", "step": "1"}); value != 35 {
		t.Fatal("wrong weather measurement")
	}
	if value, _ := metricValue(t, m, "weather_forecast_valid_time_seconds", map[string]string{"zip": "02108", "step": "1"}); value != float64(finished.Forecast.Points[0].Time.Unix()) {
		t.Fatal("forecast time confused with scrape time")
	}
	cancel()
	service.workers.Wait()
	if testutil.ToFloat64(m.active) != 0 {
		t.Fatal("active workers did not return to zero")
	}
}

func TestMetricsCountFailureOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := newTestJobService(ctx, providerFunc(func(context.Context, ForecastRequest) (Forecast, error) {
		return Forecast{}, errors.New("provider failure")
	}), 1, 1)
	defer func() { cancel(); service.workers.Wait() }()
	job, err := service.submit(ForecastRequest{"10001", 1})
	if err != nil {
		t.Fatal(err)
	}
	if waitJob(t, service, job.ID).Status != "failed" {
		t.Fatal("job did not fail")
	}
	service.fail(job.ID, errors.New("duplicate failure"))
	if testutil.ToFloat64(service.metrics.finished.WithLabelValues("failed")) != 1 {
		t.Fatal("failure counted more than once")
	}
	if testutil.ToFloat64(service.metrics.alerts.WithLabelValues("high_temperature")) != 0 {
		t.Fatal("failed job counted an alert")
	}
}

func TestMetricsHTTPLabelsDoNotContainRequestIDs(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job), metrics: newAppMetrics(make(chan string, 2), alertRules{defaultHighTempC, defaultLowTempC, defaultHighWindMPS})}
	handler := routes(service, false)
	for _, path := range []string{"/forecasts/private-id-A", "/forecasts/private-id-B", "/arbitrary-secret-path"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("invalid scrape response: %d", w.Code)
	}
	for _, text := range []string{"private-id-A", "private-id-B", "arbitrary-secret-path", "OPENWEATHER_API_KEY"} {
		if strings.Contains(w.Body.String(), text) {
			t.Fatalf("metric leaked unbounded/private label %s", text)
		}
	}
	if value, _ := metricValue(t, service.metrics, "weather_http_requests_total", map[string]string{"route": "lookup", "method": "GET", "status": "404"}); value != 2 {
		t.Fatal("lookups were not grouped under one route")
	}
	if strings.Contains(w.Body.String(), `route="metrics"`) {
		t.Fatal("scrape request counted itself")
	}
}

func TestMetricsQueueAndCapacityRejection(t *testing.T) {
	queue := make(chan string, 1)
	service := &jobService{jobs: make(map[string]Job), queue: queue, maxJobs: 2, metrics: newAppMetrics(queue, alertRules{35, 0, 15})}
	if _, err := service.submit(ForecastRequest{"10001", 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.submit(ForecastRequest{"10001", 1}); !errors.Is(err, errBusy) {
		t.Fatal("queue did not reject overflow")
	}
	if testutil.ToFloat64(service.metrics.accepted) != 1 || testutil.ToFloat64(service.metrics.rejected) != 1 {
		t.Fatal("capacity counters wrong")
	}
	if value, _ := metricValue(t, service.metrics, "weather_queue_depth", nil); value != 1 {
		t.Fatal("queue depth wrong")
	}
}

func TestForecastMetricsBoundedReplacedAndExpired(t *testing.T) {
	m := newAppMetrics(nil, alertRules{35, 0, 15})
	now := time.Now().Add(-time.Minute)
	for i := 0; i < maxMetricLocations+2; i++ {
		f := testForecast(ForecastRequest{fmt.Sprintf("%05d", i), 5})
		point := f.Points[0]
		for len(f.Points) < 50 {
			point.Time = point.Time.Add(time.Hour)
			f.Points = append(f.Points, point)
		}
		m.recordForecast(f, nil, now.Add(time.Duration(i)*time.Second))
	}
	if len(m.forecasts) != maxMetricLocations {
		t.Fatal("location cap not respected")
	}
	if _, exists := metricValue(t, m, "weather_forecast_temperature_celsius", map[string]string{"zip": "00000"}); exists {
		t.Fatal("oldest location not evicted")
	}
	if _, exists := metricValue(t, m, "weather_forecast_temperature_celsius", map[string]string{"step": "41"}); exists {
		t.Fatal("forecast step labels are unbounded")
	}
	zip := fmt.Sprintf("%05d", maxMetricLocations+1)
	f := testForecast(ForecastRequest{zip, 1})
	m.recordForecast(f, nil, time.Now())
	if _, exists := metricValue(t, m, "weather_forecast_temperature_celsius", map[string]string{"zip": zip, "step": "2"}); exists {
		t.Fatal("shorter replacement kept stale points")
	}
	m.recordForecast(f, nil, time.Now().Add(-metricForecastTTL-time.Second))
	if _, exists := metricValue(t, m, "weather_forecast_temperature_celsius", map[string]string{"zip": zip}); exists {
		t.Fatal("stale weather snapshot not expired")
	}
}

func TestConcurrentMetricScrapesAndUpdates(t *testing.T) {
	m := newAppMetrics(nil, alertRules{35, 0, 15})
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for j := 0; j < 10; j++ {
				m.recordForecast(testForecast(ForecastRequest{fmt.Sprintf("%05d", i), 1}), nil, time.Now())
				if _, err := m.registry.Gather(); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	workers.Wait()
}

func TestProviderMetricsCountAttemptsWithoutLeakingKeys(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401); fmt.Fprint(w, "private-test-key") }))
	defer server.Close()
	m := newAppMetrics(nil, alertRules{35, 0, 15})
	provider := &openWeather{key: "private-test-key", base: server.URL, client: server.Client(), metrics: m}
	if _, err := provider.Fetch(context.Background(), ForecastRequest{"10001", 1}); err == nil {
		t.Fatal("expected provider failure")
	}
	if testutil.ToFloat64(m.providerCalls.WithLabelValues("geocoding", "error")) != 1 {
		t.Fatal("provider error not counted")
	}
	w := httptest.NewRecorder()
	m.handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(w.Body.String(), "private-test-key") {
		t.Fatal("key leaked in metrics")
	}
}
