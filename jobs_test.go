package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type providerFunc func(context.Context, ForecastRequest) (Forecast, error)

func (f providerFunc) Fetch(ctx context.Context, input ForecastRequest) (Forecast, error) {
	return f(ctx, input)
}

func waitJob(t *testing.T, service *jobService, id string) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, _ := service.lookup(id)
		if job.Status == "completed" || job.Status == "failed" {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("request did not finish")
	return Job{}
}

func TestConcurrentHTTPRequestsStayIsolated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan ForecastRequest, 4)
	release := make(chan struct{})
	service := newTestJobService(ctx, providerFunc(func(ctx context.Context, input ForecastRequest) (Forecast, error) {
		entered <- input
		select {
		case <-release:
			return testForecast(input), nil
		case <-ctx.Done():
			return Forecast{}, ctx.Err()
		}
	}), 4, 16)
	defer func() { cancel(); service.workers.Wait() }()
	handler := routes(service, true)
	inputs := []string{`{"zip":"02108","days":1}`, `{"zip":"10001","days":2}`, `{"zip":"90210","days":3}`, `{"zip":"12601","days":5}`}
	jobs := make(chan Job, 4)
	var requests sync.WaitGroup
	for _, body := range inputs {
		requests.Add(1)
		go func(body string) {
			defer requests.Done()
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/forecasts", strings.NewReader(body)))
			if w.Code != http.StatusAccepted {
				t.Errorf("submit: HTTP %d: %s", w.Code, w.Body.String())
				return
			}
			var job Job
			if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
				t.Error(err)
				return
			}
			jobs <- job
		}(body)
	}
	requests.Wait()
	// All four providers must start before any can finish. This proves overlap,
	// rather than merely testing four requests in sequence.
	for range inputs {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not process requests concurrently")
		}
	}
	close(release)
	close(jobs)
	ids := map[string]bool{}
	for submitted := range jobs {
		if ids[submitted.ID] {
			t.Fatal("duplicate request ID")
		}
		ids[submitted.ID] = true
		finished := waitJob(t, service, submitted.ID)
		if finished.Status != "completed" || finished.Forecast.ZIP != submitted.Request.ZIP || finished.Forecast.Days != submitted.Request.Days {
			t.Fatalf("result mixed up: %+v", finished)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/forecasts/"+submitted.ID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("lookup returned %d", w.Code)
		}
	}
	if len(ids) != 4 {
		t.Fatalf("got %d jobs, want four", len(ids))
	}
}

func TestValidationAndMissingKey(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job)}
	for _, body := range []string{
		`{"zip":"1234","days":1}`, `{"zip":"12a45","days":1}`,
		`{"zip":12345,"days":1}`, `{"zip":"10001","days":0}`,
		`{"zip":"10001","days":6}`, `{"zip":"10001","days":1.5}`,
		`{"zip":"10001","days":1,"extra":true}`, `{"zip":"10001","days":1} {}`, `null`,
	} {
		w := httptest.NewRecorder()
		routes(service, true).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/forecasts", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s returned %d", body, w.Code)
		}
	}
	w := httptest.NewRecorder()
	routes(service, false).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/forecasts", strings.NewReader(`{"zip":"02108","days":1}`)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing key returned %d", w.Code)
	}
	w = httptest.NewRecorder()
	routes(service, false).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatal("liveness should work without a key")
	}
	w = httptest.NewRecorder()
	routes(service, false).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/forecasts/missing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatal("unknown job should be 404")
	}
}

func TestQueueCapacityAndExpiry(t *testing.T) {
	service := &jobService{jobs: make(map[string]Job), queue: make(chan string, 1), maxJobs: 2}
	input := ForecastRequest{ZIP: "02108", Days: 1}
	if _, err := service.submit(input); err != nil {
		t.Fatal(err)
	}
	if _, err := service.submit(input); !errors.Is(err, errBusy) {
		t.Fatal("full queue must reject submission")
	}
	if len(service.jobs) != 1 {
		t.Fatal("rejected job was retained")
	}
	service.jobs["expired"] = Job{ID: "expired", Status: "completed", UpdatedAt: time.Now().Add(-31 * time.Minute)}
	if _, ok := service.lookup("expired"); ok {
		t.Fatal("completed result did not expire")
	}
}

func TestProviderTimeoutBecomesFailedJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := newTestJobService(ctx, providerFunc(func(ctx context.Context, _ ForecastRequest) (Forecast, error) {
		<-ctx.Done()
		return Forecast{}, ctx.Err()
	}), 1, 1)
	service.timeout = 10 * time.Millisecond
	defer func() { cancel(); service.workers.Wait() }()
	job, err := service.submit(ForecastRequest{ZIP: "10001", Days: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result := waitJob(t, service, job.ID); result.Status != "failed" || result.Error == "" {
		t.Fatalf("timeout result: %+v", result)
	}
}

// Unit tests substitute only the transport; completion still runs the evaluator.
type publisherFunc func(context.Context, forecastEvent) error

func (f publisherFunc) Publish(ctx context.Context, event forecastEvent) error { return f(ctx, event) }
func testForecast(input ForecastRequest) Forecast {
	now := time.Now().UTC()
	return Forecast{ZIP: input.ZIP, Days: input.Days, WindowStart: now, WindowEnd: now.Add(time.Duration(input.Days) * 24 * time.Hour),
		Points: []ForecastPoint{{Time: now.Add(time.Hour), TemperatureCelsius: 20, WindMetersPerSec: 5}}}
}
func newTestJobService(ctx context.Context, provider forecastProvider, workers, queueSize int) *jobService {
	var service *jobService
	publisher := publisherFunc(func(ctx context.Context, event forecastEvent) error {
		return service.evaluateEvent(event, kafkaReceipt{Topic: "unit-test", Partition: 0, Offset: 1})
	})
	service = newJobService(ctx, provider, workers, queueSize, publisher, alertRules{35, 0, 15})
	return service
}
