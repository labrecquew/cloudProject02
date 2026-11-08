package app

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const maxMetricLocations = 16
const metricForecastTTL = 30 * time.Minute

var alertTypes = []string{"high_temperature", "low_temperature", "high_wind"}

type forecastSnapshot struct {
	forecast Forecast
	alerts   []Alert
	updated  time.Time
}

type appMetrics struct {
	registry      *prometheus.Registry
	accepted      prometheus.Counter
	rejected      prometheus.Counter
	finished      *prometheus.CounterVec
	alerts        *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	queueWait     prometheus.Histogram
	active        prometheus.Gauge
	httpRequests  *prometheus.CounterVec
	httpDuration  *prometheus.HistogramVec
	providerCalls *prometheus.CounterVec
	providerTime  *prometheus.HistogramVec
	kafkaOps      *prometheus.CounterVec
	ignored       *prometheus.CounterVec
	invalid       prometheus.Counter
	mu            sync.Mutex
	forecasts     map[string]forecastSnapshot
	descriptors   map[string]*prometheus.Desc
}

func newAppMetrics(queue chan string, rules alertRules) *appMetrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	}
	counts := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	latencyBuckets := []float64{0.1, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300}
	m := &appMetrics{
		registry: prometheus.NewRegistry(), forecasts: make(map[string]forecastSnapshot), descriptors: make(map[string]*prometheus.Desc),
		accepted:      counter("weather_requests_accepted_total", "Forecast jobs accepted since app startup."),
		rejected:      counter("weather_requests_rejected_total", "Forecast jobs rejected because queue or result storage is full."),
		finished:      counts("weather_requests_finished_total", "Forecast jobs finished after evaluation or failure.", "status"),
		alerts:        counts("weather_alerts_total", "Triggered forecast-point alerts, counted once per evaluated job.", "type"),
		duration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "weather_request_duration_seconds", Help: "Submission-to-evaluation or failure latency, including queue wait.", Buckets: latencyBuckets}, []string{"status"}),
		queueWait:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "weather_queue_wait_duration_seconds", Help: "Time a forecast job waited before worker processing.", Buckets: latencyBuckets}),
		active:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "weather_active_workers", Help: "Workers retrieving, publishing, or waiting for evaluation."}),
		httpRequests:  counts("weather_http_requests_total", "HTTP responses by bounded route, method, and status labels; excludes metrics scrapes.", "route", "method", "status"),
		httpDuration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "weather_http_request_duration_seconds", Help: "HTTP handler latency; asynchronous acceptance is not full job latency.", Buckets: prometheus.DefBuckets}, []string{"route"}),
		providerCalls: counts("weather_provider_calls_total", "Outgoing OpenWeather calls; excludes waiting for the rate limiter.", "endpoint", "result"),
		providerTime:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "weather_provider_request_duration_seconds", Help: "OpenWeather call duration after rate-limit waiting.", Buckets: prometheus.DefBuckets}, []string{"endpoint"}),
		kafkaOps:      counts("weather_kafka_operations_total", "Kafka publish, fetch, and offset-commit attempts.", "operation", "result"),
		ignored:       counts("weather_kafka_ignored_events_total", "Ignored forecast events by bounded reason.", "reason"),
		invalid:       counter("weather_kafka_invalid_records_total", "Malformed or invalid Kafka records discarded by the consumer."),
	}
	for _, status := range []string{"completed", "failed"} {
		m.finished.WithLabelValues(status)
		m.duration.WithLabelValues(status)
	}
	for _, kind := range alertTypes {
		m.alerts.WithLabelValues(kind)
	}
	for _, operation := range []string{"publish", "consume", "commit"} {
		for _, result := range []string{"success", "error"} {
			m.kafkaOps.WithLabelValues(operation, result)
		}
	}
	for _, endpoint := range []string{"geocoding", "forecast"} {
		for _, result := range []string{"success", "error"} {
			m.providerCalls.WithLabelValues(endpoint, result)
		}
	}
	for _, reason := range []string{"duplicate", "unknown_job", "failed_job"} {
		m.ignored.WithLabelValues(reason)
	}
	for name, help := range map[string]string{
		"weather_forecast_temperature_celsius":    "Latest evaluated forecast temperature by ZIP and forecast step (1-40). Sample timestamp is scrape time, not forecast valid time.",
		"weather_forecast_wind_meters_per_second": "Latest evaluated forecast wind by ZIP and forecast step (1-40).",
		"weather_forecast_valid_time_seconds":     "UTC Unix time predicted by a forecast step; use as the forecast time axis.",
	} {
		m.descriptors[name] = prometheus.NewDesc(name, help, []string{"zip", "step"}, nil)
	}
	for name, help := range map[string]string{
		"weather_forecast_updated_time_seconds":      "UTC Unix time when this ZIP's latest forecast was evaluated. Snapshots expire after 30 minutes.",
		"weather_forecast_window_start_time_seconds": "Start of the latest requested forecast window as UTC Unix time.",
		"weather_forecast_window_end_time_seconds":   "End of the latest requested forecast window as UTC Unix time.",
	} {
		m.descriptors[name] = prometheus.NewDesc(name, help, []string{"zip"}, nil)
	}
	m.descriptors["weather_forecast_alerts"] = prometheus.NewDesc("weather_forecast_alerts", "Alert counts in the latest evaluated forecast for a ZIP, not cumulative counts.", []string{"zip", "type"}, nil)
	thresholdTemp := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "weather_alert_threshold_temperature_celsius", Help: "Configured high and low temperature thresholds."}, []string{"type"})
	thresholdTemp.WithLabelValues("high_temperature").Set(rules.HighTempC)
	thresholdTemp.WithLabelValues("low_temperature").Set(rules.LowTempC)
	thresholdWind := prometheus.NewGauge(prometheus.GaugeOpts{Name: "weather_alert_threshold_wind_meters_per_second", Help: "Configured high wind threshold."})
	thresholdWind.Set(rules.HighWind)
	m.registry.MustRegister(m, m.accepted, m.rejected, m.finished, m.alerts, m.duration, m.queueWait, m.active,
		m.httpRequests, m.httpDuration, m.providerCalls, m.providerTime, m.kafkaOps, m.ignored, m.invalid, thresholdTemp, thresholdWind,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "weather_queue_depth", Help: "Jobs currently waiting in the Go channel."}, func() float64 { return float64(len(queue)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "weather_queue_capacity", Help: "Maximum jobs that can wait in the Go channel."}, func() float64 { return float64(cap(queue)) }),
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *appMetrics) finishedJob(job Job) {
	m.finished.WithLabelValues(job.Status).Inc()
	m.duration.WithLabelValues(job.Status).Observe(job.UpdatedAt.Sub(job.CreatedAt).Seconds())
	if job.Status != "completed" {
		return
	}
	for _, alert := range job.Alerts {
		m.alerts.WithLabelValues(alert.Type).Inc()
	}
	m.recordForecast(*job.Forecast, job.Alerts, job.UpdatedAt)
}

func (m *appMetrics) expireLocked(now time.Time) {
	for zip, snapshot := range m.forecasts {
		if now.Sub(snapshot.updated) >= metricForecastTTL {
			delete(m.forecasts, zip)
		}
	}
}

func (m *appMetrics) recordForecast(f Forecast, alerts []Alert, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(now)
	if _, exists := m.forecasts[f.ZIP]; !exists && len(m.forecasts) >= maxMetricLocations {
		var oldest string
		for zip, snapshot := range m.forecasts {
			if oldest == "" || snapshot.updated.Before(m.forecasts[oldest].updated) {
				oldest = zip
			}
		}
		delete(m.forecasts, oldest)
	}
	// Copy data owned by the job to keep scrapes independent of callers.
	f.Points = append([]ForecastPoint(nil), f.Points[:min(len(f.Points), 40)]...)
	m.forecasts[f.ZIP] = forecastSnapshot{f, append([]Alert(nil), alerts...), now}
}

func (m *appMetrics) Describe(out chan<- *prometheus.Desc) {
	for _, desc := range m.descriptors {
		out <- desc
	}
}

func (m *appMetrics) Collect(out chan<- prometheus.Metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(time.Now())
	emit := func(name string, value float64, labels ...string) {
		out <- prometheus.MustNewConstMetric(m.descriptors[name], prometheus.GaugeValue, value, labels...)
	}
	for zip, snapshot := range m.forecasts {
		f := snapshot.forecast
		emit("weather_forecast_updated_time_seconds", float64(snapshot.updated.Unix()), zip)
		emit("weather_forecast_window_start_time_seconds", float64(f.WindowStart.Unix()), zip)
		emit("weather_forecast_window_end_time_seconds", float64(f.WindowEnd.Unix()), zip)
		for i, point := range f.Points {
			step := strconv.Itoa(i + 1)
			emit("weather_forecast_temperature_celsius", point.TemperatureCelsius, zip, step)
			emit("weather_forecast_wind_meters_per_second", point.WindMetersPerSec, zip, step)
			emit("weather_forecast_valid_time_seconds", float64(point.Time.Unix()), zip, step)
		}
		counts := map[string]float64{}
		for _, alert := range snapshot.alerts {
			counts[alert.Type]++
		}
		for _, kind := range alertTypes {
			emit("weather_forecast_alerts", counts[kind], zip, kind)
		}
	}
}

func (m *appMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

type measuredResponse struct {
	http.ResponseWriter
	status int
}

func (w *measuredResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *measuredResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (m *appMetrics) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		response := &measuredResponse{ResponseWriter: w}
		next.ServeHTTP(response, r)
		if response.status == 0 {
			response.status = http.StatusOK
		}
		route := "other"
		switch {
		case r.URL.Path == "/healthz":
			route = "health"
		case r.URL.Path == "/forecasts":
			route = "submit"
		case r.Pattern == "GET /forecasts/{id}":
			route = "lookup"
		}
		method := r.Method
		switch method {
		case "GET", "HEAD", "POST", "OPTIONS":
		default:
			method = "other"
		}
		m.httpRequests.WithLabelValues(route, method, strconv.Itoa(response.status)).Inc()
		m.httpDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}
