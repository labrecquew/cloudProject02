package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write HTTP response", "error", err)
	}
}

func routes(jobs *jobService, configured bool) http.Handler {
	mux := http.NewServeMux()
	registerWeb(mux, jobs, configured)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /forecasts", func(w http.ResponseWriter, r *http.Request) {
		var input ForecastRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "send JSON with zip as a string and days as an integer"})
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must contain exactly one JSON object"})
			return
		}
		if err := input.validate(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if !configured {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "OPENWEATHER_API_KEY is not configured"})
			return
		}
		job, err := jobs.submit(input)
		if err != nil {
			if errors.Is(err, errBusy) {
				w.Header().Set("Retry-After", "5")
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			} else {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not submit request"})
			}
			return
		}
		w.Header().Set("Location", "/forecasts/"+job.ID)
		writeJSON(w, http.StatusAccepted, job)
	})
	mux.HandleFunc("GET /forecasts/{id}", func(w http.ResponseWriter, r *http.Request) {
		job, ok := jobs.lookup(r.PathValue("id"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "request not found"})
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
	if jobs.metrics != nil {
		mux.Handle("GET /metrics", jobs.metrics.handler())
		return jobs.metrics.instrument(mux)
	}
	return mux
}

func envPositiveInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 || n > 256 {
		slog.Error("configuration must be an integer between 1 and 256", "setting", name)
		os.Exit(1)
	}
	return n
}

func envFloat(name string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil {
		slog.Error("invalid numeric alert setting", "setting", name)
		os.Exit(1)
	}
	return n
}

func envText(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// Run starts the HTTP server, weather workers, and Kafka consumer.
func Run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	key := strings.TrimSpace(os.Getenv("OPENWEATHER_API_KEY"))
	if key == "" {
		slog.Warn("weather requests disabled: OPENWEATHER_API_KEY is not configured")
	}
	// 1.1 seconds between calls keeps this process below 60 calls/minute.
	// Each forecast request needs two calls: geocoding and weather retrieval.
	limiter := time.NewTicker(1100 * time.Millisecond)
	defer limiter.Stop()
	provider := &openWeather{
		key: key, base: "https://api.openweathermap.org",
		client: &http.Client{Timeout: 10 * time.Second}, ticks: limiter.C,
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	rules := alertRules{
		HighTempC: envFloat("HIGH_TEMP_C", defaultHighTempC),
		LowTempC:  envFloat("LOW_TEMP_C", defaultLowTempC),
		HighWind:  envFloat("HIGH_WIND_MPS", defaultHighWindMPS),
	}
	if err := rules.validate(); err != nil {
		slog.Error("invalid alert rules", "error", err)
		os.Exit(1)
	}
	brokers := strings.Split(envText("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	startupCtx, cancelStartup := context.WithTimeout(ctx, 20*time.Second)
	bus, err := newKafkaBus(startupCtx, brokers, envText("KAFKA_TOPIC", "weather.forecasts"), envText("KAFKA_GROUP", "weather-alerts"))
	cancelStartup()
	if err != nil {
		slog.Error("Kafka startup failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()
	jobs := newJobService(workerCtx, provider, envPositiveInt("WORKER_COUNT", 4), envPositiveInt("QUEUE_SIZE", 16), bus, rules)
	provider.metrics, bus.metrics = jobs.metrics, jobs.metrics
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); bus.consume(workerCtx, jobs.evaluateEvent) }()
	server := &http.Server{
		Addr: addr, Handler: routes(jobs, key != ""),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("starting weather monitor", "address", addr)
		serverErrors <- server.ListenAndServe()
	}()
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP shutdown failed", "error", err)
		}
	}
	cancelWorkers()
	jobs.workers.Wait()
	<-consumerDone
}
