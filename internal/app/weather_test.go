package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOpenWeatherForecastWindowsAndUnits(t *testing.T) {
	for _, days := range []int{1, 5} {
		t.Run(fmt.Sprint(days), func(t *testing.T) {
			baseTime := time.Now().UTC().Truncate(time.Second)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Query().Get("appid") != "test-key" {
					t.Error("API key missing")
				}
				if r.URL.Path == "/geo/1.0/zip" {
					if r.URL.Query().Get("zip") != "02108,US" {
						t.Error("leading-zero ZIP or country lost")
					}
					fmt.Fprint(w, `{"zip":"02108","name":"Boston","lat":42.36,"lon":-71.06,"country":"US"}`)
					return
				}
				if r.URL.Path != "/data/2.5/forecast" || r.URL.Query().Get("units") != "metric" || r.URL.Query().Get("lat") != "42.36" {
					t.Error("incorrect forecast endpoint, coordinates, or units")
				}
				points := []map[string]any{}
				// Include past and beyond-horizon points to verify filtering.
				for i := -1; i <= 41; i++ {
					points = append(points, map[string]any{"dt": baseTime.Add(time.Duration(i)*3*time.Hour + time.Hour).Unix(), "main": map[string]any{"temp": 12.5}, "wind": map[string]any{"speed": 4.2}})
				}
				json.NewEncoder(w).Encode(map[string]any{"list": points})
			}))
			defer server.Close()
			provider := &openWeather{key: "test-key", client: server.Client(), base: server.URL}
			forecast, err := provider.Fetch(context.Background(), ForecastRequest{ZIP: "02108", Days: days})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 2 || len(forecast.Points) != days*8 || forecast.ZIP != "02108" {
				t.Fatalf("unexpected calls/forecast: calls=%d points=%d zip=%s", calls, len(forecast.Points), forecast.ZIP)
			}
			for _, point := range forecast.Points {
				if point.Time.Before(forecast.WindowStart) || !point.Time.Before(forecast.WindowEnd) || point.TemperatureCelsius != 12.5 || point.WindMetersPerSec != 4.2 {
					t.Fatalf("incorrect normalized point: %+v", point)
				}
			}
		})
	}
}

func TestProviderFailuresDoNotExposeAPIKey(t *testing.T) {
	for _, code := range []int{401, 404, 429, 500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				fmt.Fprint(w, "secret-value")
			}))
			defer server.Close()
			provider := &openWeather{key: "secret-value", client: server.Client(), base: server.URL}
			_, err := provider.Fetch(context.Background(), ForecastRequest{ZIP: "10001", Days: 1})
			if err == nil || strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("unsafe/missing error: %v", err)
			}
		})
	}
}

func TestInvalidProviderResponses(t *testing.T) {
	for _, response := range []string{`not JSON`, `{}`, `{"list":[]}`, `{"list":[{"dt":123}]}`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/geo/1.0/zip" {
					fmt.Fprint(w, `{"zip":"10001","name":"New York","lat":40,"lon":-74,"country":"US"}`)
					return
				}
				fmt.Fprint(w, response)
			}))
			defer server.Close()
			provider := &openWeather{key: "test-key", client: server.Client(), base: server.URL}
			if _, err := provider.Fetch(context.Background(), ForecastRequest{ZIP: "10001", Days: 1}); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestCancelledRateLimitWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := &openWeather{key: "test-key", ticks: make(chan time.Time)}
	if err := provider.get(ctx, "/test", url.Values{}, nil); err != context.Canceled {
		t.Fatalf("got %v, want cancellation", err)
	}
}
