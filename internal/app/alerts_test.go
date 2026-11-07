package app

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestAlertThresholdBoundaries(t *testing.T) {
	rules := alertRules{defaultHighTempC, 0, 15}
	for _, tc := range []struct {
		name       string
		temp, wind float64
		types      []string
	}{
		{"normal", 20, 5, nil},
		{"below 90F", (89.9 - 32) * 5 / 9, 14.99, nil},
		{"exactly 90F", (90.0 - 32) * 5 / 9, 5, []string{"high_temperature"}},
		{"above 90F", (90.1 - 32) * 5 / 9, 5, []string{"high_temperature"}},
		{"low equals", 0, 5, []string{"low_temperature"}},
		{"low exceeds", -1, 5, []string{"low_temperature"}},
		{"above low", 0.01, 5, nil},
		{"wind equals", 20, 15, []string{"high_wind"}},
		{"combined", 35, 16, []string{"high_temperature", "high_wind"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Now().UTC()
			alerts := rules.evaluate(Forecast{Points: []ForecastPoint{{at, tc.temp, tc.wind}}})
			if len(alerts) != len(tc.types) {
				t.Fatalf("got %+v, want %v", alerts, tc.types)
			}
			for i, expected := range tc.types {
				if alerts[i].Type != expected || !alerts[i].Time.Equal(at) {
					t.Fatalf("incorrect alert: %+v", alerts[i])
				}
			}
		})
	}
}

func TestInvalidAlertRules(t *testing.T) {
	for _, rules := range []alertRules{{0, 35, 15}, {0, 0, 15}, {35, 0, -1}, {math.NaN(), 0, 15}, {35, 0, math.Inf(1)}} {
		if rules.validate() == nil {
			t.Fatalf("accepted invalid rules %+v", rules)
		}
	}
}

func TestCompletionRequiresEvaluationAndIgnoresDuplicates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan forecastEvent, 1)
	service := newJobService(ctx, providerFunc(func(_ context.Context, input ForecastRequest) (Forecast, error) {
		f := testForecast(input)
		f.Points[0].TemperatureCelsius = 35
		f.Points[0].WindMetersPerSec = 15
		return f, nil
	}), 1, 1, publisherFunc(func(_ context.Context, event forecastEvent) error { events <- event; return nil }), alertRules{35, 0, 15})
	defer func() { cancel(); service.workers.Wait() }()
	job, err := service.submit(ForecastRequest{"02108", 1})
	if err != nil {
		t.Fatal(err)
	}
	var event forecastEvent
	select {
	case event = <-events:
	case <-time.After(3 * time.Second):
		t.Fatal("no published event")
	}
	if current, _ := service.lookup(job.ID); current.Status != "awaiting_evaluation" || current.Forecast != nil {
		t.Fatal("job completed before consumption")
	}
	bad := event
	bad.Forecast.Days = 5
	if service.evaluateEvent(bad, kafkaReceipt{}) == nil {
		t.Fatal("mismatched event was accepted")
	}
	receipt := kafkaReceipt{"test", 1, 8}
	if err := service.evaluateEvent(event, receipt); err != nil {
		t.Fatal(err)
	}
	first := waitJob(t, service, job.ID)
	if len(first.Alerts) != 2 || first.Kafka.Offset != 8 {
		t.Fatalf("incorrect evaluated result: %+v", first)
	}
	if err := service.evaluateEvent(event, kafkaReceipt{"test", 1, 9}); err != nil {
		t.Fatal(err)
	}
	second, _ := service.lookup(job.ID)
	if len(second.Alerts) != 2 || !second.UpdatedAt.Equal(first.UpdatedAt) || second.Kafka.Offset != 8 {
		t.Fatal("duplicate changed completed result")
	}
}

func TestPublicationFailureAndMissingConsumer(t *testing.T) {
	for _, failPublish := range []bool{true, false} {
		t.Run(map[bool]string{true: "publish failure", false: "consumer timeout"}[failPublish], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			service := newJobService(ctx, providerFunc(func(_ context.Context, input ForecastRequest) (Forecast, error) { return testForecast(input), nil }), 1, 1,
				publisherFunc(func(context.Context, forecastEvent) error {
					if failPublish {
						return errBusy
					}
					return nil
				}), alertRules{35, 0, 15})
			service.timeout = 15 * time.Millisecond
			defer func() { cancel(); service.workers.Wait() }()
			job, err := service.submit(ForecastRequest{"10001", 1})
			if err != nil {
				t.Fatal(err)
			}
			result := waitJob(t, service, job.ID)
			if result.Status != "failed" || result.Forecast != nil || result.Kafka != nil {
				t.Fatalf("false success: %+v", result)
			}
		})
	}
}
