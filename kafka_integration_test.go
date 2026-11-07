//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// Set KAFKA_TEST_BROKERS and run go test -tags integration -run TestKafkaRoundTrip.
// The provider is controlled, but publication, consumption and offset commits
// use a real broker. No weather API quota is consumed.
func TestKafkaRoundTrip(t *testing.T) {
	address := os.Getenv("KAFKA_TEST_BROKERS")
	if address == "" {
		t.Skip("set KAFKA_TEST_BROKERS to test a real broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic := fmt.Sprintf("weather-test-%d", time.Now().UnixNano())
	bus, err := newKafkaBus(ctx, strings.Split(address, ","), topic, topic+"-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	var service *jobService
	service = newJobService(ctx, providerFunc(func(_ context.Context, input ForecastRequest) (Forecast, error) {
		f := testForecast(input)
		f.Points[0].TemperatureCelsius = 35
		f.Points[0].WindMetersPerSec = 15
		f.Points = append(f.Points, ForecastPoint{f.Points[0].Time.Add(time.Hour), 0, 1})
		return f, nil
	}), 4, 8, bus, alertRules{35, 0, 15})
	duplicates := make(chan string, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		bus.consume(ctx, func(event forecastEvent, receipt kafkaReceipt) error {
			before, _ := service.lookup(event.JobID)
			err := service.evaluateEvent(event, receipt)
			if before.Status == "completed" {
				duplicates <- event.JobID
			}
			return err
		})
	}()
	defer func() { cancel(); service.workers.Wait(); <-done }()
	var submitted []Job
	for _, input := range []ForecastRequest{{"02108", 1}, {"10001", 2}, {"90210", 3}, {"12601", 5}} {
		job, err := service.submit(input)
		if err != nil {
			t.Fatal(err)
		}
		submitted = append(submitted, job)
	}
	for _, job := range submitted {
		for {
			result, _ := service.lookup(job.ID)
			if result.Status == "failed" {
				t.Fatalf("pipeline failed: %s", result.Error)
			}
			if result.Status == "completed" {
				if result.Forecast.ZIP != job.Request.ZIP || result.Forecast.Days != job.Request.Days || len(result.Alerts) != 3 || result.Kafka.Topic != topic {
					t.Fatalf("incorrect result: %+v", result)
				}
				t.Logf("ZIP %s days %d: 3 alerts, partition %d offset %d", result.Request.ZIP, result.Request.Days, result.Kafka.Partition, result.Kafka.Offset)
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("Kafka result timed out")
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	// Same key means these records share a partition. A malformed record must
	// not block the valid duplicate after it or change the completed result.
	first, _ := service.lookup(submitted[0].ID)
	if err := bus.writer.WriteMessages(ctx, kafka.Message{Key: []byte(first.ID), Value: []byte("invalid-json")}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, forecastEvent{1, first.ID, *first.Forecast}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-duplicates:
		if id != first.ID {
			t.Fatal("unexpected duplicate ID")
		}
	case <-ctx.Done():
		t.Fatal("consumer did not pass malformed record to process duplicate")
	}
	// A fresh request is a progress barrier after poison/duplicate publication.
	last, err := service.submit(ForecastRequest{"30301", 1})
	if err != nil {
		t.Fatal(err)
	}
	for {
		result, _ := service.lookup(last.ID)
		if result.Status == "completed" {
			break
		}
		if result.Status == "failed" {
			t.Fatal(result.Error)
		}
		select {
		case <-ctx.Done():
			t.Fatal("consumer did not recover")
		case <-time.After(10 * time.Millisecond):
		}
	}
	after, _ := service.lookup(first.ID)
	if len(after.Alerts) != 3 || !after.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatal("duplicate altered completed alerts")
	}
}
