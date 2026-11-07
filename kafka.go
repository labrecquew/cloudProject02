package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type forecastEvent struct {
	Version  int      `json:"version"`
	JobID    string   `json:"job_id"`
	Forecast Forecast `json:"forecast"`
}

type kafkaReceipt struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type forecastPublisher interface {
	Publish(context.Context, forecastEvent) error
}

type kafkaBus struct {
	writer *kafka.Writer
	reader *kafka.Reader
}

func newKafkaBus(ctx context.Context, brokers []string, topic, group string) (*kafkaBus, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("Kafka brokers, topic, and consumer group are required")
	}
	// Create the topic explicitly; single-broker project, three partitions.
	dialer := &kafka.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	controller, err := conn.Controller()
	conn.Close()
	if err != nil {
		return nil, fmt.Errorf("find Kafka controller: %w", err)
	}
	admin, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, fmt.Errorf("connect to Kafka controller: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		admin.SetDeadline(deadline)
	}
	err = admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
	admin.Close()
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return nil, fmt.Errorf("create forecast topic: %w", err)
	}
	return &kafkaBus{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{},
			RequiredAcks: kafka.RequireAll, MaxAttempts: 2,
			BatchTimeout: 10 * time.Millisecond, WriteTimeout: 5 * time.Second,
			Transport: &kafka.Transport{DialTimeout: 3 * time.Second},
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: topic, GroupID: group, Dialer: dialer,
			StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 2 * 1024 * 1024,
			MaxWait: time.Second, CommitInterval: 0,
		}),
	}, nil
}

func (b *kafkaBus) Publish(ctx context.Context, event forecastEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return errors.New("could not encode forecast event")
	}
	if err := b.writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.JobID), Value: value}); err != nil {
		return errors.New("Kafka could not confirm forecast publication")
	}
	return nil
}

func (b *kafkaBus) consume(ctx context.Context, handle func(forecastEvent, kafkaReceipt) error) {
	for ctx.Err() == nil {
		message, err := b.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("Kafka consumption failed; retrying")
			if !waitForRetry(ctx) {
				return
			}
			continue
		}
		var event forecastEvent
		err = json.Unmarshal(message.Value, &event)
		if err == nil && (event.JobID == "" || string(message.Key) != event.JobID || event.Version != 1) {
			err = errors.New("invalid forecast event envelope")
		}
		if err == nil {
			err = handle(event, kafkaReceipt{message.Topic, message.Partition, message.Offset})
		}
		if err != nil {
			// Invalid records are logged and skipped so one poison record cannot
			// permanently prevent later valid requests from being evaluated.
			slog.Warn("discarding invalid forecast event", "partition", message.Partition, "offset", message.Offset)
		}
		// Commit only after handling. Retry a failed commit before fetching more.
		// Redelivery is safe because job completion is idempotent by request ID.
		for ctx.Err() == nil {
			if err := b.reader.CommitMessages(ctx, message); err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			slog.Warn("Kafka offset commit failed; retrying")
			if !waitForRetry(ctx) {
				return
			}
		}
	}
}

func waitForRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (b *kafkaBus) Close() {
	b.reader.Close()
	if err := b.writer.Close(); err != nil {
		slog.Warn("Kafka producer close failed")
	}
}
