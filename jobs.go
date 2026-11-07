package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"sync"
	"time"
)

var errBusy = errors.New("request capacity reached; try again later")

type Job struct {
	ID        string          `json:"id"`
	Request   ForecastRequest `json:"request"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Forecast  *Forecast       `json:"forecast,omitempty"`
	Alerts    []Alert         `json:"alerts"`
	Rules     *alertRules     `json:"alert_rules,omitempty"`
	Kafka     *kafkaReceipt   `json:"kafka,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type jobService struct {
	mu        sync.Mutex
	jobs      map[string]Job
	done      map[string]chan struct{}
	queue     chan string
	provider  forecastProvider
	publisher forecastPublisher
	rules     alertRules
	maxJobs   int
	timeout   time.Duration
	workers   sync.WaitGroup
}

func newJobService(ctx context.Context, provider forecastProvider, workers, queueSize int, publisher forecastPublisher, rules alertRules) *jobService {
	s := &jobService{
		jobs: make(map[string]Job), done: make(map[string]chan struct{}), queue: make(chan string, queueSize),
		provider: provider, publisher: publisher, rules: rules, maxJobs: 256, timeout: 30 * time.Second,
	}
	for i := 0; i < workers; i++ {
		s.workers.Add(1)
		go func() { defer s.workers.Done(); s.worker(ctx) }()
	}
	return s
}

func (s *jobService) cleanupLocked(now time.Time) {
	for id, job := range s.jobs {
		if (job.Status == "completed" || job.Status == "failed") && now.Sub(job.UpdatedAt) >= 30*time.Minute {
			delete(s.jobs, id)
		}
	}
}

func (s *jobService) submit(input ForecastRequest) (Job, error) {
	if err := input.validate(); err != nil {
		return Job{}, err
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return Job{}, errors.New("could not create request ID")
	}
	now := time.Now().UTC()
	job := Job{ID: hex.EncodeToString(bytes[:]), Request: input, Status: "queued", CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(now)
	if len(s.jobs) >= s.maxJobs {
		return Job{}, errBusy
	}
	s.jobs[job.ID] = job
	select {
	case s.queue <- job.ID:
		return job, nil
	default:
		delete(s.jobs, job.ID)
		return Job{}, errBusy
	}
}

func (s *jobService) lookup(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(time.Now())
	job, ok := s.jobs[id]
	return job, ok
}

func (s *jobService) fail(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	// An acknowledgment can fail even if the consumer already processed the
	// record. Never overwrite a confirmed evaluated result with a later error.
	if job.Status == "completed" {
		return
	}
	job.Status, job.Error, job.UpdatedAt = "failed", err.Error(), time.Now().UTC()
	s.jobs[id] = job
}

func (s *jobService) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.queue:
			if ctx.Err() != nil {
				return
			}
			s.process(ctx, id)
		}
	}
}

func (s *jobService) process(ctx context.Context, id string) {
	s.mu.Lock()
	job := s.jobs[id]
	job.Status, job.UpdatedAt = "running", time.Now().UTC()
	s.jobs[id] = job
	s.mu.Unlock()
	jobCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	forecast, err := s.provider.Fetch(jobCtx, job.Request)
	if err != nil {
		s.fail(id, err)
		return
	}
	if s.publisher == nil {
		s.fail(id, errors.New("Kafka publisher is not configured"))
		return
	}
	done := make(chan struct{})
	s.mu.Lock()
	job.Status, job.UpdatedAt = "awaiting_evaluation", time.Now().UTC()
	s.jobs[id], s.done[id] = job, done
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.done, id); s.mu.Unlock() }()
	if err := s.publisher.Publish(jobCtx, forecastEvent{Version: 1, JobID: id, Forecast: forecast}); err != nil {
		s.fail(id, err)
		return
	}
	select {
	case <-done:
	case <-jobCtx.Done():
		s.fail(id, errors.New("request timed out before Kafka evaluation completed"))
	}
}

func (s *jobService) evaluateEvent(event forecastEvent, receipt kafkaReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[event.JobID]
	// Results are in memory: old records after restart/expiry cannot restore
	// lookups. Ignore them. This deployment supports one application instance.
	if !ok || job.Status == "completed" || job.Status == "failed" {
		return nil
	}
	if event.Version != 1 || job.Status != "awaiting_evaluation" || event.Forecast.ZIP != job.Request.ZIP || event.Forecast.Days != job.Request.Days {
		return errors.New("forecast event does not match the pending request")
	}
	f := event.Forecast
	if len(f.Points) == 0 || f.WindowStart.IsZero() || !f.WindowEnd.After(f.WindowStart) {
		return errors.New("invalid forecast window")
	}
	for i, point := range f.Points {
		if point.Time.Before(f.WindowStart) || !point.Time.Before(f.WindowEnd) || math.IsNaN(point.TemperatureCelsius) || math.IsInf(point.TemperatureCelsius, 0) ||
			math.IsNaN(point.WindMetersPerSec) || math.IsInf(point.WindMetersPerSec, 0) || point.WindMetersPerSec < 0 || (i > 0 && !point.Time.After(f.Points[i-1].Time)) {
			return errors.New("invalid forecast measurement")
		}
	}
	job.Forecast, job.Alerts, job.Rules, job.Kafka = &f, s.rules.evaluate(f), &s.rules, &receipt
	job.Status, job.UpdatedAt = "completed", time.Now().UTC()
	s.jobs[event.JobID] = job
	if done := s.done[event.JobID]; done != nil {
		close(done)
	}
	return nil
}
