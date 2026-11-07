package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	Error     string          `json:"error,omitempty"`
}

type jobService struct {
	mu       sync.Mutex
	jobs     map[string]Job
	queue    chan string
	provider forecastProvider
	maxJobs  int
	timeout  time.Duration
	workers  sync.WaitGroup
}

func newJobService(ctx context.Context, provider forecastProvider, workers, queueSize int) *jobService {
	s := &jobService{
		jobs: make(map[string]Job), queue: make(chan string, queueSize),
		provider: provider, maxJobs: 256, timeout: 30 * time.Second,
	}
	for i := 0; i < workers; i++ {
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			s.worker(ctx)
		}()
	}
	return s
}

// Cleanup happens under the same lock as lookup/submission. Only finished jobs
// expire, after 30 minutes. The store also has a hard capacity bound.
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

func (s *jobService) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.queue:
			if ctx.Err() != nil {
				return
			}
			s.mu.Lock()
			job := s.jobs[id]
			job.Status, job.UpdatedAt = "running", time.Now().UTC()
			s.jobs[id] = job
			s.mu.Unlock()

			jobCtx, cancel := context.WithTimeout(ctx, s.timeout)
			forecast, err := s.provider.Fetch(jobCtx, job.Request)
			cancel()
			job.UpdatedAt = time.Now().UTC()
			if err != nil {
				job.Status, job.Error = "failed", err.Error()
			} else {
				job.Status, job.Forecast = "completed", &forecast
			}
			s.mu.Lock()
			s.jobs[id] = job
			s.mu.Unlock()
		}
	}
}
