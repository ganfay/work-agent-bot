package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"scraper/internal/config"
	"scraper/internal/djinni"
)

type fakeRepo struct {
	mu      sync.Mutex
	seen    map[string]bool
	deleted []string
	errGUID string
}

func (r *fakeRepo) SaveJobIfNotExists(_ context.Context, job djinni.Job) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.errGUID != "" && job.GUID == r.errGUID {
		return false, errors.New("db error")
	}
	if r.seen == nil {
		r.seen = make(map[string]bool)
	}
	if r.seen[job.GUID] {
		return false, nil
	}
	r.seen[job.GUID] = true
	return true, nil
}

func (r *fakeRepo) DeleteJobByGUID(_ context.Context, guid string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, guid)
	r.deleted = append(r.deleted, guid)
	return nil
}

type fakePublisher struct {
	mu        sync.Mutex
	published []djinni.Job
	eofQueues []string
	pubErr    error
	eofErr    error
}

func (p *fakePublisher) PublishJob(_ context.Context, _ string, job djinni.Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pubErr != nil {
		return p.pubErr
	}
	p.published = append(p.published, job)
	return nil
}

func (p *fakePublisher) PublishBatchEOF(_ context.Context, queueName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.eofErr != nil {
		return p.eofErr
	}
	p.eofQueues = append(p.eofQueues, queueName)
	return nil
}

func testConfig(runOnce bool) *config.Config {
	cfg := &config.Config{}
	cfg.Scraper.RSSURL = "https://example.com/rss"
	cfg.Scraper.Interval = 50 * time.Millisecond
	cfg.Scraper.RunOnce = runOnce
	return cfg
}

func TestRun_RunOncePublishesNewJobsAndEOF(t *testing.T) {
	repo := &fakeRepo{seen: map[string]bool{"old-1": true}}
	pub := &fakePublisher{}
	cfg := testConfig(true)

	fetchCalls := 0
	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		fetchCalls++
		return []djinni.Job{
			{GUID: "old-1", Title: "Already Seen"},
			{GUID: "new-1", Title: "Go Backend"},
			{GUID: "new-2", Title: "Go + AI"},
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := run(ctx, cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("run returned unexpected error: %v", err)
	}

	if fetchCalls != 1 {
		t.Fatalf("expected 1 fetch call, got %d", fetchCalls)
	}
	if len(pub.published) != 2 {
		t.Fatalf("expected 2 published jobs, got %d", len(pub.published))
	}
	if pub.published[0].GUID != "new-1" || pub.published[1].GUID != "new-2" {
		t.Fatalf("unexpected published jobs: %+v", pub.published)
	}
	if len(pub.eofQueues) != 1 || pub.eofQueues[0] != "new_vacancies" {
		t.Fatalf("expected batch EOF on new_vacancies, got %+v", pub.eofQueues)
	}
}

func TestRun_RunOnceEmptyFeedStillPublishesEOF(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		return nil, nil
	}

	if err := run(context.Background(), cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("run returned unexpected error: %v", err)
	}

	if len(pub.published) != 0 {
		t.Fatalf("expected 0 published jobs, got %d", len(pub.published))
	}
	if len(pub.eofQueues) != 1 {
		t.Fatalf("expected batch EOF even on empty feed, got %+v", pub.eofQueues)
	}
}

func TestRun_RunOnceFetchErrorStillPublishesEOF(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		return nil, errors.New("network timeout")
	}

	if err := run(context.Background(), cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("run returned unexpected error: %v", err)
	}

	if len(pub.eofQueues) != 1 {
		t.Fatalf("expected batch EOF even on fetch error, got %+v", pub.eofQueues)
	}
}

func TestRun_DaemonModeRunsUntilContextCancelledWithoutEOF(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	cfg := testConfig(false)
	cfg.Scraper.Interval = 15 * time.Millisecond

	var mu sync.Mutex
	fetchCalls := 0
	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		mu.Lock()
		fetchCalls++
		mu.Unlock()
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	if err := run(ctx, cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("run returned unexpected error: %v", err)
	}

	mu.Lock()
	calls := fetchCalls
	mu.Unlock()

	if calls < 2 {
		t.Fatalf("expected daemon mode to run multiple iterations, got %d", calls)
	}
	if len(pub.eofQueues) != 0 {
		t.Fatalf("daemon mode must not publish batch EOF, got %+v", pub.eofQueues)
	}
}

func TestRunScrapeIteration_SkipsDBErrorAndCancelledContext(t *testing.T) {
	repo := &fakeRepo{errGUID: "bad-1"}
	pub := &fakePublisher{}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		return []djinni.Job{
			{GUID: "bad-1", Title: "Broken"},
			{GUID: "good-1", Title: "Valid"},
		}, nil
	}

	if err := runScrapeIteration(context.Background(), cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("runScrapeIteration returned unexpected error: %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].GUID != "good-1" {
		t.Fatalf("expected only good-1 published, got %+v", pub.published)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runScrapeIteration(cancelledCtx, cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("runScrapeIteration returned unexpected error: %v", err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("expected no additional publishes on cancelled ctx, got %+v", pub.published)
	}
}

func TestRunScrapeIteration_RollsBackDBRecordOnPublishFailure(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{pubErr: errors.New("rabbitmq down")}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		return []djinni.Job{{GUID: "retry-1", Title: "Go Dev"}}, nil
	}

	if err := runScrapeIteration(context.Background(), cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("runScrapeIteration returned unexpected error: %v", err)
	}

	if len(repo.deleted) != 1 || repo.deleted[0] != "retry-1" {
		t.Fatalf("expected rollback delete for retry-1, got %+v", repo.deleted)
	}
	if repo.seen["retry-1"] {
		t.Fatalf("expected retry-1 to be removed from seen set after publish failure")
	}

	// Next cycle after RabbitMQ recovers should publish retry-1
	pub.pubErr = nil
	if err := runScrapeIteration(context.Background(), cfg, repo, pub, "new_vacancies", fetcher); err != nil {
		t.Fatalf("runScrapeIteration returned unexpected error: %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].GUID != "retry-1" {
		t.Fatalf("expected retry-1 to be published after recovery, got %+v", pub.published)
	}
}
