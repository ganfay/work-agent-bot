package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"scraper/internal/config"
	"scraper/internal/djinni"

	amqp "github.com/rabbitmq/amqp091-go"
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
	cfg.Scraper.LogPath = "logs/test.log"
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

func TestRun_IntervalNonPositiveFallsBackToDefault(t *testing.T) {
	for _, interval := range []time.Duration{0, -5 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			cfg := testConfig(false)
			cfg.Scraper.Interval = interval

			var gotInterval time.Duration
			var stopped atomic.Bool
			var fetchCount atomic.Int32

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
				fetchCount.Add(1)
				return nil, nil
			}

			tickCh := make(chan time.Time)
			newTicker := func(d time.Duration) (<-chan time.Time, func()) {
				gotInterval = d
				cancel()
				return tickCh, func() { stopped.Store(true) }
			}

			err := runWithTicker(ctx, cfg, &fakeRepo{}, &fakePublisher{}, "new_vacancies", fetcher, newTicker)
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if gotInterval != 15*time.Minute {
				t.Fatalf("expected fallback interval 15m, got %v", gotInterval)
			}
			if fetchCount.Load() != 1 {
				t.Fatalf("expected initial fetch once, got %d", fetchCount.Load())
			}
			if !stopped.Load() {
				t.Fatal("expected ticker stop function to be called")
			}
		})
	}
}

func TestRun_RunOncePublishBatchEOFFailureReturnsError(t *testing.T) {
	wantErr := errors.New("eof publish failed")
	repo := &fakeRepo{}
	pub := &fakePublisher{eofErr: wantErr}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		return []djinni.Job{{GUID: "j1", Title: "Valid Job"}}, nil
	}

	err := run(context.Background(), cfg, repo, pub, "new_vacancies", fetcher)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error wrapping %v, got %v", wantErr, err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("expected valid job still published, got %d", len(pub.published))
	}
}

func TestRunScrapeIteration_RecoversFromPanicAndLogsStack(t *testing.T) {
	prevLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		panic("boom in parser")
	}

	err := runScrapeIteration(context.Background(), testConfig(true), &fakeRepo{}, &fakePublisher{}, "new_vacancies", fetcher)
	if err == nil {
		t.Fatal("expected non-nil error after panic recovery")
	}
	if !strings.Contains(err.Error(), "boom in parser") {
		t.Fatalf("expected panic message in error, got %v", err)
	}
	logOut := buf.String()
	if !strings.Contains(logOut, "panic recovered") || !strings.Contains(logOut, "TestRunScrapeIteration_RecoversFromPanicAndLogsStack") {
		t.Fatalf("expected panic recovery message and stack trace in log, got:\n%s", logOut)
	}
}

func TestRun_PanicInRunOnceStillPublishesEOFAndReturnsError(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	cfg := testConfig(true)

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		panic("fatal parse panic")
	}

	err := run(context.Background(), cfg, repo, pub, "new_vacancies", fetcher)
	if err == nil || !strings.Contains(err.Error(), "fatal parse panic") {
		t.Fatalf("expected panic error returned in RUN_ONCE=true, got %v", err)
	}
	if len(pub.eofQueues) != 1 || pub.eofQueues[0] != "new_vacancies" {
		t.Fatalf("expected best-effort batch EOF published despite panic, got %+v", pub.eofQueues)
	}
}

func TestRun_PanicInDaemonModeContinuesLoop(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	cfg := testConfig(false)
	cfg.Scraper.Interval = time.Minute

	var attempt atomic.Int32
	tickCh := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetcher := func(_ context.Context, _ string) ([]djinni.Job, error) {
		if attempt.Add(1) == 1 {
			panic("transient panic on first cycle")
		}
		return []djinni.Job{{GUID: "recovered-1", Title: "Go Dev"}}, nil
	}

	newTicker := func(_ time.Duration) (<-chan time.Time, func()) {
		go func() {
			tickCh <- time.Now()
			// Wait until second cycle publishes before canceling context.
			for {
				pub.mu.Lock()
				n := len(pub.published)
				pub.mu.Unlock()
				if n >= 1 {
					cancel()
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		return tickCh, func() {}
	}

	err := runWithTicker(ctx, cfg, repo, pub, "new_vacancies", fetcher, newTicker)
	if err != nil {
		t.Fatalf("expected daemon mode to exit cleanly after cancel, got %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].GUID != "recovered-1" {
		t.Fatalf("expected second iteration to publish recovered-1, got %+v", pub.published)
	}
	if len(pub.eofQueues) != 0 {
		t.Fatalf("daemon mode must not publish EOF, got %+v", pub.eofQueues)
	}
}

type orderTracker struct {
	mu    sync.Mutex
	order []string
}

func (o *orderTracker) record(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.order = append(o.order, name)
}

func (o *orderTracker) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.order...)
}

type fakeCloser struct {
	tracker    *orderTracker
	closeCount atomic.Int32
}

func (c *fakeCloser) Close() error {
	c.closeCount.Add(1)
	if c.tracker != nil {
		c.tracker.record("logger")
	}
	return nil
}

type fakeAppRepo struct {
	fakeRepo
	tracker    *orderTracker
	closeCount atomic.Int32
}

func (r *fakeAppRepo) Close() {
	r.closeCount.Add(1)
	if r.tracker != nil {
		r.tracker.record("repo")
	}
}

type fakeAppBroker struct {
	fakePublisher
	tracker      *orderTracker
	declareErr   error
	closeCount   atomic.Int32
	declareCalls atomic.Int32
}

func (b *fakeAppBroker) DeclareQueue(name string) (amqp.Queue, error) {
	b.declareCalls.Add(1)
	if b.declareErr != nil {
		return amqp.Queue{}, b.declareErr
	}
	return amqp.Queue{Name: name}, nil
}

func (b *fakeAppBroker) Close() {
	b.closeCount.Add(1)
	if b.tracker != nil {
		b.tracker.record("broker")
	}
}

func TestRunApp_StartupErrorCleanup(t *testing.T) {
	fastRetry := retryConfig{
		maxAttempts:    3,
		initialBackoff: 0,
		maxBackoff:     0,
	}

	t.Run("loadConfig error", func(t *testing.T) {
		wantErr := errors.New("bad config")
		loggerCalled := false
		deps := appDeps{
			loadConfig: func(string) (*config.Config, error) { return nil, wantErr },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) {
				loggerCalled = true
				return slog.Default(), nil, nil
			},
			retryCfg: fastRetry,
		}
		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) || loggerCalled {
			t.Fatalf("expected loadConfig error without calling logger, got err=%v loggerCalled=%v", err, loggerCalled)
		}
	})

	t.Run("setupLogger error", func(t *testing.T) {
		wantErr := errors.New("bad logger")
		repoCalled := false
		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return nil, nil, wantErr },
			newRepo: func(string) (appRepository, error) {
				repoCalled = true
				return nil, nil
			},
			retryCfg: fastRetry,
		}
		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) || repoCalled {
			t.Fatalf("expected setupLogger error without calling repo, got err=%v repoCalled=%v", err, repoCalled)
		}
	})

	t.Run("newRepo exhausted retries closes logCloser", func(t *testing.T) {
		wantErr := errors.New("pg down")
		lc := &fakeCloser{}
		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), lc, nil },
			newRepo:     func(string) (appRepository, error) { return nil, wantErr },
			retryCfg:    fastRetry,
		}
		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected repo error %v, got %v", wantErr, err)
		}
		if lc.closeCount.Load() != 1 {
			t.Fatalf("expected logCloser closed once, got %d", lc.closeCount.Load())
		}
	})

	t.Run("newBroker exhausted retries closes repo and logCloser", func(t *testing.T) {
		wantErr := errors.New("rmq down")
		tracker := &orderTracker{}
		lc := &fakeCloser{tracker: tracker}
		repo := &fakeAppRepo{tracker: tracker}
		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), lc, nil },
			newRepo:     func(string) (appRepository, error) { return repo, nil },
			newBroker:   func(string) (appBroker, error) { return nil, wantErr },
			retryCfg:    fastRetry,
		}
		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected broker error %v, got %v", wantErr, err)
		}
		if repo.closeCount.Load() != 1 || lc.closeCount.Load() != 1 {
			t.Fatalf("expected repo and logger closed, repo=%d logger=%d", repo.closeCount.Load(), lc.closeCount.Load())
		}
		got := strings.Join(tracker.snapshot(), ",")
		if got != "repo,logger" {
			t.Fatalf("expected LIFO close order repo,logger; got %s", got)
		}
	})

	t.Run("DeclareQueue failure closes broker on every attempt plus repo and logCloser", func(t *testing.T) {
		wantErr := errors.New("declare failed")
		tracker := &orderTracker{}
		lc := &fakeCloser{tracker: tracker}
		repo := &fakeAppRepo{tracker: tracker}
		var createdBrokers []*fakeAppBroker

		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), lc, nil },
			newRepo:     func(string) (appRepository, error) { return repo, nil },
			newBroker: func(string) (appBroker, error) {
				b := &fakeAppBroker{declareErr: wantErr}
				createdBrokers = append(createdBrokers, b)
				return b, nil
			},
			retryCfg: fastRetry,
		}

		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected DeclareQueue error %v, got %v", wantErr, err)
		}
		if len(createdBrokers) != fastRetry.maxAttempts {
			t.Fatalf("expected %d broker attempts, got %d", fastRetry.maxAttempts, len(createdBrokers))
		}
		for i, b := range createdBrokers {
			if b.closeCount.Load() != 1 {
				t.Fatalf("expected broker attempt %d closed once, got %d", i+1, b.closeCount.Load())
			}
		}
		if repo.closeCount.Load() != 1 || lc.closeCount.Load() != 1 {
			t.Fatalf("expected repo and logger closed, repo=%d logger=%d", repo.closeCount.Load(), lc.closeCount.Load())
		}
	})

	t.Run("happy path closes in LIFO order broker, repo, logger", func(t *testing.T) {
		tracker := &orderTracker{}
		lc := &fakeCloser{tracker: tracker}
		repo := &fakeAppRepo{tracker: tracker}
		mq := &fakeAppBroker{tracker: tracker}
		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), lc, nil },
			newRepo:     func(string) (appRepository, error) { return repo, nil },
			newBroker:   func(string) (appBroker, error) { return mq, nil },
			fetchJobs:   func(context.Context, string) ([]djinni.Job, error) { return nil, nil },
			retryCfg:    fastRetry,
		}

		if err := runApp(context.Background(), ".env", deps); err != nil {
			t.Fatalf("expected nil error on happy path, got %v", err)
		}
		got := strings.Join(tracker.snapshot(), ",")
		if got != "broker,repo,logger" {
			t.Fatalf("expected LIFO close order broker,repo,logger; got %s", got)
		}
	})

	t.Run("PublishBatchEOF error returns error and closes all resources", func(t *testing.T) {
		wantErr := errors.New("eof fail")
		tracker := &orderTracker{}
		lc := &fakeCloser{tracker: tracker}
		repo := &fakeAppRepo{tracker: tracker}
		mq := &fakeAppBroker{
			tracker:       tracker,
			fakePublisher: fakePublisher{eofErr: wantErr},
		}
		deps := appDeps{
			loadConfig:  func(string) (*config.Config, error) { return testConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), lc, nil },
			newRepo:     func(string) (appRepository, error) { return repo, nil },
			newBroker:   func(string) (appBroker, error) { return mq, nil },
			fetchJobs:   func(context.Context, string) ([]djinni.Job, error) { return nil, nil },
			retryCfg:    fastRetry,
		}

		err := runApp(context.Background(), ".env", deps)
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected error %v, got %v", wantErr, err)
		}
		got := strings.Join(tracker.snapshot(), ",")
		if got != "broker,repo,logger" {
			t.Fatalf("expected LIFO close order broker,repo,logger; got %s", got)
		}
	})
}

func TestRetryConnect_TransientRecoveryAndContextCancel(t *testing.T) {
	t.Run("succeeds on third attempt", func(t *testing.T) {
		attempts := 0
		cfg := retryConfig{maxAttempts: 4, initialBackoff: time.Millisecond, maxBackoff: 5 * time.Millisecond}
		val, err := retryConnect(context.Background(), "pg", cfg, func() (string, error) {
			attempts++
			if attempts < 3 {
				return "", errors.New("temporary")
			}
			return "connected", nil
		})
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if val != "connected" || attempts != 3 {
			t.Fatalf("expected (connected, 3), got (%q, %d)", val, attempts)
		}
	})

	t.Run("exhausts maxAttempts and wraps lastErr", func(t *testing.T) {
		errLast := errors.New("final failure")
		attempts := 0
		cfg := retryConfig{maxAttempts: 3, initialBackoff: time.Millisecond, maxBackoff: 2 * time.Millisecond}
		_, err := retryConnect(context.Background(), "rmq", cfg, func() (int, error) {
			attempts++
			if attempts == 3 {
				return 0, errLast
			}
			return 0, errors.New("earlier failure")
		})
		if !errors.Is(err, errLast) || attempts != 3 {
			t.Fatalf("expected wrapped %v after 3 attempts, got err=%v attempts=%d", errLast, err, attempts)
		}
	})

	t.Run("cancels during long backoff promptly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(15 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		cfg := retryConfig{maxAttempts: 3, initialBackoff: 5 * time.Second, maxBackoff: 5 * time.Second}
		_, err := retryConnect(ctx, "pg", cfg, func() (int, error) {
			return 0, errors.New("down")
		})
		elapsed := time.Since(start)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if elapsed > 200*time.Millisecond {
			t.Fatalf("expected prompt cancellation under 200ms, took %v", elapsed)
		}
	})
}
