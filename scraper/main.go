package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"scraper/internal/broker"
	"scraper/internal/config"
	"scraper/internal/djinni"
	"scraper/internal/repository"
	"scraper/pkg/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultQueueName      = "new_vacancies"
	defaultScrapeInterval = 15 * time.Minute
)

type appRepository interface {
	JobRepository
	Close()
}

type appBroker interface {
	JobPublisher
	DeclareQueue(name string) (amqp.Queue, error)
	Close()
}

type retryConfig struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func defaultRetryConfig() retryConfig {
	return retryConfig{
		maxAttempts:    5,
		initialBackoff: 1 * time.Second,
		maxBackoff:     10 * time.Second,
	}
}

type appDeps struct {
	loadConfig  func(envPath string) (*config.Config, error)
	setupLogger func(filePath string) (*slog.Logger, io.Closer, error)
	newRepo     func(dsn string) (appRepository, error)
	newBroker   func(url string) (appBroker, error)
	fetchJobs   JobFetcher
	retryCfg    retryConfig
}

func defaultAppDeps() appDeps {
	return appDeps{
		loadConfig:  config.LoadConfig,
		setupLogger: logger.Setup,
		newRepo: func(dsn string) (appRepository, error) {
			return repository.NewJobRepository(dsn)
		},
		newBroker: func(url string) (appBroker, error) {
			return broker.NewRabbitMQ(url)
		},
		fetchJobs: djinni.FetchJobs,
		retryCfg:  defaultRetryConfig(),
	}
}

func main() {
	os.Exit(runCLI())
}

func runCLI() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runApp(ctx, "../.env", defaultAppDeps()); err != nil {
		return 1
	}
	return 0
}

func runApp(ctx context.Context, envPath string, deps appDeps) error {
	// 1. Initialize configuration
	cfg, err := deps.loadConfig(envPath)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return fmt.Errorf("load config: %w", err)
	}

	// 2. Initialize structured logging with file rotation (slog + lumberjack)
	_, logCloser, err := deps.setupLogger(cfg.Scraper.LogPath)
	if err != nil {
		slog.Error("failed to initialize logger", "error", err)
		return fmt.Errorf("setup logger: %w", err)
	}
	if logCloser != nil {
		defer func() {
			if closeErr := logCloser.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "failed to close logger: %v\n", closeErr)
			}
		}()
	}
	slog.Info("starting scraper daemon")

	// 3. Initialize Database Repository (with bounded startup retry)
	repo, err := retryConnect(ctx, "postgres", deps.retryCfg, func() (appRepository, error) {
		return deps.newRepo(cfg.DSN())
	})
	if err != nil {
		slog.Error("repository initialization failed", "error", err)
		return err
	}
	defer func() {
		slog.Info("closing database connection pool...")
		repo.Close()
	}()

	// 4. Initialize RabbitMQ Broker and declare target queue (with bounded startup retry)
	queueName := defaultQueueName
	mq, err := retryConnect(ctx, "rabbitmq", deps.retryCfg, func() (appBroker, error) {
		b, dialErr := deps.newBroker(cfg.URL())
		if dialErr != nil {
			return nil, dialErr
		}
		if _, declErr := b.DeclareQueue(queueName); declErr != nil {
			b.Close()
			return nil, fmt.Errorf("declare queue %q: %w", queueName, declErr)
		}
		return b, nil
	})
	if err != nil {
		slog.Error("broker initialization failed", "queue", queueName, "error", err)
		return err
	}
	defer func() {
		slog.Info("closing rabbitmq connection and channels...")
		mq.Close()
	}()

	// 5. Run scraper in batch (RUN_ONCE=true) or daemon mode
	if err := run(ctx, cfg, repo, mq, queueName, deps.fetchJobs); err != nil {
		slog.Error("scraper exited with error", "error", err)
		return err
	}
	return nil
}

func retryConnect[T any](
	ctx context.Context,
	name string,
	cfg retryConfig,
	connect func() (T, error),
) (T, error) {
	var zero T
	maxAttempts := cfg.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := cfg.initialBackoff
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, fmt.Errorf("%s initialization cancelled: %w", name, err)
		}

		res, err := connect()
		if err == nil {
			return res, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}

		slog.Warn("dependency connection attempt failed; retrying",
			"dependency", name,
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff", backoff.String(),
			"error", err,
		)

		if backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, fmt.Errorf("%s startup retry cancelled: %w (last error: %v)", name, ctx.Err(), lastErr)
			case <-timer.C:
			}
			backoff *= 2
			if cfg.maxBackoff > 0 && backoff > cfg.maxBackoff {
				backoff = cfg.maxBackoff
			}
		}
	}

	return zero, fmt.Errorf("%s failed after %d attempt(s): %w", name, maxAttempts, lastErr)
}

type JobRepository interface {
	SaveJobIfNotExists(ctx context.Context, job djinni.Job) (bool, error)
	DeleteJobByGUID(ctx context.Context, guid string) error
}

type JobPublisher interface {
	PublishJob(ctx context.Context, queueName string, job djinni.Job) error
	PublishBatchEOF(ctx context.Context, queueName string) error
}

type JobFetcher func(ctx context.Context, rssURL string) ([]djinni.Job, error)

type tickerFactory func(d time.Duration) (<-chan time.Time, func())

func stdTickerFactory(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

func run(
	ctx context.Context,
	cfg *config.Config,
	repo JobRepository,
	mq JobPublisher,
	queueName string,
	fetchJobs JobFetcher,
) error {
	return runWithTicker(ctx, cfg, repo, mq, queueName, fetchJobs, stdTickerFactory)
}

func runWithTicker(
	ctx context.Context,
	cfg *config.Config,
	repo JobRepository,
	mq JobPublisher,
	queueName string,
	fetchJobs JobFetcher,
	newTicker tickerFactory,
) error {
	slog.Info("executing initial scrape cycle", "run_once", cfg.Scraper.RunOnce)
	iterErr := runScrapeIteration(ctx, cfg, repo, mq, queueName, fetchJobs)

	if cfg.Scraper.RunOnce {
		var eofErr error
		if ctx.Err() == nil {
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			eofErr = mq.PublishBatchEOF(pubCtx, queueName)
			cancel()
			if eofErr != nil {
				slog.Error("failed to publish batch EOF marker", "queue", queueName, "error", eofErr)
			} else {
				slog.Info("published batch EOF marker", "queue", queueName)
			}
		}
		if iterErr != nil && eofErr != nil {
			return fmt.Errorf("%w (also failed to publish batch EOF: %v)", iterErr, eofErr)
		}
		if eofErr != nil {
			return fmt.Errorf("publish batch EOF to %q: %w", queueName, eofErr)
		}
		if iterErr != nil {
			return iterErr
		}
		slog.Info("RUN_ONCE=true: scraper batch run completed, exiting cleanly")
		return nil
	}

	if iterErr != nil {
		slog.Error("initial scrape iteration failed; continuing in daemon mode", "error", iterErr)
	}

	interval := cfg.Scraper.Interval
	if interval <= 0 {
		slog.Warn("non-positive SCRAPE_INTERVAL configured; falling back to 15m", "configured", interval)
		interval = defaultScrapeInterval
	}

	tickCh, stopTicker := newTicker(interval)
	defer stopTicker()

	slog.Info("scraper service entered daemon mode", "interval", interval.String())

	for {
		select {
		case <-ctx.Done():
			slog.Info("termination signal received: starting graceful shutdown...")
			slog.Info("scraper daemon stopped successfully.")
			return nil

		case <-tickCh:
			if ctx.Err() != nil {
				continue
			}
			slog.Info("ticker triggered periodic scrape cycle")
			if err := runScrapeIteration(ctx, cfg, repo, mq, queueName, fetchJobs); err != nil {
				slog.Error("periodic scrape iteration failed; continuing", "error", err)
			}
		}
	}
}

// runScrapeIteration fetches jobs, deduplicates them in Postgres, and publishes new ones to RabbitMQ.
// Panics inside an iteration are recovered and returned as an error with stack trace logging.
func runScrapeIteration(
	ctx context.Context,
	cfg *config.Config,
	repo JobRepository,
	mq JobPublisher,
	queueName string,
	fetchJobs JobFetcher,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic recovered during scrape iteration",
				"panic", r,
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("scrape iteration panicked: %v", r)
		}
	}()

	// Check if the service was already requested to shut down
	if ctx.Err() != nil {
		return nil
	}

	jobs, err := fetchJobs(ctx, cfg.Scraper.RSSURL)
	if err != nil {
		slog.Error("failed to fetch jobs from djinni", "error", err)
		return nil
	}

	if len(jobs) == 0 {
		slog.Info("no jobs found in RSS feed")
		return nil
	}

	newJobsCount := 0

	for _, job := range jobs {
		// Stop processing if shutdown signal was intercepted mid-iteration
		if ctx.Err() != nil {
			slog.Warn("scrape iteration interrupted by shutdown signal")
			return nil
		}

		isNew, err := repo.SaveJobIfNotExists(ctx, job)
		if err != nil {
			slog.Error("failed to save job to database", "guid", job.GUID, "error", err)
			continue
		}

		if isNew {
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := mq.PublishJob(pubCtx, queueName, job)
			cancel()

			if err != nil {
				slog.Error("failed to publish job to queue; rolling back db record for retry", "guid", job.GUID, "error", err)
				rbCtx, rbCancel := context.WithTimeout(context.Background(), 5*time.Second)
				if rbErr := repo.DeleteJobByGUID(rbCtx, job.GUID); rbErr != nil {
					slog.Error("failed to rollback job record after publish failure", "guid", job.GUID, "error", rbErr)
				}
				rbCancel()
				continue
			}

			slog.Info("processed and published new vacancy", "title", job.Title, "guid", job.GUID)
			newJobsCount++
		}
	}

	slog.Info("finished processing cycle", "new_jobs", newJobsCount, "total_scraped", len(jobs))
	return nil
}
