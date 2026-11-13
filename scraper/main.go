package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"scraper/internal/broker"
	"scraper/internal/config"
	"scraper/internal/djinni"
	"scraper/internal/repository"
	"scraper/pkg/logger"
)

func main() {
	// 1. Initialize configuration
	cfg := config.LoadConfig("../.env")

	// 2. Initialize structured logging with file rotation (slog + lumberjack)
	logger.Setup(cfg.Scraper.LogPath)
	slog.Info("starting scraper daemon")

	// 3. Graceful shutdown setup via signal.NotifyContext
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 4. Initialize Database Repository
	repo, err := repository.NewJobRepository(cfg.DSN())
	if err != nil {
		slog.Error("repository initialization failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("closing database connection pool...")
		repo.Close()
	}()

	// 5. Initialize RabbitMQ Broker
	mq, err := broker.NewRabbitMQ(cfg.URL())
	if err != nil {
		slog.Error("broker initialization failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("closing rabbitmq connection and channels...")
		mq.Close()
	}()

	queueName := "new_vacancies"
	_, err = mq.DeclareQueue(queueName)
	if err != nil {
		slog.Error("queue declaration failed", "queue", queueName, "error", err)
		os.Exit(1)
	}

	// 6. Run scraper in batch (RUN_ONCE=true) or daemon mode
	run(ctx, cfg, repo, mq, queueName, djinni.FetchJobs)
}

type JobRepository interface {
	SaveJobIfNotExists(ctx context.Context, job djinni.Job) (bool, error)
}

type JobPublisher interface {
	PublishJob(ctx context.Context, queueName string, job djinni.Job) error
	PublishBatchEOF(ctx context.Context, queueName string) error
}

type JobFetcher func(rssURL string) ([]djinni.Job, error)

func run(
	ctx context.Context,
	cfg *config.Config,
	repo JobRepository,
	mq JobPublisher,
	queueName string,
	fetchJobs JobFetcher,
) {
	slog.Info("executing initial scrape cycle", "run_once", cfg.Scraper.RunOnce)
	runScrapeIteration(ctx, cfg, repo, mq, queueName, fetchJobs)

	if cfg.Scraper.RunOnce {
		if ctx.Err() == nil {
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := mq.PublishBatchEOF(pubCtx, queueName); err != nil {
				slog.Error("failed to publish batch EOF marker", "queue", queueName, "error", err)
			} else {
				slog.Info("published batch EOF marker", "queue", queueName)
			}
			cancel()
		}
		slog.Info("RUN_ONCE=true: scraper batch run completed, exiting cleanly")
		return
	}

	ticker := time.NewTicker(cfg.Scraper.Interval)
	defer ticker.Stop()

	slog.Info("scraper service entered daemon mode", "interval", cfg.Scraper.Interval.String())

	for {
		select {
		case <-ctx.Done():
			slog.Info("termination signal received: starting graceful shutdown...")
			slog.Info("scraper daemon stopped successfully.")
			return

		case <-ticker.C:
			slog.Info("ticker triggered periodic scrape cycle")
			runScrapeIteration(ctx, cfg, repo, mq, queueName, fetchJobs)
		}
	}
}

// runScrapeIteration fetches jobs, deduplicates them in Postgres, and publishes new ones to RabbitMQ
func runScrapeIteration(
	ctx context.Context,
	cfg *config.Config,
	repo JobRepository,
	mq JobPublisher,
	queueName string,
	fetchJobs JobFetcher,
) {
	// Check if the service was already requested to shut down
	if ctx.Err() != nil {
		return
	}

	jobs, err := fetchJobs(cfg.Scraper.RSSURL)
	if err != nil {
		slog.Error("failed to fetch jobs from djinni", "error", err)
		return
	}

	if len(jobs) == 0 {
		slog.Info("no jobs found in RSS feed")
		return
	}

	newJobsCount := 0

	for _, job := range jobs {
		// Stop processing if shutdown signal was intercepted mid-iteration
		if ctx.Err() != nil {
			slog.Warn("scrape iteration interrupted by shutdown signal")
			return
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
				slog.Error("failed to publish job to queue", "guid", job.GUID, "error", err)
				continue
			}

			slog.Info("processed and published new vacancy", "title", job.Title, "guid", job.GUID)
			newJobsCount++
		}
	}

	slog.Info("finished processing cycle", "new_jobs", newJobsCount, "total_scraped", len(jobs))
}
