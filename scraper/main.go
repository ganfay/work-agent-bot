package main

import (
	"context"
	"log/slog"
	"os"
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
	slog.Info("starting scraper service")

	// 3. Initialize Database Repository
	repo, err := repository.NewJobRepository(cfg.DSN())
	if err != nil {
		slog.Error("repository initialization failed", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	// 4. Initialize RabbitMQ Broker
	mq, err := broker.NewRabbitMQ(cfg.URL())
	if err != nil {
		slog.Error("broker initialization failed", "error", err)
		os.Exit(1)
	}
	defer mq.Close()

	queueName := "new_vacancies"
	_, err = mq.DeclareQueue(queueName)
	if err != nil {
		slog.Error("queue declaration failed", "queue", queueName, "error", err)
		os.Exit(1)
	}

	// 5. Fetch jobs from Djinni RSS
	jobs, err := djinni.FetchJobs(cfg.Scraper.RSSURL)
	if err != nil {
		slog.Error("failed to fetch jobs", "error", err)
		os.Exit(1)
	}

	if len(jobs) == 0 {
		slog.Info("no jobs found in RSS feed")
		return
	}

	// 6. Deduplicate and Publish
	newJobsCount := 0
	ctx := context.Background()

	for _, job := range jobs {
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
