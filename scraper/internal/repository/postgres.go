package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"scraper/internal/djinni"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(dsn string) (*JobRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	slog.Info("postgresql connection initialized")
	return &JobRepository{pool: pool}, nil
}

func (r *JobRepository) Close() {
	r.pool.Close()
}

// SaveJobIfNotExists returns true if the job was inserted (is new), and false if it already existed
func (r *JobRepository) SaveJobIfNotExists(ctx context.Context, job djinni.Job) (bool, error) {
	query := `
		INSERT INTO jobs (guid, title, description, status)
		VALUES ($1, $2, $3, 'new')
		ON CONFLICT (guid) DO NOTHING
		RETURNING id;
	`

	var id int
	err := r.pool.QueryRow(ctx, query, job.GUID, job.Title, job.Description).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert job: %w", err)
	}

	return true, nil
}
