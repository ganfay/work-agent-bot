package repository

import (
	"context"
	"fmt"
	"log/slog"
	"scraper/internal/djinni"

	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(dsn string) (*JobRepository, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
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
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert job: %w", err)
	}

	return true, nil
}
