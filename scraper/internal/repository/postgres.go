package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"scraper/internal/djinni"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultQueryTimeout   = 10 * time.Second
)

type pgxPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Ping(ctx context.Context) error
	Close()
}

type poolOpener func(ctx context.Context, dsn string) (pgxPool, error)

var pgPasswordKVPattern = regexp.MustCompile(`(?i)\bpassword\s*=\s*(?:'(?:\\.|[^'])*'|"(?:\\.|[^"])*"|\S+)`)

type sanitizedError struct {
	msg   string
	cause error
}

func (e *sanitizedError) Error() string {
	return e.msg
}

func (e *sanitizedError) Unwrap() error {
	return e.cause
}

func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if schemeIdx := strings.Index(dsn, "://"); schemeIdx >= 0 {
		prefix := dsn[:schemeIdx+3]
		rest := dsn[schemeIdx+3:]
		authEnd := len(rest)
		for _, sep := range []string{"/", "?", "#"} {
			if idx := strings.Index(rest, sep); idx >= 0 && idx < authEnd {
				authEnd = idx
			}
		}
		authority := rest[:authEnd]
		suffix := rest[authEnd:]
		if atIdx := strings.LastIndex(authority, "@"); atIdx >= 0 {
			return prefix + "xxxxx:xxxxx@" + authority[atIdx+1:] + suffix
		}
	}
	return pgPasswordKVPattern.ReplaceAllString(dsn, "password=xxxxx")
}

func sanitizePGError(err error) error {
	if err == nil {
		return nil
	}
	var parseErr *pgconn.ParseConfigError
	if errors.As(err, &parseErr) {
		parseErr.ConnString = redactDSN(parseErr.ConnString)
		msg := err.Error()
		if redacted := redactDSN(msg); redacted != msg {
			return &sanitizedError{
				msg:   redacted,
				cause: parseErr.Unwrap(),
			}
		}
	}
	return err
}

func defaultOpenPool(ctx context.Context, dsn string) (pgxPool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, sanitizePGError(err)
	}
	if cfg.ConnConfig.ConnectTimeout <= 0 {
		cfg.ConnConfig.ConnectTimeout = defaultConnectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, sanitizePGError(err)
	}
	return pool, nil
}

type JobRepository struct {
	pool pgxPool
}

func NewJobRepository(dsn string) (*JobRepository, error) {
	return newJobRepository(dsn, defaultOpenPool)
}

func newJobRepository(dsn string, open poolOpener) (*JobRepository, error) {
	if open == nil {
		open = defaultOpenPool
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultConnectTimeout)
	defer cancel()

	pool, err := open(ctx, dsn)
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
	if r == nil || r.pool == nil {
		return
	}
	r.pool.Close()
}

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return context.WithTimeout(ctx, d)
	}
	return ctx, func() {}
}

// SaveJobIfNotExists returns true if the job was inserted (is new), and false if it already existed.
func (r *JobRepository) SaveJobIfNotExists(ctx context.Context, job djinni.Job) (bool, error) {
	if r == nil || r.pool == nil {
		return false, errors.New("job repository is not initialized")
	}
	if strings.TrimSpace(job.GUID) == "" {
		return false, errors.New("job GUID must not be empty")
	}

	qCtx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	query := `
		INSERT INTO jobs (guid, title, description, status)
		VALUES ($1, $2, $3, 'new')
		ON CONFLICT (guid) DO NOTHING
		RETURNING id;
	`

	var id int
	err := r.pool.QueryRow(qCtx, query, job.GUID, job.Title, job.Description).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert job: %w", err)
	}

	return true, nil
}

// DeleteJobByGUID removes a job record, used as a compensating rollback if broker publishing fails.
func (r *JobRepository) DeleteJobByGUID(ctx context.Context, guid string) error {
	if r == nil || r.pool == nil {
		return errors.New("job repository is not initialized")
	}
	if strings.TrimSpace(guid) == "" {
		return errors.New("job GUID must not be empty")
	}

	qCtx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	_, err := r.pool.Exec(qCtx, `DELETE FROM jobs WHERE guid = $1;`, guid)
	if err != nil {
		return fmt.Errorf("failed to delete job %q: %w", guid, err)
	}
	return nil
}
