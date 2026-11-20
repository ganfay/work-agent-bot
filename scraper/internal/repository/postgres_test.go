package repository

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"scraper/internal/djinni"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeRow struct {
	id  int
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if ptr, ok := dest[0].(*int); ok {
			*ptr = r.id
		}
	}
	return nil
}

type fakePool struct {
	mu              sync.Mutex
	pingErr         error
	queryRowErr     error
	queryRowID      int
	execErr         error
	closeCount      int
	queryRowCalls   int
	execCalls       int
	lastQueryArgs   []any
	lastExecArgs    []any
	lastHasDeadline bool
	lastDeadline    time.Time
}

func (p *fakePool) QueryRow(ctx context.Context, _ string, args ...any) pgx.Row {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queryRowCalls++
	p.lastQueryArgs = append([]any(nil), args...)
	p.lastDeadline, p.lastHasDeadline = ctx.Deadline()
	if p.queryRowErr != nil {
		return fakeRow{err: p.queryRowErr}
	}
	return fakeRow{id: p.queryRowID}
}

func (p *fakePool) Exec(ctx context.Context, _ string, arguments ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execCalls++
	p.lastExecArgs = append([]any(nil), arguments...)
	p.lastDeadline, p.lastHasDeadline = ctx.Deadline()
	return pgconn.CommandTag{}, p.execErr
}

func (p *fakePool) Ping(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pingErr
}

func (p *fakePool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeCount++
}

func TestNewJobRepository_InvalidDSN(t *testing.T) {
	_, err := NewJobRepository("://bad-dsn")
	if err == nil {
		t.Fatal("expected error for malformed DSN")
	}
}

func TestNewJobRepository_MalformedDSNRedactsCredentials(t *testing.T) {
	t.Run("non-postgres scheme with multiple colons in password", func(t *testing.T) {
		_, err := NewJobRepository("pg://u:sec:ret@localhost/db")
		if err == nil {
			t.Fatal("expected error for malformed DSN, got nil")
		}
		if strings.Contains(err.Error(), "sec") || strings.Contains(err.Error(), "ret") {
			t.Fatalf("expected malformed DSN credentials redacted from error, got %q", err.Error())
		}
		var parseErr *pgconn.ParseConfigError
		if errors.As(err, &parseErr) {
			if strings.Contains(parseErr.ConnString, "sec") || strings.Contains(parseErr.ConnString, "ret") {
				t.Fatalf("expected parseErr.ConnString redacted, got %q", parseErr.ConnString)
			}
		}
	})

	t.Run("keyword-value DSN with invalid port redacts password", func(t *testing.T) {
		_, err := NewJobRepository("host=localhost user=u password=SUPER_PG_SECRET port=badport")
		if err == nil {
			t.Fatal("expected error for invalid keyword-value DSN, got nil")
		}
		if strings.Contains(err.Error(), "SUPER_PG_SECRET") {
			t.Fatalf("expected keyword-value password redacted from error, got %q", err.Error())
		}
		var parseErr *pgconn.ParseConfigError
		if errors.As(err, &parseErr) && strings.Contains(parseErr.ConnString, "SUPER_PG_SECRET") {
			t.Fatalf("expected parseErr.ConnString redacted, got %q", parseErr.ConnString)
		}
	})
}

func TestNewJobRepository_OpenerAndPingHandling(t *testing.T) {
	t.Run("opener error propagates", func(t *testing.T) {
		wantErr := errors.New("dial failed")
		_, err := newJobRepository("postgres://u:p@localhost:5432/db", func(_ context.Context, _ string) (pgxPool, error) {
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected error %v, got %v", wantErr, err)
		}
	})

	t.Run("ping failure closes pool and returns error", func(t *testing.T) {
		wantErr := errors.New("ping timeout")
		pool := &fakePool{pingErr: wantErr}
		_, err := newJobRepository("postgres://u:p@localhost:5432/db", func(_ context.Context, _ string) (pgxPool, error) {
			return pool, nil
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected ping error %v, got %v", wantErr, err)
		}
		if pool.closeCount != 1 {
			t.Fatalf("expected pool.Close() called once on ping failure, got %d", pool.closeCount)
		}
	})

	t.Run("success initializes repository and Close delegates to pool", func(t *testing.T) {
		pool := &fakePool{}
		repo, err := newJobRepository("postgres://u:p@localhost:5432/db", func(_ context.Context, _ string) (pgxPool, error) {
			return pool, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		repo.Close()
		if pool.closeCount != 1 {
			t.Fatalf("expected pool.Close() called once, got %d", pool.closeCount)
		}
	})
}

func TestJobRepository_NilSafety(t *testing.T) {
	var nilRepo *JobRepository
	nilRepo.Close()

	zeroRepo := &JobRepository{}
	zeroRepo.Close()

	if _, err := nilRepo.SaveJobIfNotExists(context.Background(), djinni.Job{GUID: "g-1"}); err == nil {
		t.Fatal("expected error on nil repository SaveJobIfNotExists")
	}
	if _, err := zeroRepo.SaveJobIfNotExists(context.Background(), djinni.Job{GUID: "g-1"}); err == nil {
		t.Fatal("expected error on nil pool SaveJobIfNotExists")
	}
	if err := nilRepo.DeleteJobByGUID(context.Background(), "g-1"); err == nil {
		t.Fatal("expected error on nil repository DeleteJobByGUID")
	}
	if err := zeroRepo.DeleteJobByGUID(context.Background(), "g-1"); err == nil {
		t.Fatal("expected error on nil pool DeleteJobByGUID")
	}
}

func TestSaveJobIfNotExists_BehaviorAndTimeouts(t *testing.T) {
	t.Run("inserted new job", func(t *testing.T) {
		pool := &fakePool{queryRowID: 42}
		repo := &JobRepository{pool: pool}
		job := djinni.Job{GUID: "guid-1", Title: "Go Dev", Description: "Desc"}

		isNew, err := repo.SaveJobIfNotExists(context.Background(), job)
		if err != nil {
			t.Fatalf("SaveJobIfNotExists failed: %v", err)
		}
		if !isNew {
			t.Fatal("expected isNew=true")
		}
		if len(pool.lastQueryArgs) != 3 || pool.lastQueryArgs[0] != "guid-1" || pool.lastQueryArgs[1] != "Go Dev" || pool.lastQueryArgs[2] != "Desc" {
			t.Fatalf("unexpected query args: %+v", pool.lastQueryArgs)
		}
		if !pool.lastHasDeadline {
			t.Fatal("expected default timeout deadline applied when ctx has none")
		}
	})

	t.Run("conflict returns false, nil", func(t *testing.T) {
		pool := &fakePool{queryRowErr: pgx.ErrNoRows}
		repo := &JobRepository{pool: pool}

		isNew, err := repo.SaveJobIfNotExists(context.Background(), djinni.Job{GUID: "dup-1"})
		if err != nil {
			t.Fatalf("expected nil error on conflict, got %v", err)
		}
		if isNew {
			t.Fatal("expected isNew=false on conflict")
		}
	})

	t.Run("database error propagates", func(t *testing.T) {
		wantErr := errors.New("connection reset")
		pool := &fakePool{queryRowErr: wantErr}
		repo := &JobRepository{pool: pool}

		isNew, err := repo.SaveJobIfNotExists(context.Background(), djinni.Job{GUID: "g-1"})
		if !errors.Is(err, wantErr) || isNew {
			t.Fatalf("expected (false, %v), got (%v, %v)", wantErr, isNew, err)
		}
	})

	t.Run("empty or whitespace GUID rejected without querying", func(t *testing.T) {
		pool := &fakePool{}
		repo := &JobRepository{pool: pool}

		for _, guid := range []string{"", "   "} {
			_, err := repo.SaveJobIfNotExists(context.Background(), djinni.Job{GUID: guid})
			if err == nil || !strings.Contains(err.Error(), "GUID") {
				t.Fatalf("expected GUID validation error for %q, got %v", guid, err)
			}
		}
		if pool.queryRowCalls != 0 {
			t.Fatalf("expected 0 DB queries on invalid GUID, got %d", pool.queryRowCalls)
		}
	})

	t.Run("preserves shorter caller deadline", func(t *testing.T) {
		pool := &fakePool{queryRowID: 1}
		repo := &JobRepository{pool: pool}

		shortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		wantDeadline, _ := shortCtx.Deadline()

		if _, err := repo.SaveJobIfNotExists(shortCtx, djinni.Job{GUID: "g-deadline"}); err != nil {
			t.Fatalf("SaveJobIfNotExists failed: %v", err)
		}
		if !pool.lastHasDeadline || !pool.lastDeadline.Equal(wantDeadline) {
			t.Fatalf("expected caller deadline %v preserved, got %v (has=%v)", wantDeadline, pool.lastDeadline, pool.lastHasDeadline)
		}
	})
}

func TestDeleteJobByGUID_BehaviorAndTimeouts(t *testing.T) {
	t.Run("deletes by guid and applies default timeout", func(t *testing.T) {
		pool := &fakePool{}
		repo := &JobRepository{pool: pool}

		if err := repo.DeleteJobByGUID(context.Background(), "guid-del"); err != nil {
			t.Fatalf("DeleteJobByGUID failed: %v", err)
		}
		if len(pool.lastExecArgs) != 1 || pool.lastExecArgs[0] != "guid-del" {
			t.Fatalf("unexpected exec args: %+v", pool.lastExecArgs)
		}
		if !pool.lastHasDeadline {
			t.Fatal("expected default timeout deadline applied when ctx has none")
		}
	})

	t.Run("database error propagates", func(t *testing.T) {
		wantErr := errors.New("exec error")
		pool := &fakePool{execErr: wantErr}
		repo := &JobRepository{pool: pool}

		err := repo.DeleteJobByGUID(context.Background(), "guid-del")
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected %v, got %v", wantErr, err)
		}
	})

	t.Run("empty or whitespace GUID rejected without calling Exec", func(t *testing.T) {
		pool := &fakePool{}
		repo := &JobRepository{pool: pool}

		for _, guid := range []string{"", "  \t "} {
			if err := repo.DeleteJobByGUID(context.Background(), guid); err == nil {
				t.Fatalf("expected error for empty GUID %q", guid)
			}
		}
		if pool.execCalls != 0 {
			t.Fatalf("expected 0 Exec calls on invalid GUID, got %d", pool.execCalls)
		}
	})
}
