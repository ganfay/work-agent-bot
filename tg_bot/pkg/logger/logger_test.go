package logger

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func preserveDefaultLogger(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(prev)
	})
}

func TestSetup_CreatesDirectoryWritesLogsAndCloses(t *testing.T) {
	preserveDefaultLogger(t)

	logPath := filepath.Join(t.TempDir(), "nested", "logs", "tg_bot.log")
	l, closer, err := Setup(logPath)
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	if l == nil || closer == nil {
		t.Fatalf("expected non-nil logger and closer")
	}

	l.Info("hello bot logger", "component", "tg_bot")
	if err := closer.Close(); err != nil {
		t.Fatalf("closer.Close() returned error: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), "hello bot logger") {
		t.Fatalf("expected log message in file, got: %s", string(data))
	}
}

func TestSetup_EmptyPathReturnsError(t *testing.T) {
	preserveDefaultLogger(t)

	for _, p := range []string{"", "   ", "\t\n"} {
		if _, _, err := Setup(p); err == nil {
			t.Fatalf("expected error for empty path %q", p)
		}
	}
}

func TestSetup_UnwritableParentReturnsError(t *testing.T) {
	preserveDefaultLogger(t)

	blockingFile := filepath.Join(t.TempDir(), "not_a_dir")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("create blocking file: %v", err)
	}

	if _, _, err := Setup(filepath.Join(blockingFile, "tg_bot.log")); err == nil {
		t.Fatalf("expected error when parent path is a regular file")
	}
}

func TestSetup_ConcurrentLoggingRaceSafe(t *testing.T) {
	preserveDefaultLogger(t)

	logPath := filepath.Join(t.TempDir(), "concurrent.log")
	l, closer, err := Setup(logPath)
	if err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}

	const workers = 10
	const perWorker = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := range workers {
		go func(id int) {
			defer wg.Done()
			for i := range perWorker {
				l.Info("concurrent log entry", "worker", id, "seq", i)
			}
		}(g)
	}
	wg.Wait()

	if err := closer.Close(); err != nil {
		t.Fatalf("closer.Close() error: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Count(strings.TrimSpace(string(data)), "concurrent log entry")
	if lines != workers*perWorker {
		t.Fatalf("expected %d log entries, got %d", workers*perWorker, lines)
	}
}
