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

	logPath := filepath.Join(t.TempDir(), "nested", "dir", "scraper.log")
	l, closer, err := Setup(logPath)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	if l == nil || closer == nil {
		t.Fatalf("expected non-nil logger and closer")
	}
	if slog.Default() != l {
		t.Fatalf("expected slog.Default() to be set to returned logger")
	}

	l.Info("hello logger", "k", "v")
	if err := closer.Close(); err != nil {
		t.Fatalf("closer.Close failed: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "hello logger") || !strings.Contains(got, "k=v") {
		t.Fatalf("expected log line in file, got %q", got)
	}
}

func TestSetup_EmptyPathReturnsError(t *testing.T) {
	preserveDefaultLogger(t)

	for _, path := range []string{"", "   "} {
		_, _, err := Setup(path)
		if err == nil {
			t.Fatalf("expected error for empty log path %q", path)
		}
	}
}

func TestSetup_UnwritableParentReturnsError(t *testing.T) {
	preserveDefaultLogger(t)

	parentFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker file: %v", err)
	}

	_, _, err := Setup(filepath.Join(parentFile, "scraper.log"))
	if err == nil {
		t.Fatal("expected error when parent path is a regular file")
	}
}

func TestSetup_ConcurrentLoggingRaceSafe(t *testing.T) {
	preserveDefaultLogger(t)

	logPath := filepath.Join(t.TempDir(), "concurrent.log")
	l, closer, err := Setup(logPath)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				l.Info("concurrent entry", "goroutine", id, "iter", i)
			}
		}(g)
	}
	wg.Wait()

	if err := closer.Close(); err != nil {
		t.Fatalf("closer.Close failed: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if count := strings.Count(string(data), "concurrent entry"); count != 200 {
		t.Fatalf("expected 200 log entries, got %d", count)
	}
}
