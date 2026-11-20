package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var scraperEnvKeys = []string{
	"PG_USER",
	"PG_PASSWORD",
	"PG_HOST",
	"PG_PORT",
	"PG_DB",
	"PG_SSLMODE",
	"RMQ_USER",
	"RMQ_PASS",
	"RMQ_HOST",
	"RMQ_PORT",
	"DJINNI_RSS_URL",
	"LOG_PATH",
	"SCRAPE_INTERVAL",
	"RUN_ONCE",
}

func isolateScraperEnv(t *testing.T) {
	t.Helper()
	type saved struct {
		val string
		ok  bool
	}
	prev := make(map[string]saved, len(scraperEnvKeys))
	for _, k := range scraperEnvKeys {
		v, ok := os.LookupEnv(k)
		prev[k] = saved{val: v, ok: ok}
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range scraperEnvKeys {
			s := prev[k]
			if s.ok {
				_ = os.Setenv(k, s.val)
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

func setRequiredScraperEnv(t *testing.T) {
	t.Helper()
	isolateScraperEnv(t)
	t.Setenv("PG_USER", "u")
	t.Setenv("PG_PASSWORD", "p")
	t.Setenv("PG_DB", "db")
	t.Setenv("RMQ_USER", "ru")
	t.Setenv("RMQ_PASS", "rp")
	t.Setenv("DJINNI_RSS_URL", "https://example.com/rss")
}

func TestLoadConfig_RunOnceDefaultAndOverride(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequiredScraperEnv(t)

	cfg, err := LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Scraper.RunOnce {
		t.Fatalf("expected RunOnce default false, got true")
	}

	t.Setenv("RUN_ONCE", "true")
	cfg, err = LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if !cfg.Scraper.RunOnce {
		t.Fatalf("expected RunOnce true when RUN_ONCE=true")
	}
}

func TestConfig_URLEncodesSpecialCharactersInCredentials(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequiredScraperEnv(t)
	t.Setenv("PG_PASSWORD", "p@ss:word/1#2")
	t.Setenv("RMQ_PASS", "r@mq:pass/3")

	cfg, err := LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	wantDSN := "postgres://u:p%40ss%3Aword%2F1%232@localhost:5432/db?sslmode=disable"
	if got := cfg.DSN(); got != wantDSN {
		t.Fatalf("unexpected DSN:\n got: %s\nwant: %s", got, wantDSN)
	}
	wantURL := "amqp://ru:r%40mq%3Apass%2F3@localhost:5672/"
	if got := cfg.URL(); got != wantURL {
		t.Fatalf("unexpected AMQP URL:\n got: %s\nwant: %s", got, wantURL)
	}
}

func TestLoadConfig_MissingRequiredEnvReturnsError(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateScraperEnv(t)

	_, err := LoadConfig("/nonexistent/.env")
	if err == nil {
		t.Fatal("expected error when required env vars are missing")
	}
}

func TestLoadConfig_ValidEnvFile(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateScraperEnv(t)

	envFile := filepath.Join(t.TempDir(), ".env")
	content := strings.Join([]string{
		"PG_USER=fileuser",
		"PG_PASSWORD=filepass",
		"PG_HOST=pg-host",
		"PG_PORT=5433",
		"PG_DB=filedb",
		"PG_SSLMODE=require",
		"RMQ_USER=rmquser",
		"RMQ_PASS=rmqpass",
		"RMQ_HOST=rmq-host",
		"RMQ_PORT=5673",
		"DJINNI_RSS_URL=https://djinni.co/jobs/rss/",
		"LOG_PATH=/tmp/test-scraper.log",
		"SCRAPE_INTERVAL=30s",
		"RUN_ONCE=true",
	}, "\n")
	if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	cfg, err := LoadConfig(envFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Database.User != "fileuser" || cfg.Database.Host != "pg-host" || cfg.Database.Port != "5433" {
		t.Fatalf("unexpected database config: %+v", cfg.Database)
	}
	if cfg.Scraper.Interval != 30*time.Second || !cfg.Scraper.RunOnce {
		t.Fatalf("unexpected scraper config: %+v", cfg.Scraper)
	}
	if !strings.HasSuffix(cfg.DSN(), "?sslmode=require") {
		t.Fatalf("expected DSN to end with ?sslmode=require, got %q", cfg.DSN())
	}
}

func TestLoadConfig_InvalidEnvFileReturnsErrorImmediately(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequiredScraperEnv(t)

	envFile := filepath.Join(t.TempDir(), "bad.env")
	if err := os.WriteFile(envFile, []byte("SCRAPE_INTERVAL=invalid\n"), 0o600); err != nil {
		t.Fatalf("write bad env file: %v", err)
	}

	_, err := LoadConfig(envFile)
	if err == nil {
		t.Fatal("expected error from invalid env file, must not fall back to process env")
	}
}

func TestLoadConfig_DotEnvInWorkingDirAndErrorPropagation(t *testing.T) {
	t.Run("loads valid dot env from working directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Chdir(tmpDir)
		isolateScraperEnv(t)

		content := strings.Join([]string{
			"PG_USER=cwduser",
			"PG_PASSWORD=cwdpass",
			"PG_DB=cwddb",
			"RMQ_USER=cwdrmq",
			"RMQ_PASS=cwdrmqpass",
			"DJINNI_RSS_URL=https://djinni.co/jobs/rss/",
		}, "\n")
		if err := os.WriteFile(filepath.Join(tmpDir, ".env"), []byte(content), 0o600); err != nil {
			t.Fatalf("write .env: %v", err)
		}

		cfg, err := LoadConfig("/nonexistent/.env")
		if err != nil {
			t.Fatalf("LoadConfig failed: %v", err)
		}
		if cfg.Database.User != "cwduser" {
			t.Fatalf("expected cwduser, got %q", cfg.Database.User)
		}
	})

	t.Run("propagates invalid dot env error without swallowing", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Chdir(tmpDir)
		setRequiredScraperEnv(t)

		if err := os.WriteFile(filepath.Join(tmpDir, ".env"), []byte("SCRAPE_INTERVAL=not-a-duration\n"), 0o600); err != nil {
			t.Fatalf("write .env: %v", err)
		}

		_, err := LoadConfig("/nonexistent/.env")
		if err == nil {
			t.Fatal("expected error for invalid .env in working dir")
		}
	})
}

func TestLoadConfig_RSSURLValidation(t *testing.T) {
	tests := []struct {
		name    string
		rssURL  string
		wantErr bool
	}{
		{name: "empty", rssURL: "", wantErr: true},
		{name: "whitespace", rssURL: "   ", wantErr: true},
		{name: "not a url", rssURL: "not-a-url", wantErr: true},
		{name: "ftp scheme", rssURL: "ftp://djinni.co/rss", wantErr: true},
		{name: "file scheme", rssURL: "file:///etc/passwd", wantErr: true},
		{name: "empty host", rssURL: "https:///rss", wantErr: true},
		{name: "empty hostname with port", rssURL: "http://:8080/rss", wantErr: true},
		{name: "valid http", rssURL: "http://example.com/rss", wantErr: false},
		{name: "valid https with query", rssURL: "https://djinni.co/jobs/rss/?primary_keyword=Golang", wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setRequiredScraperEnv(t)
			t.Setenv("DJINNI_RSS_URL", tc.rssURL)

			cfg, err := LoadConfig("/nonexistent/.env")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for DJINNI_RSS_URL=%q, got nil", tc.rssURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for DJINNI_RSS_URL=%q: %v", tc.rssURL, err)
			}
			if cfg.Scraper.RSSURL != tc.rssURL {
				t.Fatalf("expected RSSURL=%q, got %q", tc.rssURL, cfg.Scraper.RSSURL)
			}
		})
	}
}

func TestConfig_PGSSLModeDefaultAndOverride(t *testing.T) {
	t.Run("default disable", func(t *testing.T) {
		t.Chdir(t.TempDir())
		setRequiredScraperEnv(t)

		cfg, err := LoadConfig("/nonexistent/.env")
		if err != nil {
			t.Fatalf("LoadConfig failed: %v", err)
		}
		if cfg.Database.SSLMode != "disable" {
			t.Fatalf("expected default sslmode disable, got %q", cfg.Database.SSLMode)
		}
		if !strings.Contains(cfg.DSN(), "sslmode=disable") {
			t.Fatalf("expected sslmode=disable in DSN %q", cfg.DSN())
		}
	})

	t.Run("override require", func(t *testing.T) {
		t.Chdir(t.TempDir())
		setRequiredScraperEnv(t)
		t.Setenv("PG_SSLMODE", "require")

		cfg, err := LoadConfig("/nonexistent/.env")
		if err != nil {
			t.Fatalf("LoadConfig failed: %v", err)
		}
		if cfg.Database.SSLMode != "require" {
			t.Fatalf("expected sslmode require, got %q", cfg.Database.SSLMode)
		}
		if !strings.Contains(cfg.DSN(), "sslmode=require") {
			t.Fatalf("expected sslmode=require in DSN %q", cfg.DSN())
		}
	})

	t.Run("empty string env normalized to disable", func(t *testing.T) {
		t.Chdir(t.TempDir())
		setRequiredScraperEnv(t)
		t.Setenv("PG_SSLMODE", "   ")

		cfg, err := LoadConfig("/nonexistent/.env")
		if err != nil {
			t.Fatalf("LoadConfig failed: %v", err)
		}
		if cfg.Database.SSLMode != "disable" {
			t.Fatalf("expected empty sslmode to normalize to disable, got %q", cfg.Database.SSLMode)
		}
	})

	t.Run("zero value Config DSN defaults sslmode=disable", func(t *testing.T) {
		cfg := &Config{}
		if !strings.Contains(cfg.DSN(), "sslmode=disable") {
			t.Fatalf("expected zero-value DSN to contain sslmode=disable, got %q", cfg.DSN())
		}
	})
}
