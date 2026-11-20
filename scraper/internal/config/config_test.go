package config

import (
	"testing"
)

func setRequiredScraperEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PG_USER", "u")
	t.Setenv("PG_PASSWORD", "p")
	t.Setenv("PG_DB", "db")
	t.Setenv("RMQ_USER", "ru")
	t.Setenv("RMQ_PASS", "rp")
	t.Setenv("DJINNI_RSS_URL", "https://example.com/rss")
}

func TestLoadConfig_RunOnceDefaultAndOverride(t *testing.T) {
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
