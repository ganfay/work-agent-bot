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

	cfg := LoadConfig("/nonexistent/.env")
	if cfg.Scraper.RunOnce {
		t.Fatalf("expected RunOnce default false, got true")
	}

	t.Setenv("RUN_ONCE", "true")
	cfg = LoadConfig("/nonexistent/.env")
	if !cfg.Scraper.RunOnce {
		t.Fatalf("expected RunOnce true when RUN_ONCE=true")
	}
}
