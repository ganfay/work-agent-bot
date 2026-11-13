package config

import (
	"testing"
	"time"
)

func setRequiredBotEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TG_BOT_TOKEN", "tok")
	t.Setenv("TG_USER_ID", "12345")
	t.Setenv("RMQ_USER", "ru")
	t.Setenv("RMQ_PASS", "rp")
}

func TestLoadConfig_BatchDefaultsAndOverrides(t *testing.T) {
	setRequiredBotEnv(t)

	cfg := LoadConfig("/nonexistent/.env")
	if cfg.Bot.RunOnce {
		t.Fatalf("expected RunOnce default false")
	}
	if cfg.Bot.BatchIdleTimeout != 90*time.Second {
		t.Fatalf("expected BatchIdleTimeout default 90s, got %v", cfg.Bot.BatchIdleTimeout)
	}
	if cfg.Bot.PollGrace != 15*time.Second {
		t.Fatalf("expected PollGrace default 15s, got %v", cfg.Bot.PollGrace)
	}

	t.Setenv("RUN_ONCE", "true")
	t.Setenv("TG_BATCH_IDLE_TIMEOUT", "45s")
	t.Setenv("TG_POLL_GRACE", "20s")
	cfg = LoadConfig("/nonexistent/.env")
	if !cfg.Bot.RunOnce || cfg.Bot.BatchIdleTimeout != 45*time.Second || cfg.Bot.PollGrace != 20*time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg.Bot)
	}
}
