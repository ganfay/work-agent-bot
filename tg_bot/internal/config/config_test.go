package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func isolateBotEnv(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	for _, key := range []string{
		"TG_BOT_TOKEN",
		"TG_USER_ID",
		"RMQ_USER",
		"RMQ_PASS",
		"RMQ_HOST",
		"RMQ_PORT",
		"TG_LOG_PATH",
		"RUN_ONCE",
		"TG_BATCH_IDLE_TIMEOUT",
		"TG_POLL_GRACE",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func setRequiredBotEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TG_BOT_TOKEN", "tok")
	t.Setenv("TG_USER_ID", "12345")
	t.Setenv("RMQ_USER", "ru")
	t.Setenv("RMQ_PASS", "rp")
}

func TestLoadConfig_BatchDefaultsAndOverrides(t *testing.T) {
	isolateBotEnv(t)
	setRequiredBotEnv(t)

	cfg, err := LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("unexpected LoadConfig error: %v", err)
	}
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
	cfg, err = LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("unexpected LoadConfig override error: %v", err)
	}
	if !cfg.Bot.RunOnce || cfg.Bot.BatchIdleTimeout != 45*time.Second || cfg.Bot.PollGrace != 20*time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg.Bot)
	}
}

func TestConfig_URLEncodesPasswordAndNormalizesDurations(t *testing.T) {
	isolateBotEnv(t)
	setRequiredBotEnv(t)
	t.Setenv("RMQ_PASS", "p@ss:word/1")
	t.Setenv("TG_BATCH_IDLE_TIMEOUT", "0s")
	t.Setenv("TG_POLL_GRACE", "-5s")

	cfg, err := LoadConfig("/nonexistent/.env")
	if err != nil {
		t.Fatalf("unexpected LoadConfig error: %v", err)
	}
	wantURL := "amqp://ru:p%40ss%3Aword%2F1@localhost:5672/"
	if got := cfg.RabbitMQURL(); got != wantURL {
		t.Fatalf("unexpected RabbitMQURL:\n got: %s\nwant: %s", got, wantURL)
	}
	if cfg.Bot.BatchIdleTimeout != 90*time.Second {
		t.Fatalf("expected non-positive idle timeout to normalize to 90s, got %v", cfg.Bot.BatchIdleTimeout)
	}
	if cfg.Bot.PollGrace != 0 {
		t.Fatalf("expected negative poll grace to clamp to 0, got %v", cfg.Bot.PollGrace)
	}
}

func TestLoadConfig_MissingRequiredEnvReturnsError(t *testing.T) {
	isolateBotEnv(t)

	if _, err := LoadConfig("/nonexistent/.env"); err == nil {
		t.Fatalf("expected error when required env vars are missing")
	}
}

func TestLoadConfig_ValidEnvFile(t *testing.T) {
	t.Run("explicit envPath", func(t *testing.T) {
		isolateBotEnv(t)
		dir := t.TempDir()
		envFile := filepath.Join(dir, "custom.env")
		content := strings.Join([]string{
			"TG_BOT_TOKEN=123456:ABC-DEF",
			"TG_USER_ID=987654",
			"RMQ_USER=botuser",
			"RMQ_PASS=botpass",
			"RMQ_HOST=rabbitmq",
			"RMQ_PORT=5673",
			"TG_LOG_PATH=logs/custom.log",
		}, "\n")
		if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
			t.Fatalf("write env file: %v", err)
		}

		cfg, err := LoadConfig(envFile)
		if err != nil {
			t.Fatalf("unexpected LoadConfig error: %v", err)
		}
		if cfg.Telegram.UserID != 987654 || cfg.RabbitMQ.Host != "rabbitmq" || cfg.RabbitMQ.Port != "5673" {
			t.Fatalf("unexpected loaded config: %+v", cfg)
		}
	})

	t.Run("working directory .env", func(t *testing.T) {
		isolateBotEnv(t)
		content := strings.Join([]string{
			"TG_BOT_TOKEN=123456:XYZ",
			"TG_USER_ID=555",
			"RMQ_USER=u",
			"RMQ_PASS=p",
		}, "\n")
		if err := os.WriteFile(".env", []byte(content), 0o600); err != nil {
			t.Fatalf("write .env: %v", err)
		}

		cfg, err := LoadConfig("/nonexistent/.env")
		if err != nil {
			t.Fatalf("unexpected LoadConfig error from .env: %v", err)
		}
		if cfg.Telegram.UserID != 555 || cfg.Telegram.BotToken != "123456:XYZ" {
			t.Fatalf("unexpected config from .env: %+v", cfg)
		}
	})
}

func TestLoadConfig_InvalidEnvFileReturnsErrorImmediately(t *testing.T) {
	t.Run("malformed explicit envPath does not fall back to process env", func(t *testing.T) {
		isolateBotEnv(t)
		setRequiredBotEnv(t)
		envFile := filepath.Join(t.TempDir(), "bad.env")
		if err := os.WriteFile(envFile, []byte("TG_USER_ID=notanint\n"), 0o600); err != nil {
			t.Fatalf("write bad.env: %v", err)
		}

		if _, err := LoadConfig(envFile); err == nil {
			t.Fatalf("expected error for invalid explicit env file instead of falling back to process env")
		}
	})

	t.Run("malformed working directory .env does not fall back to process env", func(t *testing.T) {
		isolateBotEnv(t)
		setRequiredBotEnv(t)
		if err := os.WriteFile(".env", []byte("TG_USER_ID=0\nTG_BOT_TOKEN=tok\nRMQ_USER=u\nRMQ_PASS=p\n"), 0o600); err != nil {
			t.Fatalf("write .env: %v", err)
		}

		if _, err := LoadConfig("/nonexistent/.env"); err == nil {
			t.Fatalf("expected error for invalid working-dir .env instead of falling back to process env")
		}
	})
}

func TestLoadConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, cfg *Config)
	}{
		{name: "TG_USER_ID zero", env: map[string]string{"TG_USER_ID": "0"}, wantErr: true},
		{name: "TG_USER_ID negative", env: map[string]string{"TG_USER_ID": "-1"}, wantErr: true},
		{name: "TG_USER_ID negative forty-two", env: map[string]string{"TG_USER_ID": "-42"}, wantErr: true},
		{name: "TG_USER_ID non-int", env: map[string]string{"TG_USER_ID": "notanint"}, wantErr: true},
		{name: "TG_USER_ID positive", env: map[string]string{"TG_USER_ID": "12345"}, wantErr: false},
		{name: "TG_BOT_TOKEN empty", env: map[string]string{"TG_BOT_TOKEN": ""}, wantErr: true},
		{name: "TG_BOT_TOKEN whitespace", env: map[string]string{"TG_BOT_TOKEN": "   "}, wantErr: true},
		{name: "TG_BOT_TOKEN internal space", env: map[string]string{"TG_BOT_TOKEN": "tok en"}, wantErr: true},
		{name: "RMQ_USER empty", env: map[string]string{"RMQ_USER": ""}, wantErr: true},
		{name: "RMQ_USER whitespace", env: map[string]string{"RMQ_USER": "   "}, wantErr: true},
		{name: "RMQ_PASS empty", env: map[string]string{"RMQ_PASS": ""}, wantErr: true},
		{name: "RMQ_HOST empty", env: map[string]string{"RMQ_HOST": ""}, wantErr: true},
		{name: "RMQ_HOST whitespace", env: map[string]string{"RMQ_HOST": "   "}, wantErr: true},
		{name: "RMQ_HOST space", env: map[string]string{"RMQ_HOST": "rmq host"}, wantErr: true},
		{name: "RMQ_HOST at", env: map[string]string{"RMQ_HOST": "u@host"}, wantErr: true},
		{name: "RMQ_HOST slash", env: map[string]string{"RMQ_HOST": "host/vhost"}, wantErr: true},
		{name: "RMQ_HOST query", env: map[string]string{"RMQ_HOST": "host?a=1"}, wantErr: true},
		{name: "RMQ_HOST fragment", env: map[string]string{"RMQ_HOST": "host#f"}, wantErr: true},
		{name: "RMQ_PORT empty", env: map[string]string{"RMQ_PORT": ""}, wantErr: true},
		{name: "RMQ_PORT whitespace", env: map[string]string{"RMQ_PORT": "   "}, wantErr: true},
		{name: "RMQ_PORT zero", env: map[string]string{"RMQ_PORT": "0"}, wantErr: true},
		{name: "RMQ_PORT negative", env: map[string]string{"RMQ_PORT": "-1"}, wantErr: true},
		{name: "RMQ_PORT overflow", env: map[string]string{"RMQ_PORT": "65536"}, wantErr: true},
		{name: "RMQ_PORT badport", env: map[string]string{"RMQ_PORT": "badport"}, wantErr: true},
		{name: "RMQ_PORT plus sign", env: map[string]string{"RMQ_PORT": "+5672"}, wantErr: true},
		{name: "RMQ_PORT decimal", env: map[string]string{"RMQ_PORT": "56.72"}, wantErr: true},
		{name: "RMQ_PORT min valid", env: map[string]string{"RMQ_PORT": "1"}, wantErr: false},
		{name: "RMQ_PORT max valid", env: map[string]string{"RMQ_PORT": "65535"}, wantErr: false},
		{
			name:    "RMQ_PORT padded normalized",
			env:     map[string]string{"RMQ_PORT": " 5672 "},
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				if cfg.RabbitMQ.Port != "5672" {
					t.Fatalf("expected trimmed port 5672, got %q", cfg.RabbitMQ.Port)
				}
			},
		},
		{name: "TG_LOG_PATH empty", env: map[string]string{"TG_LOG_PATH": ""}, wantErr: true},
		{name: "TG_LOG_PATH whitespace", env: map[string]string{"TG_LOG_PATH": "   "}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateBotEnv(t)
			setRequiredBotEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg, err := LoadConfig("/nonexistent/.env")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected validation error, got nil (cfg=%+v)", cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

func TestLoadConfig_ErrorsDoNotLeakSecrets(t *testing.T) {
	const secretToken = "SECRET_TG_TOKEN_VALUE"
	const secretPass = "SECRET_RMQ_PASS_VALUE"

	t.Run("invalid RMQ_PORT does not leak secrets", func(t *testing.T) {
		isolateBotEnv(t)
		setRequiredBotEnv(t)
		t.Setenv("TG_BOT_TOKEN", secretToken)
		t.Setenv("RMQ_PASS", secretPass)
		t.Setenv("RMQ_PORT", "badport")

		_, err := LoadConfig("/nonexistent/.env")
		if err == nil {
			t.Fatalf("expected error")
		}
		if strings.Contains(err.Error(), secretToken) || strings.Contains(err.Error(), secretPass) {
			t.Fatalf("error leaked secret: %v", err)
		}
	})

	t.Run("invalid TG_USER_ID does not leak secrets", func(t *testing.T) {
		isolateBotEnv(t)
		setRequiredBotEnv(t)
		t.Setenv("TG_BOT_TOKEN", secretToken)
		t.Setenv("RMQ_PASS", secretPass)
		t.Setenv("TG_USER_ID", "0")

		_, err := LoadConfig("/nonexistent/.env")
		if err == nil {
			t.Fatalf("expected error")
		}
		if strings.Contains(err.Error(), secretToken) || strings.Contains(err.Error(), secretPass) {
			t.Fatalf("error leaked secret: %v", err)
		}
	})

	t.Run("token whitespace error does not leak token", func(t *testing.T) {
		isolateBotEnv(t)
		setRequiredBotEnv(t)
		t.Setenv("TG_BOT_TOKEN", "SECRET_TOK EN")

		_, err := LoadConfig("/nonexistent/.env")
		if err == nil {
			t.Fatalf("expected error")
		}
		if strings.Contains(err.Error(), "SECRET_TOK") {
			t.Fatalf("error leaked token: %v", err)
		}
	})
}
