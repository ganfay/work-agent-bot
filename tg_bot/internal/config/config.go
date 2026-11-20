package config

import (
	"log/slog"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Telegram struct {
		BotToken string `env:"TG_BOT_TOKEN" env-required:"true"`
		UserID   int64  `env:"TG_USER_ID" env-required:"true"`
	}
	RabbitMQ struct {
		User     string `env:"RMQ_USER" env-required:"true"`
		Password string `env:"RMQ_PASS" env-required:"true"`
		Host     string `env:"RMQ_HOST" env-default:"localhost"`
		Port     string `env:"RMQ_PORT" env-default:"5672"`
	}
	Bot struct {
		LogPath          string        `env:"TG_LOG_PATH" env-default:"logs/tg_bot.log"`
		RunOnce          bool          `env:"RUN_ONCE" env-default:"false"`
		BatchIdleTimeout time.Duration `env:"TG_BATCH_IDLE_TIMEOUT" env-default:"90s"`
		PollGrace        time.Duration `env:"TG_POLL_GRACE" env-default:"15s"`
	}
}

func (c *Config) RabbitMQURL() string {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.RabbitMQ.User, c.RabbitMQ.Password),
		Host:   net.JoinHostPort(c.RabbitMQ.Host, c.RabbitMQ.Port),
		Path:   "/",
	}
	return u.String()
}

func (c *Config) normalize() {
	if c.Bot.BatchIdleTimeout <= 0 {
		c.Bot.BatchIdleTimeout = 90 * time.Second
	}
	if c.Bot.PollGrace < 0 {
		c.Bot.PollGrace = 0
	}
}

func LoadConfig(envPath string) *Config {
	var cfg Config
	var err error

	// 1. Try provided path if exists
	if envPath != "" {
		if _, statErr := os.Stat(envPath); statErr == nil {
			if err = cleanenv.ReadConfig(envPath, &cfg); err == nil {
				cfg.normalize()
				return &cfg
			}
		}
	}

	// 2. Try current directory .env if exists
	if _, statErr := os.Stat(".env"); statErr == nil {
		if err = cleanenv.ReadConfig(".env", &cfg); err == nil {
			cfg.normalize()
			return &cfg
		}
	}

	// 3. Fallback to process environment variables (standard in Docker / Prod)
	err = cleanenv.ReadEnv(&cfg)
	if err != nil {
		slog.Error("failed to load bot configuration", "error", err)
		os.Exit(1)
	}
	cfg.normalize()
	return &cfg
}
