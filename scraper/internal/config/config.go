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
	Database struct {
		User     string `env:"PG_USER" env-required:"true"`
		Password string `env:"PG_PASSWORD" env-required:"true"`
		Host     string `env:"PG_HOST" env-default:"localhost"`
		Port     string `env:"PG_PORT" env-default:"5432"`
		Name     string `env:"PG_DB" env-required:"true"`
	}
	RabbitMQ struct {
		User     string `env:"RMQ_USER" env-required:"true"`
		Password string `env:"RMQ_PASS" env-required:"true"`
		Host     string `env:"RMQ_HOST" env-default:"localhost"`
		Port     string `env:"RMQ_PORT" env-default:"5672"`
	}
	Scraper struct {
		RSSURL   string        `env:"DJINNI_RSS_URL" env-required:"true"`
		LogPath  string        `env:"LOG_PATH" env-default:"logs/scraper.log"`
		Interval time.Duration `env:"SCRAPE_INTERVAL" env-default:"15m"`
		RunOnce  bool          `env:"RUN_ONCE" env-default:"false"`
	}
}

// DSN builds the PostgreSQL connection string with properly URL-encoded credentials.
func (c *Config) DSN() string {
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.Database.User, c.Database.Password),
		Host:     net.JoinHostPort(c.Database.Host, c.Database.Port),
		Path:     "/" + c.Database.Name,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// URL builds the RabbitMQ connection string with properly URL-encoded credentials.
func (c *Config) URL() string {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.RabbitMQ.User, c.RabbitMQ.Password),
		Host:   net.JoinHostPort(c.RabbitMQ.Host, c.RabbitMQ.Port),
		Path:   "/",
	}
	return u.String()
}

// LoadConfig parses the .env file or reads directly from environment variables
func LoadConfig(envPath string) *Config {
	var cfg Config
	var err error

	// 1. Try provided path if exists
	if envPath != "" {
		if _, statErr := os.Stat(envPath); statErr == nil {
			if err = cleanenv.ReadConfig(envPath, &cfg); err == nil {
				return &cfg
			}
		}
	}

	// 2. Try current directory .env if exists
	if _, statErr := os.Stat(".env"); statErr == nil {
		if err = cleanenv.ReadConfig(".env", &cfg); err == nil {
			return &cfg
		}
	}

	// 3. Fallback to process environment variables (standard in Docker / Prod)
	err = cleanenv.ReadEnv(&cfg)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}
	return &cfg
}
