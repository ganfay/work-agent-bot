package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
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
		SSLMode  string `env:"PG_SSLMODE" env-default:"disable"`
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
	sslMode := strings.TrimSpace(c.Database.SSLMode)
	if sslMode == "" {
		sslMode = "disable"
	}
	q := url.Values{}
	q.Set("sslmode", sslMode)

	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.Database.User, c.Database.Password),
		Host:     net.JoinHostPort(c.Database.Host, c.Database.Port),
		Path:     "/" + c.Database.Name,
		RawQuery: q.Encode(),
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

// Validate normalizes and validates configuration values after loading.
func (c *Config) Validate() error {
	rssURL := strings.TrimSpace(c.Scraper.RSSURL)
	if rssURL == "" {
		return errors.New("DJINNI_RSS_URL must not be empty")
	}
	parsed, err := url.Parse(rssURL)
	if err != nil {
		return fmt.Errorf("invalid DJINNI_RSS_URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid DJINNI_RSS_URL scheme %q: must be http or https", parsed.Scheme)
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return errors.New("invalid DJINNI_RSS_URL: host must not be empty")
	}
	c.Scraper.RSSURL = rssURL

	if strings.TrimSpace(c.Database.SSLMode) == "" {
		c.Database.SSLMode = "disable"
	}
	return nil
}

// LoadConfig parses the .env file or reads directly from environment variables.
func LoadConfig(envPath string) (*Config, error) {
	var cfg Config

	// 1. Try provided path if it exists
	if envPath != "" {
		_, statErr := os.Stat(envPath)
		if statErr == nil {
			if err := cleanenv.ReadConfig(envPath, &cfg); err != nil {
				return nil, fmt.Errorf("load config %q: %w", envPath, err)
			}
			if err := cfg.Validate(); err != nil {
				return nil, err
			}
			return &cfg, nil
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("stat config %q: %w", envPath, statErr)
		}
	}

	// 2. Try current directory .env if it exists
	if envPath != ".env" {
		_, statErr := os.Stat(".env")
		if statErr == nil {
			if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
				return nil, fmt.Errorf("load config \".env\": %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return nil, err
			}
			return &cfg, nil
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("stat config \".env\": %w", statErr)
		}
	}

	// 3. Fallback to process environment variables (standard in Docker / Prod)
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, fmt.Errorf("load env config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
