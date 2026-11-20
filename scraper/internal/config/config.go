package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
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

func validateHost(envName, raw string) (string, error) {
	host := strings.TrimSpace(raw)
	if host == "" {
		return "", fmt.Errorf("%s must not be empty", envName)
	}
	if strings.ContainsAny(host, " \t\r\n@/?#") {
		return "", fmt.Errorf("invalid %s %q: must not contain whitespace or @/?#", envName, raw)
	}
	return host, nil
}

func validatePort(envName, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%s must not be empty", envName)
	}
	if strings.HasPrefix(trimmed, "+") {
		return "", fmt.Errorf("invalid %s %q: must be an integer between 1 and 65535", envName, raw)
	}
	port, err := strconv.Atoi(trimmed)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("invalid %s %q: must be an integer between 1 and 65535", envName, raw)
	}
	return strconv.Itoa(port), nil
}

func redactURLForError(raw string) string {
	if raw == "" {
		return ""
	}
	if u, err := url.Parse(raw); err == nil {
		if u.User != nil {
			u.User = url.UserPassword("xxxxx", "xxxxx")
		}
		if u.RawQuery != "" {
			if q, qErr := url.ParseQuery(u.RawQuery); qErr == nil {
				for k := range q {
					q[k] = []string{"xxxxx"}
				}
				u.RawQuery = q.Encode()
			} else {
				u.RawQuery = "redacted"
			}
		}
		return u.String()
	}

	schemeIdx := strings.Index(raw, "://")
	if schemeIdx < 0 {
		return "<redacted>"
	}
	prefix := raw[:schemeIdx+3]
	rest := raw[schemeIdx+3:]

	authEnd := len(rest)
	for _, sep := range []string{"/", "?", "#"} {
		if idx := strings.Index(rest, sep); idx >= 0 && idx < authEnd {
			authEnd = idx
		}
	}
	authority := rest[:authEnd]
	suffix := rest[authEnd:]

	if atIdx := strings.LastIndex(authority, "@"); atIdx >= 0 {
		authority = "xxxxx:xxxxx@" + authority[atIdx+1:]
	} else {
		return "<redacted>"
	}

	if qIdx := strings.Index(suffix, "?"); qIdx >= 0 {
		fragIdx := strings.Index(suffix[qIdx:], "#")
		if fragIdx >= 0 {
			suffix = suffix[:qIdx] + "?<redacted>" + suffix[qIdx+fragIdx:]
		} else {
			suffix = suffix[:qIdx] + "?<redacted>"
		}
	}
	return prefix + authority + suffix
}

func sanitizeURLError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return &url.Error{
			Op:  urlErr.Op,
			URL: redactURLForError(urlErr.URL),
			Err: urlErr.Err,
		}
	}
	return err
}

// Validate normalizes and validates configuration values after loading.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Database.User) == "" {
		return errors.New("PG_USER must not be empty")
	}
	pgHost, err := validateHost("PG_HOST", c.Database.Host)
	if err != nil {
		return err
	}
	c.Database.Host = pgHost

	pgPort, err := validatePort("PG_PORT", c.Database.Port)
	if err != nil {
		return err
	}
	c.Database.Port = pgPort

	pgDB := strings.TrimSpace(c.Database.Name)
	if pgDB == "" {
		return errors.New("PG_DB must not be empty")
	}
	c.Database.Name = pgDB

	if strings.TrimSpace(c.RabbitMQ.User) == "" {
		return errors.New("RMQ_USER must not be empty")
	}
	rmqHost, err := validateHost("RMQ_HOST", c.RabbitMQ.Host)
	if err != nil {
		return err
	}
	c.RabbitMQ.Host = rmqHost

	rmqPort, err := validatePort("RMQ_PORT", c.RabbitMQ.Port)
	if err != nil {
		return err
	}
	c.RabbitMQ.Port = rmqPort

	rssURL := strings.TrimSpace(c.Scraper.RSSURL)
	if rssURL == "" {
		return errors.New("DJINNI_RSS_URL must not be empty")
	}
	parsed, err := url.Parse(rssURL)
	if err != nil {
		return fmt.Errorf("invalid DJINNI_RSS_URL: %w", sanitizeURLError(err))
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
