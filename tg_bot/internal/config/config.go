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

// Validate normalizes and validates configuration values after loading.
func (c *Config) Validate() error {
	token := strings.TrimSpace(c.Telegram.BotToken)
	if token == "" {
		return errors.New("TG_BOT_TOKEN must not be empty")
	}
	if strings.ContainsAny(token, " \t\r\n") {
		return errors.New("invalid TG_BOT_TOKEN: must not contain whitespace")
	}
	c.Telegram.BotToken = token

	if c.Telegram.UserID <= 0 {
		return errors.New("TG_USER_ID must be a positive integer")
	}

	if strings.TrimSpace(c.RabbitMQ.User) == "" {
		return errors.New("RMQ_USER must not be empty")
	}
	if c.RabbitMQ.Password == "" {
		return errors.New("RMQ_PASS must not be empty")
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

	logPath := strings.TrimSpace(c.Bot.LogPath)
	if logPath == "" {
		return errors.New("TG_LOG_PATH must not be empty")
	}
	c.Bot.LogPath = logPath

	c.normalize()
	return nil
}

func LoadConfig(envPath string) (*Config, error) {
	var cfg Config

	// 1. Try provided path if exists
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

	// 2. Try current directory .env if exists
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
