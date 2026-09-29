package config

import (
	"fmt"
	"log/slog"
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
	}
}

// DSN builds the PostgreSQL connection string
func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.Database.User, c.Database.Password, c.Database.Host, c.Database.Port, c.Database.Name)
}

// URL builds the RabbitMQ connection string
func (c *Config) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/",
		c.RabbitMQ.User, c.RabbitMQ.Password, c.RabbitMQ.Host, c.RabbitMQ.Port)
}

// LoadConfig parses the .env file and populates the Config struct
func LoadConfig(envPath string) *Config {
	var cfg Config
	err := cleanenv.ReadConfig(envPath, &cfg)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}
	return &cfg
}
