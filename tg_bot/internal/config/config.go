package config

import (
	"fmt"
	"log/slog"
	"os"

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
		LogPath string `env:"TG_LOG_PATH" env-default:"logs/tg_bot.log"`
	}
}

func (c *Config) RabbitMQURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/",
		c.RabbitMQ.User, c.RabbitMQ.Password, c.RabbitMQ.Host, c.RabbitMQ.Port)
}

func LoadConfig(envPath string) *Config {
	var cfg Config
	err := cleanenv.ReadConfig(envPath, &cfg)
	if err != nil {
		slog.Error("failed to load bot configuration", "error", err)
		os.Exit(1)
	}
	return &cfg
}
