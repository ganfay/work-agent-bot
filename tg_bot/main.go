package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tg_bot/internal/bot"
	"tg_bot/internal/broker"
	"tg_bot/internal/config"
	"tg_bot/pkg/logger"
)

func main() {
	cfg := config.LoadConfig("../.env")

	logger.Setup(cfg.Bot.LogPath)
	slog.Info("starting telegram bot service")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rmq, err := broker.NewRabbitMQ(cfg.RabbitMQURL())
	if err != nil {
		slog.Error("failed to connect to rabbitmq", "error", err)
		os.Exit(1)
	}
	defer rmq.Close()

	telegramBot, err := bot.New(cfg.Telegram.BotToken, rmq)
	if err != nil {
		slog.Error("failed to initialize telegram bot", "error", err)
		os.Exit(1)
	}

	go telegramBot.Start()
	defer telegramBot.Stop()

	// 6. Отримуємо Go-канал із черги review_queue
	msgs, err := rmq.ConsumeProposals()
	if err != nil {
		slog.Error("failed to start consuming review_queue", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("listening for AI proposals on 'review_queue'...")
		for {
			select {
			case <-ctx.Done():
				slog.Info("stopping message consumer loop...")
				return

			case msg, ok := <-msgs:
				if !ok {
					return
				}

				var proposal broker.Proposal
				if err := json.Unmarshal(msg.Body, &proposal); err != nil {
					slog.Error("invalid JSON from python worker", "error", err)
					_ = msg.Nack(false, false)
					continue
				}

				slog.Info("received vacancy for review", "title", proposal.Title, "score", proposal.MatchScore)

				err := telegramBot.SendVacancy(cfg.Telegram.UserID, proposal)
				if err != nil {
					slog.Error("failed to send telegram message to user", "error", err)
					_ = msg.Nack(false, true)
					continue
				}

				_ = msg.Ack(false)
				slog.Info("proposal delivered to telegram successfully", "title", proposal.Title)
			}
		}
	}()

	slog.Info("telegram bot service is running. Press CTRL+C to exit.")

	<-ctx.Done()
	slog.Info("shutdown signal received: closing connections...")
}
