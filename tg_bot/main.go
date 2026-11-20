package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

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

	telegramBot, err := bot.New(cfg.Telegram.BotToken, cfg.Telegram.UserID, rmq)
	if err != nil {
		slog.Error("failed to initialize telegram bot", "error", err)
		os.Exit(1)
	}

	go telegramBot.Start()
	defer telegramBot.Stop()

	msgs, err := rmq.ConsumeProposals()
	if err != nil {
		slog.Error("failed to start consuming review_queue", "error", err)
		os.Exit(1)
	}

	slog.Info("telegram bot service started", "run_once", cfg.Bot.RunOnce)
	if err := runConsumerLoop(
		ctx,
		msgs,
		telegramBot,
		cfg.Telegram.UserID,
		cfg.Bot.RunOnce,
		cfg.Bot.BatchIdleTimeout,
		cfg.Bot.PollGrace,
	); err != nil {
		slog.Error("consumer loop terminated with error", "error", err)
		telegramBot.Stop()
		rmq.Close()
		os.Exit(1)
	}
	slog.Info("shutdown complete: closing connections...")
}

var errBrokerChannelClosed = errors.New("review_queue channel closed unexpectedly")

type ProposalSender interface {
	SendVacancy(userID int64, p broker.Proposal) error
}

func runConsumerLoop(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	sender ProposalSender,
	userID int64,
	runOnce bool,
	idleTimeout time.Duration,
	pollGrace time.Duration,
) error {
	slog.Info("listening for AI proposals on 'review_queue'...")

	var idleTimer *time.Timer
	var idleCh <-chan time.Time
	if runOnce {
		if idleTimeout <= 0 {
			idleTimeout = 90 * time.Second
		}
		idleTimer = time.NewTimer(idleTimeout)
		defer idleTimer.Stop()
		idleCh = idleTimer.C
	}

consumeLoop:
	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping message consumer loop...")
			return nil

		case <-idleCh:
			slog.Info("batch idle timeout reached on review_queue", "timeout", idleTimeout.String())
			break consumeLoop

		case msg, ok := <-msgs:
			if !ok {
				slog.Info("review_queue channel closed")
				if !runOnce && ctx.Err() == nil {
					return errBrokerChannelClosed
				}
				break consumeLoop
			}

			if runOnce && idleTimer != nil {
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(idleTimeout)
			}

			var proposal broker.Proposal
			if err := json.Unmarshal(msg.Body, &proposal); err != nil {
				slog.Error("invalid JSON from python worker", "error", err)
				_ = msg.Nack(false, false)
				continue
			}

			if proposal.BatchEOF {
				_ = msg.Ack(false)
				slog.Info("received batch EOF marker on review_queue")
				if runOnce {
					break consumeLoop
				}
				continue
			}

			slog.Info("received vacancy for review", "title", proposal.Title, "score", proposal.MatchScore)

			err := sender.SendVacancy(userID, proposal)
			if err != nil {
				slog.Error("failed to send telegram message to user", "error", err)
				requeue := !runOnce && !msg.Redelivered
				_ = msg.Nack(false, requeue)
				continue
			}

			_ = msg.Ack(false)
			slog.Info("proposal delivered to telegram successfully", "title", proposal.Title)
		}
	}

	if runOnce && pollGrace > 0 && ctx.Err() == nil {
		slog.Info("waiting Telegram poll grace window for pending Approve/Reject callbacks...", "grace", pollGrace.String())
		graceTimer := time.NewTimer(pollGrace)
		defer graceTimer.Stop()
		select {
		case <-ctx.Done():
		case <-graceTimer.C:
		}
	}
	return nil
}
