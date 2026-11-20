package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	tele "gopkg.in/telebot.v4"

	"tg_bot/internal/bot"
	"tg_bot/internal/broker"
	"tg_bot/internal/config"
	"tg_bot/pkg/logger"
)

type appBroker interface {
	ProposalConsumer
	DLQPublisher
	SendFeedback(jobTitle, reason string) error
	Close()
}

type appBot interface {
	ProposalSender
	StartAsync() <-chan struct{}
	Stop()
}

type retryConfig struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func defaultRetryConfig() retryConfig {
	return retryConfig{
		maxAttempts:    5,
		initialBackoff: 1 * time.Second,
		maxBackoff:     10 * time.Second,
	}
}

type appDeps struct {
	loadConfig  func(envPath string) (*config.Config, error)
	setupLogger func(filePath string) (*slog.Logger, io.Closer, error)
	newBroker   func(url string) (appBroker, error)
	newBot      func(token string, allowedUserID int64, rabbit appBroker) (appBot, error)
	retryCfg    retryConfig
}

func defaultAppDeps() appDeps {
	return appDeps{
		loadConfig:  config.LoadConfig,
		setupLogger: logger.Setup,
		newBroker: func(url string) (appBroker, error) {
			return broker.NewRabbitMQ(url)
		},
		newBot: func(token string, allowedUserID int64, rabbit appBroker) (appBot, error) {
			return bot.New(token, allowedUserID, rabbit)
		},
		retryCfg: defaultRetryConfig(),
	}
}

func main() {
	os.Exit(runCLI())
}

func runCLI() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runApp(ctx, "../.env", defaultAppDeps()); err != nil {
		return 1
	}
	return 0
}

func isPermanentTelegramInitError(err error) bool {
	return errors.Is(err, tele.ErrUnauthorized) || errors.Is(err, tele.ErrNotFound)
}

func runApp(ctx context.Context, envPath string, deps appDeps) error {
	cfg, err := deps.loadConfig(envPath)
	if err != nil {
		slog.Error("failed to load bot configuration", "error", err)
		return fmt.Errorf("load config: %w", err)
	}

	_, logCloser, err := deps.setupLogger(cfg.Bot.LogPath)
	if err != nil {
		slog.Error("failed to initialize logger", "error", err)
		return fmt.Errorf("setup logger: %w", err)
	}
	if logCloser != nil {
		defer func() {
			if closeErr := logCloser.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "failed to close logger: %v\n", closeErr)
			}
		}()
	}
	slog.Info("starting telegram bot service")

	rmq, err := retryConnect(ctx, "rabbitmq", deps.retryCfg, nil, func() (appBroker, error) {
		return deps.newBroker(cfg.RabbitMQURL())
	})
	if err != nil {
		slog.Error("failed to connect to rabbitmq", "error", err)
		return err
	}
	defer func() {
		slog.Info("closing rabbitmq connection and channels...")
		rmq.Close()
	}()

	telegramBot, err := retryConnect(ctx, "telegram", deps.retryCfg, isPermanentTelegramInitError, func() (appBot, error) {
		return deps.newBot(cfg.Telegram.BotToken, cfg.Telegram.UserID, rmq)
	})
	if err != nil {
		slog.Error("failed to initialize telegram bot", "error", err)
		return err
	}

	msgs, err := rmq.ConsumeProposals(ctx)
	if err != nil {
		slog.Error("failed to start consuming review_queue", "error", err)
		return fmt.Errorf("consume review_queue: %w", err)
	}

	telegramBot.StartAsync()
	defer func() {
		slog.Info("stopping telegram bot...")
		telegramBot.Stop()
	}()

	slog.Info("telegram bot service started", "run_once", cfg.Bot.RunOnce)
	var loopErr error
	if cfg.Bot.RunOnce {
		loopErr = runConsumerLoop(
			ctx,
			msgs,
			telegramBot,
			rmq,
			cfg.Telegram.UserID,
			true,
			cfg.Bot.BatchIdleTimeout,
			cfg.Bot.PollGrace,
			defaultSendRetryConfig(),
		)
	} else {
		loopErr = runDaemonConsumerLoopWithInitial(
			ctx,
			msgs,
			rmq,
			telegramBot,
			rmq,
			cfg.Telegram.UserID,
			defaultSendRetryConfig(),
			initialReconnectBackoff,
			maxReconnectBackoff,
			sleepWithContext,
		)
	}
	if loopErr != nil {
		slog.Error("consumer loop terminated with error", "error", loopErr)
		return loopErr
	}
	slog.Info("shutdown complete: closing connections...")
	return nil
}

func retryConnect[T any](
	ctx context.Context,
	name string,
	cfg retryConfig,
	isPermanent func(error) bool,
	connect func() (T, error),
) (T, error) {
	var zero T
	maxAttempts := cfg.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := cfg.initialBackoff
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, fmt.Errorf("%s initialization cancelled: %w", name, err)
		}

		res, err := connect()
		if err == nil {
			return res, nil
		}
		if isPermanent != nil && isPermanent(err) {
			return zero, fmt.Errorf("%s permanent failure: %w", name, err)
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}

		slog.Warn("dependency connection attempt failed; retrying",
			"dependency", name,
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff", backoff.String(),
			"error", err,
		)

		if backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, fmt.Errorf("%s startup retry cancelled: %w (last error: %v)", name, ctx.Err(), lastErr)
			case <-timer.C:
			}
			backoff *= 2
			if cfg.maxBackoff > 0 && backoff > cfg.maxBackoff {
				backoff = cfg.maxBackoff
			}
		}
	}

	return zero, fmt.Errorf("%s failed after %d attempt(s): %w", name, maxAttempts, lastErr)
}

var errBrokerChannelClosed = errors.New("review_queue channel closed unexpectedly")

const (
	dlqPublishTimeout         = 5 * time.Second
	initialReconnectBackoff   = 1 * time.Second
	maxReconnectBackoff       = 30 * time.Second
	healthySessionMinDuration = 5 * time.Second
)

type ProposalSender interface {
	SendVacancy(userID int64, p broker.Proposal) error
}

type ProposalConsumer interface {
	ConsumeProposals(ctx context.Context) (<-chan amqp.Delivery, error)
}

type DLQPublisher interface {
	PublishReviewDLQ(ctx context.Context, rawBody []byte, reason string) error
}

type sendRetryConfig struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	sleep          func(ctx context.Context, d time.Duration) error
}

func defaultSendRetryConfig() sendRetryConfig {
	return sendRetryConfig{
		maxAttempts:    5,
		initialBackoff: 2 * time.Second,
		maxBackoff:     30 * time.Second,
		sleep:          sleepWithContext,
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func extractTelegramStatusCode(msg string) (int, bool) {
	trimmed := strings.TrimSpace(msg)
	if !strings.HasSuffix(trimmed, ")") {
		return 0, false
	}
	openIdx := strings.LastIndex(trimmed, " (")
	if openIdx < 0 {
		return 0, false
	}
	codeStr := trimmed[openIdx+2 : len(trimmed)-1]
	if len(codeStr) != 3 {
		return 0, false
	}
	code, err := strconv.Atoi(codeStr)
	if err != nil {
		return 0, false
	}
	return code, true
}

func isPermanentTelegramError(err error) bool {
	if err == nil {
		return false
	}
	var floodErr tele.FloodError
	if errors.As(err, &floodErr) {
		return false
	}
	var apiErr *tele.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code >= 400 && apiErr.Code < 500 && apiErr.Code != http.StatusTooManyRequests
	}
	if errors.Is(err, tele.ErrBadRecipient) || errors.Is(err, tele.ErrUnsupportedWhat) {
		return true
	}
	if code, ok := extractTelegramStatusCode(err.Error()); ok {
		return code >= 400 && code < 500 && code != http.StatusTooManyRequests
	}
	return false
}

func floodRetryDelay(err error) (time.Duration, bool) {
	var floodErr tele.FloodError
	if errors.As(err, &floodErr) {
		ra := floodErr.RetryAfter
		if ra < 0 {
			ra = 0
		}
		return time.Duration(ra+1) * time.Second, true
	}
	return 0, false
}

func sendProposalWithRetry(
	ctx context.Context,
	sender ProposalSender,
	userID int64,
	proposal broker.Proposal,
	cfg sendRetryConfig,
) error {
	maxAttempts := cfg.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := cfg.initialBackoff
	if backoff <= 0 {
		backoff = 2 * time.Second
	}
	maxBackoff := cfg.maxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	sleepFn := cfg.sleep
	if sleepFn == nil {
		sleepFn = sleepWithContext
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := sender.SendVacancy(userID, proposal)
		if err == nil {
			return nil
		}
		if isPermanentTelegramError(err) {
			return err
		}
		if attempt == maxAttempts {
			return err
		}

		var wait time.Duration
		if floodWait, ok := floodRetryDelay(err); ok {
			wait = floodWait
		} else {
			wait = backoff
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		slog.Warn("telegram send failed; retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"wait", wait.String(),
			"title", proposal.Title,
			"error", err,
		)
		if sleepErr := sleepFn(ctx, wait); sleepErr != nil {
			return sleepErr
		}
	}
	return nil
}

type closedChecker interface {
	IsClosed() bool
}

func isAcknowledgerClosed(ack amqp.Acknowledger) bool {
	if ack == nil {
		return true
	}
	if cc, ok := ack.(closedChecker); ok {
		return cc.IsClosed()
	}
	return false
}

func safeAck(msg amqp.Delivery) {
	if msg.Acknowledger == nil || isAcknowledgerClosed(msg.Acknowledger) {
		slog.Warn("skipping ack on nil or closed amqp channel", "delivery_tag", msg.DeliveryTag)
		return
	}
	if err := msg.Ack(false); err != nil {
		slog.Warn("failed to ack message", "delivery_tag", msg.DeliveryTag, "error", err)
	}
}

func safeNack(msg amqp.Delivery, requeue bool) {
	if msg.Acknowledger == nil || isAcknowledgerClosed(msg.Acknowledger) {
		slog.Warn("skipping nack on nil or closed amqp channel", "delivery_tag", msg.DeliveryTag)
		return
	}
	if err := msg.Nack(false, requeue); err != nil {
		slog.Warn("failed to nack message", "delivery_tag", msg.DeliveryTag, "requeue", requeue, "error", err)
	}
}

func routeFailedMessage(ctx context.Context, dlq DLQPublisher, msg amqp.Delivery, reason string) {
	if ctx.Err() != nil {
		return
	}
	if dlq != nil {
		dlqCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
		err := dlq.PublishReviewDLQ(dlqCtx, msg.Body, reason)
		cancel()
		if err == nil {
			safeAck(msg)
			return
		}
		slog.Error("failed to publish failed proposal to review_queue.dlq", "error", err)
	}
	safeNack(msg, false)
}

func stopAndDrainTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func decodeProposalObject(raw []byte) (broker.Proposal, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return broker.Proposal{}, errors.New("payload must be a JSON object")
	}
	var proposal broker.Proposal
	if err := json.Unmarshal(trimmed, &proposal); err != nil {
		return broker.Proposal{}, err
	}
	return proposal, nil
}

func runDaemonConsumerLoop(
	ctx context.Context,
	consumer ProposalConsumer,
	sender ProposalSender,
	dlq DLQPublisher,
	userID int64,
	retryCfg sendRetryConfig,
) error {
	return runDaemonConsumerLoopWithBackoff(
		ctx,
		consumer,
		sender,
		dlq,
		userID,
		retryCfg,
		initialReconnectBackoff,
		maxReconnectBackoff,
		sleepWithContext,
	)
}

func runDaemonConsumerLoopWithBackoff(
	ctx context.Context,
	consumer ProposalConsumer,
	sender ProposalSender,
	dlq DLQPublisher,
	userID int64,
	retryCfg sendRetryConfig,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	sleepFn func(ctx context.Context, d time.Duration) error,
) error {
	return runDaemonConsumerLoopWithInitial(
		ctx,
		nil,
		consumer,
		sender,
		dlq,
		userID,
		retryCfg,
		initialBackoff,
		maxBackoff,
		sleepFn,
	)
}

func runDaemonConsumerLoopWithInitial(
	ctx context.Context,
	initialMsgs <-chan amqp.Delivery,
	consumer ProposalConsumer,
	sender ProposalSender,
	dlq DLQPublisher,
	userID int64,
	retryCfg sendRetryConfig,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	sleepFn func(ctx context.Context, d time.Duration) error,
) error {
	if initialBackoff <= 0 {
		initialBackoff = initialReconnectBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = maxReconnectBackoff
	}
	if sleepFn == nil {
		sleepFn = sleepWithContext
	}

	backoff := initialBackoff
	nextMsgs := initialMsgs
	for {
		if ctx.Err() != nil {
			return nil
		}

		var msgs <-chan amqp.Delivery
		if nextMsgs != nil {
			msgs = nextMsgs
			nextMsgs = nil
		} else {
			var err error
			msgs, err = consumer.ConsumeProposals(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.Warn("failed to consume review_queue; retrying", "backoff", backoff.String(), "error", err)
				if sleepErr := sleepFn(ctx, backoff); sleepErr != nil {
					return nil
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}

		connectedAt := time.Now()
		err := runConsumerLoop(ctx, msgs, sender, dlq, userID, false, 0, 0, retryCfg)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if !errors.Is(err, errBrokerChannelClosed) {
			return err
		}

		if time.Since(connectedAt) >= healthySessionMinDuration {
			backoff = initialBackoff
		}
		slog.Warn("review_queue consumer disconnected; reconnecting", "backoff", backoff.String(), "error", err)
		if sleepErr := sleepFn(ctx, backoff); sleepErr != nil {
			return nil
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func runConsumerLoop(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	sender ProposalSender,
	dlq DLQPublisher,
	userID int64,
	runOnce bool,
	idleTimeout time.Duration,
	pollGrace time.Duration,
	retryCfg sendRetryConfig,
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
				stopAndDrainTimer(idleTimer)
			}

			proposal, err := decodeProposalObject(msg.Body)
			if err != nil {
				slog.Error("invalid JSON from python worker", "error", err)
				routeFailedMessage(ctx, dlq, msg, "json_decode: "+err.Error())
				if runOnce && idleTimer != nil && ctx.Err() == nil {
					idleTimer.Reset(idleTimeout)
				}
				continue
			}

			if proposal.BatchEOF {
				safeAck(msg)
				slog.Info("received batch EOF marker on review_queue")
				if runOnce {
					break consumeLoop
				}
				continue
			}

			slog.Info("received vacancy for review", "title", proposal.Title, "score", proposal.MatchScore)

			err = sendProposalWithRetry(ctx, sender, userID, proposal, retryCfg)
			if err != nil {
				if ctx.Err() != nil {
					slog.Info("telegram delivery interrupted by shutdown; leaving message unacked", "title", proposal.Title)
					return nil
				}
				slog.Error("failed to send telegram message to user", "title", proposal.Title, "error", err)
				routeFailedMessage(ctx, dlq, msg, "telegram_send: "+err.Error())
				if runOnce && idleTimer != nil {
					idleTimer.Reset(idleTimeout)
				}
				continue
			}

			safeAck(msg)
			slog.Info("proposal delivered to telegram successfully", "title", proposal.Title)
			if runOnce && idleTimer != nil {
				idleTimer.Reset(idleTimeout)
			}
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
