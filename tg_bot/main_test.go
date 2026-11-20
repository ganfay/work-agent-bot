package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	tele "gopkg.in/telebot.v4"

	"tg_bot/internal/broker"
	"tg_bot/internal/config"
)

type fakeAck struct {
	mu      sync.Mutex
	acks    []uint64
	nacks   []uint64
	requeue []bool
	closed  bool
}

func (f *fakeAck) Ack(tag uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, tag)
	return nil
}

func (f *fakeAck) Nack(tag uint64, _ bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks = append(f.nacks, tag)
	f.requeue = append(f.requeue, requeue)
	return nil
}

func (f *fakeAck) Reject(_ uint64, _ bool) error { return nil }

func (f *fakeAck) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeSender struct {
	mu   sync.Mutex
	sent []broker.Proposal
	err  error
	fn   func(attempt int, userID int64, p broker.Proposal) error
	calls int
}

func (s *fakeSender) SendVacancy(userID int64, p broker.Proposal) error {
	s.mu.Lock()
	s.calls++
	attempt := s.calls
	fn := s.fn
	err := s.err
	s.mu.Unlock()

	if fn != nil {
		if fnErr := fn(attempt, userID, p); fnErr != nil {
			return fnErr
		}
		s.mu.Lock()
		s.sent = append(s.sent, p)
		s.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.sent = append(s.sent, p)
	s.mu.Unlock()
	return nil
}

func instantSleep(_ context.Context, _ time.Duration) error {
	return nil
}

func fastTestRetryConfig() sendRetryConfig {
	return sendRetryConfig{
		maxAttempts:    3,
		initialBackoff: time.Millisecond,
		maxBackoff:     5 * time.Millisecond,
		sleep:          instantSleep,
	}
}

type fakeDLQ struct {
	mu      sync.Mutex
	bodies  [][]byte
	reasons []string
	err     error
}

func (f *fakeDLQ) PublishReviewDLQ(_ context.Context, rawBody []byte, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	cp := make([]byte, len(rawBody))
	copy(cp, rawBody)
	f.bodies = append(f.bodies, cp)
	f.reasons = append(f.reasons, reason)
	return nil
}

func makeDelivery(ack amqp.Acknowledger, tag uint64, body []byte) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  tag,
		Body:         body,
	}
}

// buildRealFloodError obtains a genuine tele.FloodError (with initialized unexported err field)
// from a local httptest server so .Error() never panics.
func buildRealFloodError(t *testing.T, retryAfter int) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"Test"}}`))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          false,
			"error_code":  429,
			"description": "Too Many Requests",
			"parameters":  map[string]any{"retry_after": retryAfter},
		})
	}))
	t.Cleanup(srv.Close)

	tgBot, err := tele.NewBot(tele.Settings{
		Token:   "123:ABC",
		URL:     srv.URL,
		Client:  srv.Client(),
		Offline: true,
	})
	if err != nil {
		t.Fatalf("tele.NewBot failed: %v", err)
	}
	_, sendErr := tgBot.Send(tele.ChatID(42), "hello")
	if sendErr == nil {
		t.Fatalf("expected FloodError")
	}
	return sendErr
}

func TestRunConsumerLoop_RunOnceProcessesAndStopsOnEOF(t *testing.T) {
	ack := &fakeAck{}
	sender := &fakeSender{}
	msgs := make(chan amqp.Delivery, 4)

	pBytes, _ := json.Marshal(broker.Proposal{Title: "Go Engineer", MatchScore: 90})
	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})

	msgs <- makeDelivery(ack, 1, pBytes)
	msgs <- makeDelivery(ack, 2, eofBytes)

	start := time.Now()
	if err := runConsumerLoop(context.Background(), msgs, sender, nil, 42, true, 5*time.Second, 25*time.Millisecond, fastTestRetryConfig()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if len(sender.sent) != 1 || sender.sent[0].Title != "Go Engineer" {
		t.Fatalf("expected 1 proposal sent, got %+v", sender.sent)
	}
	if len(ack.acks) != 2 || ack.acks[0] != 1 || ack.acks[1] != 2 {
		t.Fatalf("expected acks [1, 2], got %+v", ack.acks)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected pollGrace wait >=20ms, got %v", elapsed)
	}
}

func TestRunConsumerLoop_RunOnceIdleTimeout(t *testing.T) {
	sender := &fakeSender{}
	msgs := make(chan amqp.Delivery)

	start := time.Now()
	if err := runConsumerLoop(context.Background(), msgs, sender, nil, 42, true, 30*time.Millisecond, 10*time.Millisecond, fastTestRetryConfig()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 35*time.Millisecond {
		t.Fatalf("expected idleTimeout+pollGrace >=35ms, got %v", elapsed)
	}
}

func TestRunConsumerLoop_HandlesBadJSONAndSendError(t *testing.T) {
	ack := &fakeAck{}
	sender := &fakeSender{err: errors.New("telegram 429")}
	dlq := &fakeDLQ{}
	msgs := make(chan amqp.Delivery, 4)

	pBytes, _ := json.Marshal(broker.Proposal{Title: "Go Engineer", MatchScore: 88})
	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})

	msgs <- makeDelivery(ack, 10, []byte("{bad-json"))
	msgs <- makeDelivery(ack, 11, pBytes)
	msgs <- makeDelivery(ack, 12, eofBytes)

	if err := runConsumerLoop(context.Background(), msgs, sender, dlq, 42, true, time.Second, 0, fastTestRetryConfig()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(dlq.bodies) != 2 {
		t.Fatalf("expected 2 DLQ entries (bad JSON + exhausted send error), got %d", len(dlq.bodies))
	}
	if len(ack.nacks) != 0 {
		t.Fatalf("expected 0 nacks when DLQ succeeds, got %+v", ack.nacks)
	}
	if len(ack.acks) != 3 || ack.acks[0] != 10 || ack.acks[1] != 11 || ack.acks[2] != 12 {
		t.Fatalf("expected acks [10, 11, 12], got %+v", ack.acks)
	}
}

func TestRunConsumerLoop_DaemonModeIgnoresEOFAndExitsOnCancel(t *testing.T) {
	ack := &fakeAck{}
	sender := &fakeSender{}
	msgs := make(chan amqp.Delivery, 2)

	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
	msgs <- makeDelivery(ack, 1, eofBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := runConsumerLoop(ctx, msgs, sender, nil, 42, false, 5*time.Millisecond, 100*time.Millisecond, fastTestRetryConfig()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 35*time.Millisecond {
		t.Fatalf("daemon mode exited too early (%v), should wait for ctx.Done()", elapsed)
	}
	if len(ack.acks) != 1 || ack.acks[0] != 1 {
		t.Fatalf("expected EOF acked in daemon mode too, got %+v", ack.acks)
	}
}

func TestRunConsumerLoop_DaemonModeReturnsErrorOnBrokerDisconnect(t *testing.T) {
	sender := &fakeSender{}
	msgs := make(chan amqp.Delivery)
	close(msgs)

	err := runConsumerLoop(context.Background(), msgs, sender, nil, 42, false, time.Second, 0, fastTestRetryConfig())
	if !errors.Is(err, errBrokerChannelClosed) {
		t.Fatalf("expected errBrokerChannelClosed on unexpected close in daemon mode, got %v", err)
	}
}

func TestSendProposalWithRetry_FloodErrorSleepsRetryAfterPlusOne(t *testing.T) {
	floodErr := buildRealFloodError(t, 12)
	var slept []time.Duration

	sender := &fakeSender{
		fn: func(attempt int, _ int64, _ broker.Proposal) error {
			if attempt == 1 {
				return floodErr
			}
			return nil
		},
	}

	cfg := sendRetryConfig{
		maxAttempts:    3,
		initialBackoff: 2 * time.Second,
		maxBackoff:     30 * time.Second,
		sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}

	if err := sendProposalWithRetry(context.Background(), sender, 42, broker.Proposal{Title: "Go"}, cfg); err != nil {
		t.Fatalf("expected retry after FloodError to succeed, got %v", err)
	}
	if len(slept) != 1 || slept[0] != 13*time.Second {
		t.Fatalf("expected sleep of 13s (RetryAfter+1), got %v", slept)
	}
}

func TestSendProposalWithRetry_FloodErrorRespectsContextCancel(t *testing.T) {
	floodErr := buildRealFloodError(t, 60)
	ack := &fakeAck{}
	dlq := &fakeDLQ{}
	msgs := make(chan amqp.Delivery, 2)
	pBytes, _ := json.Marshal(broker.Proposal{Title: "Go Engineer"})
	msgs <- makeDelivery(ack, 55, pBytes)

	ctx, cancel := context.WithCancel(context.Background())
	sender := &fakeSender{
		fn: func(_ int, _ int64, _ broker.Proposal) error {
			return floodErr
		},
	}
	cfg := sendRetryConfig{
		maxAttempts:    3,
		initialBackoff: time.Second,
		maxBackoff:     10 * time.Second,
		sleep: func(c context.Context, _ time.Duration) error {
			cancel()
			return c.Err()
		},
	}

	err := runConsumerLoop(ctx, msgs, sender, dlq, 42, true, time.Second, 0, cfg)
	if err != nil {
		t.Fatalf("expected clean nil return on ctx cancel, got %v", err)
	}
	if len(ack.acks) != 0 || len(ack.nacks) != 0 || len(dlq.bodies) != 0 {
		t.Fatalf("expected message left unacked and not DLQ'd on ctx cancel; acks=%v nacks=%v dlq=%d", ack.acks, ack.nacks, len(dlq.bodies))
	}
}

func TestSendProposalWithRetry_ExponentialBackoffAndExhaustionToDLQ(t *testing.T) {
	var slept []time.Duration
	sender := &fakeSender{err: errors.New("502 Bad Gateway (502)")}
	ack := &fakeAck{}
	dlq := &fakeDLQ{}
	msgs := make(chan amqp.Delivery, 2)

	pBytes, _ := json.Marshal(broker.Proposal{Title: "Go"})
	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
	msgs <- makeDelivery(ack, 1, pBytes)
	msgs <- makeDelivery(ack, 2, eofBytes)

	cfg := sendRetryConfig{
		maxAttempts:    4,
		initialBackoff: 2 * time.Millisecond,
		maxBackoff:     5 * time.Millisecond,
		sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	}
	if err := runConsumerLoop(context.Background(), msgs, sender, dlq, 42, true, time.Second, 0, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sender.calls != 4 {
		t.Fatalf("expected 4 attempts, got %d", sender.calls)
	}
	wantSlept := []time.Duration{2 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond}
	if len(slept) != len(wantSlept) {
		t.Fatalf("expected backoffs %v, got %v", wantSlept, slept)
	}
	for i := range wantSlept {
		if slept[i] != wantSlept[i] {
			t.Fatalf("backoff[%d]=%v, want %v", i, slept[i], wantSlept[i])
		}
	}
	if len(dlq.bodies) != 1 || len(ack.acks) != 2 || len(ack.nacks) != 0 {
		t.Fatalf("expected DLQ+Ack after exhaustion, dlq=%d acks=%v nacks=%v", len(dlq.bodies), ack.acks, ack.nacks)
	}
}

func TestSendProposalWithRetry_DoesNotRetryPermanentTelegram4xx(t *testing.T) {
	permanentErrs := []error{
		tele.ErrBlockedByUser,
		tele.ErrTooLongMessage,
		errors.New("telegram: Bad Request: can't parse entities (400)"),
	}

	for _, permErr := range permanentErrs {
		sender := &fakeSender{err: permErr}
		ack := &fakeAck{}
		dlq := &fakeDLQ{}
		msgs := make(chan amqp.Delivery, 2)
		pBytes, _ := json.Marshal(broker.Proposal{Title: "Go"})
		eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
		msgs <- makeDelivery(ack, 7, pBytes)
		msgs <- makeDelivery(ack, 8, eofBytes)

		if err := runConsumerLoop(context.Background(), msgs, sender, dlq, 42, true, time.Second, 0, fastTestRetryConfig()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sender.calls != 1 {
			t.Fatalf("expected permanent error %v to fail on attempt 1 without retry, got %d calls", permErr, sender.calls)
		}
		if len(dlq.bodies) != 1 || len(ack.acks) != 2 {
			t.Fatalf("expected permanent error routed to DLQ+Ack, dlq=%d acks=%v", len(dlq.bodies), ack.acks)
		}
	}
}

func TestRunConsumerLoop_BadJSONAndClosedAcknowledger(t *testing.T) {
	t.Run("non-object JSON routes to DLQ and falls back to Nack on DLQ error", func(t *testing.T) {
		ack := &fakeAck{}
		dlq := &fakeDLQ{err: errors.New("dlq unavailable")}
		sender := &fakeSender{}
		msgs := make(chan amqp.Delivery, 3)

		eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
		msgs <- makeDelivery(ack, 1, []byte("null"))
		msgs <- makeDelivery(ack, 2, []byte("[1,2,3]"))
		msgs <- makeDelivery(ack, 3, eofBytes)

		if err := runConsumerLoop(context.Background(), msgs, sender, dlq, 42, true, time.Second, 0, fastTestRetryConfig()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ack.nacks) != 2 || ack.requeue[0] || ack.requeue[1] {
			t.Fatalf("expected 2 non-requeued nacks when DLQ fails, got nacks=%v requeue=%v", ack.nacks, ack.requeue)
		}
	})

	t.Run("closed acknowledger skips ack and nack safely", func(t *testing.T) {
		ack := &fakeAck{closed: true}
		dlq := &fakeDLQ{}
		sender := &fakeSender{}
		msgs := make(chan amqp.Delivery, 2)

		eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
		msgs <- makeDelivery(ack, 1, []byte("{bad"))
		msgs <- makeDelivery(ack, 2, eofBytes)

		if err := runConsumerLoop(context.Background(), msgs, sender, dlq, 42, true, time.Second, 0, fastTestRetryConfig()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ack.acks) != 0 || len(ack.nacks) != 0 {
			t.Fatalf("expected closed acknowledger to skip Ack/Nack, got acks=%v nacks=%v", ack.acks, ack.nacks)
		}
	})
}

func TestRunConsumerLoop_IdleTimerDoesNotExpireDuringSlowSend(t *testing.T) {
	ack := &fakeAck{}
	msgs := make(chan amqp.Delivery, 2)
	p1, _ := json.Marshal(broker.Proposal{Title: "First"})
	p2, _ := json.Marshal(broker.Proposal{Title: "Second"})
	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})

	sender := &fakeSender{
		fn: func(attempt int, _ int64, _ broker.Proposal) error {
			if attempt == 1 {
				time.Sleep(40 * time.Millisecond) // longer than idleTimeout=25ms
				msgs <- makeDelivery(ack, 2, p2)
				msgs <- makeDelivery(ack, 3, eofBytes)
			}
			return nil
		},
	}

	msgs <- makeDelivery(ack, 1, p1)
	if err := runConsumerLoop(context.Background(), msgs, sender, nil, 42, true, 25*time.Millisecond, 0, fastTestRetryConfig()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sender.sent) != 2 {
		t.Fatalf("expected both proposals delivered without premature idle timeout, got %d", len(sender.sent))
	}
}

type fakeDaemonConsumer struct {
	mu       sync.Mutex
	outcomes []func() (<-chan amqp.Delivery, error)
}

func (c *fakeDaemonConsumer) ConsumeProposals(_ context.Context) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn := c.outcomes[0]
	if len(c.outcomes) > 1 {
		c.outcomes = c.outcomes[1:]
	}
	return fn()
}

func TestRunDaemonConsumerLoop_ReconnectsOnDisconnectAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch1 := make(chan amqp.Delivery)
	close(ch1)
	ch2 := make(chan amqp.Delivery)
	close(ch2)
	ch3 := make(chan amqp.Delivery, 1)

	ack := &fakeAck{}
	pBytes, _ := json.Marshal(broker.Proposal{Title: "Recovered"})
	ch3 <- makeDelivery(ack, 9, pBytes)

	consumer := &fakeDaemonConsumer{
		outcomes: []func() (<-chan amqp.Delivery, error){
			func() (<-chan amqp.Delivery, error) { return ch1, nil },
			func() (<-chan amqp.Delivery, error) { return nil, errors.New("temporary consume err") },
			func() (<-chan amqp.Delivery, error) { return ch2, nil },
			func() (<-chan amqp.Delivery, error) { return ch3, nil },
		},
	}

	sender := &fakeSender{
		fn: func(_ int, _ int64, _ broker.Proposal) error {
			cancel()
			return nil
		},
	}

	err := runDaemonConsumerLoopWithBackoff(
		ctx,
		consumer,
		sender,
		nil,
		42,
		fastTestRetryConfig(),
		time.Millisecond,
		5*time.Millisecond,
		instantSleep,
	)
	if err != nil {
		t.Fatalf("expected nil on cancel, got %v", err)
	}
	if len(sender.sent) != 1 || sender.sent[0].Title != "Recovered" {
		t.Fatalf("expected recovered message delivered, got %+v", sender.sent)
	}
}

type orderTracker struct {
	mu     sync.Mutex
	events []string
}

func (o *orderTracker) record(ev string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, ev)
}

func (o *orderTracker) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	cp := make([]string, len(o.events))
	copy(cp, o.events)
	return cp
}

type fakeCloser struct {
	tracker *orderTracker
}

func (f *fakeCloser) Close() error {
	f.tracker.record("logger")
	return nil
}

type fakeAppBroker struct {
	tracker    *orderTracker
	consumeCh  chan amqp.Delivery
	consumeErr error
}

func (b *fakeAppBroker) ConsumeProposals(_ context.Context) (<-chan amqp.Delivery, error) {
	if b.consumeErr != nil {
		return nil, b.consumeErr
	}
	return b.consumeCh, nil
}

func (b *fakeAppBroker) PublishReviewDLQ(_ context.Context, _ []byte, _ string) error { return nil }
func (b *fakeAppBroker) SendFeedback(_, _ string) error                              { return nil }
func (b *fakeAppBroker) Close()                                                      { b.tracker.record("broker") }

type fakeAppBot struct {
	tracker *orderTracker
	started bool
	stopped bool
	done    chan struct{}
}

func (b *fakeAppBot) SendVacancy(_ int64, _ broker.Proposal) error { return nil }
func (b *fakeAppBot) StartAsync() <-chan struct{} {
	b.started = true
	b.done = make(chan struct{})
	close(b.done)
	return b.done
}
func (b *fakeAppBot) Stop() {
	b.stopped = true
	b.tracker.record("bot")
}

func validTestConfig(runOnce bool) *config.Config {
	cfg := &config.Config{}
	cfg.Telegram.BotToken = "123:ABC"
	cfg.Telegram.UserID = 42
	cfg.RabbitMQ.User = "u"
	cfg.RabbitMQ.Password = "p"
	cfg.RabbitMQ.Host = "localhost"
	cfg.RabbitMQ.Port = "5672"
	cfg.Bot.LogPath = "logs/test.log"
	cfg.Bot.RunOnce = runOnce
	cfg.Bot.BatchIdleTimeout = time.Second
	cfg.Bot.PollGrace = 0
	return cfg
}

func TestRunApp_StartupErrorCleanup(t *testing.T) {
	fastRetry := retryConfig{maxAttempts: 2, initialBackoff: time.Millisecond, maxBackoff: 2 * time.Millisecond}

	t.Run("loadConfig error does not call setupLogger", func(t *testing.T) {
		loggerCalled := false
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig: func(string) (*config.Config, error) { return nil, errors.New("bad config") },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) {
				loggerCalled = true
				return nil, nil, nil
			},
			retryCfg: fastRetry,
		})
		if err == nil || loggerCalled {
			t.Fatalf("expected error without calling setupLogger; err=%v loggerCalled=%v", err, loggerCalled)
		}
	})

	t.Run("setupLogger error does not call newBroker", func(t *testing.T) {
		brokerCalled := false
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig:  func(string) (*config.Config, error) { return validTestConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return nil, nil, errors.New("bad log") },
			newBroker: func(string) (appBroker, error) {
				brokerCalled = true
				return nil, nil
			},
			retryCfg: fastRetry,
		})
		if err == nil || brokerCalled {
			t.Fatalf("expected error without calling newBroker; err=%v brokerCalled=%v", err, brokerCalled)
		}
	})

	t.Run("newBroker exhausted retries closes logCloser", func(t *testing.T) {
		tracker := &orderTracker{}
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig:  func(string) (*config.Config, error) { return validTestConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), &fakeCloser{tracker: tracker}, nil },
			newBroker:   func(string) (appBroker, error) { return nil, errors.New("dial refused") },
			retryCfg:    fastRetry,
		})
		if err == nil {
			t.Fatalf("expected error")
		}
		if got := strings.Join(tracker.snapshot(), ","); got != "logger" {
			t.Fatalf("expected cleanup [logger], got %q", got)
		}
	})

	t.Run("newBot permanent ErrUnauthorized fails in 1 attempt and closes broker+logger", func(t *testing.T) {
		tracker := &orderTracker{}
		botAttempts := 0
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig:  func(string) (*config.Config, error) { return validTestConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), &fakeCloser{tracker: tracker}, nil },
			newBroker:   func(string) (appBroker, error) { return &fakeAppBroker{tracker: tracker}, nil },
			newBot: func(string, int64, appBroker) (appBot, error) {
				botAttempts++
				return nil, tele.ErrUnauthorized
			},
			retryCfg: retryConfig{maxAttempts: 5, initialBackoff: time.Millisecond, maxBackoff: 5 * time.Millisecond},
		})
		if err == nil || botAttempts != 1 {
			t.Fatalf("expected permanent failure in 1 attempt, got attempts=%d err=%v", botAttempts, err)
		}
		if got := strings.Join(tracker.snapshot(), ","); got != "broker,logger" {
			t.Fatalf("expected LIFO cleanup broker,logger, got %q", got)
		}
	})

	t.Run("ConsumeProposals failure closes broker+logger without StartAsync/Stop", func(t *testing.T) {
		tracker := &orderTracker{}
		botInst := &fakeAppBot{tracker: tracker}
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig:  func(string) (*config.Config, error) { return validTestConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), &fakeCloser{tracker: tracker}, nil },
			newBroker:   func(string) (appBroker, error) { return &fakeAppBroker{tracker: tracker, consumeErr: errors.New("consume fail")}, nil },
			newBot:      func(string, int64, appBroker) (appBot, error) { return botInst, nil },
			retryCfg:    fastRetry,
		})
		if err == nil || botInst.started || botInst.stopped {
			t.Fatalf("expected error without starting/stopping bot, started=%v stopped=%v err=%v", botInst.started, botInst.stopped, err)
		}
		if got := strings.Join(tracker.snapshot(), ","); got != "broker,logger" {
			t.Fatalf("expected LIFO cleanup broker,logger, got %q", got)
		}
	})

	t.Run("happy path RUN_ONCE + EOF closes in LIFO order bot,broker,logger", func(t *testing.T) {
		tracker := &orderTracker{}
		ch := make(chan amqp.Delivery, 1)
		eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})
		ch <- makeDelivery(&fakeAck{}, 1, eofBytes)

		botInst := &fakeAppBot{tracker: tracker}
		err := runApp(context.Background(), ".env", appDeps{
			loadConfig:  func(string) (*config.Config, error) { return validTestConfig(true), nil },
			setupLogger: func(string) (*slog.Logger, io.Closer, error) { return slog.Default(), &fakeCloser{tracker: tracker}, nil },
			newBroker:   func(string) (appBroker, error) { return &fakeAppBroker{tracker: tracker, consumeCh: ch}, nil },
			newBot:      func(string, int64, appBroker) (appBot, error) { return botInst, nil },
			retryCfg:    fastRetry,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := strings.Join(tracker.snapshot(), ","); got != "bot,broker,logger" {
			t.Fatalf("expected LIFO cleanup bot,broker,logger, got %q", got)
		}
	})
}

func TestRetryConnect_TransientRecoveryAndContextCancel(t *testing.T) {
	t.Run("succeeds on 3rd attempt", func(t *testing.T) {
		attempts := 0
		res, err := retryConnect(context.Background(), "dep", retryConfig{
			maxAttempts:    4,
			initialBackoff: time.Millisecond,
			maxBackoff:     5 * time.Millisecond,
		}, nil, func() (string, error) {
			attempts++
			if attempts < 3 {
				return "", errors.New("transient")
			}
			return "ok", nil
		})
		if err != nil || res != "ok" || attempts != 3 {
			t.Fatalf("expected ok on 3rd attempt, got res=%q attempts=%d err=%v", res, attempts, err)
		}
	})

	t.Run("exhausts maxAttempts and wraps lastErr", func(t *testing.T) {
		sentinel := errors.New("boom")
		_, err := retryConnect(context.Background(), "dep", retryConfig{
			maxAttempts:    3,
			initialBackoff: time.Millisecond,
			maxBackoff:     2 * time.Millisecond,
		}, nil, func() (string, error) {
			return "", sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected wrapped sentinel error, got %v", err)
		}
	})

	t.Run("cancels promptly during long backoff", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, err := retryConnect(ctx, "dep", retryConfig{
			maxAttempts:    5,
			initialBackoff: 5 * time.Second,
			maxBackoff:     10 * time.Second,
		}, nil, func() (string, error) {
			return "", errors.New("refused")
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("cancel took too long: %v", elapsed)
		}
	})
}
