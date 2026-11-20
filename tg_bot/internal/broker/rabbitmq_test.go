package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type declaredQueue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

type publishedRecord struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

type fakeDeferredConfirm struct {
	mu      sync.Mutex
	acked   bool
	err     error
	waitCh  chan struct{}
	waited  bool
}

func (d *fakeDeferredConfirm) WaitContext(ctx context.Context) (bool, error) {
	d.mu.Lock()
	d.waited = true
	waitCh := d.waitCh
	acked := d.acked
	err := d.err
	d.mu.Unlock()

	if waitCh != nil {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-waitCh:
			d.mu.Lock()
			acked = d.acked
			err = d.err
			d.mu.Unlock()
		}
	}
	return acked, err
}

type fakeAMQPChannel struct {
	mu             sync.Mutex
	closed         bool
	confirmCalled  bool
	confirmErr     error
	qosCalls       int
	qosErr         error
	declared       []declaredQueue
	declareErr     error
	consumeErr     error
	deliveries     chan amqp.Delivery
	pubErr         error
	pubSeqErr      []error
	publishes      []publishedRecord
	confirmFactory func(rec publishedRecord) (deferredConfirmation, error)
}

func newFakeAMQPChannel() *fakeAMQPChannel {
	return &fakeAMQPChannel{
		deliveries: make(chan amqp.Delivery, 4),
	}
}

func (c *fakeAMQPChannel) Confirm(_ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmCalled = true
	return c.confirmErr
}

func (c *fakeAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.declareErr != nil {
		return amqp.Queue{}, c.declareErr
	}
	c.declared = append(c.declared, declaredQueue{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	})
	return amqp.Queue{Name: name}, nil
}

func (c *fakeAMQPChannel) Qos(_, _ int, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.qosCalls++
	return c.qosErr
}

func (c *fakeAMQPChannel) Consume(_, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	return c.deliveries, nil
}

func (c *fakeAMQPChannel) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, amqp.ErrClosed
	}
	if len(c.pubSeqErr) > 0 {
		err := c.pubSeqErr[0]
		c.pubSeqErr = c.pubSeqErr[1:]
		if err != nil {
			return nil, err
		}
	}
	if c.pubErr != nil {
		return nil, c.pubErr
	}
	rec := publishedRecord{
		exchange:  exchange,
		key:       key,
		mandatory: mandatory,
		immediate: immediate,
		msg:       msg,
	}
	c.publishes = append(c.publishes, rec)
	if c.confirmFactory != nil {
		return c.confirmFactory(rec)
	}
	return &fakeDeferredConfirm{acked: true}, nil
}

func (c *fakeAMQPChannel) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeAMQPChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

type fakeAMQPConn struct {
	mu       sync.Mutex
	closed   bool
	channels []*fakeAMQPChannel
	chanIdx  int
	chanErr  error
}

func (c *fakeAMQPConn) Channel() (amqpChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, amqp.ErrClosed
	}
	if c.chanErr != nil {
		return nil, c.chanErr
	}
	if c.chanIdx < len(c.channels) {
		ch := c.channels[c.chanIdx]
		c.chanIdx++
		return ch, nil
	}
	ch := newFakeAMQPChannel()
	c.channels = append(c.channels, ch)
	c.chanIdx++
	return ch, nil
}

func (c *fakeAMQPConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeAMQPConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func TestNewRabbitMQ_DeclaresQueuesAndRedactsDialError(t *testing.T) {
	consumeCh := newFakeAMQPChannel()
	publishCh := newFakeAMQPChannel()
	conn := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh, publishCh}}

	r, err := newRabbitMQWithDialer("amqp://myuser:s3cr3tp@ss@localhost:5672/", func(_ context.Context, _ string) (amqpConn, error) {
		return conn, nil
	})
	if err != nil {
		t.Fatalf("newRabbitMQWithDialer failed: %v", err)
	}
	defer r.Close()

	if consumeCh.qosCalls != 1 {
		t.Fatalf("expected Qos called once on consumeCh, got %d", consumeCh.qosCalls)
	}
	if !publishCh.confirmCalled {
		t.Fatalf("expected Confirm(false) called on publishCh")
	}
	if len(consumeCh.declared) != 2 {
		t.Fatalf("expected 2 queues declared on consumeCh, got %+v", consumeCh.declared)
	}
	for _, q := range consumeCh.declared {
		if !q.durable || q.autoDelete || q.exclusive || q.noWait || q.args != nil {
			t.Fatalf("unexpected QueueDeclare args for %s: %+v", q.name, q)
		}
	}
	if consumeCh.declared[0].name != ReviewQueue || consumeCh.declared[1].name != ReviewDLQ {
		t.Fatalf("unexpected consumeCh queue order: %+v", consumeCh.declared)
	}
	if len(publishCh.declared) != 2 || publishCh.declared[0].name != FeedbackQueue || publishCh.declared[1].name != ReviewDLQ {
		t.Fatalf("unexpected publishCh declared queues: %+v", publishCh.declared)
	}

	rawURL := "amqp://admin:SuperSecretPass@rmq.internal:5672/"
	_, err = newRabbitMQWithDialer(rawURL, func(_ context.Context, u string) (amqpConn, error) {
		return nil, &url.Error{Op: "dial", URL: u, Err: errors.New("dial tcp: connection refused for " + u)}
	})
	if err == nil {
		t.Fatalf("expected dial error")
	}
	if strings.Contains(err.Error(), "admin") || strings.Contains(err.Error(), "SuperSecretPass") {
		t.Fatalf("dial error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "xxxxx:xxxxx@rmq.internal:5672") {
		t.Fatalf("expected redacted authority in error, got: %v", err)
	}
}

func TestRedactAMQPURL_AndSanitizeAMQPError(t *testing.T) {
	raw := "amqp://user%40org:p%3A%2Fss@broker.local:5672/"
	redacted := redactAMQPURL(raw)
	if strings.Contains(redacted, "user") || strings.Contains(redacted, "ss") {
		t.Fatalf("redactAMQPURL leaked credentials: %s", redacted)
	}
	if redacted != "amqp://xxxxx:xxxxx@broker.local:5672/" {
		t.Fatalf("unexpected redacted URL: %s", redacted)
	}

	wrapped := &url.Error{
		Op:  "dial",
		URL: raw,
		Err: errors.New("auth failed for " + raw + " and https://api.telegram.org/bot12345:SECRET_BOT_TOK/sendMessage"),
	}
	sanitized := sanitizeAMQPError(wrapped, raw)
	var urlErr *url.Error
	if !errors.As(sanitized, &urlErr) {
		t.Fatalf("expected *url.Error to remain unwrap-compatible")
	}
	msg := sanitized.Error()
	if strings.Contains(msg, "p%3A%2Fss") || strings.Contains(msg, "SECRET_BOT_TOK") {
		t.Fatalf("sanitized error leaked secrets: %s", msg)
	}
	if !strings.Contains(msg, "/bot<token>") {
		t.Fatalf("expected bot token redaction, got: %s", msg)
	}
}

func TestSendFeedback_RecoversClosedPublishChannelAndConnection(t *testing.T) {
	consumeCh := newFakeAMQPChannel()
	pubCh1 := newFakeAMQPChannel()
	pubCh2 := newFakeAMQPChannel()
	conn1 := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh, pubCh1, pubCh2}}

	consumeCh2 := newFakeAMQPChannel()
	pubCh3 := newFakeAMQPChannel()
	conn2 := &fakeAMQPConn{channels: []*fakeAMQPChannel{pubCh3, consumeCh2}}

	dialCount := 0
	r, err := newRabbitMQWithDialer("amqp://u:p@localhost:5672/", func(_ context.Context, _ string) (amqpConn, error) {
		dialCount++
		if dialCount == 1 {
			return conn1, nil
		}
		return conn2, nil
	})
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer r.Close()

	// Scenario 1: only publishCh closed -> reopens publishCh on conn1 without touching consumeCh or redialing
	_ = pubCh1.Close()
	if err := r.SendFeedback("Go Dev", "Не той стек"); err != nil {
		t.Fatalf("SendFeedback after publishCh close failed: %v", err)
	}
	if dialCount != 1 {
		t.Fatalf("expected no redial when conn1 still healthy, got dialCount=%d", dialCount)
	}
	if consumeCh.IsClosed() {
		t.Fatalf("expected consumeCh to remain open on publishCh-only recovery")
	}
	if !pubCh2.confirmCalled || len(pubCh2.publishes) != 1 || pubCh2.publishes[0].key != FeedbackQueue {
		t.Fatalf("expected publish on recovered pubCh2 with confirms, got publishes=%+v", pubCh2.publishes)
	}

	// Scenario 2: whole conn1 closed -> redials conn2, opens confirmed pubCh3, publishes successfully
	_ = conn1.Close()
	if err := r.SendFeedback("Backend Go", "Мало досвіду"); err != nil {
		t.Fatalf("SendFeedback after conn close failed: %v", err)
	}
	if dialCount != 2 {
		t.Fatalf("expected redial on closed conn, got dialCount=%d", dialCount)
	}
	if !pubCh3.confirmCalled || len(pubCh3.publishes) != 1 {
		t.Fatalf("expected publish on pubCh3 after redial, got publishes=%+v", pubCh3.publishes)
	}
}

func TestPublishReviewDLQ_EnvelopeAndConfirmation(t *testing.T) {
	consumeCh := newFakeAMQPChannel()
	publishCh := newFakeAMQPChannel()
	conn := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh, publishCh}}

	rawURL := "amqp://user:secret@broker:5672/"
	r, err := newRabbitMQWithDialer(rawURL, func(_ context.Context, _ string) (amqpConn, error) {
		return conn, nil
	})
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer r.Close()

	longReason := "failed at " + rawURL + " /bot123:SECRET_TOKEN " + strings.Repeat("x", 1200)
	if err := r.PublishReviewDLQ(context.Background(), []byte(`{"title":"Go Job"}`), longReason); err != nil {
		t.Fatalf("PublishReviewDLQ valid JSON failed: %v", err)
	}
	if err := r.PublishReviewDLQ(context.Background(), []byte("{bad\xffjson"), "json_decode error"); err != nil {
		t.Fatalf("PublishReviewDLQ invalid JSON failed: %v", err)
	}

	if len(publishCh.publishes) != 2 {
		t.Fatalf("expected 2 DLQ publishes, got %d", len(publishCh.publishes))
	}

	var env1 map[string]any
	if err := json.Unmarshal(publishCh.publishes[0].msg.Body, &env1); err != nil {
		t.Fatalf("unmarshal DLQ envelope 1: %v", err)
	}
	if env1["original_queue"] != ReviewQueue {
		t.Fatalf("expected original_queue=%s, got %v", ReviewQueue, env1["original_queue"])
	}
	reasonStr, _ := env1["error_reason"].(string)
	if strings.Contains(reasonStr, "secret") || strings.Contains(reasonStr, "SECRET_TOKEN") {
		t.Fatalf("DLQ error_reason leaked secret: %s", reasonStr)
	}
	if len([]rune(reasonStr)) > maxDLQReasonRunes {
		t.Fatalf("expected error_reason bounded to %d runes, got %d", maxDLQReasonRunes, len([]rune(reasonStr)))
	}
	payloadMap, ok := env1["payload"].(map[string]any)
	if !ok || payloadMap["title"] != "Go Job" {
		t.Fatalf("expected JSON payload preserved as object, got %#v", env1["payload"])
	}

	var env2 map[string]any
	if err := json.Unmarshal(publishCh.publishes[1].msg.Body, &env2); err != nil {
		t.Fatalf("unmarshal DLQ envelope 2: %v", err)
	}
	if _, ok := env2["payload"].(string); !ok {
		t.Fatalf("expected invalid JSON payload stored as string, got %T", env2["payload"])
	}
}

func TestConsumeProposals_ReconnectsWhenClosed(t *testing.T) {
	consumeCh1 := newFakeAMQPChannel()
	publishCh1 := newFakeAMQPChannel()
	consumeCh2 := newFakeAMQPChannel()
	conn1 := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh1, publishCh1, consumeCh2}}

	consumeCh3 := newFakeAMQPChannel()
	publishCh3 := newFakeAMQPChannel()
	conn2 := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh3, publishCh3}}

	dials := 0
	r, err := newRabbitMQWithDialer("amqp://u:p@localhost:5672/", func(_ context.Context, _ string) (amqpConn, error) {
		dials++
		if dials == 1 {
			return conn1, nil
		}
		return conn2, nil
	})
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer r.Close()

	_ = consumeCh1.Close()
	ch, err := r.ConsumeProposals(context.Background())
	if err != nil || ch != consumeCh2.deliveries {
		t.Fatalf("expected consumeCh2 deliveries after channel close, ch=%v err=%v", ch, err)
	}

	_ = conn1.Close()
	ch, err = r.ConsumeProposals(context.Background())
	if err != nil || ch != consumeCh3.deliveries {
		t.Fatalf("expected consumeCh3 deliveries after conn close, ch=%v err=%v", ch, err)
	}
}

func TestClose_IsIdempotentAndCancelsInFlightOps(t *testing.T) {
	consumeCh := newFakeAMQPChannel()
	waiting := make(chan struct{})
	publishStarted := make(chan struct{})
	publishCh := newFakeAMQPChannel()
	publishCh.confirmFactory = func(_ publishedRecord) (deferredConfirmation, error) {
		close(publishStarted)
		return &fakeDeferredConfirm{waitCh: waiting}, nil
	}
	conn := &fakeAMQPConn{channels: []*fakeAMQPChannel{consumeCh, publishCh}}

	r, err := newRabbitMQWithDialer("amqp://u:p@localhost:5672/", func(_ context.Context, _ string) (amqpConn, error) {
		return conn, nil
	})
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.SendFeedback("Job", "Reason")
	}()

	<-publishStarted
	start := time.Now()
	r.Close()
	r.Close() // idempotent

	select {
	case sendErr := <-errCh:
		if sendErr == nil {
			t.Fatalf("expected SendFeedback to abort on Close()")
		}
	case <-time.After(time.Second):
		t.Fatalf("SendFeedback did not unblock after Close()")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Close() took too long")
	}
}
