package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ReviewQueue   = "review_queue"
	ReviewDLQ     = "review_queue.dlq"
	FeedbackQueue = "feedback_queue"

	initTimeout       = 10 * time.Second
	dialTimeout       = 10 * time.Second
	closeTimeout      = 2 * time.Second
	publishTimeout    = 5 * time.Second
	maxDLQReasonRunes = 1000
)

var (
	errClientClosed = errors.New("rabbitmq client is closed")
	amqpURLPattern  = regexp.MustCompile(`amqps?://[^\s"'<>]+`)
	botTokenPattern = regexp.MustCompile(`/bot\d+:[A-Za-z0-9_-]+`)
)

type sanitizedError struct {
	msg   string
	cause error
}

func (e *sanitizedError) Error() string {
	return e.msg
}

func (e *sanitizedError) Unwrap() error {
	return e.cause
}

func redactAMQPURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	if u, err := url.Parse(rawURL); err == nil {
		if u.User != nil {
			u.User = url.UserPassword("xxxxx", "xxxxx")
			return u.String()
		}
		return rawURL
	}

	schemeIdx := strings.Index(rawURL, "://")
	if schemeIdx < 0 {
		return rawURL
	}
	prefix := rawURL[:schemeIdx+3]
	rest := rawURL[schemeIdx+3:]

	authEnd := len(rest)
	for _, sep := range []string{"/", "?", "#"} {
		if idx := strings.Index(rest, sep); idx >= 0 && idx < authEnd {
			authEnd = idx
		}
	}
	authority := rest[:authEnd]
	suffix := rest[authEnd:]

	atIdx := strings.LastIndex(authority, "@")
	if atIdx < 0 {
		return rawURL
	}
	return prefix + "xxxxx:xxxxx@" + authority[atIdx+1:] + suffix
}

func extractUserinfoAt(rawURL string) string {
	schemeIdx := strings.Index(rawURL, "://")
	if schemeIdx < 0 {
		return ""
	}
	rest := rawURL[schemeIdx+3:]
	authEnd := len(rest)
	for _, sep := range []string{"/", "?", "#"} {
		if idx := strings.Index(rest, sep); idx >= 0 && idx < authEnd {
			authEnd = idx
		}
	}
	authority := rest[:authEnd]
	atIdx := strings.LastIndex(authority, "@")
	if atIdx <= 0 {
		return ""
	}
	return authority[:atIdx+1]
}

func redactErrorText(msg string, rawURLs ...string) string {
	out := msg
	for _, raw := range rawURLs {
		if raw == "" {
			continue
		}
		if redacted := redactAMQPURL(raw); redacted != raw {
			out = strings.ReplaceAll(out, raw, redacted)
		}
		if userinfoAt := extractUserinfoAt(raw); userinfoAt != "" {
			out = strings.ReplaceAll(out, userinfoAt, "xxxxx:xxxxx@")
		}
	}
	out = amqpURLPattern.ReplaceAllStringFunc(out, redactAMQPURL)
	return botTokenPattern.ReplaceAllString(out, "/bot<token>")
}

func sanitizeAMQPError(err error, rawURL string) error {
	if err == nil {
		return nil
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		innerErr := sanitizeAMQPError(urlErr.Err, rawURL)
		redactedURL := redactAMQPURL(urlErr.URL)
		if direct, ok := err.(*url.Error); ok && direct == urlErr {
			return &url.Error{
				Op:  urlErr.Op,
				URL: redactedURL,
				Err: innerErr,
			}
		}
		return &sanitizedError{
			msg:   redactErrorText(err.Error(), rawURL, urlErr.URL),
			cause: innerErr,
		}
	}

	msg := err.Error()
	redactedMsg := redactErrorText(msg, rawURL)
	if redactedMsg == msg {
		return err
	}
	return errors.New(redactedMsg)
}

type Proposal struct {
	BatchEOF        bool     `json:"_batch_eof,omitempty"`
	Title           string   `json:"title"`
	Link            string   `json:"link"`
	MatchScore      int      `json:"match_score"`
	Summary         string   `json:"summary"`
	Pros            []string `json:"pros"`
	FluffPercentage int      `json:"fluff_percentage"`
	FinalLetter     string   `json:"final_letter"`
}

type deferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

type amqpChannel interface {
	Confirm(noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error)
	IsClosed() bool
	Close() error
}

type amqpConn interface {
	Channel() (amqpChannel, error)
	IsClosed() bool
	Close() error
}

type dialFunc func(ctx context.Context, url string) (amqpConn, error)

type realAMQPConn struct {
	conn *amqp.Connection
}

func (c *realAMQPConn) Channel() (amqpChannel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &realAMQPChannel{ch: ch}, nil
}

func (c *realAMQPConn) IsClosed() bool {
	return c.conn.IsClosed()
}

func (c *realAMQPConn) Close() error {
	return c.conn.CloseDeadline(time.Now().Add(closeTimeout))
}

type realAMQPChannel struct {
	ch *amqp.Channel
}

func (c *realAMQPChannel) Confirm(noWait bool) error {
	return c.ch.Confirm(noWait)
}

func (c *realAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return c.ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *realAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return c.ch.Qos(prefetchCount, prefetchSize, global)
}

func (c *realAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *realAMQPChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error) {
	dc, err := c.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	if dc == nil {
		return nil, nil
	}
	return dc, nil
}

func (c *realAMQPChannel) IsClosed() bool {
	return c.ch.IsClosed()
}

func (c *realAMQPChannel) Close() error {
	return c.ch.Close()
}

func defaultDial(ctx context.Context, rawURL string) (amqpConn, error) {
	dialer := &net.Dialer{Timeout: dialTimeout}
	conn, err := amqp.DialConfig(rawURL, amqp.Config{
		Locale: "en_US",
		Dial: func(network, addr string) (net.Conn, error) {
			netConn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			deadline := time.Now().Add(dialTimeout)
			if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
				deadline = ctxDeadline
			}
			_ = netConn.SetDeadline(deadline)
			return netConn, nil
		},
	})
	if err != nil {
		return nil, sanitizeAMQPError(err, rawURL)
	}
	return &realAMQPConn{conn: conn}, nil
}

type RabbitMQ struct {
	mu              sync.Mutex
	url             string
	dial            dialFunc
	conn            amqpConn
	consumeCh       amqpChannel
	publishCh       amqpChannel
	closed          bool
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	return newRabbitMQWithDialer(url, defaultDial)
}

func newRabbitMQWithDialer(rawURL string, dial dialFunc) (*RabbitMQ, error) {
	if dial == nil {
		dial = defaultDial
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	r := &RabbitMQ{
		url:             rawURL,
		dial:            dial,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}

	initCtx, cancel := context.WithTimeout(lifecycleCtx, initTimeout)
	defer cancel()

	r.mu.Lock()
	if _, err := r.ensureConsumeChannelLocked(initCtx); err != nil {
		r.resetLocked()
		r.mu.Unlock()
		lifecycleCancel()
		return nil, err
	}
	if _, err := r.ensurePublishChannelLocked(initCtx); err != nil {
		r.resetLocked()
		r.mu.Unlock()
		lifecycleCancel()
		return nil, err
	}
	r.mu.Unlock()

	slog.Info("connected to rabbitmq successfully")
	return r, nil
}

func (r *RabbitMQ) resetConsumeChannelLocked() {
	if r.consumeCh != nil {
		_ = r.consumeCh.Close()
		r.consumeCh = nil
	}
}

func (r *RabbitMQ) resetPublishChannelLocked() {
	if r.publishCh != nil {
		_ = r.publishCh.Close()
		r.publishCh = nil
	}
}

func (r *RabbitMQ) resetLocked() {
	r.resetPublishChannelLocked()
	r.resetConsumeChannelLocked()
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
}

func (r *RabbitMQ) ensureConnLocked(ctx context.Context) error {
	if r.closed {
		return errClientClosed
	}
	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}
	r.resetLocked()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.lifecycleCtx.Err(); err != nil {
		return errClientClosed
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	stopWatch := context.AfterFunc(r.lifecycleCtx, cancel)
	defer stopWatch()

	conn, err := r.dial(dialCtx, r.url)
	if err != nil {
		if r.closed || r.lifecycleCtx.Err() != nil {
			return errClientClosed
		}
		return fmt.Errorf("failed to connect to rabbitmq: %w", sanitizeAMQPError(err, r.url))
	}
	r.conn = conn
	return nil
}

func (r *RabbitMQ) openConsumeChannelLocked() (amqpChannel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open consume channel: %w", err)
	}
	if _, err := ch.QueueDeclare(ReviewQueue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to declare %s: %w", ReviewQueue, err)
	}
	if _, err := ch.QueueDeclare(ReviewDLQ, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to declare %s: %w", ReviewDLQ, err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to set qos on consume channel: %w", err)
	}
	return ch, nil
}

func (r *RabbitMQ) ensureConsumeChannelLocked(ctx context.Context) (amqpChannel, error) {
	if r.closed {
		return nil, errClientClosed
	}
	if r.conn != nil && !r.conn.IsClosed() && r.consumeCh != nil && !r.consumeCh.IsClosed() {
		return r.consumeCh, nil
	}

	if r.conn != nil && !r.conn.IsClosed() {
		r.resetConsumeChannelLocked()
		ch, err := r.openConsumeChannelLocked()
		if err == nil {
			r.consumeCh = ch
			return ch, nil
		}
		// Connection may be broken without IsClosed() yet; reset and re-dial once.
	}

	r.resetLocked()
	if err := r.ensureConnLocked(ctx); err != nil {
		return nil, err
	}
	ch, err := r.openConsumeChannelLocked()
	if err != nil {
		r.resetLocked()
		return nil, err
	}
	r.consumeCh = ch
	return ch, nil
}

func (r *RabbitMQ) openPublishChannelLocked() (amqpChannel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open publish channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}
	if _, err := ch.QueueDeclare(FeedbackQueue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to declare %s: %w", FeedbackQueue, err)
	}
	if _, err := ch.QueueDeclare(ReviewDLQ, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to declare %s: %w", ReviewDLQ, err)
	}
	return ch, nil
}

func (r *RabbitMQ) ensurePublishChannelLocked(ctx context.Context) (amqpChannel, error) {
	if r.closed {
		return nil, errClientClosed
	}
	if r.conn != nil && !r.conn.IsClosed() && r.publishCh != nil && !r.publishCh.IsClosed() {
		return r.publishCh, nil
	}

	if r.conn != nil && !r.conn.IsClosed() {
		r.resetPublishChannelLocked()
		ch, err := r.openPublishChannelLocked()
		if err == nil {
			r.publishCh = ch
			return ch, nil
		}
		// Connection may be broken without IsClosed() yet; reset and re-dial once.
	}

	r.resetLocked()
	if err := r.ensureConnLocked(ctx); err != nil {
		return nil, err
	}
	ch, err := r.openPublishChannelLocked()
	if err != nil {
		r.resetLocked()
		return nil, err
	}
	r.publishCh = ch
	return ch, nil
}

func (r *RabbitMQ) ConsumeProposals(ctx context.Context) (<-chan amqp.Delivery, error) {
	if r == nil {
		return nil, errors.New("rabbitmq client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopWatch := context.AfterFunc(r.lifecycleCtx, cancel)
	defer stopWatch()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, errClientClosed
	}

	ch, err := r.ensureConsumeChannelLocked(opCtx)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		ReviewQueue,
		"",
		false, // manual ACK
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.resetConsumeChannelLocked()
		if r.conn == nil || r.conn.IsClosed() {
			r.resetLocked()
		}
		return nil, fmt.Errorf("failed to consume %s: %w", ReviewQueue, err)
	}
	return msgs, nil
}

func (r *RabbitMQ) publish(ctx context.Context, queueName string, body []byte) error {
	if r == nil {
		return errors.New("rabbitmq client is nil")
	}
	if strings.TrimSpace(queueName) == "" {
		return errors.New("queue name must not be empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopWatch := context.AfterFunc(r.lifecycleCtx, cancel)
	defer stopWatch()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errClientClosed
	}

	ch, err := r.ensurePublishChannelLocked(opCtx)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(opCtx, "", queueName, false, false, msg)
	if err != nil {
		if opCtx.Err() != nil {
			r.mu.Unlock()
			return opCtx.Err()
		}
		r.resetPublishChannelLocked()
		if r.conn == nil || r.conn.IsClosed() {
			r.resetLocked()
		}
		ch, err = r.ensurePublishChannelLocked(opCtx)
		if err != nil {
			r.mu.Unlock()
			return err
		}
		dc, err = ch.PublishWithDeferredConfirmWithContext(opCtx, "", queueName, false, false, msg)
		if err != nil {
			r.resetPublishChannelLocked()
			if r.conn == nil || r.conn.IsClosed() {
				r.resetLocked()
			}
			r.mu.Unlock()
			return fmt.Errorf("failed to publish message to %s: %w", queueName, err)
		}
	}

	if dc == nil {
		r.resetPublishChannelLocked()
		r.mu.Unlock()
		return errors.New("rabbitmq publisher confirms not active")
	}

	usedCh := ch
	r.mu.Unlock()

	acked, err := dc.WaitContext(opCtx)
	if err != nil {
		r.mu.Lock()
		if !r.closed && r.publishCh == usedCh && (errors.Is(err, context.DeadlineExceeded) || usedCh.IsClosed()) {
			r.resetPublishChannelLocked()
		}
		r.mu.Unlock()
		return fmt.Errorf("await publish confirmation on %s: %w", queueName, err)
	}
	if !acked {
		r.mu.Lock()
		if !r.closed && r.publishCh == usedCh {
			r.resetPublishChannelLocked()
		}
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq publish to %s was nacked or channel closed before confirmation", queueName)
	}

	return nil
}

func (r *RabbitMQ) SendFeedback(jobTitle, reason string) error {
	payload := map[string]any{
		"job_title":  jobTitle,
		"reason":     reason,
		"status":     "rejected",
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	return r.publish(ctx, FeedbackQueue, body)
}

type dlqEnvelope struct {
	OriginalQueue string `json:"original_queue"`
	FailedAt      string `json:"failed_at"`
	ErrorReason   string `json:"error_reason"`
	Payload       any    `json:"payload"`
}

func boundRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes])
}

func (r *RabbitMQ) PublishReviewDLQ(ctx context.Context, rawBody []byte, reason string) error {
	if r == nil {
		return errors.New("rabbitmq client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	safeReason := boundRunes(redactErrorText(reason, r.url), maxDLQReasonRunes)

	var payload any
	if json.Valid(rawBody) {
		payload = json.RawMessage(bytes.Clone(rawBody))
	} else {
		payload = string(bytes.ToValidUTF8(rawBody, []byte("\uFFFD")))
	}

	env := dlqEnvelope{
		OriginalQueue: ReviewQueue,
		FailedAt:      time.Now().UTC().Format(time.RFC3339),
		ErrorReason:   safeReason,
		Payload:       payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("failed to marshal review_queue.dlq envelope: %w", err)
	}
	return r.publish(ctx, ReviewDLQ, body)
}

func (r *RabbitMQ) Close() {
	if r == nil {
		return
	}
	if r.lifecycleCancel != nil {
		r.lifecycleCancel()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	r.closed = true
	r.resetLocked()
}
