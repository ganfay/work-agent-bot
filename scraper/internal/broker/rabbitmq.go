package broker

import (
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

	"scraper/internal/djinni"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	initTimeout  = 10 * time.Second
	dialTimeout  = 10 * time.Second
	closeTimeout = 2 * time.Second
)

var (
	errClientClosed = errors.New("rabbitmq client is closed")
	amqpURLPattern  = regexp.MustCompile(`amqps?://[^\s"'<>]+`)
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
	return amqpURLPattern.ReplaceAllStringFunc(out, redactAMQPURL)
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

type deferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

type amqpChannel interface {
	Confirm(noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
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

func defaultDial(ctx context.Context, url string) (amqpConn, error) {
	dialer := &net.Dialer{Timeout: dialTimeout}
	conn, err := amqp.DialConfig(url, amqp.Config{
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
		return nil, sanitizeAMQPError(err, url)
	}
	return &realAMQPConn{conn: conn}, nil
}

type RabbitMQ struct {
	mu         sync.Mutex
	url        string
	dial       dialFunc
	conn       amqpConn
	ch         amqpChannel
	queues     map[string]struct{}
	queueOrder []string
	closed     bool
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	return newRabbitMQWithDialer(url, defaultDial)
}

func newRabbitMQWithDialer(url string, dial dialFunc) (*RabbitMQ, error) {
	if dial == nil {
		dial = defaultDial
	}
	r := &RabbitMQ{
		url:    url,
		dial:   dial,
		queues: make(map[string]struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()

	r.mu.Lock()
	_, err := r.ensureChannelLocked(ctx)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}

	slog.Info("rabbitmq connection initialized")
	return r, nil
}

func (r *RabbitMQ) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}
	r.closed = true
	r.resetLocked()
}

func (r *RabbitMQ) resetChannelLocked() {
	if r.ch != nil {
		_ = r.ch.Close()
		r.ch = nil
	}
}

func (r *RabbitMQ) resetLocked() {
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.resetChannelLocked()
}

func (r *RabbitMQ) openConfirmedChannelLocked() (amqpChannel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return ch, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}
	for _, qName := range r.queueOrder {
		if _, err := ch.QueueDeclare(qName, true, false, false, false, nil); err != nil {
			return ch, fmt.Errorf("failed to redeclare queue %q: %w", qName, err)
		}
	}
	return ch, nil
}

func (r *RabbitMQ) ensureChannelLocked(ctx context.Context) (amqpChannel, error) {
	if r.closed {
		return nil, errClientClosed
	}
	if r.conn != nil && !r.conn.IsClosed() && r.ch != nil && !r.ch.IsClosed() {
		return r.ch, nil
	}

	if r.conn != nil && !r.conn.IsClosed() {
		r.resetChannelLocked()
		ch, err := r.openConfirmedChannelLocked()
		if err == nil {
			r.ch = ch
			return ch, nil
		}
		r.ch = ch
	}
	r.resetLocked()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	conn, err := r.dial(ctx, r.url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", sanitizeAMQPError(err, r.url))
	}
	r.conn = conn

	ch, err := r.openConfirmedChannelLocked()
	if err != nil {
		r.ch = ch
		r.resetLocked()
		return nil, err
	}
	r.ch = ch
	return ch, nil
}

func (r *RabbitMQ) DeclareQueue(name string) (amqp.Queue, error) {
	if r == nil {
		return amqp.Queue{}, errors.New("rabbitmq client is nil")
	}
	if strings.TrimSpace(name) == "" {
		return amqp.Queue{}, errors.New("queue name must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return amqp.Queue{}, errClientClosed
	}

	ch, err := r.ensureChannelLocked(context.Background())
	if err != nil {
		return amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(
		name,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		if r.conn == nil || r.conn.IsClosed() {
			r.resetLocked()
		} else {
			r.resetChannelLocked()
		}
		return amqp.Queue{}, fmt.Errorf("failed to declare queue %q: %w", name, err)
	}

	if _, exists := r.queues[name]; !exists {
		r.queues[name] = struct{}{}
		r.queueOrder = append(r.queueOrder, name)
	}

	return q, nil
}

func (r *RabbitMQ) PublishJob(ctx context.Context, queueName string, job djinni.Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}
	return r.publish(ctx, queueName, body)
}

func (r *RabbitMQ) PublishBatchEOF(ctx context.Context, queueName string) error {
	return r.publish(ctx, queueName, []byte(`{"_batch_eof":true}`))
}

func (r *RabbitMQ) publish(ctx context.Context, queueName string, body []byte) error {
	if r == nil {
		return errors.New("rabbitmq client is nil")
	}
	if strings.TrimSpace(queueName) == "" {
		return errors.New("queue name must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errClientClosed
	}

	ch, err := r.ensureChannelLocked(ctx)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, msg)
	if err != nil {
		if ctx.Err() != nil {
			r.mu.Unlock()
			return err
		}
		r.resetLocked()
		ch, err = r.ensureChannelLocked(ctx)
		if err != nil {
			r.mu.Unlock()
			return err
		}
		dc, err = ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, msg)
		if err != nil {
			r.resetLocked()
			r.mu.Unlock()
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	if dc == nil {
		r.resetLocked()
		r.mu.Unlock()
		return errors.New("rabbitmq publisher confirms not active")
	}

	usedCh := ch
	r.mu.Unlock()

	acked, err := dc.WaitContext(ctx)
	if err != nil {
		r.mu.Lock()
		if !r.closed && r.ch == usedCh && (errors.Is(err, context.DeadlineExceeded) || usedCh.IsClosed()) {
			r.resetLocked()
		}
		r.mu.Unlock()
		return fmt.Errorf("await publish confirmation: %w", err)
	}
	if !acked {
		r.mu.Lock()
		if !r.closed && r.ch == usedCh {
			r.resetLocked()
		}
		r.mu.Unlock()
		return errors.New("rabbitmq publish was nacked or channel closed before confirmation")
	}

	return nil
}
