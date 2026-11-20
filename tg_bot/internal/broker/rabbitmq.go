package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var amqpURLPattern = regexp.MustCompile(`amqps?://[^\s"'<>]+`)

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

type RabbitMQ struct {
	conn      *amqp.Connection
	consumeCh *amqp.Channel
	publishCh *amqp.Channel
	pubMu     sync.Mutex
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", sanitizeAMQPError(err, url))
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open consume channel: %w", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		_ = consumeCh.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open publish channel: %w", err)
	}

	closeAll := func() {
		_ = publishCh.Close()
		_ = consumeCh.Close()
		_ = conn.Close()
	}

	// review_queue (звідки бот читає готові тексти)
	_, err = consumeCh.QueueDeclare(
		"review_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to declare review_queue: %w", err)
	}

	_, err = publishCh.QueueDeclare(
		"feedback_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to declare feedback_queue: %w", err)
	}

	if err := consumeCh.Qos(1, 0, false); err != nil {
		closeAll()
		return nil, fmt.Errorf("failed to set qos on consume channel: %w", err)
	}

	slog.Info("connected to rabbitmq successfully")
	return &RabbitMQ{
		conn:      conn,
		consumeCh: consumeCh,
		publishCh: publishCh,
	}, nil
}

func (r *RabbitMQ) ConsumeProposals() (<-chan amqp.Delivery, error) {
	msgs, err := r.consumeCh.Consume(
		"review_queue",
		"",
		false, // manual ACK
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to consume review_queue: %w", err)
	}
	return msgs, nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.pubMu.Lock()
	defer r.pubMu.Unlock()

	if r.publishCh == nil {
		return fmt.Errorf("publish channel is closed")
	}

	return r.publishCh.PublishWithContext(
		ctx,
		"",               // exchange
		"feedback_queue", // routing key
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
}

func (r *RabbitMQ) Close() {
	r.pubMu.Lock()
	if r.publishCh != nil {
		_ = r.publishCh.Close()
		r.publishCh = nil
	}
	r.pubMu.Unlock()

	if r.consumeCh != nil {
		_ = r.consumeCh.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}
