package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Proposal struct {
	Title           string   `json:"title"`
	Link            string   `json:"link"`
	MatchScore      int      `json:"match_score"`
	Summary         string   `json:"summary"`
	Pros            []string `json:"pros"`
	FluffPercentage int      `json:"fluff_percentage"`
	FinalLetter     string   `json:"final_letter"`
}

type RabbitMQ struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// review_queue (звідки бот читає готові тексти)
	_, err = ch.QueueDeclare(
		"review_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare review_queue: %w", err)
	}

	_, err = ch.QueueDeclare(
		"feedback_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare feedback_queue: %w", err)
	}

	_ = ch.Qos(1, 0, false)

	slog.Info("connected to rabbitmq successfully")
	return &RabbitMQ{conn: conn, ch: ch}, nil
}

func (r *RabbitMQ) ConsumeProposals() (<-chan amqp.Delivery, error) {
	msgs, err := r.ch.Consume(
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

	return r.ch.PublishWithContext(
		context.Background(),
		"",               // exchange
		"feedback_queue", // routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

func (r *RabbitMQ) Close() {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}
