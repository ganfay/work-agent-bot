package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"tg_bot/internal/broker"
)

type fakeAck struct {
	mu      sync.Mutex
	acks    []uint64
	nacks   []uint64
	requeue []bool
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

type fakeSender struct {
	mu   sync.Mutex
	sent []broker.Proposal
	err  error
}

func (s *fakeSender) SendVacancy(_ int64, p broker.Proposal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, p)
	return nil
}

func makeDelivery(ack amqp.Acknowledger, tag uint64, body []byte) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  tag,
		Body:         body,
	}
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
	runConsumerLoop(context.Background(), msgs, sender, 42, true, 5*time.Second, 25*time.Millisecond)
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
	runConsumerLoop(context.Background(), msgs, sender, 42, true, 30*time.Millisecond, 10*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 35*time.Millisecond {
		t.Fatalf("expected idleTimeout+pollGrace >=35ms, got %v", elapsed)
	}
}

func TestRunConsumerLoop_HandlesBadJSONAndSendError(t *testing.T) {
	ack := &fakeAck{}
	sender := &fakeSender{err: errors.New("telegram 429")}
	msgs := make(chan amqp.Delivery, 4)

	pBytes, _ := json.Marshal(broker.Proposal{Title: "Go Engineer", MatchScore: 88})
	eofBytes, _ := json.Marshal(broker.Proposal{BatchEOF: true})

	msgs <- makeDelivery(ack, 10, []byte("{bad-json"))
	msgs <- makeDelivery(ack, 11, pBytes)
	msgs <- makeDelivery(ack, 12, eofBytes)

	runConsumerLoop(context.Background(), msgs, sender, 42, true, time.Second, 0)

	if len(ack.nacks) != 2 {
		t.Fatalf("expected 2 nacks, got %+v", ack.nacks)
	}
	if ack.requeue[0] != false {
		t.Fatalf("expected bad JSON nack requeue=false")
	}
	if ack.requeue[1] != false {
		t.Fatalf("expected send error nack requeue=false in runOnce mode")
	}
	if len(ack.acks) != 1 || ack.acks[0] != 12 {
		t.Fatalf("expected EOF ack [12], got %+v", ack.acks)
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
	runConsumerLoop(ctx, msgs, sender, 42, false, 5*time.Millisecond, 100*time.Millisecond)
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

	err := runConsumerLoop(context.Background(), msgs, sender, 42, false, time.Second, 0)
	if !errors.Is(err, errBrokerChannelClosed) {
		t.Fatalf("expected errBrokerChannelClosed on unexpected close in daemon mode, got %v", err)
	}
}
