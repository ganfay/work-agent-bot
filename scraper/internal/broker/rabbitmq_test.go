package broker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"scraper/internal/djinni"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeDeferredConfirm struct {
	mu      sync.Mutex
	done    chan struct{}
	acked   bool
	waitErr error
	closed  bool
}

func newFakeDeferredConfirm(acked bool, waitErr error) *fakeDeferredConfirm {
	dc := &fakeDeferredConfirm{
		done:    make(chan struct{}),
		acked:   acked,
		waitErr: waitErr,
		closed:  true,
	}
	close(dc.done)
	return dc
}

func newBlockingFakeDeferredConfirm() *fakeDeferredConfirm {
	return &fakeDeferredConfirm{
		done: make(chan struct{}),
	}
}

func (d *fakeDeferredConfirm) resolve(acked bool, waitErr error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.acked = acked
	d.waitErr = waitErr
	d.closed = true
	close(d.done)
}

func (d *fakeDeferredConfirm) WaitContext(ctx context.Context) (bool, error) {
	select {
	case <-d.done:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.acked, d.waitErr
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

type publishedRecord struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

type declareCall struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
}

type fakeChannel struct {
	mu           sync.Mutex
	closed       bool
	confirmCalls []bool
	confirmErr   error
	declared     []declareCall
	declareErr   error
	published    []publishedRecord
	publishErr   error
	nilDC        bool
	nextDC       func() deferredConfirmation
	pendingDCs   []*fakeDeferredConfirm
	closeCalls   int
}

func (c *fakeChannel) Confirm(noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmCalls = append(c.confirmCalls, noWait)
	return c.confirmErr
}

func (c *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, _ amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.declared = append(c.declared, declareCall{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
	})
	if c.declareErr != nil {
		c.closed = true
		return amqp.Queue{}, c.declareErr
	}
	return amqp.Queue{Name: name}, nil
}

func (c *fakeChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, amqp.ErrClosed
	}
	if c.publishErr != nil {
		return nil, c.publishErr
	}
	c.published = append(c.published, publishedRecord{
		exchange:  exchange,
		key:       key,
		mandatory: mandatory,
		immediate: immediate,
		msg:       msg,
	})
	if c.nilDC {
		return nil, nil
	}
	if c.nextDC != nil {
		dc := c.nextDC()
		if fdc, ok := dc.(*fakeDeferredConfirm); ok {
			c.pendingDCs = append(c.pendingDCs, fdc)
		}
		return dc, nil
	}
	return newFakeDeferredConfirm(true, nil), nil
}

func (c *fakeChannel) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeChannel) Close() error {
	c.mu.Lock()
	c.closed = true
	c.closeCalls++
	pending := append([]*fakeDeferredConfirm(nil), c.pendingDCs...)
	c.pendingDCs = nil
	c.mu.Unlock()

	for _, dc := range pending {
		dc.resolve(false, nil)
	}
	return nil
}

func (c *fakeChannel) setClosed(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = v
}

type fakeConn struct {
	mu         sync.Mutex
	closed     bool
	channels   []*fakeChannel
	channelErr error
	nextChFn   func() *fakeChannel
	closeCalls int
}

func (c *fakeConn) Channel() (amqpChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, amqp.ErrClosed
	}
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	var ch *fakeChannel
	if c.nextChFn != nil {
		ch = c.nextChFn()
	} else {
		ch = &fakeChannel{}
	}
	c.channels = append(c.channels, ch)
	return ch, nil
}

func (c *fakeConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.closeCalls++
	chs := append([]*fakeChannel(nil), c.channels...)
	c.mu.Unlock()

	for _, ch := range chs {
		_ = ch.Close()
	}
	return nil
}

func (c *fakeConn) setClosed(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = v
}

func TestNewRabbitMQ_DialAndSetupFailures(t *testing.T) {
	t.Run("dial error", func(t *testing.T) {
		wantErr := errors.New("dial refused")
		_, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected dial error %v, got %v", wantErr, err)
		}
	})

	t.Run("channel error closes connection", func(t *testing.T) {
		wantErr := errors.New("channel open failed")
		conn := &fakeConn{channelErr: wantErr}
		_, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected channel error %v, got %v", wantErr, err)
		}
		if conn.closeCalls == 0 {
			t.Fatal("expected connection to be closed when Channel() fails")
		}
	})

	t.Run("confirm error closes channel and connection", func(t *testing.T) {
		wantErr := errors.New("confirm select failed")
		ch := &fakeChannel{confirmErr: wantErr}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		_, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected confirm error %v, got %v", wantErr, err)
		}
		if ch.closeCalls == 0 || conn.closeCalls == 0 {
			t.Fatalf("expected channel and conn close, got ch=%d conn=%d", ch.closeCalls, conn.closeCalls)
		}
	})

	t.Run("success enables Confirm(false)", func(t *testing.T) {
		ch := &fakeChannel{}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if err != nil {
			t.Fatalf("newRabbitMQWithDialer failed: %v", err)
		}
		defer r.Close()

		if len(ch.confirmCalls) != 1 || ch.confirmCalls[0] != false {
			t.Fatalf("expected Confirm(false) called once, got %v", ch.confirmCalls)
		}
	})
}

func TestDeclareQueue_ValidationAndTracking(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
	r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
		return conn, nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	if _, err := r.DeclareQueue(""); err == nil {
		t.Fatal("expected error for empty queue name")
	}
	if _, err := r.DeclareQueue("   "); err == nil {
		t.Fatal("expected error for whitespace queue name")
	}

	q, err := r.DeclareQueue("new_vacancies")
	if err != nil {
		t.Fatalf("DeclareQueue failed: %v", err)
	}
	if q.Name != "new_vacancies" {
		t.Fatalf("expected queue name new_vacancies, got %q", q.Name)
	}
	if len(ch.declared) != 1 || !ch.declared[0].durable || ch.declared[0].autoDelete || ch.declared[0].exclusive || ch.declared[0].noWait {
		t.Fatalf("unexpected declare call options: %+v", ch.declared)
	}

	// QueueDeclare error closes/resets channel.
	ch.mu.Lock()
	ch.declareErr = errors.New("access refused")
	ch.mu.Unlock()
	if _, err := r.DeclareQueue("bad_queue"); err == nil {
		t.Fatal("expected error when QueueDeclare fails")
	}
	if ch.closeCalls == 0 {
		t.Fatal("expected channel to be closed/reset after QueueDeclare error")
	}

	// Closed client rejects DeclareQueue.
	r.Close()
	if _, err := r.DeclareQueue("after_close"); !errors.Is(err, errClientClosed) {
		t.Fatalf("expected errClientClosed after Close, got %v", err)
	}
}

func TestPublishJob_ConfirmsAndErrors(t *testing.T) {
	t.Run("publishes persistent JSON and awaits confirm", func(t *testing.T) {
		ch := &fakeChannel{}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		job := djinni.Job{
			Title:       "Go Engineer",
			Link:        "https://djinni.co/jobs/1",
			Description: "Build systems",
			PubDate:     "2026-01-01",
			GUID:        "guid-1",
		}
		if err := r.PublishJob(context.Background(), "new_vacancies", job); err != nil {
			t.Fatalf("PublishJob failed: %v", err)
		}

		if len(ch.published) != 1 {
			t.Fatalf("expected 1 published message, got %d", len(ch.published))
		}
		rec := ch.published[0]
		if rec.key != "new_vacancies" || rec.msg.DeliveryMode != amqp.Persistent || rec.msg.ContentType != "application/json" {
			t.Fatalf("unexpected publishing metadata: %+v", rec)
		}
		var gotJob djinni.Job
		if err := json.Unmarshal(rec.msg.Body, &gotJob); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}
		if gotJob != job {
			t.Fatalf("published job mismatch: got %+v, want %+v", gotJob, job)
		}
	})

	t.Run("empty queue name", func(t *testing.T) {
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return &fakeConn{}, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if err := r.PublishJob(context.Background(), "", djinni.Job{GUID: "g"}); err == nil {
			t.Fatal("expected error for empty queue name")
		}
	})

	t.Run("pre-canceled context", func(t *testing.T) {
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return &fakeConn{}, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := r.PublishJob(canceledCtx, "new_vacancies", djinni.Job{GUID: "g"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("broker nack", func(t *testing.T) {
		ch := &fakeChannel{
			nextDC: func() deferredConfirmation { return newFakeDeferredConfirm(false, nil) },
		}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g"}); err == nil {
			t.Fatal("expected error on broker nack")
		}
	})

	t.Run("deadline exceeded in WaitContext", func(t *testing.T) {
		ch := &fakeChannel{
			nextDC: func() deferredConfirmation { return newBlockingFakeDeferredConfirm() },
		}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		tCtx, tCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer tCancel()
		if err := r.PublishJob(tCtx, "new_vacancies", djinni.Job{GUID: "g"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("nil DeferredConfirmation", func(t *testing.T) {
		ch := &fakeChannel{nilDC: true}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g"}); err == nil || !strings.Contains(err.Error(), "confirms") {
			t.Fatalf("expected publisher confirms not active error, got %v", err)
		}
	})
}

func TestPublishBatchEOF_PayloadAndConfirm(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
	r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
		return conn, nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer r.Close()

	if err := r.PublishBatchEOF(context.Background(), "new_vacancies"); err != nil {
		t.Fatalf("PublishBatchEOF failed: %v", err)
	}
	if len(ch.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(ch.published))
	}
	rec := ch.published[0]
	if string(rec.msg.Body) != `{"_batch_eof":true}` {
		t.Fatalf("unexpected EOF payload: %s", string(rec.msg.Body))
	}
	if rec.msg.DeliveryMode != amqp.Persistent || rec.msg.ContentType != "application/json" {
		t.Fatalf("unexpected EOF publishing headers: %+v", rec.msg)
	}
}

func TestRabbitMQ_RecoveryAndRedeclaresQueues(t *testing.T) {
	t.Run("channel closed opens new channel and redeclares tracked queues", func(t *testing.T) {
		ch1 := &fakeChannel{}
		ch2 := &fakeChannel{}
		var chSeq atomic.Int32
		conn := &fakeConn{
			nextChFn: func() *fakeChannel {
				if chSeq.Add(1) == 1 {
					return ch1
				}
				return ch2
			},
		}
		var dialCount atomic.Int32
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			dialCount.Add(1)
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if _, err := r.DeclareQueue("new_vacancies"); err != nil {
			t.Fatalf("DeclareQueue failed: %v", err)
		}

		// Simulate channel closed by broker while conn stays open.
		ch1.setClosed(true)

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g-1"}); err != nil {
			t.Fatalf("PublishJob after channel recovery failed: %v", err)
		}
		if dialCount.Load() != 1 {
			t.Fatalf("expected no redial when conn is still open, got %d dials", dialCount.Load())
		}
		if len(ch2.confirmCalls) != 1 || ch2.confirmCalls[0] != false {
			t.Fatalf("expected Confirm(false) on recovered channel, got %v", ch2.confirmCalls)
		}
		if len(ch2.declared) != 1 || ch2.declared[0].name != "new_vacancies" {
			t.Fatalf("expected new_vacancies redeclared on ch2, got %+v", ch2.declared)
		}
		if len(ch2.published) != 1 {
			t.Fatalf("expected publish on ch2, got %d", len(ch2.published))
		}
	})

	t.Run("connection closed redials and redeclares queues", func(t *testing.T) {
		ch1 := &fakeChannel{}
		conn1 := &fakeConn{nextChFn: func() *fakeChannel { return ch1 }}
		ch2 := &fakeChannel{}
		conn2 := &fakeConn{nextChFn: func() *fakeChannel { return ch2 }}

		var dialCount atomic.Int32
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			if dialCount.Add(1) == 1 {
				return conn1, nil
			}
			return conn2, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if _, err := r.DeclareQueue("new_vacancies"); err != nil {
			t.Fatalf("DeclareQueue failed: %v", err)
		}

		// Simulate dropped TCP connection.
		conn1.setClosed(true)

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g-2"}); err != nil {
			t.Fatalf("PublishJob after redial failed: %v", err)
		}
		if dialCount.Load() != 2 {
			t.Fatalf("expected 2 dials after connection drop, got %d", dialCount.Load())
		}
		if len(ch2.declared) != 1 || ch2.declared[0].name != "new_vacancies" {
			t.Fatalf("expected tracked queue redeclared on conn2, got %+v", ch2.declared)
		}
		if len(ch2.published) != 1 {
			t.Fatalf("expected publish on conn2/ch2, got %d", len(ch2.published))
		}
	})

	t.Run("publish transport error resets and retries once", func(t *testing.T) {
		ch1 := &fakeChannel{publishErr: errors.New("broken pipe")}
		conn1 := &fakeConn{nextChFn: func() *fakeChannel { return ch1 }}
		ch2 := &fakeChannel{}
		conn2 := &fakeConn{nextChFn: func() *fakeChannel { return ch2 }}

		var dialCount atomic.Int32
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			if dialCount.Add(1) == 1 {
				return conn1, nil
			}
			return conn2, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g-3"}); err != nil {
			t.Fatalf("expected retry to succeed, got %v", err)
		}
		if dialCount.Load() != 2 || len(ch2.published) != 1 {
			t.Fatalf("expected reconnect and 1 publish on ch2, dials=%d pubs=%d", dialCount.Load(), len(ch2.published))
		}
	})

	t.Run("nack resets channel for subsequent publish", func(t *testing.T) {
		ch1 := &fakeChannel{
			nextDC: func() deferredConfirmation { return newFakeDeferredConfirm(false, nil) },
		}
		conn1 := &fakeConn{nextChFn: func() *fakeChannel { return ch1 }}
		ch2 := &fakeChannel{}
		conn2 := &fakeConn{nextChFn: func() *fakeChannel { return ch2 }}

		var dialCount atomic.Int32
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			if dialCount.Add(1) == 1 {
				return conn1, nil
			}
			return conn2, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer r.Close()

		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "nack"}); err == nil {
			t.Fatal("expected nack error")
		}
		if err := r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "ok"}); err != nil {
			t.Fatalf("expected next publish on fresh connection/channel to succeed, got %v", err)
		}
		if dialCount.Load() != 2 || len(ch2.published) != 1 {
			t.Fatalf("expected recovery dial and publish, dials=%d pubs=%d", dialCount.Load(), len(ch2.published))
		}
	})
}

func TestRabbitMQ_ConcurrencyAndCloseUnblocksWait(t *testing.T) {
	t.Run("Close unblocks concurrent WaitContext without deadlock", func(t *testing.T) {
		waitEntered := make(chan struct{})
		blockingDC := newBlockingFakeDeferredConfirm()
		ch := &fakeChannel{
			nextDC: func() deferredConfirmation {
				close(waitEntered)
				return blockingDC
			},
		}
		conn := &fakeConn{nextChFn: func() *fakeChannel { return ch }}
		var dialCount atomic.Int32
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			dialCount.Add(1)
			return conn, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		pubDone := make(chan error, 1)
		go func() {
			pubDone <- r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "blocked"})
		}()

		<-waitEntered
		// Small yield to ensure PublishJob is waiting inside WaitContext after releasing r.mu.
		time.Sleep(10 * time.Millisecond)

		closeDone := make(chan struct{})
		go func() {
			r.Close()
			r.Close() // idempotent
			close(closeDone)
		}()

		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("Close() deadlocked while PublishJob was waiting for confirmation")
		}

		select {
		case pubErr := <-pubDone:
			if pubErr == nil {
				t.Fatal("expected PublishJob to fail when channel closed during WaitContext")
			}
		case <-time.After(time.Second):
			t.Fatal("PublishJob did not unblock after Close()")
		}

		// Post-Close calls fail without dialing.
		if err := r.PublishBatchEOF(context.Background(), "new_vacancies"); !errors.Is(err, errClientClosed) {
			t.Fatalf("expected errClientClosed after Close, got %v", err)
		}
		if dialCount.Load() != 1 {
			t.Fatalf("expected no redial after Close, got %d", dialCount.Load())
		}
	})

	t.Run("concurrent publish declare and close race-safe", func(t *testing.T) {
		r, err := newRabbitMQWithDialer("amqp://localhost", func(_ context.Context, _ string) (amqpConn, error) {
			return &fakeConn{}, nil
		})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, _ = r.DeclareQueue("new_vacancies")
					_ = r.PublishJob(context.Background(), "new_vacancies", djinni.Job{GUID: "g"})
					_ = r.PublishBatchEOF(context.Background(), "new_vacancies")
				}
			}(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			r.Close()
		}()
		wg.Wait()
	})
}
