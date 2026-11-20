package bot

import (
	"bytes"
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
	"unicode/utf8"

	"tg_bot/internal/broker"

	tele "gopkg.in/telebot.v4"
)

type fakeFeedbackPublisher struct {
	mu    sync.Mutex
	calls []struct {
		jobTitle string
		reason   string
	}
	err       error
	started   chan struct{}
	release   chan struct{}
	blockOnce bool
}

func (f *fakeFeedbackPublisher) SendFeedback(jobTitle, reason string) error {
	f.mu.Lock()
	shouldBlock := f.blockOnce
	if shouldBlock {
		f.blockOnce = false
	}
	started := f.started
	release := f.release
	f.mu.Unlock()

	if shouldBlock {
		if started != nil {
			close(started)
		}
		if release != nil {
			<-release
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct {
		jobTitle string
		reason   string
	}{jobTitle: jobTitle, reason: reason})
	return nil
}

func (f *fakeFeedbackPublisher) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type tgAPICall struct {
	Method string
	Body   string
}

type mockTelegramServer struct {
	srv            *httptest.Server
	mu             sync.Mutex
	calls          []tgAPICall
	editStatusCode int
	editErrBody    string
	blockUpdates   chan struct{}
}

func newMockTelegramServer(t *testing.T) *mockTelegramServer {
	t.Helper()
	m := &mockTelegramServer{
		editStatusCode: http.StatusOK,
	}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		method := parts[len(parts)-1]

		if method == "getUpdates" {
			m.mu.Lock()
			blockCh := m.blockUpdates
			m.mu.Unlock()
			if blockCh != nil {
				select {
				case <-r.Context().Done():
					return
				case <-blockCh:
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
			return
		}

		m.mu.Lock()
		m.calls = append(m.calls, tgAPICall{Method: method, Body: string(bodyBytes)})
		editCode := m.editStatusCode
		editErrBody := m.editErrBody
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if method == "editMessageText" && editCode != http.StatusOK {
			w.WriteHeader(editCode)
			_, _ = w.Write([]byte(editErrBody))
			return
		}
		if method == "answerCallbackQuery" {
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			return
		}
		resp, _ := json.Marshal(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 777,
				"chat":       map[string]any{"id": 42, "type": "private"},
				"text":       "ok",
			},
		})
		_, _ = w.Write(resp)
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockTelegramServer) CallsByMethod(method string) []tgAPICall {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []tgAPICall
	for _, c := range m.calls {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

func newTestBot(t *testing.T, srv *httptest.Server, allowedUserID int64, pub FeedbackPublisher) *Bot {
	t.Helper()
	b, err := newWithSettings(tele.Settings{
		Token:   "123456:TESTTOKEN",
		URL:     srv.URL,
		Client:  srv.Client(),
		Offline: true,
	}, allowedUserID, pub)
	if err != nil {
		t.Fatalf("newWithSettings failed: %v", err)
	}
	return b
}

func TestFormatProposalHTML_TruncatesLongContentAndEscapesHTML(t *testing.T) {
	p := broker.Proposal{
		Title:           strings.Repeat("Go <Dev> ", 50),
		Link:            "https://djinni.co/jobs/123",
		MatchScore:      88,
		FluffPercentage: 5,
		Summary:         strings.Repeat("Strong match with Go & Postgres. ", 40),
		FinalLetter:     strings.Repeat("Built SplitCore with pgxpool & RabbitMQ. ", 100),
	}

	got := formatProposalHTML(p)
	if utf8.RuneCountInString(got) >= 4096 {
		t.Fatalf("formatted HTML exceeded Telegram 4096 limit: %d runes", utf8.RuneCountInString(got))
	}
	if !strings.Contains(got, "&lt;Dev&gt;") {
		t.Fatalf("expected HTML escaping of title, got: %s", got)
	}
	if !strings.Contains(got, `href="https://djinni.co/jobs/123"`) {
		t.Fatalf("expected valid Djinni link in HTML")
	}
}

func TestFormatProposalHTML_OmitsInvalidLink(t *testing.T) {
	p := broker.Proposal{
		Title:       "Go Dev",
		Link:        "javascript:alert(1)",
		MatchScore:  80,
		Summary:     "Good fit",
		FinalLetter: "Hi there",
	}

	got := formatProposalHTML(p)
	if strings.Contains(got, "<a href=") {
		t.Fatalf("expected invalid URL scheme to be omitted, got: %s", got)
	}
}

func TestSaveProposal_EvictsOldestMessageID(t *testing.T) {
	b := &Bot{proposals: make(map[int]broker.Proposal)}
	for i := 1; i <= 501; i++ {
		b.saveProposal(i, broker.Proposal{Title: "Job"})
	}
	if len(b.proposals) != 500 {
		t.Fatalf("expected cap of 500 proposals, got %d", len(b.proposals))
	}
	if _, ok := b.getProposal(1); ok {
		t.Fatalf("expected oldest msgID=1 to be evicted")
	}
	if _, ok := b.getProposal(501); !ok {
		t.Fatalf("expected newest msgID=501 to be retained")
	}
}

func TestNew_Validation(t *testing.T) {
	pub := &fakeFeedbackPublisher{}

	if _, err := New("", 42, pub); err == nil {
		t.Fatalf("expected error for empty token")
	}
	if _, err := New("   \t\n", 42, pub); err == nil {
		t.Fatalf("expected error for whitespace token")
	}
	if _, err := New("tok en", 42, pub); err == nil {
		t.Fatalf("expected error for token with whitespace")
	}
	if _, err := New("123:ABC", 0, pub); err == nil {
		t.Fatalf("expected error for allowedUserID == 0")
	}
	if _, err := New("123:ABC", -10, pub); err == nil {
		t.Fatalf("expected error for allowedUserID < 0")
	}
	if _, err := New("123:ABC", 42, nil); err == nil {
		t.Fatalf("expected error for nil publisher")
	}
}

func TestAuthMiddleware_FailClosed(t *testing.T) {
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{}
	b := newTestBot(t, mock.srv, 42, pub)

	// 1. Nil sender
	b.tg.ProcessUpdate(tele.Update{
		ID:      1,
		Message: &tele.Message{ID: 1, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "/start"},
	})
	// 2. Wrong user ID
	b.tg.ProcessUpdate(tele.Update{
		ID:      2,
		Message: &tele.Message{ID: 2, Sender: &tele.User{ID: 99, Username: "attacker"}, Chat: &tele.Chat{ID: 99, Type: tele.ChatPrivate}, Text: "/start"},
	})
	// 3. Group chat even when Sender.ID matches allowedUserID
	b.tg.ProcessUpdate(tele.Update{
		ID:      3,
		Message: &tele.Message{ID: 3, Sender: &tele.User{ID: 42, Username: "owner"}, Chat: &tele.Chat{ID: -100123, Type: tele.ChatGroup}, Text: "/start"},
	})
	if got := len(mock.CallsByMethod("sendMessage")); got != 0 {
		t.Fatalf("expected 0 sendMessage calls for blocked updates, got %d", got)
	}

	// 4. Allowed private chat
	b.tg.ProcessUpdate(tele.Update{
		ID:      4,
		Message: &tele.Message{ID: 4, Sender: &tele.User{ID: 42, Username: "owner"}, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "/start"},
	})
	sends := mock.CallsByMethod("sendMessage")
	if len(sends) != 1 || !strings.Contains(sends[0].Body, "AI Job Hunter Bot") {
		t.Fatalf("expected /start welcome message, got %+v", sends)
	}
}

func TestHandlers_ApproveRejectCancel(t *testing.T) {
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{}
	b := newTestBot(t, mock.srv, 42, pub)

	// Cache hit scenario for approve
	b.saveProposal(101, broker.Proposal{
		Title:       "Senior Go Backend",
		Link:        "https://djinni.co/jobs/101",
		MatchScore:  92,
		Summary:     "Great Go role",
		FinalLetter: "Hello!",
	})
	msg101 := &tele.Message{ID: 101, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "🚀 Senior Go Backend"}
	b.tg.ProcessUpdate(tele.Update{
		ID:       10,
		Callback: &tele.Callback{ID: "cb1", Sender: &tele.User{ID: 42}, Message: msg101, Data: "\fbtn_approve"},
	})
	edits := mock.CallsByMethod("editMessageText")
	if len(edits) != 1 || !strings.Contains(edits[0].Body, "СХВАЛЕНО ТОБОЮ") {
		t.Fatalf("expected approve edit, got %+v", edits)
	}

	// Cache miss scenario for reject -> cancel (restores Djinni URL button from EntityTextLink)
	msg202Text := "🚀 Staff Go\n🔗 Відкрити на Djinni"
	msg202 := &tele.Message{
		ID:   202,
		Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate},
		Text: msg202Text,
		Entities: []tele.MessageEntity{
			{Type: tele.EntityTextLink, Offset: 15, Length: 18, URL: "https://djinni.co/jobs/202"},
		},
	}
	b.tg.ProcessUpdate(tele.Update{
		ID:       11,
		Callback: &tele.Callback{ID: "cb2", Sender: &tele.User{ID: 42}, Message: msg202, Data: "\fbtn_reject"},
	})
	edits = mock.CallsByMethod("editMessageText")
	if len(edits) != 2 || !strings.Contains(edits[1].Body, "reason_exp") {
		t.Fatalf("expected reject reason menu edit, got %+v", edits)
	}

	b.tg.ProcessUpdate(tele.Update{
		ID:       12,
		Callback: &tele.Callback{ID: "cb3", Sender: &tele.User{ID: 42}, Message: msg202, Data: "\freason_cancel"},
	})
	edits = mock.CallsByMethod("editMessageText")
	if len(edits) != 3 || !strings.Contains(edits[2].Body, "https://djinni.co/jobs/202") {
		t.Fatalf("expected cancel edit to restore Djinni URL button from entity, got %+v", edits)
	}
}

func TestHandlers_ConcurrentDoubleTapIdempotency(t *testing.T) {
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		blockOnce: true,
	}
	b := newTestBot(t, mock.srv, 42, pub)
	b.saveProposal(303, broker.Proposal{Title: "Go Developer", FinalLetter: "Hi"})

	msg := &tele.Message{ID: 303, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "🚀 Go Developer"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.tg.ProcessUpdate(tele.Update{
			ID:       20,
			Callback: &tele.Callback{ID: "cb_a", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_exp"},
		})
	}()

	<-pub.started
	// Second concurrent tap while first is in-flight
	b.tg.ProcessUpdate(tele.Update{
		ID:       21,
		Callback: &tele.Callback{ID: "cb_b", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_stack"},
	})

	close(pub.release)
	wg.Wait()

	// Third tap after completion
	b.tg.ProcessUpdate(tele.Update{
		ID:       22,
		Callback: &tele.Callback{ID: "cb_c", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_domain"},
	})

	if pub.CallCount() != 1 {
		t.Fatalf("expected SendFeedback called exactly once, got %d", pub.CallCount())
	}

	answers := mock.CallsByMethod("answerCallbackQuery")
	var sawPending, sawDone bool
	for _, a := range answers {
		if strings.Contains(a.Body, "Запит уже обробляється") {
			sawPending = true
		}
		if strings.Contains(a.Body, "Дію вже виконано") {
			sawDone = true
		}
	}
	if !sawPending || !sawDone {
		t.Fatalf("expected both pending and done callback responses, got %+v", answers)
	}
}

func TestHandlers_ReasonRetryAfterPublishFailureAndNoDuplicateAfterEditFailure(t *testing.T) {
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{err: errors.New("rmq down")}
	b := newTestBot(t, mock.srv, 42, pub)
	b.saveProposal(404, broker.Proposal{Title: "Go Platform", FinalLetter: "Hi"})
	msg := &tele.Message{ID: 404, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "🚀 Go Platform"}

	// 1. SendFeedback fails -> reverts to idle so user can retry
	b.tg.ProcessUpdate(tele.Update{
		ID:       30,
		Callback: &tele.Callback{ID: "cb1", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_exp"},
	})
	if pub.CallCount() != 0 {
		t.Fatalf("expected 0 successful publishes")
	}

	// 2. RabbitMQ recovers, but Telegram editMessageText returns 500
	pub.mu.Lock()
	pub.err = nil
	pub.mu.Unlock()
	mock.mu.Lock()
	mock.editStatusCode = http.StatusInternalServerError
	mock.editErrBody = `{"ok":false,"error_code":500,"description":"Internal Server Error"}`
	mock.mu.Unlock()

	b.tg.ProcessUpdate(tele.Update{
		ID:       31,
		Callback: &tele.Callback{ID: "cb2", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_exp"},
	})
	if pub.CallCount() != 1 {
		t.Fatalf("expected 1 successful publish after retry, got %d", pub.CallCount())
	}

	// 3. Subsequent tap must NOT duplicate feedback even though edit failed
	b.tg.ProcessUpdate(tele.Update{
		ID:       32,
		Callback: &tele.Callback{ID: "cb3", Sender: &tele.User{ID: 42}, Message: msg, Data: "\freason_stack"},
	})
	if pub.CallCount() != 1 {
		t.Fatalf("expected SendFeedback not to be duplicated after edit failure, got %d", pub.CallCount())
	}
}

func TestFormatProposalHTML_ProsLongLinkAndUTF16Budget(t *testing.T) {
	p := broker.Proposal{
		Title:           strings.Repeat("🚀 Go & <Rust> ", 30),
		Link:            "https://djinni.co/jobs/" + strings.Repeat("a", 800),
		MatchScore:      95,
		FluffPercentage: 3,
		Summary:         strings.Repeat("💡 Strong backend fit & low fluff. ", 30),
		Pros: []string{
			"Go 1.25 + pgxpool",
			"RabbitMQ & Docker",
			"No legacy PHP",
		},
		FinalLetter: strings.Repeat("🎯 Built production Go services & pipelines. ", 120),
	}

	htmlOut := formatProposalHTML(p)
	if utf8.RuneCountInString(htmlOut) > maxBaseRawRunes {
		t.Fatalf("raw HTML exceeded %d runes: %d", maxBaseRawRunes, utf8.RuneCountInString(htmlOut))
	}
	if !strings.Contains(htmlOut, "✅ <b>Плюси:</b>") || !strings.Contains(htmlOut, "Go 1.25 + pgxpool") {
		t.Fatalf("expected Pros section in formatted HTML, got: %s", htmlOut)
	}
	if !strings.Contains(htmlOut, "Відкрити на Djinni") {
		t.Fatalf("expected 800-char valid link included")
	}

	// Pathological 3500-char Link + dense < > & escaping
	pHuge := broker.Proposal{
		Title:       "Go Dev",
		Link:        "https://djinni.co/jobs/" + strings.Repeat("x", 3500),
		MatchScore:  90,
		Summary:     "Solid match",
		FinalLetter: strings.Repeat("<script>&\"test\"", 400),
	}
	htmlHuge := formatProposalHTML(pHuge)
	if utf8.RuneCountInString(htmlHuge) > maxBaseRawRunes {
		t.Fatalf("huge-link HTML exceeded %d runes: %d", maxBaseRawRunes, utf8.RuneCountInString(htmlHuge))
	}
	if strings.Contains(htmlHuge, "Відкрити на Djinni") {
		t.Fatalf("expected pathological 3500-char inline link to be omitted so FinalLetter is preserved")
	}
	if !strings.Contains(htmlHuge, "&lt;script&gt;&amp;") {
		t.Fatalf("expected FinalLetter to remain present and escaped")
	}
}

func TestRenderBaseHTML_CacheMissPreservesEntitiesAndCapsLength(t *testing.T) {
	prefix := "🚀 " // 2 (surrogate) + 1 = 3 UTF-16 units
	boldTitle := "Go Backend"
	linkText := "Відкрити на Djinni"
	codeText := "Hello <World> & Co\n" + strings.Repeat("🚀", 2100)
	fullText := prefix + boldTitle + "\n" + linkText + "\n" + codeText + rejectPromptText

	prefixU16 := utf16Len(prefix)
	titleU16 := utf16Len(boldTitle)
	linkStart := prefixU16 + titleU16 + 1
	linkU16 := utf16Len(linkText)
	codeStart := linkStart + linkU16 + 1
	codeU16 := utf16Len(codeText)
	promptStart := codeStart + codeU16

	msg := &tele.Message{
		ID:   999, // not in b.proposals
		Text: fullText,
		Entities: []tele.MessageEntity{
			{Type: tele.EntityBold, Offset: prefixU16, Length: titleU16},
			{Type: tele.EntityTextLink, Offset: linkStart, Length: linkU16, URL: "https://djinni.co/jobs/999"},
			{Type: tele.EntityCode, Offset: codeStart, Length: codeU16},
			{Type: tele.EntityItalic, Offset: promptStart + 2, Length: utf16Len(rejectPromptText) - 2},
		},
	}
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{}
	botInst := newTestBot(t, mock.srv, 42, pub)
	msg.Chat = &tele.Chat{ID: 42, Type: tele.ChatPrivate}

	botInst.tg.ProcessUpdate(tele.Update{
		ID:       50,
		Callback: &tele.Callback{ID: "cb_miss", Sender: &tele.User{ID: 42}, Message: msg, Data: "\fbtn_approve"},
	})

	edits := mock.CallsByMethod("editMessageText")
	if len(edits) != 1 {
		t.Fatalf("expected 1 editMessageText call, got %d", len(edits))
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(edits[0].Body), &payload); err != nil {
		t.Fatalf("unmarshal edit payload: %v", err)
	}
	editedText, _ := payload["text"].(string)
	if !strings.Contains(editedText, "<b>Go Backend</b>") {
		t.Fatalf("expected bold entity preserved on cache miss, got: %s", editedText)
	}
	if !strings.Contains(editedText, `<a href="https://djinni.co/jobs/999">Відкрити на Djinni</a>`) {
		t.Fatalf("expected link entity preserved on cache miss, got: %s", editedText)
	}
	if !strings.Contains(editedText, "<code>Hello &lt;World&gt; &amp; Co") || !strings.Contains(editedText, "</code>") {
		t.Fatalf("expected code entity preserved and properly closed, got: %s", editedText)
	}
	if strings.Contains(editedText, "Вкажіть причину відхилення") {
		t.Fatalf("expected reject prompt suffix to be stripped, got: %s", editedText)
	}
	if utf8.RuneCountInString(editedText) > 4096 {
		t.Fatalf("expected edited text <= 4096 runes, got %d", utf8.RuneCountInString(editedText))
	}
}

func TestOnError_RoutesToSlog(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	mock := newMockTelegramServer(t)
	mock.mu.Lock()
	mock.editStatusCode = http.StatusInternalServerError
	mock.editErrBody = `{"ok":false,"error_code":500,"description":"boom"}`
	mock.mu.Unlock()

	pub := &fakeFeedbackPublisher{}
	b := newTestBot(t, mock.srv, 42, pub)

	b.tg.ProcessUpdate(tele.Update{
		ID: 60,
		Callback: &tele.Callback{
			ID:      "cb_err",
			Sender:  &tele.User{ID: 42},
			Message: &tele.Message{ID: 600, Chat: &tele.Chat{ID: 42, Type: tele.ChatPrivate}, Text: "🚀 Job"},
			Data:    "\fbtn_approve",
		},
	})

	// Also verify nil context does not panic
	b.tg.OnError(errors.New("nil context error"), nil)

	logged := buf.String()
	if !strings.Contains(logged, "telebot handler error") || !strings.Contains(logged, "nil context error") {
		t.Fatalf("expected OnError to log via slog, got: %s", logged)
	}
}

func TestBot_StopWithoutStartAndDoubleStopDoNotDeadlock(t *testing.T) {
	mock := newMockTelegramServer(t)
	pub := &fakeFeedbackPublisher{}
	b := newTestBot(t, mock.srv, 42, pub)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Stop()
		b.Stop()
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("Stop() without Start() deadlocked")
	}
}

func TestBot_EarlyStopCancelsGetUpdatesImmediately(t *testing.T) {
	mock := newMockTelegramServer(t)
	mock.mu.Lock()
	mock.blockUpdates = make(chan struct{})
	mock.mu.Unlock()
	t.Cleanup(func() {
		close(mock.blockUpdates)
	})

	pub := &fakeFeedbackPublisher{}
	b := newTestBot(t, mock.srv, 42, pub)

	startDone := b.StartAsync()
	time.Sleep(15 * time.Millisecond)

	stopStart := time.Now()
	b.Stop()
	b.Stop() // double stop must also be safe
	elapsed := time.Since(stopStart)

	select {
	case <-startDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("StartAsync channel did not close promptly")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("early Stop() took %v, expected < 200ms", elapsed)
	}
}

func TestCancelUpdatesTransport_KeepsContextAliveUntilBodyClose(t *testing.T) {
	pollCtx, pollCancel := context.WithCancel(context.Background())
	defer pollCancel()

	var bodyCtx context.Context
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		bodyCtx = req.Context()
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write([]byte(`{"ok":true,"result":[{"update_id":1}]}`))
			_ = pw.Close()
		}()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       pr,
			Header:     make(http.Header),
		}, nil
	})

	tr := &cancelUpdatesTransport{base: rt, pollCtx: pollCtx}
	req, err := http.NewRequest(http.MethodPost, "http://example.com/bot123/getUpdates", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if bodyCtx.Err() != nil {
		t.Fatalf("expected request context to remain active while reading body, got %v", bodyCtx.Err())
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !strings.Contains(string(data), `"update_id":1`) {
		t.Fatalf("unexpected body: %s", string(data))
	}
	_ = resp.Body.Close()
	if bodyCtx.Err() == nil {
		t.Fatalf("expected request context to be canceled after resp.Body.Close()")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
