package bot

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"tg_bot/internal/broker"

	tele "gopkg.in/telebot.v4"
)

const (
	maxTitleRunes     = 200
	maxSummaryRunes   = 700
	maxLetterRunes    = 2200
	maxProItemRunes   = 160
	maxProsCount      = 5
	maxProsTotalUTF16 = 450
	maxBaseUTF16      = 3900
	maxBaseRawRunes   = 3900
	maxActionStates   = 1000

	rejectPromptText = "\n\nВкажіть причину відхилення для навчання ШІ:"
)

type actionState uint8

const (
	actionIdle actionState = iota
	actionPending
	actionDone
)

func isValidHTTPURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func utf16Len(s string) int {
	total := 0
	for _, r := range s {
		n := utf16.RuneLen(r)
		if n < 0 {
			n = 1
		}
		total += n
	}
	return total
}

func truncateUTF16(s string, maxUnits int) string {
	if maxUnits <= 0 {
		return ""
	}
	if utf16Len(s) <= maxUnits {
		return s
	}
	if maxUnits == 1 {
		return "…"
	}
	limit := maxUnits - 1
	var b strings.Builder
	units := 0
	for _, r := range s {
		n := utf16.RuneLen(r)
		if n < 0 {
			n = 1
		}
		if units+n > limit {
			break
		}
		b.WriteRune(r)
		units += n
	}
	b.WriteRune('…')
	return b.String()
}

func shrinkEscapedToRawBudget(s string, maxEscapedRunes int) string {
	if maxEscapedRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(html.EscapeString(s)) <= maxEscapedRunes {
		return s
	}
	runes := []rune(s)
	if maxEscapedRunes == 1 {
		if utf8.RuneCountInString(html.EscapeString("…")) <= maxEscapedRunes {
			return "…"
		}
		return ""
	}
	lo, hi := 0, len(runes)
	best := ""
	for lo <= hi {
		mid := (lo + hi) / 2
		candidate := string(runes[:mid])
		if mid < len(runes) && mid > 0 {
			candidate = string(runes[:mid-1]) + "…"
		}
		if utf8.RuneCountInString(html.EscapeString(candidate)) <= maxEscapedRunes {
			best = candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return best
}

type notifyPoller struct {
	inner tele.Poller
	once  sync.Once
	ready chan struct{}
}

func (p *notifyPoller) Poll(b *tele.Bot, updates chan tele.Update, stop chan struct{}) {
	p.once.Do(func() { close(p.ready) })
	p.inner.Poll(b, updates, stop)
}

type cancelOnCloseBody struct {
	io.ReadCloser
	once    sync.Once
	cleanup func()
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cleanup)
	return err
}

type cancelUpdatesTransport struct {
	base    http.RoundTripper
	pollCtx context.Context
}

func (t *cancelUpdatesTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	if !strings.HasSuffix(req.URL.Path, "/getUpdates") {
		return base.RoundTrip(req)
	}
	reqCtx, cancel := context.WithCancel(req.Context())
	stopWatch := context.AfterFunc(t.pollCtx, cancel)
	cleanup := func() {
		stopWatch()
		cancel()
	}
	resp, err := base.RoundTrip(req.Clone(reqCtx))
	if err != nil || resp == nil || resp.Body == nil {
		cleanup()
		return resp, err
	}
	resp.Body = &cancelOnCloseBody{
		ReadCloser: resp.Body,
		cleanup:    cleanup,
	}
	return resp, nil
}

// FeedbackPublisher publishes user rejection feedback for AI training.
type FeedbackPublisher interface {
	SendFeedback(jobTitle, reason string) error
}

type Bot struct {
	tg            *tele.Bot
	publisher     FeedbackPublisher
	allowedUserID int64

	pollCancel  context.CancelFunc
	pollerReady chan struct{}
	stateMu     sync.Mutex
	started     bool
	stopped     bool
	startDone   chan struct{}
	stopOnce    sync.Once

	mu           sync.RWMutex
	proposals    map[int]broker.Proposal
	actionStates map[int]actionState
}

func New(token string, allowedUserID int64, publisher FeedbackPublisher) (*Bot, error) {
	return newWithSettings(tele.Settings{
		Token:       token,
		Poller:      &tele.LongPoller{Timeout: 10 * time.Second},
		Synchronous: true,
	}, allowedUserID, publisher)
}

func newWithSettings(pref tele.Settings, allowedUserID int64, publisher FeedbackPublisher) (*Bot, error) {
	trimmedToken := strings.TrimSpace(pref.Token)
	if trimmedToken == "" {
		return nil, fmt.Errorf("telegram bot token must not be empty")
	}
	if strings.ContainsAny(trimmedToken, " \t\r\n") {
		return nil, fmt.Errorf("telegram bot token must not contain whitespace")
	}
	if allowedUserID <= 0 {
		return nil, fmt.Errorf("allowed user ID must be positive, got %d", allowedUserID)
	}
	if publisher == nil {
		return nil, fmt.Errorf("feedback publisher must not be nil")
	}

	pollCtx, pollCancel := context.WithCancel(context.Background())
	ready := make(chan struct{})

	innerPoller := pref.Poller
	if innerPoller == nil {
		innerPoller = &tele.LongPoller{Timeout: 10 * time.Second}
	}
	pref.Token = trimmedToken
	pref.Synchronous = true
	pref.Poller = &notifyPoller{inner: innerPoller, ready: ready}

	if pref.Client == nil {
		pref.Client = &http.Client{
			Timeout:   time.Minute,
			Transport: &cancelUpdatesTransport{pollCtx: pollCtx},
		}
	} else {
		clientCopy := *pref.Client
		clientCopy.Transport = &cancelUpdatesTransport{
			base:    pref.Client.Transport,
			pollCtx: pollCtx,
		}
		pref.Client = &clientCopy
	}

	if pref.OnError == nil {
		pref.OnError = func(err error, c tele.Context) {
			if err == nil {
				return
			}
			attrs := []any{"error", err}
			if c != nil {
				attrs = append(attrs, "update_id", c.Update().ID)
				if sender := c.Sender(); sender != nil {
					attrs = append(attrs, "sender_id", sender.ID)
				}
			}
			slog.Error("telebot handler error", attrs...)
		}
	}

	b, err := tele.NewBot(pref)
	if err != nil {
		pollCancel()
		return nil, fmt.Errorf("failed to create bot: %w", err)
	}

	instance := &Bot{
		tg:            b,
		publisher:     publisher,
		allowedUserID: allowedUserID,
		pollCancel:    pollCancel,
		pollerReady:   ready,
		proposals:     make(map[int]broker.Proposal),
		actionStates:  make(map[int]actionState),
	}

	instance.registerHandlers()
	return instance, nil
}

func (b *Bot) StartAsync() <-chan struct{} {
	b.stateMu.Lock()
	if b.started || b.stopped {
		done := b.startDone
		b.stateMu.Unlock()
		if done == nil {
			closedCh := make(chan struct{})
			close(closedCh)
			return closedCh
		}
		return done
	}
	b.started = true
	done := make(chan struct{})
	b.startDone = done
	b.stateMu.Unlock()

	go func() {
		defer close(done)
		slog.Info("telegram bot started listening for events")
		b.tg.Start()
	}()
	return done
}

func (b *Bot) Start() {
	<-b.StartAsync()
}

func (b *Bot) Stop() {
	if b == nil {
		return
	}
	b.stateMu.Lock()
	b.stopped = true
	started := b.started
	done := b.startDone
	b.stateMu.Unlock()

	if b.pollCancel != nil {
		b.pollCancel()
	}
	if !started {
		return
	}
	<-b.pollerReady
	b.stopOnce.Do(func() {
		b.tg.Stop()
	})
	<-done
}

func formatProsSection(pros []string) (rawHTML string, renderedText string) {
	if len(pros) == 0 {
		return "", ""
	}
	var htmlBuf strings.Builder
	var textBuf strings.Builder
	count := 0
	for _, rawPro := range pros {
		clean := strings.TrimSpace(rawPro)
		if clean == "" {
			continue
		}
		item := truncateUTF16(clean, maxProItemRunes)
		if count == 0 {
			htmlBuf.WriteString("\n\n✅ <b>Плюси:</b>")
			textBuf.WriteString("\n\n✅ Плюси:")
		}
		nextHTML := "\n• " + html.EscapeString(item)
		nextText := "\n• " + item
		if utf16Len(textBuf.String()+nextText) > maxProsTotalUTF16 {
			break
		}
		htmlBuf.WriteString(nextHTML)
		textBuf.WriteString(nextText)
		count++
		if count >= maxProsCount {
			break
		}
	}
	if count == 0 {
		return "", ""
	}
	return htmlBuf.String(), textBuf.String()
}

func formatProposalHTML(p broker.Proposal) string {
	title := truncateUTF16(p.Title, maxTitleRunes)
	summary := truncateUTF16(p.Summary, maxSummaryRunes)
	prosHTML, prosText := formatProsSection(p.Pros)

	buildParts := func(t, s, pHTML, pText string, includeLink bool) (string, string, string) {
		linkHTML := ""
		linkText := ""
		if includeLink && isValidHTTPURL(p.Link) {
			escapedLink := html.EscapeString(strings.TrimSpace(p.Link))
			linkHTML = fmt.Sprintf("\n🔗 <a href=\"%s\">Відкрити на Djinni</a>", escapedLink)
			linkText = "\n🔗 Відкрити на Djinni"
		}
		prefixHTML := fmt.Sprintf(
			"🚀 <b>%s</b>\n🎯 <b>Match Score:</b> %d/100\n📊 <b>Fluff Index:</b> %d%%%s\n\n💡 <b>Аналіз:</b> %s%s\n\n📝 <b>Готовий супровідний лист (тапни щоб скопіювати):</b>\n<code>",
			html.EscapeString(t),
			p.MatchScore,
			p.FluffPercentage,
			linkHTML,
			html.EscapeString(s),
			pHTML,
		)
		prefixText := fmt.Sprintf(
			"🚀 %s\n🎯 Match Score: %d/100\n📊 Fluff Index: %d%%%s\n\n💡 Аналіз: %s%s\n\n📝 Готовий супровідний лист (тапни щоб скопіювати):\n",
			t,
			p.MatchScore,
			p.FluffPercentage,
			linkText,
			s,
			pText,
		)
		return prefixHTML, prefixText, "</code>"
	}

	includeLink := isValidHTTPURL(p.Link)
	prefixHTML, prefixText, suffixHTML := buildParts(title, summary, prosHTML, prosText, includeLink)
	if includeLink && (utf8.RuneCountInString(prefixHTML)+utf8.RuneCountInString(suffixHTML) > maxBaseRawRunes-200 ||
		utf16Len(prefixText) > maxBaseUTF16-200) {
		includeLink = false
		prefixHTML, prefixText, suffixHTML = buildParts(title, summary, prosHTML, prosText, false)
	}

	if utf8.RuneCountInString(prefixHTML)+utf8.RuneCountInString(suffixHTML) > maxBaseRawRunes-100 ||
		utf16Len(prefixText) > maxBaseUTF16-100 {
		prosHTML, prosText = "", ""
		prefixHTML, prefixText, suffixHTML = buildParts(title, summary, "", "", includeLink)
	}

	if overhead := utf8.RuneCountInString(prefixHTML) + utf8.RuneCountInString(suffixHTML); overhead > maxBaseRawRunes-100 {
		excess := overhead - (maxBaseRawRunes - 100)
		escSummaryRunes := utf8.RuneCountInString(html.EscapeString(summary))
		if escSummaryRunes > excess {
			summary = shrinkEscapedToRawBudget(summary, escSummaryRunes-excess)
		} else {
			summary = ""
		}
		prefixHTML, prefixText, suffixHTML = buildParts(title, summary, prosHTML, prosText, includeLink)
		if overhead2 := utf8.RuneCountInString(prefixHTML) + utf8.RuneCountInString(suffixHTML); overhead2 > maxBaseRawRunes-50 {
			excess2 := overhead2 - (maxBaseRawRunes - 50)
			escTitleRunes := utf8.RuneCountInString(html.EscapeString(title))
			if escTitleRunes > excess2 {
				title = shrinkEscapedToRawBudget(title, escTitleRunes-excess2)
			} else {
				title = ""
			}
			prefixHTML, prefixText, suffixHTML = buildParts(title, summary, prosHTML, prosText, includeLink)
		}
	}

	remUTF16 := maxBaseUTF16 - utf16Len(prefixText)
	if remUTF16 > maxLetterRunes {
		remUTF16 = maxLetterRunes
	}
	if remUTF16 < 0 {
		remUTF16 = 0
	}

	remRaw := maxBaseRawRunes - utf8.RuneCountInString(prefixHTML) - utf8.RuneCountInString(suffixHTML)
	if remRaw < 0 {
		remRaw = 0
	}

	letter := truncateUTF16(p.FinalLetter, remUTF16)
	letter = shrinkEscapedToRawBudget(letter, remRaw)

	return prefixHTML + html.EscapeString(letter) + suffixHTML
}

func (b *Bot) getProposal(msgID int) (broker.Proposal, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	p, ok := b.proposals[msgID]
	return p, ok
}

func (b *Bot) saveProposal(msgID int, p broker.Proposal) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.proposals) >= 500 {
		oldestID := 0
		first := true
		for k := range b.proposals {
			if first || k < oldestID {
				oldestID = k
				first = false
			}
		}
		if !first {
			delete(b.proposals, oldestID)
		}
	}
	b.proposals[msgID] = p
}

func (b *Bot) beginAction(msgID int) actionState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if state, ok := b.actionStates[msgID]; ok && state != actionIdle {
		return state
	}
	if len(b.actionStates) >= maxActionStates {
		oldestID := 0
		first := true
		for k, st := range b.actionStates {
			if st == actionPending {
				continue
			}
			if first || k < oldestID {
				oldestID = k
				first = false
			}
		}
		if !first {
			delete(b.actionStates, oldestID)
		}
	}
	b.actionStates[msgID] = actionPending
	return actionIdle
}

func (b *Bot) endAction(msgID int, next actionState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if next == actionIdle {
		delete(b.actionStates, msgID)
		return
	}
	b.actionStates[msgID] = next
}

func entityTags(e tele.MessageEntity) (openTag, closeTag string, ok bool) {
	switch e.Type {
	case tele.EntityBold:
		return "<b>", "</b>", true
	case tele.EntityItalic:
		return "<i>", "</i>", true
	case tele.EntityUnderline:
		return "<u>", "</u>", true
	case tele.EntityStrikethrough:
		return "<s>", "</s>", true
	case tele.EntityCode:
		return "<code>", "</code>", true
	case tele.EntityCodeBlock:
		if strings.TrimSpace(e.Language) != "" {
			return fmt.Sprintf(`<pre><code class="language-%s">`, html.EscapeString(strings.TrimSpace(e.Language))), "</code></pre>", true
		}
		return "<pre>", "</pre>", true
	case tele.EntitySpoiler:
		return "<tg-spoiler>", "</tg-spoiler>", true
	case tele.EntityTextLink:
		if !isValidHTTPURL(e.URL) {
			return "", "", false
		}
		return fmt.Sprintf(`<a href="%s">`, html.EscapeString(strings.TrimSpace(e.URL))), "</a>", true
	default:
		return "", "", false
	}
}

type htmlEntitySpan struct {
	start    int
	end      int
	openTag  string
	closeTag string
}

func renderEntitiesHTML(units []uint16, cut int, appendedEllipsis bool, entities []tele.MessageEntity) string {
	var spans []htmlEntitySpan
	for _, e := range entities {
		if e.Length <= 0 || e.Offset < 0 || e.Offset >= cut {
			continue
		}
		openTag, closeTag, ok := entityTags(e)
		if !ok {
			continue
		}
		end := e.Offset + e.Length
		if end > cut {
			end = cut
		}
		if end <= e.Offset {
			continue
		}
		spans = append(spans, htmlEntitySpan{
			start:    e.Offset,
			end:      end,
			openTag:  openTag,
			closeTag: closeTag,
		})
	}

	sort.Slice(spans, func(i, j int) bool {
		if spans[i].start != spans[j].start {
			return spans[i].start < spans[j].start
		}
		return spans[i].end > spans[j].end
	})

	type openItem struct {
		end      int
		openTag  string
		closeTag string
	}

	var out strings.Builder
	var stack []openItem
	pos := 0
	spanIdx := 0

	closeAt := func(targetPos int) {
		for len(stack) > 0 && stack[len(stack)-1].end == targetPos {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			out.WriteString(top.closeTag)
		}
		// Handle rare non-nested overlaps: if an inner span ended earlier than top
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].end == targetPos {
				var reopen []openItem
				for j := len(stack) - 1; j >= i; j-- {
					out.WriteString(stack[j].closeTag)
					if j > i {
						reopen = append(reopen, stack[j])
					}
				}
				stack = stack[:i]
				for k := len(reopen) - 1; k >= 0; k-- {
					out.WriteString(reopen[k].openTag)
					stack = append(stack, reopen[k])
				}
				break
			}
		}
	}

	nextEventPos := func() int {
		next := cut
		if spanIdx < len(spans) && spans[spanIdx].start < next {
			next = spans[spanIdx].start
		}
		for _, item := range stack {
			if item.end < next {
				next = item.end
			}
		}
		return next
	}

	for pos < cut || spanIdx < len(spans) || len(stack) > 0 {
		closeAt(pos)
		for spanIdx < len(spans) && spans[spanIdx].start == pos {
			sp := spans[spanIdx]
			spanIdx++
			out.WriteString(sp.openTag)
			stack = append(stack, openItem{end: sp.end, openTag: sp.openTag, closeTag: sp.closeTag})
		}
		if pos >= cut && len(stack) == 0 && spanIdx >= len(spans) {
			break
		}
		next := nextEventPos()
		if next > pos {
			chunk := string(utf16.Decode(units[pos:next]))
			out.WriteString(html.EscapeString(chunk))
			pos = next
		} else if len(stack) == 0 && spanIdx >= len(spans) {
			break
		}
	}

	if appendedEllipsis {
		out.WriteRune('…')
	}
	return out.String()
}

func extractProposalLinkFromEntities(entities []tele.MessageEntity) string {
	for _, e := range entities {
		if e.Type == tele.EntityTextLink && isValidHTTPURL(e.URL) {
			return strings.TrimSpace(e.URL)
		}
	}
	return ""
}

func (b *Bot) renderBaseHTML(c tele.Context) (string, string) {
	msg := c.Message()
	if msg == nil {
		return "", "Unknown Job"
	}
	if p, ok := b.getProposal(msg.ID); ok {
		return formatProposalHTML(p), p.Title
	}
	lines := strings.Split(msg.Text, "\n")
	jobTitle := "Unknown Job"
	if len(lines) > 0 && strings.TrimSpace(lines[0]) != "" {
		jobTitle = strings.TrimPrefix(lines[0], "🚀 ")
	}
	clean := strings.TrimSuffix(msg.Text, rejectPromptText)
	units := utf16.Encode([]rune(clean))
	cut := len(units)
	appendedEllipsis := false
	if cut > maxBaseUTF16 {
		cut = maxBaseUTF16 - 1
		if cut > 0 && units[cut-1] >= 0xD800 && units[cut-1] <= 0xDBFF {
			cut--
		}
		appendedEllipsis = true
	}

	rendered := renderEntitiesHTML(units, cut, appendedEllipsis, msg.Entities)
	for utf8.RuneCountInString(rendered) > maxBaseRawRunes && cut > 0 {
		excess := utf8.RuneCountInString(rendered) - maxBaseRawRunes
		step := excess / 2
		if step < 1 {
			step = 1
		}
		if step > cut {
			cut = 0
		} else {
			cut -= step
			if cut > 0 && units[cut-1] >= 0xD800 && units[cut-1] <= 0xDBFF {
				cut--
			}
		}
		appendedEllipsis = true
		rendered = renderEntitiesHTML(units, cut, appendedEllipsis, msg.Entities)
	}
	return rendered, jobTitle
}

func (b *Bot) registerHandlers() {
	b.tg.Use(func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			sender := c.Sender()
			chat := c.Chat()
			if b.allowedUserID <= 0 || sender == nil || sender.ID != b.allowedUserID ||
				chat == nil || chat.ID != b.allowedUserID ||
				(chat.Type != "" && chat.Type != tele.ChatPrivate) {
				var senderID, chatID int64
				var username string
				if sender != nil {
					senderID = sender.ID
					username = sender.Username
				}
				if chat != nil {
					chatID = chat.ID
				}
				slog.Warn("blocked unauthorized telegram access",
					"sender_id", senderID,
					"username", username,
					"chat_id", chatID,
				)
				return nil
			}
			return next(c)
		}
	})

	// 1. Команда /start
	b.tg.Handle("/start", func(c tele.Context) error {
		msg := `🤖 <b>AI Job Hunter Bot онлайн!</b>

Я слухаю чергу RabbitMQ. Щойно 4 ШІ-агенти знайдуть і перевірять вакансію під твій стек — сюди прилетить готовий лист.

<i>Порада: просто натисни на текст листа, щоб скопіювати його в буфер обміну!</i>`
		return c.Send(msg, tele.ModeHTML)
	})

	menu := b.tg.NewMarkup()
	btnApprove := menu.Data("✅ Схвалити", "btn_approve")
	btnReject := menu.Data("❌ Відхилити", "btn_reject")

	btnReasonSalary := menu.Data("📉 Мало досвіду / Senior", "reason_exp")
	btnReasonStack := menu.Data("⚠️ Не той стек", "reason_stack")
	btnReasonDomain := menu.Data("🏢 Нецікавий проєкт", "reason_domain")
	btnCancel := menu.Data("🔙 Скасувати", "reason_cancel")

	guardAction := func(c tele.Context) (int, bool) {
		msg := c.Message()
		if msg == nil {
			return 0, false
		}
		switch b.beginAction(msg.ID) {
		case actionPending:
			_ = c.Respond(&tele.CallbackResponse{Text: "⏳ Запит уже обробляється..."})
			return 0, false
		case actionDone:
			_ = c.Respond(&tele.CallbackResponse{Text: "✅ Дію вже виконано"})
			return 0, false
		default:
			return msg.ID, true
		}
	}

	b.tg.Handle(&btnApprove, func(c tele.Context) error {
		msgID, ok := guardAction(c)
		if !ok {
			return nil
		}

		_ = c.Respond(&tele.CallbackResponse{Text: "✅ Схвалено! Відгук готовий до відправки."})

		baseHTML, _ := b.renderBaseHTML(c)
		updatedText := baseHTML + "\n\n────────────────\n<b>Статус:</b> ✅ <b>СХВАЛЕНО ТОБОЮ</b>"
		if err := c.Edit(updatedText, tele.ModeHTML); err != nil &&
			!errors.Is(err, tele.ErrMessageNotModified) &&
			!errors.Is(err, tele.ErrSameMessageContent) {
			b.endAction(msgID, actionIdle)
			return err
		}
		b.endAction(msgID, actionDone)
		return nil
	})

	b.tg.Handle(&btnReject, func(c tele.Context) error {
		msgID, ok := guardAction(c)
		if !ok {
			return nil
		}

		_ = c.Respond(&tele.CallbackResponse{Text: "Оберіть причину відхилення"})

		reasonMenu := b.tg.NewMarkup()
		reasonMenu.Inline(
			reasonMenu.Row(btnReasonSalary),
			reasonMenu.Row(btnReasonStack),
			reasonMenu.Row(btnReasonDomain),
			reasonMenu.Row(btnCancel),
		)

		baseHTML, _ := b.renderBaseHTML(c)
		err := c.Edit(baseHTML+"\n\n<i>Вкажіть причину відхилення для навчання ШІ:</i>", reasonMenu, tele.ModeHTML)
		b.endAction(msgID, actionIdle)
		return err
	})

	handleReasonChoice := func(c tele.Context, reasonText string) error {
		msgID, ok := guardAction(c)
		if !ok {
			return nil
		}

		baseHTML, jobTitle := b.renderBaseHTML(c)

		err := b.publisher.SendFeedback(jobTitle, reasonText)
		if err != nil {
			b.endAction(msgID, actionIdle)
			slog.Error("failed to publish feedback to rabbitmq", "error", err)
			_ = c.Respond(&tele.CallbackResponse{Text: "⚠️ Помилка збереження фідбеку, спробуйте ще раз"})
			return nil
		}

		// Mark done immediately after RabbitMQ publish succeeds so edit failure cannot trigger duplicate feedback.
		b.endAction(msgID, actionDone)

		slog.Info("feedback published to rabbitmq", "job", jobTitle, "reason", reasonText)
		_ = c.Respond(&tele.CallbackResponse{Text: "Фідбек зафіксовано!"})

		finalText := baseHTML + fmt.Sprintf("\n\n────────────────\n<b>Статус:</b> ❌ <b>ВІДХИЛЕНО</b> (Причина: <i>%s</i>)", html.EscapeString(reasonText))
		return c.Edit(finalText, tele.ModeHTML)
	}

	b.tg.Handle(&btnReasonSalary, func(c tele.Context) error {
		return handleReasonChoice(c, "Мало досвіду / Занадто сеньйористо")
	})

	b.tg.Handle(&btnReasonStack, func(c tele.Context) error {
		return handleReasonChoice(c, "Невідповідний стек технологій")
	})

	b.tg.Handle(&btnReasonDomain, func(c tele.Context) error {
		return handleReasonChoice(c, "Нецікавий проєкт або домен")
	})

	b.tg.Handle(&btnCancel, func(c tele.Context) error {
		msgID, ok := guardAction(c)
		if !ok {
			return nil
		}

		_ = c.Respond(&tele.CallbackResponse{Text: "Дію скасовано"})

		baseHTML, _ := b.renderBaseHTML(c)

		revertMenu := b.tg.NewMarkup()
		link := ""
		if msg := c.Message(); msg != nil {
			if p, found := b.getProposal(msg.ID); found && isValidHTTPURL(p.Link) {
				link = strings.TrimSpace(p.Link)
			} else {
				link = extractProposalLinkFromEntities(msg.Entities)
			}
		}
		if link != "" {
			revertMenu.Inline(
				revertMenu.Row(btnApprove, btnReject),
				revertMenu.Row(revertMenu.URL("↗️ Відкрити Djinni", link)),
			)
		} else {
			revertMenu.Inline(
				revertMenu.Row(btnApprove, btnReject),
			)
		}

		err := c.Edit(baseHTML, revertMenu, tele.ModeHTML)
		b.endAction(msgID, actionIdle)
		return err
	})
}

func (b *Bot) SendVacancy(userID int64, p broker.Proposal) error {
	text := formatProposalHTML(p)

	menu := b.tg.NewMarkup()
	btnApprove := menu.Data("✅ Схвалити", "btn_approve")
	btnReject := menu.Data("❌ Відхилити", "btn_reject")

	if isValidHTTPURL(p.Link) {
		btnDjinni := menu.URL("↗️ Відкрити Djinni", strings.TrimSpace(p.Link))
		menu.Inline(
			menu.Row(btnApprove, btnReject),
			menu.Row(btnDjinni),
		)
	} else {
		menu.Inline(
			menu.Row(btnApprove, btnReject),
		)
	}

	targetChat := tele.ChatID(userID)
	sent, err := b.tg.Send(targetChat, text, menu, tele.ModeHTML)
	if err != nil {
		return err
	}
	if sent != nil {
		b.saveProposal(sent.ID, p)
	}
	return nil
}
