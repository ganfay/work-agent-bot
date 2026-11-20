package bot

import (
	"fmt"
	"html"
	"log/slog"
	"strings"
	"sync"
	"time"

	"tg_bot/internal/broker"

	tele "gopkg.in/telebot.v4"
)

type Bot struct {
	tg            *tele.Bot
	rabbit        *broker.RabbitMQ
	allowedUserID int64

	mu        sync.RWMutex
	proposals map[int]broker.Proposal
}

func New(token string, allowedUserID int64, rabbit *broker.RabbitMQ) (*Bot, error) {
	pref := tele.Settings{
		Token:  token,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}

	b, err := tele.NewBot(pref)
	if err != nil {
		return nil, fmt.Errorf("failed to create bot: %w", err)
	}

	instance := &Bot{
		tg:            b,
		rabbit:        rabbit,
		allowedUserID: allowedUserID,
		proposals:     make(map[int]broker.Proposal),
	}

	instance.registerHandlers()
	return instance, nil
}

func (b *Bot) Start() {
	slog.Info("telegram bot started listening for events")
	b.tg.Start()
}

func (b *Bot) Stop() {
	b.tg.Stop()
}

func formatProposalHTML(p broker.Proposal) string {
	return fmt.Sprintf(`🚀 <b>%s</b>
🎯 <b>Match Score:</b> %d/100
📊 <b>Fluff Index:</b> %d%%
🔗 <a href="%s">Відкрити на Djinni</a>

💡 <b>Аналіз:</b> %s

📝 <b>Готовий супровідний лист (тапни щоб скопіювати):</b>
<code>%s</code>`,
		html.EscapeString(p.Title),
		p.MatchScore,
		p.FluffPercentage,
		html.EscapeString(p.Link),
		html.EscapeString(p.Summary),
		html.EscapeString(p.FinalLetter),
	)
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
		for k := range b.proposals {
			delete(b.proposals, k)
			break
		}
	}
	b.proposals[msgID] = p
}

func (b *Bot) renderBaseHTML(c tele.Context) (string, string) {
	if p, ok := b.getProposal(c.Message().ID); ok {
		return formatProposalHTML(p), p.Title
	}
	lines := strings.Split(c.Message().Text, "\n")
	jobTitle := "Unknown Job"
	if len(lines) > 0 {
		jobTitle = strings.TrimPrefix(lines[0], "🚀 ")
	}
	clean := strings.TrimSuffix(c.Message().Text, "\n\nВкажіть причину відхилення для навчання ШІ:")
	return html.EscapeString(clean), jobTitle
}

func (b *Bot) registerHandlers() {
	b.tg.Use(func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			if b.allowedUserID != 0 && (c.Sender() == nil || c.Sender().ID != b.allowedUserID) {
				slog.Warn("blocked unauthorized telegram access", "sender", c.Sender())
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

	b.tg.Handle(&btnApprove, func(c tele.Context) error {
		_ = c.Respond(&tele.CallbackResponse{Text: "✅ Схвалено! Відгук готовий до відправки."})

		baseHTML, _ := b.renderBaseHTML(c)
		updatedText := baseHTML + "\n\n────────────────\n<b>Статус:</b> ✅ <b>СХВАЛЕНО ТОБОЮ</b>"
		return c.Edit(updatedText, tele.ModeHTML)
	})

	b.tg.Handle(&btnReject, func(c tele.Context) error {
		_ = c.Respond(&tele.CallbackResponse{Text: "Оберіть причину відхилення"})

		reasonMenu := b.tg.NewMarkup()
		reasonMenu.Inline(
			reasonMenu.Row(btnReasonSalary),
			reasonMenu.Row(btnReasonStack),
			reasonMenu.Row(btnReasonDomain),
			reasonMenu.Row(btnCancel),
		)

		baseHTML, _ := b.renderBaseHTML(c)
		return c.Edit(baseHTML+"\n\n<i>Вкажіть причину відхилення для навчання ШІ:</i>", reasonMenu, tele.ModeHTML)
	})

	handleReasonChoice := func(c tele.Context, reasonText string) error {
		_ = c.Respond(&tele.CallbackResponse{Text: "Фідбек зафіксовано!"})

		baseHTML, jobTitle := b.renderBaseHTML(c)

		err := b.rabbit.SendFeedback(jobTitle, reasonText)
		if err != nil {
			slog.Error("failed to publish feedback to rabbitmq", "error", err)
		} else {
			slog.Info("feedback published to rabbitmq", "job", jobTitle, "reason", reasonText)
		}

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
		_ = c.Respond(&tele.CallbackResponse{Text: "Дію скасовано"})

		baseHTML, _ := b.renderBaseHTML(c)

		revertMenu := b.tg.NewMarkup()
		if p, ok := b.getProposal(c.Message().ID); ok && p.Link != "" {
			revertMenu.Inline(
				revertMenu.Row(btnApprove, btnReject),
				revertMenu.Row(revertMenu.URL("↗️ Відкрити Djinni", p.Link)),
			)
		} else {
			revertMenu.Inline(
				revertMenu.Row(btnApprove, btnReject),
			)
		}
		return c.Edit(baseHTML, revertMenu, tele.ModeHTML)
	})
}

func (b *Bot) SendVacancy(userID int64, p broker.Proposal) error {
	text := formatProposalHTML(p)

	menu := b.tg.NewMarkup()
	btnApprove := menu.Data("✅ Схвалити", "btn_approve")
	btnReject := menu.Data("❌ Відхилити", "btn_reject")
	btnDjinni := menu.URL("↗️ Відкрити Djinni", p.Link)

	menu.Inline(
		menu.Row(btnApprove, btnReject),
		menu.Row(btnDjinni),
	)

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
