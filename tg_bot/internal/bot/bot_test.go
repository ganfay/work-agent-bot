package bot

import (
	"strings"
	"testing"
	"unicode/utf8"

	"tg_bot/internal/broker"
)

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
