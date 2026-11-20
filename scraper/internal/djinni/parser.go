package djinni

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
)

const (
	defaultUserAgent = "work-agent-bot-scraper/1.0"
	maxFeedBodyBytes = 10 << 20 // 10 MiB
	fetchTimeout     = 15 * time.Second
)

// Job represents a clean, parsed vacancy from Djinni.
type Job struct {
	Title       string `json:"Title"`
	Link        string `json:"Link"`
	Description string `json:"Description"`
	PubDate     string `json:"PubDate"`
	GUID        string `json:"GUID"`
}

// FetchJobs downloads and parses the RSS feed into a slice of Jobs.
func FetchJobs(ctx context.Context, rssURL string) ([]Job, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	fp := gofeed.NewParser()
	fp.UserAgent = defaultUserAgent
	fp.MaxByteSize = maxFeedBodyBytes
	fp.Client = &http.Client{Timeout: fetchTimeout}

	feed, err := fp.ParseURLWithContext(rssURL, fetchCtx)
	if err != nil {
		return nil, fmt.Errorf("fetch rss feed: %w", err)
	}

	jobs := make([]Job, 0, len(feed.Items))
	for _, item := range feed.Items {
		guid := strings.TrimSpace(item.GUID)
		link := strings.TrimSpace(item.Link)
		if guid == "" {
			guid = link
		}
		if guid == "" {
			slog.Warn("skipping RSS item with empty GUID and Link", "title", item.Title)
			continue
		}

		jobs = append(jobs, Job{
			Title:       strings.TrimSpace(item.Title),
			Link:        link,
			Description: item.Description,
			PubDate:     item.Published,
			GUID:        guid,
		})
	}

	slog.Info("successfully fetched vacancies from djinni", "count", len(jobs))
	return jobs, nil
}
