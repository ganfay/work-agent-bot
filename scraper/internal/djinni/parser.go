package djinni

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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

func redactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<redacted>"
	}
	if u.User != nil {
		u.User = url.UserPassword("xxxxx", "xxxxx")
	}
	if u.RawQuery != "" {
		if q, qErr := url.ParseQuery(u.RawQuery); qErr == nil {
			for k := range q {
				q[k] = []string{"xxxxx"}
			}
			u.RawQuery = q.Encode()
		} else {
			u.RawQuery = "redacted"
		}
	}
	return u.String()
}

func sanitizeFetchError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		innerErr := sanitizeFetchError(urlErr.Err)
		sanitizedURL := redactURL(urlErr.URL)
		if direct, ok := err.(*url.Error); ok && direct == urlErr {
			return &url.Error{
				Op:  urlErr.Op,
				URL: sanitizedURL,
				Err: innerErr,
			}
		}
		return &sanitizedError{
			msg:   fmt.Sprintf("%s %q: %v", urlErr.Op, sanitizedURL, innerErr),
			cause: innerErr,
		}
	}
	return err
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
		return nil, fmt.Errorf("fetch rss feed: %w", sanitizeFetchError(err))
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
