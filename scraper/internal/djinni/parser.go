package djinni

import (
	"log/slog"

	"github.com/mmcdole/gofeed"
)

// Job represents a clean, parsed vacancy from Djinni
type Job struct {
	Title       string
	Link        string
	Description string
	PubDate     string
	GUID        string
}

// FetchJobs downloads and parses the RSS feed into a slice of Jobs
func FetchJobs(rssURL string) ([]Job, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(rssURL)
	if err != nil {
		return nil, err
	}

	var jobs []Job
	for _, item := range feed.Items {
		jobs = append(jobs, Job{
			Title:       item.Title,
			Link:        item.Link,
			Description: item.Description,
			PubDate:     item.Published,
			GUID:        item.GUID,
		})
	}

	slog.Info("successfully fetched vacancies from djinni", "count", len(jobs))
	return jobs, nil
}
