package djinni

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchJobs_HandlesMissingGUIDFallbackAndSkipsEmptyItems(t *testing.T) {
	rssXML := `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Djinni</title>
    <item>
      <title>  Go Backend Dev  </title>
      <link>https://djinni.co/jobs/1</link>
      <guid>guid-1</guid>
      <description>Go + Postgres</description>
    </item>
    <item>
      <title>No GUID Job</title>
      <link>https://djinni.co/jobs/2</link>
      <description>Fallback to link</description>
    </item>
    <item>
      <title>Malformed Empty Item</title>
      <description>No guid and no link</description>
    </item>
  </channel>
</rss>`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(rssXML))
	}))
	defer srv.Close()

	jobs, err := FetchJobs(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("FetchJobs failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 valid jobs, got %d: %+v", len(jobs), jobs)
	}
	if jobs[0].GUID != "guid-1" || jobs[0].Title != "Go Backend Dev" {
		t.Fatalf("unexpected first job: %+v", jobs[0])
	}
	if jobs[1].GUID != "https://djinni.co/jobs/2" {
		t.Fatalf("expected GUID fallback to Link, got %q", jobs[1].GUID)
	}
}
