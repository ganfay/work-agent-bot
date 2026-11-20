package djinni

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mmcdole/gofeed"
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

func TestFetchJobs_SetsUserAgentHeader(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>Djinni</title></channel></rss>`))
	}))
	defer srv.Close()

	_, err := FetchJobs(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("FetchJobs failed: %v", err)
	}
	if gotUA != "work-agent-bot-scraper/1.0" {
		t.Fatalf("expected User-Agent %q, got %q", "work-agent-bot-scraper/1.0", gotUA)
	}
}

func TestFetchJobs_HTTPErrorStatus(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			_, err := FetchJobs(context.Background(), srv.URL)
			if err == nil {
				t.Fatalf("expected error for HTTP %d", status)
			}
			var httpErr gofeed.HTTPError
			if !errors.As(err, &httpErr) {
				t.Fatalf("expected gofeed.HTTPError, got %T (%v)", err, err)
			}
			if httpErr.StatusCode != status {
				t.Fatalf("expected status %d, got %d", status, httpErr.StatusCode)
			}
		})
	}
}

func TestFetchJobs_MalformedXMLReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte("<rss><channel><item><title>unclosed"))
	}))
	defer srv.Close()

	_, err := FetchJobs(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error for malformed XML")
	}
}

func TestFetchJobs_ContextCanceledAndTimeout(t *testing.T) {
	t.Run("pre-canceled context", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>`))
		}))
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := FetchJobs(ctx, srv.URL)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("context deadline exceeded", func(t *testing.T) {
		unblock := make(chan struct{})
		defer close(unblock)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-unblock:
			}
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()

		_, err := FetchJobs(ctx, srv.URL)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

func TestFetchJobs_RejectsOversizedFeedBody(t *testing.T) {
	t.Run("unclosed oversized body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/rss+xml")
			_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><item><title>Big</title><description>`))
			chunk := []byte(strings.Repeat("A", 64*1024))
			for total := 0; total < (11 << 20); total += len(chunk) {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}))
		defer srv.Close()

		jobs, err := FetchJobs(context.Background(), srv.URL)
		if !errors.Is(err, gofeed.ErrResponseTooLarge) {
			t.Fatalf("expected gofeed.ErrResponseTooLarge, got %v", err)
		}
		if jobs != nil {
			t.Fatalf("expected nil jobs on oversized response, got %d", len(jobs))
		}
	})

	t.Run("valid RSS followed by oversized trailing padding", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/rss+xml")
			_, _ = w.Write([]byte(`<?xml version="1.0"?><rss version="2.0"><channel><item><title>Ok</title><link>https://djinni.co/jobs/1</link><guid>g1</guid></item></channel></rss>`))
			chunk := []byte(strings.Repeat(" ", 64*1024))
			for total := 0; total < (11 << 20); total += len(chunk) {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}))
		defer srv.Close()

		jobs, err := FetchJobs(context.Background(), srv.URL)
		if !errors.Is(err, gofeed.ErrResponseTooLarge) {
			t.Fatalf("expected gofeed.ErrResponseTooLarge even with trailing padding, got %v", err)
		}
		if jobs != nil {
			t.Fatalf("expected nil jobs on oversized response, got %d", len(jobs))
		}
	})
}

func TestFetchJobs_RedactsURLCredentialsAndPreservesContextErrors(t *testing.T) {
	t.Run("network error redacts userinfo and query values", func(t *testing.T) {
		rssURL := "https://rss_user:SECRET_PASS@127.0.0.1:1/rss?token=QUERY_SECRET"
		_, err := FetchJobs(context.Background(), rssURL)
		if err == nil {
			t.Fatal("expected error for unreachable server, got nil")
		}
		for _, secret := range []string{"rss_user", "SECRET_PASS", "QUERY_SECRET"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("expected error not to contain %q, got %q", secret, err.Error())
			}
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			for _, secret := range []string{"rss_user", "SECRET_PASS", "QUERY_SECRET"} {
				if strings.Contains(urlErr.URL, secret) {
					t.Fatalf("expected urlErr.URL not to contain %q, got %q", secret, urlErr.URL)
				}
			}
		}
	})

	t.Run("canceled context redacts URL secrets and preserves context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rssURL := "https://rss_user:SECRET_PASS@127.0.0.1:1/rss?token=QUERY_SECRET"
		_, err := FetchJobs(ctx, rssURL)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		for _, secret := range []string{"rss_user", "SECRET_PASS", "QUERY_SECRET"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("expected error not to contain %q, got %q", secret, err.Error())
			}
		}
	})

	t.Run("deadline exceeded redacts URL secrets and preserves context.DeadlineExceeded", func(t *testing.T) {
		unblock := make(chan struct{})
		defer close(unblock)

		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-unblock:
			}
		}))
		defer srv.Close()

		parsed, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("parse server URL: %v", err)
		}
		parsed.User = url.UserPassword("rss_user", "SECRET_PASS")
		parsed.Path = "/rss"
		parsed.RawQuery = "token=QUERY_SECRET"

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()

		_, err = FetchJobs(ctx, parsed.String())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		for _, secret := range []string{"rss_user", "SECRET_PASS", "QUERY_SECRET"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("expected error not to contain %q, got %q", secret, err.Error())
			}
		}
	})
}
