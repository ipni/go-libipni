package ipnisync

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// maxBodySnippet is the maximum number of response body bytes included in a
// FetchError.
const maxBodySnippet = 256

// FetchError is returned when an HTTP fetch fails, either because the
// retryable client exhausted its retries or because the server responded
// with a non-success status. It carries the final response status, the
// number of attempts made, and a sanitized snippet of the response body so
// callers can inspect why the fetch failed.
type FetchError struct {
	URL        string
	StatusCode int
	Attempts   int
	RetryAfter string
	Body       string
	Err        error
}

// Error renders the fetch failure. When Attempts is non-zero the string is
// prefixed with the retryable client's "giving up" text, so pre-existing
// error strings remain prefixes of the rendered string.
func (e *FetchError) Error() string {
	var sb strings.Builder
	if e.Attempts > 0 {
		// GET is the only method ipnisync issues and URL is stored in its
		// redacted form, matching the retryable client's error format.
		fmt.Fprintf(&sb, "GET %s giving up after %d attempt(s)", e.URL, e.Attempts)
		if e.Err != nil {
			fmt.Fprintf(&sb, ": %s", e.Err)
		}
		if e.StatusCode != 0 {
			fmt.Fprintf(&sb, ": non success http fetch response at %s: %d", e.URL, e.StatusCode)
		}
	} else {
		fmt.Fprintf(&sb, "non success http fetch response at %s: %d", e.URL, e.StatusCode)
	}
	if e.RetryAfter != "" {
		fmt.Fprintf(&sb, " (retry-after: %s)", e.RetryAfter)
	}
	if e.Body != "" {
		fmt.Fprintf(&sb, " body: %q", e.Body)
	}
	return sb.String()
}

// Unwrap returns the underlying transport error, if any.
func (e *FetchError) Unwrap() error {
	return e.Err
}

// redactURL mirrors the URL redaction used by go-retryablehttp error strings
// so FetchError.Error() renders the same URL form.
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	ru := *u
	if _, has := ru.User.Password(); has {
		ru.User = url.UserPassword(ru.User.Username(), "xxxxx")
	}
	return ru.String()
}

// sanitizeSnippet reduces a response body to a short single-line snippet for
// inclusion in an error message. It caps the input at max bytes, drops
// non-printable runes, collapses whitespace runs, and appends "..." when the
// input was truncated.
func sanitizeSnippet(b []byte, max int) string {
	truncated := len(b) > max
	if truncated {
		b = b[:max]
	}
	var sb strings.Builder
	prevSpace := false
	for _, r := range string(b) {
		switch {
		case unicode.IsSpace(r):
			if !prevSpace && sb.Len() > 0 {
				sb.WriteByte(' ')
				prevSpace = true
			}
		case !unicode.IsPrint(r):
			// drop non-printable runes
		default:
			sb.WriteRune(r)
			prevSpace = false
		}
	}
	s := strings.TrimRight(sb.String(), " ")
	if truncated {
		s += "..."
	}
	return s
}
