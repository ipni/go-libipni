package ipnisync

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
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
	Method     string
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
	method := e.Method
	if method == "" {
		method = "GET"
	}
	var sb strings.Builder
	if e.Attempts > 0 {
		// URL is stored in its redacted form, matching the retryable
		// client's error format.
		fmt.Fprintf(&sb, "%s %s giving up after %d attempt(s)", method, e.URL, e.Attempts)
		if e.Err != nil {
			fmt.Fprintf(&sb, ": %s", e.Err)
		}
		if e.StatusCode != 0 {
			fmt.Fprintf(&sb, ": non success http fetch response at %s: %d", e.URL, e.StatusCode)
		}
	} else {
		fmt.Fprintf(&sb, "non success http fetch response at %s: %d", e.URL, e.StatusCode)
		if e.Err != nil {
			fmt.Fprintf(&sb, ": %s", e.Err)
		}
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

// maxHexSnippet is the maximum number of raw response body bytes hex-encoded
// in a FetchError when the body is not meaningfully printable.
const maxHexSnippet = 32

// sanitizeSnippet reduces a response body to a short single-line snippet for
// inclusion in an error message. It caps the input at max bytes, drops
// non-printable runes, collapses whitespace runs, and appends "..." when the
// input was truncated. Invalid UTF-8 bytes are treated as non-printable
// rather than becoming replacement characters. When the printable result is
// empty or more than half the decoded bytes were dropped, the snippet is the
// hex encoding of up to the first maxHexSnippet raw bytes (with "..." when
// more bytes remain) instead of a garbled text rendering.
func sanitizeSnippet(b []byte, max int) string {
	if len(b) == 0 {
		return ""
	}
	truncated := len(b) > max
	if truncated {
		b = b[:max]
	}
	raw := b
	var sb strings.Builder
	prevSpace := false
	total, dropped := 0, 0
	for i := 0; i < len(raw); {
		r, size := utf8.DecodeRune(raw[i:])
		total++
		switch {
		case r == utf8.RuneError && size == 1:
			// invalid UTF-8 byte; count it as non-printable
			dropped++
		case unicode.IsSpace(r):
			if !prevSpace && sb.Len() > 0 {
				sb.WriteByte(' ')
				prevSpace = true
			}
		case !unicode.IsPrint(r):
			// drop non-printable runes
			dropped++
		default:
			sb.WriteRune(r)
			prevSpace = false
		}
		i += size
	}
	s := strings.TrimRight(sb.String(), " ")
	if s == "" || dropped*2 > total {
		n := maxHexSnippet
		if len(raw) < n {
			n = len(raw)
		}
		hexed := "hex:" + hex.EncodeToString(raw[:n])
		if len(raw) > n {
			hexed += "..."
		}
		return hexed
	}
	if dropped > 0 {
		s += fmt.Sprintf(" [%d non-printable bytes]", dropped)
	}
	if truncated {
		s += "..."
	}
	return s
}
