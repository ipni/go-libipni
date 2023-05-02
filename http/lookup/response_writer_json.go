package lookup

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/ipni/go-libipni/apierror"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

var (
	_ selectiveResponseWriter = (*jsonResponseWriter)(nil)
	_ http.ResponseWriter     = (*jsonResponseWriter)(nil)
)

type jsonResponseWriter struct {
	w          http.ResponseWriter
	f          http.Flusher
	encoder    *json.Encoder
	nd         bool
	preferJson bool
}

func newJsonResponseWriter(w http.ResponseWriter, preferJson bool) jsonResponseWriter {
	return jsonResponseWriter{
		w:          w,
		encoder:    json.NewEncoder(w),
		preferJson: preferJson,
	}
}

func (i *jsonResponseWriter) Accept(r *http.Request) error {
	accepts := r.Header.Values("Accept")
	var okJson bool
	for _, accept := range accepts {
		amts := strings.Split(accept, ",")
		for _, amt := range amts {
			mt, _, err := mime.ParseMediaType(amt)
			if err != nil {
				return apierror.New(errors.New("invalid Accept header"), http.StatusBadRequest)
			}
			switch mt {
			case mediaTypeNDJson:
				i.nd = true
			case mediaTypeJson:
				okJson = true
			case mediaTypeAny:
				i.nd = !i.preferJson
				okJson = true
			}
			if i.nd && okJson {
				break
			}
		}
	}

	switch {
	case len(accepts) == 0:
		// If there is no `Accept` header and JSON is preferred then be forgiving and fall back
		// onto JSON media type. Otherwise, strictly require `Accept` header.
		if !i.preferJson {
			return apierror.New(errors.New("Accept header must be specified"), http.StatusBadRequest)
		}
	case !okJson && !i.nd:
		return apierror.New(fmt.Errorf("media type not supported: %s", accepts), http.StatusBadRequest)
	}

	i.f, _ = i.w.(http.Flusher)

	if i.nd {
		i.w.Header().Set("Content-Type", mediaTypeNDJson)
		i.w.Header().Set("Connection", "Keep-Alive")
		i.w.Header().Set("X-Content-Type-Options", "nosniff")
	} else {
		i.w.Header().Set("Content-Type", mediaTypeJson)
	}
	return nil
}

func (i *jsonResponseWriter) Header() http.Header {
	return i.w.Header()
}

func (i *jsonResponseWriter) Write(b []byte) (int, error) {
	return i.w.Write(b)
}

func (i *jsonResponseWriter) WriteHeader(code int) {
	i.w.WriteHeader(code)
}
