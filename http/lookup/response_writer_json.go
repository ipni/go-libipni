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

func (irw *jsonResponseWriter) Accept(r *http.Request) error {
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
				irw.nd = true
			case mediaTypeJson:
				okJson = true
			case mediaTypeAny:
				irw.nd = !irw.preferJson
				okJson = true
			}
			if irw.nd && okJson {
				break
			}
		}
	}

	switch {
	case len(accepts) == 0:
		// If there is no `Accept` header and JSON is preferred then be forgiving and fall back
		// onto JSON media type. Otherwise, strictly require `Accept` header.
		if !irw.preferJson {
			return apierror.New(errors.New("Accept header must be specified"), http.StatusBadRequest)
		}
	case !okJson && !irw.nd:
		return apierror.New(fmt.Errorf("media type not supported: %s", accepts), http.StatusBadRequest)
	}

	irw.f, _ = irw.w.(http.Flusher)

	if irw.nd {
		irw.w.Header().Set("Content-Type", mediaTypeNDJson)
		irw.w.Header().Set("Connection", "Keep-Alive")
		irw.w.Header().Set("X-Content-Type-Options", "nosniff")
	} else {
		irw.w.Header().Set("Content-Type", mediaTypeJson)
	}
	return nil
}

func (irw *jsonResponseWriter) Header() http.Header {
	return irw.w.Header()
}

func (irw *jsonResponseWriter) Write(b []byte) (int, error) {
	return irw.w.Write(b)
}

func (irw *jsonResponseWriter) WriteHeader(code int) {
	irw.w.WriteHeader(code)
}
