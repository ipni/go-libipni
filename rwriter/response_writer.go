package rwriter

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/apierror"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

type ResponseWriter struct {
	w        http.ResponseWriter
	f        http.Flusher
	cid      cid.Cid
	encoder  *json.Encoder
	mh       multihash.Multihash
	mhCode   uint64
	nd       bool
	pathType string
	status   int
}

func New(w http.ResponseWriter, r *http.Request, options ...Option) (*ResponseWriter, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	accepts := r.Header.Values("Accept")
	var nd, okJson bool
	for _, accept := range accepts {
		amts := strings.Split(accept, ",")
		for _, amt := range amts {
			mt, _, err := mime.ParseMediaType(amt)
			if err != nil {
				return nil, apierror.New(errors.New("invalid Accept header"), http.StatusBadRequest)
			}
			switch mt {
			case mediaTypeNDJson:
				nd = true
			case mediaTypeJson:
				okJson = true
			case mediaTypeAny:
				nd = !opts.preferJson
				okJson = true
			}
			if nd && okJson {
				break
			}
		}
	}

	if len(accepts) == 0 {
		if !opts.preferJson {
			// If there is no `Accept` header and JSON is preferred then be
			// forgiving and fall back onto JSON media type. Otherwise,
			// strictly require `Accept` header.
			return nil, apierror.New(errors.New("accept header must be specified"), http.StatusBadRequest)
		}
	} else if !okJson && !nd {
		return nil, apierror.New(fmt.Errorf("media type not supported: %s", accepts), http.StatusBadRequest)
	}

	var b []byte
	var cidKey cid.Cid
	var mh multihash.Multihash

	pathType := path.Base(path.Dir(r.URL.Path))
	if pathType == "" {
		return nil, apierror.New(errors.New("missing resource type"), http.StatusBadRequest)
	}
	switch pathType {
	case opts.mhPathType:
		mhStr := strings.TrimSpace(path.Base(r.URL.Path))
		b, err = base58.Decode(mhStr)
		if err != nil {
			b, err = hex.DecodeString(mhStr)
			if err != nil {
				return nil, apierror.New(multihash.ErrInvalidMultihash, http.StatusBadRequest)
			}
		}
		mh = multihash.Multihash(b)
		cidKey = cid.NewCidV1(cid.Raw, mh)
	case opts.cidPathType:
		cidKey, err = cid.Decode(strings.TrimSpace(path.Base(r.URL.Path)))
		if err != nil {
			return nil, apierror.New(err, http.StatusBadRequest)
		}
		b = cidKey.Hash()
		mh = multihash.Multihash(b)
	default:
		return nil, apierror.New(errors.New("unsupported resource type"), http.StatusBadRequest)
	}

	dm, err := multihash.Decode(b)
	if err != nil {
		return nil, apierror.New(err, http.StatusBadRequest)
	}

	flusher, _ := w.(http.Flusher)
	if nd {
		w.Header().Set("Content-Type", mediaTypeNDJson)
		w.Header().Set("Connection", "Keep-Alive")
		w.Header().Set("X-Content-Type-Options", "nosniff")
	} else {
		w.Header().Set("Content-Type", mediaTypeJson)
	}

	return &ResponseWriter{
		w:        w,
		f:        flusher,
		cid:      cidKey,
		encoder:  json.NewEncoder(w),
		mh:       mh,
		mhCode:   dm.Code,
		nd:       nd,
		pathType: pathType,
		status:   http.StatusOK,
	}, nil
}

func (w *ResponseWriter) Multihash() multihash.Multihash {
	return w.mh
}

func (w *ResponseWriter) MultihashCode() uint64 {
	return w.mhCode
}

func (w *ResponseWriter) IsND() bool {
	return w.nd
}

func (w *ResponseWriter) PathType() string {
	return w.pathType
}

func (w *ResponseWriter) Flush() {
	if w.f != nil {
		w.f.Flush()
	}
}

func (w *ResponseWriter) Cid() cid.Cid {
	return w.cid
}

func (w *ResponseWriter) Encoder() *json.Encoder {
	return w.encoder
}

func (w *ResponseWriter) Header() http.Header {
	return w.w.Header()
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	if statusCode != http.StatusOK {
		w.status = statusCode
		w.w.WriteHeader(statusCode)
	}
}

func (w *ResponseWriter) StatusCode() int {
	return w.status
}

func MatchQueryParam(r *http.Request, key, value string) (bool, bool) {
	labels, present := r.URL.Query()[key]
	if !present {
		return false, false
	}
	for _, label := range labels {
		if label == value {
			return true, true
		}
	}
	return true, false
}
