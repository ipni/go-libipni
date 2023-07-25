package rwriter

import (
	"net/http"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
)

type ProviderResponseWriter struct {
	ResponseWriter
	count  int
	result model.MultihashResult
}

func NewProviderResponseWriter(w *ResponseWriter) *ProviderResponseWriter {
	return &ProviderResponseWriter{
		ResponseWriter: *w,
		result: model.MultihashResult{
			Multihash: w.Multihash(),
		},
	}
}

func (pw *ProviderResponseWriter) WriteProviderResult(pr model.ProviderResult) error {
	if pw.nd {
		err := pw.encoder.Encode(pr)
		if err != nil {
			return err
		}
		pw.Flush()
	} else {
		pw.result.ProviderResults = append(pw.result.ProviderResults, pr)
	}
	pw.count++
	return nil
}

func (pw *ProviderResponseWriter) Close() error {
	if pw.count == 0 {
		return apierror.New(nil, http.StatusNotFound)
	}
	if pw.nd {
		return nil
	}
	return pw.encoder.Encode(model.FindResponse{
		MultihashResults: []model.MultihashResult{pw.result},
	})
}
