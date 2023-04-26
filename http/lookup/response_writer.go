package lookup

import (
	"io"
	"net/http"

	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

type (
	selectiveResponseWriter interface {
		http.ResponseWriter
		Accept(r *http.Request) error
	}
	lookupResponseWriter interface {
		io.Closer
		selectiveResponseWriter
		Key() multihash.Multihash
		WriteProviderResult(model.ProviderResult) error
	}
	errHttpResponse struct {
		message string
		status  int
	}
)

func (e errHttpResponse) Error() string {
	return e.message
}

func (e errHttpResponse) WriteTo(w http.ResponseWriter) {
	http.Error(w, e.message, e.status)
}
