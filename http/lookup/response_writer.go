package lookup

import (
	"io"
	"net/http"

	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

type LookupResponseWriter interface {
	io.Closer
	selectiveResponseWriter
	Key() multihash.Multihash
	WriteProviderResult(model.ProviderResult) error
}
type selectiveResponseWriter interface {
	http.ResponseWriter
	Accept(r *http.Request) error
}
