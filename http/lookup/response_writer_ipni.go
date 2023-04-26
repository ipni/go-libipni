package lookup

import (
	"net/http"
	"path"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var (
	_ lookupResponseWriter = (*ipniLookupResponseWriter)(nil)

	newline = []byte("\n")
)

type ipniLookupResponseWriter struct {
	jsonResponseWriter
	result model.MultihashResult
	count  int
	// isMultihash is true if the request is for a multihash, false if it is for a CID
	isMultihash bool
}

func NewIPNILookupResponseWriter(w http.ResponseWriter, preferJson bool, isMultihash bool) lookupResponseWriter {
	return &ipniLookupResponseWriter{
		jsonResponseWriter: newJsonResponseWriter(w, preferJson),
		isMultihash:        isMultihash,
	}
}

func (i *ipniLookupResponseWriter) Accept(r *http.Request) error {
	if err := i.jsonResponseWriter.Accept(r); err != nil {
		return err
	}
	var mh multihash.Multihash
	var err error
	if i.isMultihash {
		smh := strings.TrimPrefix(path.Base(r.URL.Path), "multihash/")
		mh, err = multihash.FromB58String(smh)
	} else {
		scid := strings.TrimPrefix(path.Base(r.URL.Path), "cid/")
		var c cid.Cid
		c, err = cid.Decode(scid)
		if err == nil {
			mh = multihash.Multihash(c.Hash())
		}
	}
	if err != nil {
		return errHttpResponse{message: err.Error(), status: http.StatusBadRequest}
	}
	i.result.Multihash = mh
	return nil
}

func (i *ipniLookupResponseWriter) Key() multihash.Multihash {
	return i.result.Multihash
}

func (i *ipniLookupResponseWriter) WriteProviderResult(pr model.ProviderResult) error {
	if i.nd {
		if err := i.encoder.Encode(pr); err != nil {
			return err
		}
		if _, err := i.w.Write(newline); err != nil {
			return err
		}
		if i.f != nil {
			i.f.Flush()
		}
	} else {
		i.result.ProviderResults = append(i.result.ProviderResults, pr)
	}
	i.count++
	return nil
}

func (i *ipniLookupResponseWriter) Close() error {
	if i.count == 0 {
		return errHttpResponse{status: http.StatusNotFound}
	}
	if i.nd {
		return nil
	}
	return i.encoder.Encode(i.result)
}
