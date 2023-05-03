package lookup

import (
	"net/http"
	"path"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var (
	_ LookupResponseWriter = (*ipniLookupResponseWriter)(nil)

	newline = []byte("\n")
)

type ipniLookupResponseWriter struct {
	jsonResponseWriter
	result model.MultihashResult
	count  int
	// isMultihash is true if the request is for a multihash, false if it is for a CID
	isMultihash bool
}

func NewIPNILookupResponseWriter(w http.ResponseWriter, preferJson bool, isMultihash bool) *ipniLookupResponseWriter {
	return &ipniLookupResponseWriter{
		jsonResponseWriter: newJsonResponseWriter(w, preferJson),
		isMultihash:        isMultihash,
	}
}

func (ilpw *ipniLookupResponseWriter) Accept(r *http.Request) error {
	if err := ilpw.jsonResponseWriter.Accept(r); err != nil {
		return err
	}
	var mh multihash.Multihash
	var err error
	if ilpw.isMultihash {
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
		return apierror.New(err, http.StatusBadRequest)
	}
	ilpw.result.Multihash = mh
	return nil
}

func (ilpw *ipniLookupResponseWriter) Key() multihash.Multihash {
	return ilpw.result.Multihash
}

func (ilpw *ipniLookupResponseWriter) WriteProviderResult(pr model.ProviderResult) error {
	if ilpw.nd {
		if err := ilpw.encoder.Encode(pr); err != nil {
			return err
		}
		if _, err := ilpw.w.Write(newline); err != nil {
			return err
		}
		if ilpw.f != nil {
			ilpw.f.Flush()
		}
	} else {
		ilpw.result.ProviderResults = append(ilpw.result.ProviderResults, pr)
	}
	ilpw.count++
	return nil
}

func (ilpw *ipniLookupResponseWriter) Close() error {
	if ilpw.count == 0 {
		return apierror.New(nil, http.StatusNotFound)
	}
	if ilpw.nd {
		return nil
	}
	return ilpw.encoder.Encode(ilpw.result)
}
