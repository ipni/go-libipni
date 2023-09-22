package client

import (
	"context"
	"errors"
	"net/http"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

// Finder is the interface implemented by all find clients.
type Finder interface {
	// Find queries for provider content records for a single multihash. If no
	// results are found then an empty response without error is returned.
	Find(context.Context, multihash.Multihash) (*model.FindResponse, error)
}

// FindBatch is a convenience function to lookup results for multiple
// multihashes. This works with either the Client or DHashClient. If no results
// are found then an empty response without error is returned.
func FindBatch(ctx context.Context, finder Finder, mhs []multihash.Multihash) (*model.FindResponse, error) {
	var resp *model.FindResponse
	for i := range mhs {
		r, err := finder.Find(ctx, mhs[i])
		if err != nil {
			var ae *apierror.Error
			if errors.As(err, &ae) && ae.Status() == http.StatusNotFound {
				continue
			}
			return nil, err
		}
		if r == nil || len(r.MultihashResults) == 0 {
			// multihash not found
			continue
		}
		if resp == nil {
			resp = r
		} else {
			resp.MultihashResults = append(resp.MultihashResults, r.MultihashResults...)
		}
	}
	if resp == nil {
		resp = &model.FindResponse{}
	}
	return resp, nil
}
