package httpclient

import (
	"net/http"
	"path"

	"github.com/ipfs/go-cid"
)

const (
	shardKeyHeader = "x-ipni-dhstore-shard-key"
	cidPath        = "cid"
)

// ShardingRoundTripper adds x-ipni-dhstore-shard-key header to all GET requests for metadata, cid and multihash.
type ShardingRoundTripper struct {
	http.RoundTripper
}

// NewShardingClient creates a new http.Client with ShardingRoundTripper transport. The client should be used for sharded metadata, cid and multihash lookups only.
// Using the client for other purposes will not make any harm but will not bring any benefits either.
func NewShardingClient() *http.Client {
	return &http.Client{
		Transport: &ShardingRoundTripper{
			RoundTripper: http.DefaultTransport,
		},
	}
}

// RoundTrip adds x-ipni-dhstore-shard-key header to all GET requests for metadata, cid and multihash.
// It follows the following rules:
//   - If this is not a GET request - do nothing;
//   - If the request path conforms to ".../metadata/XYZ" - then XYZ will be set as x-ipni-dhstore-shard-key header as-is;
//   - If the request path conforms to ".../cid/XYZ" or ".../multihash/XYZ" -  the last path of the URL will be treated as cid/multihash and will be parsed with cid.Decode.
//     If that succeeds - B58 of the Multihash will be added as x-ipni-dhstore-shard-key header. Otherwise no x-ipni-dhstore-shard-key will be added.
func (cr *ShardingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet {
		var u, object, objectId, shardKey string
		u = r.URL.Path
		u, objectId = path.Split(u)
		// the remaining url should be at least as long as "/cid/"
		if len(u) < len(cidPath)+2 {
			goto DEFAULT
		}
		// don't forget to remove the last "/"
		_, object = path.Split(u[:len(u)-1])
		if object == metadataPath {
			shardKey = objectId
		} else if (object == findPath) || (object == cidPath) {
			// it's safe to use cid.Decode for both cids and multihashes. In the latter case, the string will be trated as B58 encoded multihash.
			c, err := cid.Decode(objectId)
			if err == nil {
				shardKey = c.Hash().B58String()
			}
		}
		if len(shardKey) > 0 {
			r.Header.Set(shardKeyHeader, shardKey)
		}
	}
DEFAULT:
	return cr.RoundTripper.RoundTrip(r)
}
