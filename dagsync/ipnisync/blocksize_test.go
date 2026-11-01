package ipnisync_test

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// blockServer serves the blocks in bag, keyed by CID string, and the given
// head message.
func blockServer(t *testing.T, bag map[string][]byte, head string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := path.Base(r.URL.Path)
		if name == "head" && head != "" {
			_, _ = w.Write([]byte(head))
			return
		}
		data, ok := bag[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newBlockSyncer(t *testing.T, srvURL string, pubID peer.ID, blockHook func(peer.ID, cid.Cid), opts ...ipnisync.ClientOption) (*ipnisync.Syncer, *memstore.Store) {
	store := &memstore.Store{}
	ls := cidlink.DefaultLinkSystem()
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	u, err := url.Parse(srvURL)
	require.NoError(t, err)
	maddr, err := maurl.FromURL(u)
	require.NoError(t, err)
	syncer, err := ipnisync.NewSync(ls, blockHook, opts...).NewSyncer(peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{maddr},
	})
	require.NoError(t, err)
	return syncer, store
}

func rawCid(t *testing.T, data []byte) cid.Cid {
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(uint64(multicodec.Raw), mh)
}

func TestFetchBlockSizeLimit(t *testing.T) {
	const limit = 1024
	tests := []struct {
		name     string
		maxSize  int64
		dataSize int
		wantErr  bool
	}{
		{name: "at limit", maxSize: limit, dataSize: limit},
		{name: "over limit", maxSize: limit, dataSize: limit + 1, wantErr: true},
		{name: "at default limit", dataSize: ipnisync.DefaultMaxBlockSize},
		{name: "over default limit", dataSize: ipnisync.DefaultMaxBlockSize + 1, wantErr: true},
		{name: "max int64 limit", maxSize: math.MaxInt64, dataSize: limit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte{'x'}, tc.dataSize)
			c := rawCid(t, data)
			srv := blockServer(t, map[string][]byte{c.String(): data}, "")
			pubID, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
			require.NoError(t, err)
			var opts []ipnisync.ClientOption
			if tc.maxSize != 0 {
				opts = append(opts, ipnisync.ClientMaxBlockSize(tc.maxSize))
			}
			syncer, store := newBlockSyncer(t, srv.URL, pubID, nil, opts...)

			err = syncer.Sync(t.Context(), c, selectorparse.CommonSelector_MatchPoint)
			_, stored := store.Bag[c.KeyString()]
			if tc.wantErr {
				require.ErrorIs(t, err, ipnisync.ErrBlockTooLarge)
				require.ErrorIs(t, err, ipnisync.ErrMalformed)
				require.False(t, stored)
			} else {
				require.NoError(t, err)
				require.True(t, stored)
			}
		})
	}
}

func TestGetHeadSizeLimit(t *testing.T) {
	pubID, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
	require.NoError(t, err)

	t.Run("within limit", func(t *testing.T) {
		srv := blockServer(t, nil, sampleNFTStorageHead)
		syncer, _ := newBlockSyncer(t, srv.URL, pubID, nil)
		head, err := syncer.GetHead(t.Context())
		require.NoError(t, err)
		require.Equal(t, sampleNFTStorageCid, head.String())
	})

	// Leading whitespace keeps the head message valid JSON
	t.Run("at limit", func(t *testing.T) {
		padding := strings.Repeat(" ", ipnisync.MaxHeadSize-len(sampleNFTStorageHead))
		srv := blockServer(t, nil, padding+sampleNFTStorageHead)
		syncer, _ := newBlockSyncer(t, srv.URL, pubID, nil)
		head, err := syncer.GetHead(t.Context())
		require.NoError(t, err)
		require.Equal(t, sampleNFTStorageCid, head.String())
	})

	t.Run("over limit", func(t *testing.T) {
		padding := strings.Repeat(" ", ipnisync.MaxHeadSize+1-len(sampleNFTStorageHead))
		srv := blockServer(t, nil, padding+sampleNFTStorageHead)
		syncer, _ := newBlockSyncer(t, srv.URL, pubID, nil)
		_, err := syncer.GetHead(t.Context())
		require.ErrorIs(t, err, ipnisync.ErrHeadTooLarge)
		// A publisher can serve a smaller head later, so the error is not
		// permanent
		require.NotErrorIs(t, err, ipnisync.ErrMalformed)
	})
}
