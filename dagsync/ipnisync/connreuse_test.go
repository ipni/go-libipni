package ipnisync_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// TestSyncReusesConnection checks that consecutive syncs with a plain HTTP
// publisher fetch their blocks over one keep-alive connection. A caller that
// syncs a DAG one block at a time, such as a HAMT walked node by node, would
// otherwise open a new connection for every sync and could run out of local
// ports.
func TestSyncReusesConnection(t *testing.T) {
	type connKey struct{}
	data := []byte("block")
	c := rawCid(t, data)
	var (
		mu    sync.Mutex
		conns = make(map[net.Conn]struct{})
	)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ipnisync.IPNIPath+"/"+c.String() {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		conns[r.Context().Value(connKey{}).(net.Conn)] = struct{}{}
		mu.Unlock()
		_, _ = w.Write(data)
	}))
	srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, connKey{}, c)
	}
	srv.Start()
	t.Cleanup(srv.Close)

	syncer, store := newBlockSyncer(t, srv.URL, peer.ID("publisher"), func(peer.ID, cid.Cid) {})
	for range 20 {
		clear(store.Bag)
		require.NoError(t, syncer.Sync(t.Context(), c, selectorparse.CommonSelector_MatchPoint))
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, conns, 1)
}
