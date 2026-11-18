package ipnisync_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// TestFetchErrorBodyDrain checks that a sync returns when a publisher answers
// with an error status and a body that does not end, even with no HTTP
// timeout. The server sends 1 KiB more than the client drains, then stalls. A
// client that drains a response body more than once, or without a limit, reads
// into the stall and blocks until the test context ends.
func TestFetchErrorBodyDrain(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			stop := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write(make([]byte, 4096+1024))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-stop:
				}
			}))
			t.Cleanup(srv.Close)
			t.Cleanup(func() { close(stop) })

			syncer, _ := newBlockSyncer(t, srv.URL, peer.ID("publisher"), func(peer.ID, cid.Cid) {}, ipnisync.ClientHTTPTimeout(0))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := syncer.Sync(ctx, rawCid(t, []byte("block")), selectorparse.CommonSelector_ExploreAllRecursively)
			require.Error(t, err)
			require.NoError(t, ctx.Err())
		})
	}
}

// TestFetchRetryClosesResponse checks that, when a plain HTTP publisher
// answers 404 or 403 on the IPNI path, the client closes that response before
// it retries without the path. The first body is larger than the drain limit,
// so its connection stays in use until the body is closed. A closed body
// leaves the connection closed, reused by the retry, or idle in the client's
// pool, possibly after a background drain. While the retried request is
// served, the handler accepts the reused connection, or closes the idle
// connections of the transport the syncer uses until the first connection
// closes. A response that is still open keeps its connection in use, so it
// does not close and the test fails.
func TestFetchRetryClosesResponse(t *testing.T) {
	// The syncer's client has no transport of its own, so it uses
	// http.DefaultTransport.
	transport := http.DefaultTransport.(*http.Transport)
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			type connKey struct{}
			c := rawCid(t, []byte("block"))
			var (
				mu          sync.Mutex
				firstConn   net.Conn
				firstClosed = make(chan struct{})
				released    atomic.Bool
			)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn := r.Context().Value(connKey{}).(net.Conn)
				switch r.URL.Path {
				case ipnisync.IPNIPath + "/" + c.String():
					mu.Lock()
					firstConn = conn
					mu.Unlock()
					w.WriteHeader(status)
					_, _ = w.Write(make([]byte, 2*4096))
					return
				case "/" + c.String():
					mu.Lock()
					reused := conn == firstConn
					mu.Unlock()
					released.Store(reused || closeIdleUntil(transport, firstClosed, 5*time.Second))
				}
				http.NotFound(w, r)
			}))
			srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
				return context.WithValue(ctx, connKey{}, c)
			}
			srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
				mu.Lock()
				defer mu.Unlock()
				if state == http.StateClosed && c == firstConn {
					close(firstClosed)
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)

			syncer, _ := newBlockSyncer(t, srv.URL, peer.ID("publisher"), func(peer.ID, cid.Cid) {})
			err := syncer.Sync(t.Context(), c, selectorparse.CommonSelector_ExploreAllRecursively)
			require.Error(t, err)
			require.True(t, released.Load())
		})
	}
}

// closeIdleUntil closes the idle connections of transport every 10ms until
// connClosed is closed. It returns false if timeout passes first.
func closeIdleUntil(transport *http.Transport, connClosed <-chan struct{}, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		transport.CloseIdleConnections()
		select {
		case <-connClosed:
			return true
		case <-deadline:
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}
