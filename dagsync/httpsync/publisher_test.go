package httpsync_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/dagsync/httpsync"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestNewPublisherForListener(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	store := &correctedMemStore{&memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	rootLnk, err := lsys.Store(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.Prefix{Version: 1, Codec: 0x0129, MhType: 0x12, MhLength: 32}}, basicnode.NewString("borp"))
	req.NoError(err)

	for _, handlerPath := range []string{"", "/", "boop/bop", "/boop/bop"} {
		t.Run("with path "+handlerPath, func(t *testing.T) {
			addr, err := net.ResolveTCPAddr("tcp", "192.168.200.1:8080")
			req.NoError(err)
			l := fakeListener{addr}
			privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
			req.NoError(err)
			sender := &fakeSender{}
			subject, err := httpsync.NewPublisherForListener(l, handlerPath, lsys, privKey, httpsync.WithAnnounceSenders(sender))
			req.NoError(err)

			subject.SetRoot(ctx, rootLnk.(cidlink.Link).Cid)
			req.NoError(subject.AnnounceHead(ctx))
			req.Len(sender.msgs, 1)
			req.Equal(rootLnk.(cidlink.Link).Cid, sender.msgs[0].Cid)
			req.Len(sender.msgs[0].Addrs, 1)
			maddr, err := multiaddr.NewMultiaddrBytes(sender.msgs[0].Addrs[0])
			req.NoError(err)
			pathPart := strings.TrimLeft(handlerPath, "/")
			expectedMaddr := "/ip4/192.168.200.1/tcp/8080/http"
			if pathPart != "" {
				expectedMaddr += "/httpath/" + url.PathEscape(pathPart)
			}
			req.Equal(expectedMaddr, maddr.String())

			resp := &mockResponseWriter{}
			u := &url.URL{
				Path: handlerPath + "/head",
			}
			if !strings.HasPrefix(handlerPath, "/") {
				u.Path = "/" + u.Path
			}
			subject.ServeHTTP(resp, &http.Request{URL: u})
			req.Equal(0, resp.status) // not explicitly set
			req.Nil(resp.header)
			respNode, err := ipld.Decode(resp.body, dagjson.Decode)
			req.NoError(err)
			headCid := mustCid(t, respNode, ipld.ParsePath("/head"))
			req.Equal(rootLnk.(cidlink.Link).Cid, headCid)
			expectedPubkey, err := crypto.MarshalPublicKey(privKey.GetPublic())
			req.NoError(err)
			pubkey := mustBytes(t, respNode, ipld.ParsePath("/pubkey"))
			req.Equal(expectedPubkey, pubkey)
			expectedSig, err := privKey.Sign(rootLnk.(cidlink.Link).Cid.Bytes())
			req.NoError(err)
			sig := mustBytes(t, respNode, ipld.ParsePath("/sig"))
			req.Equal(expectedSig, sig)
			// nothing extra?
			req.ElementsMatch([]string{"head", "pubkey", "sig"}, mapKeys(t, respNode))
		})
	}
}

func mapKeys(t *testing.T, n ipld.Node) []string {
	var keys []string
	require.Equal(t, n.Kind(), datamodel.Kind_Map)
	mi := n.MapIterator()
	for !mi.Done() {
		k, _, err := mi.Next()
		require.NoError(t, err)
		require.Equal(t, k.Kind(), datamodel.Kind_String)
		ks, err := k.AsString()
		require.NoError(t, err)
		keys = append(keys, ks)
	}
	return keys
}

func mustCid(t *testing.T, n ipld.Node, path ipld.Path) cid.Cid {
	for path.Len() > 0 {
		var err error
		var ps ipld.PathSegment
		ps, path = path.Shift()
		n, err = n.LookupBySegment(ps)
		require.NoError(t, err)
	}
	require.Equal(t, n.Kind(), datamodel.Kind_Link)
	lnkNode, err := n.AsLink()
	require.NoError(t, err)
	return lnkNode.(cidlink.Link).Cid
}

func mustBytes(t *testing.T, n ipld.Node, path ipld.Path) []byte {
	c, err := traversal.Get(n, path)
	require.NoError(t, err)
	require.Equal(t, c.Kind(), datamodel.Kind_Bytes)
	b, err := c.AsBytes()
	require.NoError(t, err)
	return b
}

var _ announce.Sender = (*fakeSender)(nil)

type fakeSender struct {
	msgs []message.Message
}

func (s *fakeSender) Send(ctx context.Context, msg message.Message) error {
	if s.msgs == nil {
		s.msgs = make([]message.Message, 0)
	}
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *fakeSender) Close() error { return nil }

type fakeListener struct {
	addr net.Addr
}

func (l fakeListener) Accept() (c net.Conn, err error) { return }
func (l fakeListener) Close() error                    { return nil }
func (l fakeListener) Addr() net.Addr                  { return l.addr }

type mockResponseWriter struct {
	header http.Header
	body   []byte
	status int
}

func (m *mockResponseWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *mockResponseWriter) Write(data []byte) (int, error) {
	m.body = append(m.body, data...)
	return len(data), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.status = statusCode
}

// TODO: remove when this is fixed in IPLD prime
type correctedMemStore struct {
	*memstore.Store
}

func (cms *correctedMemStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := cms.Store.Get(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return data, err
}

func (cms *correctedMemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := cms.Store.GetStream(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return rc, err
}
