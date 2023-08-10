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
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/dagsync/httpsync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
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
			subject, err := httpsync.NewPublisherForListener(l, handlerPath, lsys, privKey)
			req.NoError(err)

			rootCid := rootLnk.(cidlink.Link).Cid
			subject.SetRoot(rootCid)
			req.NoError(announce.Send(ctx, rootCid, subject.Addrs(), sender))
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

func TestPublisherWithLibp2pHTTP(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)

	publisherStore := &correctedMemStore{&memstore.Store{
		Bag: make(map[string][]byte),
	}}
	publisherLsys := cidlink.DefaultLinkSystem()
	publisherLsys.TrustedStorage = true
	publisherLsys.SetReadStorage(publisherStore)
	publisherLsys.SetWriteStorage(publisherStore)

	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 256, rand.Reader)
	req.NoError(err)

	publisher, err := httpsync.NewPublisherHandler(publisherLsys, privKey)
	req.NoError(err)

	// Use same identity as publisher. This is necessary so that same ID that
	// the publisher uses to sign head/ query responses is the same as the ID
	// used to identify the publisherStreamHost. Otherwise, it would be
	// necessary for the sync client to know both IDs: one for the stream host
	// to connect to, and one for the publisher to validate the dignatuse with.
	publisherStreamHost, err := libp2p.New(libp2p.Identity(privKey), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	req.NoError(err)

	// This is the "HTTP Host". It's like the libp2p "stream host" (aka core
	// host.Host), but it uses HTTP semantics instead of stream semantics.
	//
	// You can pass in options on creation like a stream host to do HTTP over
	// libp2p streams, and multiaddrs to create listeners on.
	publisherHost, err := libp2phttp.New(
		libp2phttp.StreamHost(publisherStreamHost),
		libp2phttp.ListenAddrs([]multiaddr.Multiaddr{multiaddr.StringCast("/ip4/127.0.0.1/tcp/0/http")}),
	)
	req.NoError(err)

	go publisherHost.Serve()
	defer publisherHost.Close()

	protoID := protocol.ID("/ipni-sync/1")

	serverStreamMa := publisherHost.Addrs()[0]
	serverHTTPMa := publisherHost.Addrs()[1]
	req.Contains(serverHTTPMa.String(), "/http")

	t.Log("libp2p stream server address:", serverStreamMa.String())
	t.Log("libp2p http server address:", serverHTTPMa.String())

	// Here is where we attach our request handler. Note that we are mounting
	// the "/ipni-sync/1" protocol at /ipni/. libp2phttp manages this mapping
	// and clients can learn about the mapping at .well-known/libp2p.
	//
	// In this case we also want out HTTP handler to not even know about the
	// prefix, so we use the stdlib http.StripPrefix.
	publisherHost.SetHttpHandlerAtPath(protoID, "/ipni/", http.StripPrefix("/ipni/", publisher))

	link, err := publisherLsys.Store(
		ipld.LinkContext{Ctx: ctx},
		cidlink.LinkPrototype{
			Prefix: cid.Prefix{
				Version:  1,
				Codec:    uint64(multicodec.DagJson),
				MhType:   uint64(multicodec.Sha2_256),
				MhLength: -1,
			},
		},
		fluent.MustBuildMap(basicnode.Prototype.Map, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("fish").AssignString("lobster")
			na.AssembleEntry("fish1").AssignString("lobster1")
			na.AssembleEntry("fish2").AssignString("lobster2")
			na.AssembleEntry("fish0").AssignString("lobster0")
		}))
	req.NoError(err)
	publisher.SetRoot(link.(cidlink.Link).Cid)

	testCases := []struct {
		name          string
		publisher     peer.AddrInfo
		newClientHost func(t *testing.T) *libp2phttp.HTTPHost
	}{
		{
			"HTTP transport",
			peer.AddrInfo{Addrs: []multiaddr.Multiaddr{serverHTTPMa}},
			func(t *testing.T) *libp2phttp.HTTPHost {
				clientHost, err := libp2phttp.New()
				req.NoError(err)
				return clientHost
			},
		},
		{
			"libp2p stream transport",
			peer.AddrInfo{ID: publisherStreamHost.ID(), Addrs: []multiaddr.Multiaddr{serverStreamMa}},
			func(t *testing.T) *libp2phttp.HTTPHost {
				clientStreamHost, err := libp2p.New(libp2p.NoListenAddrs)
				req.NoError(err)
				clientHost, err := libp2phttp.New(libp2phttp.StreamHost(clientStreamHost))
				req.NoError(err)
				return clientHost
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Plumbing to set up the test.
			clientStore := &correctedMemStore{&memstore.Store{
				Bag: make(map[string][]byte),
			}}
			clientLsys := cidlink.DefaultLinkSystem()
			clientLsys.TrustedStorage = true
			clientLsys.SetReadStorage(clientStore)
			clientLsys.SetWriteStorage(clientStore)
			clientSync := httpsync.NewLibp2pSync(clientLsys, tc.newClientHost(t), protoID, nil)

			// In a dagsync Subscriber, the clientSync is created once and
			// lives for the lifetime of the Subscriber (lifetime of indexer),
			// The clientSyncer is created for each sync operation and only
			// lives for the duration of the sync. The publisher's address may
			// change from one sync to the next, and we do not know the
			// addresses ahead of time.
			clientSyncer, err := clientSync.NewSyncer(tc.publisher.ID, tc.publisher.Addrs)
			req.NoError(err)

			headCid, err := clientSyncer.GetHead(ctx)
			req.NoError(err)

			req.Equal(link.(cidlink.Link).Cid, headCid)

			clientSyncer.Sync(ctx, headCid, selectorparse.CommonSelector_MatchPoint)
			require.NoError(t, err)

			// Assert that data is loadable from the link system.
			wantLink := cidlink.Link{Cid: headCid}
			node, err := clientLsys.Load(ipld.LinkContext{Ctx: ctx}, wantLink, basicnode.Prototype.Any)
			require.NoError(t, err)

			// Assert synced node link matches the computed link, i.e. is spec-compliant.
			gotLink, err := clientLsys.ComputeLink(wantLink.Prototype(), node)
			require.NoError(t, err)
			require.Equal(t, gotLink, wantLink, "computed %s but got %s", gotLink.String(), wantLink.String())
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
