package ipnisync_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
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

const (
	// Sample data extracted directly from http://ipfs-advertisement.s3.us-west-2.amazonaws.com
	// Signed by provider QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC
	sampleNFTStorageCid  = "baguqeeranpqrweyey2zsab2mmt33ixc3jkg27p5ri3abr5di2pbbsbaig74q"
	sampleNFTStorageHead = `{"head":{"/":"` + sampleNFTStorageCid + `"},"pubkey":{"/":{"bytes":"CAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYi9qW5J1UIE4CUaRxxoROyqHkKZ2nhGRGWDurhNhPiNQ+n0sCy8rREEF9lertFt2n81c1Ik4W/8HZKxvk8PYKStrlGWjur6UoFyt+WuS/1hkRVyqEXjzBF7cLvfYQ75UaATIbLhXWpXqys1DVdh2snD0jnDugF4J72ZboIz6gwZC+BEd5axeVaibB9gJcg+5P48ihq9SAbr4dQUS47OgISMNb3f6nHfK7FQFF/KYx80byJYMJ9Oxsw8CB6C8pmDTdqvzYBT9kCUdY+loN/IcBqEeNw/UF7l3ay/ZJ2Yq437k6kn5BoxaZfxlbZHItoBjiLSJ9FSD7gpnUO+lJAh9bAgMBAAE="}},"sig":{"/":{"bytes":"NYkxmG812wa4DOCsZwH7NGLDERRkwtVwLNykf61RFug5VWNB1mKQjp0M3g0EBhVlf4dWqeh7ZCeVIC1qhAIONKw9VBAq4ITi4DTOlpx4yFphcNcCPGWNSfV0Qlosct6r64VmA4KtnRlYhwf6EsZ0gxcnZySbsv7KENHttmXLmO0ZBNQzG8dBNrp6thwiJbK4A1mw6+J6Ut4VzVwFUzJjONQzc0RpvUV7MwD1l6ZP83ucuOUT4Vw1F2yjVnFDgEa14N2tJfhxw2ZY2mHEiPH1pJL1dVxcjXRkiILV3V/qy1W5/cw+HhQHM3BIAgSBeWwSb7gbculHuBnOPDhHVKBmuQ=="}}}`
	sampleNFTStorageAd   = `{"Addresses":["/dns4/elastic.dag.house/tcp/443/wss"],"ContextID":{"/":{"bytes":"YmFndXFlZXJhdW1qbGM3MjRhenFucmRiNXh3dDJ3bWdxMmZ4N2lrd2N6MmxtNHhlNWR0dWx4NHIzemQycQ=="}},"Entries":{"/":"baguqeeraumjlc724azqnrdb5xwt2wmgq2fx7ikwcz2lm4xe5dtulx4r3zd2q"},"IsRm":false,"Metadata":{"/":{"bytes":"gBI"}},"PreviousID":{"/":"baguqeeramj6uf7ie5brhk5ivzi7e4mccndzke6fizc4qaie6t73xrvspkxrq"},"Provider":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","Signature":{"/":{"bytes":"CqsCCAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYi9qW5J1UIE4CUaRxxoROyqHkKZ2nhGRGWDurhNhPiNQ+n0sCy8rREEF9lertFt2n81c1Ik4W/8HZKxvk8PYKStrlGWjur6UoFyt+WuS/1hkRVyqEXjzBF7cLvfYQ75UaATIbLhXWpXqys1DVdh2snD0jnDugF4J72ZboIz6gwZC+BEd5axeVaibB9gJcg+5P48ihq9SAbr4dQUS47OgISMNb3f6nHfK7FQFF/KYx80byJYMJ9Oxsw8CB6C8pmDTdqvzYBT9kCUdY+loN/IcBqEeNw/UF7l3ay/ZJ2Yq437k6kn5BoxaZfxlbZHItoBjiLSJ9FSD7gpnUO+lJAh9bAgMBAAESGy9pbmRleGVyL2luZ2VzdC9hZFNpZ25hdHVyZRoiEiAz1niaKM3G2J40Bz/3wQbElyuBh1+2Q1E9SBj9wNsE8iqAAgOO1BKwq1RRy7AkZksWRrDlClhXU5IHAiy9pHuYtI/ePbVANiMAisjIEkd7jtJx7uct+/q2BTTTVcmZS7iE4OMTUymVbPQJ21qrzB6l5hulKD5ieedkJngAPCpizXmI1Z32Ib1zkuEFMraRcFaQ0YWqBKoIBJjjO4POGIdB2SgrCO0aFSd94k+2lyudMeWK+OisGLI7r6+ovd8g1VmcspEgl6pfdlHvThM3TdYGa46LO3kSCZmTzbI/XPnbMKaITvbuS3p8gm6elxNagx7Jxw4oP7hVyINSJ9chRu/w0RiBO986WRwhkGz1jW5jUF6VG/cQODzwjhuACteHf9TWp18"}}}`
)

func TestIPNISync_NFTStorage_DigestCheck(t *testing.T) {
	pubid, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
	require.NoError(t, err)
	tests := []struct {
		name, headCid, head, headAd, wantErr string
	}{
		{
			name:    "mismatching hash is not synced",
			headCid: sampleNFTStorageCid,
			head:    sampleNFTStorageHead,
			headAd:  "fish",
			wantErr: "hash digest mismatch",
		},
		{
			name:    "technically invalid but matching digest is synced",
			headCid: sampleNFTStorageCid,
			head:    sampleNFTStorageHead,
			headAd:  sampleNFTStorageAd,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch path.Base(r.URL.Path) {
				case "head":
					_, err := w.Write([]byte(test.head))
					require.NoError(t, err)
				case test.headCid:
					_, err := w.Write([]byte(test.headAd))
					require.NoError(t, err)
				default:
					http.NotFound(w, r)
				}
			}))
			defer pub.Close()

			ls := cidlink.DefaultLinkSystem()
			store := &memstore.Store{}
			ls.SetWriteStorage(store)
			ls.SetReadStorage(store)

			puburl, err := url.Parse(pub.URL)
			require.NoError(t, err)
			pubmaddr, err := maurl.FromURL(puburl)
			require.NoError(t, err)

			sync := ipnisync.NewSync(ls, nil)
			pubInfo := peer.AddrInfo{
				ID:    pubid,
				Addrs: []multiaddr.Multiaddr{pubmaddr},
			}
			syncer, err := sync.NewSyncer(pubInfo)
			require.NoError(t, err)

			head, err := syncer.GetHead(ctx)
			require.NoError(t, err)

			err = syncer.Sync(ctx, head, selectorparse.CommonSelector_MatchPoint)

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				_, exists := store.Bag[head.KeyString()]
				require.False(t, exists)
			} else {
				require.NoError(t, err)
				_, exists := store.Bag[head.KeyString()]
				require.True(t, exists)

				// Assert that, even though the CID does not match the computed link
				// the original CID can be loaded from the linksystem.
				wantLink := cidlink.Link{Cid: head}
				node, err := ls.Load(ipld.LinkContext{Ctx: ctx}, wantLink, basicnode.Prototype.Any)
				require.NoError(t, err)

				gotLink, err := ls.ComputeLink(wantLink.Prototype(), node)
				require.NoError(t, err)
				require.NotEqual(t, gotLink, wantLink)
			}
		})
	}
}

func TestIPNIsync_AcceptsSpecCompliantDagJson(t *testing.T) {
	const testTopic = "/test/topic"
	ctx := t.Context()
	pubID, pubPrK, _ := random.Identity()

	// Instantiate a dagsync publisher.
	publs := cidlink.DefaultLinkSystem()
	pubstore := &memstore.Store{}
	publs.SetWriteStorage(pubstore)
	publs.SetReadStorage(pubstore)

	pub, err := ipnisync.NewPublisher(publs, pubPrK, ipnisync.WithHeadTopic(testTopic), ipnisync.WithHTTPListenAddrs("0.0.0.0:0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pub.Close()) })

	link, err := publs.Store(
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
	require.NoError(t, err)
	pub.SetRoot(link.(cidlink.Link).Cid)

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	sync := ipnisync.NewSync(ls, nil)
	pubInfo := peer.AddrInfo{
		ID:    pubID,
		Addrs: pub.Addrs(),
	}
	syncer, err := sync.NewSyncer(pubInfo)
	require.NoError(t, err)

	head, err := syncer.GetHead(ctx)
	require.NoError(t, err)

	err = syncer.Sync(ctx, head, selectorparse.CommonSelector_MatchPoint)
	require.NoError(t, err)

	// Assert that data is loadable from the link system.
	wantLink := cidlink.Link{Cid: head}
	node, err := ls.Load(ipld.LinkContext{Ctx: ctx}, wantLink, basicnode.Prototype.Any)
	require.NoError(t, err)

	// Assert synced node link matches the computed link, i.e. is spec-compliant.
	gotLink, err := ls.ComputeLink(wantLink.Prototype(), node)
	require.NoError(t, err)
	require.Equal(t, gotLink, wantLink, "computed %s but got %s", gotLink.String(), wantLink.String())
}

func TestIPNIsync_NotFoundReturnsContentNotFoundErr(t *testing.T) {
	ctx := t.Context()
	pubID, pubPrK, _ := random.Identity()

	// Instantiate a dagsync publisher.
	publs := cidlink.DefaultLinkSystem()

	publs.StorageReadOpener = func(lnkCtx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		return nil, ipld.ErrNotExists{}
	}

	pub, err := ipnisync.NewPublisher(publs, pubPrK, ipnisync.WithHTTPListenAddrs("0.0.0.0:0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pub.Close()) })

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	sync := ipnisync.NewSync(ls, nil)
	pubInfo := peer.AddrInfo{
		ID:    pubID,
		Addrs: pub.Addrs(),
	}
	syncer, err := sync.NewSyncer(pubInfo)
	require.NoError(t, err)

	mh, err := multihash.Sum([]byte("fish"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	nonExistingCid := cid.NewCidV1(cid.Raw, mh)

	err = syncer.Sync(ctx, nonExistingCid, selectorparse.CommonSelector_MatchPoint)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "content not found")
}

func TestRequestTypeHint(t *testing.T) {
	pubID, pubPrK, _ := random.Identity()
	var lastReqTypeHint string

	// Instantiate a dagsync publisher.
	publs := cidlink.DefaultLinkSystem()

	publs.StorageReadOpener = func(lnkCtx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		if lnkCtx.Ctx != nil {
			hint, err := ipnisync.CidSchemaFromCtx(lnkCtx.Ctx)
			require.NoError(t, err)
			require.NotEmpty(t, hint)
			lastReqTypeHint = hint
		} else {
			lastReqTypeHint = ""
		}

		require.NotEmpty(t, lastReqTypeHint, "missing expected context value")
		return nil, ipld.ErrNotExists{}
	}

	pub, err := ipnisync.NewPublisher(publs, pubPrK, ipnisync.WithHTTPListenAddrs("0.0.0.0:0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pub.Close()) })

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	sync := ipnisync.NewSync(ls, nil)
	pubInfo := peer.AddrInfo{
		ID:    pubID,
		Addrs: pub.Addrs(),
	}
	syncer, err := sync.NewSyncer(pubInfo)
	require.NoError(t, err)

	testCid, err := cid.Decode(sampleNFTStorageCid)
	require.NoError(t, err)

	ctx, err := ipnisync.CtxWithCidSchema(context.Background(), ipnisync.CidSchemaAdvertisement)
	require.NoError(t, err)
	_ = syncer.Sync(ctx, testCid, selectorparse.CommonSelector_MatchPoint)
	require.Equal(t, ipnisync.CidSchemaAdvertisement, lastReqTypeHint)

	ctx, err = ipnisync.CtxWithCidSchema(context.Background(), ipnisync.CidSchemaEntryChunk)
	require.NoError(t, err)
	_ = syncer.Sync(ctx, testCid, selectorparse.CommonSelector_MatchPoint)
	require.Equal(t, ipnisync.CidSchemaEntryChunk, lastReqTypeHint)

	ctx, err = ipnisync.CtxWithCidSchema(context.Background(), "bad")
	require.ErrorIs(t, err, ipnisync.ErrUnknownCidSchema)
	err = syncer.Sync(ctx, testCid, selectorparse.CommonSelector_MatchPoint)
	require.ErrorIs(t, err, ipnisync.ErrUnknownCidSchema)
}

// newFetchErrorTestSyncer creates a Syncer that fetches from the plain HTTP
// server at serverURL. A non-zero retryMax configures the retryable HTTP
// client with a short backoff.
func newFetchErrorTestSyncer(t *testing.T, serverURL string, retryMax int, httpTimeout time.Duration) *ipnisync.Syncer {
	t.Helper()
	pubID, _, _ := random.Identity()

	puburl, err := url.Parse(serverURL)
	require.NoError(t, err)
	pubmaddr, err := maurl.FromURL(puburl)
	require.NoError(t, err)

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	var opts []ipnisync.ClientOption
	if retryMax != 0 {
		opts = append(opts, ipnisync.ClientHTTPRetry(retryMax, 10*time.Millisecond, 20*time.Millisecond))
	}
	if httpTimeout != 0 {
		opts = append(opts, ipnisync.ClientHTTPTimeout(httpTimeout))
	}

	sync := ipnisync.NewSync(ls, nil, opts...)
	syncer, err := sync.NewSyncer(peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{pubmaddr},
	})
	require.NoError(t, err)
	return syncer
}

func TestFetchError_RetryExhaustedStatus(t *testing.T) {
	const retryMax = 2
	const attempts = retryMax + 1
	longBody := strings.Repeat("x", 1000)

	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(longBody))
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, retryMax, 0)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, http.StatusInternalServerError, fe.StatusCode)
	require.Equal(t, attempts, fe.Attempts)
	require.NoError(t, fe.Err)
	require.Empty(t, fe.RetryAfter)
	// The body is capped at 256 bytes with "..." appended when truncated.
	require.Equal(t, strings.Repeat("x", 256)+"...", fe.Body)
	require.Contains(t, fe.URL, pub.URL)

	// The pre-change error string remains a prefix, with the status and body
	// appended. The Get "<url>": prefix is the url.Error wrap added by
	// net/http around the round tripper error.
	want := fmt.Sprintf("fetch request failed: Get %q: GET %s giving up after %d attempt(s): non success http fetch response at %s: %d body: %q",
		fe.URL, fe.URL, attempts, fe.URL, fe.StatusCode, fe.Body)
	require.Equal(t, want, err.Error())
}

func TestFetchError_RetryAfter(t *testing.T) {
	const retryMax = 2
	const attempts = retryMax + 1

	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// DefaultBackoff honors Retry-After on 429, so keep it small.
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("slow down"))
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, retryMax, 0)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, http.StatusTooManyRequests, fe.StatusCode)
	require.Equal(t, attempts, fe.Attempts)
	require.NoError(t, fe.Err)
	require.Equal(t, "1", fe.RetryAfter)
	require.Equal(t, "slow down", fe.Body)

	want := fmt.Sprintf("fetch request failed: Get %q: GET %s giving up after %d attempt(s): non success http fetch response at %s: %d (retry-after: 1) body: %q",
		fe.URL, fe.URL, attempts, fe.URL, fe.StatusCode, fe.Body)
	require.Equal(t, want, err.Error())
}

func TestFetchError_ConnectionRefused(t *testing.T) {
	const retryMax = 2
	const attempts = retryMax + 1

	// Use a local address that is not listening.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverURL := "http://" + ln.Addr().String()
	require.NoError(t, ln.Close())

	syncer := newFetchErrorTestSyncer(t, serverURL, retryMax, 0)

	_, err = syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Zero(t, fe.StatusCode)
	require.Equal(t, attempts, fe.Attempts)
	require.Error(t, fe.Err)
	require.Empty(t, fe.RetryAfter)
	require.Empty(t, fe.Body)

	// The transport error string must be byte identical to the pre-change
	// format.
	want := fmt.Sprintf("fetch request failed: Get %q: GET %s giving up after %d attempt(s): %s", fe.URL, fe.URL, attempts, fe.Err)
	require.Equal(t, want, err.Error())
}

func TestFetchError_Timeout(t *testing.T) {
	const retryMax = 2
	const attempts = retryMax + 1

	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, ipnisync.IPNIPath) {
			// Accept the connection but never respond.
			<-r.Context().Done()
			return
		}
		// Respond to the libp2phttp well-known metadata probe so NewSyncer
		// falls back to plain HTTP.
		http.NotFound(w, r)
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, retryMax, 300*time.Millisecond)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Zero(t, fe.StatusCode)
	require.Equal(t, attempts, fe.Attempts)
	require.Error(t, fe.Err)
	require.Contains(t, fe.Err.Error(), "deadline exceeded")

	want := fmt.Sprintf("fetch request failed: Get %q: GET %s giving up after %d attempt(s): %s", fe.URL, fe.URL, attempts, fe.Err)
	require.Equal(t, want, err.Error())
}

func TestFetchError_ContextCanceled(t *testing.T) {
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, ipnisync.IPNIPath) {
			<-r.Context().Done()
			return
		}
		http.NotFound(w, r)
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, 2, 0)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	_, err := syncer.GetHead(ctx)
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, 1, fe.Attempts)
	require.ErrorIs(t, fe.Err, context.DeadlineExceeded)
	require.NotEmpty(t, fe.URL)
	require.Contains(t, fe.URL, pub.URL)

	// The URL must be present in the rendered string (no double space).
	require.Contains(t, err.Error(), "GET "+fe.URL+" giving up after 1 attempt(s): context deadline exceeded")
}

func TestFetchError_NotFound(t *testing.T) {
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, 2, 0)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ipld.ErrNotExists{})
	require.Contains(t, err.Error(), "content not found")

	// The 404 path must not produce a FetchError.
	var fe *ipnisync.FetchError
	require.False(t, errors.As(err, &fe))
}

func TestFetchError_NonRetryPath(t *testing.T) {
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, 0, 0)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, http.StatusInternalServerError, fe.StatusCode)
	require.Zero(t, fe.Attempts)
	require.NoError(t, fe.Err)
	require.Empty(t, fe.RetryAfter)
	require.Empty(t, fe.Body)

	// With zero attempts the error renders without the "giving up" prefix.
	require.Equal(t, fmt.Sprintf("non success http fetch response at %s: %d", fe.URL, fe.StatusCode), err.Error())
}

func TestFetchError_MethodRendering(t *testing.T) {
	const urlStr = "https://example.com/ipni/head"
	transportErr := errors.New("connection refused")

	tests := []struct {
		name string
		fe   *ipnisync.FetchError
		want string
	}{
		{
			name: "empty method defaults to GET",
			fe:   &ipnisync.FetchError{URL: urlStr, Attempts: 3, Err: transportErr},
			want: fmt.Sprintf("GET %s giving up after 3 attempt(s): connection refused", urlStr),
		},
		{
			name: "GET method is byte identical to the default",
			fe:   &ipnisync.FetchError{Method: "GET", URL: urlStr, Attempts: 3, Err: transportErr},
			want: fmt.Sprintf("GET %s giving up after 3 attempt(s): connection refused", urlStr),
		},
		{
			name: "POST method renders POST",
			fe:   &ipnisync.FetchError{Method: "POST", URL: urlStr, Attempts: 3, Err: transportErr},
			want: fmt.Sprintf("POST %s giving up after 3 attempt(s): connection refused", urlStr),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.fe.Error())
		})
	}
}

func TestFetchError_ZeroAttemptsErr(t *testing.T) {
	const urlStr = "https://example.com/ipni/head"

	// With Err nil the zero-attempts string is unchanged.
	feNil := &ipnisync.FetchError{URL: urlStr, StatusCode: http.StatusInternalServerError}
	require.Equal(t,
		fmt.Sprintf("non success http fetch response at %s: %d", urlStr, http.StatusInternalServerError),
		feNil.Error())

	// With Err set the error is rendered after the status.
	feErr := &ipnisync.FetchError{URL: urlStr, StatusCode: http.StatusInternalServerError, Err: errors.New("boom")}
	require.Equal(t,
		fmt.Sprintf("non success http fetch response at %s: %d: boom", urlStr, http.StatusInternalServerError),
		feErr.Error())
}

// fetchBodySnippet returns the FetchError.Body produced when fetching from a
// server that responds with the given body and a 500 status. The non-zero
// retry max routes the final response through fetchErrorHandler, which is the
// only path that populates Body.
func fetchBodySnippet(t *testing.T, body []byte) string {
	t.Helper()

	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(body)
	}))
	defer pub.Close()

	syncer := newFetchErrorTestSyncer(t, pub.URL, 2, 0)

	_, err := syncer.GetHead(t.Context())
	require.Error(t, err)

	var fe *ipnisync.FetchError
	require.ErrorAs(t, err, &fe)
	return fe.Body
}

func TestSanitizeSnippet_PlainTextUnchanged(t *testing.T) {
	require.Equal(t, "hello world", fetchBodySnippet(t, []byte("hello world")))
}

func TestSanitizeSnippet_TextTruncation(t *testing.T) {
	// A long plain-text body is capped at 256 bytes and ends in "...".
	got := fetchBodySnippet(t, []byte(strings.Repeat("x", 1000)))
	require.Equal(t, strings.Repeat("x", 256)+"...", got)
}

func TestSanitizeSnippet_ControlBytes(t *testing.T) {
	// A few control bytes keep the text and append the dropped count.
	require.Equal(t, "hello world [1 non-printable bytes]", fetchBodySnippet(t, []byte("hello world\x00")))
}

func TestSanitizeSnippet_InvalidUTF8Hex(t *testing.T) {
	// A body of invalid UTF-8 / protobuf-style bytes renders as hex rather
	// than replacement characters.
	body := []byte{0x0a, 0x03, 0x68, 0x69, 0x00, 0xff, 0x80, 0x81}
	got := fetchBodySnippet(t, body)
	require.Equal(t, "hex:0a03686900ff8081", got)
	require.NotContains(t, got, "\uFFFD", "must not contain a replacement character")
}

func TestSanitizeSnippet_LongBinaryTruncation(t *testing.T) {
	// A long binary body renders as hex with a truncation marker.
	body := make([]byte, 100) // all NUL bytes
	got := fetchBodySnippet(t, body)
	require.True(t, strings.HasPrefix(got, "hex:"), "got %q", got)
	require.True(t, strings.HasSuffix(got, "..."), "got %q", got)
}
