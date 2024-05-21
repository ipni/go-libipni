package ipnisync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	headschema "github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/go-libipni/mautil"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("dagsync/ipnisync")

var ErrNoHTTPServer = errors.New("publisher has libp2p server without HTTP")

// Sync provides sync functionality for use with all http syncs.
type Sync struct {
	blockHook   func(peer.ID, cid.Cid)
	client      http.Client
	lsys        ipld.LinkSystem
	httpTimeout time.Duration

	// libp2phttp
	clientHost      *libp2phttp.Host
	clientHostMutex sync.Mutex
	authPeerID      bool
	rclient         *retryablehttp.Client
}

// Syncer provides sync functionality for a single sync with a peer.
type Syncer struct {
	client   *http.Client
	peerInfo peer.AddrInfo
	rootURL  url.URL
	urls     []*url.URL
	sync     *Sync

	// For legacy HTTP and external server support without IPNI path.
	noPath    bool
	plainHTTP bool
}

// NewSync creates a new Sync.
func NewSync(lsys ipld.LinkSystem, blockHook func(peer.ID, cid.Cid), options ...ClientOption) *Sync {
	opts := getClientOpts(options)

	s := &Sync{
		blockHook: blockHook,
		client: http.Client{
			Timeout: opts.httpTimeout,
		},
		clientHost: &libp2phttp.Host{
			StreamHost: opts.streamHost,
			// Allow compatibility with older version of libp2phttp.
			//
			// This should be deprecated once all storage providers have
			// upgraded to a version of ipni that uses go-libp2p v0.34.0.
			EnableCompatibilityWithLegacyWellKnownEndpoint: true,
		},
		lsys:        lsys,
		authPeerID:  opts.authPeerID,
		httpTimeout: opts.httpTimeout,
	}

	if opts.httpRetryMax != 0 {
		// Configure retryable HTTP client created by calls to NewSyncer.
		s.rclient = &retryablehttp.Client{
			RetryWaitMin: opts.httpRetryWaitMin,
			RetryWaitMax: opts.httpRetryWaitMax,
			RetryMax:     opts.httpRetryMax,
		}
	}

	return s
}

// NewSyncer creates a new Syncer to use for a single sync operation against a
// peer. A value for peerInfo.ID is optional for the HTTP transport.
func (s *Sync) NewSyncer(peerInfo peer.AddrInfo) (*Syncer, error) {
	var cli http.Client
	var httpClient *http.Client
	var err error
	var rtOpts []libp2phttp.RoundTripperOption
	if s.authPeerID {
		rtOpts = append(rtOpts, libp2phttp.ServerMustAuthenticatePeerID)
	}

	peerInfo = mautil.CleanPeerAddrInfo(peerInfo)
	if len(peerInfo.Addrs) == 0 {
		if s.clientHost.StreamHost == nil {
			return nil, errors.New("no peer addrs and no stream host")
		}
		peerStore := s.clientHost.StreamHost.Peerstore()
		if peerStore == nil {
			return nil, errors.New("no peer addrs and no stream host peerstore")
		}
		peerInfo.Addrs = peerStore.Addrs(peerInfo.ID)
		if len(peerInfo.Addrs) == 0 {
			return nil, errors.New("no peer addrs and none found in peertore")
		}
	}

	s.clientHostMutex.Lock()
	cli, err = s.clientHost.NamespacedClient(ProtocolID, peerInfo, rtOpts...)
	s.clientHostMutex.Unlock()
	var plainHTTP bool
	if err != nil {
		if strings.Contains(err.Error(), "limit exceeded") {
			return nil, err
		}
		httpAddrs := mautil.FindHTTPAddrs(peerInfo.Addrs)
		if len(httpAddrs) == 0 {
			return nil, ErrNoHTTPServer
		}
		log.Infow("Publisher is not a libp2phttp server. Using plain http", "err", err, "peer", peerInfo.ID)
		httpClient = &s.client
		plainHTTP = true
	} else {
		log.Infow("Publisher supports libp2phttp", "peer", peerInfo.ID)
		httpClient = &cli
	}
	httpClient.Timeout = s.httpTimeout

	if s.rclient != nil {
		// Instantiate retryable HTTP client.
		rclient := &retryablehttp.Client{
			HTTPClient:   httpClient,
			RetryWaitMin: s.rclient.RetryWaitMin,
			RetryWaitMax: s.rclient.RetryWaitMax,
			RetryMax:     s.rclient.RetryMax,
			CheckRetry:   retryablehttp.DefaultRetryPolicy,
			Backoff:      retryablehttp.DefaultBackoff,
		}
		httpClient = rclient.StandardClient()
	}

	urls := make([]*url.URL, len(peerInfo.Addrs))
	for i, addr := range peerInfo.Addrs {
		u, err := maurl.ToURL(addr)
		if err != nil {
			return nil, err
		}
		urls[i] = u.JoinPath(IPNIPath)
	}

	return &Syncer{
		client:   httpClient,
		peerInfo: peerInfo,
		rootURL:  *urls[0],
		urls:     urls[1:],
		sync:     s,

		plainHTTP: plainHTTP,
	}, nil
}

func (s *Sync) Close() {
	s.client.CloseIdleConnections()
	s.clientHostMutex.Lock()
	s.clientHost.Close()
	s.clientHostMutex.Unlock()
}

// GetHead fetches the head of the peer's advertisement chain.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	var signedHead *headschema.SignedHead
	err := s.fetch(ctx, "head", func(msg io.Reader) error {
		var err error
		signedHead, err = headschema.Decode(msg)
		return err
	})
	if err != nil {
		return cid.Undef, err
	}

	signerID, err := signedHead.Validate()
	if err != nil {
		return cid.Undef, err
	}
	if s.peerInfo.ID == "" {
		log.Warn("Cannot verify publisher signature without peer ID")
	} else if signerID != s.peerInfo.ID {
		return cid.Undef, fmt.Errorf("found head signed by an unexpected peer, peerID: %s, signed-by: %s", s.peerInfo.ID.String(), signerID.String())
	}

	// TODO: Check that the returned topic, if any, matches the expected topic.
	//if signedHead.Topic != nil && *signedHead.Topic != "" && expectedTopic != "" {
	//	if *signedHead.Topic != expectedTopic {
	//		return nil, ErrTopicMismatch
	//	}
	//}

	return signedHead.Head.(cidlink.Link).Cid, nil
}

func (s *Syncer) SameAddrs(maddrs []multiaddr.Multiaddr) bool {
	return mautil.MultiaddrsEqual(s.peerInfo.Addrs, maddrs)
}

// Sync syncs the peer's advertisement chain or entries chain.
func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	xsel, err := selector.CompileSelector(sel)
	if err != nil {
		return fmt.Errorf("failed to compile selector: %w", err)
	}

	cids, err := s.walkFetch(ctx, nextCid, xsel)
	if err != nil {
		return fmt.Errorf("failed to traverse requested dag: %w", err)
	}

	// The blockHook callback gets called for every synced block, even if block
	// is already stored locally.
	//
	// We are purposefully not doing this in the StorageReadOpener because the
	// hook can do anything, including deleting the block from the block store.
	// If it did that then we would not be able to continue our traversal. So
	// instead we remember the blocks seen during traversal and then call the
	// hook at the end when we no longer care what it does with the blocks.
	if s.sync.blockHook != nil {
		for _, c := range cids {
			s.sync.blockHook(s.peerInfo.ID, c)
		}
	}

	s.client.CloseIdleConnections()
	return nil
}

// walkFetch is run by a traversal of the selector. For each block that the
// selector walks over, walkFetch will look to see if it can find it in the
// local data store. If it cannot, it will then go and get it over HTTP.
func (s *Syncer) walkFetch(ctx context.Context, rootCid cid.Cid, sel selector.Selector) ([]cid.Cid, error) {
	// Track the order of cids seen during traversal so that the block hook
	// function gets called in the same order.
	var traversalOrder []cid.Cid
	getMissingLs := cidlink.DefaultLinkSystem()
	// Trusted because it will be hashed/verified on the way into the link
	// system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid
		// fetchBlock checks if the node is already present in storage.
		err := s.fetchBlock(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch block for cid %s: %w", c, err)
		}

		r, err := s.sync.lsys.StorageReadOpener(lc, l)
		if err == nil {
			traversalOrder = append(traversalOrder, c)
		}
		return r, err
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     getMissingLs,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
		Path: datamodel.NewPath([]datamodel.PathSegment{}),
	}

	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: rootCid}, basicnode.Prototype.Any)
	if err != nil {
		return nil, fmt.Errorf("failed to load node for root cid %s: %w", rootCid, err)
	}

	err = progress.WalkMatching(rootNode, sel, func(_ traversal.Progress, _ datamodel.Node) error { return nil })
	if err != nil {
		return nil, err
	}
	return traversalOrder, nil
}

func (s *Syncer) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
nextURL:
	fetchURL := s.rootURL.JoinPath(rsrc)
	var doneRetry bool
retry:
	req, err := http.NewRequestWithContext(ctx, "GET", fetchURL.String(), nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		if len(s.urls) != 0 {
			log.Errorw("Fetch request failed, will retry with next address", "err", err)
			s.rootURL = *s.urls[0]
			s.urls = s.urls[1:]
			if s.noPath {
				s.rootURL.Path = strings.TrimSuffix(s.rootURL.Path, strings.Trim(IPNIPath, "/"))
			}
			goto nextURL
		}
		if !doneRetry && errors.Is(err, network.ErrReset) {
			log.Errorw("stream reset err, retrying", "publisher", s.peerInfo.ID, "url", fetchURL.String())
			// Only retry the same fetch once.
			doneRetry = true
			goto retry
		}
		return fmt.Errorf("fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return cb(resp.Body)
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, resp.Body)
		if s.plainHTTP && !s.noPath {
			// Try again with no path for legacy http.
			log.Warnw("Plain HTTP got not found response, retrying without IPNI path for legacy HTTP")
			s.rootURL.Path = strings.TrimSuffix(s.rootURL.Path, strings.Trim(IPNIPath, "/"))
			s.noPath = true
			goto nextURL
		}
		log.Errorw("Block not found from HTTP publisher", "resource", rsrc)
		// Include the string "content not found" so that indexers that have not
		// upgraded gracefully handle the error case. Because, this string is
		// being checked already.
		return fmt.Errorf("content not found: %w", ipld.ErrNotExists{})
	case http.StatusForbidden:
		_, _ = io.Copy(io.Discard, resp.Body)
		if s.plainHTTP && !s.noPath {
			// Try again with no path for legacy http.
			log.Warnw("Plain HTTP got forbidden response, retrying without IPNI path for legacy HTTP")
			s.rootURL.Path = strings.TrimSuffix(s.rootURL.Path, strings.Trim(IPNIPath, "/"))
			s.noPath = true
			goto nextURL
		}
		fallthrough
	default:
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("non success http fetch response at %s: %d", fetchURL.String(), resp.StatusCode)
	}
}

// fetchBlock fetches an item into the datastore at c if not locally available.
func (s *Syncer) fetchBlock(ctx context.Context, c cid.Cid) error {
	n, err := s.sync.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	// node is already present.
	if n != nil && err == nil {
		return nil
	}

	return s.fetch(ctx, c.String(), func(data io.Reader) error {
		writer, committer, err := s.sync.lsys.StorageWriteOpener(ipld.LinkContext{Ctx: ctx})
		if err != nil {
			log.Errorw("Failed to get write opener", "err", err)
			return err
		}
		tee := io.TeeReader(data, writer)
		sum, err := multihash.SumStream(tee, c.Prefix().MhType, c.Prefix().MhLength)
		if err != nil {
			return err
		}
		if !bytes.Equal(c.Hash(), sum) {
			err := fmt.Errorf("hash digest mismatch; expected %s but got %s", c.Hash().B58String(), sum.B58String())
			log.Errorw("Failed to persist fetched block with mismatching digest", "cid", c, "err", err)
			return err
		}
		if err = committer(cidlink.Link{Cid: c}); err != nil {
			log.Errorw("Failed to commit", "err", err)
			return err
		}
		return nil
	})
}
