package httpsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/maurl"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const defaultHttpTimeout = 10 * time.Second

var log = logging.Logger("dagsync/httpsync")

// Sync provides sync functionality for use with all http syncs.
type Sync struct {
	blockHook func(peer.ID, cid.Cid)
	client    *http.Client
	lsys      ipld.LinkSystem

	// libp2phttp
	clientHost *libp2phttp.HTTPHost
	protoID    protocol.ID
}

// NewSync creates a new Sync.
func NewSync(lsys ipld.LinkSystem, client *http.Client, blockHook func(peer.ID, cid.Cid)) *Sync {
	if client == nil {
		client = &http.Client{
			Timeout: defaultHttpTimeout,
		}
	}
	return &Sync{
		blockHook: blockHook,
		client:    client,
		lsys:      lsys,
	}
}

var errHeadFromUnexpectedPeer = errors.New("found head signed from an unexpected peer")

// Syncer provides sync functionality for a single sync with a peer.
type Syncer struct {
	client  *http.Client
	peerID  peer.ID
	protos  libp2phttp.WellKnownProtoMap
	rootURL url.URL
	urls    []*url.URL
	sync    *Sync
}

func NewLibp2pSync(lsys ipld.LinkSystem, clientHost *libp2phttp.HTTPHost, protoID protocol.ID, blockHook func(peer.ID, cid.Cid)) *Sync {
	return &Sync{
		blockHook: blockHook,
		lsys:      lsys,

		clientHost: clientHost,
		protoID:    protoID,
	}
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
//
// TODO: Replace arguments with peer.AddrInfo
func (s *Sync) NewSyncer(peerID peer.ID, addrs []multiaddr.Multiaddr) (*Syncer, error) {
	peerInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}
	if s.clientHost != nil {
		return s.newLibp2pSyncer(peerInfo)
	}
	return s.newSyncer(peerInfo)
}

func (s *Sync) newLibp2pSyncer(peerInfo peer.AddrInfo) (*Syncer, error) {
	httpClient, err := s.clientHost.NamespacedClient(s.protoID, peerInfo)
	if err != nil {
		return nil, err
	}

	sr := &Syncer{
		client:  &httpClient,
		peerID:  peerInfo.ID,
		rootURL: url.URL{Path: "/"},
		urls:    nil,
		sync:    s,
	}

	if peerInfo.ID != "" {
		sr.protos, err = s.clientHost.GetAndStorePeerProtoMap(httpClient.Transport, peerInfo.ID)
		if err != nil {
			return nil, err
		}
	}

	return sr, nil
}

func (s *Sync) newSyncer(peerInfo peer.AddrInfo) (*Syncer, error) {
	urls := make([]*url.URL, len(peerInfo.Addrs))
	for i, addr := range peerInfo.Addrs {
		var err error
		urls[i], err = maurl.ToURL(addr)
		if err != nil {
			return nil, err
		}
	}

	return &Syncer{
		client:  s.client,
		peerID:  peerInfo.ID,
		rootURL: *urls[0],
		urls:    urls[1:],
		sync:    s,
	}, nil
}

func (s *Sync) Close() {
	s.client.CloseIdleConnections()
}

func (s *Syncer) PeerProtoMap() libp2phttp.WellKnownProtoMap {
	return s.protos
}

// GetHead fetches the head of the peer's advertisement chain.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	var head cid.Cid
	var pubKey ic.PubKey

	err := s.fetch(ctx, "head", func(msg io.Reader) error {
		var err error
		pubKey, head, err = openSignedHeadWithIncludedPubKey(msg)
		return err
	})
	if err != nil {
		return cid.Undef, err
	}

	peerIDFromSig, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return cid.Undef, err
	}

	if s.peerID == "" {
		log.Warn("cannot verify publisher signature without peer ID")
	} else if peerIDFromSig != s.peerID {
		return cid.Undef, errHeadFromUnexpectedPeer
	}

	return head, nil
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

	// We run the block hook to emulate the behavior of graphsync's
	// `OnIncomingBlockHook` callback (gets called even if block is already stored
	// locally).
	//
	// We are purposefully not doing this in the StorageReadOpener because the
	// hook can do anything, including deleting the block from the block store. If
	// it did that then we would not be able to continue our traversal. So instead
	// we remember the blocks seen during traversal and then call the hook at the
	// end when we no longer care what it does with the blocks.
	if s.sync.blockHook != nil {
		for _, c := range cids {
			s.sync.blockHook(s.peerID, c)
		}
	}

	s.client.CloseIdleConnections()
	return nil
}

// walkFetch is run by a traversal of the selector.  For each block that the
// selector walks over, walkFetch will look to see if it can find it in the
// local data store. If it cannot, it will then go and get it over HTTP.  This
// emulates way libp2p/graphsync fetches data, but the actual fetch of data is
// done over HTTP.
func (s *Syncer) walkFetch(ctx context.Context, rootCid cid.Cid, sel selector.Selector) ([]cid.Cid, error) {
	// Track the order of cids we've seen during our traversal so we can call the
	// block hook function in the same order. We emulate the behavior of
	// graphsync's `OnIncomingBlockHook`, this means we call the blockhook even if
	// we have the block locally.
	var traversalOrder []cid.Cid
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid
		r, err := s.sync.lsys.StorageReadOpener(lc, l)
		if err == nil {
			// Found block read opener, so return it.
			traversalOrder = append(traversalOrder, c)
			return r, nil
		}

		if err = s.fetchBlock(ctx, c); err != nil {
			return nil, fmt.Errorf("failed to fetch block for cid %s: %w", c, err)
		}

		r, err = s.sync.lsys.StorageReadOpener(lc, l)
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
	err = progress.WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	return traversalOrder, nil
}

func (s *Syncer) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
nextURL:
	fetchURL := s.rootURL.JoinPath(rsrc)

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
			goto nextURL
		}
		return fmt.Errorf("fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		log.Errorw("Block not found from HTTP publisher", "resource", rsrc)
		// Include the string "content not found" so that indexers that have not
		// upgraded gracefully handle the error case. Because, this string is
		// being checked already.
		return fmt.Errorf("content not found: %w", ipld.ErrNotExists{})
	case http.StatusOK:
		log.Debugw("Found block from HTTP publisher", "resource", rsrc)
		return cb(resp.Body)
	default:
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
