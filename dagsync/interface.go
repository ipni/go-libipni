package dagsync

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Publisher is an interface for updating the published dag.
type Publisher interface {
	// Addrs returns the addresses that the publisher is listening on.
	Addrs() []multiaddr.Multiaddr
	// ID returns the peer ID associated with the publisher.
	ID() peer.ID
	// Protocol returns multiaddr protocol code (P_P2P or P_HTTP).
	Protocol() int
	// SetRoot sets the root CID without publishing it.
	SetRoot(cid.Cid)
	// Close publisher.
	Close() error
}

// Syncer is the interface used to sync with a data source.
type Syncer interface {
	GetHead(context.Context) (cid.Cid, error)
	Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error
	SameAddrs([]multiaddr.Multiaddr) bool
}
