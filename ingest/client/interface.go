package client

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Interface is the interface implemented by all ingest clients.
type Interface interface {
	// Announce announces a new head CID directly to the indexer.
	Announce(ctx context.Context, provider *peer.AddrInfo, root cid.Cid) error
	// IndexContent creates an index directly on the indexer.
	IndexContent(ctx context.Context, providerID peer.ID, privateKey crypto.PrivKey, m multihash.Multihash, contextID []byte, metadata []byte, addrs []string) error
	// Register registers a provider directly with an indexer.
	Register(ctx context.Context, providerID peer.ID, privateKey crypto.PrivKey, addrs []string) error
}
