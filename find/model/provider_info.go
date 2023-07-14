package model

import (
	"github.com/ipfs/go-cid"
	_ "github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	// AddrInfo contains the provider's peer ID and set of Multiaddr addresses
	// that content may be retrieved from.
	AddrInfo peer.AddrInfo
	// LastAdvertisement identifies the latest advertisement the indexer has
	// ingested.
	LastAdvertisement cid.Cid `json:",omitempty"`
	// LastAdvertisementTime is the time the latest advertisement was received.
	LastAdvertisementTime string `json:",omitempty"`
	// Lag is the current sync lag for this provider. A non-zero lag tells us
	// that there is a sync in progress, and how man advertisements that sync
	// has left to complete.
	Lag int `json:",omitempty"`
	// Publisher contains the peer ID and Multiaddr addresses of this
	// provider's advertisement publisher. Content advertisements are available
	// at these addresses.
	Publisher *peer.AddrInfo `json:",omitempty"`
	// ExtendedProviders describes extended providers registered for this
	// provider.
	ExtendedProviders *ExtendedProviders `json:",omitempty"`
	// FrozenAt identifies the last advertisement that was received before the
	// indexer became frozen.
	FrozenAt cid.Cid `json:",omitempty"`
	// FrozenAtTime is the time that the FrozenAt advertisement was received.
	FrozenAtTime string `json:",omitempty"`
	// Inactive means that no update has been received for the configured
	// Discovery.PollInterval, and the publisher is not responding to polls.
	Inactive bool `json:",omitempty"`
	// LastError is a description of the last ingestion error to occur for this
	// provider.
	LastError string `json:",omitempty"`
	// LastErrorTime is the time that LastError occurred.
	LastErrorTime string `json:",omitempty"`
}

// ExtendedProviders contains chain-level and context-level extended provider
// sets.
type ExtendedProviders struct {
	// Providers contains a chain-level set of extended providers.
	Providers []peer.AddrInfo `json:",omitempty"`
	// ContextualProviders contains a context-level sets of extended providers.
	Contextual []ContextualExtendedProviders `json:",omitempty"`
	// Metadatas contains a list of metadata overrides for this provider. If
	// extended provider's metadata is not specified then the main provider's
	// metadata is used instead.
	Metadatas [][]byte `json:",omitempty"`
}

// ContextualExtendedProviders holds information about context-level extended
// providers. These can either replace or compliment (union) the chain-level
// extended providers, which is driven by the Override flag.
type ContextualExtendedProviders struct {
	// Override defines whether chain-level extended providers are used for
	// this ContextID. If true, then the chain-level extended providers are
	// ignored.
	Override bool
	// ContextID defines the context ID that the extended providers have been
	// published for,
	ContextID string
	// Providers contains a list of context-level extended providers IDs and
	// addresses.
	Providers []peer.AddrInfo
	// Metadatas contains a list of content level metadata overrides for this
	// provider.
	Metadatas [][]byte `json:",omitempty"`
}
