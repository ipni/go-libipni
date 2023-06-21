package dagsync

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// latestSyncHandler defines how to store the latest synced cid for a given
// peer and how to fetch it. dagsync guarantees this will not be called
// concurrently for the same peer, but it may be called concurrently for
// different peers.
type latestSyncHandler struct {
	m sync.Map
}

func (h *latestSyncHandler) setLatestSync(p peer.ID, c cid.Cid) {
	h.m.Store(p, c)
}

func (h *latestSyncHandler) getLatestSync(p peer.ID) (cid.Cid, bool) {
	v, ok := h.m.Load(p)
	if !ok {
		return cid.Undef, false
	}
	return v.(cid.Cid), true
}
