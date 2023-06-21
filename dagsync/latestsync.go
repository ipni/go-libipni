package dagsync

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// LatestSyncHandler defines how to store the latest synced cid for a given
// peer and how to fetch it. dagsync guarantees this will not be called
// concurrently for the same peer, but it may be called concurrently for
// different peers.
type LatestSyncHandler interface {
	SetLatestSync(peer peer.ID, cid cid.Cid)
	GetLatestSync(peer peer.ID) (cid.Cid, bool)
}

type DefaultLatestSyncHandler struct {
	m sync.Map
}

func (h *DefaultLatestSyncHandler) SetLatestSync(p peer.ID, c cid.Cid) {
	log.Infow("Updating latest sync", "cid", c, "peer", p)
	h.m.Store(p, c)
}

func (h *DefaultLatestSyncHandler) GetLatestSync(p peer.ID) (cid.Cid, bool) {
	v, ok := h.m.Load(p)
	if !ok {
		return cid.Undef, false
	}
	return v.(cid.Cid), true
}
