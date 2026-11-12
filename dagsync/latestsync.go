package dagsync

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// latestSyncHandler stores the latest synced CID of each peer. It is safe for
// concurrent use. A sync with a peer and a call to SetLatestSync for the same
// peer can write its record at the same time, and the later write wins.
type latestSyncHandler struct {
	m sync.Map
}

// setLatestSync records c as the latest sync of p. cid.Undef removes the
// record.
func (h *latestSyncHandler) setLatestSync(p peer.ID, c cid.Cid) {
	if c == cid.Undef {
		h.m.Delete(p)
		return
	}
	h.m.Store(p, c)
}

func (h *latestSyncHandler) getLatestSync(p peer.ID) (cid.Cid, bool) {
	v, ok := h.m.Load(p)
	if !ok {
		return cid.Undef, false
	}
	return v.(cid.Cid), true
}
