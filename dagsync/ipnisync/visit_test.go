package ipnisync_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestSyncVisitsLinkOnce checks that a sync visits each block once, when
// many paths in the DAG lead to the same block.
func TestSyncVisitsLinkOnce(t *testing.T) {
	const depth = 16

	// Each node links twice to the next node, so the DAG has 2^depth paths
	// through depth+1 blocks.
	pubStore := &memstore.Store{}
	pubLs := cidlink.DefaultLinkSystem()
	pubLs.SetWriteStorage(pubStore)
	pubLs.SetReadStorage(pubStore)
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}}
	var next ipld.Link
	for i := 0; i <= depth; i++ {
		node, err := qp.BuildList(basicnode.Prototype.Any, -1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Int(int64(i)))
			if next != nil {
				qp.ListEntry(la, qp.Link(next))
				qp.ListEntry(la, qp.Link(next))
			}
		})
		require.NoError(t, err)
		next, err = pubLs.Store(ipld.LinkContext{}, lp, node)
		require.NoError(t, err)
	}
	bag := make(map[string][]byte, len(pubStore.Bag))
	for k, v := range pubStore.Bag {
		c, err := cid.Cast([]byte(k))
		require.NoError(t, err)
		bag[c.String()] = v
	}

	srv := blockServer(t, bag, "")
	pubID, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
	require.NoError(t, err)
	var hookCount int
	syncer, store := newBlockSyncer(t, srv.URL, pubID, func(peer.ID, cid.Cid) { hookCount++ })

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, syncer.Sync(ctx, next.(cidlink.Link).Cid, sel))
	require.Len(t, store.Bag, depth+1)
	require.Equal(t, depth+1, hookCount)
}
