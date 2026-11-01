package dagsync_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

// TestSyncHAMTEntriesDepthLimit checks that a HAMT entries sync stops at
// dagsync.MaxHamtSyncDepth, and does not fetch blocks below that depth.
func TestSyncHAMTEntriesDepthLimit(t *testing.T) {
	te := setupPublisherSubscriber(t, nil)

	// Each block has the shape of a HAMT node: a list of a bitmap and a list
	// of elements, here one link to the next block. The selector counts a
	// recursion level for each step into a child, so reaching the next block
	// takes two levels, one into the elements and one into the link. A depth
	// limit of MaxHamtSyncDepth therefore syncs MaxHamtSyncDepth/2 blocks.
	const chainLen = dagsync.MaxHamtSyncDepth
	const wantFetched = dagsync.MaxHamtSyncDepth / 2
	linkProto := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagJson),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: -1,
	}}
	// links[i] is the block i levels above the deepest one, so the head is
	// the last
	var links []ipld.Link
	var next ipld.Link
	for i := range chainLen {
		node, err := qp.BuildList(basicnode.Prototype.Any, 2, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Bytes([]byte{0x80, byte(i), byte(i >> 8)}))
			qp.ListEntry(la, qp.List(-1, func(la datamodel.ListAssembler) {
				if next != nil {
					qp.ListEntry(la, qp.Link(next))
				}
			}))
		})
		require.NoError(t, err)
		next, err = te.srcLinkSys.Store(ipld.LinkContext{}, linkProto, node)
		require.NoError(t, err)
		links = append(links, next)
	}

	pubInfo := peer.AddrInfo{ID: te.pub.ID(), Addrs: te.pub.Addrs()}
	head := links[len(links)-1].(cidlink.Link).Cid
	require.NoError(t, te.sub.SyncHAMTEntries(t.Context(), pubInfo, head))

	// The wantFetched blocks nearest the head are fetched, and none below
	for i, lnk := range links {
		has, err := te.dstStore.Has(t.Context(), datastore.NewKey(lnk.(cidlink.Link).Cid.String()))
		require.NoError(t, err)
		require.Equal(t, i >= chainLen-wantFetched, has, "block %d levels below the head", chainLen-1-i)
	}
}
