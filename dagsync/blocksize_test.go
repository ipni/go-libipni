package dagsync_test

import (
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestMaxBlockSizeOption(t *testing.T) {
	const maxSize = 64
	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.MaxBlockSize(maxSize)})
	linkProto := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagJson),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: -1,
	}}
	pubInfo := peer.AddrInfo{ID: te.pub.ID(), Addrs: te.pub.Addrs()}

	// The dag-json encoding of a string adds two quote characters
	small, err := te.srcLinkSys.Store(ipld.LinkContext{}, linkProto, basicnode.NewString(strings.Repeat("s", maxSize-2)))
	require.NoError(t, err)
	require.NoError(t, te.sub.SyncOneEntry(t.Context(), pubInfo, small.(cidlink.Link).Cid))

	large, err := te.srcLinkSys.Store(ipld.LinkContext{}, linkProto, basicnode.NewString(strings.Repeat("l", maxSize-1)))
	require.NoError(t, err)
	err = te.sub.SyncOneEntry(t.Context(), pubInfo, large.(cidlink.Link).Cid)
	require.ErrorIs(t, err, ipnisync.ErrBlockTooLarge)
}
