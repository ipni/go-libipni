package dagsync_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/dagsync/test"
	"github.com/libp2p/go-libp2p"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type httpTestEnv struct {
	srcHost    host.Host
	dstHost    host.Host
	pub        dagsync.Publisher
	sub        *dagsync.Subscriber
	srcStore   *dssync.MutexDatastore
	srcLinkSys linking.LinkSystem
	dstStore   *dssync.MutexDatastore
}

func setupPublisherSubscriber(t *testing.T, subscriberOptions []dagsync.Option) httpTestEnv {
	srcPrivKey, _, err := ic.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err, "Err generating private key")

	srcHost = test.MkTestHost(t, libp2p.Identity(srcPrivKey))
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLinkSys := test.MkLinkSystem(srcStore)

	pub, err := ipnisync.NewPublisher(srcLinkSys, srcPrivKey, ipnisync.WithHTTPListenAddrs("127.0.0.1:0"))
	require.NoError(t, err)
	t.Cleanup(func() {
		pub.Close()
	})

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLinkSys := test.MkLinkSystem(dstStore)
	dstHost := test.MkTestHost(t)

	subscriberOptions = append(subscriberOptions, dagsync.StrictAdsSelector(false))
	sub, err := dagsync.NewSubscriber(dstHost, dstStore, dstLinkSys, testTopic, subscriberOptions...)
	require.NoError(t, err)
	t.Cleanup(func() {
		sub.Close()
	})

	return httpTestEnv{
		srcHost:    srcHost,
		dstHost:    dstHost,
		pub:        pub,
		sub:        sub,
		srcStore:   srcStore,
		srcLinkSys: srcLinkSys,
		dstStore:   dstStore,
	}
}

func TestManualSync(t *testing.T) {
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
		t.Log("http block hook got", c, "from", p)
	}

	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.BlockHook(blockHook)})

	rootLnk, err := test.Store(te.srcStore, basicnode.NewString("hello world"))
	require.NoError(t, err)
	te.pub.SetRoot(rootLnk.(cidlink.Link).Cid)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerInfo := peer.AddrInfo{
		ID:    te.srcHost.ID(),
		Addrs: te.pub.Addrs(),
	}
	syncCid, err := te.sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)

	require.Equal(t, rootLnk.(cidlink.Link).Cid, syncCid)

	_, ok := blocksSeenByHook[syncCid]
	require.True(t, ok, "hook did not get", syncCid)
}

func TestSyncHttpFailsUnexpectedPeer(t *testing.T) {
	te := setupPublisherSubscriber(t, nil)

	rootLnk, err := test.Store(te.srcStore, basicnode.NewString("hello world"))
	require.NoError(t, err)
	te.pub.SetRoot(rootLnk.(cidlink.Link).Cid)

	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)

	defer cancel()
	_, otherPubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err, "failed to make another peerid")
	otherPeerID, err := peer.IDFromPublicKey(otherPubKey)
	require.NoError(t, err, "failed to make another peerid")

	// This fails because the head msg is signed by srcHost.ID(), but we are asking this to check if it's signed by otherPeerID.
	peerInfo := peer.AddrInfo{
		ID:    otherPeerID,
		Addrs: te.pub.Addrs(),
	}
	_, err = te.sub.SyncAdChain(ctx, peerInfo)
	require.ErrorContains(t, err, "unexpected peer")
}

func TestSyncFnHttp(t *testing.T) {
	var blockHookCalls int
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blockHookCalls++
		blocksSeenByHook[c] = struct{}{}
	}
	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.BlockHook(blockHook)})

	// Store the whole chain in source node
	chainLnks := test.MkChain(te.srcLinkSys, true)

	watcher, cancelWatcher := te.sub.OnSyncFinished()
	defer cancelWatcher()

	// Try to sync with a non-existing cid to chack that sync returns with err,
	// and SyncFinished watcher does not get event.
	cids := test.RandomCids(1)
	ctx, syncncl := context.WithTimeout(context.Background(), time.Second)
	defer syncncl()

	peerInfo := peer.AddrInfo{
		ID:    te.srcHost.ID(),
		Addrs: te.pub.Addrs(),
	}
	_, err := te.sub.SyncAdChain(ctx, peerInfo, dagsync.WithHeadAdCid(cids[0]))
	require.ErrorContains(t, err, "failed to traverse requested dag")
	syncncl()

	select {
	case <-time.After(updateTimeout):
	case <-watcher:
		t.Fatal("watcher should not receive event if sync error")
	}

	// Assert the latestSync is updated by explicit sync when cid and selector are unset.
	newHead := chainLnks[0].(cidlink.Link).Cid
	te.pub.SetRoot(newHead)

	lnk := chainLnks[1]

	require.Nil(t, te.sub.GetLatestSync(te.srcHost.ID()))

	// Sync with publisher via HTTP.
	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()

	syncCid, err := te.sub.SyncAdChain(ctx, peerInfo, dagsync.WithHeadAdCid(lnk.(cidlink.Link).Cid))
	require.NoError(t, err)

	require.Equal(t, lnk.(cidlink.Link).Cid, syncCid, "sync'd cid unexpected")
	_, err = te.dstStore.Get(context.Background(), datastore.NewKey(syncCid.String()))
	require.NoError(t, err, "data not in receiver store")
	syncncl()

	_, ok := blocksSeenByHook[lnk.(cidlink.Link).Cid]
	require.True(t, ok, "block hook did not see link cid")
	require.Equal(t, 11, blockHookCalls)

	// Assert the latestSync is not updated by explicit sync when cid is set
	require.Nil(t, te.sub.GetLatestSync(te.srcHost.ID()))

	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err = te.sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)
	require.Equal(t, newHead, syncCid, "sync'd cid unexpected")
	_, err = te.dstStore.Get(context.Background(), datastore.NewKey(syncCid.String()))
	require.NoError(t, err, "data not in receiver store")
	syncncl()

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync from published update")
	case syncFin, open := <-watcher:
		require.True(t, open, "sync finished channel closed with no event")
		require.Equal(t, newHead, syncFin.Cid)
	}
	cancelWatcher()

	assertLatestSyncEquals(t, te.sub, te.srcHost.ID(), newHead)
}
