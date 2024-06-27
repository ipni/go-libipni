package dagsync_test

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/dagsync/test"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestLatestSyncSuccess(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost(t)
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[1])),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[2])
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[1])
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[0])
}

func TestFirstSyncDepth(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost(t)
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.FirstSyncDepth(1),
		dagsync.RecvAnnounce(testTopic),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	lnk := chainLnks[0]
	adCid := lnk.(cidlink.Link).Cid

	pub.SetRoot(adCid)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	err = sub.Announce(ctx, adCid, pubInfo)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for sync to propagate")
	case syncDone, open := <-watcher:
		require.True(t, open, "event channel closed without receiving event")
		require.Equal(t, adCid, syncDone.Cid, "sync returned unexpected cid")
		_, err := dstStore.Get(context.Background(), datastore.NewKey(adCid.String()))
		require.NoError(t, err, "data not in receiver store")
		require.Equal(t, 1, syncDone.Count)
	}
}

func TestSyncFn(t *testing.T) {
	t.Parallel()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost(t)
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	var blockHookCalls int
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blockHookCalls++
		blocksSeenByHook[c] = struct{}{}
	}

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.BlockHook(blockHook),
		dagsync.RecvAnnounce("", announce.WithTopic(topics[1])),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	err = srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	watcher, cancelWatcher := sub.OnSyncFinished()
	defer cancelWatcher()

	// Try to sync with a non-existing cid to check that sync returns with err,
	// and SyncFinished watcher does not get event.
	cids := random.Cids(1)
	ctx, syncncl := context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	peerInfo := peer.AddrInfo{
		ID: srcHost.ID(),
	}
	_, err = sub.SyncAdChain(ctx, peerInfo, dagsync.WithHeadAdCid(cids[0]))
	require.Error(t, err, "expected error when no content to sync")
	syncncl()

	select {
	case <-time.After(updateTimeout):
	case <-watcher:
		t.Fatal("watcher should not receive event if sync error")
	}

	lnk := chainLnks[1]

	// Sync with publisher without publisher publishing to gossipsub channel.
	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err := sub.SyncAdChain(ctx, peerInfo, dagsync.WithHeadAdCid(lnk.(cidlink.Link).Cid))
	require.NoError(t, err)

	if !syncCid.Equals(lnk.(cidlink.Link).Cid) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	_, err = dstStore.Get(context.Background(), datastore.NewKey(syncCid.String()))
	require.NoError(t, err)
	syncncl()

	_, ok := blocksSeenByHook[lnk.(cidlink.Link).Cid]
	require.True(t, ok, "block hook did not see link cid")
	require.Equal(t, 11, blockHookCalls)

	// Assert the latestSync is not updated by explicit sync when cid is set
	require.Nil(t, sub.GetLatestSync(srcHost.ID()), "Sync should not update latestSync")

	// Assert the latestSync is updated by explicit sync when cid and selector are unset.
	newHead := chainLnks[0].(cidlink.Link).Cid
	pub.SetRoot(newHead)
	err = announce.Send(context.Background(), newHead, pub.Addrs(), p2pSender)
	require.NoError(t, err)

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync from published update")
	case syncFin, open := <-watcher:
		require.True(t, open, "sync finished channel closed with no event")
		require.Equalf(t, newHead, syncFin.Cid, "Should have been updated to %s, got %s", newHead, syncFin.Cid)
	}
	cancelWatcher()

	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err = sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)

	if !syncCid.Equals(newHead) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	_, err = dstStore.Get(context.Background(), datastore.NewKey(syncCid.String()))
	require.NoError(t, err, "data not in receiver store")
	syncncl()

	assertLatestSyncEquals(t, sub, srcHost.ID(), newHead)
}

func TestPartialSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	testStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)
	testLnkS := test.MkLinkSystem(testStore)

	chainLnks := test.MkChain(testLnkS, true)

	dstHost := test.MkTestHost(t)
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()
	test.MkChain(srcLnkS, true)

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[1])),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	err = sub.SetLatestSync(srcHost.ID(), chainLnks[3].(cidlink.Link).Cid)
	require.NoError(t, err)

	err = srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Fetching first few nodes.
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[2])

	// Check that first nodes hadn't been synced
	_, err = dstStore.Get(context.Background(), datastore.NewKey(chainLnks[3].(cidlink.Link).Cid.String()))
	require.ErrorIs(t, err, datastore.ErrNotFound, "data should not be in receiver store")

	// Set latest sync so we pass through one of the links
	err = sub.SetLatestSync(srcHost.ID(), chainLnks[1].(cidlink.Link).Cid)
	require.NoError(t, err)
	assertLatestSyncEquals(t, sub, srcHost.ID(), chainLnks[1].(cidlink.Link).Cid)

	// Update all the chain from scratch again.
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[0])

	// Check if the node we pass through was retrieved
	_, err = dstStore.Get(context.Background(), datastore.NewKey(chainLnks[1].(cidlink.Link).Cid.String()))
	require.ErrorIs(t, err, datastore.ErrNotFound, "data should not be in receiver store")
}

func TestStepByStepSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)

	srcHost, srcPrivKey := test.MkTestHostPK(t)
	dstHost := test.MkTestHost(t)

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	dstLnkS := test.MkLinkSystem(dstStore)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[1])),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	// Store half of the chain in destination to simulate the partial sync.
	test.MkChain(dstLnkS, false)

	// Sync the rest of the chain
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[1])
	senderAnnounceTest(t, pub, p2pSender, sub, dstStore, watcher, srcHost.ID(), chainLnks[0])
}

func TestLatestSyncFailure(t *testing.T) {
	t.Parallel()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)
	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	chainLnks := test.MkChain(srcLnkS, true)

	dstHost := test.MkTestHost(t)
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	t.Log("source host:", srcHost.ID())
	t.Log("targer host:", dstHost.ID())

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	err = srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	t.Log("Testing sync fail when the other end does not have the data")
	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	// Announce bad CID.
	badCid := random.Cids(1)[0]
	err = sub.Announce(context.Background(), badCid, pubInfo)
	require.NoError(t, err)
	// Check for fetch failure.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for sync finished event")
	case event, open := <-watcher:
		require.True(t, open)
		require.Equal(t, badCid, event.Cid)
		require.Error(t, event.Err)
		require.ErrorContains(t, event.Err, "non success http fetch response")
	}

	cncl()
	sub.Close()

	sub2, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub2.Close()

	t.Log("Testing hitting stop node works correctly")

	err = sub2.SetLatestSync(srcHost.ID(), chainLnks[2].(cidlink.Link).Cid)
	require.NoError(t, err)
	watcher, cncl = sub2.OnSyncFinished()
	defer cncl()

	rootCid := chainLnks[0].(cidlink.Link).Cid
	pub.SetRoot(rootCid)
	err = sub2.Announce(context.Background(), rootCid, pubInfo)
	require.NoError(t, err)

	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for sync finished event")
	case event, open := <-watcher:
		require.True(t, open)
		require.Equal(t, rootCid, event.Cid)
		require.Equal(t, 3, event.Count)
	}

	t.Log("Testing no update when node to sync is stop node")
	err = sub2.Announce(context.Background(), rootCid, pubInfo)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
	case <-watcher:
		require.FailNow(t, "should not have received a sync finished event")
	}
}

func TestUpdatePeerstoreAddr(t *testing.T) {
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS := test.MkLinkSystem(dstStore)
	dstHost := test.MkTestHost(t)
	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)
	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)
	defer pub.Close()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	require.NoError(t, test.WaitForP2PPublisher(pub, dstHost, testTopic))

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}

	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[2])
	require.Equal(t, pubInfo.Addrs, dstHost.Peerstore().Addrs(pub.ID()))

	// Update publisher address, sync, and check that peerstore matches.
	maddr, err := multiaddr.NewMultiaddr("/dns4/localhost/tcp/" + path.Base(pub.Addrs()[0].String()))
	require.NoError(t, err)
	pubInfo.Addrs = []multiaddr.Multiaddr{maddr}
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[1])
	require.Equal(t, pubInfo.Addrs, dstHost.Peerstore().Addrs(pub.ID()))
}

func TestSyncOnAnnounceIPNI(t *testing.T) {
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost(t)
	dstLnkS := test.MkLinkSystem(dstStore)

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)
	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)
	defer pub.Close()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	t.Log("Testing announce-sync with libp2phttp publisher at:", pub.Addrs())
	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[2])
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[1])
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[0])
}

func TestSyncOnAnnounceHTTP(t *testing.T) {
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost(t)
	dstLnkS := test.MkLinkSystem(dstStore)

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	_, srcPrivKey := test.MkTestHostPK(t)
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)
	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithHTTPListenAddrs("http://127.0.0.1:0"), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)
	defer pub.Close()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	t.Log("Testing announce-sync with HTTP publisher at:", pub.Addrs())
	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[2])
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[1])
	announceTest(t, pub, sub, dstStore, watcher, pubInfo, chainLnks[0])
}

func TestCancelDeadlock(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)
	dstHost := test.MkTestHost(t)

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost))
	require.NoError(t, err)
	defer pub.Close()

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS, dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	c := chainLnks[2].(cidlink.Link).Cid
	pub.SetRoot(c)

	peerInfo := peer.AddrInfo{
		ID:    srcHost.ID(),
		Addrs: srcHost.Addrs(),
	}
	_, err = sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)
	// Now there should be an event on the watcher channel.

	c = chainLnks[1].(cidlink.Link).Cid
	pub.SetRoot(c)

	_, err = sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)
	// Now the event dispatcher is blocked writing to the watcher channel.

	// It is necessary to wait a bit for the event dispatcher to block.
	time.Sleep(time.Second)

	// Cancel watching for sync finished. This should unblock the event
	// dispatcher, otherwise it will deadlock.
	done := make(chan struct{})
	go func() {
		cncl()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Drain the watcher so sub can close and test can exit.
		for range watcher {
		}
		t.Fatal("cancel did not return")
	}
}

func announceTest(t *testing.T, pub dagsync.Publisher, sub *dagsync.Subscriber, dstStore datastore.Batching, watcher <-chan dagsync.SyncFinished, peerInfo peer.AddrInfo, lnk ipld.Link) {
	t.Helper()
	c := lnk.(cidlink.Link).Cid
	if c != cid.Undef {
		pub.SetRoot(c)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := sub.Announce(ctx, c, peerInfo)
		require.NoError(t, err)
	}
	waitLatestSync(t, dstStore, watcher, c)
	assertLatestSyncEquals(t, sub, peerInfo.ID, c)
}

func senderAnnounceTest(t *testing.T, pub dagsync.Publisher, sender announce.Sender, sub *dagsync.Subscriber, dstStore datastore.Batching, watcher <-chan dagsync.SyncFinished, peerID peer.ID, lnk ipld.Link) {
	t.Helper()
	c := lnk.(cidlink.Link).Cid
	if c != cid.Undef {
		pub.SetRoot(c)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := announce.Send(ctx, c, pub.Addrs(), sender)
		require.NoError(t, err)
	}
	waitLatestSync(t, dstStore, watcher, c)
	assertLatestSyncEquals(t, sub, peerID, c)
}

func waitLatestSync(t *testing.T, dstStore datastore.Batching, watcher <-chan dagsync.SyncFinished, expectedSync cid.Cid) {
	t.Helper()
	select {
	case <-time.After(updateTimeout):
		require.FailNow(t, "timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		require.True(t, open, "event channel closed without receiving event")
		require.Equal(t, expectedSync, downstream.Cid, "sync returned unexpected cid")
		_, err := dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
	}
}

func assertLatestSyncEquals(t *testing.T, sub *dagsync.Subscriber, peerID peer.ID, want cid.Cid) {
	t.Helper()
	latest := sub.GetLatestSync(peerID)
	require.NotNil(t, latest, "latest sync is nil")
	got := latest.(cidlink.Link)
	require.Equal(t, want, got.Cid, "latestSync not updated correctly")
}
