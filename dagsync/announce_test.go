package dagsync_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/dagsync/test"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestAnnounceReplace(t *testing.T) {
	t.Parallel()
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost(t)
	dstLnkS, blocked := test.MkBlockedLinkSystem(dstStore)
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
	}

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS, dagsync.RecvAnnounce(testTopic),
		dagsync.BlockHook(blockHook))
	require.NoError(t, err)
	defer sub.Close()

	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)
	defer pub.Close()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	firstCid := chainLnks[2].(cidlink.Link).Cid
	pub.SetRoot(firstCid)

	srcHostInfo := peer.AddrInfo{
		ID:    srcHost.ID(),
		Addrs: srcHost.Addrs(),
	}

	// Have the subscriber receive an announce.  This is the same as if it was
	// published by the publisher without having to wait for it to arrive.
	err = sub.Announce(context.Background(), firstCid, srcHostInfo)
	require.NoError(t, err)
	t.Log("Sent announce for first CID", firstCid)

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	// This first announce should start the handler and hit the blocked read.
	var release chan<- struct{}
	select {
	case release = <-blocked:
	case <-timer.C:
		t.Fatal("timed out waiting for sync 1")
	}

	// Announce two more times.
	c := chainLnks[1].(cidlink.Link).Cid
	pub.SetRoot(c)
	err = sub.Announce(context.Background(), c, srcHostInfo)
	require.NoError(t, err)
	t.Log("Sent announce for second CID", c)

	lastCid := chainLnks[0].(cidlink.Link).Cid
	pub.SetRoot(lastCid)
	err = sub.Announce(context.Background(), lastCid, srcHostInfo)
	require.NoError(t, err)
	t.Log("Sent announce for last CID", lastCid)

	close(release)

	select {
	case release = <-blocked:
	case <-timer.C:
		t.Fatal("timed out waiting for sync 2")
	}
	close(release)

	// Validate that sink for first CID happened.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		require.True(t, open, "event channel closed without receiving event")
		require.Equal(t, firstCid, downstream.Cid, "sync returned unexpected first cid")
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, downstream.Cid.String()+" not in receiver store")
		t.Log("Received sync notification for first CID:", firstCid)
	}

	// Validate that sink for last CID happened.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		require.True(t, open, "event channle closed without receiving event")
		require.Equal(t, lastCid, downstream.Cid, "sync returned unexpected last cid")
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, downstream.Cid.String()+" not in receiver store")
		t.Log("Received sync notification for last CID:", lastCid)
	}

	// Validate that no additional updates happen.
	select {
	case <-time.After(3 * time.Second):
	case changeEvent, open := <-watcher:
		require.Falsef(t, open,
			"no exchange should have been performed, but got change from peer %s for cid %s",
			changeEvent.PeerID, changeEvent.Cid)
	}

	_, ok := blocksSeenByHook[chainLnks[2].(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link2")

	_, ok = blocksSeenByHook[chainLnks[1].(cidlink.Link).Cid]
	require.False(t, ok, "hook should not have seen replaced link1")

	_, ok = blocksSeenByHook[chainLnks[0].(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link0")
}

func TestAnnounce_LearnsHttpPublisherAddr(t *testing.T) {
	// Instantiate a HTTP publisher
	pubh := test.MkTestHost(t)
	pubds := dssync.MutexWrap(datastore.NewMapDatastore())
	publs := test.MkLinkSystem(pubds)
	pub, err := ipnisync.NewPublisher(publs, pubh.Peerstore().PrivKey(pubh.ID()), ipnisync.WithHTTPListenAddrs("0.0.0.0:0"))
	require.NoError(t, err)
	defer pub.Close()

	// Store one CID at publisher
	wantOneMsg := "fish"
	oneLink, err := test.Store(pubds, basicnode.NewString(wantOneMsg))
	require.NoError(t, err)
	oneC := oneLink.(cidlink.Link).Cid

	// Store another CID at publisher
	wantAnotherMsg := "lobster"
	anotherLink, err := test.Store(pubds, basicnode.NewString(wantAnotherMsg))
	require.NoError(t, err)
	anotherC := anotherLink.(cidlink.Link).Cid

	// Instantiate a subscriber
	subh := test.MkTestHost(t)
	subds := dssync.MutexWrap(datastore.NewMapDatastore())
	subls := test.MkLinkSystem(subds)
	sub, err := dagsync.NewSubscriber(subh, subls, dagsync.RecvAnnounce(testTopic), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Announce one CID to the subscriber. Note that announce does a sync in the background.
	// That's why we use one cid here and another for sync so that we can concretely assert that
	// data was synced via the sync call and not via the earlier background sync via announce.
	err = sub.Announce(ctx, oneC, peer.AddrInfo{ID: pubh.ID(), Addrs: pub.Addrs()})
	require.NoError(t, err)

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to finish")
	case <-watcher:
	}

	// Now assert that we can sync another CID because, the subscriber should have learned the
	// address of publisher via earlier announce.
	peerInfo := peer.AddrInfo{
		ID: pubh.ID(),
	}
	gotc, err := sub.SyncAdChain(ctx, peerInfo, dagsync.WithHeadAdCid(anotherC))
	require.NoError(t, err)
	require.Equal(t, anotherC, gotc)
	gotNode, err := subls.Load(ipld.LinkContext{Ctx: ctx}, anotherLink, basicnode.Prototype.String)
	require.NoError(t, err)
	gotAnotherMsg, err := gotNode.AsString()
	require.NoError(t, err)
	require.Equal(t, wantAnotherMsg, gotAnotherMsg)
}

func TestAnnounceRepublish(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	srcLnkS := test.MkLinkSystem(srcStore)
	dstHost := test.MkTestHost(t)

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	dstStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS2 := test.MkLinkSystem(dstStore2)
	dstHost2 := test.MkTestHost(t)

	topics := test.WaitForMeshWithMessage(t, testTopic, dstHost, dstHost2)

	sub2, err := dagsync.NewSubscriber(dstHost2, dstLnkS2,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[1])), dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub2.Close()

	sub1, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[0]), announce.WithResend(true)),
		dagsync.StrictAdsSelector(false))
	require.NoError(t, err)
	defer sub1.Close()

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)
	defer pub.Close()

	watcher2, cncl := sub2.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	firstCid := chainLnks[2].(cidlink.Link).Cid
	pub.SetRoot(firstCid)

	// Announce one CID to subscriber1.
	pubInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	err = sub1.Announce(context.Background(), firstCid, pubInfo)
	require.NoError(t, err)
	t.Log("Sent announce for first CID", firstCid)

	// Validate that sink for first CID happened on subscriber2.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher2:
		require.True(t, open, "event channel closed without receiving event")
		require.True(t, downstream.Cid.Equals(firstCid), "sync returned unexpected first cid %s, expected %s", downstream.Cid, firstCid)
		_, err = dstStore2.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in second receiver store: %s", err)
		t.Log("Received sync notification for first CID:", firstCid)
	}
}

func TestAllowPeerReject(t *testing.T) {
	t.Parallel()

	// Set function to reject anything except dstHost, which is not the one
	// generating the update.
	var destID peer.ID
	allow := func(peerID peer.ID) bool {
		return peerID == destID
	}

	// Init dagsync publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, dstHost, pub, sub, sender := initPubSub(t, srcStore, dstStore, allow)
	defer srcHost.Close()
	defer dstHost.Close()
	defer pub.Close()
	defer sub.Close()

	destID = dstHost.ID()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	pub.SetRoot(c)
	err := announce.Send(context.Background(), c, pub.Addrs(), sender)
	require.NoError(t, err)

	select {
	case <-time.After(3 * time.Second):
	case _, open := <-watcher:
		require.False(t, open, "something was exchanged, and that is wrong")
	}
}

func TestAllowPeerAllows(t *testing.T) {
	t.Parallel()

	// Set function to allow any peer.
	allow := func(_ peer.ID) bool {
		return true
	}

	// Init dagsync publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, dstHost, pub, sub, sender := initPubSub(t, srcStore, dstStore, allow)
	defer srcHost.Close()
	defer dstHost.Close()
	defer pub.Close()
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	pub.SetRoot(c)
	err := announce.Send(context.Background(), c, pub.Addrs(), sender)
	require.NoError(t, err)

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for SyncFinished")
	case <-watcher:
	}
}

func mkLnk(t *testing.T, srcStore datastore.Batching) cid.Cid {
	// Update root with item
	np := basicnode.Prototype__Any{}
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	require.NoError(t, ma.AssembleKey().AssignString("hey"))
	require.NoError(t, ma.AssembleValue().AssignString("it works!"))
	require.NoError(t, ma.AssembleKey().AssignString("yes"))
	require.NoError(t, ma.AssembleValue().AssignBool(true))
	require.NoError(t, ma.Finish())

	n := nb.Build()
	lnk, err := test.Store(srcStore, n)
	require.NoError(t, err)

	return lnk.(cidlink.Link).Cid
}

func initPubSub(t *testing.T, srcStore, dstStore datastore.Batching, allowPeer func(peer.ID) bool) (host.Host, host.Host, dagsync.Publisher, *dagsync.Subscriber, announce.Sender) {
	srcHost, srcPrivKey := test.MkTestHostPK(t)
	dstHost := test.MkTestHost(t)
	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	srcLnkS := test.MkLinkSystem(srcStore)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]), p2psender.WithExtraData([]byte("t01000")))
	require.NoError(t, err)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic(testTopic))
	require.NoError(t, err)

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic, announce.WithTopic(topics[1]), announce.WithAllowPeer(allowPeer)))
	require.NoError(t, err)

	err = srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)

	return srcHost, dstHost, pub, sub, p2pSender
}
