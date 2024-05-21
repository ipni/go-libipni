package dagsync_test

import (
	"context"
	cryptorand "crypto/rand"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/dagsync/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

const (
	testTopic     = "/dagsync/testtopic"
	updateTimeout = 3 * time.Second
)

type pubMeta struct {
	pub dagsync.Publisher
	h   host.Host
}

func TestScopedBlockHook(t *testing.T) {
	err := quick.Check(func(ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			ds := dssync.MutexWrap(datastore.NewMapDatastore())
			pubHost, privKey := test.MkTestHostPK(t)

			lsys := test.MkLinkSystem(ds)
			pub, err := ipnisync.NewPublisher(lsys, privKey, ipnisync.WithStreamHost(pubHost))
			require.NoError(t, err)

			head := ll.Build(t, lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			pub.SetRoot(head.(cidlink.Link).Cid)

			subHost := test.MkTestHost(t)
			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			var calledGeneralBlockHookTimes int64
			sub, err := dagsync.NewSubscriber(subHost, subLsys,
				dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					atomic.AddInt64(&calledGeneralBlockHookTimes, 1)
				}),
				dagsync.StrictAdsSelector(false),
			)
			require.NoError(t, err)

			var calledScopedBlockHookTimes int64
			peerInfo := peer.AddrInfo{
				ID:    pubHost.ID(),
				Addrs: pubHost.Addrs(),
			}
			_, err = sub.SyncAdChain(context.Background(), peerInfo, dagsync.ScopedBlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
				atomic.AddInt64(&calledScopedBlockHookTimes, 1)
			}))
			require.NoError(t, err)

			require.Zero(t, atomic.LoadInt64(&calledGeneralBlockHookTimes),
				"General block hook should not have been called when scoped block hook is set")

			require.Equal(t, int64(ll.Length), atomic.LoadInt64(&calledScopedBlockHookTimes),
				"Didn't call scoped block hook enough times")

			anotherLL := llBuilder{
				Length: ll.Length,
				Seed:   ll.Seed + 1,
			}.Build(t, lsys)

			pub.SetRoot(anotherLL.(cidlink.Link).Cid)

			_, err = sub.SyncAdChain(context.Background(), peerInfo)
			require.NoError(t, err)

			require.Equal(t, int64(ll.Length), atomic.LoadInt64(&calledGeneralBlockHookTimes),
				"General hook should have been called only in secod sync")
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestSyncedCidsReturned(t *testing.T) {
	err := quick.Check(func(ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			ds := dssync.MutexWrap(datastore.NewMapDatastore())
			pubHost, privKey := test.MkTestHostPK(t)
			lsys := test.MkLinkSystem(ds)
			pub, err := ipnisync.NewPublisher(lsys, privKey, ipnisync.WithStreamHost(pubHost))
			require.NoError(t, err)

			head := ll.Build(t, lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			pub.SetRoot(head.(cidlink.Link).Cid)

			subHost := test.MkTestHost(t)
			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			sub, err := dagsync.NewSubscriber(subHost, subLsys, dagsync.StrictAdsSelector(false))
			require.NoError(t, err)

			onFinished, cancel := sub.OnSyncFinished()
			defer cancel()
			peerInfo := peer.AddrInfo{
				ID:    pubHost.ID(),
				Addrs: pubHost.Addrs(),
			}
			_, err = sub.SyncAdChain(context.Background(), peerInfo)
			require.NoError(t, err)

			finishedVal := <-onFinished
			require.Equalf(t, int(ll.Length), finishedVal.Count,
				"The finished value should include %d synced cids, but has %d", ll.Length, finishedVal.Count)

			require.Equal(t, head.(cidlink.Link).Cid, finishedVal.Cid,
				"The latest synced cid should be the head and first in the list")
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestConcurrentSync(t *testing.T) {
	err := quick.Check(func(ll llBuilder, publisherCount uint8) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			if publisherCount == 0 {
				// Empty case
				return
			}

			var publishers []pubMeta

			// limit to at most 10 concurrent publishers
			publisherCount := int(publisherCount)%10 + 1

			subHost := test.MkTestHost(t)

			for i := 0; i < publisherCount; i++ {
				ds := dssync.MutexWrap(datastore.NewMapDatastore())
				pubHost, privKey := test.MkTestHostPK(t)

				lsys := test.MkLinkSystem(ds)
				pub, err := ipnisync.NewPublisher(lsys, privKey, ipnisync.WithStreamHost(pubHost))
				require.NoError(t, err)

				publishers = append(publishers, pubMeta{pub, pubHost})

				head := ll.Build(t, lsys)
				if head == nil {
					// We built an empty list. So nothing to test.
					return
				}

				pub.SetRoot(head.(cidlink.Link).Cid)
			}

			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			var calledTimes int64
			sub, err := dagsync.NewSubscriber(subHost, subLsys,
				dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					atomic.AddInt64(&calledTimes, 1)
				}),
				dagsync.StrictAdsSelector(false),
			)
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			for _, pub := range publishers {
				wg.Add(1)

				go func(pub pubMeta) {
					defer wg.Done()
					peerInfo := peer.AddrInfo{
						ID:    pub.h.ID(),
						Addrs: pub.h.Addrs(),
					}
					_, err := sub.SyncAdChain(context.Background(), peerInfo)
					if err != nil {
						panic("sync failed: " + err.Error())
					}
				}(pub)
			}

			doneChan := make(chan struct{})

			go func() {
				wg.Wait()
				close(doneChan)
			}()

			select {
			case <-time.After(20 * time.Second):
				t.Fatal("Failed to sync")
			case <-doneChan:
			}

			require.Equalf(t, int64(ll.Length)*int64(publisherCount), atomic.LoadInt64(&calledTimes),
				"Didn't call block hook for each publisher. Expected %d saw %d", int(ll.Length)*publisherCount, calledTimes)
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestSync(t *testing.T) {
	err := quick.Check(func(dpsb dagsyncPubSubBuilder, ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			t.Parallel()
			pubSys := newHostSystem(t)
			subSys := newHostSystem(t)

			calledTimes := 0
			pub, sub, _ := dpsb.Build(t, testTopic, pubSys, subSys,
				[]dagsync.Option{dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					calledTimes++
				})},
			)

			head := ll.Build(t, pubSys.lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			pub.SetRoot(head.(cidlink.Link).Cid)

			peerInfo := peer.AddrInfo{
				ID:    pub.ID(),
				Addrs: pub.Addrs(),
			}
			_, err := sub.SyncAdChain(context.Background(), peerInfo)
			require.NoError(t, err)
			calledTimesFirstSync := calledTimes
			latestSync := sub.GetLatestSync(pubSys.host.ID())
			require.Equal(t, head, latestSync, "Subscriber did not persist latest sync")
			// Now sync again. We shouldn't call the hook.
			_, err = sub.SyncAdChain(context.Background(), peerInfo)
			require.NoError(t, err)
			require.Equalf(t, calledTimes, calledTimesFirstSync,
				"Subscriber called the block hook multiple times for the same sync. Expected %d, got %d", calledTimesFirstSync, calledTimes)
			require.Equal(t, int(ll.Length), calledTimes, "Subscriber did not call the block hook exactly once for each block")
		})
	}, &quick.Config{
		MaxCount: 5,
	})
	require.NoError(t, err)
}

// TestSyncWithHydratedDataStore tests what happens if we call sync when the
// subscriber datastore already has the dag.
func TestSyncWithHydratedDataStore(t *testing.T) {
	err := quick.Check(func(dpsb dagsyncPubSubBuilder, ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			t.Parallel()
			pubPrivKey, _, err := crypto.GenerateEd25519Key(cryptorand.Reader)
			require.NoError(t, err)

			pubDs := dssync.MutexWrap(datastore.NewMapDatastore())
			pubSys := hostSystem{
				privKey: pubPrivKey,
				host:    test.MkTestHost(t, libp2p.Identity(pubPrivKey)),
				ds:      pubDs,
				lsys:    test.MkLinkSystem(pubDs),
			}
			subDs := dssync.MutexWrap(datastore.NewMapDatastore())
			subSys := hostSystem{
				host: test.MkTestHost(t),
				ds:   subDs,
				lsys: test.MkLinkSystem(subDs),
			}

			calledTimes := 0
			var calledWith []cid.Cid
			pub, sub, _ := dpsb.Build(t, testTopic, pubSys, subSys,
				[]dagsync.Option{dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					calledWith = append(calledWith, c)
					calledTimes++
				})},
			)

			head := ll.Build(t, pubSys.lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			pub.SetRoot(head.(cidlink.Link).Cid)

			// Sync once to hydrate the datastore
			// Note we set the cid we are syncing to so we don't update the latestSync.
			peerInfo := peer.AddrInfo{
				ID:    pub.ID(),
				Addrs: pub.Addrs(),
			}
			_, err = sub.SyncAdChain(context.Background(), peerInfo, dagsync.WithHeadAdCid(head.(cidlink.Link).Cid))
			require.NoError(t, err)
			require.Equal(t, int(ll.Length), calledTimes, "Subscriber did not call the block hook exactly once for each block")
			require.Equal(t, head.(cidlink.Link).Cid, calledWith[0], "Subscriber did not call the block hook in the correct order")

			calledTimesFirstSync := calledTimes

			// Now sync again. We might call the hook because we don't have the latestSync persisted.
			_, err = sub.SyncAdChain(context.Background(), peerInfo)
			require.NoError(t, err)
			require.GreaterOrEqual(t, calledTimes, calledTimesFirstSync, "Expected to have called block hook twice. Once for each sync.")
		})
	}, &quick.Config{
		MaxCount: 5,
	})
	require.NoError(t, err)
}

func TestRoundTripSimple(t *testing.T) {
	// Init dagsync publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, dstHost, pub, sub, sender := initPubSub(t, srcStore, dstStore, nil)
	defer srcHost.Close()
	defer dstHost.Close()
	defer pub.Close()
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	require.NoError(t, err)

	rootCid := lnk.(cidlink.Link).Cid
	pub.SetRoot(rootCid)
	err = announce.Send(context.Background(), rootCid, pub.Addrs(), sender)
	require.NoError(t, err)

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream := <-watcher:
		require.Equalf(t, lnk.(cidlink.Link).Cid, downstream.Cid,
			"sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
	}
}

func TestRoundTrip(t *testing.T) {
	// Init dagsync publisher and subscriber
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost1, privKey1 := test.MkTestHostPK(t)
	srcLnkS1 := test.MkLinkSystem(srcStore1)

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2, privKey2 := test.MkTestHostPK(t)
	srcLnkS2 := test.MkLinkSystem(srcStore2)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost(t)
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, "testTopic", srcHost1, srcHost2, dstHost)

	p2pSender1, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub1, err := ipnisync.NewPublisher(srcLnkS1, privKey1, ipnisync.WithStreamHost(srcHost1))
	require.NoError(t, err)
	defer pub1.Close()

	p2pSender2, err := p2psender.New(nil, "", p2psender.WithTopic(topics[1]))
	require.NoError(t, err)

	pub2, err := ipnisync.NewPublisher(srcLnkS2, privKey2, ipnisync.WithStreamHost(srcHost2))
	require.NoError(t, err)
	defer pub2.Close()

	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
		t.Log("block hook got", c, "from", p)
	}

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce("", announce.WithTopic(topics[2])),
		dagsync.BlockHook(blockHook),
		dagsync.StrictAdsSelector(false),
	)
	require.NoError(t, err)
	defer sub.Close()

	watcher1, cncl1 := sub.OnSyncFinished()
	defer cncl1()
	watcher2, cncl2 := sub.OnSyncFinished()
	defer cncl2()

	// Update root on publisher one with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := test.Store(srcStore1, itm1)
	require.NoError(t, err)
	// Update root on publisher one with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := test.Store(srcStore2, itm2)
	require.NoError(t, err)

	rootCid1 := lnk1.(cidlink.Link).Cid
	pub1.SetRoot(rootCid1)
	err = announce.Send(context.Background(), rootCid1, pub1.Addrs(), p2pSender1)
	require.NoError(t, err)
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk1.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk1.(cidlink.Link), watcher2)

	rootCid2 := lnk2.(cidlink.Link).Cid
	pub2.SetRoot(rootCid2)
	err = announce.Send(context.Background(), rootCid2, pub2.Addrs(), p2pSender2)
	require.NoError(t, err)
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk2.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk2.(cidlink.Link), watcher2)

	require.Equal(t, 2, len(blocksSeenByHook))

	_, ok := blocksSeenByHook[lnk1.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link1")

	_, ok = blocksSeenByHook[lnk2.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link2")
}

func TestHttpPeerAddrPeerstore(t *testing.T) {
	pubHostSys := newHostSystem(t)
	subHostSys := newHostSystem(t)

	pub, sub, _ := dagsyncPubSubBuilder{
		IsHttp: true,
	}.Build(t, testTopic, pubHostSys, subHostSys, nil)

	ll := llBuilder{
		Length: 3,
		Seed:   1,
	}.Build(t, pubHostSys.lsys)

	// a new link on top of ll
	nextLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, ll)

	prevHead := ll
	head := nextLL

	pub.SetRoot(prevHead.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	_, err := sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)

	pub.SetRoot(head.(cidlink.Link).Cid)

	// Now call sync again with no address. The subscriber should re-use the
	// previous address and succeeed.
	peerInfo.Addrs = nil
	_, err = sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)
}

func TestSyncFinishedAlwaysDelivered(t *testing.T) {
	t.Parallel()
	pubHostSys := newHostSystem(t)
	subHostSys := newHostSystem(t)

	pub, sub, _ := dagsyncPubSubBuilder{}.Build(t, testTopic, pubHostSys, subHostSys, nil)

	ll := llBuilder{
		Length: 1,
		Seed:   1,
	}.Build(t, pubHostSys.lsys)

	// a new link on top of ll
	nextLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, ll)

	headLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, nextLL)

	// Purposefully not pulling from this channel yet to build up events.
	onSyncFinishedChan, cncl := sub.OnSyncFinished()
	defer cncl()

	pub.SetRoot(ll.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	_, err := sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)

	pub.SetRoot(nextLL.(cidlink.Link).Cid)

	_, err = sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)

	pub.SetRoot(headLL.(cidlink.Link).Cid)

	_, err = sub.SyncAdChain(context.Background(), peerInfo)
	require.NoError(t, err)

	head := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, headLL)

	pub.SetRoot(head.(cidlink.Link).Cid)

	// This is blocked until we read from onSyncFinishedChan
	syncDoneCh := make(chan error)
	go func() {
		_, err = sub.SyncAdChain(context.Background(), peerInfo)
		syncDoneCh <- err
	}()

	timer := time.NewTimer(10 * time.Second)
	select {
	case <-syncDoneCh:
		// Sync should have finished SyncFinished events are always delivered.
		require.NoError(t, err)
	case <-timer.C:
		t.Fatal("timed out waiting for sync to finish")
	}
	timer.Stop()

	timer.Reset(2 * time.Second)
	select {
	case <-onSyncFinishedChan:
	case <-timer.C:
		t.Fatal("did not get event from onSyncFinishedChan")
	}

	var count int
	for done := false; !done; {
		select {
		case <-onSyncFinishedChan:
			count++
		case <-timer.C:
			done = true
		}
	}
	timer.Stop()

	require.Equal(t, 3, count)
}

func TestMaxAsyncSyncs(t *testing.T) {
	// Create two publishers
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost1, privKey1 := test.MkTestHostPK(t)
	srcLnkS1 := test.MkLinkSystem(srcStore1)
	pub1, err := ipnisync.NewPublisher(srcLnkS1, privKey1, ipnisync.WithStreamHost(srcHost1))
	require.NoError(t, err)
	defer pub1.Close()

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2, privKey2 := test.MkTestHostPK(t)
	srcLnkS2 := test.MkLinkSystem(srcStore2)
	pub2, err := ipnisync.NewPublisher(srcLnkS2, privKey2, ipnisync.WithStreamHost(srcHost2))
	require.NoError(t, err)
	defer pub2.Close()

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost(t)
	dstLnkS, blocked := test.MkBlockedLinkSystem(dstStore)
	blocksSeenByHook := make(map[cid.Cid]struct{})
	bhMutex := new(sync.Mutex)
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		bhMutex.Lock()
		blocksSeenByHook[c] = struct{}{}
		bhMutex.Unlock()
	}

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic),
		dagsync.BlockHook(blockHook),
		dagsync.StrictAdsSelector(false),
		// If this value is > 1, then test must fail.
		dagsync.MaxAsyncConcurrency(1),
	)
	require.NoError(t, err)
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Update root on publisher1 with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := test.Store(srcStore1, itm1)
	require.NoError(t, err)
	rootCid1 := lnk1.(cidlink.Link).Cid
	pub1.SetRoot(rootCid1)

	// Update root on publisher2 with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := test.Store(srcStore2, itm2)
	require.NoError(t, err)
	rootCid2 := lnk2.(cidlink.Link).Cid
	pub2.SetRoot(rootCid2)

	err = sub.Announce(context.Background(), rootCid1, peer.AddrInfo{ID: pub1.ID(), Addrs: pub1.Addrs()})
	require.NoError(t, err)
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)

	err = sub.Announce(context.Background(), rootCid2, peer.AddrInfo{ID: pub2.ID(), Addrs: pub2.Addrs()})
	require.NoError(t, err)
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	var release chan<- struct{}
	select {
	case release = <-blocked:
	case <-timer.C:
		t.Fatal("timed out waiting for sync 1")
	}
	select {
	case <-timer.C:
	case release2 := <-blocked:
		close(release)
		close(release2)
		var done bool
		for !done {
			select {
			case release2 = <-blocked:
				close(release2)
			case <-timer.C:
				done = true
			}
		}
		t.Fatal("unexpected sync when sync blocked and concurrency set to 1")
	}

	close(release)

	timer.Reset(updateTimeout)
	select {
	case <-timer.C:
		t.Fatal("timed out waiting for sync 2")
	case release2 := <-blocked:
		close(release2)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for sync")
		case downstream := <-watcher:
			t.Log("got sync:", downstream.Cid)
		}
	}
	timer.Stop()

	_, ok := blocksSeenByHook[lnk1.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link1")

	_, ok = blocksSeenByHook[lnk2.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link2")

	// ----- Check concurrenty > 1 -----

	dstStore = dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost = test.MkTestHost(t)
	dstLnkS, blocked = test.MkBlockedLinkSystem(dstStore)
	blocksSeenByHook = make(map[cid.Cid]struct{})

	sub, err = dagsync.NewSubscriber(dstHost, dstLnkS,
		dagsync.RecvAnnounce(testTopic),
		dagsync.BlockHook(blockHook),
		dagsync.StrictAdsSelector(false),
		dagsync.MaxAsyncConcurrency(2),
	)
	require.NoError(t, err)
	defer sub.Close()

	err = sub.Announce(context.Background(), rootCid1, peer.AddrInfo{ID: pub1.ID(), Addrs: pub1.Addrs()})
	require.NoError(t, err)
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)

	err = sub.Announce(context.Background(), rootCid2, peer.AddrInfo{ID: pub2.ID(), Addrs: pub2.Addrs()})
	require.NoError(t, err)
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)

	timer = time.NewTimer(time.Second)
	select {
	case release = <-blocked:
	case <-timer.C:
		t.Fatal("timed out waiting for sync 1")
	}
	select {
	case <-timer.C:
		t.Fatal("expected another sync")
	case release2 := <-blocked:
		close(release)
		close(release2)
		var done bool
		for !done {
			select {
			case release2 = <-blocked:
				close(release2)
			case <-timer.C:
				done = true
			}
		}
	}
}

func waitForSync(t *testing.T, logPrefix string, store *dssync.MutexDatastore, expectedCid cidlink.Link, watcher <-chan dagsync.SyncFinished) {
	t.Helper()
	select {
	case <-time.After(updateTimeout):
		require.FailNow(t, "timed out waiting for sync to propogate")
	case downstream := <-watcher:
		require.Equal(t, expectedCid.Cid, downstream.Cid, "sync'd cid unexpected")
		_, err := store.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
		t.Log(logPrefix+" got sync:", downstream.Cid)
	}

}

func TestCloseSubscriber(t *testing.T) {
	st := dssync.MutexWrap(datastore.NewMapDatastore())
	sh := test.MkTestHost(t)

	lsys := test.MkLinkSystem(st)

	sub, err := dagsync.NewSubscriber(sh, lsys, dagsync.StrictAdsSelector(false))
	require.NoError(t, err)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	err = sub.Close()
	require.NoError(t, err)

	select {
	case _, open := <-watcher:
		require.False(t, open, "Watcher channel should have been closed")
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for watcher to close")
	}

	err = sub.Close()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		cncl()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(updateTimeout):
		t.Fatal("OnSyncFinished cancel func did not return after Close")
	}
}

func TestIdleHandlerCleaner(t *testing.T) {
	t.Parallel()
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
	}

	ttl := time.Second
	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.BlockHook(blockHook), dagsync.IdleHandlerTTL(ttl)})

	rootLnk, err := test.Store(te.srcStore, basicnode.NewString("hello world"))
	require.NoError(t, err)
	te.pub.SetRoot(rootLnk.(cidlink.Link).Cid)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Do a sync to create the handler.
	peerInfo := peer.AddrInfo{
		ID:    te.srcHost.ID(),
		Addrs: te.pub.Addrs(),
	}
	_, err = te.sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)

	// Check that the handler is preeent by seeing if it can be removed.
	require.True(t, te.sub.RemoveHandler(te.srcHost.ID()), "Expected handler to be present")

	// Do another sync to re-create the handler.
	_, err = te.sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)

	// For long enough for the idle cleaner to remove the handler, and check
	// that it was removed.
	time.Sleep(3 * ttl)
	require.False(t, te.sub.RemoveHandler(te.srcHost.ID()), "Expected handler to already be removed")
}

type dagsyncPubSubBuilder struct {
	IsHttp      bool
	P2PAnnounce bool
}

type hostSystem struct {
	privKey crypto.PrivKey
	host    host.Host
	ds      datastore.Batching
	lsys    ipld.LinkSystem
}

func newHostSystem(t *testing.T) hostSystem {
	privKey, _, err := crypto.GenerateEd25519Key(cryptorand.Reader)
	require.NoError(t, err)
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	return hostSystem{
		privKey: privKey,
		host:    test.MkTestHost(t, libp2p.Identity(privKey)),
		lsys:    test.MkLinkSystem(ds),
	}
}

func (b dagsyncPubSubBuilder) Build(t *testing.T, topicName string, pubSys hostSystem, subSys hostSystem, subOpts []dagsync.Option) (dagsync.Publisher, *dagsync.Subscriber, []announce.Sender) {
	var senders []announce.Sender
	if !b.P2PAnnounce {
		p2pSender, err := p2psender.New(pubSys.host, topicName)
		require.NoError(t, err)
		senders = append(senders, p2pSender)
	}

	var pub dagsync.Publisher
	var err error
	if b.IsHttp {
		pub, err = ipnisync.NewPublisher(pubSys.lsys, pubSys.privKey, ipnisync.WithHeadTopic(topicName), ipnisync.WithHTTPListenAddrs("127.0.0.1:0"))
		require.NoError(t, err)
	} else {
		pub, err = ipnisync.NewPublisher(pubSys.lsys, pubSys.privKey, ipnisync.WithStreamHost(pubSys.host), ipnisync.WithHeadTopic(topicName))
		require.NoError(t, err)
	}

	subOpts = append(subOpts, dagsync.StrictAdsSelector(false))
	sub, err := dagsync.NewSubscriber(subSys.host, subSys.lsys, subOpts...)
	require.NoError(t, err)

	return pub, sub, senders
}

type llBuilder struct {
	Length uint8
	Seed   int64
}

func (b llBuilder) Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link {
	return b.BuildWithPrev(t, lsys, nil)
}

func (b llBuilder) BuildWithPrev(t *testing.T, lsys ipld.LinkSystem, prev datamodel.Link) datamodel.Link {
	var linkproto = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}

	rng := rand.New(rand.NewSource(b.Seed))
	for i := 0; i < int(b.Length); i++ {
		p := basicnode.Prototype.Map
		b := p.NewBuilder()
		ma, err := b.BeginMap(2)
		require.NoError(t, err)
		eb, err := ma.AssembleEntry("Value")
		require.NoError(t, err)
		err = eb.AssignInt(int64(rng.Intn(100)))
		require.NoError(t, err)
		eb, err = ma.AssembleEntry("Next")
		require.NoError(t, err)
		if prev != nil {
			err = eb.AssignLink(prev)
			require.NoError(t, err)
		} else {
			err = eb.AssignNull()
			require.NoError(t, err)
		}
		err = ma.Finish()
		require.NoError(t, err)

		n := b.Build()

		prev, err = lsys.Store(linking.LinkContext{}, linkproto, n)
		require.NoError(t, err)
	}

	return prev
}
