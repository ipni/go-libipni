package dagsync_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multicodec"
)

var srcHost host.Host

func ExamplePublisher() {
	// Init dagsync publisher and subscriber.
	srcPrivKey, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	srcHost, _ = libp2p.New(libp2p.Identity(srcPrivKey))
	defer srcHost.Close()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := makeLinkSystem(srcStore)

	pub, err := ipnisync.NewPublisher(srcLnkS, srcPrivKey, ipnisync.WithStreamHost(srcHost), ipnisync.WithHeadTopic("/indexer/ingest/testnet"))
	if err != nil {
		panic(err)
	}
	defer pub.Close()

	// Update root on publisher one with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := store(srcStore, itm1)
	if err != nil {
		panic(err)
	}
	pub.SetRoot(lnk1.(cidlink.Link).Cid)
	log.Print("Publish 1:", lnk1.(cidlink.Link).Cid)

	// Update root on publisher one with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := store(srcStore, itm2)
	if err != nil {
		panic(err)
	}
	pub.SetRoot(lnk2.(cidlink.Link).Cid)
	log.Print("Publish 2:", lnk2.(cidlink.Link).Cid)
}

func ExampleSubscriber() {
	dstHost, _ := libp2p.New()
	defer dstHost.Close()

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkSys := makeLinkSystem(dstStore)

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)

	sub, err := dagsync.NewSubscriber(dstHost, dstLnkSys, dagsync.RecvAnnounce("/indexer/ingest/testnet"))
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	// Connections are made after Subscriber is created, so that the connecting
	// host sees that the destination host has pubsub.
	dstPeerInfo := dstHost.Peerstore().PeerInfo(dstHost.ID())
	if err = srcHost.Connect(context.Background(), dstPeerInfo); err != nil {
		panic(err)
	}

	watcher, cancelWatcher := sub.OnSyncFinished()
	defer cancelWatcher()

	for syncFin := range watcher {
		fmt.Println("Finished sync to", syncFin.Cid, "with peer:", syncFin.PeerID)
	}
}

func makeLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(lnk.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			return ds.Put(lctx.Ctx, datastore.NewKey(lnk.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func store(srcStore datastore.Batching, n ipld.Node) (ipld.Link, error) {
	linkproto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}
	lsys := makeLinkSystem(srcStore)

	return lsys.Store(ipld.LinkContext{}, linkproto, n)
}
