## dagsync

dagsync is an interface for maintaining a synchronized [IPLD dag](https://docs.ipld.io/) of IPNI advertisements between a publisher and a subscriber's current state for that publisher.

## Usage

Typically an application will be either a provider or a subscriber, but may be both.

### Publisher

Create a dagsync publisher. Update its root to publish a new advertisement. Send announcement messages to inform indexers a new advertisement is available.

```golang
publisher, err := ipnisync.NewPublisher(linkSys, privKey,
	ipnisync.WithHTTPListenAddrs("http://127.0.0.1:0"),
	ipnisync.WithStreamHost(publisherStreamHost),
)
if err != nil {
	panic(err)
}

// Create announcement senders to send advertisement announcements to indexers.
var senders []announce.Sender
httpSender, err := httpsender.New(announceURLs, id)
if err != nil {
	panic(err)
}
senders = append(senders, httpSender)
p2pSender, err := p2psender.New(publisherStreamHost, pubTopicName)
if err != nil {
	panic(err)
}
senders = append(senders, p2pSender)

// ...

// Publish updated root.
adCid := lnk.(cidlink.Link).Cid
err = publisher.SetRoot(adCid)
if err != nil {
	panic(err)
}
// Announce new advertisement.
err := announce.Send(ctx, adCid, adsPublishedHereAddrs, senders...)
if err != nil {
	panic(err)
}
```

### Subscriber

The `Subscriber` reads advertisement chains from index-providers. Its announcement receiver receives libp2p pubsub messages from a topic and direct HTTP announcements. The Subscriber reads advertisements is response to announcement messages.

Create a `Subscriber`:

```golang
sub, err := dagsync.NewSubscriber(dstHost, dstLinkSys, dagsync.RecvAnnounce(pubTopicName))
if err != nil {
	panic(err)
}

```
Optionally, request notification of updates:

```golang
watcher, cancelWatcher := sub.OnSyncFinished()
defer cancelWatcher()
go watch(watcher)

func watch(notifications <-chan dagsync.SyncFinished) {
	for {
		syncFinished := <-notifications
		// newHead is now available in the local dataStore
	}
}
```

To shutdown a `Subscriber`, call its `Close()` method.

A `Subscriber` can be created with announce receive options that include a function that determines if the `Subscriber` accepts or rejects announcements from a publisher.
```golang
sub, err := dagsync.NewSubscriber(dstHost, dstLinkSys,
	dagsync.RecvAnnounce(pubTopicName, announce.WithALlowPeer(allowPeer)),
)

```

The `Subscriber` keeps track of the latest head for each publisher that it has synced. This avoids exchanging the whole DAG from scratch in every update and instead downloads only the part that has not been synced. This value is not persisted as part of the library. If you want to start a `Subscriber` which has already partially synced with a provider you can use the `SetLatestSync` method:
```golang
sub, err := dagsync.NewSubscriber(dstHost, dstLinkSys)
if err != nil {
    panic(err)
}
// Set up partially synced publishers
sub.SetLatestSync(peerID1, lastSync1)
sub.SetLatestSync(peerID2, lastSync2)
sub.SetLatestSync(peerID3, lastSync3)
```
