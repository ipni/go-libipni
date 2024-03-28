package dagsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/channelqueue"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/mautil"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("dagsync")

const (
	tempAddrTTL = 24 * time.Hour // must be long enough for ad chain to sync
)

// BlockHookFunc is the signature of a function that is called when a received.
type BlockHookFunc func(peer.ID, cid.Cid, SegmentSyncActions)

// Subscriber reads chains of advertisements from index-providers (publishers)
// and keeps track of the latest advertisement read from each publisher.
// Advertisements are read when explicitly requested, or in response to
// announcement messages if an announcement receiver is configured.
//
// An announcement receiver can receive announcements that are broadcast over
// libp2p gossip pubsub, and sent directly over HTTP. A receiver can be given
// an optional externally-defined function to determines whether to allow or
// deny messages from specific peers.
//
// A stateful message handler is maintained for each advertisement source peer.
// Messages from separate peers are handled concurrently, and multiple messages
// from the same peer are handled serially. If a handler is busy handling a
// message, and more messages arrive from the same peer, then the last message
// replaces the previous unhandled message to avoid having to maintain queues
// of messages. Handlers do not have persistent goroutines, but start a new
// goroutine to handle a single message.
type Subscriber struct {
	host host.Host

	addrTTL time.Duration

	handlers      map[peer.ID]*handler
	handlersMutex sync.Mutex

	// A map of block hooks to call for a specific peer id if the
	// generalBlockHook is overridden within a sync via ScopedBlockHook sync
	// option.
	scopedBlockHook      map[peer.ID]func(peer.ID, cid.Cid)
	scopedBlockHookMutex *sync.RWMutex
	generalBlockHook     BlockHookFunc

	// inEvents is used to send a SyncFinished from a peer handler to the
	// distributeEvents goroutine.
	inEvents chan SyncFinished

	addEventChan chan chan<- SyncFinished
	rmEventChan  chan chan<- SyncFinished

	// closing signals that the Subscriber is closing.
	closing chan struct{}
	// closeOnce ensures that the Close only happens once.
	closeOnce sync.Once
	// watchDone signals that the watch function exited.
	watchDone chan struct{}
	asyncWG   sync.WaitGroup

	ipniSync *ipnisync.Sync

	// A separate peerstore is used to store HTTP addresses. This is necessary
	// when peers have both libp2p and HTTP addresses, and a sync is requested
	// over a libp2p transport. Since libp2p transports do not use an explicit
	// multiaddr and depend on the libp2p peerstore, the HTTP addresses cannot
	// be stored in the libp2p peerstore as those are not usable by the libp2p
	// transport.
	httpPeerstore peerstore.Peerstore

	idleHandlerTTL    time.Duration
	latestSyncHandler latestSyncHandler
	lastKnownSync     LastKnownSyncFunc
	// syncSem is a counting semaphore that limits the number of concurrent
	// async syncs.
	syncSem chan struct{}

	adsDepthLimit  selector.RecursionLimit
	firstSyncDepth int64
	segDepthLimit  int64

	receiver *announce.Receiver

	// Track explicit Sync calls in progress and allow them to complete before
	// closing subscriber.
	expSyncClosed bool
	expSyncMutex  sync.Mutex
	expSyncWG     sync.WaitGroup

	// selector sequence for advertisements
	//
	// This selector sequence is wrapped with selector logic that stops
	// traversal when the latest synced link is reached. So, this only
	// specifies the selection sequence itself.
	adsSelectorSeq ipld.Node

	// selectorOne selects one multihash entries or HAMT block.
	selectorOne ipld.Node
	// selectorAll selects all multihash HAMT blocks.
	selectorAll ipld.Node
	// selectEnts selects multihash entries blocks up to depth limit.
	selectorEnts ipld.Node
}

// SyncFinished notifies an OnSyncFinished reader that a specified peer
// completed a sync. The channel receives events from providers that are
// manually synced to the latest, as well as those auto-discovered.
type SyncFinished struct {
	// Cid is the CID identifying the link that finished and is now the latest
	// sync for a specific peer.
	Cid cid.Cid
	// PeerID identifies the peer this SyncFinished event pertains to. This is
	// the publisher of the advertisement chain.
	PeerID peer.ID
	// Count is the number of CIDs synced.
	Count int
	// Err is used to return a failure to complete an asynchronous sync in
	// response to an announcement.
	Err error
}

// handler holds state that is specific to a peer
type handler struct {
	subscriber *Subscriber
	// asyncMutex serializes the handling of individual asymchronous as chain
	// syncs started by announce message. It protects the starting of new async
	// goroutines and the counting of running async syncs.
	asyncMutex sync.Mutex
	// syncMutex serializes the handling of individual syncs. This should only
	// guard the actual handling of a sync, nothing else.
	syncMutex sync.Mutex
	// peerID is the ID of the peer this handler is responsible for. This is
	// the publisher of an advertisement chain.
	peerID peer.ID
	// pendingMsg is an announce message queued for async handling.
	pendingMsg atomic.Pointer[announce.Announce]
	// expires is the time the handler is removed if it remains idle.
	expires time.Time
	// syncer is a sync client for this handler's peer.
	syncer Syncer
}

// wrapBlockHook wraps a possibly nil block hook func to allow dispatching to a
// blockhook func that is scoped within a .Sync call.
func wrapBlockHook() (*sync.RWMutex, map[peer.ID]func(peer.ID, cid.Cid), func(peer.ID, cid.Cid)) {
	var scopedBlockHookMutex sync.RWMutex
	scopedBlockHook := make(map[peer.ID]func(peer.ID, cid.Cid))
	return &scopedBlockHookMutex, scopedBlockHook, func(peerID peer.ID, cid cid.Cid) {
		scopedBlockHookMutex.RLock()
		f, ok := scopedBlockHook[peerID]
		scopedBlockHookMutex.RUnlock()
		if ok {
			f(peerID, cid)
		}
	}
}

// NewSubscriber creates a new Subscriber that processes pubsub messages and
// syncs dags advertised using the specified selector.
func NewSubscriber(host host.Host, lsys ipld.LinkSystem, options ...Option) (*Subscriber, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	scopedBlockHookMutex, scopedBlockHook, blockHook := wrapBlockHook()

	httpPeerstore, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, err
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreAll(ssb.ExploreRecursiveEdge())

	ipniSync := ipnisync.NewSync(lsys, blockHook,
		ipnisync.ClientStreamHost(host),
		ipnisync.ClientHTTPTimeout(opts.httpTimeout),
		ipnisync.ClientHTTPRetry(opts.httpRetryMax, opts.httpRetryWaitMin, opts.httpRetryWaitMax))

	s := &Subscriber{
		host: host,

		addrTTL: opts.addrTTL,
		closing: make(chan struct{}),

		handlers: make(map[peer.ID]*handler),
		inEvents: make(chan SyncFinished, 1),

		addEventChan: make(chan chan<- SyncFinished),
		rmEventChan:  make(chan chan<- SyncFinished),

		ipniSync: ipniSync,

		httpPeerstore: httpPeerstore,

		scopedBlockHookMutex: scopedBlockHookMutex,
		scopedBlockHook:      scopedBlockHook,
		generalBlockHook:     opts.blockHook,

		idleHandlerTTL:    opts.idleHandlerTTL,
		latestSyncHandler: latestSyncHandler{},
		lastKnownSync:     opts.lastKnownSync,

		adsDepthLimit:  recursionLimit(opts.adsDepthLimit),
		firstSyncDepth: opts.firstSyncDepth,
		segDepthLimit:  opts.segDepthLimit,

		selectorOne: ssb.ExploreRecursive(selector.RecursionLimitDepth(0), all).Node(),
		selectorAll: ssb.ExploreRecursive(selector.RecursionLimitNone(), all).Node(),
		selectorEnts: ssb.ExploreRecursive(recursionLimit(opts.entriesDepthLimit),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Next", ssb.ExploreRecursiveEdge()) // Next field in EntryChunk
			})).Node(),
	}

	if opts.strictAdsSelSeq {
		s.adsSelectorSeq = ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}).Node()
	} else {
		s.adsSelectorSeq = ssb.ExploreAll(ssb.ExploreRecursiveEdge()).Node()
	}

	if opts.hasRcvr {
		if opts.maxAsyncSyncs > 0 {
			s.syncSem = make(chan struct{}, opts.maxAsyncSyncs)
		}
		s.receiver, err = announce.NewReceiver(host, opts.rcvrTopic, opts.rcvrOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create announcement receiver: %w", err)
		}
		s.watchDone = make(chan struct{})
		// Start watcher to read announce messages.
		go s.watch()
	}
	// Start distributor to send SyncFinished messages to interested parties.
	go s.distributeEvents()
	// Start goroutine to remove idle publisher handlers.
	go s.idleHandlerCleaner()

	return s, nil
}

// HttpPeerStore returns the subscriber's HTTP peer store.
func (s *Subscriber) HttpPeerStore() peerstore.Peerstore {
	return s.httpPeerstore
}

// GetLatestSync returns the latest synced CID for the specified peer.
func (s *Subscriber) GetLatestSync(peerID peer.ID) ipld.Link {
	c, ok := s.latestSyncHandler.getLatestSync(peerID)
	if ok && c != cid.Undef {
		return cidlink.Link{Cid: c}
	}
	if s.lastKnownSync != nil {
		c, ok = s.lastKnownSync(peerID)
		if ok && c != cid.Undef {
			s.latestSyncHandler.setLatestSync(peerID, c)
			return cidlink.Link{Cid: c}
		}
	}
	return nil
}

// SetLatestSync sets the latest synced CID for a specified peer.
func (s *Subscriber) SetLatestSync(peerID peer.ID, latestSync cid.Cid) error {
	if latestSync == cid.Undef {
		return errors.New("cannot set latest sync to undefined value")
	}
	s.latestSyncHandler.setLatestSync(peerID, latestSync)
	return nil
}

// Close shuts down the Subscriber.
func (s *Subscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		err = s.doClose()
	})
	return err
}

func (s *Subscriber) doClose() error {
	// Cancel idle handler cleaner.
	close(s.closing)

	// Block any additional explicit Sync calls.
	s.expSyncMutex.Lock()
	s.expSyncClosed = true
	s.expSyncMutex.Unlock()
	// Wait for explicit Syncs calls to finish.
	s.expSyncWG.Wait()

	var err error
	if s.receiver != nil {
		// Close receiver and wait for watch to exit.
		err = s.receiver.Close()
		if err != nil {
			err = fmt.Errorf("error closing receiver: %w", err)
		}
		<-s.watchDone
	}

	// Wait for any syncs to complete.
	s.asyncWG.Wait()

	// Stop the distribution goroutine.
	close(s.inEvents)

	s.httpPeerstore.Close()

	return err
}

// OnSyncFinished creates a channel that receives change notifications, and
// adds that channel to the list of notification channels.
//
// Calling the returned cancel function removes the notification channel from
// the list of channels to be notified on changes, and it closes the channel to
// allow any reading goroutines to stop waiting on the channel.
func (s *Subscriber) OnSyncFinished() (<-chan SyncFinished, context.CancelFunc) {
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	cq := channelqueue.New[SyncFinished](-1)
	ch := cq.In()
	s.addEventChan <- ch

	cncl := func() {
		if ch == nil {
			return
		}
		select {
		case s.rmEventChan <- ch:
		case <-s.closing:
		}
		ch = nil
	}
	log.Debug("Subscriber OnSyncFinished configured")
	return cq.Out(), cncl
}

// RemoveHandler removes a handler for a publisher.
func (s *Subscriber) RemoveHandler(peerID peer.ID) bool {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()

	// Check for existing handler, remove if found.
	if _, ok := s.handlers[peerID]; !ok {
		return false
	}

	log.Infow("Removing sync handler", "peer", peerID)
	delete(s.handlers, peerID)

	return true
}

// SyncAdChain performs a one-off explicit sync with the given peer (publisher)
// for an advertisement chain, and updates the latest synced link to it.
//
// The latest root CID is queried from the peer directly. In the event where
// there is no latest root, i.e. querying the latest CID returns cid.Undef,
// this function returns cid.Undef with nil error.
//
// The latest synced CID is returned when this sync is complete. Any
// OnSyncFinished readers will also get a SyncFinished when the sync succeeds.
//
// It is the responsibility of the caller to make sure the given CID appears
// after the latest sync in order to avid re-syncing of content that may have
// previously been synced.
func (s *Subscriber) SyncAdChain(ctx context.Context, peerInfo peer.AddrInfo, options ...SyncOption) (cid.Cid, error) {
	s.expSyncMutex.Lock()
	if s.expSyncClosed {
		s.expSyncMutex.Unlock()
		return cid.Undef, errors.New("shutdown")
	}
	s.expSyncWG.Add(1)
	s.expSyncMutex.Unlock()
	defer s.expSyncWG.Done()

	opts := getSyncOpts(options)
	if opts.blockHook == nil {
		opts.blockHook = s.generalBlockHook
	}

	peerInfo = mautil.CleanPeerAddrInfo(peerInfo)
	var err error
	peerInfo, err = removeIDFromAddrs(peerInfo)
	if err != nil {
		return cid.Undef, err
	}

	log := log.With("peer", peerInfo.ID)

	hnd := s.getOrCreateHandler(peerInfo.ID)

	syncer, updatePeerstore, err := hnd.makeSyncer(peerInfo, true)
	if err != nil {
		return cid.Undef, err
	}

	// Set depth limit to ads depth limit unless scoped depth is non-zero.
	depthLimit := s.adsDepthLimit
	if opts.depthLimit != 0 {
		depthLimit = recursionLimit(opts.depthLimit)
	}
	// If traversal should not stop at the latest synced, then construct
	// advertisement chain selector to behave accordingly.
	var stopLnk ipld.Link
	if opts.resync {
		if opts.stopAdCid != cid.Undef {
			stopLnk = cidlink.Link{Cid: opts.stopAdCid}
		}
	} else {
		if opts.stopAdCid != cid.Undef {
			stopLnk = cidlink.Link{Cid: opts.stopAdCid}
		} else {
			stopLnk = s.GetLatestSync(peerInfo.ID)
		}
	}

	var updateLatest bool
	nextCid := opts.headAdCid
	if nextCid == cid.Undef {
		// Query the peer for the latest CID
		nextCid, err = syncer.GetHead(ctx)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot query head for sync: %w", err)
		}

		// Check if there is a latest CID.
		if nextCid == cid.Undef {
			// There is no head; nothing to sync.
			log.Info("No head to sync")
			return cid.Undef, nil
		}

		// Update the latest unless a specific CID to sync was given.
		updateLatest = true
	}
	var stopAtCid cid.Cid
	if stopLnk != nil {
		stopAtCid = stopLnk.(cidlink.Link).Cid
		if stopAtCid == nextCid {
			log.Infow("cid to sync to is the stop node. Nothing to do")
			return nextCid, nil
		}
	} else if s.firstSyncDepth != 0 && opts.depthLimit == 0 {
		depthLimit = recursionLimit(s.firstSyncDepth)
	}

	log = log.With("cid", nextCid)
	log.Debug("Start advertisement chain sync")

	if ctx.Err() != nil {
		return cid.Undef, fmt.Errorf("sync canceled: %w", ctx.Err())
	}

	segdl := s.segDepthLimit
	if opts.segDepthLimit != 0 {
		segdl = opts.segDepthLimit
	}

	sel := ExploreRecursiveWithStopNode(depthLimit, s.adsSelectorSeq, stopLnk)

	syncCount, err := hnd.handle(ctx, nextCid, sel, syncer, opts.blockHook, segdl, stopAtCid)
	if err != nil {
		return cid.Undef, fmt.Errorf("sync handler failed: %w", err)
	}

	// The sync succeeded, so remember this address in the appropriate
	// peerstore. Add to peerstore before sending the SyncFinished event so
	// that the address is present before anything triggered by the event is
	// run.
	updatePeerstore()

	if updateLatest {
		hnd.sendSyncFinishedEvent(nextCid, syncCount)
	}

	return nextCid, nil
}

func (s *Subscriber) SyncOneEntry(ctx context.Context, peerInfo peer.AddrInfo, entCid cid.Cid) error {
	return s.syncEntries(ctx, peerInfo, entCid, s.selectorOne, s.generalBlockHook, -1)
}

// SyncEntries syncs the entries chain starting with block identified by entCid.
func (s *Subscriber) SyncEntries(ctx context.Context, peerInfo peer.AddrInfo, entCid cid.Cid, options ...SyncOption) error {
	opts := getSyncOpts(options)
	if opts.blockHook == nil {
		opts.blockHook = s.generalBlockHook
	}

	// If a scoped depth limit is specified, then create a new entries selector
	// for that depth limit. Otherwise, use the existing entries selector that
	// has the entries depth limit built in.
	selectorEnts := s.selectorEnts
	if opts.depthLimit != 0 {
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		selectorEnts = ssb.ExploreRecursive(recursionLimit(opts.depthLimit),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Next", ssb.ExploreRecursiveEdge())
			})).Node()
	}

	return s.syncEntries(ctx, peerInfo, entCid, selectorEnts, opts.blockHook, s.segDepthLimit)
}

func (s *Subscriber) SyncHAMTEntries(ctx context.Context, peerInfo peer.AddrInfo, entCid cid.Cid, options ...SyncOption) error {
	opts := getSyncOpts(options)
	if opts.blockHook == nil {
		opts.blockHook = s.generalBlockHook
	}

	return s.syncEntries(ctx, peerInfo, entCid, s.selectorAll, opts.blockHook, -1)
}

func (s *Subscriber) syncEntries(ctx context.Context, peerInfo peer.AddrInfo, entCid cid.Cid, sel ipld.Node, bh BlockHookFunc, segdl int64) error {
	if entCid == cid.Undef {
		log.Infow("No entries to sync", "peer", peerInfo.ID)
		return nil
	}

	s.expSyncMutex.Lock()
	if s.expSyncClosed {
		s.expSyncMutex.Unlock()
		return errors.New("shutdown")
	}
	s.expSyncWG.Add(1)
	s.expSyncMutex.Unlock()
	defer s.expSyncWG.Done()

	peerInfo = mautil.CleanPeerAddrInfo(peerInfo)
	peerInfo, err := removeIDFromAddrs(peerInfo)
	if err != nil {
		return err
	}

	hnd := s.getOrCreateHandler(peerInfo.ID)

	syncer, _, err := hnd.makeSyncer(peerInfo, false)
	if err != nil {
		return err
	}

	log.Debugw("Start entries sync", "peer", peerInfo.ID, "cid", entCid)

	_, err = hnd.handle(ctx, entCid, sel, syncer, bh, segdl, cid.Undef)
	if err != nil {
		return fmt.Errorf("sync handler failed: %w", err)
	}

	return nil
}

func removeIDFromAddrs(peerInfo peer.AddrInfo) (peer.AddrInfo, error) {
	// Chop off the p2p ID from any of the addresses.
	for i, peerAddr := range peerInfo.Addrs {
		peerAddr, pid := peer.SplitAddr(peerAddr)
		if pid != "" {
			peerInfo.Addrs[i] = peerAddr
			if peerInfo.ID == "" {
				peerInfo.ID = pid
			}
		}
	}
	if peerInfo.ID == "" {
		return peerInfo, errors.New("empty peer id")
	}
	return peerInfo, nil
}

// distributeEvents reads a SyncFinished, sent by a peer handler, and copies
// the even to all channels in outEventsChans. This delivers the SyncFinished
// to all OnSyncFinished channel readers.
func (s *Subscriber) distributeEvents() {
	var outEventsChans []chan<- SyncFinished

	for {
		select {
		case event, ok := <-s.inEvents:
			if !ok {
				// Dismiss any event readers.
				for _, ch := range outEventsChans {
					close(ch)
				}
				return
			}
			// Send update to all change notification channels.
			for _, ch := range outEventsChans {
				ch <- event
			}
		case ch := <-s.addEventChan:
			outEventsChans = append(outEventsChans, ch)
		case ch := <-s.rmEventChan:
			for i, ca := range outEventsChans {
				if ca == ch {
					outEventsChans[i] = outEventsChans[len(outEventsChans)-1]
					outEventsChans[len(outEventsChans)-1] = nil
					outEventsChans = outEventsChans[:len(outEventsChans)-1]
					close(ch)
					break
				}
			}
		}
	}
}

// getOrCreateHandler returns an existing handler or creates a new one for the
// specified peer (publisher).
func (s *Subscriber) getOrCreateHandler(peerID peer.ID) *handler {
	expires := time.Now().Add(s.idleHandlerTTL)

	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()

	// Check for existing handler, return if found.
	hnd, ok := s.handlers[peerID]
	if ok {
		hnd.expires = expires
	} else {
		hnd = &handler{
			subscriber: s,
			peerID:     peerID,
			expires:    expires,
		}
		s.handlers[peerID] = hnd
	}

	return hnd
}

// idleHandlerCleaner periodically looks for idle handlers to remove. This
// prevents accumulation of handlers that are no longer in use.
func (s *Subscriber) idleHandlerCleaner() {
	t := time.NewTimer(s.idleHandlerTTL)

	for {
		select {
		case now := <-t.C:
			s.handlersMutex.Lock()
			for pid, hnd := range s.handlers {
				if now.After(hnd.expires) {
					delete(s.handlers, pid)
					log.Debugw("Removed idle handler", "peer", pid)
				}
			}
			s.handlersMutex.Unlock()
			t.Reset(s.idleHandlerTTL)
		case <-s.closing:
			t.Stop()
			return
		}
	}
}

// watch fetches announce messages from the Reciever.
func (s *Subscriber) watch() {
	defer close(s.watchDone)

	// Cancel any pending messages if this function exits.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		amsg, err := s.receiver.Next(context.Background())
		if err != nil {
			// This is a normal result of shutting down the Receiver.
			log.Infow("Done handling announce messages", "reason", err)
			break
		}

		hnd := s.getOrCreateHandler(amsg.PeerID)

		// Set the message to be handled by the waiting goroutine.
		oldMsg := hnd.pendingMsg.Swap(&amsg)
		// If rhw previous pending message was not nil, then there is an
		// existing request to sync the ad chain.
		if oldMsg != nil {
			log.Infow("Pending announce replaced by new", "previous_cid", oldMsg.Cid, "new_cid", amsg.Cid, "peer", hnd.peerID)
			continue
		}

		// If old message is nil, then any previous message is already handled.
		// Start a new goroutine to handle this message.
		s.asyncWG.Add(1)
		go func() {
			// Wait for any previous asyncSyncAdChain to finish before removing the
			// latest pending messaged and reducing the available items in the sync
			// semaphore.
			hnd.asyncMutex.Lock()
			defer hnd.asyncMutex.Unlock()
			if s.syncSem != nil {
				select {
				case s.syncSem <- struct{}{}:
					defer func() {
						<-s.syncSem
					}()
				case <-ctx.Done():
				}
			}
			hnd.asyncSyncAdChain(ctx)
			s.asyncWG.Done()
		}()
	}
}

// Announce handles a direct announce message, that was not received over
// pubsub. The message is resent over pubsub, if the Receiver is configured to
// do so. The peerID and addrs are those of the advertisement publisher, since
// an announce message announces the availability of an advertisement and where
// to retrieve it from.
func (s *Subscriber) Announce(ctx context.Context, nextCid cid.Cid, peerInfo peer.AddrInfo) error {
	if s.receiver == nil {
		return nil
	}
	return s.receiver.Direct(ctx, nextCid, peerInfo)
}

// delNotPresent removes from the peerStore the peer's multiaddrs that are not
// present in the specified addrs slice.
func delNotPresent(peerStore peerstore.Peerstore, peerID peer.ID, addrs []multiaddr.Multiaddr) {
	var del []multiaddr.Multiaddr
	oldAddrs := peerStore.Addrs(peerID)
	for _, old := range oldAddrs {
		keep := false
		for _, new := range addrs {
			if old.Equal(new) {
				keep = true
				break
			}
		}
		if !keep {
			del = append(del, old)
		}
	}
	if len(del) != 0 {
		peerStore.SetAddrs(peerID, del, 0)
	}
}

func (h *handler) makeSyncer(peerInfo peer.AddrInfo, doUpdate bool) (Syncer, func(), error) {
	s := h.subscriber

	// Check for an HTTP address in peerAddrs, or if not given, in the http
	// peerstore.
	var httpAddrs []multiaddr.Multiaddr
	if len(peerInfo.Addrs) == 0 {
		httpAddrs = s.httpPeerstore.Addrs(peerInfo.ID)
	} else {
		httpAddrs = mautil.FindHTTPAddrs(peerInfo.Addrs)
		if doUpdate && len(httpAddrs) != 0 {
			delNotPresent(s.httpPeerstore, peerInfo.ID, httpAddrs)
		}
	}

	var update func()
	if len(httpAddrs) != 0 {
		// Store this http address so that future calls to sync will work without a
		// peerAddr (given that it happens within the TTL)
		s.httpPeerstore.AddAddrs(peerInfo.ID, httpAddrs, tempAddrTTL)
		httpPeerInfo := peer.AddrInfo{
			ID:    peerInfo.ID,
			Addrs: httpAddrs,
		}
		if h.syncer == nil || !h.syncer.SameAddrs(httpAddrs) {
			syncer, err := s.ipniSync.NewSyncer(httpPeerInfo)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot create ipni-sync handler: %w", err)
			}
			h.syncer = syncer
		}
		if doUpdate {
			update = func() {
				// Store http address so that future calls to sync will work
				// without a peerAddr (given that it happens within the TTL)
				s.httpPeerstore.AddAddrs(peerInfo.ID, httpAddrs, s.addrTTL)
			}
		}
		return h.syncer, update, nil
	}
	if doUpdate {
		peerStore := s.host.Peerstore()
		if peerStore != nil && len(peerInfo.Addrs) != 0 {
			delNotPresent(peerStore, peerInfo.ID, peerInfo.Addrs)
			// Add it to peerstore with a small TTL first, and extend it if/when
			// sync with it completes. In case the peerstore already has this
			// address and the existing TTL is greater than this temp one, this is
			// a no-op. In other words, the TTL is never decreased here.
			peerStore.AddAddrs(peerInfo.ID, peerInfo.Addrs, tempAddrTTL)
			update = func() {
				peerStore.AddAddrs(peerInfo.ID, peerInfo.Addrs, s.addrTTL)
			}
		} else {
			update = func() {}
		}
	}

	if h.syncer == nil || !h.syncer.SameAddrs(peerInfo.Addrs) {
		syncer, err := s.ipniSync.NewSyncer(peerInfo)
		if err != nil {
			if errors.Is(err, ipnisync.ErrNoHTTPServer) {
				err = fmt.Errorf("data-transfer sync is not supported: %w", err)
			}
			return nil, nil, fmt.Errorf("cannot create ipni-sync handler: %w", err)
		}
		h.syncer = syncer
	}
	return h.syncer, update, nil
}

// asyncSyncAdChain processes the latest announce message received over pubsub
// or HTTP. This functions runs in an independent goroutine, with no more than
// one goroutine per advertisement publisher.
func (h *handler) asyncSyncAdChain(ctx context.Context) {
	if ctx.Err() != nil {
		log.Warnw("Abandoned pending sync", "err", ctx.Err(), "peer", h.peerID)
		return
	}

	// Get the latest pending message.
	amsg := h.pendingMsg.Swap(nil)

	adsDepthLimit := h.subscriber.adsDepthLimit
	nextCid := amsg.Cid
	latestSyncLink := h.subscriber.GetLatestSync(h.peerID)
	var stopAtCid cid.Cid
	if latestSyncLink != nil {
		stopAtCid = latestSyncLink.(cidlink.Link).Cid
		if stopAtCid == nextCid {
			log.Infow("CID to sync to is the stop node. Nothing to do.", "peer", h.peerID)
			return
		}
	} else if h.subscriber.firstSyncDepth != 0 {
		// If nothing synced yet, use first sync depth if configured.
		adsDepthLimit = recursionLimit(h.subscriber.firstSyncDepth)
	}

	peerInfo := peer.AddrInfo{
		ID:    amsg.PeerID,
		Addrs: amsg.Addrs,
	}
	syncer, updatePeerstore, err := h.makeSyncer(peerInfo, true)
	if err != nil {
		log.Errorw("Cannot make syncer for announce", "err", err, "peer", h.peerID)
		return
	}

	sel := ExploreRecursiveWithStopNode(adsDepthLimit, h.subscriber.adsSelectorSeq, latestSyncLink)
	syncCount, err := h.handle(ctx, nextCid, sel, syncer, h.subscriber.generalBlockHook, h.subscriber.segDepthLimit, stopAtCid)
	if err != nil {
		// Failed to handle the sync, so allow another announce for the same CID.
		if h.subscriber.receiver != nil {
			h.subscriber.receiver.UncacheCid(nextCid)
		}
		log.Errorw("Cannot process message", "err", err, "peer", h.peerID)
		h.subscriber.inEvents <- SyncFinished{
			Cid:    nextCid,
			PeerID: h.peerID,
			Err:    err,
		}
		return
	}
	updatePeerstore()
	h.sendSyncFinishedEvent(nextCid, syncCount)
}

var _ SegmentSyncActions = (*segmentedSync)(nil)

type (
	// SegmentSyncActions allows the user to control the flow of segmented sync
	// by either choosing which CID should be synced in the next sync cycle or
	// setting the error that should mark the sync as failed.
	SegmentSyncActions interface {
		// SetNextSyncCid sets the cid that will be synced in the next
		// segmented sync. Note that the last call to this function during a
		// segmented sync cycle dictates which CID will be synced in the next
		// cycle.
		//
		// At least one call to this function must be made for the segmented
		// sync cycles to continue. Because, otherwise the CID that should be
		// used in the next segmented sync cycle cannot be known.
		//
		// If no calls are made to this function or next CID is set to
		// cid.Undef, the sync will terminate and any CIDs that are synced so
		// far will be included in a SyncFinished event.
		SetNextSyncCid(cid.Cid)

		// FailSync fails the sync and returns the given error as soon as the
		// current segment sync finishes. The last call to this function during
		// a segmented sync cycle dictates the error value. Passing nil as
		// error will cancel sync failure.
		FailSync(error)
	}
	// SegmentBlockHookFunc is called for each synced block, similarly to
	// BlockHookFunc. Except that it provides SegmentSyncActions to the hook
	// allowing the user to control the flow of segmented sync by determining
	// which CID should be used in the next segmented sync cycle by decoding
	// the synced block.
	//
	// SegmentSyncActions also allows the user to signal any errors that may
	// occur during the hook execution to terminate the sync and mark it as
	// failed.
	SegmentBlockHookFunc func(peer.ID, cid.Cid, SegmentSyncActions)
	segmentedSync        struct {
		nextSyncCid *cid.Cid
		err         error
	}
)

// SetNextSyncCid sets the CID that will be synced in the next segmented sync.
func (ss *segmentedSync) SetNextSyncCid(c cid.Cid) {
	ss.nextSyncCid = &c
}

// FailSync fails the sync and returns the given error when the current segment
// sync finishes.
func (ss *segmentedSync) FailSync(err error) {
	ss.err = err
}

func (ss *segmentedSync) reset() {
	ss.nextSyncCid = nil
	ss.err = nil
}

func (h *handler) sendSyncFinishedEvent(c cid.Cid, count int) {
	h.subscriber.latestSyncHandler.setLatestSync(h.peerID, c)
	h.subscriber.inEvents <- SyncFinished{
		Cid:    c,
		PeerID: h.peerID,
		Count:  count,
	}
}

// handle processes a message from the peer that the handler is responsible for.
func (h *handler) handle(ctx context.Context, nextCid cid.Cid, sel ipld.Node, syncer Syncer, bh BlockHookFunc, segdl int64, stopAtCid cid.Cid) (int, error) {
	log := log.With("cid", nextCid, "peer", h.peerID)

	segSync := &segmentedSync{
		nextSyncCid: &nextCid,
	}

	var syncedCount int
	hook := func(p peer.ID, c cid.Cid) {
		syncedCount++
		if bh != nil {
			bh(p, c, segSync)
		}
	}

	// Wait for any previous sync for this peer ID to finish. This is necessary
	// to protect the scopedBlockHook map from having having another hook
	// mapped to this peer ID.
	h.syncMutex.Lock()
	h.subscriber.scopedBlockHookMutex.Lock()
	h.subscriber.scopedBlockHook[h.peerID] = hook
	h.subscriber.scopedBlockHookMutex.Unlock()
	defer func() {
		h.subscriber.scopedBlockHookMutex.Lock()
		delete(h.subscriber.scopedBlockHook, h.peerID)
		h.subscriber.scopedBlockHookMutex.Unlock()
		h.syncMutex.Unlock()
	}()

	var syncBySegment bool
	var origLimit selector.RecursionLimit
	// Only attempt to detect recursion limit in original selector if maximum
	// segment depth is larger than zero and there is a block hook set; either
	// general or scoped.
	//
	// Not that we need at least one block hook to let the caller decide which
	// CID to sync in next segment. Therefore, it has to be set for segmented
	// sync to function correctly.
	if segdl > 0 && bh != nil {
		origLimit, syncBySegment = getRecursionLimit(sel)
		// Do not sync using segments if the depth limit in the selector is
		// already less than the configured maximum segment depth limit.
		if syncBySegment &&
			origLimit.Mode() == selector.RecursionLimit_Depth &&
			origLimit.Depth() <= segdl {
			syncBySegment = false
		}
	}

	// Revert back to sync without segmentation if original limit was not
	// detected, due to:
	// - segment depth limit being negative; meaning segmentation is explicitly
	//   disabled, or
	// - no block hook is configured; meaning we don't have a way to determine
	//   next CID during segmented sync,
	// - original selector does not explore recursively, and therefore, has
	//   no top level recursion limit, or
	// - original selector has a recursion depth limit that is already less
	//   than the maximum segment depth limit.
	if !syncBySegment {
		log.Debug("Starting non-segmented sync")
		err := syncer.Sync(ctx, nextCid, sel)
		if err != nil {
			return 0, err
		}
		log.Debugw("Non-segmented sync completed", "syncedCount", syncedCount)
		return syncedCount, nil
	}

	var nextDepth = segdl
	var depthSoFar int64

	log.Debug("Starting segmented sync")
SegSyncLoop:
	for {
		segmentSel, ok := withRecursionLimit(sel, selector.RecursionLimitDepth(nextDepth))
		if !ok {
			// This should not happen if we were able to extract origLimit from
			// sel. If this happens there is likely a bug. Fail fast.
			return 0, fmt.Errorf("failed to construct segment selector with recursion depth limit of %d", nextDepth)
		}
		nextCid = *segSync.nextSyncCid
		segSync.reset()
		err := syncer.Sync(ctx, nextCid, segmentSel)
		if err != nil {
			return 0, err
		}
		depthSoFar += nextDepth

		if segSync.err != nil {
			return 0, segSync.err
		}

		// If hook action is not called, or next CID is set to cid.Undef then break out of the
		// segmented sync cycle.
		if segSync.nextSyncCid == nil || segSync.nextSyncCid.Equals(cid.Undef) {
			break
		}

		if stopAtCid != cid.Undef && *segSync.nextSyncCid == stopAtCid {
			log.Debugw("Reached stop node in segmented sync; stopping.")
			break
		}

		switch origLimit.Mode() {
		case selector.RecursionLimit_None:
			continue
		case selector.RecursionLimit_Depth:
			if depthSoFar >= origLimit.Depth() {
				break SegSyncLoop
			}
			remainingDepth := origLimit.Depth() - depthSoFar
			if remainingDepth < segdl {
				nextDepth = remainingDepth
			}
		default:
			return 0, fmt.Errorf("unknown recursion limit mode: %v", origLimit.Mode())
		}
	}

	log.Debugw("Segmented sync completed", "syncedCount", syncedCount)
	return syncedCount, nil
}
