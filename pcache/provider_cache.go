package pcache

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("pcache")

var ErrClosed = errors.New("cache closed")

// ProviderSource in an interface that the cache uses to fetch provider
// information for one or all providers from a specific supplier of that
// information. The cache can be configured with any number of sources that
// supply provider information.
type ProviderSource interface {
	Fetch(context.Context, peer.ID) (*model.ProviderInfo, error)
	FetchAll(context.Context) ([]*model.ProviderInfo, error)
}

// CtxExtendedInfo contains cached contextual extended provider information.
type CtxExtendedInfo struct {
	Override  bool
	Providers []peer.AddrInfo
	Metadatas [][]byte
}

// ExtendedInfo contains cached extended provider information.
type ExtendedInfo struct {
	CtxExtended map[string]CtxExtendedInfo
	Providers   []peer.AddrInfo
	Metadatas   [][]byte
}

// ProviderInfo contains cached provider information.
type ProviderInfo struct {
	AddrInfo peer.AddrInfo
	Extended *ExtendedInfo
}

type cacheInfo struct {
	addrInfo   peer.AddrInfo
	extended   *model.ExtendedProviders
	expiresAt  time.Time
	lastUpdate time.Time
	seq        uint
	updateSeq  uint
}

type refreshReq struct {
	ctx      context.Context
	pid      peer.ID
	response chan *ProviderInfo
}

// ProviderCache is a lock-free provider information cache for high-performance
// concurrent reads.
type ProviderCache struct {
	read        atomic.Pointer[readOnly]
	sources     []ProviderSource
	stop        context.CancelFunc
	stopped     chan struct{}
	refreshReqs chan *refreshReq

	updatesLastRefresh atomic.Uint32
}

// readOnly is an immutable struct stored atomically in the cache read field.
// It contains two maps of provider ID to serialized provider info. The m map
// is the main cache data and the u map contains updates that have not yet been
// moved into the main map. The reason for the u map is so that a small number
// of updates do not cause the entire main map to be regenerated.
type readOnly struct {
	m map[peer.ID]*ProviderInfo
	u map[peer.ID]*ProviderInfo
}

// New creates a new provider cache.
func New(options ...Option) (*ProviderCache, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if len(opts.sources) == 0 {
		return nil, errors.New("no provider information sources")
	}

	pc := &ProviderCache{
		sources:     opts.sources,
		stopped:     make(chan struct{}),
		refreshReqs: make(chan *refreshReq),
	}

	var runCtx context.Context
	runCtx, pc.stop = context.WithCancel(context.Background())
	go pc.run(runCtx, opts.preload, opts.refreshIn, opts.ttl)

	if opts.preload {
		pc.refreshReqs <- nil
	}

	return pc, nil
}

// Get returns the provider information for the provider specified by pid. If
// provider information is not available, then a nil slice is returned. An
// error results from the context being canceled or the cache closing.
//
// Do not modify values in the returned ProviderInfo.
func (pc *ProviderCache) Get(ctx context.Context, pid peer.ID) (*ProviderInfo, error) {
	read := pc.loadReadOnly()

	pinfo, ok := read.u[pid]
	if !ok {
		pinfo, ok = read.m[pid]
		if !ok {
			// Cache miss.
			return pc.refresh(ctx, pid, true)
		}
	}
	// Cache hit.
	return pinfo, nil
}

// GetResults retrieves information about the provicer specified by pid and
// composes a slice ProviderResults got the provider. If provider information
// is not available, then a nil slice is returned. An error results from the
// context being canceled or the cache closing.
func (pc *ProviderCache) GetResults(ctx context.Context, pid peer.ID, ctxID, metadata []byte) ([]model.ProviderResult, error) {
	pinfo, err := pc.Get(ctx, pid)
	if err != nil {
		// Context canceled or cache closed.
		return nil, err
	}
	// Could not fetch info for specified provider.
	if pinfo == nil {
		return nil, nil
	}

	var results []model.ProviderResult
	results = append(results, model.ProviderResult{
		ContextID: ctxID,
		Metadata:  metadata,
		Provider:  &pinfo.AddrInfo,
	})

	// return results if there are no further extended providers to unpack
	if pinfo.Extended == nil {
		return results, nil
	}

	// If override is set to true at the context level then the chain
	// level EPs should be ignored for this context ID
	var override bool

	// Adding context-level EPs if they exist
	ctxExtended, ok := pinfo.Extended.CtxExtended[string(ctxID)]
	if ok {
		override = ctxExtended.Override
		for i, xpinfo := range ctxExtended.Providers {
			xmd := ctxExtended.Metadatas[i]
			// Skippng the main provider's record if its metadata is nil or is
			// the same as the one retrieved from the indexer, because such EP
			// record does not advertise any new protocol.
			if xpinfo.ID == pid && (len(xmd) == 0 || bytes.Equal(xmd, metadata)) {
				continue
			}
			// Use metadata from advertisement if one hasn't been specified for
			// the extended provider
			if xmd == nil {
				xmd = metadata
			}
			xpinfo := xpinfo
			results = append(results, model.ProviderResult{
				ContextID: ctxID,
				Metadata:  xmd,
				Provider:  &xpinfo,
			})
		}
	}

	// If override is true then do not include chain-level EPs
	if override {
		return results, nil
	}

	// Adding chain-level EPs if such exist
	for i, xpinfo := range pinfo.Extended.Providers {
		xmd := pinfo.Extended.Metadatas[i]
		// Skippng the main provider's record if its metadata is nil or is the
		// same as the one retrieved from the indexer, because such EP record
		// does not advertise any new protocol.
		if xpinfo.ID == pid && (len(xmd) == 0 || bytes.Equal(xmd, metadata)) {
			continue
		}
		// Use metadata from advertisement if one hasn't been specified for the
		// extended provider
		if len(xmd) == 0 {
			xmd = metadata
		}
		xpinfo := xpinfo
		results = append(results, model.ProviderResult{
			ContextID: ctxID,
			Metadata:  xmd,
			Provider:  &xpinfo,
		})
	}

	return results, nil
}

// Close stops the cache refresh goroutine.
func (pc *ProviderCache) Close() {
	pc.stop()
	<-pc.stopped
}

// ForceRefresh initiates an immediate cache refresh.
func (pc *ProviderCache) ForceRefresh(ctx context.Context, wait bool) error {
	_, err := pc.refresh(ctx, "", wait)
	return err
}

func (pc *ProviderCache) Len() int {
	read := pc.loadReadOnly()
	return len(read.m) + len(read.u)
}

// UpdatesLastRefresh returns the number of updates seen by the last refresh.
func (pc *ProviderCache) UpdatesLastRefresh() int {
	return int(pc.updatesLastRefresh.Load())
}

func (pc *ProviderCache) refresh(ctx context.Context, pid peer.ID, wait bool) (*ProviderInfo, error) {
	req := &refreshReq{
		ctx: ctx,
		pid: pid,
	}
	if wait {
		req.response = make(chan *ProviderInfo, 1)
	}

	select {
	case pc.refreshReqs <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pc.stopped:
		return nil, ErrClosed
	}

	if !wait {
		return nil, nil
	}

	select {
	case pinfo := <-req.response:
		return pinfo, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pc.stopped:
		return nil, ErrClosed
	}
}

func (pc *ProviderCache) run(ctx context.Context, preload bool, refreshIn, ttl time.Duration) {
	var seq uint
	write := make(map[peer.ID]*cacheInfo)

	if preload {
		pc.refreshAll(ctx, ttl, seq, write)
	}

	t := time.NewTimer(refreshIn)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			seq++
			pc.refreshAll(ctx, ttl, seq, write)
			t.Reset(refreshIn)
		case req := <-pc.refreshReqs:
			if req == nil {
				continue
			}
			if req.ctx != nil && req.ctx.Err() != nil {
				continue
			}
			if req.pid == peer.ID("") {
				seq++
				pc.refreshAll(ctx, ttl, seq, write)
				if req.response != nil {
					close(req.response)
				}
				continue
			}
			pinfo := pc.refreshOne(ctx, req.pid, ttl, seq, write)
			if req.response != nil {
				req.response <- pinfo
			}
		case <-ctx.Done():
			close(pc.stopped)
			return
		}
	}
}

func (pc *ProviderCache) loadReadOnly() readOnly {
	if p := pc.read.Load(); p != nil {
		return *p
	}
	return readOnly{}
}

func (pc *ProviderCache) refreshOne(ctx context.Context, pid peer.ID, ttl time.Duration, seq uint, write map[peer.ID]*cacheInfo) *ProviderInfo {
	_, ok := write[pid]
	if ok {
		// Stored by previous request.
		read := pc.loadReadOnly()
		pinfo, ok := read.u[pid]
		if ok {
			return pinfo
		}
		pinfo, ok = read.m[pid]
		if ok {
			return pinfo
		}
		// This should never happen. Appropriate to panic here.
		log.Errorw("Cached data not found in read-only data", "provider", pid)
		delete(write, pid)
	}

	cinfo := &cacheInfo{
		seq:       seq,
		updateSeq: seq,
	}

	for _, src := range pc.sources {
		fetchedInfo, err := src.Fetch(ctx, pid)
		if err != nil {
			log.Warnw("Cannot fetch provider info", "err", err, "source", src)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			continue
		}
		if fetchedInfo == nil {
			continue
		}
		lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
		if !lastUpdate.After(cinfo.lastUpdate) {
			// Skip - not newer that what is here already.
			continue
		}
		cinfo.lastUpdate = lastUpdate
		cinfo.addrInfo = fetchedInfo.AddrInfo
		cinfo.extended = fetchedInfo.ExtendedProviders
	}
	if len(cinfo.addrInfo.Addrs) == 0 {
		// No provider info, cache negative entry.
		cinfo.expiresAt = time.Now().Add(ttl)
	}
	write[pid] = cinfo

	read := pc.loadReadOnly()

	// Regenerate and add to extended read map.
	updates := make(map[peer.ID]*ProviderInfo, len(read.u)+1)
	for id, pinfo := range read.u {
		updates[id] = pinfo
	}
	pinfo := apiToCacheInfo(cinfo.addrInfo, cinfo.extended)
	updates[pid] = pinfo

	// If the update map is small relative to the main map, do not generate a
	// new main map yet.
	if !needMerge(len(updates), len(read.m)) {
		pc.read.Store(&readOnly{m: read.m, u: updates})
		return pinfo
	}

	// Generate main map.
	m := make(map[peer.ID]*ProviderInfo, len(write))
	for pid := range write {
		pinfo, ok := updates[pid]
		if !ok {
			pinfo = read.m[pid]
		}
		m[pid] = pinfo
	}

	// Replace old readOnly map with new.
	pc.read.Store(&readOnly{m: m})

	return pinfo
}

func (pc *ProviderCache) refreshAll(ctx context.Context, ttl time.Duration, seq uint, write map[peer.ID]*cacheInfo) {
	for _, src := range pc.sources {
		// Get provider info from each source.
		fetchedInfos, err := src.FetchAll(ctx)
		if err != nil {
			log.Errorw("cannot fetch provider info", "err", err, "source", src)
			if errors.Is(err, context.Canceled) {
				return
			}
			continue
		}

		// Collect latest info on each provider.
		for _, fetchedInfo := range fetchedInfos {
			pid := fetchedInfo.AddrInfo.ID
			cinfo, ok := write[pid]
			if !ok {
				// Fetched new provider information, add it to cache.
				lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
				write[pid] = &cacheInfo{
					addrInfo:   fetchedInfo.AddrInfo,
					extended:   fetchedInfo.ExtendedProviders,
					lastUpdate: lastUpdate,
					seq:        seq,
					updateSeq:  seq,
				}
				continue
			}

			cinfo.seq = seq // provider is still present
			cinfo.expiresAt = time.Time{}

			lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
			if !lastUpdate.After(cinfo.lastUpdate) {
				// Skip - not newer than what is here already.
				continue
			}
			cinfo.lastUpdate = lastUpdate

			if maddrsEqual(cinfo.addrInfo.Addrs, fetchedInfo.AddrInfo.Addrs) &&
				extendedEqual(cinfo.extended, fetchedInfo.ExtendedProviders) {
				// Skip - addresses and extended info still the same.
				continue
			}
			// Addresses or entended info changed.
			cinfo.addrInfo = fetchedInfo.AddrInfo
			cinfo.extended = fetchedInfo.ExtendedProviders
			cinfo.updateSeq = seq // updated provider info
		}
	}

	read := pc.loadReadOnly()

	// Shallow-copy update map.
	updates := make(map[peer.ID]*ProviderInfo, len(read.u))
	for pid, pinfo := range read.u {
		updates[pid] = pinfo
	}

	var updateCount int
	for pid, cinfo := range write {
		if cinfo.seq != seq {
			// Provider no longer present.
			now := time.Now()
			if cinfo.expiresAt.IsZero() {
				// Set removal timer for dead provider.
				cinfo.expiresAt = now.Add(ttl)
			} else if now.After(cinfo.expiresAt) {
				// Dead provider or negative cache entry expired, remove from
				// cache.
				delete(write, pid)
				// Store nil in updates to override anything in main map.
				updates[pid] = nil
				updateCount++
			}
		} else if cinfo.updateSeq == seq {
			// Address updated, update read-only data.
			updates[pid] = apiToCacheInfo(cinfo.addrInfo, cinfo.extended)
			updateCount++
		}
	}

	pc.updatesLastRefresh.Store(uint32(updateCount))

	// If the update map is small relative to the main map, do not generate a
	// new main map yet.
	if !needMerge(len(updates), len(read.m)) {
		pc.read.Store(&readOnly{m: read.m, u: updates})
		return
	}

	// Generate main map.
	m := make(map[peer.ID]*ProviderInfo, len(write))
	for pid := range write {
		pinfo, ok := updates[pid]
		if !ok {
			pinfo = read.m[pid]
		}
		m[pid] = pinfo
	}

	// Replace old readOnly map with new.
	pc.read.Store(&readOnly{m: m})
}

func apiToCacheInfo(addrInfo peer.AddrInfo, extProviders *model.ExtendedProviders) *ProviderInfo {
	// Return nil if this is a negative cache entry.
	if len(addrInfo.Addrs) == 0 {
		return nil
	}

	pinfo := &ProviderInfo{
		AddrInfo: addrInfo,
	}

	if extProviders != nil {
		pinfo.Extended = &ExtendedInfo{
			Providers: extProviders.Providers,
			Metadatas: extProviders.Metadatas,
		}

		if len(extProviders.Contextual) != 0 {
			cxps := make(map[string]CtxExtendedInfo, len(extProviders.Contextual))
			for _, cxp := range extProviders.Contextual {
				cxps[cxp.ContextID] = CtxExtendedInfo{
					Override:  cxp.Override,
					Providers: cxp.Providers,
					Metadatas: cxp.Metadatas,
				}
			}
			pinfo.Extended.CtxExtended = cxps
		}
	}

	return pinfo
}

// needMerge returns true if update set u should be merged into main set m, to
// maintain the lowest overall cost of applying cache updates. The optimal time
// to merge is when the sum(1..len(u)) > len(m). This is when the cumulative
// cost of iterating u exceeds the cost of iterating m.
//
// For comparisons of different merge calculations, see:
// https://go.dev/play/p/uxROTy8NxIk
func needMerge(u, m int) bool {
	return u*(u+1) > m*2
}

func maddrsEqual(a, b []multiaddr.Multiaddr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func extendedEqual(a, b *model.ExtendedProviders) bool {
	if a == b {
		return true
	}
	if len(a.Providers) != len(b.Providers) {
		return false
	}
	if len(a.Metadatas) != len(b.Metadatas) {
		return false
	}
	if len(a.Contextual) != len(b.Contextual) {
		return false
	}

	for i := range a.Providers {
		if a.Providers[i].ID != b.Providers[i].ID {
			return false
		}
		if !maddrsEqual(a.Providers[i].Addrs, b.Providers[i].Addrs) {
			return false
		}
	}
	for i := range a.Metadatas {
		if !bytes.Equal(a.Metadatas[i], b.Metadatas[i]) {
			return false
		}
	}
	for i, ctxA := range a.Contextual {
		ctxB := a.Contextual[i]
		if ctxA.Override != ctxB.Override {
			return false
		}
		if ctxA.ContextID != ctxB.ContextID {
			return false
		}
		for j := range ctxA.Providers {
			if ctxA.Providers[j].ID != ctxB.Providers[j].ID {
				return false
			}
			if !maddrsEqual(ctxA.Providers[j].Addrs, ctxB.Providers[j].Addrs) {
				return false
			}
		}
		for j := range ctxA.Metadatas {
			if !bytes.Equal(ctxA.Metadatas[j], ctxB.Metadatas[j]) {
				return false
			}
		}
	}
	return true
}
