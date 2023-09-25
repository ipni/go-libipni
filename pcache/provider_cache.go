package pcache

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("pcache")

var ErrClosed = errors.New("cache closed")

// ProviderSource in an interface that the cache uses to fetch provider
// information. Each ProviderSource is a specific supplier of provider
// information. The cache can be configured with any number of sources that
// supply provider information.
type ProviderSource interface {
	// Fetch gets provider information for a specific provider.
	Fetch(context.Context, peer.ID) (*model.ProviderInfo, error)
	// Fetch gets provider information for all providers.
	FetchAll(context.Context) ([]*model.ProviderInfo, error)
	// String returns a description of the source.
	String() string
}

// ProviderCache is a lock-free provider information cache for high-performance
// concurrent reads.
type ProviderCache struct {
	read    atomic.Pointer[readOnly]
	sources []ProviderSource
	ttl     time.Duration

	seq       uint
	write     map[peer.ID]*cacheInfo
	writeLock chan struct{}

	needsRefresh atomic.Bool
	refreshIn    time.Duration
	refreshTimer *time.Timer
}

// cacheInfo contains writable cache info.
type cacheInfo struct {
	provider   *model.ProviderInfo
	expiresAt  time.Time
	lastUpdate time.Time
	seq        uint
	updateSeq  uint
}

// ctxExtendedInfo contains cached read-only contextual extended provider
// information.
type ctxExtendedInfo struct {
	override  bool
	providers []peer.AddrInfo
	metadatas [][]byte
}

// readProviderInfo contains cached read-only provider information.
type readProviderInfo struct {
	provider    *model.ProviderInfo
	ctxExtended map[string]ctxExtendedInfo
}

// readOnly is an immutable struct stored atomically in the cache read field.
// It contains two maps of provider ID to serialized provider info. The m map
// is the main cache data and the u map contains updates that have not yet been
// moved into the main map. The reason for the u map is so that a small number
// of updates do not cause the entire main map to be regenerated.
type readOnly struct {
	m map[peer.ID]*readProviderInfo
	u map[peer.ID]*readProviderInfo
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
		sources:   opts.sources,
		ttl:       opts.ttl,
		refreshIn: opts.refreshIn,

		write:     make(map[peer.ID]*cacheInfo),
		writeLock: make(chan struct{}, 1),
	}

	if opts.preload {
		_ = pc.Refresh(context.Background())
	}

	if opts.refreshIn != 0 {
		pc.refreshTimer = time.AfterFunc(opts.refreshIn, func() {
			pc.needsRefresh.Store(true)
		})
	}

	return pc, nil
}

func (pc *ProviderCache) Get(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	rpi, err := pc.getReadOnly(ctx, pid)
	if err != nil {
		return nil, err
	}
	if rpi == nil {
		return nil, nil
	}
	return rpi.provider, nil
}

func (pc *ProviderCache) List() []*model.ProviderInfo {
	read := pc.loadReadOnly()
	size := len(read.m) + len(read.u)
	if size == 0 {
		return nil
	}
	m := make(map[peer.ID]*model.ProviderInfo, size)
	for pid, rpi := range read.m {
		if rpi != nil {
			m[pid] = rpi.provider
		}
	}
	for pid, rpi := range read.u {
		if rpi != nil {
			m[pid] = rpi.provider
		} else {
			// Negative cache update; remove from output.
			delete(m, pid)
		}
	}
	pinfos := make([]*model.ProviderInfo, len(m))
	var i int
	for _, pi := range m {
		pinfos[i] = pi
		i++
	}
	return pinfos
}

// GetResults retrieves information about the provider specified by peer ID and
// composes a slice of ProviderResults for the provider and any extended
// providers. If provider information is not available, then a nil slice is
// returned. An error results from the context being canceled or the cache
// closing.
func (pc *ProviderCache) GetResults(ctx context.Context, pid peer.ID, ctxID, metadata []byte) ([]model.ProviderResult, error) {
	rpi, err := pc.getReadOnly(ctx, pid)
	if err != nil {
		// Context canceled or cache closed.
		return nil, err
	}
	// Could not fetch info for specified provider.
	if rpi == nil {
		return nil, nil
	}

	var results []model.ProviderResult
	results = append(results, model.ProviderResult{
		ContextID: ctxID,
		Metadata:  metadata,
		Provider:  &rpi.provider.AddrInfo,
	})

	// Return results if there are no further extended providers to unpack.
	if rpi.provider.ExtendedProviders == nil {
		return results, nil
	}

	// If override is set to true at the context level then the chain
	// level EPs should be ignored for this context ID
	var override bool

	// Adding context-level EPs if they exist
	ctxExtended, ok := rpi.ctxExtended[string(ctxID)]
	if ok {
		override = ctxExtended.override
		for i, xpinfo := range ctxExtended.providers {
			xmd := ctxExtended.metadatas[i]
			// Skipping the main provider's record if its metadata is nil or is
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

	// If override is true then do not include chain-level EPs.
	if override {
		return results, nil
	}

	// Adding chain-level EPs if such exist
	extended := rpi.provider.ExtendedProviders
	for i, xpinfo := range extended.Providers {
		xmd := extended.Metadatas[i]
		// Skipping the main provider's record if its metadata is nil or is the
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

func (pc *ProviderCache) Len() int {
	read := pc.loadReadOnly()
	return len(read.m) + len(read.u)
}

// Refresh initiates an immediate cache refresh.
func (pc *ProviderCache) Refresh(ctx context.Context) error {
	select {
	case pc.writeLock <- struct{}{}:
	default:
		// Refresh already in progress, wait for it to finish.
		select {
		case pc.writeLock <- struct{}{}:
			<-pc.writeLock
		case <-ctx.Done():
		}
		return ctx.Err()
	}
	defer func() {
		<-pc.writeLock
	}()

	pc.seq++
	seq := pc.seq

	for _, src := range pc.sources {
		// Get provider info from each source.
		fetchedInfos, err := src.FetchAll(ctx)
		if err != nil {
			log.Errorw("cannot fetch provider info", "err", err, "source", src)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		// Collect latest info on each provider.
		for _, fetchedInfo := range fetchedInfos {
			pid := fetchedInfo.AddrInfo.ID
			cinfo, ok := pc.write[pid]
			if !ok {
				// Fetched new provider information, add it to cache.
				lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
				pc.write[pid] = &cacheInfo{
					provider:   fetchedInfo,
					lastUpdate: lastUpdate,
					seq:        seq,
					updateSeq:  seq,
				}
				continue
			}

			cinfo.seq = seq // provider is still present
			cinfo.expiresAt = time.Time{}

			lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
			if lastUpdate.IsZero() {
				// Use a non-zero time to prevent unnecessary updates.
				lastUpdate = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
			}
			if !lastUpdate.After(cinfo.lastUpdate) {
				// Skip - not newer than what is here already.
				continue
			}
			// Source has later advertisement in chain, so update cache.
			cinfo.lastUpdate = lastUpdate
			cinfo.provider = fetchedInfo
			cinfo.updateSeq = seq // updated provider info
		}
	}

	read := pc.loadReadOnly()

	// Shallow-copy update map.
	updates := make(map[peer.ID]*readProviderInfo, len(read.u))
	for pid, rpi := range read.u {
		updates[pid] = rpi
	}

	for pid, cinfo := range pc.write {
		if cinfo.seq != seq {
			// Provider no longer present.
			now := time.Now()
			if cinfo.expiresAt.IsZero() {
				// Set removal timer for dead provider.
				cinfo.expiresAt = now.Add(pc.ttl)
			} else if now.After(cinfo.expiresAt) {
				// Dead provider or negative cache entry expired, remove from
				// cache.
				delete(pc.write, pid)
				// Store nil in updates to override anything in main map.
				updates[pid] = nil
			}
		} else if cinfo.updateSeq == seq {
			// Address updated, update read-only data.
			updates[pid] = apiToCacheInfo(cinfo.provider)
		}
	}

	// If the update map is small relative to the main map, do not generate a
	// new main map yet.
	if !needMerge(len(updates), len(read.m)) {
		pc.read.Store(&readOnly{m: read.m, u: updates})
		return nil
	}

	// Generate main map.
	m := make(map[peer.ID]*readProviderInfo, len(pc.write))
	for pid := range pc.write {
		rpi, ok := updates[pid]
		if !ok {
			rpi = read.m[pid]
		}
		m[pid] = rpi
	}

	// Replace old readOnly map with new.
	pc.read.Store(&readOnly{m: m})
	return nil
}

// get returns the provider information for the provider specified by id. If
// provider information is not available, then a nil slice is returned. An
// error results from the context being canceled or the cache closing.
//
// Do not modify values in the returned readProviderInfo.
func (pc *ProviderCache) getReadOnly(ctx context.Context, pid peer.ID) (*readProviderInfo, error) {
	read := pc.loadReadOnly()

	rpi, ok := read.u[pid]
	if !ok {
		rpi, ok = read.m[pid]
		if !ok {
			// Cache miss.
			var err error
			rpi, err = pc.fetchMissing(ctx, pid)
			if err != nil {
				return nil, err
			}
		}
	}

	// If a refresh interval defined, and elapsed, then trigger a refresh.
	if pc.refreshTimer != nil && pc.needsRefresh.CompareAndSwap(true, false) {
		go func() {
			pc.Refresh(context.Background())
			pc.refreshTimer.Reset(pc.refreshIn)
		}()
	}

	// Cache hit.
	return rpi, nil
}

func (pc *ProviderCache) loadReadOnly() readOnly {
	if p := pc.read.Load(); p != nil {
		return *p
	}
	return readOnly{}
}

// fetchMissing fetches information about a single provider that is missing
// from the cache. If that information cannot be fetched, then a negative cache
// entry is created.
func (pc *ProviderCache) fetchMissing(ctx context.Context, pid peer.ID) (*readProviderInfo, error) {
	select {
	case pc.writeLock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() {
		<-pc.writeLock
	}()

	seq := pc.seq

	_, ok := pc.write[pid]
	if ok {
		// Stored by previous request.
		read := pc.loadReadOnly()
		rpi, ok := read.u[pid]
		if ok {
			return rpi, nil
		}
		rpi, ok = read.m[pid]
		if ok {
			return rpi, nil
		}
		// This should never happen. Appropriate to panic here.
		log.Errorw("Cached data not found in read-only data", "provider", pid)
		delete(pc.write, pid)
	}

	cinfo := &cacheInfo{
		seq:       seq,
		updateSeq: seq,
	}

	for _, src := range pc.sources {
		fetchedInfo, err := src.Fetch(ctx, pid)
		if err != nil {
			var apiErr *apierror.Error
			if errors.As(err, &apiErr) && apiErr.Status() == http.StatusNotFound {
				continue
			}
			log.Errorw("Cannot fetch provider info", "err", err, "source", src)
			if errors.Is(err, context.Canceled) {
				return nil, ctx.Err()
			}
			continue
		}
		if fetchedInfo == nil {
			continue
		}
		lastUpdate, _ := time.Parse(time.RFC3339, fetchedInfo.LastAdvertisementTime)
		if lastUpdate.IsZero() {
			// Use a non-zero time to prevent unnecessary updates.
			lastUpdate = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
		}
		if !lastUpdate.After(cinfo.lastUpdate) {
			// Skip - not newer that what is here already.
			continue
		}
		cinfo.lastUpdate = lastUpdate
		cinfo.provider = fetchedInfo
	}
	if cinfo.provider == nil {
		// No provider info, cache negative entry.
		cinfo.expiresAt = time.Now().Add(pc.ttl)
		log.Infow("Provider info not found at any source", "provider", pid)
	}
	pc.write[pid] = cinfo

	read := pc.loadReadOnly()

	// Regenerate and add to extended read map.
	updates := make(map[peer.ID]*readProviderInfo, len(read.u)+1)
	for id, rpi := range read.u {
		updates[id] = rpi
	}
	rpinfo := apiToCacheInfo(cinfo.provider)
	updates[pid] = rpinfo

	// If the update map is small relative to the main map, do not generate a
	// new main map yet.
	if !needMerge(len(updates), len(read.m)) {
		pc.read.Store(&readOnly{m: read.m, u: updates})
		return rpinfo, nil
	}

	// Generate main map.
	m := make(map[peer.ID]*readProviderInfo, len(pc.write))
	for pid := range pc.write {
		rpi, ok := updates[pid]
		if !ok {
			rpi = read.m[pid]
		}
		m[pid] = rpi
	}

	// Replace old readOnly map with new.
	pc.read.Store(&readOnly{m: m})

	return rpinfo, nil
}

func apiToCacheInfo(provider *model.ProviderInfo) *readProviderInfo {
	// Return nil if this is a negative cache entry.
	if provider == nil {
		return nil
	}

	rpinfo := &readProviderInfo{
		provider: provider,
	}

	extProviders := provider.ExtendedProviders
	if extProviders != nil && len(extProviders.Contextual) != 0 {
		cxps := make(map[string]ctxExtendedInfo, len(extProviders.Contextual))
		for _, cxp := range extProviders.Contextual {
			cxps[cxp.ContextID] = ctxExtendedInfo{
				override:  cxp.Override,
				providers: cxp.Providers,
				metadatas: cxp.Metadatas,
			}
		}
		rpinfo.ctxExtended = cxps
	}

	return rpinfo
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
