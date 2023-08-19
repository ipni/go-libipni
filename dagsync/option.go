package dagsync

import (
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// defaultAddrTTL is the default amount of time that addresses discovered
	// from pubsub messages will remain in the peerstore. This is twice the
	// default provider poll interval.
	defaultAddrTTL = 48 * time.Hour
	// defaultIdleHandlerTTL is the default time after which idle publisher
	// handlers are removed.
	defaultIdleHandlerTTL = time.Hour
	// defaultSegDepthLimit disables (-1) segmented sync by default.
	defaultSegDepthLimit = -1
	// Maximum number of in-progress graphsync requests.
	defaultGsMaxInRequests  = 1024
	defaultGsMaxOutRequests = 1024
	// defaultHttpTimeout is time limit for requests made by the HTTP client.
	defaultHttpTimeout = 10 * time.Second
)

type LastKnownSyncFunc func(peer.ID) (cid.Cid, bool)

// config contains all options for configuring Subscriber.
type config struct {
	addrTTL time.Duration

	topic *pubsub.Topic

	blockHook BlockHookFunc

	idleHandlerTTL time.Duration
	lastKnownSync  LastKnownSyncFunc
	maxAsyncSyncs  int

	hasRcvr  bool
	rcvrOpts []announce.Option

	adsDepthLimit     int64
	entriesDepthLimit int64
	segDepthLimit     int64

	gsMaxInRequests  uint64
	gsMaxOutRequests uint64

	strictAdsSelSeq bool

	httpTimeout      time.Duration
	httpRetryMax     int
	httpRetryWaitMin time.Duration
	httpRetryWaitMax time.Duration
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		addrTTL:          defaultAddrTTL,
		httpTimeout:      defaultHttpTimeout,
		idleHandlerTTL:   defaultIdleHandlerTTL,
		segDepthLimit:    defaultSegDepthLimit,
		gsMaxInRequests:  defaultGsMaxInRequests,
		gsMaxOutRequests: defaultGsMaxOutRequests,
		strictAdsSelSeq:  true,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// AddrTTL sets the peerstore address time-to-live for addresses discovered
// from pubsub messages.
func AddrTTL(addrTTL time.Duration) Option {
	return func(c *config) error {
		c.addrTTL = addrTTL
		return nil
	}
}

// Topic provides an existing pubsub topic.
func Topic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}

// HttpTimeout specifies a time limit for HTTP requests made by the sync
// HTTP client. A value of zero means no timeout.
func HttpTimeout(to time.Duration) Option {
	return func(c *config) error {
		c.httpTimeout = to
		return nil
	}
}

// RetryableHTTPClient configures a retriable HTTP client. Setting retryMax to
// zero, the default, disables the retriable client.
func RetryableHTTPClient(retryMax int, waitMin, waitMax time.Duration) Option {
	return func(c *config) error {
		if waitMin > waitMax {
			return errors.New("minimum retry wait time cannot be greater than maximum")
		}
		if retryMax < 0 {
			retryMax = 0
		}
		c.httpRetryMax = retryMax
		c.httpRetryWaitMin = waitMin
		c.httpRetryWaitMax = waitMax
		return nil
	}
}

// BlockHook adds a hook that is run when a block is received via
// Subscriber.Sync along with a SegmentSyncActions to control the sync flow if
// segmented sync is enabled. Note that if segmented sync is disabled, calls on
// SegmentSyncActions will have no effect. See: SegmentSyncActions,
// SegmentDepthLimit, ScopedBlockHook.
func BlockHook(blockHook BlockHookFunc) Option {
	return func(c *config) error {
		c.blockHook = blockHook
		return nil
	}
}

// IdleHandlerTTL configures the time after which idle handlers are removed.
func IdleHandlerTTL(ttl time.Duration) Option {
	return func(c *config) error {
		c.idleHandlerTTL = ttl
		return nil
	}
}

// Checks that advertisement blocks contain a "PreviousID" field. This can be
// set to false to not do the check if there is no reason to do so.
func StrictAdsSelector(strict bool) Option {
	return func(c *config) error {
		c.strictAdsSelSeq = strict
		return nil
	}
}

// AdsDepthLimit sets the maximum number of advertisements in a chain to sync.
// Defaults to unlimited if not specified or set < 1.
func AdsDepthLimit(limit int64) Option {
	return func(c *config) error {
		c.adsDepthLimit = limit
		return nil
	}
}

// EntriesDepthLimit sets the maximum number of multihash entries blocks to
// sync per advertisement. Defaults to unlimited if not set or set to < 1.
func EntriesDepthLimit(depth int64) Option {
	return func(c *config) error {
		c.entriesDepthLimit = depth
		return nil
	}
}

// SegmentDepthLimit sets the maximum recursion depth limit for a segmented sync.
// Setting the depth to a value less than zero disables segmented sync completely.
// Disabled by default.
//
// For segmented sync to function at least one of BlockHook or ScopedBlockHook
// must be set.
func SegmentDepthLimit(depth int64) Option {
	return func(c *config) error {
		c.segDepthLimit = depth
		return nil
	}
}

// RecvAnnounce enables an announcement message receiver.
func RecvAnnounce(opts ...announce.Option) Option {
	return func(c *config) error {
		c.hasRcvr = true
		c.rcvrOpts = opts
		return nil
	}
}

// MaxAsyncConcurrency sets the maximum number of concurrent asynchrouous syncs
// (started by announce messages). This only takes effect if there is an
// announcement receiver configured by the RecvAnnounce option.
func MaxAsyncConcurrency(n int) Option {
	return func(c *config) error {
		if n != 0 {
			if n < 0 {
				n = 0
			}
			c.maxAsyncSyncs = n
		}
		return nil
	}
}

// WithMaxGraphsyncRequests sets the maximum number of in-progress inbound and
// outbound graphsync requests.
func WithMaxGraphsyncRequests(maxIn, maxOut uint64) Option {
	return func(c *config) error {
		c.gsMaxInRequests = maxIn
		c.gsMaxOutRequests = maxOut
		return nil
	}
}

// WithLastKnownSync sets a function that returns the last known sync, when it
// is not already known to dagsync. This will generally be some CID that is
// known to have already been seen, so that there is no need to fetch portions
// of the dag before this.
func WithLastKnownSync(f LastKnownSyncFunc) Option {
	return func(c *config) error {
		c.lastKnownSync = f
		return nil
	}
}

type syncCfg struct {
	headAdCid     cid.Cid
	stopAdCid     cid.Cid
	blockHook     BlockHookFunc
	depthLimit    int64
	segDepthLimit int64
	resync        bool
}

type SyncOption func(*syncCfg)

// getSyncOpts creates a syncCfg and applies SyncOptions to it.
func getSyncOpts(opts []SyncOption) syncCfg {
	var cfg syncCfg
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithHeadAdCid explicitly specifies an advertisement CID to sync to, instead
// of getting this by querying the publisher.
func WithHeadAdCid(headAd cid.Cid) SyncOption {
	return func(sc *syncCfg) {
		sc.headAdCid = headAd
	}
}

// WithStopAdCid explicitly specifies an advertisement CID to stop at, instead
// of using the latest synced advertisement CID..
func WithStopAdCid(stopAd cid.Cid) SyncOption {
	return func(sc *syncCfg) {
		sc.stopAdCid = stopAd
	}
}

// WithResyncAds causes the current sync to ignore advertisements that have been
// previously synced. When true, sync does not record the latest synced CID or
// send sync finished notification.
func WithAdsResync(resync bool) SyncOption {
	return func(sc *syncCfg) {
		sc.resync = resync
	}
}

// ScopedDepthLimit provides a sync depth limit for the current sync. This
// applies to both advertisement and entries chains. If zero or not specified,
// the Subscriber ads or entries depth limit is used. Set to -1 for no limits.
func ScopedDepthLimit(limit int64) SyncOption {
	return func(sc *syncCfg) {
		sc.depthLimit = limit
	}
}

// ScopedSegmentDepthLimit is the equivalent of SegmentDepthLimit option but
// only applied to a single sync. If zero or not specified, the Subscriber
// SegmentDepthLimit option is used instead. Set to -1 for no limits.
//
// For segmented sync to function at least one of BlockHook or ScopedBlockHook
// must be set. See: SegmentDepthLimit.
func ScopedSegmentDepthLimit(depth int64) SyncOption {
	return func(sc *syncCfg) {
		sc.segDepthLimit = depth
	}
}

// ScopedBlockHook is the equivalent of BlockHook option but only applied to a
// single sync. If not specified, the Subscriber BlockHook option is used
// instead. Specifying the ScopedBlockHook will override the Subscriber level
// BlockHook for the current sync.
//
// Calls to SegmentSyncActions from bloc hook will have no impact if segmented
// sync is disabled. See: BlockHook, SegmentDepthLimit,
// ScopedSegmentDepthLimit.
func ScopedBlockHook(hook BlockHookFunc) SyncOption {
	return func(sc *syncCfg) {
		sc.blockHook = hook
	}
}
