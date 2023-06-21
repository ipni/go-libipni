package dagsync

import (
	"fmt"
	"net/http"
	"time"

	dt "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/announce"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
	// Maximum number of in-prgress graphsync requests.
	defaultGsMaxInRequests  = 1024
	defaultGsMaxOutRequests = 1024
)

// config contains all options for configuring Subscriber.
type config struct {
	addrTTL   time.Duration
	allowPeer announce.AllowPeerFunc
	filterIPs bool

	topic *pubsub.Topic

	dtManager     dt.Manager
	graphExchange graphsync.GraphExchange

	blockHook  BlockHookFunc
	httpClient *http.Client

	syncRecLimit selector.RecursionLimit

	idleHandlerTTL    time.Duration
	latestSyncHandler LatestSyncHandler

	resendAnnounce bool

	segDepthLimit int64

	gsMaxInRequests  uint64
	gsMaxOutRequests uint64
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		addrTTL:          defaultAddrTTL,
		idleHandlerTTL:   defaultIdleHandlerTTL,
		segDepthLimit:    defaultSegDepthLimit,
		gsMaxInRequests:  defaultGsMaxInRequests,
		gsMaxOutRequests: defaultGsMaxOutRequests,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// AllowPeer sets the function that determines whether to allow or reject
// messages from a peer.
func AllowPeer(allowPeer announce.AllowPeerFunc) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
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

// DtManager provides an existing datatransfer manager.
func DtManager(dtManager dt.Manager, gs graphsync.GraphExchange) Option {
	return func(c *config) error {
		c.dtManager = dtManager
		c.graphExchange = gs
		return nil
	}
}

// HttpClient provides Subscriber with an existing http client.
func HttpClient(client *http.Client) Option {
	return func(c *config) error {
		c.httpClient = client
		return nil
	}
}

// BlockHook adds a hook that is run when a block is received via Subscriber.Sync along with a
// SegmentSyncActions to control the sync flow if segmented sync is enabled.
// Note that if segmented sync is disabled, calls on SegmentSyncActions will have no effect.
// See: SegmentSyncActions, SegmentDepthLimit, ScopedBlockHook.
func BlockHook(blockHook BlockHookFunc) Option {
	return func(c *config) error {
		c.blockHook = blockHook
		return nil
	}
}

// FilterIPs removes any private, loopback, or unspecified IP multiaddrs from
// addresses supplied in announce messages.
func FilterIPs(enable bool) Option {
	return func(c *config) error {
		c.filterIPs = enable
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

// SegmentDepthLimit sets the maximum recursion depth limit for a segmented sync.
// Setting the depth to a value less than zero disables segmented sync completely.
// Disabled by default.
// Note that for segmented sync to function at least one of BlockHook or ScopedBlockHook must be
// set.
func SegmentDepthLimit(depth int64) Option {
	return func(c *config) error {
		c.segDepthLimit = depth
		return nil
	}
}

// SyncRecursionLimit sets the recursion limit of the background syncing process.
// Defaults to selector.RecursionLimitNone if not specified.
func SyncRecursionLimit(limit selector.RecursionLimit) Option {
	return func(c *config) error {
		c.syncRecLimit = limit
		return nil
	}
}

// ResendAnnounce determines whether to resend the direct announce mesages
// (those that are not received via pubsub) over pubsub.
func ResendAnnounce(enable bool) Option {
	return func(c *config) error {
		c.resendAnnounce = enable
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

// UseLatestSyncHandler sets the latest sync handler to use.
func UseLatestSyncHandler(h LatestSyncHandler) Option {
	return func(c *config) error {
		c.latestSyncHandler = h
		return nil
	}
}

type syncCfg struct {
	alwaysUpdateLatest bool
	scopedBlockHook    BlockHookFunc
	segDepthLimit      int64
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

func AlwaysUpdateLatest() SyncOption {
	return func(sc *syncCfg) {
		sc.alwaysUpdateLatest = true
	}
}

// ScopedBlockHook is the equivalent of BlockHook option but only applied to a
// single sync. If not specified, the Subscriber BlockHook option is used
// instead. Specifying the ScopedBlockHook will override the Subscriber level
// BlockHook for the current sync.
//
// Note that calls to SegmentSyncActions from bloc hook will have no impact if
// segmented sync is disabled. See: BlockHook, SegmentDepthLimit,
// ScopedSegmentDepthLimit.
func ScopedBlockHook(hook BlockHookFunc) SyncOption {
	return func(sc *syncCfg) {
		sc.scopedBlockHook = hook
	}
}

// ScopedSegmentDepthLimit is the equivalent of SegmentDepthLimit option but
// only applied to a single sync. If not specified, the Subscriber
// SegmentDepthLimit option is used instead.
//
// Note that for segmented sync to function at least one of BlockHook or
// ScopedBlockHook must be set. See: SegmentDepthLimit.
func ScopedSegmentDepthLimit(depth int64) SyncOption {
	return func(sc *syncCfg) {
		sc.segDepthLimit = depth
	}
}
