package dtsync

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// Maximum number of in-prgress graphsync requests.
	defaultGsMaxInRequests  = 1024
	defaultGsMaxOutRequests = 1024
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	allowPeer func(peer.ID) bool

	gsMaxInRequests  uint64
	gsMaxOutRequests uint64
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
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

// WithAllowPeer sets the function that determines whether to allow or reject
// graphsync sessions from a peer.
func WithAllowPeer(allowPeer func(peer.ID) bool) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
}

func WithMaxGraphsyncRequests(maxIn, maxOut uint64) Option {
	return func(c *config) error {
		c.gsMaxInRequests = maxIn
		c.gsMaxOutRequests = maxOut
		return nil
	}
}
