package ipnisync

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// pubConfig contains all options for configuring Publisher.
type config struct {
	handlerPath string
	startServer bool
	topic       string

	streamHost host.Host
	requireTLS bool
	httpAddrs  []string
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getPubOpts creates a pubConfig and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		startServer: true,
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

func WithHTTPListenAddrs(addrs ...string) Option {
	return func(c *config) error {
		c.httpAddrs = append(c.httpAddrs, addrs...)
		return nil
	}
}

// WithHandlerPath sets the path used to handle requests to this publisher.
// This should only include the path before the /ipni/v1/ad/ part of the path.
func WithHandlerPath(urlPath string) Option {
	return func(c *config) error {
		c.handlerPath = urlPath
		return nil
	}
}

// WithHeadTopic sets the optional topic returned in a head query response.
// This is the topic on which advertisement are announced.
func WithHeadTopic(topic string) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}

// WithStartServer, if true, starts an http server listening on the given
// address. an HTTP server. If this option is not specified, then no server is
// started and this will need to be done by the caller.
func WithStartServer(start bool) Option {
	return func(c *config) error {
		c.startServer = start
		return nil
	}
}

// WithRequireTLS tells whether to allow the publisher to serve non-secure http
// (false) or to require https (true). Default is false, allowing non-secure
// HTTP.
func WithRequireTLS(require bool) Option {
	return func(c *config) error {
		c.requireTLS = require
		return nil
	}
}

func WithStreamHost(h host.Host) Option {
	return func(c *config) error {
		c.streamHost = h
		return nil
	}
}

type clientConfig struct {
	authPeerID bool
	timeout    time.Duration
	streamHost host.Host
}

// Option is a function that sets a value in a config.
type ClientOption func(*clientConfig)

// getClientOpts creates a pubConfig and applies Options to it.
func getClientOpts(opts []ClientOption) clientConfig {
	var cfg clientConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func ClientAuthPeerID(require bool) ClientOption {
	return func(c *clientConfig) {
		c.authPeerID = require
	}
}

func ClientTimeout(to time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.timeout = to
	}
}

func ClientStreamHost(h host.Host) ClientOption {
	return func(c *clientConfig) {
		c.streamHost = h
	}
}
