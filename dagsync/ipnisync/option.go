package ipnisync

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// config contains all options for configuring Publisher.
type config struct {
	handlerPath string
	startServer bool
	topic       string

	streamHost host.Host
	requireTLS bool
	httpAddrs  []string
	tlsConfig  *tls.Config
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

// WithHTTPListenAddrs sets the HTTP addresses to listen on. These are in
// addresses:port format and may be prefixed with "https://" or "http://" or to
// specify whether or not TLS is required. If there is no prefix, then one is
// assumed based on the value specified by WithRequireTLS.
//
// Setting HTTP listen addresses is optional when a stream host is provided by
// the WithStreamHost option.
func WithHTTPListenAddrs(addrs ...string) Option {
	return func(c *config) error {
		for _, addr := range addrs {
			if addr != "" {
				c.httpAddrs = append(c.httpAddrs, addr)
			}
		}
		return nil
	}
}

// WithHandlerPath sets the path used to handle requests to this publisher.
// This specifies the portion of the path before the implicit /ipni/v1/ad/ part
// of the path. Calling WithHandlerPath("/foo/bar") configures the publisher to
// handle HTTP requests on the path "/foo/bar/ipni/v1/ad/".
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

// WithRequireTLS tells whether to allow the publisher to require https (true)
// or to serve non-secure http (false). Default is false, allowing non-secure
// HTTP.
func WithRequireTLS(require bool) Option {
	return func(c *config) error {
		c.requireTLS = require
		return nil
	}
}

// WithStreamHost specifies an optional stream based libp2p host used to do
// HTTP over libp2p streams.
func WithStreamHost(h host.Host) Option {
	return func(c *config) error {
		c.streamHost = h
		return nil
	}
}

// WithTLSConfig sets the TLS config for the HTTP server.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *config) error {
		c.tlsConfig = tlsConfig
		return nil
	}
}

type clientConfig struct {
	authPeerID bool
	streamHost host.Host

	httpTimeout      time.Duration
	httpRetryMax     int
	httpRetryWaitMin time.Duration
	httpRetryWaitMax time.Duration
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

// ClientAuthServerPeerID tells the sync client that it must authenticate the
// server's peer ID.
func ClientAuthServerPeerID(require bool) ClientOption {
	return func(c *clientConfig) {
		c.authPeerID = require
	}
}

// ClientHTTPTimeout specifies a time limit for HTTP requests made by the sync
// client. A value of zero means no timeout.
func ClientHTTPTimeout(to time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.httpTimeout = to
	}
}

// ClientStreamHost specifies an optional stream based libp2p host used by the
// sync client to do HTTP over libp2p streams.
func ClientStreamHost(h host.Host) ClientOption {
	return func(c *clientConfig) {
		c.streamHost = h
	}
}

// ClientHTTPRetry configures a retriable HTTP client. Setting retryMax to
// zero, the default, disables the retriable client.
func ClientHTTPRetry(retryMax int, waitMin, waitMax time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.httpRetryMax = retryMax
		c.httpRetryWaitMin = waitMin
		c.httpRetryWaitMax = waitMax
	}
}
