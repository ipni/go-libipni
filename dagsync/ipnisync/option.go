package ipnisync

import (
	"fmt"
)

// pubConfig contains all options for configuring Publisher.
type config struct {
	handlerPath string
	startServer bool
	topic       string
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

// WithServer, if true, starts an http server listening on the given address.
// an HTTP server. If this option is not specified, then no server is started
// and this will need to be done by the caller.
func WithServer(serve bool) Option {
	return func(c *config) error {
		c.startServer = serve
		return nil
	}
}
