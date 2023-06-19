package httpsender

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ipni/go-libipni"
)

const defaultTimeout = time.Minute

type config struct {
	timeout   time.Duration
	client    *http.Client
	userAgent string
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		timeout:   defaultTimeout,
		userAgent: "go-libipni/" + libipni.Release,
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// WithTimeout configures the timeout to wait for a response.
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) error {
		cfg.timeout = timeout
		return nil
	}
}

// WithClient uses an existing http.Client with the Sender.
func WithClient(c *http.Client) Option {
	return func(cfg *config) error {
		cfg.client = c
		return nil
	}
}

// WithUserAgent sets the value used for the User-Agent header.
func WithUserAgent(userAgent string) Option {
	return func(cfg *config) error {
		cfg.userAgent = userAgent
		return nil
	}
}
