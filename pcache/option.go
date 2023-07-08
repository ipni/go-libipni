package pcache

import (
	"fmt"
	"net/http"
	"time"
)

const (
	defaultRefreshIn = 5 * time.Minute
	defaultTTL       = 10 * time.Minute
)

type config struct {
	httpClient *http.Client
	refreshIn  time.Duration
	sources    []ProviderSource
	ttl        time.Duration
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		httpClient: http.DefaultClient,
		refreshIn:  defaultRefreshIn,
		ttl:        defaultTTL,
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// WithClient allows creation of the http client using an underlying network
// round tripper / client.
func WithClient(c *http.Client) Option {
	return func(cfg *config) error {
		if c != nil {
			cfg.httpClient = c
		}
		return nil
	}
}

// WithRefreshInterval sets the interval to wait between cache refreshes. If
// set to 0, then automatic refresh is disabled.
//
// Default is 5 minutes
func WithRefreshInterval(interval time.Duration) Option {
	return func(cfg *config) error {
		cfg.refreshIn = interval
		return nil
	}
}

// WithSource adds a new provider information source for the cache to pull
// provider information from. If multiple sources provide the information for
// the same providers, then the provider record with the most recent
// LastAdvertisementTime is uesd.
func WithSource(src ProviderSource) Option {
	return func(cfg *config) error {
		cfg.sources = append(cfg.sources, src)
		return nil
	}
}

// WithTTL sets the provider cache entry time-to-live duration. This is the
// time that provider information remains in the cache after it is not longer
// available from any of the original sources. The time should be long enough
// to cover temporary unavailability of sources.
//
// This is also the amount of time that a negative cache entry will remain in
// the cache before being removed at the next refresh.
//
// Default is 10 minutes.
func WithTTL(ttl time.Duration) Option {
	return func(cfg *config) error {
		cfg.ttl = ttl
		return nil
	}
}
