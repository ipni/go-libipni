package pcache

import (
	"fmt"
	"net/http"
	"time"
)

const (
	defaultRefreshIn = 2 * time.Minute
	defaultTTL       = 10 * time.Minute
)

type config struct {
	httpClient *http.Client
	preload    bool
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
		preload:    true,
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

// WithPreload enables or disabled cache preload. Preload performs an initial
// refresh of the cache to populate it. This results in faster lookup times
// when information for multiple providers will be needed, even for a few
// providers. Generally, preload should always be enabled, but can be disabled
// if there are specific situations that require it.
//
// Default is true (enabled).
func WithPreload(preload bool) Option {
	return func(cfg *config) error {
		cfg.preload = preload
		return nil
	}
}

// WithClient sets the HTTP client to use when WithSourceURL is used.
func WithClient(c *http.Client) Option {
	return func(cfg *config) error {
		if c != nil {
			cfg.httpClient = c
		}
		return nil
	}
}

// WithRefreshInterval sets the minimul time interval to wait between automatic
// cache refreshes. Once the interval has elapsed since the last refresh, an
// new refresh is started at nest cache Get. If set to 0, then automatic
// refresh is disabled.
//
// Default is 2 minutes.
func WithRefreshInterval(interval time.Duration) Option {
	return func(cfg *config) error {
		cfg.refreshIn = interval
		return nil
	}
}

// WithSource adds one or more new provider information sources for the cache
// to pull provider information from. If multiple sources provide the
// information for the same providers, then the provider record with the most
// recent LastAdvertisementTime is uesd.
func WithSource(src ...ProviderSource) Option {
	return func(cfg *config) error {
		cfg.sources = append(cfg.sources, src...)
		return nil
	}
}

// WithSourceURL adds one or more new HTTP sources of provider information. It
// is a convenient alternative to calling NewHTTPSource and WithSource for each
// source URL. To use an alternative HTTP client with each source, call
// WithClient first.
func WithSourceURL(urls ...string) Option {
	return func(cfg *config) error {
		for _, u := range urls {
			httpSrc, err := NewHTTPSource(u, cfg.httpClient)
			if err != nil {
				return err
			}
			cfg.sources = append(cfg.sources, httpSrc)
		}
		return nil
	}
}

// WithTTL sets the time that provider information remains in the cache after
// it is not longer available from any of the original sources. The time should
// be long enough to cover temporary unavailability of sources.
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
