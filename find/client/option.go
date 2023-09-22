package client

import (
	"fmt"
	"net/http"
	"time"
)

const (
	// defaultPcacheTTL is the default time to live for provider info cache.
	defaultPcacheTTL = 5 * time.Minute
)

type config struct {
	httpClient    *http.Client
	providersURLs []string
	dhstoreURL    string
	dhstoreAPI    DHStoreAPI
	metadataOnly  bool
	pcacheTTL     time.Duration
	preload       bool
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		httpClient: http.DefaultClient,
		pcacheTTL:  defaultPcacheTTL,
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

// WithMetadataOnly configures lookups to only return metadata and not provider
// information. This means that is it is not necessary to specify providers
// URLs, using WithProvidersURL, when provider information is available from a
// separate location.
func WithMetadataOnly(metadataOnly bool) Option {
	return func(cfg *config) error {
		cfg.metadataOnly = metadataOnly
		return nil
	}
}

// WithProvidersURL specifies one or more URLs for retrieving provider
// information (/providers and /providers/<pid> endpoints). Multiple URLs may
// be given to specify multiple sources of provider information.
//
// This option is ignored if WithMetadataOnly(true) is specified.
func WithProvidersURL(urls ...string) Option {
	return func(cfg *config) error {
		cfg.providersURLs = append(cfg.providersURLs, urls...)
		return nil
	}
}

// WithDHStoreURL specifies a URL for dhstore (/multihash and /metadata
// endpoints). If not specified then a WithDHStoreAPI should be used to provide
// access to dhstore data.
func WithDHStoreURL(u string) Option {
	return func(cfg *config) error {
		cfg.dhstoreURL = u
		return nil
	}
}

// WithDHStoreAPI configures an interface to use for doing multihash and
// metadata lookups with dhstore. If this is not configured, then dhstore
// lookups are done using the dhstoreURL.
func WithDHStoreAPI(dhsAPI DHStoreAPI) Option {
	return func(cfg *config) error {
		cfg.dhstoreAPI = dhsAPI
		return nil
	}
}

// WithPcacheTTL sets the time that provider information remains in the cache
// after it is not longer available from any of the original sources.
//
// This option is ignored if WithMetadataOnly(true) is specified.
func WithPcacheTTL(ttl time.Duration) Option {
	return func(cfg *config) error {
		cfg.pcacheTTL = ttl
		return nil
	}
}

// WithPcachePreload enables or disabled preloading the cache. Generally this
// should be enabled, even for short-lived clients needing to look up few
// providers.
//
// Default is true (enabled). This option is ignored if WithMetadataOnly(true)
// is specified.
func WithPcachePreload(preload bool) Option {
	return func(cfg *config) error {
		cfg.preload = preload
		return nil
	}
}
