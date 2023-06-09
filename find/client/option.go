package client

import (
	"fmt"
	"net/http"
)

type config struct {
	httpClient *http.Client
	dhstoreURL string
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		httpClient: http.DefaultClient,
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
		cfg.httpClient = c
		return nil
	}
}

// WithDHStoreURL allows specifying different URLs for dhstore (/multihash and /metadata endpoints) and storetheindex (/providers endpoint).
// This might be useful as dhstore and storetheindex are different services that might not necessarily be behind the same URL. However
// the data from both of them is required to assemble results.
func WithDHStoreURL(u string) Option {
	return func(cfg *config) error {
		cfg.dhstoreURL = u
		return nil
	}
}
