package rwriter

import (
	"fmt"
)

type config struct {
	cidPathType string
	mhPathType  string
	preferJson  bool
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		mhPathType:  "multihash",
		cidPathType: "cid",
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

func WithCidPathType(pathElem string) Option {
	return func(cfg *config) error {
		cfg.cidPathType = pathElem
		return nil
	}
}

func WithMultihashPathType(pathElem string) Option {
	return func(cfg *config) error {
		cfg.mhPathType = pathElem
		return nil
	}
}

func WithPreferJson(preferJson bool) Option {
	return func(cfg *config) error {
		cfg.preferJson = preferJson
		return nil
	}
}
