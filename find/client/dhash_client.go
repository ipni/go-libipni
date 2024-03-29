package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/dhash"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

const metadataPath = "metadata"

var log = logging.Logger("dhash-client")

// DHStoreAPI defines multihash and metadata find functions. The default
// implementation defines functions to do this over HTTP. A replacement
// implementation that implements the interface can optionally be provided when
// creating a DHashClient.
type DHStoreAPI interface {
	// FindMultihash does a dh-multihash lookup and returns a
	// model.FindResponse with EncryptedMultihashResults. Returns no data and
	// no error, (nil, nil), if data not found.
	FindMultihash(context.Context, multihash.Multihash) ([]model.EncryptedMultihashResult, error)
	// FindMetadata takes a value-key-hash, does a metadata lookup, and returns
	// encrypted metadata. Returns no data and no error, (nil, nil), if
	// metadata not found
	FindMetadata(context.Context, []byte) ([]byte, error)
}

// DHashClient is a client that does double-hashed lookups on a dhstore. By
// default, it does multihash and metadata lookups over HTTP. If given a
// DHStoreAPI, it can do the lookups any the underlying implementation defined.
type DHashClient struct {
	dhstoreAPI DHStoreAPI
	pcache     *pcache.ProviderCache
}

// NewDHashClient instantiates a new client that uses Reader Privacy API for
// querying data. It requires more roundtrips to fulfill one query however it
// also protects the user from a passive observer. dhstoreURL specifies the URL
// of the double hashed store that can respond to find encrypted multihash and
// find encrypted metadata requests.
func NewDHashClient(options ...Option) (*DHashClient, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	var pc *pcache.ProviderCache
	if !opts.metadataOnly {
		if len(opts.providersURLs) == 0 {
			if opts.dhstoreURL == "" {
				return nil, errors.New("no source of provider information")
			}
			opts.providersURLs = []string{opts.dhstoreURL}
		}

		pc, err = pcache.New(pcache.WithTTL(opts.pcacheTTL), pcache.WithPreload(opts.preload),
			pcache.WithSourceURL(opts.providersURLs...))
		if err != nil {
			return nil, err
		}
	}

	var dhsAPI DHStoreAPI
	if opts.dhstoreAPI != nil {
		dhsAPI = opts.dhstoreAPI
	} else {
		var dhsURL *url.URL
		if opts.dhstoreURL == "" {
			if len(opts.providersURLs) == 0 {
				return nil, errors.New("no destination for metadata lookup")
			}
			opts.dhstoreURL = opts.providersURLs[0]
		}
		dhsURL, err = url.Parse(opts.dhstoreURL)
		if err != nil {
			return nil, err
		}
		if dhsURL.Scheme != "http" && dhsURL.Scheme != "https" {
			return nil, fmt.Errorf("url must have http or https scheme: %s", opts.dhstoreURL)
		}
		dhsAPI = &dhstoreHTTP{
			c:             opts.httpClient,
			dhFindURL:     dhsURL.JoinPath("encrypted", findPath),
			dhMetadataURL: dhsURL.JoinPath(metadataPath),
		}
	}

	return &DHashClient{
		dhstoreAPI: dhsAPI,
		pcache:     pc,
	}, nil
}

func (c *DHashClient) PCache() *pcache.ProviderCache {
	return c.pcache
}

// Find launches FindAsync in a separate go routine and assembles the result
// into FindResponse as if it was a synchronous invocation. If no results are
// found then an empty response without error is returned.
func (c *DHashClient) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	resChan := make(chan model.ProviderResult)
	errChan := make(chan error, 1)

	go func() {
		errChan <- c.FindAsync(ctx, mh, resChan)
	}()

	var providerResults []model.ProviderResult
	for pr := range resChan {
		providerResults = append(providerResults, pr)
	}
	err := <-errChan
	if err != nil {
		return nil, err
	}
	if len(providerResults) == 0 {
		return &model.FindResponse{}, nil
	}

	return &model.FindResponse{
		MultihashResults: []model.MultihashResult{
			{
				Multihash:       mh,
				ProviderResults: providerResults,
			},
		},
	}, nil
}

// FindAsync implements double hashed lookup workflow. FindAsync returns
// results on resChan until there are no more results or error. When finished,
// resChan is closed and the error or nil is returned.
func (c *DHashClient) FindAsync(ctx context.Context, mh multihash.Multihash, resChan chan<- model.ProviderResult) error {
	defer close(resChan)

	encryptedMultihashResults, err := c.dhstoreAPI.FindMultihash(ctx, dhash.SecondMultihash(mh))
	if err != nil {
		return err
	}

	for _, emhrs := range encryptedMultihashResults {
		for _, evk := range emhrs.EncryptedValueKeys {
			vk, err := dhash.DecryptValueKey(evk, mh)
			// skip errors as we don't want to fail the whole query, warn
			// instead. Same applies to the rest of the loop.
			if err != nil {
				log.Warnw("Error decrypting value key", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			pid, ctxID, err := dhash.SplitValueKey(vk)
			if err != nil {
				log.Warnw("Error splitting value key", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			// fetch and decrypt metadata.
			metadata, err := c.fetchMetadata(ctx, vk)
			if err != nil {
				log.Warnw("Error fetching metadata", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}
			if len(metadata) == 0 {
				// Metadata not found; multihash has no metadata. This was
				// probably deleted by context ID and the associated
				// multihashes were not removed.
				continue
			}

			// If only fetching metadata. The provider ID is still needed in
			// order to identify which provider the metadata is associated
			// with.
			if c.pcache == nil {
				pr := model.ProviderResult{
					ContextID: ctxID,
					Metadata:  metadata,
					Provider: &peer.AddrInfo{
						ID: pid,
					},
				}
				select {
				case resChan <- pr:
				case <-ctx.Done():
					return ctx.Err()
				}
				continue
			}

			prs, err := c.pcache.GetResults(ctx, pid, ctxID, metadata)
			if err != nil {
				log.Warnw("Error fetching provider infos", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			for _, pr := range prs {
				select {
				case resChan <- pr:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return nil
}

// fetchMetadata fetches metadata from a remote server using a value-key-hash,
// and then decrypts the metadata using the value-key.
func (c *DHashClient) fetchMetadata(ctx context.Context, vk []byte) ([]byte, error) {
	encryptedMetadata, err := c.dhstoreAPI.FindMetadata(ctx, dhash.SHA256(vk, nil))
	if err != nil {
		return nil, err
	}
	if len(encryptedMetadata) == 0 {
		return nil, nil
	}
	return dhash.DecryptMetadata(encryptedMetadata, vk)
}
