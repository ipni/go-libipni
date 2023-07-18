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

	if len(opts.providersURLs) == 0 {
		if opts.dhstoreURL == "" {
			return nil, errors.New("no source of provider information")
		}
		opts.providersURLs = []string{opts.dhstoreURL}
	}

	pc, err := pcache.New(pcache.WithTTL(opts.pcacheTTL), pcache.WithPreload(opts.preload),
		pcache.WithSourceURL(opts.providersURLs...))
	if err != nil {
		return nil, err
	}

	var dhsAPI DHStoreAPI
	if opts.dhstoreAPI != nil {
		dhsAPI = opts.dhstoreAPI
	} else {
		var dhsURL *url.URL
		if opts.dhstoreURL == "" {
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
			dhFindURL:     dhsURL.JoinPath(findPath),
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
// into FindResponse as if it was a synchronous invocation.
func (c *DHashClient) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	resChan := make(chan model.ProviderResult)
	errChan := make(chan error, 1)

	go func() {
		errChan <- c.FindAsync(ctx, mh, resChan)
	}()

	mhr := model.MultihashResult{
		Multihash: mh,
	}
	for pr := range resChan {
		mhr.ProviderResults = append(mhr.ProviderResults, pr)
	}
	err := <-errChan
	if err != nil {
		return nil, err
	}
	return &model.FindResponse{
		MultihashResults: []model.MultihashResult{mhr},
	}, nil
}

// FindAsync implements double hashed lookup workflow. FindAsync returns
// results on resChan until there are no more results or error. When finished,
// resChan is closed and the error or nil is returned.
func (c *DHashClient) FindAsync(ctx context.Context, mh multihash.Multihash, resChan chan<- model.ProviderResult) error {
	defer close(resChan)

	dhmh, err := dhash.SecondMultihash(mh)
	if err != nil {
		return err
	}

	encryptedMultihashResults, err := c.dhstoreAPI.FindMultihash(ctx, dhmh)
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

			pid, ctxId, err := dhash.SplitValueKey(vk)
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

			prs, err := c.pcache.GetResults(ctx, pid, ctxId, metadata)
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
