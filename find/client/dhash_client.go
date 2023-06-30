package client

import (
	"context"
	"net/url"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/dhash"
	"github.com/ipni/go-libipni/find/model"
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
	// model.FindResponse with EncryptedMultihashResults.
	FindMultihash(context.Context, multihash.Multihash) ([]model.EncryptedMultihashResult, error)
	// FindMetadata takes a value-key-hash, does a metadata lookup, and returns
	// encrypted metadata.
	FindMetadata(context.Context, []byte) ([]byte, error)
}

// DHashClient is a client that does double-hashed lookups on a dhstore. By
// default, it does multihash and metadata lookups over HTTP. If given a
// DHStoreAPI, it can do the lookups any the underlying implementation defined.
type DHashClient struct {
	dhstoreAPI DHStoreAPI
	pcache     *providerCache
}

// NewDHashClient instantiates a new client that uses Reader Privacy API for
// querying data. It requires more roundtrips to fulfill one query however it
// also protects the user from a passive observer. dhstoreURL specifies the URL
// of the double hashed store that can respond to find encrypted multihash and
// find encrypted metadata requests.
func NewDHashClient(stiURL string, options ...Option) (*DHashClient, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	sURL, err := parseURL(stiURL)
	if err != nil {
		return nil, err
	}

	pcache, err := newProviderCache(sURL, opts.httpClient, opts.pcacheTTL)
	if err != nil {
		return nil, err
	}

	var dhsAPI DHStoreAPI
	if opts.dhstoreAPI != nil {
		dhsAPI = opts.dhstoreAPI
	} else {
		dhsURL := sURL
		if len(opts.dhstoreURL) > 0 {
			dhsURL, err = parseURL(opts.dhstoreURL)
			if err != nil {
				return nil, err
			}
		}
		dhsAPI = &dhstoreHTTP{
			c:             opts.httpClient,
			dhFindURL:     dhsURL.JoinPath(findPath),
			dhMetadataURL: dhsURL.JoinPath(metadataPath),
		}
	}

	return &DHashClient{
		dhstoreAPI: dhsAPI,
		pcache:     pcache,
	}, nil
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

			prs, err := c.pcache.getResults(ctx, pid, ctxId, metadata)
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
	return dhash.DecryptMetadata(encryptedMetadata, vk)
}

func parseURL(su string) (*url.URL, error) {
	if !strings.HasPrefix(su, "http://") && !strings.HasPrefix(su, "https://") {
		su = "http://" + su
	}
	return url.Parse(su)
}
