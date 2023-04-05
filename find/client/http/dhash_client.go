package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/dhash"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

const (
	metadataPath = "metadata"
	// pcacheTTL is the time to live for provider info cache.
	pcacheTTL = 5 * time.Minute
)

var log = logging.Logger("dhash-client")

type DHashClient struct {
	Client

	dhFindURL     *url.URL
	dhMetadataURL *url.URL
	pcache        *providerCache
}

// NewDHashClient instantiates a new client that uses Reader Privacy API for
// querying data. It requires more roundtrips to fullfill one query however it
// also protects the user from a passive observer. dhstoreURL specifies the URL
// of the double hashed store that can respond to find encrypted multihash and
// find encrypted metadata requests. stiURL specifies the URL of indexer that
// can respond to find provider requests. dhstoreURL and stiURL are expected to
// be the same when these services are deployed behing a proxy - indexstar.
func NewDHashClient(dhstoreURL, stiURL string, options ...Option) (*DHashClient, error) {
	c, err := New(stiURL, options...)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(dhstoreURL, "http://") && !strings.HasPrefix(dhstoreURL, "https://") {
		dhstoreURL = "http://" + dhstoreURL
	}
	dhsURL, err := url.Parse(dhstoreURL)
	if err != nil {
		return nil, err
	}

	return &DHashClient{
		Client:        *c,
		dhFindURL:     dhsURL.JoinPath(findPath),
		dhMetadataURL: dhsURL.JoinPath(metadataPath),
		pcache: &providerCache{
			ttl:    pcacheTTL,
			pinfos: make(map[peer.ID]*pinfoWrapper),
			pinfoFetcher: func(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
				return c.GetProvider(ctx, pid)
			},
		},
	}, nil
}

func (c *DHashClient) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	// query value keys from indexer
	smh, err := dhash.SecondMultihash(mh)
	if err != nil {
		return nil, err
	}
	u := c.dhFindURL.JoinPath(smh.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	findResponse := &model.FindResponse{}
	err = json.Unmarshal(body, findResponse)
	if err != nil {
		return nil, err
	}

	err = c.decryptFindResponse(ctx, findResponse, map[string]multihash.Multihash{smh.B58String(): mh})
	if err != nil {
		return nil, err
	}

	return findResponse, nil
}

// decryptFindResponse decrypts EncMultihashResults and appends the decrypted
// values to MultihashResults. It also fetches provider info and metadata for
// each value key.
func (c *DHashClient) decryptFindResponse(ctx context.Context, resp *model.FindResponse, unhasher map[string]multihash.Multihash) error {
	// decrypt each value key using the original multihash
	// then for each decrypted value key fetch provider's addr info.
	for _, encRes := range resp.EncryptedMultihashResults {
		mh, found := unhasher[encRes.Multihash.B58String()]
		if !found {
			continue
		}

		mhr := model.MultihashResult{
			Multihash: mh,
		}
		for _, evk := range encRes.EncryptedValueKeys {
			vk, err := dhash.DecryptValueKey(evk, mh)
			// skip errors as we don't want to fail the whole query, warn instead. Same applies to the rest of the loop.
			if err != nil {
				log.Warnw("Error decrypting value key", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			pid, ctxId, err := dhash.SplitValueKey(vk)
			if err != nil {
				log.Warnw("Error splitting value key", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			// fetch metadata
			metadata, err := c.fetchMetadata(ctx, vk)
			if err != nil {
				log.Warnw("Error fetching metadata", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			// fetch provider info alongside extended providers
			results, err := c.pcache.getResults(ctx, pid, ctxId, metadata)
			if err != nil {
				log.Warnw("Error fetching provider info", "multihash", mh.B58String(), "evk", b58.Encode(evk), "err", err)
				continue
			}

			mhr.ProviderResults = append(mhr.ProviderResults, results...)
		}
		if len(mhr.ProviderResults) > 0 {
			resp.MultihashResults = append(resp.MultihashResults, mhr)
		}
	}
	return nil
}

func (c *DHashClient) fetchMetadata(ctx context.Context, vk []byte) ([]byte, error) {
	u := c.dhMetadataURL.JoinPath(b58.Encode(dhash.SHA256(vk, nil)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	type (
		GetMetadataResponse struct {
			EncryptedMetadata []byte `json:"EncryptedMetadata"`
		}
	)

	findResponse := &GetMetadataResponse{}
	err = json.Unmarshal(body, findResponse)

	if err != nil {
		return nil, err
	}

	return dhash.DecryptMetadata(findResponse.EncryptedMetadata, vk)
}

// providerCache caches ProviderInfo objects as well as indexes
// ContextualExtendedProviders by ContextID. ProviderInfos are evicted from the
// cache after ttl. Missing / expired ProviderInfos are refreshed via
// pinfoFetcher function.
//
// This struct is designed to be independent from double hashed client itself
// so that it can be used in the libp2p client once it materialises.
type providerCache struct {
	ttl          time.Duration
	pinfos       map[peer.ID]*pinfoWrapper
	pinfoFetcher func(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error)
}

type pinfoWrapper struct {
	ts    time.Time
	pinfo *model.ProviderInfo
	cxps  map[string]*model.ContextualExtendedProviders
}

func (pc *providerCache) getResults(ctx context.Context, pid peer.ID, ctxID []byte, metadata []byte) ([]model.ProviderResult, error) {
	wrapper := pc.pinfos[pid]

	// If ProviderInfo isn't in the cache or if the record has expired - try to
	// fetch a new ProviderInfo and update the cache
	if wrapper == nil || time.Since(wrapper.ts) > pc.ttl {
		pinfo, err := pc.pinfoFetcher(ctx, pid)
		if err != nil {
			return nil, err
		}

		wrapper = &pinfoWrapper{
			ts:    time.Now(),
			pinfo: pinfo,
			cxps:  make(map[string]*model.ContextualExtendedProviders),
		}
		pc.pinfos[pinfo.AddrInfo.ID] = wrapper
		if pinfo.ExtendedProviders != nil {
			for _, cxp := range pinfo.ExtendedProviders.Contextual {
				wrapper.cxps[cxp.ContextID] = &cxp
			}
		}
	}

	results := make([]model.ProviderResult, 0, 1)

	results = append(results, model.ProviderResult{
		ContextID: ctxID,
		Metadata:  metadata,
		Provider:  &wrapper.pinfo.AddrInfo,
	})

	// return results if there are no further extended providers to unpack
	if wrapper.pinfo.ExtendedProviders == nil {
		return results, nil
	}

	// If override is set to true at the context level then the chain
	// level EPs should be ignored for this context ID
	override := false

	// Adding context-level EPs if they exist
	if contextualEpRecord, ok := wrapper.cxps[string(ctxID)]; ok {
		override = contextualEpRecord.Override
		for i, xpinfo := range contextualEpRecord.Providers {
			xmd := contextualEpRecord.Metadatas[i]
			// Skippng the main provider's record if its metadata is nil or is
			// the same as the one retrieved from the indexer, because such EP
			// record does not advertise any new protocol.
			if xpinfo.ID == wrapper.pinfo.AddrInfo.ID &&
				(len(xmd) == 0 || bytes.Equal(xmd, metadata)) {
				continue
			}
			// Use metadata from advertisement if one hasn't been specified for
			// the extended provider
			if xmd == nil {
				xmd = metadata
			}

			results = append(results, model.ProviderResult{
				ContextID: ctxID,
				Metadata:  xmd,
				Provider:  &xpinfo,
			})
		}
	}

	// If override is true then don't include chain-level EPs
	if override {
		return results, nil
	}

	// Adding chain-level EPs if such exist
	for i, xpinfo := range wrapper.pinfo.ExtendedProviders.Providers {
		xmd := wrapper.pinfo.ExtendedProviders.Metadatas[i]
		// Skippng the main provider's record if its metadata is nil or is the
		// same as the one retrieved from the indexer, because such EP record
		// does not advertise any new protocol.
		if xpinfo.ID == wrapper.pinfo.AddrInfo.ID &&
			(len(xmd) == 0 || bytes.Equal(xmd, metadata)) {
			continue
		}
		// Use metadata from advertisement if one hasn't been specified for the
		// extended provider
		if xmd == nil {
			xmd = metadata
		}
		results = append(results, model.ProviderResult{
			ContextID: ctxID,
			Metadata:  xmd,
			Provider:  &xpinfo,
		})
	}

	return results, nil
}
