package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

// providerCache caches ProviderInfo objects as well as indexes
// ContextualExtendedProviders by ContextID. ProviderInfos are evicted from the
// cache after ttl. Missing / expired ProviderInfos get fetched from the pURL.
//
// This struct is designed to be independent from double hashed client itself
// so that it can be used in the libp2p client once it materialises.
//
// Safe to be used concurrently.
type providerCache struct {
	ttl    time.Duration
	lock   sync.RWMutex
	pinfos map[peer.ID]*pinfoWrapper
	pUrl   *url.URL
	c      *http.Client
}

// NewProviderCache creates a new provider cache that can be sahred across multiple clients.
func newProviderCache(u *url.URL, c *http.Client) (*providerCache, error) {
	pUrl := u.JoinPath(providersPath)

	if c == nil {
		c = http.DefaultClient
	}

	return &providerCache{
		ttl:    pcacheTTL,
		pUrl:   pUrl,
		c:      c,
		pinfos: make(map[peer.ID]*pinfoWrapper),
	}, nil
}

type pinfoWrapper struct {
	ts    time.Time
	pinfo *model.ProviderInfo
	cxps  map[string]*model.ContextualExtendedProviders
}

func (pc *providerCache) getPInfoWrapper(pid peer.ID) *pinfoWrapper {
	pc.lock.RLock()
	defer pc.lock.RUnlock()
	return pc.pinfos[pid]
}

func (pc *providerCache) setPInforWrapper(pid peer.ID, wrapper *pinfoWrapper) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	pc.pinfos[pid] = wrapper
}

func (pc *providerCache) fetchPInfo(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	u := pc.pUrl.JoinPath(pid.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := pc.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var providerInfo model.ProviderInfo
	err = json.Unmarshal(body, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (pc *providerCache) getResults(ctx context.Context, pid peer.ID, ctxID []byte, metadata []byte) ([]model.ProviderResult, error) {
	wrapper := pc.getPInfoWrapper(pid)

	// If ProviderInfo isn't in the cache or if the record has expired - try to
	// fetch a new ProviderInfo and update the cache
	if wrapper == nil || time.Since(wrapper.ts) > pc.ttl {
		pinfo, err := pc.fetchPInfo(ctx, pid)
		if err != nil {
			return nil, err
		}

		wrapper = &pinfoWrapper{
			ts:    time.Now(),
			pinfo: pinfo,
			cxps:  make(map[string]*model.ContextualExtendedProviders),
		}
		if pinfo.ExtendedProviders != nil {
			for _, cxp := range pinfo.ExtendedProviders.Contextual {
				wrapper.cxps[cxp.ContextID] = &cxp
			}
		}
		pc.setPInforWrapper(pid, wrapper)
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
		if len(xmd) == 0 {
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
