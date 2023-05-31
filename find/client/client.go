package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	findPath      = "multihash"
	providersPath = "providers"
	statsPath     = "stats"
)

// Client is an http client for the indexer find API
type Client struct {
	c            *http.Client
	findURL      *url.URL
	providersURL *url.URL
	statsURL     *url.URL
}

// Client must implement Interface.
var _ Interface = (*Client)(nil)

// New creates a new find HTTP client.
func New(baseURL string, options ...Option) (*Client, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = ""

	return &Client{
		c:            opts.httpClient,
		findURL:      u.JoinPath(findPath),
		providersURL: u.JoinPath(providersPath),
		statsURL:     u.JoinPath(statsPath),
	}, nil
}

// Find looks up content entries by multihash.
func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*model.FindResponse, error) {
	u := c.findURL.JoinPath(m.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	return c.sendRequest(req)
}

// FindBatch looks up content entries for a batch of multihashes
func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}
	data, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.findURL.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return c.sendRequest(req)
}

func (c *Client) ListProviders(ctx context.Context) ([]*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.providersURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var providers []*model.ProviderInfo
	err = json.Unmarshal(body, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (c *Client) GetProvider(ctx context.Context, providerID peer.ID) (*model.ProviderInfo, error) {
	u := c.providersURL.JoinPath(providerID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
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

func (c *Client) GetStats(ctx context.Context) (*model.Stats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.statsURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
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

	return model.UnmarshalStats(body)
}

func (c *Client) sendRequest(req *http.Request) (*model.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return &model.FindResponse{}, nil
		}
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(b)
}
