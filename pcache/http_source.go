package pcache

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

const providersPath = "providers"

type httpSource struct {
	url    *url.URL
	client *http.Client
}

func NewHTTPSource(srcURL string, client *http.Client) (ProviderSource, error) {
	u, err := url.Parse(srcURL)
	if err != nil {
		return nil, err
	}
	u.Path = ""
	u = u.JoinPath(providersPath)

	if client == nil {
		client = http.DefaultClient
	}

	return &httpSource{
		url:    u,
		client: client,
	}, nil
}

func (s *httpSource) Fetch(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	u := s.url.JoinPath(pid.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := s.client.Do(req)
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

func (s *httpSource) FetchAll(ctx context.Context) ([]*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := s.client.Do(req)
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