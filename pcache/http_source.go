package pcache

import (
	"context"
	"encoding/json"
	"fmt"
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
	header http.Header
}

func NewHTTPSource(srcURL string, client *http.Client) (ProviderSource, error) {
	u, err := url.Parse(srcURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("url must have http or https scheme: %s", srcURL)
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

func (s *httpSource) AddHeader(key, value string) {
	if s.header == nil {
		s.header = make(map[string][]string)
	}
	s.header.Add(key, value)
}

func (s *httpSource) Fetch(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	u := s.url.JoinPath(pid.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for key, vals := range s.header {
		for _, val := range vals {
			req.Header.Add(key, val)
		}
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
		if resp.StatusCode != http.StatusNotFound {
			return nil, nil
		}
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
	for key, vals := range s.header {
		for _, val := range vals {
			req.Header.Add(key, val)
		}
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

	var providers []*model.ProviderInfo
	err = json.Unmarshal(body, &providers)
	if err != nil {
		return nil, err
	}
	return providers, nil
}

func (s *httpSource) String() string {
	return s.url.String()
}
