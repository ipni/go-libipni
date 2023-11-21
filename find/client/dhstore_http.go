package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

type dhstoreHTTP struct {
	c             *http.Client
	dhFindURL     *url.URL
	dhMetadataURL *url.URL
}

// FindMultihash implements DHStoreAPI. Returns no data and no error if data
// not found.
func (d *dhstoreHTTP) FindMultihash(ctx context.Context, dhmh multihash.Multihash) ([]model.EncryptedMultihashResult, error) {
	u := d.dhFindURL.JoinPath(dhmh.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := d.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	encResponse := &model.FindResponse{}
	err = json.Unmarshal(body, encResponse)
	if err != nil {
		return nil, err
	}
	return encResponse.EncryptedMultihashResults, nil
}

// FindMetadata implements DHStoreAPI. Returns no data and no error if metadata
// not found.
func (d *dhstoreHTTP) FindMetadata(ctx context.Context, hvk []byte) ([]byte, error) {
	u := d.dhMetadataURL.JoinPath(b58.Encode(hvk))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := d.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	type GetMetadataResponse struct {
		EncryptedMetadata []byte `json:"EncryptedMetadata"`
	}

	findResponse := &GetMetadataResponse{}
	err = json.Unmarshal(body, findResponse)
	if err != nil {
		return nil, err
	}

	return findResponse.EncryptedMetadata, nil
}
