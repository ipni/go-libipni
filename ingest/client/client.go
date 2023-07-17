package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/ingest/model"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	announcePath     = "ingest/announce"
	registerPath     = "register"
	indexContentPath = "ingest/content"
)

// Client is an http client for the indexer ingest API
type Client struct {
	c               *http.Client
	indexContentURL string
	announceURL     string
	registerURL     string
}

// Client must implement Interface.
var _ Interface = (*Client)(nil)

// New creates a new ingest http client. If an http.Client is not provide by
// the WithClient option, then the default client is used.
func New(baseURL string, options ...Option) (*Client, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("url must have http or https scheme: %s", baseURL)
	}
	u.Path = ""

	return &Client{
		c:               opts.httpClient,
		indexContentURL: u.JoinPath(indexContentPath).String(),
		announceURL:     u.JoinPath(announcePath).String(),
		registerURL:     u.JoinPath(registerPath).String(),
	}, nil
}

// Announce announces a new root CID directly to the indexer.
func (c *Client) Announce(ctx context.Context, provider *peer.AddrInfo, root cid.Cid) error {
	p2paddrs, err := peer.AddrInfoToP2pAddrs(provider)
	if err != nil {
		return err
	}
	msg := message.Message{
		Cid: root,
	}

	if len(p2paddrs) != 0 {
		msg.SetAddrs(p2paddrs)
	}

	buf := bytes.NewBuffer(nil)
	if err := msg.MarshalCBOR(buf); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.announceURL, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return apierror.FromResponse(resp.StatusCode, body)
	}
	return nil
}

// IndexContent creates an index directly on the indexer. This bypasses the
// process of ingesting advertisements. It is used to index special one-off
// content that is outside of an advertisement chain, and is intentionally
// limited to indexing a single multihash.
func (c *Client) IndexContent(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, m multihash.Multihash, contextID []byte, metadata []byte, addrs []string) error {
	data, err := model.MakeIngestRequest(providerID, privateKey, m, contextID, metadata, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.indexContentURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return apierror.FromResponse(resp.StatusCode, body)
	}
	return nil
}

// Register registers a provider directly with an indexer. The primary use is
// update an indexer with new provider addresses without having to wait until a
// new advertisement is ingested.
func (c *Client) Register(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, addrs []string) error {
	data, err := model.MakeRegisterRequest(providerID, privateKey, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.registerURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return apierror.FromResponse(resp.StatusCode, body)
	}
	return nil
}
