package httpclient

import (
	"github.com/ipni/go-libipni/find/client"
)

type Client = client.Client
type Option = client.Option

// Deprecated: Use github.com/ipni/go-libipni/find/client
func New(baseURL string, options ...client.Option) (*client.Client, error) {
	return client.New(baseURL, options...)
}

type DHashClient = client.DHashClient

// Deprecated: Use github.com/ipni/go-libipni/find/client
func NewDHashClient(dhstoreURL, stiURL string, options ...client.Option) (*client.DHashClient, error) {
	return client.NewDHashClient(append(options, client.WithDHStoreURL(dhstoreURL), client.WithProvidersURL(stiURL))...)
}
