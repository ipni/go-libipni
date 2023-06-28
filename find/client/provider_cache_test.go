package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ipni/go-libipni/find/model"
	"github.com/stretchr/testify/require"
)

func TestChainLevelExtendedProviderIsAsExpected(t *testing.T) {
	var want []model.ProviderResult
	err := json.Unmarshal([]byte(`[
  {
    "ContextID": "bG9ic3Rlcg==",
    "Metadata": "YmFycmVsZXll",
    "Provider": {
      "ID": "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC",
      "Addrs": [
        "/ip4/209.94.92.6/tcp/24001"
      ]
    }
  },
  {
    "ContextID": "bG9ic3Rlcg==",
    "Metadata": "gBI=",
    "Provider": {
      "ID": "12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h",
      "Addrs": [
        "/ip4/209.94.92.6/tcp/24123"
      ]
    }
  },
  {
    "ContextID": "bG9ic3Rlcg==",
    "Metadata": "oBIA",
    "Provider": {
      "ID": "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC",
      "Addrs": [
        "/ip4/209.94.92.6/tcp/7575/http"
      ]
    }
  }
]`), &want)
	require.NoError(t, err)

	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		_, err := res.Write([]byte(`{
  "AddrInfo": {
    "ID": "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC",
    "Addrs": [
      "/ip4/209.94.92.6/tcp/24001"
    ]
  },
  "LastAdvertisement": {
    "/": "baguqeerami47x7t3d4raobz7s2cq5np2fy2lhl3xh2ppiwuevxwbdamuikla"
  },
  "LastAdvertisementTime": "2023-06-28T12:21:06Z",
  "Publisher": {
    "ID": "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC",
    "Addrs": [
      "/ip4/209.94.92.6/tcp/24001"
    ]
  },
  "IndexCount": 157535217,
  "ExtendedProviders": {
    "Providers": [
      {
        "ID": "12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h",
        "Addrs": [
          "/ip4/209.94.92.6/tcp/24123"
        ]
      },
      {
        "ID": "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC",
        "Addrs": [
          "/ip4/209.94.92.6/tcp/7575/http"
        ]
      }
    ],
    "Metadatas": [
      "gBI=",
      "oBIA"
    ]
  },
  "FrozenAt": null
}`))
		require.NoError(t, err)
	}))
	defer func() { testServer.Close() }()

	u, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	subject, err := newProviderCache(u, nil)
	require.NoError(t, err)

	contextID := []byte("lobster")
	metadata := []byte("barreleye")
	got, err := subject.getResults(context.Background(), "fish", contextID, metadata)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
