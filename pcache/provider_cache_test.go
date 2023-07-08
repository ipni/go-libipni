package pcache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

type mockSource struct {
	infos []*model.ProviderInfo

	callFetchAll int
}

func newMockSource(pids ...peer.ID) *mockSource {
	s := &mockSource{}
	for _, pid := range pids {
		s.addInfo(pid)
	}
	return s
}

func (s *mockSource) addInfo(pid peer.ID) {
	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/192.168.0.%d/tcp/24001", len(s.infos)+2))
	if err != nil {
		panic(err)
	}
	info := &model.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    pid,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
		LastAdvertisementTime: time.Now().Format(time.RFC3339),
	}
	s.infos = append(s.infos, info)
}

func (s *mockSource) Fetch(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	for _, info := range s.infos {
		if pid == info.AddrInfo.ID {
			return info, nil
		}
	}
	return nil, nil
}

func (s *mockSource) FetchAll(ctx context.Context) ([]*model.ProviderInfo, error) {
	s.callFetchAll++
	return s.infos, nil
}

func TestProviderCache(t *testing.T) {
	pid1, err := peer.Decode("12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC")
	require.NoError(t, err)

	pid2, err := peer.Decode("12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h")
	require.NoError(t, err)

	src := newMockSource(pid1)
	pc, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)
	require.Equal(t, 1, pc.Len())
	require.Equal(t, 1, src.callFetchAll)

	// Cache hit main
	pinfo, err := pc.Get(context.Background(), pid1)
	require.NoError(t, err)
	require.NotNil(t, pinfo)

	// Cache miss
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)
	require.Equal(t, 2, pc.Len())

	// Negative cache hit update
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)

	err = pc.Refresh(context.Background())
	require.NoError(t, err)

	// Negative cache hit main
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)

	src.addInfo(pid2)

	// Should still get negative cache hit.
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)

	// Refresh single provider.
	err = pc.Refresh(context.Background())
	require.NoError(t, err)

	// Should see new provider now.
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.NotNil(t, pinfo)
	require.Equal(t, pid2, pinfo.AddrInfo.ID)
}

func TestOverlappingSources(t *testing.T) {
	pid1, err := peer.Decode("12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC")
	require.NoError(t, err)

	pid2, err := peer.Decode("12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h")
	require.NoError(t, err)

	now := time.Now()

	src1 := newMockSource(pid1)
	src2 := newMockSource(pid2, pid1)
	src2.infos[1].LastAdvertisementTime = now.Add(time.Second).Format(time.RFC3339)

	pc, err := pcache.New(pcache.WithSource(src1), pcache.WithSource(src2))
	require.NoError(t, err)
	require.Equal(t, 2, pc.Len())

	// Check that provider pid1 came from src2, since it had the later
	// timestamp.
	pinfo, err := pc.Get(context.Background(), pid1)
	require.NoError(t, err)
	require.Equal(t, src2.infos[1].AddrInfo.Addrs[0], pinfo.AddrInfo.Addrs[0])
	require.NotEqual(t, src1.infos[0].AddrInfo.Addrs[0], pinfo.AddrInfo.Addrs[0])

	// Make src1 have newer timestamp for pid1.
	src1.infos[0].LastAdvertisementTime = now.Add(5 * time.Second).Format(time.RFC3339)
	err = pc.Refresh(context.Background())
	require.NoError(t, err)

	// Refresh and check that provider pid1 came from src1.
	pinfo, err = pc.Get(context.Background(), pid1)
	require.NoError(t, err)
	require.NotEqual(t, src2.infos[1].AddrInfo.Addrs[0], pinfo.AddrInfo.Addrs[0])
	require.Equal(t, src1.infos[0].AddrInfo.Addrs[0], pinfo.AddrInfo.Addrs[0])
}

func TestChainLevelExtendedProviderIsAsExpected(t *testing.T) {
	pid, err := peer.Decode("12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC")
	require.NoError(t, err)

	pid2, err := peer.Decode("12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h")
	require.NoError(t, err)

	var want []model.ProviderResult
	err = json.Unmarshal([]byte(`[
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

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		pinfo := `{
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
}`
		if req.URL.Path == "/providers" {
			_, err := w.Write([]byte(`[` + pinfo + `]`))
			require.NoError(t, err)
			return
		}
		if path.Base(req.URL.Path) != "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC" {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		_, err := w.Write([]byte(pinfo))
		require.NoError(t, err)
	}))
	defer testServer.Close()

	// Test HTTP source.
	src, err := pcache.NewHTTPSource(testServer.URL, nil)
	require.NoError(t, err)

	pi, err := src.Fetch(context.Background(), pid)
	require.NoError(t, err)
	require.NotNil(t, pi)
	require.Equal(t, pid, pi.AddrInfo.ID)

	_, err = src.Fetch(context.Background(), pid2)
	require.Error(t, err)

	pis, err := src.FetchAll(context.Background())
	require.NoError(t, err)
	require.NotZero(t, len(pis))
	require.Equal(t, pid, pis[0].AddrInfo.ID)

	// Test ProviderCache
	subject, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)
	require.Equal(t, 1, subject.Len())

	contextID := []byte("lobster")
	metadata := []byte("barreleye")
	got, err := subject.GetResults(context.Background(), pid, contextID, metadata)
	require.NoError(t, err)
	require.Equal(t, want, got)

	got, err = subject.GetResults(context.Background(), pid2, contextID, metadata)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestWithLiveSite(t *testing.T) {
	t.Skip("Only for manual testing")

	src, err := pcache.NewHTTPSource("https://inga.prod.cid.contact", nil)
	require.NoError(t, err)

	pc, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)
	t.Log("Cache size:", pc.Len())

	pid, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
	require.NoError(t, err)

	pinfo, err := pc.Get(context.Background(), pid)
	require.NoError(t, err)
	require.NotNil(t, pinfo)

	// Providers with extended provider info should have hade updates within
	// this time window. Updates should not be expected unless addresses or
	// extended info changes.
	t.Log("Waiting 20s to refresh and check for updates")
	time.Sleep(20 * time.Second)
	err = pc.Refresh(context.Background())
	require.NoError(t, err)
	t.Log("Updates last refresh:", pc.UpdatesLastRefresh())
}
