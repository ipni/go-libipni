package pcache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var pid1, pid2 peer.ID

func init() {
	var err error
	pid1, err = peer.Decode("12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC")
	if err != nil {
		panic(err)
	}
	pid2, err = peer.Decode("12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h")
	if err != nil {
		panic(err)
	}
}

type mockSource struct {
	infos []*model.ProviderInfo

	callFetch    atomic.Int32
	callFetchAll atomic.Int32
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
	s.callFetch.Add(1)
	for _, info := range s.infos {
		if pid == info.AddrInfo.ID {
			return info, nil
		}
	}
	return nil, nil
}

func (s *mockSource) FetchAll(ctx context.Context) ([]*model.ProviderInfo, error) {
	s.callFetchAll.Add(1)
	return s.infos, nil
}

func (s *mockSource) String() string {
	return "mockSource"
}

func TestProviderCache(t *testing.T) {
	src := newMockSource(pid1)
	pc, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)
	require.Equal(t, 1, pc.Len())
	require.Equal(t, int32(1), src.callFetchAll.Load())

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

	pinfos := pc.List()
	require.Equal(t, 2, len(pinfos))
}

func TestOverlappingSources(t *testing.T) {
	now := time.Now()

	src1 := newMockSource(pid1)
	src2 := newMockSource(pid2, pid1)
	src2.infos[1].LastAdvertisementTime = now.Add(time.Second).Format(time.RFC3339)

	pc, err := pcache.New(pcache.WithSource(src1, src2))
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

	pi, err := src.Fetch(context.Background(), pid1)
	require.NoError(t, err)
	require.NotNil(t, pi)
	require.Equal(t, pid1, pi.AddrInfo.ID)

	_, err = src.Fetch(context.Background(), pid2)
	require.Error(t, err)

	pis, err := src.FetchAll(context.Background())
	require.NoError(t, err)
	require.NotZero(t, len(pis))
	require.Equal(t, pid1, pis[0].AddrInfo.ID)

	// Test ProviderCache
	subject, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)
	require.Equal(t, 1, subject.Len())

	contextID := []byte("lobster")
	metadata := []byte("barreleye")
	got, err := subject.GetResults(context.Background(), pid1, contextID, metadata)
	require.NoError(t, err)
	require.Equal(t, want, got)

	got, err = subject.GetResults(context.Background(), pid2, contextID, metadata)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestNoPreload(t *testing.T) {
	src1 := newMockSource(pid1)
	src2 := newMockSource(pid2)

	pc, err := pcache.New(pcache.WithSource(src1, src2), pcache.WithPreload(false))
	require.NoError(t, err)
	require.Zero(t, pc.Len())
	require.Zero(t, src1.callFetchAll.Load())
	require.Zero(t, src2.callFetchAll.Load())

	pinfos := pc.List()
	require.Zero(t, len(pinfos))

	pinfo, err := pc.Get(context.Background(), pid1)
	require.NoError(t, err)
	require.NotNil(t, pinfo)
	require.Equal(t, 1, pc.Len())

	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.NotNil(t, pinfo)
	require.Equal(t, 2, pc.Len())
}

func TestNoTimestamp(t *testing.T) {
	src1 := newMockSource(pid1)
	src2 := newMockSource(pid2)

	// Test get missing and refresh with no timestamp.
	src1.infos[0].LastAdvertisementTime = ""
	src2.infos[0].LastAdvertisementTime = ""
	pc, err := pcache.New(pcache.WithSource(src1, src2), pcache.WithPreload(false))
	require.NoError(t, err)
	require.Zero(t, pc.Len())

	pinfo, err := pc.Get(context.Background(), pid1)
	require.NoError(t, err)
	require.NotNil(t, pinfo)
	require.Equal(t, 1, pc.Len())

	err = pc.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, pc.Len())
}

func TestAutoRefresh(t *testing.T) {
	src1 := newMockSource(pid1)

	pc, err := pcache.New(pcache.WithSource(src1), pcache.WithRefreshInterval(200*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, 1, pc.Len())
	require.Equal(t, int32(1), src1.callFetchAll.Load())

	_, err = pc.Get(context.Background(), pid1)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(1), src1.callFetchAll.Load())

	_, err = pc.Get(context.Background(), pid1)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(2), src1.callFetchAll.Load())
}

func TestTTL(t *testing.T) {
	src := newMockSource(pid1)
	pc, err := pcache.New(pcache.WithSource(src), pcache.WithRefreshInterval(0),
		pcache.WithTTL(200*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, 1, pc.Len())
	require.Equal(t, int32(1), src.callFetchAll.Load())

	// Test TTL of disappeared provider
	origInfos := src.infos
	src.infos = nil
	require.NoError(t, pc.Refresh(context.Background()))
	require.Equal(t, 1, len(pc.List()))

	time.Sleep(220 * time.Millisecond)
	require.NoError(t, pc.Refresh(context.Background()))
	require.Zero(t, len(pc.List()))

	// Provider reappears.
	src.infos = origInfos
	require.NoError(t, pc.Refresh(context.Background()))
	require.Equal(t, 1, len(pc.List()))

	// Test TTL of negative entry
	pinfo, err := pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)
	require.Equal(t, int32(1), src.callFetch.Load())

	src.infos = nil // Cause cache update that moves neg entry to main map
	require.NoError(t, pc.Refresh(context.Background()))
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)
	require.Equal(t, int32(1), src.callFetch.Load())

	// Refresh after TTL should remove negative cache entry, and next Get
	// should call Fetch again.
	time.Sleep(220 * time.Millisecond)
	require.NoError(t, pc.Refresh(context.Background()))
	pinfo, err = pc.Get(context.Background(), pid2)
	require.NoError(t, err)
	require.Nil(t, pinfo)
	require.Equal(t, int32(2), src.callFetch.Load())
}
