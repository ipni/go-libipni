package httpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardingClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := r.URL.Path
		if r.Method != http.MethodGet {
			require.Equal(t, "", r.Header.Get(shardKeyHeader))
			return
		}
		if strings.Contains(u, "cid") {
			require.Equal(t, "QmUtQvv4erpcYDyoL8jgzXHqy9dQUi3a5gGTVDSdDicw4x", r.Header.Get(shardKeyHeader))
		} else if strings.Contains(u, "multihash") {
			require.Equal(t, "QmZ7nrfFMcrnroRWkZCAiALDEYK5Z5gkEFsSMAaoFfQmAw", r.Header.Get(shardKeyHeader))
		} else if strings.Contains(u, "metadata") {
			require.Equal(t, "ABCD", r.Header.Get(shardKeyHeader))
		} else {
			require.Equal(t, "", r.Header.Get(shardKeyHeader))
		}
	}))

	c := NewShardingClient()
	sendRequest(t, c, server.URL+"/multihash/QmZ7nrfFMcrnroRWkZCAiALDEYK5Z5gkEFsSMAaoFfQmAw", http.MethodGet)
	sendRequest(t, c, server.URL+"/cid/bafybeidbjeqjovk2zdwh2dngy7tckid7l7qab5wivw2v5es4gphqxvsqqu", http.MethodGet)
	sendRequest(t, c, server.URL+"/metadata/ABCD", http.MethodGet)
	sendRequest(t, c, server.URL+"/someOthePath/ABCD", http.MethodGet)
	sendRequest(t, c, server.URL+"/someOthePath/ABCD", http.MethodPost)
}

func sendRequest(t *testing.T, c *http.Client, u, method string) {
	req, err := http.NewRequestWithContext(context.Background(), method, u, nil)
	require.NoError(t, err)
	_, err = c.Do(req)
	require.NoError(t, err)
}
