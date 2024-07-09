package rwriter_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipni/go-libipni/rwriter"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestResponseWriter(t *testing.T) {
	const testHeaderKey = "X-testing"
	const testHeaderVal = "acbdefg"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respW, err := rwriter.New(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		prw := rwriter.NewProviderResponseWriter(respW)
		w = prw

		w.Header().Set(testHeaderKey, testHeaderVal)
		require.NoError(t, err)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if string(body) == "hello" {
			http.Error(w, "world", http.StatusAccepted)
			require.Equal(t, http.StatusAccepted, prw.StatusCode())
		} else {
			http.Error(w, "ok", http.StatusOK)
			require.Equal(t, http.StatusOK, prw.StatusCode())
		}
	}))
	defer ts.Close()
	cli := http.DefaultClient

	// Check that accept headers are required.
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/multihash/2DrjgbM2tfcpUE5imXMv3HnzryEaxd1FKh8DWMDEgtFkL7MDvT", bytes.NewBuffer([]byte("hello")))
	require.NoError(t, err)
	res, err := cli.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Equal(t, "accept header must be specified", strings.TrimSpace(string(body)))

	// Check good request with StatusAccpted
	req.Header.Set("Accept", "application/json")
	res, err = cli.Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, testHeaderVal, res.Header.Get(testHeaderKey))
	require.Equal(t, "world", strings.TrimSpace(string(body)))

	// Check good request StatusOK
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/multihash/2DrjgbM2tfcpUE5imXMv3HnzryEaxd1FKh8DWMDEgtFkL7MDvT", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	res, err = cli.Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, testHeaderVal, res.Header.Get(testHeaderKey))
	require.Equal(t, "ok", strings.TrimSpace(string(body)))

	// Check that bad resource type (not "multihash" or "cid") is caught.
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/badresource/xyz", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	require.NoError(t, err)
	res, err = cli.Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Equal(t, "unsupported resource type", strings.TrimSpace(string(body)))

	// Check that invalid multihash is handled.
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/multihash/aflk324vecr-903r-0", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	res, err = cli.Do(req)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, res.Body)
	require.NoError(t, err)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	b, err := base58.Decode("2DrjgbM2tfcpUE5imXMv3HnzryEaxd1FKh8DWMDEgtFkL7MDvT")
	require.NoError(t, err)

	// Check that hex multihash is handled.
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/multihash/"+hex.EncodeToString(b), nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	res, err = cli.Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, testHeaderVal, res.Header.Get(testHeaderKey))
	require.Equal(t, "ok", strings.TrimSpace(string(body)))
}
