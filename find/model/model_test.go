package model_test

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func TestMarshal(t *testing.T) {
	// Generate some multihashes and populate indexer
	metadata := []byte("test-metadata")
	ctxID := []byte("test-context-id")
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	m2, _ := multiaddr.NewMultiaddr("/dns4/ipni.io/tcp/443/https/httpath/http-cid-data")

	providerResult := model.ProviderResult{
		ContextID: ctxID,
		Metadata:  metadata,
		Provider: &peer.AddrInfo{
			ID:    p,
			Addrs: []multiaddr.Multiaddr{m1, m2},
		},
	}

	// Masrhal response and check e2e
	resp := &model.FindResponse{
		MultihashResults: []model.MultihashResult{},
	}

	mhs := random.Multihashes(3)
	for i := range mhs {
		resp.MultihashResults = append(resp.MultihashResults, model.MultihashResult{
			Multihash:       mhs[i],
			ProviderResults: []model.ProviderResult{providerResult},
		})
	}

	b, err := model.MarshalFindResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	r2, err := model.UnmarshalFindResponse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !equalMultihashResult(resp.MultihashResults, r2.MultihashResults) {
		t.Fatal("failed marshal/unmarshaling response")
	}

}

func equalMultihashResult(res1, res2 []model.MultihashResult) bool {
	if len(res1) != len(res2) {
		return false
	}
	for i, r1 := range res1 {
		r2 := res2[i]
		if !bytes.Equal([]byte(r1.Multihash), []byte(r2.Multihash)) {
			return false
		}
		if len(r1.ProviderResults) != len(r2.ProviderResults) {
			return false
		}
		for j, pr1 := range r1.ProviderResults {
			if !pr1.Equal(r2.ProviderResults[j]) {
				return false
			}
		}
	}
	return true
}
