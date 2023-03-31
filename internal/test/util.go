package test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var globalSeed atomic.Int64

func RandomAddrs(n int) []string {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%10751)
	}
	return addrs
}

func RandomCids(t testing.TB, n int) []cid.Cid {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))

	prefix := schema.Linkproto.Prefix

	cids := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rng.Read(b)
		c, err := prefix.Sum(b)
		require.NoError(t, err)
		cids[i] = c
	}
	return cids
}

func RandomIdentity(t *testing.T) (peer.ID, crypto.PrivKey, crypto.PubKey) {
	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	providerID, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)
	return providerID, privKey, pubKey
}

func RandomMultihashes(n int) []multihash.Multihash {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))

	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		rng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error())
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}

func StringToMultiaddrs(t *testing.T, addrs []string) []multiaddr.Multiaddr {
	mAddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		require.NoError(t, err)
		mAddrs[i] = ma
	}
	return mAddrs
}
