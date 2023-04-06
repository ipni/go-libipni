package test

import (
	"fmt"
	"math/rand"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var globalSeed atomic.Int64

// RandomAddrs returns a slice of n random unique addresses.
func RandomAddrs(n int) []string {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	addrs := make([]string, n)
	addrSet := make(map[string]struct{})
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(48157)+1024)
		if _, ok := addrSet[addr]; ok {
			i--
			continue
		}
		addrs[i] = addr
	}
	return addrs
}

// RandomMultiaddrs returns a slice of n random unique Multiaddrs.
func RandomMultiaddrs(n int) []multiaddr.Multiaddr {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	maddrs := make([]multiaddr.Multiaddr, n)
	addrSet := make(map[string]struct{})
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(48157)+1024)
		if _, ok := addrSet[addr]; ok {
			i--
			continue
		}
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			panic(err)
		}
		maddrs[i] = maddr
	}
	return maddrs
}

// RandomCids returns a slice of n random unique CIDs.
func RandomCids(n int) []cid.Cid {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	prefix := schema.Linkproto.Prefix
	cids := make([]cid.Cid, n)
	set := make(map[string]struct{})
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rng.Read(b)
		if _, ok := set[string(b)]; ok {
			i--
			continue
		}
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err)
		}
		cids[i] = c
	}
	return cids
}

// RandomIdentity returns a random unique peer ID, private key, and public key.
func RandomIdentity() (peer.ID, crypto.PrivKey, crypto.PubKey) {
retry:
	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		panic(err)
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		panic(err)
	}
	idSet := make(map[peer.ID]struct{})
	if _, ok := idSet[peerID]; ok {
		goto retry
	}
	return peerID, privKey, pubKey
}

// RandomMultihashes returns a slice of n random unique Multihashes.
func RandomMultihashes(n int) []multihash.Multihash {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	prefix := schema.Linkproto.Prefix
	set := make(map[string]struct{})
	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		rng.Read(b)
		if _, ok := set[string(b)]; ok {
			i--
			continue
		}
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error())
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}
