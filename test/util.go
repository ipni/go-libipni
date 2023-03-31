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
	"github.com/multiformats/go-multihash"
)

var globalSeed atomic.Int64

func RandomAddrs(n int) []string {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(48157)+1024)
	}
	return addrs
}

func RandomCids(n int) []cid.Cid {
	rng := rand.New(rand.NewSource(globalSeed.Add(1)))
	prefix := schema.Linkproto.Prefix
	cids := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err)
		}
		cids[i] = c
	}
	return cids
}

func RandomIdentity() (peer.ID, crypto.PrivKey, crypto.PubKey) {
	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		panic(err)
	}
	providerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		panic(err)
	}
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
