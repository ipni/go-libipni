package schema_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	stischema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestAdvertisement_SignAndVerify(t *testing.T) {
	testSignAndVerify(t, func(a *stischema.Advertisement, pk crypto.PrivKey) error {
		return a.Sign(pk)
	})
}

func TestAdWithoutExtendedProvidersCanBeSignedAndVerified(t *testing.T) {
	testSignAndVerify(t, func(a *stischema.Advertisement, pk crypto.PrivKey) error {
		return a.SignWithExtendedProviders(pk, func(s string) (crypto.PrivKey, error) {
			return nil, fmt.Errorf("there are no extended providers")
		})
	})
}

func testSignAndVerify(t *testing.T, signer func(*stischema.Advertisement, crypto.PrivKey) error) {
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	peerID, priv, _ := random.Identity()

	ec := stischema.EntryChunk{
		Entries: random.Multihashes(10),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	adv := stischema.Advertisement{
		Provider: "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA",
		Addresses: []string{
			"/ip4/127.0.0.1/tcp/9999",
		},
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
	}
	err = signer(&adv, priv)
	require.NoError(t, err)

	signerID, err := adv.VerifySignature()
	require.NoError(t, err)
	require.Equal(t, peerID, signerID)

	// Show that signature can be valid even though advertisement not signed by
	// provider ID.  This is why it is necessary to check that the signer ID is
	// the expected signed after verifying the signature is valid.
	provID, err := peer.Decode(adv.Provider)
	require.NoError(t, err)
	require.NotEqual(t, signerID, provID)

	// Verification fails if something in the advertisement changes
	adv.Provider = ""
	_, err = adv.VerifySignature()
	require.NotNil(t, err)
}

func TestSignShouldFailIfAdHasExtendedProviders(t *testing.T) {
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	peerID, priv, _ := random.Identity()

	ec := stischema.EntryChunk{
		Entries: random.Multihashes(10),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1PeerID, _, _ := random.Identity()

	adv := stischema.Advertisement{
		Provider: peerID.String(),
		Addresses: []string{
			"/ip4/127.0.0.1/tcp/9999",
		},
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: random.Addrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}
	err = adv.Sign(priv)
	require.Error(t, err, "the ad can not be signed because it has extended providers")
}

func TestSignWithExtendedProviderAndVerify(t *testing.T) {
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: random.Multihashes(10),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1PeerID, ep1Priv, _ := random.Identity()
	ep2PeerID, ep2Priv, _ := random.Identity()
	mpPeerID, mpPriv, _ := random.Identity()
	mpAddrs := random.Addrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: random.Addrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
				{
					ID:        ep2PeerID.String(),
					Addresses: random.Addrs(2),
					Metadata:  []byte("ep2-metadata"),
				},
				{
					ID:        mpPeerID.String(),
					Addresses: mpAddrs,
					Metadata:  []byte("main-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		case ep2PeerID.String():
			return ep2Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.NoError(t, err)

	signerID, err := adv.VerifySignature()
	require.NoError(t, err)
	require.Equal(t, mpPeerID, signerID)
	require.Equal(t, signerID, mpPeerID)
}

func TestSigVerificationFailsIfTheAdProviderIdentityIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		randomID, _, _ := random.Identity()
		adv.Provider = randomID.String()
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderIdentityIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		// main provider is the first one on the list
		randomID, _, _ := random.Identity()
		adv.ExtendedProvider.Providers[1].ID = randomID.String()
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderMetadataIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		rng := rand.New(rand.NewSource(time.Now().Unix()))
		meta := make([]byte, 10)
		rng.Read(meta)
		adv.ExtendedProvider.Providers[1].Metadata = meta
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderAddrsAreIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ExtendedProvider.Providers[1].Addresses = random.Addrs(10)
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfOverrideIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ExtendedProvider.Override = !adv.ExtendedProvider.Override
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfContextIDIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ContextID = []byte("ABC")
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfMainProviderIsNotInExtendedList(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		// main provider is the first one on the list
		adv.ExtendedProvider.Providers = adv.ExtendedProvider.Providers[1:]
		_, err := adv.VerifySignature()
		require.Error(t, err)
		require.Equal(t, "extended providers must contain provider from the encapsulating advertisement", err.Error())
	})
}

func TestSignFailsIfMainProviderIsNotInExtendedList(t *testing.T) {
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: random.Multihashes(10),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1PeerID, ep1Priv, _ := random.Identity()
	mpPeerID, mpPriv, _ := random.Identity()
	mpAddrs := random.Addrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: random.Addrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.Error(t, err, "extended providers must contain provider from the encapsulating advertisement")
}

func extendedSignatureTest(t *testing.T, testFunc func(adv stischema.Advertisement)) {
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: random.Multihashes(10),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1PeerID, ep1Priv, _ := random.Identity()
	mpPeerID, mpPriv, _ := random.Identity()
	mpAddrs := random.Addrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        mpPeerID.String(),
					Addresses: mpAddrs,
					Metadata:  []byte("main-metadata"),
				},
				{
					ID:        ep1PeerID.String(),
					Addresses: random.Addrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.NoError(t, err)

	_, err = adv.VerifySignature()
	require.NoError(t, err)

	testFunc(adv)
}
