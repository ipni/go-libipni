package head_test

import (
	"bytes"
	"crypto/rand"
	_ "embed"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	headschema "github.com/ipni/go-libipni/dagsync/ipnisync/head"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const testTopic = "/test/topic"

func TestSignedHeadWrapUnwrap(t *testing.T) {
	head := generateSignedHead()
	gotNode, err := head.ToNode()
	require.NoError(t, err)

	head2, err := headschema.UnwrapSignedHead(gotNode)
	require.NoError(t, err)
	require.Equal(t, head, head2)
}

func TestSignedHeadEncodeDecode(t *testing.T) {
	head := generateSignedHead()
	data, err := head.Encode()
	require.NoError(t, err)

	decodedHead, err := headschema.Decode(bytes.NewBuffer(data))
	require.NoError(t, err)
	require.Equal(t, head, decodedHead)
}

func TestSignedHeadValidation(t *testing.T) {
	head := generateSignedHead()

	// Test good.
	signerID, err := head.Validate()
	require.NoError(t, err)
	require.NotEqual(t, peer.ID(""), signerID)

	// Test missing public key.
	pubkey := head.Pubkey
	head.Pubkey = nil
	_, err = head.Validate()
	require.ErrorIs(t, err, headschema.ErrNoPubkey)

	// Test missing signature.
	head.Pubkey = pubkey
	head.Sig = nil
	_, err = head.Validate()
	require.ErrorIs(t, err, headschema.ErrNoSignature)

	// Replace the public key with the wrong one.
	head = generateSignedHead()
	head.Pubkey = pubkey
	_, err = head.Validate()
	require.ErrorIs(t, err, headschema.ErrBadSignature)
}

func generateSignedHead() *headschema.SignedHead {
	privKey, _, err := ic.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err.Error())
	}
	mhs := random.Multihashes(1)
	headCid := cid.NewCidV1(cid.Raw, mhs[0])

	sh, err := headschema.NewSignedHead(headCid, testTopic, privKey)
	if err != nil {
		panic(err.Error())
	}

	return sh
}
