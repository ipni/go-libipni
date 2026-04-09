package model

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/require"
)

func TestRegisterRequest(t *testing.T) {
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	peerID, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	data, err := MakeRegisterRequest(peerID, privKey, addrs)
	require.NoError(t, err)

	peerRec, err := ReadRegisterRequest(data)
	require.NoError(t, err)

	seq0 := peerRec.Seq

	data, err = MakeRegisterRequest(peerID, privKey, addrs)
	require.NoError(t, err)

	peerRec, err = ReadRegisterRequest(data)
	require.NoError(t, err)

	require.Less(t, seq0, peerRec.Seq, "sequence not greater than last seen")
}
