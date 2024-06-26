package dhash

import (
	"bytes"
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestSalt(t *testing.T) {
	salt_len := 64
	require.Equal(t, len(secondHashPrefix), salt_len)
	require.Equal(t, len(deriveKeyPrefix), salt_len)
	require.Equal(t, len(noncePrefix), salt_len)
}

func TestEncryptSameValueWithTheSameMultihashShouldProduceTheSameOutput(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	payload := make([]byte, 256)
	_, err := rng.Read(payload)
	if err != nil {
		panic(err)
	}
	passphrase := make([]byte, 32)
	_, err = rng.Read(passphrase)
	require.NoError(t, err)

	nonce1, encrypted1, err := EncryptAES(payload, passphrase)
	require.NoError(t, err)

	nonce2, encrypted2, err := EncryptAES(payload, passphrase)
	require.NoError(t, err)

	require.True(t, bytes.Equal(nonce1, nonce2))
	require.True(t, bytes.Equal(encrypted1, encrypted2))
}

func TestCanDecryptEncryptedValue(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	payload := make([]byte, 256)
	_, err := rng.Read(payload)
	if err != nil {
		panic(err)
	}
	passphrase := make([]byte, 32)
	_, err = rng.Read(passphrase)
	require.NoError(t, err)

	nonce, encrypted, err := EncryptAES(payload, passphrase)
	require.NoError(t, err)

	decrypted, err := DecryptAES(nonce, encrypted, passphrase)
	require.NoError(t, err)

	require.True(t, bytes.Equal(payload, decrypted))
}

func TestSecondMultihash(t *testing.T) {
	mh := random.Multihashes(1)[0]
	smh := SecondMultihash(mh)

	h := sha256.New()
	h.Write(append(secondHashPrefix, mh...))
	digest := h.Sum(nil)

	decoded, err := multihash.Decode(smh)
	require.NoError(t, err)

	require.Equal(t, uint64(multihash.DBL_SHA2_256), decoded.Code)
	require.Equal(t, digest, decoded.Digest)
}
