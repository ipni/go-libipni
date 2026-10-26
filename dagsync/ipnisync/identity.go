package ipnisync

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

// MaxIdentityDigestSize is the largest identity CID digest, in bytes, that is
// accepted. It matches the default limit of boxo's verifcid. A larger block
// must be linked by a hashed CID.
const MaxIdentityDigestSize = 128

// ErrIdentityTooLarge is returned for an identity CID whose digest is larger
// than MaxIdentityDigestSize. Such a CID can never be synced, so callers
// should treat it as a permanent failure, not retry it.
var ErrIdentityTooLarge = fmt.Errorf("identity cid digest is larger than %d bytes", MaxIdentityDigestSize)

// CheckIdentitySize returns ErrIdentityTooLarge if c is an identity CID whose
// digest is larger than MaxIdentityDigestSize. Any other CID passes. It does
// not allocate unless it returns an error.
func CheckIdentitySize(c cid.Cid) error {
	if p := c.Prefix(); p.MhType == multihash.IDENTITY {
		return checkDigestSize(c, p.MhLength)
	}
	return nil
}

// identityDigestStr returns the digest of the identity CID c with prefix p, read
// in place from the CID. The digest is returned as a string, a slice of the
// CID's own string, so that no bytes are copied.
//
// It fails unless the length of c is the encoded length of p plus the digest
// length, which a CID built without validation, such as by cid.NewCidV1, may
// not match.
func identityDigestStr(c cid.Cid, p cid.Prefix) (string, error) {
	headerLen := varint.UvarintSize(p.Version) +
		varint.UvarintSize(p.Codec) +
		varint.UvarintSize(p.MhType) +
		varint.UvarintSize(uint64(p.MhLength))
	key := c.KeyString()
	if p.MhLength < 0 || len(key) != headerLen+p.MhLength {
		return "", fmt.Errorf("malformed identity cid %s", c)
	}
	return key[headerLen:], nil
}

func checkDigestSize(c cid.Cid, size int) error {
	if size > MaxIdentityDigestSize {
		return fmt.Errorf("%w: cid %s has %d bytes", ErrIdentityTooLarge, c, size)
	}
	return nil
}
