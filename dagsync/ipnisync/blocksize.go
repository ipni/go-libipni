package ipnisync

import (
	"errors"
	"fmt"
)

const (
	// DefaultMaxBlockSize is the default largest block, in bytes, that the
	// sync client accepts from a publisher. An entry chunk of 16384 sha2-256
	// multihashes is about 1.1 MiB in dag-json.
	DefaultMaxBlockSize = 4 << 20
	// MaxHeadSize is the largest signed head message, in bytes, that the sync
	// client accepts from a publisher.
	MaxHeadSize = 64 << 10
)

// ErrBlockTooLarge is returned when a publisher sends a block larger than the
// sync client's maximum block size. A block over the limit is refused on every
// sync under the same limit, so it wraps ErrMalformed.
//
// The size is checked before the bytes are checked against the CID, so that
// the client never reads more than the limit. A publisher, or a proxy in front
// of it, that sends too many bytes of the wrong content for a CID gets the
// same error. A caller that skips an advertisement on ErrMalformed skips it
// under the limit in force at the time: raising the limit later does not
// bring the advertisement back.
var ErrBlockTooLarge = fmt.Errorf("%w: block is larger than the maximum block size", ErrMalformed)

// ErrHeadTooLarge is returned when a publisher sends a head message larger
// than MaxHeadSize. A publisher can serve a different head on a later sync,
// so it does not wrap ErrMalformed.
var ErrHeadTooLarge = errors.New("head message is larger than the maximum head size")
