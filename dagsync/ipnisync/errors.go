package ipnisync

import "errors"

// ErrMalformed is wrapped by each error that comes from content a publisher
// serves and that no later sync can change, such as a block larger than the
// maximum block size or an identity CID whose digest is too large. A caller
// that syncs the entries of an advertisement can skip the advertisement
// instead of retrying it, so that the rest of the advertisement chain is
// still ingested.
var ErrMalformed = errors.New("malformed content")
