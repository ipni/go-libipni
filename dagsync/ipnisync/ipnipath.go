package ipnisync

import "github.com/libp2p/go-libp2p/core/protocol"

const (
	// IPNIPath is the path that the Publisher expects as the last port of the
	// HTTP request URL path. The sync client automatically adds this to the
	// request path.
	IPNIPath = "/ipni/v1/ad"
	// ProtocolID is the libp2p protocol ID used to identify the ipni-sync
	// protocol. With libp2phttp this protocol ID maps directly to a single
	// HTTP path, so the value of the protocol ID is the same as the IPNI path
	// for the ipni-sync protocol.
	ProtocolID = protocol.ID(IPNIPath)
)
