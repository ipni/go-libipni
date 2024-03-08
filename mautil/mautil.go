// Package mautil provides multiaddr utility functions.
package mautil

import (
	"bytes"
	"net"
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// FilterPublic returns a new slice of multiaddrs with any private, loopback,
// or unspecified multiaddrs removed.
func FilterPublic(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	filtered := multiaddr.FilterAddrs(maddrs, func(target multiaddr.Multiaddr) bool {
		if target == nil {
			return true
		}
		c, _ := multiaddr.SplitFirst(target)
		if c == nil {
			return false
		}
		switch c.Protocol().Code {
		case multiaddr.P_IP4, multiaddr.P_IP6, multiaddr.P_IP6ZONE, multiaddr.P_IPCIDR:
			return manet.IsPublicAddr(target) && !manet.IsIPUnspecified(target)
		case multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6, multiaddr.P_DNSADDR:
			return c.Value() != "localhost"
		}
		return true
	})
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func FindHTTPAddrs(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	return multiaddr.FilterAddrs(maddrs, func(target multiaddr.Multiaddr) bool {
		if target != nil {
			for _, p := range target.Protocols() {
				if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
					return true
				}
			}
		}
		return false
	})
}

func MultiaddrStringToNetAddr(maddrStr string) (net.Addr, error) {
	maddr, err := multiaddr.NewMultiaddr(maddrStr)
	if err != nil {
		return nil, err
	}
	return manet.ToNetAddr(maddr)
}

// ParsePeers parses a list of multiaddr strings into a list of AddrInfo.
func ParsePeers(addrs []string) ([]peer.AddrInfo, error) {
	maddrs, err := StringsToMultiaddrs(addrs)
	if err != nil {
		return nil, err
	}
	return peer.AddrInfosFromP2pAddrs(maddrs...)
}

func StringsToMultiaddrs(addrs []string) ([]multiaddr.Multiaddr, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	var lastErr error
	maddrs := make([]multiaddr.Multiaddr, 0, len(addrs))
	for i := range addrs {
		maddr, err := multiaddr.NewMultiaddr(addrs[i])
		if err != nil {
			lastErr = err
			continue
		}
		maddrs = append(maddrs, maddr)
	}
	return maddrs, lastErr
}

func CleanPeerAddrInfo(target peer.AddrInfo) peer.AddrInfo {
	for i := 0; i < len(target.Addrs); {
		if target.Addrs[i] == nil {
			target.Addrs[i] = target.Addrs[len(target.Addrs)-1]
			target.Addrs[len(target.Addrs)-1] = nil
			target.Addrs = target.Addrs[:len(target.Addrs)-1]
			continue
		}
		i++
	}
	return target
}

func MultiaddrsEqual(ma1, ma2 []multiaddr.Multiaddr) bool {
	if len(ma1) != len(ma2) {
		return false
	}
	if len(ma1) == 0 {
		return true // both are nil or empty
	}
	if len(ma1) == 1 {
		return ma1[0].Equal(ma2[0])
	}
	// Use slices package, as sort function does not allocate (sort.Slice does).
	slices.SortFunc(ma1, func(a, b multiaddr.Multiaddr) int { return bytes.Compare(a.Bytes(), b.Bytes()) })
	slices.SortFunc(ma2, func(a, b multiaddr.Multiaddr) int { return bytes.Compare(a.Bytes(), b.Bytes()) })
	for i := 0; i < len(ma1); i++ {
		if !ma1[i].Equal(ma2[i]) {
			return false
		}
	}
	return true
}
