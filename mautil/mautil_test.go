package mautil_test

import (
	"testing"

	"github.com/ipni/go-libipni/mautil"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestFilterPublic(t *testing.T) {
	addrs := []string{
		"/ip4/10.255.0.0/tcp/443",
		"/ip4/11.0.0.0/tcp/80",
		"/ip6/fc00::/tcp/1717",
		"/ip6/fe00::/tcp/8080",
		"/ip4/192.168.11.22/tcp/9999",
		"/dns4/example.net/tcp/1234",
		"/ip4/127.0.0.1/tcp/9999",
		"/dns4/localhost/tcp/1234",
		"/ip6/::/tcp/3105/http",
		"/ip4/0.0.0.0/tcp/3105",
	}

	maddrs, err := mautil.StringsToMultiaddrs(addrs)
	require.NoError(t, err)

	expected := []multiaddr.Multiaddr{maddrs[1], maddrs[5]}

	filtered := mautil.FilterPublic(maddrs)
	require.Equal(t, len(expected), len(filtered))

	for i := range filtered {
		require.Equal(t, expected[i], filtered[i])
	}

	filtered = mautil.FilterPublic(nil)
	require.Nil(t, filtered)
}

func TestFilterPublic_DoesNotPanicOnNilAddr(t *testing.T) {
	original := []multiaddr.Multiaddr{nil}
	got := mautil.FilterPublic(original)
	// According to the function documentation, it should return the original slice.
	require.Equal(t, original, got)
}

func TestFindHTTPAddrs(t *testing.T) {
	addrs := []string{
		"/ip4/11.0.0.0/tcp/80/http",
		"/ip6/fc00::/tcp/1717",
		"/ip6/fe00::/tcp/8080/https",
		"/dns4/example.net/tcp/1234",
	}
	maddrs, err := mautil.StringsToMultiaddrs(addrs)
	require.NoError(t, err)

	expected := []multiaddr.Multiaddr{maddrs[0], maddrs[2]}

	filtered := mautil.FindHTTPAddrs(maddrs)
	require.Equal(t, len(expected), len(filtered))

	for i := range filtered {
		require.Equal(t, expected[i], filtered[i])
	}

	filtered = mautil.FilterPublic(nil)
	require.Nil(t, filtered)
}

func TestCleanPeerAddrInfo(t *testing.T) {
	t.Run("all-nils", func(t *testing.T) {
		require.Len(t, mautil.CleanPeerAddrInfo(
			peer.AddrInfo{
				Addrs: make([]multiaddr.Multiaddr, 3),
			}).Addrs, 0)
	})

	goodAddrs, err := mautil.StringsToMultiaddrs([]string{
		"/ip4/11.0.0.0/tcp/80/http",
		"/ip6/fc00::/tcp/1717",
		"/ip6/fe00::/tcp/8080/https",
	})
	require.NoError(t, err)

	t.Run("nil-sandwich", func(t *testing.T) {
		subject := peer.AddrInfo{
			Addrs: make([]multiaddr.Multiaddr, 3),
		}
		subject.Addrs[1] = goodAddrs[0]
		cleaned := mautil.CleanPeerAddrInfo(subject)
		require.Len(t, cleaned.Addrs, 1)
		require.ElementsMatch(t, cleaned.Addrs, goodAddrs[:1])
	})
	t.Run("nil-head", func(t *testing.T) {
		subject := peer.AddrInfo{
			Addrs: make([]multiaddr.Multiaddr, 3),
		}
		subject.Addrs[1] = goodAddrs[0]
		subject.Addrs[2] = goodAddrs[1]
		cleaned := mautil.CleanPeerAddrInfo(subject)
		require.Len(t, cleaned.Addrs, 2)
		require.ElementsMatch(t, goodAddrs[:2], cleaned.Addrs)
	})
	t.Run("nil-tail", func(t *testing.T) {
		subject := peer.AddrInfo{
			Addrs: make([]multiaddr.Multiaddr, 3),
		}
		subject.Addrs[0] = goodAddrs[0]
		subject.Addrs[1] = goodAddrs[1]
		cleaned := mautil.CleanPeerAddrInfo(subject)
		require.Len(t, cleaned.Addrs, 2)
		require.ElementsMatch(t, goodAddrs[:2], cleaned.Addrs)
	})
	t.Run("no-nils", func(t *testing.T) {
		subject := peer.AddrInfo{
			Addrs: make([]multiaddr.Multiaddr, 3),
		}
		subject.Addrs[0] = goodAddrs[0]
		subject.Addrs[1] = goodAddrs[1]
		subject.Addrs[2] = goodAddrs[2]
		cleaned := mautil.CleanPeerAddrInfo(subject)
		require.Len(t, cleaned.Addrs, 3)
		require.ElementsMatch(t, goodAddrs, cleaned.Addrs)
	})
}

func TestMultiaddrsEqual(t *testing.T) {
	maddrs, err := mautil.StringsToMultiaddrs([]string{
		"/ip4/11.0.0.0/tcp/80/http",
		"/ip6/fc00::/tcp/1717",
		"/ip6/fe00::/tcp/8080/https",
		"/dns4/example.net/tcp/1234",
	})
	require.NoError(t, err)
	rev := make([]multiaddr.Multiaddr, len(maddrs))
	j := len(maddrs) - 1
	for i := 0; i <= j; i++ {
		rev[i] = maddrs[j-i]
	}
	m1 := make([]multiaddr.Multiaddr, len(maddrs))
	m2 := make([]multiaddr.Multiaddr, len(maddrs))

	copy(m1, maddrs)
	copy(m2, rev)
	require.True(t, mautil.MultiaddrsEqual(m1, m2))

	copy(m1, maddrs)
	copy(m2, rev)
	require.Truef(t, mautil.MultiaddrsEqual(m1[1:], m2[:len(m2)-1]), "m1=%v, m2=%v", m1[1:], m2[:len(m2)-1])

	copy(m1, maddrs)
	copy(m2, rev)
	require.True(t, mautil.MultiaddrsEqual(m1[2:3], m2[1:2]))

	copy(m1, maddrs)
	copy(m2, rev)
	require.True(t, mautil.MultiaddrsEqual(m1[:0], m2[:0]))

	require.True(t, mautil.MultiaddrsEqual(nil, nil))

	copy(m1, maddrs)
	copy(m2, rev)
	require.True(t, mautil.MultiaddrsEqual(m1[:0], nil))

	copy(m1, maddrs)
	copy(m2, rev)
	require.False(t, mautil.MultiaddrsEqual(m1[1:], m2[1:]))

	copy(m1, maddrs)
	copy(m2, rev)
	require.False(t, mautil.MultiaddrsEqual(m1, m2[:len(m2)-1]))

	copy(m1, maddrs)
	copy(m2, rev)
	require.False(t, mautil.MultiaddrsEqual(m1[:1], m2[:1]))
}
