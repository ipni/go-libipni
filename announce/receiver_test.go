package announce_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/announce"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const testTopic = "/announce/testtopic"

var (
	testCid      cid.Cid
	testCid2     cid.Cid
	testPeerInfo peer.AddrInfo
)

func init() {
	cids := random.Cids(2)
	testCid = cids[0]
	testCid2 = cids[1]

	testPeerInfo = random.AddrInfos(1, 1)[0]
}

func TestReceiverBasic(t *testing.T) {
	srcHost, _ := libp2p.New()
	t.Cleanup(func() { srcHost.Close() })
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	err = rcvr.Direct(context.Background(), testCid, testPeerInfo)
	require.NoError(t, err)

	amsg, err := rcvr.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, testPeerInfo.ID, amsg.PeerID)

	require.NoError(t, rcvr.Close())
}

func TestReceiverCloseWaitingNext(t *testing.T) {
	srcHost, _ := libp2p.New()
	t.Cleanup(func() { srcHost.Close() })
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	// Test close while Next is waiting.
	errChan := make(chan error)
	go func() {
		_, nextErr := rcvr.Next(context.Background())
		errChan <- nextErr
	}()

	require.NoError(t, rcvr.Close())

	select {
	case err = <-errChan:
		require.ErrorIs(t, err, announce.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error return")
	}

}

func TestReceiverCloseWaitingDirect(t *testing.T) {
	srcHost, _ := libp2p.New()
	t.Cleanup(func() { srcHost.Close() })
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	// Test close while Direct is waiting.
	errChan := make(chan error)
	go func() {
		directErr := rcvr.Direct(context.Background(), testCid, testPeerInfo)
		if directErr != nil {
			errChan <- directErr
			return
		}
		errChan <- rcvr.Direct(context.Background(), testCid2, testPeerInfo)
	}()

	require.NoError(t, rcvr.Close())

	select {
	case err = <-errChan:
		require.ErrorIs(t, err, announce.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error return")
	}
}

func TestReceiverCidCache(t *testing.T) {
	srcHost, _ := libp2p.New()
	t.Cleanup(func() { srcHost.Close() })
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	err = rcvr.Direct(context.Background(), testCid, testPeerInfo)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	amsg, err := rcvr.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, testPeerInfo.ID, amsg.PeerID)

	// Request another announce with the same CID.
	err = rcvr.Direct(context.Background(), testCid, testPeerInfo)
	require.NoError(t, err)

	// Next should not receive another announce.
	_, err = rcvr.Next(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, rcvr.Close())
}
