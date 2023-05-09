package message_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	cidStr     = "QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB"
	origPeerID = "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ"
)

var adCid cid.Cid
var maddr1, maddr2 multiaddr.Multiaddr

func init() {
	var err error
	adCid, err = cid.Decode(cidStr)
	if err != nil {
		panic(err)
	}

	maddr1, err = multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/2701")
	if err != nil {
		panic(err)
	}

	maddr2, err = multiaddr.NewMultiaddr("/dns4/localhost/tcp/7663")
	if err != nil {
		panic(err)
	}
}

func TestCBOR(t *testing.T) {
	msg := message.Message{
		Cid:       adCid,
		ExtraData: []byte("t01000"),
		OrigPeer:  origPeerID,
	}
	msg.SetAddrs([]multiaddr.Multiaddr{maddr1, maddr2})

	buf := bytes.NewBuffer(nil)
	err := msg.MarshalCBOR(buf)
	require.NoError(t, err)

	var newMsg message.Message
	err = newMsg.UnmarshalCBOR(buf)
	require.NoError(t, err)

	require.Equal(t, msg, newMsg)
}

func TestJSON(t *testing.T) {
	msg := message.Message{
		Cid:       adCid,
		ExtraData: []byte("t01000"),
		OrigPeer:  origPeerID,
	}
	msg.SetAddrs([]multiaddr.Multiaddr{maddr1, maddr2})

	data, err := json.Marshal(&msg)
	require.NoError(t, err)

	var newMsg message.Message
	err = json.Unmarshal(data, &newMsg)
	require.NoError(t, err)

	require.Equal(t, msg, newMsg)
}

func TestUnknownProtocol(t *testing.T) {
	// Encoded unknown protocol code 9999: /ip4/127.0.0.1/udp/1234/<proto-9999>
	addrData := []byte{54, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 145, 2, 4, 210, 143, 78}
	_, err := multiaddr.NewMultiaddrBytes(addrData)
	require.ErrorContains(t, err, "no protocol with code")

	var msg message.Message
	msg.SetAddrs([]multiaddr.Multiaddr{maddr1})
	msg.Addrs = append(msg.Addrs, addrData)

	addrs, err := msg.GetAddrs()
	require.NoError(t, err)
	require.Equal(t, 1, len(addrs))
}
