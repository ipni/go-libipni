package p2pclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

// Client is responsible for sending requests and receiving responses to and
// from libp2p peers. Each instance of Client communicates with a single peer
// using a single protocolID.
type Client struct {
	host     host.Host
	ownHost  bool
	peerID   peer.ID
	protoID  protocol.ID
	r        msgio.ReadCloser
	stream   network.Stream
	sendLock sync.Mutex
}

// Response is returned by SendRequest and contains the response to the
// request. It is the caller's responsibility to call Response.Close() after
// reading the data, to free the message buffer.
type Response struct {
	Data      []byte
	Err       error
	msgReader msgio.Reader
}

// Close frees the message buffer that holds the response data.
func (r *Response) Close() {
	if r.Data != nil {
		r.msgReader.ReleaseMsg(r.Data)
		r.Data = nil
		r.msgReader = nil
	}
}

const (
	// default IPNI port for libp2p client to connect to
	defaultLibp2pPort = 3003
	// Timeout to wait for a response after a request is sent
	readMessageTimeout = 10 * time.Second
)

// ErrReadTimeout is an error that occurs when no message is read within the
// timeout period.
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// New creates a new Client that communicates with a specific peer identified
// by protocolID. If host is nil, then one is created.
func New(p2pHost host.Host, peerID peer.ID, protoID protocol.ID) (*Client, error) {
	// If no host was given, create one.
	var ownHost bool
	if p2pHost == nil {
		var err error
		p2pHost, err = libp2p.New()
		if err != nil {
			return nil, err
		}
		ownHost = true
	}

	// Start a client
	return &Client{
		host:    p2pHost,
		ownHost: ownHost,
		peerID:  peerID,
		protoID: protoID,
	}, nil
}

// Connect connects the client to the host at the location specified by
// hostname. The value of hostname is a host or host:port, where the host is a
// hostname or IP address.
func (c *Client) Connect(ctx context.Context, hostname string) error {
	port := defaultLibp2pPort
	var netProto string
	if hostname == "" {
		hostname = "127.0.0.1"
		netProto = "ip4"
	} else {
		hostport := strings.SplitN(hostname, ":", 2)
		if len(hostport) > 1 {
			hostname = hostport[0]
			var err error
			port, err = strconv.Atoi(hostport[1])
			if err != nil {
				return err
			}
		}

		// Determine if hostname is a host name or IP address.
		ip := net.ParseIP(hostname)
		if ip == nil {
			netProto = "dns"
		} else if ip.To4() != nil {
			netProto = "ip4"
		} else if ip.To16() != nil {
			netProto = "ip6"
		} else {
			return fmt.Errorf("host %q does not appear to be a hostname or ip address", hostname)
		}
	}

	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d", netProto, hostname, port))
	if err != nil {
		return err
	}

	return c.ConnectAddrs(ctx, maddr)
}

func (c *Client) ConnectAddrs(ctx context.Context, maddrs ...multiaddr.Multiaddr) error {
	addrInfo := peer.AddrInfo{
		ID:    c.peerID,
		Addrs: maddrs,
	}

	return c.host.Connect(ctx, addrInfo)
}

// Self return the peer ID of this client.
func (c *Client) Self() peer.ID {
	return c.host.ID()
}

// Close resets and closes the network stream if one exists.
func (c *Client) Close() error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	if c.stream != nil {
		c.closeStream()
	}

	if c.ownHost {
		return c.host.Close()
	}
	return nil
}

// SendRequest sends out a request and reads a response.
func (c *Client) SendRequest(ctx context.Context, msg proto.Message) *Response {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	err := c.sendMessage(ctx, msg)
	if err != nil {
		return &Response{
			Err: fmt.Errorf("cannot sent request: %w", err),
		}
	}

	rsp := c.readResponse(ctx)
	if rsp.Err != nil {
		c.closeStream()
	}

	return rsp
}

// SendMessage sends out a message.
func (c *Client) SendMessage(ctx context.Context, msg proto.Message) error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	return c.sendMessage(ctx, msg)
}

func (c *Client) sendMessage(ctx context.Context, msg proto.Message) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err := c.prepStreamReader(ctx)
	if err != nil {
		return err
	}

	if err = writeMsg(c.stream, msg); err != nil {
		c.closeStream()
		return err
	}

	return nil
}

func (c *Client) prepStreamReader(ctx context.Context) error {
	if c.stream == nil {
		nstr, err := c.host.NewStream(ctx, c.peerID, c.protoID)
		if err != nil {
			return err
		}

		c.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
		c.stream = nstr
	}

	return nil
}

func (c *Client) closeStream() {
	_ = c.stream.Reset()
	c.stream = nil
	c.r = nil
}

func (c *Client) readResponse(ctx context.Context) *Response {
	rspCh := make(chan *Response, 1)
	go func(r msgio.ReadCloser, rsp chan<- *Response) {
		data, err := r.ReadMsg()
		if err != nil {
			if data != nil {
				r.ReleaseMsg(data)
			}
			rsp <- &Response{
				Err: err,
			}
			return
		}
		rsp <- &Response{
			Data:      data,
			msgReader: r,
		}
	}(c.r, rspCh)

	t := time.NewTimer(readMessageTimeout)
	defer t.Stop()

	select {
	case response := <-rspCh:
		return response
	case <-ctx.Done():
		return &Response{
			Err: ctx.Err(),
		}
	case <-t.C:
		return &Response{
			Err: ErrReadTimeout,
		}
	}
}
