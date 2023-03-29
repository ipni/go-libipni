package p2pclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	pb "github.com/ipni/go-libipni/find/pb"
	"github.com/ipni/go-libipni/internal/p2pclient"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	p2pc *p2pclient.Client
}

// Client must implement client.Interface.
var _ client.Interface = (*Client)(nil)

func New(p2pHost host.Host, peerID peer.ID) (*Client, error) {
	client, err := p2pclient.New(p2pHost, peerID, FindProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		p2pc: client,
	}, nil
}

// Connect connects the client to the host at the location specified by
// hostname.  The value of hostname is a host or host:port, where the host is a
// hostname or IP address.
func (c *Client) Connect(ctx context.Context, hostname string) error {
	return c.p2pc.Connect(ctx, hostname)
}

func (c *Client) ConnectAddrs(ctx context.Context, maddrs ...multiaddr.Multiaddr) error {
	return c.p2pc.ConnectAddrs(ctx, maddrs...)
}

func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*model.FindResponse, error) {
	return c.FindBatch(ctx, []multihash.Multihash{m})
}

func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}

	data, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req := &pb.FindMessage{
		Type: pb.FindMessage_FIND,
		Data: data,
	}

	data, err = c.sendRecv(ctx, req, pb.FindMessage_FIND_RESPONSE)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(data)
}

func (c *Client) GetProvider(ctx context.Context, providerID peer.ID) (*model.ProviderInfo, error) {
	data, err := json.Marshal(providerID)
	if err != nil {
		return nil, err
	}

	req := &pb.FindMessage{
		Type: pb.FindMessage_GET_PROVIDER,
		Data: data,
	}

	data, err = c.sendRecv(ctx, req, pb.FindMessage_GET_PROVIDER_RESPONSE)
	if err != nil {
		return nil, err
	}

	var providerInfo model.ProviderInfo
	err = json.Unmarshal(data, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (c *Client) ListProviders(ctx context.Context) ([]*model.ProviderInfo, error) {
	req := &pb.FindMessage{
		Type: pb.FindMessage_LIST_PROVIDERS,
	}

	data, err := c.sendRecv(ctx, req, pb.FindMessage_LIST_PROVIDERS_RESPONSE)
	if err != nil {
		return nil, err
	}

	var providers []*model.ProviderInfo
	err = json.Unmarshal(data, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (c *Client) GetStats(ctx context.Context) (*model.Stats, error) {
	req := &pb.FindMessage{
		Type: pb.FindMessage_GET_STATS,
	}

	data, err := c.sendRecv(ctx, req, pb.FindMessage_GET_STATS_RESPONSE)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalStats(data)
}

func (c *Client) sendRecv(ctx context.Context, req *pb.FindMessage, expectRspType pb.FindMessage_MessageType) ([]byte, error) {
	var resp pb.FindMessage
	err := c.p2pc.SendRequest(ctx, req, func(data []byte) error {
		return proto.Unmarshal(data, &resp)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.FindMessage_ERROR_RESPONSE {
			return nil, apierror.DecodeError(resp.GetData())
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
