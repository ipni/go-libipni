package dtsync

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/dagsync/p2p/protocol/head"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Publisher serves an advertisement over libp2p using data-transfer.
type Publisher struct {
	closeOnce     sync.Once
	dtManager     dt.Manager
	dtClose       dtCloseFunc
	headPublisher *head.Publisher
	host          host.Host
}

// NewPublisher creates a new dagsync publisher.
func NewPublisher(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topicName string, options ...Option) (*Publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	dtManager, _, dtClose, err := makeDataTransfer(host, ds, lsys, opts.allowPeer, opts.gsMaxInRequests, opts.gsMaxOutRequests)
	if err != nil {
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topicName, headPublisher)

	return &Publisher{
		dtManager:     dtManager,
		dtClose:       dtClose,
		headPublisher: headPublisher,
		host:          host,
	}, nil
}

func startHeadPublisher(host host.Host, topicName string, headPublisher *head.Publisher) {
	go func() {
		log := log.With("topic", topicName, "host", host.ID())
		log.Infow("Starting head publisher for topic")
		err := headPublisher.Serve(host, topicName)
		if err != http.ErrServerClosed {
			log.Errorw("Head publisher stopped serving on topic on host", "err", err)
		}
		log.Infow("Stopped head publisher")
	}()
}

// NewPublisherFromExisting instantiates publishing on an existing
// data transfer instance.
func NewPublisherFromExisting(dtManager dt.Manager, host host.Host, topicName string, lsys ipld.LinkSystem, options ...Option) (*Publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	err = configureDataTransferForDagsync(context.Background(), dtManager, lsys, opts.allowPeer)
	if err != nil {
		return nil, fmt.Errorf("cannot configure datatransfer: %w", err)
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topicName, headPublisher)

	return &Publisher{
		headPublisher: headPublisher,
		host:          host,
	}, nil
}

// Addrs returns the multiaddrs of the publisher's host.
func (p *Publisher) Addrs() []multiaddr.Multiaddr {
	return p.host.Addrs()
}

// ID returns the peer ID of the publisher's host.
func (p *Publisher) ID() peer.ID {
	return p.host.ID()
}

// Protocol returns the multihash protocol ID of the transport used by the
// publisher.
func (p *Publisher) Protocol() int {
	return multiaddr.P_P2P
}

// SetRoot sets the root CID of the advertisement chain.
func (p *Publisher) SetRoot(c cid.Cid) {
	p.headPublisher.SetRoot(c)
}

// Close closes the publisher.
func (p *Publisher) Close() error {
	var errs error
	p.closeOnce.Do(func() {
		err := p.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		if p.dtClose != nil {
			err = p.dtClose()
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}
	})
	return errs
}
