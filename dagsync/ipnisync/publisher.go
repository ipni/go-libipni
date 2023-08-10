package ipnisync

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	headschema "github.com/ipni/go-libipni/dagsync/ipnisync/head"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// Publisher serves an advertisement chain over HTTP.
type Publisher struct {
	addr        multiaddr.Multiaddr
	closer      io.Closer
	lsys        ipld.LinkSystem
	handlerPath string
	peerID      peer.ID
	privKey     ic.PrivKey
	lock        sync.Mutex
	root        cid.Cid
	topic       string
}

var _ http.Handler = (*Publisher)(nil)

// NewPublisher creates a new http publisher. It is optional with start a server, listening on the specified
// address, using the WithStartServer option.
func NewPublisher(address string, lsys ipld.LinkSystem, privKey ic.PrivKey, options ...Option) (*Publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}
	if privKey == nil {
		return nil, errors.New("private key required to sign head requests")
	}
	peerID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id from private key: %w", err)
	}
	proto, _ := multiaddr.NewMultiaddr("/http")
	handlerPath := strings.TrimPrefix(opts.handlerPath, "/")
	if handlerPath != "" {
		httpath, err := multiaddr.NewComponent("httpath", url.PathEscape(handlerPath))
		if err != nil {
			return nil, err
		}
		proto = multiaddr.Join(proto, httpath)
		handlerPath = "/" + handlerPath
	}

	pub := &Publisher{
		lsys:        lsys,
		handlerPath: path.Join(handlerPath, IpniPath),
		peerID:      peerID,
		privKey:     privKey,
		topic:       opts.topic,
	}

	var addr net.Addr
	if opts.startServer {
		l, err := net.Listen("tcp", address)
		if err != nil {
			return nil, err
		}
		pub.closer = l
		addr = l.Addr()

		// Run service on configured port.
		server := &http.Server{
			Handler: pub,
			Addr:    l.Addr().String(),
		}
		go server.Serve(l)
	} else {
		addr, err = net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
	}

	maddr, err := manet.FromNetAddr(addr)
	if err != nil {
		if pub.closer != nil {
			pub.closer.Close()
		}
		return nil, err
	}
	pub.addr = multiaddr.Join(maddr, proto)

	return pub, nil
}

// NewPublisherForListener creates a new http publisher for an existing
// listener. When providing an existing listener, running the HTTP server
// is the caller's responsibility. ServeHTTP on the returned Publisher
// can be used to handle requests. handlerPath is the path to handle
// requests on, e.g. "ipni" for `/ipni/...` requests.
//
// DEPRECATED: use NewPublisherWithoutServer(listener.Addr(), ...)
func NewPublisherForListener(listener net.Listener, handlerPath string, lsys ipld.LinkSystem, privKey ic.PrivKey) (*Publisher, error) {
	return NewPublisherWithoutServer(listener.Addr().String(), handlerPath, lsys, privKey)
}

// NewPublisherWithoutServer creates a new http publisher for an existing
// network address. When providing an existing network address, running
// the HTTP server is the caller's responsibility. ServeHTTP on the
// returned Publisher can be used to handle requests.
//
// DEPRECATED: use NewPublisher(address, lsys, privKey, WithHandlerPath(handlerPath))
func NewPublisherWithoutServer(address, handlerPath string, lsys ipld.LinkSystem, privKey ic.PrivKey, options ...Option) (*Publisher, error) {
	return NewPublisher(address, lsys, privKey, WithHandlerPath(handlerPath), WithServer(false))
}

// Addrs returns the addresses, as []multiaddress, that the Publisher is
// listening on.
func (p *Publisher) Addrs() []multiaddr.Multiaddr {
	return []multiaddr.Multiaddr{p.addr}
}

// ID returns the p2p peer ID of the Publisher.
func (p *Publisher) ID() peer.ID {
	return p.peerID
}

// Protocol returns the multihash protocol ID of the transport used by the
// publisher.
func (p *Publisher) Protocol() int {
	return multiaddr.P_HTTP
}

// SetRoot sets the head of the advertisement chain.
func (p *Publisher) SetRoot(c cid.Cid) {
	p.lock.Lock()
	p.root = c
	p.lock.Unlock()
}

// Close closes the Publisher.
func (p *Publisher) Close() error {
	return p.closer.Close()
}

// ServeHTTP implements the http.Handler interface.
func (p *Publisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.handlerPath != "" && !strings.HasPrefix(r.URL.Path, p.handlerPath) {
		http.Error(w, "invalid request path: "+r.URL.Path, http.StatusBadRequest)
		return
	}

	ask := path.Base(r.URL.Path)
	if ask == "head" {
		// serve the head
		p.lock.Lock()
		rootCid := p.root
		p.lock.Unlock()

		if rootCid == cid.Undef {
			http.Error(w, "", http.StatusNoContent)
			return
		}

		marshalledMsg, err := newEncodedSignedHead(rootCid, p.topic, p.privKey)
		if err != nil {
			log.Errorw("Failed to serve root", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		_, _ = w.Write(marshalledMsg)
		return
	}
	// interpret `ask` as a CID to serve.
	c, err := cid.Parse(ask)
	if err != nil {
		http.Error(w, "invalid request: not a cid", http.StatusBadRequest)
		return
	}
	item, err := p.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	if err != nil {
		if errors.Is(err, ipld.ErrNotExists{}) {
			http.Error(w, "cid not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unable to load data for cid", http.StatusInternalServerError)
		log.Errorw("Failed to load requested block", "err", err, "cid", c)
		return
	}
	// marshal to json and serve.
	_ = dagjson.Encode(item, w)

	// TODO: Sign message using publisher's private key.
}

func newEncodedSignedHead(rootCid cid.Cid, topic string, privKey ic.PrivKey) ([]byte, error) {
	signedHead, err := headschema.NewSignedHead(rootCid, topic, privKey)
	if err != nil {
		return nil, err
	}
	return signedHead.Encode()
}
