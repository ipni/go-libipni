package ipnisync

import (
	"errors"
	"fmt"
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
	"github.com/ipni/go-libipni/maurl"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
)

// Publisher serves an advertisement chain over HTTP.
type Publisher struct {
	lsys        ipld.LinkSystem
	handlerPath string
	peerID      peer.ID
	privKey     ic.PrivKey
	lock        sync.Mutex
	root        cid.Cid
	topic       string

	pubHost *libp2phttp.Host
	// httpAddrs is returned by Addrs when not starting the server.
	httpAddrs []multiaddr.Multiaddr
}

var _ http.Handler = (*Publisher)(nil)

// NewPublisher creates a new ipni-sync publisher. Optionally, a libp2p stream
// host can be provided to serve HTTP over libp2p.
func NewPublisher(lsys ipld.LinkSystem, privKey ic.PrivKey, options ...Option) (*Publisher, error) {
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
	if opts.streamHost != nil && opts.streamHost.ID() != peerID {
		return nil, errors.New("stream host ID must match private key ID")
	}

	pub := &Publisher{
		lsys:    lsys,
		peerID:  peerID,
		privKey: privKey,
		topic:   opts.topic,
	}

	// Construct expected request path prefix. If server is started this will
	// get stripped off. If using an external server, look for this path when
	// handling requests.
	var handlerPath string
	opts.handlerPath = strings.TrimPrefix(opts.handlerPath, "/")
	if opts.handlerPath != "" {
		handlerPath = path.Join(opts.handlerPath, IPNIPath)
	} else {
		handlerPath = strings.TrimPrefix(IPNIPath, "/")
	}

	if !opts.startServer {
		httpListenAddrs, err := httpAddrsToMultiaddrs(opts.httpAddrs, opts.requireTLS, opts.handlerPath)
		if err != nil {
			return nil, err
		}
		if opts.streamHost != nil {
			return nil, errors.New("server must be started to serve http over stream host")
		}
		// If the server is not started, the handlerPath does not get stripped
		// from the HTTP request, so leave it as part of the prefix to match in
		// the ServeHTTP handler.
		pub.handlerPath = handlerPath
		pub.httpAddrs = httpListenAddrs
		return pub, nil
	}

	httpListenAddrs, err := httpAddrsToMultiaddrs(opts.httpAddrs, opts.requireTLS, "")
	if err != nil {
		return nil, err
	}
	if len(httpListenAddrs) == 0 && opts.streamHost == nil {
		return nil, errors.New("at least one http listen address or libp2p stream host is needed")
	}

	// This is the "HTTP Host". It's like the libp2p "stream host" (aka core
	// host.Host), but it uses HTTP semantics instead of stream semantics.
	publisherHost := &libp2phttp.Host{
		StreamHost:        opts.streamHost,
		ListenAddrs:       httpListenAddrs,
		InsecureAllowHTTP: !opts.requireTLS,
		TLSConfig:         opts.tlsConfig,
	}
	pub.pubHost = publisherHost

	// Here is where this Publisher is attached as a request handler. This
	// mounts the "/ipnisync/v1" protocol at "/opt_handler_path/ipni/v1/ad/",
	// where opt_handler_path is the optional user specified handler path. If
	// opts.handlerPath is "/foo/", this mounts it at "/foo/ipni/v1/ad/". This
	// Publisher will only receive requests whose path begins with the
	// handlerPath. libp2phttp manages this mapping and clients can learn about
	// the mapping at .well-known/libp2p.
	//
	// In this case we also want the HTTP handler to not even know about the
	// prefix, so we use the stdlib http.StripPrefix.
	publisherHost.SetHTTPHandlerAtPath(ProtocolID, "/"+handlerPath, pub)

	go publisherHost.Serve()

	// Calling publisherHost.Addrs() waits until listeners are ready.
	log.Infow("Publisher ready", "listenOn", publisherHost.Addrs())

	return pub, nil
}

// Addrs returns the slice of multiaddr addresses that the Publisher is
// listening on.
//
// If the server is not started, WithStartServer(false), then this returns the
// multiaddr versions of the list of addresses given by WithHTTPListenAddrs and
// may not actually be a listening address.
func (p *Publisher) Addrs() []multiaddr.Multiaddr {
	if p.pubHost == nil {
		return p.httpAddrs
	}
	return p.pubHost.Addrs()
}

// ID returns the p2p peer ID of the Publisher.
func (p *Publisher) ID() peer.ID {
	return p.peerID
}

// Protocol returns the multiaddr protocol ID of the transport used by the
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
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.pubHost != nil {
		err := p.pubHost.Close()
		p.pubHost = nil
		return err
	}
	return nil
}

// ServeHTTP implements the http.Handler interface.
func (p *Publisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// If we expect publisher requests to have a prefix in the request path,
	// then check for the expected prefix.. This happens when using an external
	// server with this Publisher as the request handler.
	urlPath := strings.TrimPrefix(r.URL.Path, "/")
	if p.handlerPath != "" {
		// A URL path from http will have a leading "/". A URL from libp2phttp will not.
		if !strings.HasPrefix(urlPath, p.handlerPath) {
			http.Error(w, "invalid request path: "+r.URL.Path, http.StatusBadRequest)
			return
		}
	} else if path.Dir(urlPath) != "." {
		http.Error(w, "invalid request path: "+r.URL.Path, http.StatusBadRequest)
		return
	}

	ask := path.Base(r.URL.Path)
	if ask == "head" {
		// Serve the head message.
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
	// Interpret `ask` as a CID to serve.
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

func httpAddrsToMultiaddrs(httpAddrs []string, requireTLS bool, handlerPath string) ([]multiaddr.Multiaddr, error) {
	if len(httpAddrs) == 0 {
		return nil, nil
	}

	var defaultScheme string
	if requireTLS {
		defaultScheme = "https://"
	} else {
		defaultScheme = "http://"
	}
	maddrs := make([]multiaddr.Multiaddr, len(httpAddrs))
	for i, addr := range httpAddrs {
		if !strings.HasPrefix(addr, "https://") && !strings.HasPrefix(addr, "http://") {
			addr = defaultScheme + addr
		}
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		if handlerPath != "" {
			u = u.JoinPath(handlerPath)
		}
		if requireTLS && u.Scheme == "http" {
			log.Warnf("Ignored non-secure HTTP address: %s", addr)
			continue
		}
		maddrs[i], err = maurl.FromURL(u)
		if err != nil {
			return nil, err
		}
	}
	if len(maddrs) == 0 && requireTLS {
		return nil, errors.New("no usable http listen addresses: https required")
	}

	return maddrs, nil
}
