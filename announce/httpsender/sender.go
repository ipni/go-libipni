package httpsender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ipni/go-libipni/announce/message"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
)

const DefaultAnnouncePath = "/announce"

// Sender sends announce messages over HTTP.
type Sender struct {
	announceURLs []string
	client       *http.Client
	extraData    []byte
	peerID       peer.ID
	userAgent    string
}

// New creates a new Sender that sends advertisement announcement messages over
// HTTP. Announcements are sent directly to the specified URLs. The specified
// peerID is added to the multiaddrs contained in the announcements, which is
// how the publisher ID is communicated over HTTP.
func New(announceURLs []*url.URL, peerID peer.ID, options ...Option) (*Sender, error) {
	if len(announceURLs) == 0 {
		return nil, errors.New("no announce urls")
	}
	err := peerID.Validate()
	if err != nil {
		return nil, err
	}

	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	client := opts.client
	if client == nil {
		client = &http.Client{
			Timeout: opts.timeout,
		}
	}

	urls := make([]string, 0, len(announceURLs))
	seen := make(map[string]struct{}, len(announceURLs))
	for _, u := range announceURLs {
		if u.Path == "" {
			u.Path = DefaultAnnouncePath
		}
		ustr := u.String()
		if _, ok := seen[ustr]; ok {
			// Skip duplicate.
			continue
		}
		seen[ustr] = struct{}{}
		urls = append(urls, ustr)
	}

	return &Sender{
		announceURLs: urls,
		extraData:    opts.extraData,
		client:       client,
		peerID:       peerID,
		userAgent:    opts.userAgent,
	}, nil
}

// Close closes idle HTTP connections.
func (s *Sender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// Send sends the Message to the announce URLs.
func (s *Sender) Send(ctx context.Context, msg message.Message) error {
	err := s.addIDToAddrs(&msg)
	if err != nil {
		return fmt.Errorf("cannot add p2p id to message addrs: %w", err)
	}
	if len(s.extraData) != 0 {
		msg.ExtraData = s.extraData
	}
	buf := bytes.NewBuffer(nil)
	if err = msg.MarshalCBOR(buf); err != nil {
		return fmt.Errorf("cannot cbor encode announce message: %w", err)
	}
	return s.sendData(ctx, buf, false)
}

func (s *Sender) SendJson(ctx context.Context, msg message.Message) error {
	err := s.addIDToAddrs(&msg)
	if err != nil {
		return fmt.Errorf("cannot add p2p id to message addrs: %w", err)
	}
	if len(s.extraData) != 0 {
		msg.ExtraData = s.extraData
	}
	buf := new(bytes.Buffer)
	if err = json.NewEncoder(buf).Encode(msg); err != nil {
		return fmt.Errorf("cannot json encode announce message: %w", err)
	}
	return s.sendData(ctx, buf, true)
}

// addIDToAddrs adds the peerID to each of the multiaddrs in the message. This
// is necessay to communicate the publisher ID when sending an announce over
// HTTP.
func (s *Sender) addIDToAddrs(msg *message.Message) error {
	if len(msg.Addrs) == 0 {
		return nil
	}
	addrs, err := msg.GetAddrs()
	if err != nil {
		return fmt.Errorf("cannot get addrs from message: %s", err)
	}
	ai := peer.AddrInfo{
		ID:    s.peerID,
		Addrs: addrs,
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&ai)
	if err != nil {
		return err
	}
	msg.SetAddrs(p2pAddrs)
	return nil
}

func (s *Sender) sendData(ctx context.Context, buf *bytes.Buffer, js bool) error {
	if len(s.announceURLs) < 2 {
		u := s.announceURLs[0]
		err := s.sendAnnounce(ctx, u, buf, js)
		if err != nil {
			return fmt.Errorf("failed to send http announce message to %s: %w", u, err)
		}
		return nil
	}

	errChan := make(chan error)
	data := buf.Bytes()
	for _, u := range s.announceURLs {
		// Send HTTP announce to indexers concurrently. If context is canceled,
		// then requests will be canceled.
		go func(announceURL string) {
			err := s.sendAnnounce(ctx, announceURL, bytes.NewBuffer(data), js)
			if err != nil {
				errChan <- fmt.Errorf("failed to send http announce to %s: %w", announceURL, err)
				return
			}
			errChan <- nil
		}(u)
	}
	var errs error
	for i := 0; i < len(s.announceURLs); i++ {
		err := <-errChan
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

func (s *Sender) sendAnnounce(ctx context.Context, announceURL string, buf *bytes.Buffer, js bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, announceURL, buf)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", s.userAgent)
	if js {
		req.Header.Set("Content-Type", "application/json")
	} else {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("%d %s: %s", resp.StatusCode, http.StatusText(resp.StatusCode), strings.TrimSpace(string(body)))
	}
	return nil
}
