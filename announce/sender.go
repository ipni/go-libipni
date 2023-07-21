package announce

import (
	"context"
	"errors"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/multiformats/go-multiaddr"
)

// Sender is the interface for announce sender implementations.
type Sender interface {
	// Close closes the Sender.
	Close() error
	// Send sends the announce Message.
	Send(context.Context, message.Message) error
}

// Send sends an advertisement announcement message, containing the specified
// addresses and extra data, to all senders.
func Send(ctx context.Context, c cid.Cid, addrs []multiaddr.Multiaddr, senders ...Sender) error {
	// Do nothing if nothing to announce or no means to announce it.
	if c == cid.Undef || len(senders) == 0 {
		return nil
	}

	msg := message.Message{
		Cid: c,
	}
	msg.SetAddrs(addrs)

	var errs error
	for _, sender := range senders {
		if sender == nil {
			continue
		}
		if err := sender.Send(ctx, msg); err != nil {
			errs = multierror.Append(errs, err)
			if errors.Is(err, context.Canceled) {
				return err
			}
		}
	}
	return errs
}
