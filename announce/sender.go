package announce

import (
	"context"

	"github.com/ipni/go-libipni/announce/message"
)

// Sender is the interface for announce sender implementations.
type Sender interface {
	// Close closes the Sender.
	Close() error
	// Send sends the announce Message.
	Send(context.Context, message.Message) error
}
