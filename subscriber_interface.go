package rmqarc

import "context"

// Subscriber is an interface that defines the Subscribe method to process incoming messages.
type Subscriber interface {
	// Subscribe processes the incoming Message.
	// It takes a context and the Message object, and returns an error if processing fails.
	Subscribe(ctx context.Context, msg Message) error
}
