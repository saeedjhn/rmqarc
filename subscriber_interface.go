package rmqarc

import "context"

// Subscriber is an interface that defines a contract for processing incoming messages.
// It contains the Subscribe method that allows for message handling with error reporting.
// Implementations of this interface should provide their own logic for processing messages
// based on the context in which they are invoked.
//
// The Subscribe method takes a context.Context to manage the lifecycle of the message processing
// and a Message object that represents the incoming message. If the processing fails,
// it should return an error to indicate the failure.
type Subscriber interface {
	// Subscribe processes the incoming Message.
	// It takes a context and the Message object, and returns an error if processing fails.
	Subscribe(ctx context.Context, msg Message) error
}

// SubscriberFunc is a function type that provides a convenient way to implement the Subscriber interface.
// It defines the signature for processing incoming messages, allowing any function that matches this
// signature to be used as a subscriber. This makes it easy to create inline or standalone message
// processing logic without the need for a separate struct.
//
// The function takes a context and a Message as parameters, and returns an error if the processing
// fails. This flexibility allows for various implementations, from simple handlers to complex
// processing logic.
type SubscriberFunc func(ctx context.Context, msg Message) error

// Subscribe implements the Subscribe method for the SubscriberFunc type, allowing functions of this
// type to be used as Subscribers. This enables seamless integration of function-based message
// processing logic into systems that expect a Subscriber interface implementation.
func (f SubscriberFunc) Subscribe(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}
