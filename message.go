package rmqarc

import "time"

// MessageBody represents the actual content of the message.
// It contains the raw message data as well as a type that describes the format or purpose of the message.
type MessageBody struct {
	Data []byte // Raw message data
	Type string // Type or format of the message body (e.g., JSON, XML, etc.)
}

// Message represents a RabbitMQ message with metadata and body content.
// It contains all the information needed to route, process, and identify the message in the system.
type Message struct {
	ContentType     string       // MIME content type (e.g., "application/json")
	ContentEncoding string       // MIME content encoding (e.g., "gzip" or "UTF-8")
	DeliveryMode    DeliveryType // Delivery mode: Transient (0 or 1) or Persistent (2)
	Priority        uint8        // Message priority from 0 (lowest) to 9 (highest)
	CorrelationId   string       // Correlation identifier for request-response communication (e.g., for RPC)
	ReplyTo         string       // Queue or address to reply to (useful in RPC scenarios)
	Expiration      string       // Message expiration in milliseconds (e.g., "60000" for 1 minute)
	MessageId       string       // Unique identifier for the message
	Timestamp       time.Time    // Timestamp when the message was created
	Type            string       // Type or name of the message (e.g., "order.created")
	UserId          string       // Identifier of the user who created or sent the message
	AppId           string       // Identifier of the application that created the message

	Body MessageBody // The content of the message, represented as a MessageBody struct
}
