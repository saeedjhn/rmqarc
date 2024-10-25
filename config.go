package rmqarc

import (
	"time"

	"github.com/streadway/amqp"
)

// ConnectionConfig defines the configuration needed to establish a connection to RabbitMQ.
type ConnectionConfig struct {
	Host                string        // The RabbitMQ server host (e.g., "localhost").
	Username            string        // Username for RabbitMQ authentication.
	Password            string        // Password for RabbitMQ authentication.
	Port                string        // The RabbitMQ server port (e.g., "5672").
	BaseRetryTimeout    time.Duration // Base timeout for reconnection retry attempts.
	Multiplier          float64       // Multiplier factor for exponential backoff on retries.
	MaxDelay            time.Duration // Maximum delay allowed between retries.
	MaxRetry            int           // Maximum number of retry attempts for reconnection.
	CheckConnectionTime time.Duration // Time interval for checking the RabbitMQ connection status.
}

// ExchangeConfig defines the configuration needed to declare an exchange in RabbitMQ.
type ExchangeConfig struct {
	Name       string       // Name of the exchange to declare.
	Kind       ExchangeType // Type of the exchange: "direct", "fanout", "topic", or "headers".
	Durable    bool         // Whether the exchange should survive server restarts (true for durable exchanges).
	AutoDelete bool         // Whether the exchange should be automatically deleted when no longer in use.
	Internal   bool         // Whether the exchange is internal (only used for routing between exchanges).
	NoWait     bool         // Whether the server should wait for a response before declaring the exchange.
	Args       amqp.Table   // Additional arguments for exchange declaration.
}

// QueueBindConfig defines the configuration needed to declare and bind a queue in RabbitMQ.
type QueueBindConfig struct {
	Queues           []string   // List of queues to declare.
	Durable          bool       // Whether the queue should survive server restarts (true for durable queues).
	AutoDelete       bool       // Whether the queue should be automatically deleted when no longer in use.
	Exclusive        bool       // Whether the queue is exclusive to the connection that declared it.
	NoWait           bool       // Whether the server should wait for a response before declaring the queue.
	ArgsQueueDeclare amqp.Table // Additional arguments for queue declaration.
	ArgsQueueBind    amqp.Table // Additional arguments for binding the queue.
	BindingKey       string     // The routing key for binding the queue to an exchange.
	PrefetchCount    int        // The number of messages to prefetch (limit of unacknowledged messages).
	PrefetchSize     int        // The size limit for prefetching messages.
	PrefetchGlobal   bool       // Whether the prefetch settings apply globally across channels.
}

// ConsumeConfig defines the configuration for consuming messages from a queue in RabbitMQ.
type ConsumeConfig struct {
	AutoAck   bool       // Whether to automatically acknowledge messages after receiving them.
	Exclusive bool       // Whether the consumer should have exclusive access to the queue.
	NoLocal   bool       // Whether to prevent the consumer from receiving messages published on the same connection.
	NoWait    bool       // Whether the server should wait for a response before starting the consumer.
	Args      amqp.Table // Additional arguments for consuming messages.
}

// PublishConfig defines the configuration for publishing a message to an exchange in RabbitMQ.
type PublishConfig struct {
	ExchangeName string // Name of the exchange to publish the message to.
	RoutingKey   string // The routing key used for routing the message.
	Mandatory    bool   // Whether the message must be delivered to at least one queue (otherwise returned to the sender).
	Immediate    bool   // Whether the message must be immediately delivered to a consumer (otherwise returned to the sender).
}

// ExchangeAndQueueBindConfig is a combined configuration for declaring an exchange and binding it to a queue.
type ExchangeAndQueueBindConfig struct {
	ExchangeCfg  ExchangeConfig  // The configuration for declaring the exchange.
	QueueBindCfg QueueBindConfig // The configuration for declaring and binding the queue.
}
