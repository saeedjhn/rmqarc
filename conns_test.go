package rmqarc_test

import (
	"context"
	"fmt"
	"github.com/streadway/amqp"
	"testing"
	"time"

	"github.com/saeedjhn/rmqarc"
)

const (
	_connectionName  = "test.connection"
	_connectionName1 = "test.connection-1"
	_exchangeName    = "test.exchange"
	_queueName       = "test.queue"
	_bindingKeyName  = "test.key"
	_routingKeyName  = "test.key"
)

func TestNewConnection(t *testing.T) {
	t.Parallel()

	connCfg := rmqarc.ConnectionConfig{
		Host:             "localhost",
		Username:         "guest",
		Password:         "guest",
		Port:             ":5672",
		BaseRetryTimeout: 1 * time.Second,
		Multiplier:       1.5,
		MaxDelay:         60 * time.Second,
		MaxRetry:         10,
	}
	conn := rmqarc.New(connCfg, _connectionName)

	// Log the resulting connection object for debugging purposes.
	t.Logf("%#v", conn)
}

func TestConnections(t *testing.T) {
	t.Parallel()

	// Define the connection configuration for RabbitMQ.
	connCfg := rmqarc.ConnectionConfig{
		Host:             "localhost",
		Username:         "guest",
		Password:         "guest",
		Port:             ":5672",
		BaseRetryTimeout: 1 * time.Second,
		Multiplier:       1.5,
		MaxDelay:         60 * time.Second,
		MaxRetry:         5,
	}

	rmqarc.New(connCfg, _connectionName)
	rmqarc.New(connCfg, _connectionName1)

	conns := rmqarc.Connections()

	// Print the number of active connections
	t.Logf("Active connections count: %d", len(conns))

	// Iterate through the connections and log.Println their details
	for _, conn := range conns {
		t.Logf("Connection Name: %v", conn.Name)
	}
}

func TestHandleConsumeDeliveries(t *testing.T) {
	t.Parallel()

	// Creates a channel to keep the main goroutine running indefinitely, typically used to prevent the program from exiting.
	forever := make(chan bool)

	// Initialize the connection configuration
	connCfg := rmqarc.ConnectionConfig{
		Host:                "localhost",
		Username:            "guest",
		Password:            "guest",
		Port:                ":5672",
		BaseRetryTimeout:    1 * time.Second,  // Retry base timeout
		Multiplier:          1.6,              // Retry multiplier
		MaxDelay:            30 * time.Second, // Max retry delay
		MaxRetry:            3,                // Max retry attempts
		CheckConnectionTime: 5 * time.Second,  // Time to check for a connection
	}

	// Create a new connection using the connection configuration
	conn := rmqarc.New(connCfg, _connectionName)

	// Attempt to establish a connection to the RabbitMQ server
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed attempt to establish a connection to server:", err.Error())
		return
	}

	// Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       _exchangeName,         // Name of the exchange
		Kind:       rmqarc.DirectExchange, // Exchange type (e.g., direct, topic)
		Durable:    true,                  // The exchange should survive server restarts
		AutoDelete: false,                 // The exchange should not auto-delete when unused
		Internal:   false,                 // The exchange is not internal (accessible to clients)
		NoWait:     false,                 // Wait for a confirmation from the broker
		Args:       nil,                   // No additional arguments for this exchange
	}

	// Set up the exchange using the SetupExchange method.
	err = conn.SetupExchange(exchangeCfg)
	if err != nil {
		fmt.Println("Failed to set up exchange:", err.Error())
		return
	}

	// Define the queue configuration for binding queues to the exchange.
	queueCfg := rmqarc.QueueBindConfig{
		Queues:           []string{_queueName}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                 // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                  // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       _bindingKeyName,      // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                  // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                    // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                    // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                // Apply the prefetch count limit to the entire connection or just this channel
	}

	// Bind the queues to the exchange using the SetupBindQueue method.
	err = conn.SetupBindQueue(queueCfg)
	if err != nil {
		fmt.Println("Failed to bind queues to the exchange:", err.Error())
		return
	}

	// Define the consume configuration
	consumeCfg := rmqarc.ConsumeConfig{
		AutoAck:   false, // AutoAck: If false, the consumer must manually acknowledge messages (Manual Acknowledgment).
		Exclusive: false, // Exclusive: If true, the queue will only be accessible to this consumer.
		NoLocal:   false, // NoLocal: If true, messages published on the same connection will not be delivered to this consumer (rarely used in RabbitMQ).
		NoWait:    false, // NoWait: If true, the consumer does not wait for a response from the server and proceeds immediately.
		Args:      nil,   // Args: Additional arguments for consumer configuration (often nil unless specific options are needed).
	}

	// Start consuming messages from the defined queues
	deliveries, err := conn.StartConsume(consumeCfg)
	if err != nil {
		fmt.Printf("Failed to start consuming messages: %v", err)
		return
	}

	// Define messageHandler
	messageHandler := func(c rmqarc.Connection, q string, deliveries <-chan amqp.Delivery) {
		for d := range deliveries {
			m := rmqarc.Message{
				ContentType:     d.ContentType,
				ContentEncoding: d.ContentEncoding,
				DeliveryMode:    rmqarc.DeliveryType(d.DeliveryMode),
				Priority:        d.Priority,
				CorrelationId:   d.CorrelationId,
				ReplyTo:         d.ReplyTo,
				Expiration:      d.Expiration,
				MessageId:       d.MessageId,
				Timestamp:       d.Timestamp,
				Type:            d.Type,
				UserId:          d.UserId,
				AppId:           d.AppId,
				Body: rmqarc.MessageBody{
					Data: d.Body,
					Type: d.Type,
				},
			}
			//handle the custom message
			fmt.Println("Got message from queue ", m)

			err = d.Ack(false) // Success
			if err != nil {
				// Logger
				return
			}
		}
	}

	// Step 10: Loop deliveries & handle error from consume deliveries
	for q, d := range deliveries {
		go func() {
			err = conn.HandleConsumedDeliveries(q, d, messageHandler)
			if err != nil {
				// Logger
				fmt.Println(err)
			}
		}()
	}

	// Blocks the main goroutine indefinitely, waiting for a value to be sent to the 'forever' channel.
	<-forever
}

func TestHandleConsumeDeliveriesAutomatic(t *testing.T) {
	t.Parallel()

	// Creates a channel to keep the main goroutine running indefinitely, typically used to prevent the program from exiting.
	forever := make(chan bool)

	// Define an error channel to receive errors from the message processing
	errChan := make(chan error)

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is canceled to clean up resources

	//go func() {
	//	time.Sleep(8 * time.Second)
	//
	//	log.Println("cancel invoked")
	//
	//	cancel()
	//}()

	// Using a concrete subscriberFunc
	//var subscriber = ConcreteSubscriber{}

	// Using a function as a subscriberFunc
	var subscriberFunc rmqarc.SubscriberFunc = func(ctx context.Context, msg rmqarc.Message) error {
		fmt.Printf("Received message from queue: %s, MessageID: %s, Body: %s",
			msg.MessageId, msg.Body.Type, string(msg.Body.Data))
		return nil
	}

	// Initialize the connection configuration
	connCfg := rmqarc.ConnectionConfig{
		Host:                "localhost",
		Username:            "guest",
		Password:            "guest",
		Port:                ":5672",
		BaseRetryTimeout:    1 * time.Second,  // Retry base timeout
		Multiplier:          1.6,              // Retry multiplier
		MaxDelay:            30 * time.Second, // Max retry delay
		MaxRetry:            10,               // Max retry attempts
		CheckConnectionTime: 5 * time.Second,  // Time to check for a connection
	}

	// Create a new connection using the connection configuration
	conn := rmqarc.New(connCfg, _connectionName)

	// Attempt to establish a connection to the RabbitMQ server
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed attempt to establish a connection to server:", err.Error())
		return
	}

	// Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       _exchangeName,         // Name of the exchange
		Kind:       rmqarc.DirectExchange, // Exchange type (e.g., direct, topic)
		Durable:    true,                  // The exchange should survive server restarts
		AutoDelete: false,                 // The exchange should not auto-delete when unused
		Internal:   false,                 // The exchange is not internal (accessible to clients)
		NoWait:     false,                 // Wait for a confirmation from the broker
		Args:       nil,                   // No additional arguments for this exchange
	}

	// Set up the exchange using the SetupExchange method.
	err = conn.SetupExchange(exchangeCfg)
	if err != nil {
		fmt.Println("Failed to set up exchange:", err.Error())
		return
	}

	// Define the queue configuration for binding queues to the exchange.
	queueCfg := rmqarc.QueueBindConfig{
		Queues:           []string{_queueName}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                 // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                  // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       _bindingKeyName,      // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                  // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                    // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                    // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                // Apply the prefetch count limit to the entire connection or just this channel
	}

	// Bind the queues to the exchange using the SetupBindQueue method.
	err = conn.SetupBindQueue(queueCfg)
	if err != nil {
		fmt.Println("Failed to bind queues to the exchange:", err.Error())
		return
	}

	// Define the consume configuration
	consumeCfg := rmqarc.ConsumeConfig{
		AutoAck:   false, // AutoAck: If false, the consumer must manually acknowledge messages (Manual Acknowledgment).
		Exclusive: false, // Exclusive: If true, the queue will only be accessible to this consumer.
		NoLocal:   false, // NoLocal: If true, messages published on the same connection will not be delivered to this consumer (rarely used in RabbitMQ).
		NoWait:    false, // NoWait: If true, the consumer does not wait for a response from the server and proceeds immediately.
		Args:      nil,   // Args: Additional arguments for consumer configuration (often nil unless specific options are needed).
	}

	// Start consuming messages from the defined queues
	_, err = conn.StartConsume(consumeCfg)
	if err != nil {
		fmt.Printf("Failed to start consuming messages: %v", err)
		return
	}

	go conn.HandleConsumedDeliveriesAutomatic(ctx, errChan, subscriberFunc)

	go func() {
		for {
			if errCh := <-errChan; errCh != nil {
				// Logger
				fmt.Printf("Error occurred while handling automatic message deliveries: %v\n", errCh)
			}
		}
	}()

	<-forever
}

func TestConsumer(t *testing.T) {

}

func TestPublish(t *testing.T) {
	// Initialize the connection configuration
	connCfg := rmqarc.ConnectionConfig{
		Host:                "localhost",
		Username:            "guest",
		Password:            "guest",
		Port:                ":5672",
		BaseRetryTimeout:    1 * time.Second,
		Multiplier:          1.5,
		MaxDelay:            120 * time.Second,
		MaxRetry:            5,
		CheckConnectionTime: 3 * time.Second,
	}

	// Create a new connection
	conn := rmqarc.New(connCfg, _connectionName)

	// Attempt to establish a connection to the RabbitMQ server
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed attempt to establish a connection to server:", err.Error())
		return
	}

	// Define the publish configuration
	pubCfg := rmqarc.PublishConfig{
		ExchangeName: _exchangeName,   // The name of the exchange to publish to
		RoutingKey:   _routingKeyName, // The routing key for the message
		Mandatory:    false,           // Delivery is not mandatory
		Immediate:    false,           // Delivery is not immediate
	}

	// Create a message to publish
	msg := rmqarc.Message{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    rmqarc.Persistent, // Persistent delivery mode
		Priority:        0,                 // Default priority
		CorrelationId:   "12345",
		ReplyTo:         "response-queue",
		Expiration:      "",
		MessageId:       "msg-1",
		Timestamp:       time.Now(),
		Type:            "example.type",
		//UserId: "user-1",
		//AppId:  "app-1",
		Body: rmqarc.MessageBody{
			Data: []byte(`{"key": "value"}`), // The message body data
			Type: "application/json",         // The type of the message body
		},
	}

	for range 1 {
		// Publish the message
		err = conn.Publish(pubCfg, msg)
		if err != nil {
			fmt.Println("Failed to publish message: ", err.Error())
		}

		time.Sleep(2 * time.Second)
	}

	fmt.Println("Message published successfully.")
}
