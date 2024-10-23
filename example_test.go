// Package rmqarc_test contains unit tests for the rmqarc package,
// which provides functionality for managing RabbitMQ connections
// with automatic recovery, retries, and message handling. These
// tests validate the robustness, resilience, and correctness of the
// rmqarc package’s core features, such as publishing, consuming,
// connection recovery, and error handling.
//
// The rmqarc package offers high-level abstractions for working with RabbitMQ,
// including automatic reconnections in case of network failures, retries for
// failed operations, and convenient methods for consuming and publishing messages.
//
// This test package focuses on verifying the behavior of:
//   - Connection management: Ensuring automatic recovery of RabbitMQ connections.
//   - Publishing: Validating message publishing with different configurations.
//   - Consumption: Testing message consumption, including handling of message
//     acknowledgments and rejections.
//   - Error handling: Verifying that errors are appropriately handled and propagated.
//   - Context management: Ensuring graceful shutdown via context cancellations.
//
// Testing covers various edge cases, including connection failures, network
// disruptions, and message processing errors.
//
// For detailed guidance on how to write examples in Go, including best practices
// for demonstrating code usage in Go documentation, visit the official Go blog:
// https://go.dev/blog/examples
package rmqarc_test

import (
	"context"
	"fmt"
	"time"

	"github.com/saeedjhn/rmqarc"
	"github.com/streadway/amqp"
)

// ExampleNewConnection tests the creation of a new RabbitMQ connection
// with automatic recovery using the rmqarc package.
// It verifies that the connection is created with the correct configuration
// and logs the resulting connection.
func ExampleNew() {
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

	// Create a new connection instance with the provided configuration.
	conn := rmqarc.New(connCfg, "example-connection")

	// Log the resulting connection object for debugging purposes.
	fmt.Printf("%#v", conn)
}

// ExampleConnections demonstrates how to use the Connections() function
// to retrieve all active connections.
func ExampleConnections() {
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

	rmqarc.New(connCfg, "example-connection-1")
	rmqarc.New(connCfg, "example-connection-2")

	conns := rmqarc.Connections()

	// Print the number of active connections
	fmt.Printf("Active connections count: %d\n", len(conns))

	// Iterate through the connections and fmt.Println their details
	for _, conn := range conns {
		fmt.Printf("Connection Name: %v\n", conn.Name)
	}
}

// ExampleGetConnection returns an existing connection from the connection pool
// using the provided connection name (connName).
func ExampleGetConnection() {
	conn := rmqarc.GetConnection("example-connection")
	if conn == nil {
		fmt.Println("Connection not found.")
	} else {
		fmt.Println("Connection retrieved successfully.")
	}
}

// ExampleConnection_Queues demonstrates how to retrieve and fmt.Println the names
// of queues configured for the current connection.
func ExampleConnection_Queues() {
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

	// Create a new connection instance with the provided configuration.
	conn := rmqarc.New(connCfg, "example-connection")

	// Retrieve the list of queues.
	queues := conn.Queues()

	// Print the number of queues.
	fmt.Println("Number of queues configured:", len(queues))

	// If there are any queues, fmt.Println their names.
	if len(queues) > 0 {
		for _, queueName := range queues {
			fmt.Println("Queue Name:", queueName)
		}
	} else {
		fmt.Println("No queues are configured for this connection.")
	}
}

// ExampleConnection_Connect demonstrates how to establish a connection to the RabbitMQ
// server using the Connect method, and handle any errors that may occur during the process.
func ExampleConnection_Connect() {
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

	// Create a new connection instance with the provided configuration.
	conn := rmqarc.New(connCfg, "example-connection")

	// Attempt to connect to the RabbitMQ server.
	err := conn.Connect()
	if err != nil {
		// Handle the error if the connection or channel setup fails.
		fmt.Println("Failed to establish connection:", err.Error())
		return
	}

	// If the connection is successful, fmt.the success.
	fmt.Println("Successfully connected to RabbitMQ server and created a channel.")

	// You can now proceed to use the connection to publish or consume messages.
	// Example: conn.SetupExchange, ...
}

// ExampleConnection_SetupExchange demonstrates how to use the SetupExchange
// method to declare an exchange on the RabbitMQ server with specified settings.
func ExampleConnection_SetupExchange() {
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

	// Create a new connection instance with the provided configuration.
	conn := rmqarc.New(connCfg, "example-connection")

	// First, establish the connection to the RabbitMQ server.
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err.Error())
		return
	}

	// Define the exchange configuration with all required settings.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",    // Name of the exchange
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

	// If the exchange setup is successful, fmt.the success.
	fmt.Println("Exchange successfully declared:", exchangeCfg.Name)

	// Now you can proceed with binding queues or publishing messages to the exchange.
	// Example: conn.SetupBindQueue(), ...
}

// ExampleConnection_SetupBindQueue demonstrates how to use the SetupBindQueue
// method to declare and bind queues to an exchange using the specified QueueBindConfig.
func ExampleConnection_SetupBindQueue() {
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

	// Create a new connection instance with the provided configuration.
	conn := rmqarc.New(connCfg, "example-connection")

	// First, establish the connection to the RabbitMQ server.
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err.Error())
		return
	}

	// Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",    // Name of the exchange
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
		Queues: []string{
			"example-queue-1",
			"example-queue-2",
		}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                  // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                 // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                 // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                 // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                   // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       "routing-key-example", // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                   // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                     // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                     // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                 // Apply the prefetch count limit to the entire connection or just this channel
	}

	// Bind the queues to the exchange using the SetupBindQueue method.
	err = conn.SetupBindQueue(queueCfg)
	if err != nil {
		fmt.Println("Failed to bind queues to the exchange:", err.Error())
		return
	}

	// If the queue binding is successful, fmt.the success.
	fmt.Println("Queues successfully declared and bound to exchange:", exchangeCfg.Name)

	// Now, you can proceed with consuming messages from the bound queues.
	// Example: conn.StartConsume(), ...
}

// ExampleSetupExchangeAndQueue demonstrates how to configure both an exchange
// and bind queues to it using SetupExchangeAndQueue.
func ExampleConnection_SetupExchangeAndQueue() {
	// Initialize a connection configuration
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
	conn := rmqarc.New(connCfg, "example-connection")

	// Define the exchange configuration
	exCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",
		Kind:       rmqarc.DirectExchange, // Direct exchange type
		Durable:    true,                  // Survive server restarts
		AutoDelete: false,                 // Do not auto-delete
		Internal:   false,                 // Not an internal exchange
		NoWait:     false,                 // Wait for broker confirmation
		Args:       nil,                   // No extra arguments
	}

	// Define the queue binding configuration
	bindQCfg := rmqarc.QueueBindConfig{
		Queues:           []string{"queue1", "queue2"}, // Queues to bind
		Durable:          true,                         // Durable queues
		AutoDelete:       false,                        // Do not auto-delete
		Exclusive:        false,                        // Not exclusive
		NoWait:           false,                        // Wait for confirmation
		ArgsQueueDeclare: nil,                          // No extra args for declaration
		BindingKey:       "example-routing-key",        // Binding routing key
		ArgsQueueBind:    nil,                          // No extra args for binding
		PrefetchCount:    1,                            // Prefetch settings
		PrefetchSize:     0,
		PrefetchGlobal:   false,
	}

	// Combine exchange and queue configuration
	exchangeBindQueueCfg := rmqarc.ExchangeAndQueueBindConfig{
		ExchangeCfg:  exCfg,
		QueueBindCfg: bindQCfg,
	}

	// Set up the exchange and bind the queue
	err := conn.SetupExchangeAndQueue(exchangeBindQueueCfg)
	if err != nil {
		fmt.Printf("Failed to setup exchange and queue: %v", err)
		return
	}

	fmt.Println("Exchange and queues set up successfully.")
}

// ExampleConnection_StartConsume demonstrates how to start consuming messages
// from predefined queues using the StartConsume method. It shows how to set up
// a connection, define the consume configuration, and retrieve messages from
// the defined queues.
func ExampleConnection_StartConsume() {
	// Step 1: Initialize the connection configuration
	connCfg := rmqarc.ConnectionConfig{
		Host:                "localhost",
		Username:            "guest",
		Password:            "guest",
		Port:                ":5672",
		BaseRetryTimeout:    2 * time.Second,  // Retry base timeout
		Multiplier:          2.0,              // Retry multiplier
		MaxDelay:            30 * time.Second, // Max retry delay
		MaxRetry:            3,                // Max retry attempts
		CheckConnectionTime: 5 * time.Second,  // Time to check for a connection
	}

	// Step 2: Create a new connection using the connection configuration
	conn := rmqarc.New(connCfg, "example-connection")

	// Step 3: Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",    // Name of the exchange
		Kind:       rmqarc.DirectExchange, // Exchange type (e.g., direct, topic)
		Durable:    true,                  // The exchange should survive server restarts
		AutoDelete: false,                 // The exchange should not auto-delete when unused
		Internal:   false,                 // The exchange is not internal (accessible to clients)
		NoWait:     false,                 // Wait for a confirmation from the broker
		Args:       nil,                   // No additional arguments for this exchange
	}

	// Step 4: Set up the exchange using the SetupExchange method.
	err := conn.SetupExchange(exchangeCfg)
	if err != nil {
		fmt.Println("Failed to set up exchange:", err.Error())
		return
	}

	// Step 5: Define the queue configuration for binding queues to the exchange.
	queueCfg := rmqarc.QueueBindConfig{
		Queues: []string{
			"example-queue-1",
			"example-queue-2",
		}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                  // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                 // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                 // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                 // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                   // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       "routing-key-example", // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                   // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                     // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                     // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                 // Apply the prefetch count limit to the entire connection or just this channel
	}

	// Step 6: Bind the queues to the exchange using the SetupBindQueue method.
	err = conn.SetupBindQueue(queueCfg)
	if err != nil {
		fmt.Println("Failed to bind queues to the exchange:", err.Error())
		return
	}

	// Step 7: Define the consume configuration
	consumeCfg := rmqarc.ConsumeConfig{
		AutoAck:   false, // AutoAck: If false, the consumer must manually acknowledge messages (Manual Acknowledgment).
		Exclusive: false, // Exclusive: If true, the queue will only be accessible to this consumer.
		NoLocal:   false, // NoLocal: If true, messages published on the same connection will not be delivered to this consumer (rarely used in RabbitMQ).
		NoWait:    false, // NoWait: If true, the consumer does not wait for a response from the server and proceeds immediately.
		Args:      nil,   // Args: Additional arguments for consumer configuration (often nil unless specific options are needed).
	}

	// Step 8: Start consuming messages from the defined queues
	deliveries, err := conn.StartConsume(consumeCfg)
	if err != nil {
		fmt.Printf("Failed to start consuming messages: %v", err)
		return
	}

	// Step 9: Process the messages from the deliveries channels
	for queueName, deliveryChan := range deliveries {
		go func(queue string, msgs <-chan amqp.Delivery) {
			for msg := range msgs {
				fmt.Printf("Received message from %s: %s", queue, msg.Body)
				// Acknowledge the message after processing
				err = msg.Ack(false)
				if err != nil {
					fmt.Printf("Failed to acknowledge message: %v", err)
				}
			}
		}(queueName, deliveryChan)
	}

	fmt.Println("Started consuming messages from queues.")
}

// ExampleConnection_HandleConsumedDeliveries demonstrates how to consume messages from a queue
// and handle them using a custom message processing function. If the connection is lost,
// it attempts to reconnect and resumes message consumption.
func ExampleConnection_HandleConsumedDeliveries() {
	// Creates a channel to keep the main goroutine running indefinitely, typically used to prevent the program from exiting.
	forever := make(chan bool)

	// Step 1: Initialize the connection configuration
	connCfg := rmqarc.ConnectionConfig{
		Host:                "localhost",
		Username:            "guest",
		Password:            "guest",
		Port:                ":5672",
		BaseRetryTimeout:    2 * time.Second,  // Retry base timeout
		Multiplier:          2.0,              // Retry multiplier
		MaxDelay:            30 * time.Second, // Max retry delay
		MaxRetry:            3,                // Max retry attempts
		CheckConnectionTime: 5 * time.Second,  // Time to check for a connection
	}

	// Step 2: Create a new connection using the connection configuration
	conn := rmqarc.New(connCfg, "example-connection")

	// Step 3: Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",    // Name of the exchange
		Kind:       rmqarc.DirectExchange, // Exchange type (e.g., direct, topic)
		Durable:    true,                  // The exchange should survive server restarts
		AutoDelete: false,                 // The exchange should not auto-delete when unused
		Internal:   false,                 // The exchange is not internal (accessible to clients)
		NoWait:     false,                 // Wait for a confirmation from the broker
		Args:       nil,                   // No additional arguments for this exchange
	}

	// Step 4: Set up the exchange using the SetupExchange method.
	err := conn.SetupExchange(exchangeCfg)
	if err != nil {
		fmt.Println("Failed to set up exchange:", err.Error())
		return
	}

	// Step 5: Define the queue configuration for binding queues to the exchange.
	queueCfg := rmqarc.QueueBindConfig{
		Queues:           []string{"example-queue-1", "example-queue-2"}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                                           // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                                          // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                                          // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                                          // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                                            // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       "routing-key-example",                          // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                                            // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                                              // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                                              // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                                          // Apply the prefetch count limit to the entire connection or just this channel
	}

	// Step 6: Bind the queues to the exchange using the SetupBindQueue method.
	err = conn.SetupBindQueue(queueCfg)
	if err != nil {
		fmt.Println("Failed to bind queues to the exchange:", err.Error())
		return
	}

	// Step 7: Define the consume configuration
	consumeCfg := rmqarc.ConsumeConfig{
		AutoAck:   false, // AutoAck: If false, the consumer must manually acknowledge messages (Manual Acknowledgment).
		Exclusive: false, // Exclusive: If true, the queue will only be accessible to this consumer.
		NoLocal:   false, // NoLocal: If true, messages published on the same connection will not be delivered to this consumer (rarely used in RabbitMQ).
		NoWait:    false, // NoWait: If true, the consumer does not wait for a response from the server and proceeds immediately.
		Args:      nil,   // Args: Additional arguments for consumer configuration (often nil unless specific options are needed).
	}

	// Step 8: Start consuming messages from the defined queues
	deliveries, err := conn.StartConsume(consumeCfg)
	if err != nil {
		fmt.Printf("Failed to start consuming messages: %v", err)
		return
	}

	// Step 9: Define messageHandler
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

// ExampleHandleConsumedDeliveriesAutomatic demonstrates how to use
// HandleConsumedDeliveriesAutomatic to automatically process message deliveries
// from multiple queues, handling errors and reconnection attempts, while supporting
// context-based cancellation.
//
// In this example, a simple subscriber is defined that fmt.Printlns the message content and
// then acknowledges it. The message consumption runs until the context is canceled.
func ExampleConnection_HandleConsumedDeliveriesAutomatic() {
	// Creates a channel to keep the main goroutine running indefinitely, typically used to prevent the program from exiting.
	forever := make(chan bool)

	// Define an error channel to receive errors from the message processing
	errChan := make(chan error)

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is canceled to clean up resources

	// Using a concrete subscriberFunc
	//var subscriber = ConcreteSubscriber{}

	// Using a function as a subscriberFunc
	var subscriberFunc rmqarc.SubscriberFunc = func(ctx context.Context, msg rmqarc.Message) error {
		fmt.Printf("Received message from queue: %s, MessageID: %s, Body: %s",
			msg.MessageId, msg.Body.Type, string(msg.Body.Data))
		return nil
	}

	// Create a connection configuration
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

	// Initialize the connection
	conn := rmqarc.New(connCfg, "example-connection")

	// Define the exchange configuration for setting up the exchange.
	exchangeCfg := rmqarc.ExchangeConfig{
		Name:       "example-exchange",    // Name of the exchange
		Kind:       rmqarc.DirectExchange, // Exchange type (e.g., direct, topic)
		Durable:    true,                  // The exchange should survive server restarts
		AutoDelete: false,                 // The exchange should not auto-delete when unused
		Internal:   false,                 // The exchange is not internal (accessible to clients)
		NoWait:     false,                 // Wait for a confirmation from the broker
		Args:       nil,                   // No additional arguments for this exchange
	}

	// Set up the exchange using the SetupExchange method.
	err := conn.SetupExchange(exchangeCfg)
	if err != nil {
		fmt.Println("Failed to set up exchange:", err.Error())
		return
	}

	// Define the queue configuration for binding queues to the exchange.
	queueCfg := rmqarc.QueueBindConfig{
		Queues:           []string{"example-queue-1", "example-queue-2"}, // Define the queue names to be declared and bound to the exchange
		Durable:          true,                                           // The queue will survive a RabbitMQ server restart if set to true
		AutoDelete:       false,                                          // The queue will not be automatically deleted when no longer in use
		Exclusive:        false,                                          // The queue is not exclusive, meaning other connections can access it
		NoWait:           false,                                          // The client will wait for confirmation from the broker on queue declaration
		ArgsQueueDeclare: nil,                                            // Additional arguments for queue declaration (e.g., TTL or max length)
		BindingKey:       "routing-key-example",                          // The routing key used to bind the queue to the exchange
		ArgsQueueBind:    nil,                                            // Additional arguments for queue binding (can be used for advanced routing)
		PrefetchCount:    1,                                              // The number of messages the server will deliver before expecting an ack
		PrefetchSize:     0,                                              // The maximum size (in bytes) of unacknowledged messages (0 means unlimited)
		PrefetchGlobal:   false,                                          // Apply the prefetch count limit to the entire connection or just this channel
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

	go func() {

		go func() {
			for {
				if errCh := <-errChan; errCh != nil {
					// Logger
					fmt.Printf("Error occurred while handling automatic message deliveries: %v", errCh)
				}
			}
		}()

		conn.HandleConsumedDeliveriesAutomatic(ctx, errChan, subscriberFunc)

	}()

	// Blocks the main goroutine indefinitely, waiting for a value to be sent to the 'forever' channel.
	<-forever
}

// ExamplePublish demonstrates how to publish a message to an exchange using the Publish method.
func ExampleConnection_Publish() {
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
	conn := rmqarc.New(connCfg, "example-connection")

	// Define the publish configuration
	pubCfg := rmqarc.PublishConfig{
		ExchangeName: "example-exchange",    // The name of the exchange to publish to
		RoutingKey:   "example-routing-key", // The routing key for the message
		Mandatory:    false,                 // Delivery is not mandatory
		Immediate:    false,                 // Delivery is not immediate
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
		UserId:          "user-1",
		AppId:           "app-1",
		Body: rmqarc.MessageBody{
			Data: []byte(`{"key": "value"}`), // The message body data
			Type: "application/json",         // The type of the message body
		},
	}

	// Publish the message
	err := conn.Publish(pubCfg, msg)
	if err != nil {
		fmt.Println("Failed to publish message: ", err.Error())
	}

	fmt.Println("Message published successfully.")
}
