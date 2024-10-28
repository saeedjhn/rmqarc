package main

import (
	"fmt"
	"github.com/saeedjhn/rmqarc"
	"time"
)

const (
	_connectionName  = "test.connection"
	_connectionName1 = "test.connection-1"
	_exchangeName    = "test.exchange"
	_queueName       = "test.queue"
	_bindingKeyName  = "test.key"
	_routingKeyName  = "test.key"
)

func main() {
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
		//CorrelationId:   "12345",
		//ReplyTo:         "response-queue",
		//Expiration:      "",
		//MessageId:       "msg-1",
		//Timestamp:       time.Now(),
		//Type:            "example.type",
		//UserId: "user-1",
		//AppId:  "app-1",
		Body: rmqarc.MessageBody{
			Data: []byte(`{"key": "value"}`), // The message body data
			Type: "application/json",         // The type of the message body
		},
	}

	for range 3 {
		//	// Publish the message
		err = conn.Publish(pubCfg, msg)
		if err != nil {
			fmt.Println("Failed to publish message: ", err.Error())
		}

		time.Sleep(2 * time.Second)
	}

	fmt.Println("Message published successfully.")
}
