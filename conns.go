package rmqarc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var _connectionPool = make(map[string]*Connection)

// Connection represents a RabbitMQ connection along with its configurations
// such as exchange, queue, and delivery settings. It provides methods to connect,
// publish, and consume messages.
//
// Fields:
//
//	Name (string): A unique identifier for the RabbitMQ connection.
//
//	connCfg (ConnectionConfig): The configuration settings for the connection,
//	including the RabbitMQ server's credentials, host, and port.
//
//	connection (*amqp.Connection): A pointer to the AMQP connection instance,
//	which represents the active connection to the RabbitMQ server.
//
//	channel (*amqp.Channel): A pointer to the AMQP channel instance, which
//	allows for publishing and consuming messages on the RabbitMQ server.
//
//	exchangeCfg (ExchangeCfg): The configuration settings for the exchange
//	associated with this connection, detailing how messages are routed.
//
//	queueCfg (QueueBindCfg): The configuration settings for the queue, including
//	the name and properties of the queue used for message consumption.
//
//	consumeCfg (ConsumeConfig): The configuration settings for consuming messages,
//	such as acknowledgment behavior and prefetch counts.
//
//	exchangeAndQueueBindCfg (ExchangeAndQueueBindConfig): The combined configuration for both
//	the exchange and the associated queue, used during setup.
//
//	deliveries (map[string]<-chan amqp.Delivery): A map of delivery channels,
//	keyed by queue name, which receive messages delivered from the RabbitMQ server.
//
//	errChan (chan error): A channel used for reporting errors related to
//	the connection, allowing for asynchronous error handling.
type Connection struct {
	Name                    string
	ConnCfg                 ConnectionConfig
	connection              *amqp.Connection
	channel                 *amqp.Channel
	exchangeCfg             ExchangeConfig
	queueCfg                QueueBindConfig
	consumeCfg              ConsumeConfig
	exchangeAndQueueBindCfg ExchangeAndQueueBindConfig
	deliveries              map[string]<-chan amqp.Delivery
	errChan                 chan error
}

// GetConnection retrieves an existing connection from the global pool by its name.
//
// Parameters:
//
//	name (string): The name of the connection to retrieve from the pool.
//
// Returns:
//
//	*Connection: A pointer to the Connection instance associated with the
//	specified name. If the connection does not exist in the pool, the function
//	will return nil, allowing the caller to handle the absence of the connection
//	appropriately.
func GetConnection(name string) *Connection {
	return _connectionPool[name]
}

// Connections returns the entire connection pool as a map.
//
// Returns:
//
//	map[string]*Connection: A map where the keys are connection names (of type
//	string) and the values are pointers to Connection instances. This map
//	allows access to all currently established connections in the pool, enabling
//	the user to manage and utilize multiple connections easily.
func Connections() map[string]*Connection { return _connectionPool }

// New creates a new connection object, stores it in the global connection pool,
// and returns the newly created Connection instance.
//
// Parameters:
//   - connCfg: The configuration settings for the connection. This includes
//     necessary information such as username, password, host, and port.
//   - connectionName: A unique name for the connection. This name is used to
//     identify the connection in the global connection pool.
//
// Returns:
//
//	*Connection: A pointer to the newly created Connection instance if a new
//	connection was created. If a connection with the specified name already
//	exists in the pool, it returns the existing connection instead.
func New(
	config ConnectionConfig,
	connectionName string,
) *Connection {
	if c, ok := _connectionPool[connectionName]; ok {
		return c
	}

	c := &Connection{
		Name:    connectionName,
		ConnCfg: config,
		errChan: make(chan error),
	}

	_connectionPool[connectionName] = c

	return c
}

// Queues retrieves the list of queue names configured for the connection.
//
// This method returns a slice of strings containing the names of all the queues
// associated with the current connection's queue configuration. It allows users
// to access the queue names for further processing or monitoring.
//
// Returns:
//
//	[]string: A slice containing the names of the queues. If no queues are configured,
//	           it will return an empty slice.
func (c *Connection) Queues() []string {
	return c.queueCfg.Queues
}

// QueueBindConfig returns the current QueueBindConfig associated with the Connection instance.
// The QueueBindConfig includes details about how queues are bound to exchanges, including
// information like queue name, exchange name, routing key, and optional arguments.
//
// This method allows other parts of the application to retrieve the binding configuration
// for queues, which is crucial for managing RabbitMQ bindings and ensuring messages
// are routed correctly between exchanges and queues.
//
// Returns:
//   - QueueBindConfig: The current configuration for queue bindings, which contains
//     details such as the queue name, binding key and optional binding arguments.
func (c *Connection) QueueBindConfig() QueueBindConfig {
	return c.queueCfg
}

// ExchangeConfig returns the current ExchangeConfig associated with the Connection instance.
// The ExchangeConfig contains settings related to RabbitMQ exchanges, such as the exchange name,
// type, durability, and other options needed to manage exchanges.
//
// This method allows other parts of the application to access the configuration used for RabbitMQ exchanges,
// which is essential for ensuring messages are routed to the correct exchange and processed accordingly.
//
// Returns:
//   - ExchangeConfig: The configuration object containing the exchange's settings, which include
//     the exchange name, type (e.g., direct, fanout), durability, auto-delete behavior, and other exchange-specific options.
func (c *Connection) ExchangeConfig() ExchangeConfig {
	return c.exchangeCfg
}

// ConsumeConfig returns the current ConsumeConfig associated with the Connection instance.
// The ConsumeConfig contains settings for consuming messages from a RabbitMQ queue, such as
// the consumer name, auto-acknowledgment, exclusive consumer options, and other related settings.
//
// This method provides access to the configuration used for consuming messages, ensuring that
// the message consumption process adheres to the desired settings, such as whether to acknowledge
// messages automatically or whether to allow multiple consumers.
//
// Returns:
//   - ConsumeConfig: The configuration object that defines the parameters for message consumption,
//     including consumer name, auto-acknowledgment, exclusive behavior and more.
func (c *Connection) ConsumeConfig() ConsumeConfig {
	return c.consumeCfg
}

// ExchangeAndBindQueueConfig returns the configuration that combines both the exchange
// and queue binding settings for the current RabbitMQ connection.
// This configuration includes details about the exchange to which the queue is bound
// and the associated binding keys or routing configurations.
//
// The method is useful when you need to retrieve the combined settings that dictate how
// messages are routed from the exchange to the bound queues. It helps ensure that the
// exchange and queue binding configuration is properly set up for message consumption.
//
// Returns:
//   - ExchangeAndQueueBindConfig: A struct that holds the combined exchange and queue binding configuration.
//     This includes the exchange name, type, and the queue's binding keys or routing configurations.
//     It provides essential details required for routing messages correctly between exchanges and queues.
//
// Important:
//   - Ensure that the exchange and queue are correctly bound before using this configuration
//     for message consumption or publishing.
func (c *Connection) ExchangeAndBindQueueConfig() ExchangeAndQueueBindConfig {
	return c.exchangeAndQueueBindCfg
}

// Channel returns the current AMQP channel associated with the Connection instance.
// The channel is a core component used to interact with RabbitMQ, allowing for the publishing,
// consuming, and acknowledgment of messages. Channels provide a lightweight way to communicate
// with RabbitMQ over a single connection.
//
// This method gives access to the underlying *amqp.Channel, which is used to perform RabbitMQ operations.
// The returned channel can be used to declare exchanges, queues, publish messages, and set up consumers.
//
// Returns:
//   - *amqp.Channel: A pointer to the AMQP channel associated with the current connection.
//     This channel is used for sending and receiving messages with RabbitMQ, as well as other
//     queue-related operations such as binding, unbinding, and acknowledging deliveries.
//
// Important:
//   - Ensure that the channel is open and not closed before using it for any operations.
//     Closing a channel invalidates it for further use.
func (c *Connection) Channel() *amqp.Channel {
	return c.channel
}

// Connect establishes the connection to the RabbitMQ server and creates a channel for communication.
//
// This method constructs a URI using the connection configuration and attempts to connect to the
// RabbitMQ server using amqp.Dial. If the connection is successful, it sets up a listener for
// connection closure notifications and creates a communication channel.
//
// Returns:
//
//	error: If there is an error while connecting to the server or creating the channel,
//	        it returns an error wrapped with context. If the connection and channel are
//	        successfully established, it returns nil.
func (c *Connection) Connect() error {
	log.Println("Connect.Invoked")

	var err error
	uri := fmt.Sprintf("amqp://%s:%s@%s%s",
		c.ConnCfg.Username,
		c.ConnCfg.Password,
		c.ConnCfg.Host,
		c.ConnCfg.Port,
	)

	c.connection, err = amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf(
			"connection error: failed to connect to RabbitMQ server at %s: %w", uri, err)
	}

	go func() {
		<-c.connection.NotifyClose(make(chan *amqp.Error)) // Listen to NotifyClose
		c.errChan <- errors.New("Connection.Closed")
	}()

	c.channel, err = c.connection.Channel()
	if err != nil {
		return fmt.Errorf("channel error: failed to create channel after connection: %w", err)
	}

	return nil
}

// SetupExchange configures the exchange for the connection using the provided ExchangeConfig.
//
// This method declares an exchange with the specified settings in the ExchangeConfig.
// It utilizes the channel's ExchangeDeclare method to set up the exchange on the RabbitMQ server.
//
// Parameters:
//
//	connCfg: ExchangeCfg - The configuration settings for the exchange, which include:
//	  - Name: The name of the exchange.
//	  - Kind: The type of the exchange (e.g., direct, topic).
//	  - Durable: Indicates whether the exchange should survive a server restart.
//	  - AutoDelete: Indicates whether the exchange should be deleted when no longer in use.
//	  - Internal: Indicates whether the exchange is internal (i.e., used only by the broker).
//	  - NoWait: Indicates whether the client should wait for a confirmation from the broker.
//	  - Args: Additional arguments for the exchange declaration (if any).
//
// Returns:
//
//	error: If there is an error during the exchange declaration, it returns an error wrapped with context.
//	        If the exchange is successfully declared, it returns nil.
func (c *Connection) SetupExchange(cfg ExchangeConfig) error {
	log.Println("SetupExchange.Invoked")

	if err := c.channel.ExchangeDeclare(
		cfg.Name,
		cfg.Kind.String(),
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		cfg.Args,
	); err != nil {
		return fmt.Errorf("exchange setup error: failed to declare exchange '%s' of type '%s': %w",
			cfg.Name,
			cfg.Kind.String(),
			err,
		)
	}

	c.exchangeCfg = cfg

	return nil
}

// SetupBindQueue binds a queue to the exchange with the given QueueBindConfig.
// If no queues are defined in the connCfg, it binds a temporary queue.
// It iterates over all the queues specified in the QueueBindConfig and binds each queue.
//
// Parameters:
//   - connCfg (QueueBindConfig): The configuration of the queue, including the queue names, binding details, etc.
//
// Returns:
//   - error: If an error occurs during queue declaration or binding, it returns the error.
func (c *Connection) SetupBindQueue(cfg QueueBindConfig) error {
	log.Println("SetupBindQueue.Invoked")

	c.queueCfg = cfg

	if c.isEmptyQueues() {
		return c.declareAndBindQueue("", cfg)
	}

	for _, q := range cfg.Queues {
		if err := c.declareAndBindQueue(q, cfg); err != nil {
			return err
		}
	}

	return nil
}

// SetupExchangeAndQueue configures both the exchange and queue settings
// for the connection using the provided ExchangeAndQueueBindConfig. It first sets up
// the exchange defined in ExchangeConfig and then binds a queue using the
// provided QueueBindConfig. This method ensures that the exchange
// is correctly configured before proceeding to bind the queue to it.
//
// Parameters:
//   - connCfg: An ExchangeAndQueueBindConfig struct that contains the configuration
//     details for the exchange and the queue binding.
//
// Returns:
//   - error: Returns nil if the setup is successful; otherwise, returns an
//     error indicating what went wrong during the setup process.
func (c *Connection) SetupExchangeAndQueue(cfg ExchangeAndQueueBindConfig) error {
	log.Println("SetupExchangeAndQueue.Invoked")

	var err error

	if err = c.SetupExchange(cfg.ExchangeCfg); err != nil {
		return err
	}

	if err = c.SetupBindQueue(cfg.QueueBindCfg); err != nil {
		return err
	}

	c.exchangeAndQueueBindCfg = cfg

	return nil
}

// StartConsume starts consuming messages from the queues defined in the ConsumeConfig.
// It returns a map where the keys are the queue names and the values are channels
// through which deliveries are received. If no queues are defined, it creates
// and consumes from a temporary queue.
//
// Parameters:
//   - connCfg (ConsumeConfig): The configuration for consuming messages, which includes
//     queue names, prefetch count, and consumer tag details.
//
// Returns:
//   - map[string]<-chan amqp.Delivery: A map with the queue names as keys and channels
//     for receiving message deliveries as values.
//   - error: If an error occurs while starting consumption, it returns the error.
func (c *Connection) StartConsume(cfg ConsumeConfig) (map[string]<-chan amqp.Delivery, error) {
	log.Println("StartConsume.Invoked")

	m := make(map[string]<-chan amqp.Delivery)
	if c.isEmptyQueues() {
		return c.consumeFromTemporaryQueue(cfg, m)
	}

	return c.consumeFromDefinedQueues(cfg, m)
}

// HandleConsumedDeliveries processes message deliveries from a specific queue
// by invoking the provided callback function to handle the messages.
// It listens for errors on the connection's error channel and attempts to reconnect
// if the connection is lost, resuming message consumption after a successful reconnection.
//
// Parameters:
//   - queue (string): The name of the queue to consume from.
//   - delivery (<-chan amqp.Delivery): The delivery channel where messages are received.
//   - fn (func(Connection, string, <-chan amqp.Delivery)): The function to handle the messages.
//
// Returns:
//   - error: Returns an error if reconnection or consumption fails.
func (c *Connection) HandleConsumedDeliveries(
	queue string,
	delivery <-chan amqp.Delivery,
	fn func(Connection, string, <-chan amqp.Delivery),
) error {
	log.Println("handleConsumedDeliveries.Invoked")

	for {
		go fn(*c, queue, delivery)
		if err := <-c.errChan; err != nil {
			reconnectErr := c.reconnect()
			if reconnectErr != nil {
				return fmt.Errorf("error during reconnection process: %w", reconnectErr)
			}

			deliveries, consumeErr := c.StartConsume(c.consumeCfg)
			if consumeErr != nil {
				return fmt.Errorf("error restarting consumption proces: %w", consumeErr)
			}

			delivery = deliveries[queue]
		}
	}
}

// HandleConsumedDeliveriesAutomatic automatically handles message deliveries from multiple queues,
// processes them using the provided subscriber, and manages error handling, including connection
// recovery and message acknowledgment. It listens for errors and context cancellation,
// ensuring continuous consumption unless the context is explicitly cancelled.
//
// This method runs a goroutine for each queue, listening for messages, processing them,
// and sending errors back through the provided errChan. If the connection is lost or any
// other errors occur, it attempts to reconnect and resume consumption.
//
// Parameters:
//   - ctx (context.Context): The context used to manage the lifecycle of the message consumption.
//     When cancelled, all goroutines stop processing and exit gracefully.
//   - errChan (chan error): A channel for reporting errors during message consumption or connection issues.
//     If an error is encountered, it is sent through this channel. A nil value indicates successful message processing.
//   - subscriber (Subscriber): An implementation of the Subscriber interface that defines how to handle the consumed messages.
//     The method subscriber.Subscribe(ctx, Message) is called for each received message.
//
// Goroutine Behavior:
//
//	For each queue, a new goroutine is started that listens for deliveries on the delivery channel. The subscriber
//	processes each message, and based on the result, the message is either acknowledged (if successful) or rejected (if failed).
//	- If the delivery channel closes unexpectedly, the goroutine reports the error and exits.
//	- If the context is cancelled, the goroutine stops processing messages and exits gracefully.
//
// Connection Handling:
//   - The method continuously monitors the connection's error channel. If an error is detected, it tries to reconnect
//     and resumes the consumption process once the connection is re-established.
//
// Returns:
//   - None. However, errors and status updates are sent to errChan.
func (c *Connection) HandleConsumedDeliveriesAutomatic(
	ctx context.Context,
	errChan chan error,
	subscriber Subscriber,
) {
	log.Println("HandleConsumedDeliveriesAutomatic.Invoked")

	for {
		for queue, delivery := range c.deliveries {
			go func(queue string, delivery <-chan amqp.Delivery) {
				for {
					select {
					case <-ctx.Done():
						errChan <- fmt.Errorf("context cancelled: exiting goroutine for queue: %s", queue)

						return
					case msg, ok := <-delivery:
						if !ok {
							errChan <- fmt.Errorf("delivery channel closed unexpectedly for queue: %s", queue)

							return
						}

						m := Message{
							ContentType:     msg.ContentType,
							ContentEncoding: msg.ContentEncoding,
							DeliveryMode:    DeliveryType(msg.DeliveryMode),
							Priority:        msg.Priority,
							CorrelationId:   msg.CorrelationId,
							ReplyTo:         msg.ReplyTo,
							Expiration:      msg.Expiration,
							MessageId:       msg.MessageId,
							Timestamp:       msg.Timestamp,
							Type:            msg.Type,
							UserId:          msg.UserId,
							AppId:           msg.AppId,
							Body: MessageBody{
								Data: msg.Body,
								Type: msg.Type,
							},
						}

						err := subscriber.Subscribe(ctx, m)
						if err != nil {
							errChan <- fmt.Errorf("failed to process message delivery for queue %s: %w", queue, err)

							if err = msg.Reject(false); err != nil {
								errChan <- fmt.Errorf("failed to reject delivery for queue %s: %w", queue, err)
							}
						} else {
							err = msg.Ack(false) // Success
							if err != nil {
								errChan <- fmt.Errorf("failed to acknowledge delivery for queue %s: %w", queue, err)
							}

							errChan <- nil
						}
					}
				}

			}(queue, delivery)
		}
		// Wait for errors from the connection's error channel
		select {
		case err := <-c.errChan:
			if err != nil {
				// Attempt to reconnect if an error occurred
				reconnectErr := c.reconnect()
				if reconnectErr != nil {
					errChan <- fmt.Errorf("error during reconnection process: %w", reconnectErr)
					return
				}
				// Restart the consumption process
				deliveries, consumeErr := c.StartConsume(c.consumeCfg)
				if consumeErr != nil {
					errChan <- fmt.Errorf("error restarting consumption process: %w", consumeErr)
					return
				}

				// Update the deliveries map with the new delivery channels
				c.deliveries = deliveries
			}
		case <-ctx.Done():
			errChan <- errors.New("context cancelled, exiting delivery handler loop")
			return
		}
	}
}

// Publish sends a message to the configured exchange and routing key.
// It uses the provided PublishConfig to determine the routing and exchange settings.
// If an exchange is not selected in the connCfg or the connection's exchange configuration,
// it returns an error. It also checks for errors in a non-blocking manner from the error channel.
//
// Parameters:
//   - connCfg (PublishConfig): The configuration for publishing, which includes exchange name,
//     routing key, and flags like Mandatory and Immediate.
//   - msg (Message): The message to be published. It contains the content, headers, and metadata.
//
// Returns:
//   - error: Returns an error if publishing fails or if the exchange is not set.
func (c *Connection) Publish(cfg PublishConfig, msg Message) error {
	log.Println("Publish.Invoked")

	if len(cfg.ExchangeName) == 0 && len(c.exchangeCfg.Name) == 0 {
		return errors.New("exchange is not selected")
	}

	// non-blocking channel - if there is no error will go to default where we do nothing
	select {
	case err := <-c.errChan:
		if err != nil {
			return c.reconnect()
		}
	default:
	}

	p := amqp.Publishing{
		Headers:         amqp.Table{"type": msg.Body.Type},
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode.Uint8(),
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       time.Time{},
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body.Data,
	}

	ex := c.selectExchange(cfg)

	if err := c.channel.Publish(
		ex,
		cfg.RoutingKey,
		cfg.Mandatory,
		cfg.Immediate,
		p,
	); err != nil {
		return fmt.Errorf("Publish.Publish: %w", err)
	}

	return nil
}

// reconnect attempts to re-establish the RabbitMQ connection and reconfigures
// the exchange and queue settings with specified retry logic. It tries to
// connect to the RabbitMQ server multiple times as defined in the connection
// configuration. If a connection attempt fails, it will log the error and
// wait for a specified timeout before retrying. If the exchange and queue
// setup fails, it will return an error indicating the failure. The retry
// timeout is multiplied by a defined factor after each failed attempt,
// with a maximum delay enforced.
//
// Returns:
//   - error: Returns nil if the reconnection and setup are successful;
//     otherwise, returns an error containing details about the connection
//     and setup failures.

func (c *Connection) reconnect() error {
	log.Println("reconnect.Invoked")

	var (
		retryTimeout = c.ConnCfg.BaseRetryTimeout
		connErr      error
		exqErr       error
	)

	for range c.ConnCfg.MaxRetry {
		if connErr = c.Connect(); connErr == nil {
			return nil
		}

		if exqErr = c.SetupExchangeAndQueue(c.exchangeAndQueueBindCfg); exqErr == nil {
			return nil
		}

		retryTimeout = time.Duration(float64(retryTimeout) * c.ConnCfg.Multiplier)
		//log.Printf("retryTimeout: %v", retryTimeout)

		time.Sleep(retryTimeout)

		if retryTimeout > c.ConnCfg.MaxDelay {
			retryTimeout = c.ConnCfg.BaseRetryTimeout
		}
	}

	return fmt.Errorf(
		"reconnect failed after %d attempts: connection error: %w, exchange/queue setup error: %w",
		c.ConnCfg.MaxRetry,
		connErr,
		exqErr,
	)
}

func (c *Connection) isEmptyQueues() bool {
	return len(c.queueCfg.Queues) == 0 || c.queueCfg.Queues == nil
}

func (c *Connection) declareAndBindQueue(queueName string, cfg QueueBindConfig) error {
	log.Println("declareAndBindQueue.Invoked")

	q, err := c.channel.QueueDeclare(
		queueName,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		cfg.ArgsQueueDeclare,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue '%s': %w", queueName, err)
	}

	if err = c.channel.QueueBind(
		q.Name,
		cfg.BindingKey,
		c.exchangeCfg.Name,
		cfg.NoWait,
		cfg.ArgsQueueBind,
	); err != nil {
		return fmt.Errorf(
			"failed to bind queue '%s' to exchange '%s': %w",
			q.Name,
			c.exchangeCfg.Name,
			err,
		)
	}

	if err = c.channel.Qos(
		cfg.PrefetchCount,
		cfg.PrefetchSize,
		cfg.PrefetchGlobal,
	); err != nil {
		return fmt.Errorf("failed to set QoS for queue '%s': %w", q.Name, err)
	}

	return nil
}

func buildConsumeTag(queueName string) string {
	return fmt.Sprintf("%s-Tag", queueName)
}

func (c *Connection) consumeFromTemporaryQueue(
	cfg ConsumeConfig,
	deliveriesMap map[string]<-chan amqp.Delivery,
) (map[string]<-chan amqp.Delivery, error) {
	uuID := uuid.New().String()
	uuIDTag := buildConsumeTag(uuID)

	log.Printf(
		"StartConsume.Starting.To.Consume.From.Queue, ConsumerTag: %v",
		uuIDTag,
	)

	deliveries, err := c.channel.Consume(
		uuID,
		uuIDTag,
		cfg.AutoAck,
		cfg.Exclusive,
		cfg.NoLocal,
		cfg.NoWait,
		cfg.Args,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to consume from temporary queue with ConsumerTag '%s': %w",
			uuIDTag,
			err,
		)
	}

	c.consumeCfg = cfg

	deliveriesMap[uuID] = deliveries
	c.deliveries = deliveriesMap

	return deliveriesMap, nil
}

func (c *Connection) consumeFromDefinedQueues(
	cfg ConsumeConfig,
	deliveriesMap map[string]<-chan amqp.Delivery,
) (map[string]<-chan amqp.Delivery, error) {
	for _, q := range c.queueCfg.Queues {
		qTag := buildConsumeTag(q)

		log.Printf(
			"StartConsume.Starting.To.Consume.From.Queue, ConsumerTag: %v",
			buildConsumeTag(q),
		)

		deliveries, err := c.channel.Consume(
			q,
			qTag,
			cfg.AutoAck,
			cfg.Exclusive,
			cfg.NoLocal,
			cfg.NoWait,
			cfg.Args,
		)

		if err != nil {
			return nil, fmt.Errorf(
				"failed to consume from defined queue with ConsumerTag '%s': %w",
				qTag,
				err,
			)
		}

		deliveriesMap[q] = deliveries
	}

	c.consumeCfg = cfg
	c.deliveries = deliveriesMap

	return deliveriesMap, nil
}

func (c *Connection) selectExchange(cfg PublishConfig) string {
	ex := c.exchangeCfg.Name

	if len(cfg.ExchangeName) != 0 {
		ex = cfg.ExchangeName
	}

	return ex
}
