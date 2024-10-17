// Package go_rabbitmq_automatically_recovering_connections provides functionalities for managing RabbitMQ connections,
// publishing messages, subscribing to queues, and automatically handling message deliveries.
package go_rabbitmq_automatically_recovering_connections

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
//	config (ConnectionConfig): The configuration settings for the connection,
//	including the RabbitMQ server's credentials, host, and port.
//
//	connection (*amqp.Connection): A pointer to the AMQP connection instance,
//	which represents the active connection to the RabbitMQ server.
//
//	channel (*amqp.Channel): A pointer to the AMQP channel instance, which
//	allows for publishing and consuming messages on the RabbitMQ server.
//
//	exchangeCfg (ExchangeConfig): The configuration settings for the exchange
//	associated with this connection, detailing how messages are routed.
//
//	queueCfg (QueueConfig): The configuration settings for the queue, including
//	the name and properties of the queue used for message consumption.
//
//	consumeCfg (ConsumeConfig): The configuration settings for consuming messages,
//	such as acknowledgment behavior and prefetch counts.
//
//	exchangeQueueCfg (ExchangeQueueConfig): The combined configuration for both
//	the exchange and the associated queue, used during setup.
//
//	deliveries (map[string]<-chan amqp.Delivery): A map of delivery channels,
//	keyed by queue name, which receive messages delivered from the RabbitMQ server.
//
//	errChan (chan error): A channel used for reporting errors related to
//	the connection, allowing for asynchronous error handling.
type Connection struct {
	config           ConnectionConfig
	connection       *amqp.Connection
	channel          *amqp.Channel
	exchangeCfg      ExchangeConfig
	queueCfg         QueueConfig
	consumeCfg       ConsumeConfig
	exchangeQueueCfg ExchangeQueueConfig
	deliveries       map[string]<-chan amqp.Delivery
	errChan          chan error
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
//   - config: The configuration settings for the connection. This includes
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
		config:  config,
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
		c.config.Username,
		c.config.Password,
		c.config.Host,
		c.config.Port,
	)

	c.connection, err = amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf(
			"Connect.Failed.Connect.To.Server %s : %s", uri, err.Error())
	}

	go func() {
		<-c.connection.NotifyClose(make(chan *amqp.Error)) // Listen to NotifyClose
		c.errChan <- errors.New("Connection.Closed")
	}()

	c.channel, err = c.connection.Channel()
	if err != nil {
		return fmt.Errorf("Connect.Channel: %w", err)
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
//	cfg: ExchangeConfig - The configuration settings for the exchange, which include:
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
		return fmt.Errorf("SetupExchange.ExchangeDeclare: %w", err)
	}

	c.exchangeCfg = cfg

	return nil
}

// Reconnect attempts to re-establish the RabbitMQ connection and reconfigures
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
func (c *Connection) Reconnect() error {
	log.Println("Reconnect.Invoked")

	var (
		retryTimeout = c.config.BaseRetryTimeout
		connErr      error
		exqErr       error
	)

	for range c.config.MaxRetry {
		if connErr = c.Connect(); connErr == nil {
			return nil
		}

		if exqErr = c.SetupExchangeAndQueue(c.exchangeQueueCfg); exqErr == nil {
			return nil
		}

		retryTimeout = time.Duration(float64(retryTimeout) * c.config.Multiplier)
		log.Printf("retryTimeout: %v", retryTimeout)

		time.Sleep(retryTimeout)

		if retryTimeout > c.config.MaxDelay {
			retryTimeout = c.config.BaseRetryTimeout
		}
	}

	return fmt.Errorf(
		"maxRetry: %d, connErr: %w, exqErr: %w",
		c.config.MaxRetry,
		connErr,
		exqErr,
	)
}

// SetupExchangeAndQueue configures both the exchange and queue settings
// for the connection using the provided ExchangeQueueConfig. It first sets up
// the exchange defined in ExchangeConfig and then binds a queue using the
// provided DeclareAndBindQConfig. This method ensures that the exchange
// is correctly configured before proceeding to bind the queue to it.
//
// Parameters:
//   - cfg: An ExchangeQueueConfig struct that contains the configuration
//     details for the exchange and the queue binding.
//
// Returns:
//   - error: Returns nil if the setup is successful; otherwise, returns an
//     error indicating what went wrong during the setup process.
func (c *Connection) SetupExchangeAndQueue(cfg ExchangeQueueConfig) error {
	log.Println("SetupExchangeAndQueue.Invoked")

	var err error

	if err = c.SetupExchange(cfg.ExchangeConfig); err != nil {
		return fmt.Errorf("SetupExchangeAndQueue.SetupExchange: %w", err)
	}

	if err = c.SetupBindQueue(cfg.DeclareAndBindQConfig); err != nil {
		return fmt.Errorf("SetupExchangeAndQueue.SetupBindQueue: %w", err)
	}

	c.exchangeQueueCfg = cfg

	return nil
}

// SetupBindQueue binds a queue to the exchange with the given QueueConfig.
// If no queues are defined in the config, it binds a temporary queue.
// It iterates over all the queues specified in the QueueConfig and binds each queue.
//
// Parameters:
//   - cfg (QueueConfig): The configuration of the queue, including the queue names, binding details, etc.
//
// Returns:
//   - error: If an error occurs during queue declaration or binding, it returns the error.
func (c *Connection) SetupBindQueue(cfg QueueConfig) error {
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

// StartConsume starts consuming messages from the queues defined in the ConsumeConfig.
// It returns a map where the keys are the queue names and the values are channels
// through which deliveries are received. If no queues are defined, it creates
// and consumes from a temporary queue.
//
// Parameters:
//   - cfg (ConsumeConfig): The configuration for consuming messages, which includes
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

// Publish sends a message to the configured exchange and routing key.
// It uses the provided PublishConfig to determine the routing and exchange settings.
// If an exchange is not selected in the config or the connection's exchange configuration,
// it returns an error. It also checks for errors in a non-blocking manner from the error channel.
//
// Parameters:
//   - cfg (PublishConfig): The configuration for publishing, which includes exchange name,
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
			return c.Reconnect()
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
			reconnectErr := c.Reconnect()
			if reconnectErr != nil {
				return fmt.Errorf("handleConsumeDeliveries.Reconnect: %w", reconnectErr)
			}

			deliveries, consumeErr := c.StartConsume(c.consumeCfg)
			if consumeErr != nil {
				return fmt.Errorf("handleConsumeDeliveries.StartConsume: %w", consumeErr)
			}

			delivery = deliveries[queue]
		}
	}
}

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
							err = msg.Ack(false) // success work
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
				reconnectErr := c.Reconnect()
				if reconnectErr != nil {
					errChan <- fmt.Errorf("error during reconnection process: %w", reconnectErr)
					// return fmt.Errorf("HandleConsumedDeliveries.Reconnect: %w", reconnectErr)
					return
				}
				// Restart the consumption process
				deliveries, consumeErr := c.StartConsume(c.consumeCfg)
				if consumeErr != nil {
					errChan <- fmt.Errorf("error restarting consumption process: %w", consumeErr)
					// return fmt.Errorf("HandleConsumedDeliveries.StartConsume: %w", consumeErr)
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

func (c *Connection) isEmptyQueues() bool {
	return len(c.queueCfg.Queues) == 0 || c.queueCfg.Queues == nil
}

func (c *Connection) declareAndBindQueue(queueName string, cfg QueueConfig) error {
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
		return fmt.Errorf("declareAndBindQueue.QueueDeclare: %w", err)
	}

	if err = c.channel.QueueBind(
		q.Name,
		cfg.BindingKey,
		c.exchangeCfg.Name,
		cfg.NoWait,
		cfg.ArgsQueueBind,
	); err != nil {
		return fmt.Errorf("declareAndBindQueue.QueueBind: %w", err)
	}

	if err = c.channel.Qos(
		cfg.PrefetchCount,
		cfg.PrefetchSize,
		cfg.PrefetchGlobal,
	); err != nil {
		return fmt.Errorf("declareAndBindQueue.Qos: %w", err)
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
		return nil, err
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
		log.Printf(
			"StartConsume.Starting.To.Consume.From.Queue, ConsumerTag: %v",
			buildConsumeTag(q),
		)

		deliveries, err := c.channel.Consume(
			q,
			buildConsumeTag(q),
			cfg.AutoAck,
			cfg.Exclusive,
			cfg.NoLocal,
			cfg.NoWait,
			cfg.Args,
		)

		if err != nil {
			return nil, err
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
