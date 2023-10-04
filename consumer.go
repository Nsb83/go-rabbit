package rabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"strconv"
	"time"
)

type ConsumerConfig struct {
	ExchangeName      string
	ExchangeType      string
	RoutingKey        string
	QueueName         string
	ConsumerName      string
	ConsumerCount     int
	PrefetchCount     int
	ReconnectInterval time.Duration
}

type Consumer struct {
	config          ConsumerConfig
	Rabbit          *Rabbit
	MessageHandlers map[string]func(amqp.Delivery) error
}

func NewConsumer(config ConsumerConfig, rabbit *Rabbit, handlers map[string]func(amqp.Delivery) error) *Consumer {
	return &Consumer{
		config:          config,
		Rabbit:          rabbit,
		MessageHandlers: handlers,
	}
}

func (c *Consumer) Start() error {
	con, err := c.Rabbit.Connection()
	if err != nil {
		return err
	}

	chn, err := con.Channel()
	go c.closedConnectionListener(chn.NotifyClose(make(chan *amqp.Error)))
	if err != nil {
		return err
	}

	if err = chn.ExchangeDeclare(
		c.config.ExchangeName,
		c.config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err = chn.QueueDeclare(
		c.config.QueueName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-dead-letter-exchange": "DLX"},
	); err != nil {
		return err
	}

	if err = chn.QueueBind(
		c.config.QueueName,
		c.config.RoutingKey,
		c.config.ExchangeName,
		false,
		nil,
	); err != nil {
		return err
	}

	if err = chn.Qos(c.config.PrefetchCount, 0, false); err != nil {
		return err
	}

	for i := 1; i <= c.config.ConsumerCount; i++ {
		id := i
		go c.consume(chn, id)
	}
	return nil
}

func (c *Consumer) closedConnectionListener(closed <-chan *amqp.Error) {
	err := <-closed
	if err != nil {
		log.Println("INFO: Closed connection:", err)
		for {
			log.Println("INFO: Attempting to reconnect Consumer")
			if err := c.Start(); err == nil {
				log.Println("INFO: Consumer Reconnected")
				break
			}
			time.Sleep(c.config.ReconnectInterval)
		}
	} else {
		log.Println("INFO: Connection closed normally, will not reconnect")
		os.Exit(0)
	}
}

func (c *Consumer) consume(channel *amqp.Channel, id int) {
	msgs, err := channel.Consume(
		c.config.QueueName,
		fmt.Sprintf("%s (%d/%d)", c.config.ConsumerName, id, c.config.ConsumerCount),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(fmt.Sprintf("CRITICAL: Unable to start consumer (%d/%d)", id, c.config.ConsumerCount))
		return
	}
	for msg := range msgs {
		handlingF, ok := c.MessageHandlers[msg.RoutingKey]
		if !ok {
			log.Println("Routing key absente: " + msg.RoutingKey)
			msg.Nack(false, false)
			continue
		}
		err = handlingF(msg)
		if err == nil {
			msg.Ack(false)
		} else {
			msg.Nack(false, false)
		}
	}
	log.Println("consumer [" + strconv.Itoa(id) + "] Exiting ...")
}
