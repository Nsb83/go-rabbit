package rabbit

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
)

type ProducerConfig struct {
	ExchangeName string
	ExchangeType string
	Reconnect    struct {
		Interval time.Duration
	}
}

type Producer struct {
	config  ProducerConfig
	Rabbit  *Rabbit
	channel *amqp.Channel
}

func NewProducer(config ProducerConfig, rabbit *Rabbit) *Producer {
	return &Producer{
		config: config,
		Rabbit: rabbit,
	}
}

func (producer *Producer) closedConnectionListener(closed <-chan *amqp.Error) {
	errCh := <-closed
	if errCh != nil {
		for {
			log.Println(fmt.Sprintf("INFO: Attempting to reconnect producer to exchange %v", producer.config.ExchangeName))
			err := producer.Start()
			if err == nil {
				log.Println(fmt.Sprintf("INFO: Producer reconnected to exchange %v", producer.config.ExchangeName))
				break
			}
			if err != nil {
				log.Println(fmt.Sprintf("INFO: Producer %v did not start: %v", producer.config.ExchangeName, err))
			}
			time.Sleep(producer.config.Reconnect.Interval)
		}
	} else {
		log.Println("INFO: Connection closed normally, will not reconnect")
		os.Exit(0)
	}
}

func (producer *Producer) Start() error {
	con, err := producer.Rabbit.Connection()
	if err != nil {
		return err
	}
	producer.channel, err = con.Channel()
	if err != nil {
		return err
	}
	go producer.closedConnectionListener(producer.channel.NotifyClose(make(chan *amqp.Error)))

	if err = producer.channel.ExchangeDeclare(
		producer.config.ExchangeName,
		producer.config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func (producer *Producer) GetChannel() *amqp.Channel {
	if producer.channel == nil {
		newChan, err := producer.Rabbit.Channel()
		if err != nil {
			log.Println("Error creating producer channel: ", err)
			return nil
		}
		producer.channel = newChan
		return newChan
	} else {
		return producer.channel
	}
}

func (producer *Producer) SendMessage(message []byte, routingKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := producer.GetChannel().PublishWithContext(
		ctx,
		producer.config.ExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	if err != nil {
		log.Println("Failed to publish a message", err)
	}
}
