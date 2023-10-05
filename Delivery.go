package rabbit

import amqp "github.com/rabbitmq/amqp091-go"

type Delivery struct {
	amqp.Delivery
}
