package rabbit

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
)

type Config struct {
	Schema            string
	Username          string
	Password          string
	Host              string
	Port              string
	VHost             string
	ConnectionName    string
	ReconnectInterval time.Duration
}

type Rabbit struct {
	config     Config
	connection *amqp.Connection
}

func NewRabbit(config Config) *Rabbit {
	return &Rabbit{
		config: config,
	}
}

func (r *Rabbit) Connect() error {
	if r.connection == nil || r.connection.IsClosed() {
		con, err := amqp.DialConfig(fmt.Sprintf(
			"%s://%s:%s@%s:%s/%s",
			r.config.Schema,
			r.config.Username,
			r.config.Password,
			r.config.Host,
			r.config.Port,
			r.config.VHost,
		), amqp.Config{Properties: amqp.Table{"connection_name": r.config.ConnectionName}})
		if err != nil {
			return err
		}
		go r.closedConnectionListener(con.NotifyClose(make(chan *amqp.Error)))
		r.connection = con
	}
	return nil
}

func (r *Rabbit) Connection() (*amqp.Connection, error) {
	if r.connection == nil || r.connection.IsClosed() {
		return nil, errors.New("connection is not open")
	}
	return r.connection, nil
}

func (r *Rabbit) Channel() (*amqp.Channel, error) {
	chn, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}
	return chn, nil
}

func (r *Rabbit) closedConnectionListener(closed <-chan *amqp.Error) {
	err := <-closed
	if err != nil {
		log.Println("INFO: Closed connection:", err)
		for {
			log.Println("INFO: Attempting to reconnect")
			if err := r.Connect(); err == nil {
				log.Println("INFO: Reconnected")
				break
			}
			time.Sleep(r.config.ReconnectInterval)
		}
	} else {
		log.Println("INFO: Connection closed normally, will not reconnect")
		os.Exit(0)
	}
}
