package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	DurableType SimpleQueueType = iota
	TransientType
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        b,
	})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	amqpCh, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	var durable, autoDelete, exclusive bool
	switch queueType {
	case DurableType:
		durable, autoDelete, exclusive = true, false, false
	case TransientType:
		durable, autoDelete, exclusive = false, true, true
	}

	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	queue, err := amqpCh.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = amqpCh.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return amqpCh, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	amqpCh, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	if err = amqpCh.Qos(10, 0, true); err != nil {
		return err
	}

	delivery, err := amqpCh.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for v := range delivery {
			go func() {
				var tVal T
				err := json.Unmarshal(v.Body, &tVal)
				if err != nil {
					log.Printf("Unmarshal error: %v", err)
					return
				}
				ackType := handler(tVal)
				switch ackType {
				case Ack:
					err = v.Ack(false)
					if err != nil {
						log.Printf("Ack failed: %v", err)
						return
					}
					log.Println("Ack occured.")
				case NackRequeue:
					err = v.Nack(false, true)
					if err != nil {
						log.Printf("Nack failed: %v", err)
						return
					}
					log.Println("Nack occured. (false, true)")
				case NackDiscard:
					err = v.Nack(false, false)
					if err != nil {
						log.Printf("Nack failed: %v", err)
						return
					}
					log.Println("Nack occured. (false, false)")
				}
			}()
		}
	}()
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	amqpCh, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	if err = amqpCh.Qos(10, 0, true); err != nil {
		return err
	}

	delivery, err := amqpCh.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer amqpCh.Close()
		for v := range delivery {
			var tVal T
			buffer := bytes.NewBuffer(v.Body)
			dec := gob.NewDecoder(buffer)
			err := dec.Decode(&tVal)
			if err != nil {
				log.Printf("Failed to decode gob: %v\n", err)
				return
			}
			ackType := handler(tVal)
			switch ackType {
			case Ack:
				err = v.Ack(false)
				if err != nil {
					log.Printf("Ack failed: %v\n", err)
					return
				}
				log.Println("Ack occured.")
			case NackRequeue:
				err = v.Nack(false, true)
				if err != nil {
					log.Printf("Nack failed: %v\n", err)
					return
				}
				log.Println("Nack occured. (false, true)")
			case NackDiscard:
				err = v.Nack(false, false)
				if err != nil {
					log.Printf("Nack failed: %v\n", err)
					return
				}
				log.Println("Nack occured. (false, false)")
			}
		}
	}()
	return nil
}
