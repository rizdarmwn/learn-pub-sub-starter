package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	// amqpCh, err := conn.Channel()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	amqpCh, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.DurableType)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connection to rabbitmq is successful.")

	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.DurableType, handlerLogs())
	if err != nil {
		log.Fatal(err)
	}

	gamelogic.PrintServerHelp()

outerLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("sending pause message...")
			err = pubsub.PublishJSON(amqpCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatal(err)
			}
		case "resume":
			fmt.Println("sending resume message...")
			err = pubsub.PublishJSON(amqpCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatal(err)
			}
		case "quit":
			fmt.Println("exiting...")
			break outerLoop
		default:
			fmt.Println("command unrecognized.")
		}
	}
	// signalCh := make(chan os.Signal, 1)
	// signal.Notify(signalCh, os.Interrupt)
	// <-signalCh
	fmt.Println("Shutting down program...")
	fmt.Println("Closing the connection...")
}
