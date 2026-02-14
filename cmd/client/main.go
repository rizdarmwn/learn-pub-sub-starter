package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.TransientType, handlerPause(gameState))
	if err != nil {
		log.Fatal(err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.TransientType, handlerMove(gameState, ch))
	if err != nil {
		log.Fatal(err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.DurableType, handlerWar(gameState, ch))
	if err != nil {
		log.Fatal(err)
	}
outerLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				log.Println(err)
			}
		case "move":
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				log.Println(err)
				continue
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, armyMove)
			if err != nil {
				log.Println(err)
				continue
			}
			fmt.Printf("%s moved to %s with these units:\n", armyMove.Player.Username, armyMove.ToLocation)
			for _, unit := range armyMove.Units {
				fmt.Printf(" - %d\n", unit.ID)
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("invalid usage. command: spam <times_to_spam>")
				continue
			}
			spamTimes, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("invalid usage. command: spam <times_to_spam>")
				continue
			}
			for range spamTimes {
				logStr := gamelogic.GetMaliciousLog()
				err = publishGameLog(routing.GameLog{
					CurrentTime: time.Now(),
					Message:     logStr,
					Username:    username,
				}, ch)
				if err != nil {
					fmt.Printf("error publishing gamelog: %v\n", err)
					continue outerLoop
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			break outerLoop
		default:
			fmt.Println("command unrecognized.")
		}
	}
}
