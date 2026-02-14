package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				log.Println(err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("%s won a war against %s", winner, loser),
				Username:    gs.Player.Username,
			}
			if err := publishGameLog(gl, ch); err != nil {
				log.Println(err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
				Username:    gs.Player.Username,
			}
			if err := publishGameLog(gl, ch); err != nil {
				log.Println(err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			log.Println("error: cannot determine outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(gl routing.GameLog, ch *amqp.Channel) error {
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gl.Username, gl)
}
