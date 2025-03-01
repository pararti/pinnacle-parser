package core

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bytedance/sonic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pararti/pinnacle-parser/internal/models/kafkadata"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

type ConsumerKafka struct {
	logger   *logger.Logger
	consumer *kafka.Consumer
}

func NewConsumerKafka(l *logger.Logger, options *options.Options) *ConsumerKafka {
	addr := options.KafkaAddress + ":" + options.KafkaPort
	l.Info("Kafka consumer address: ", addr)

	kconf := &kafka.ConfigMap{
		"bootstrap.servers":  addr,
		"group.id":           "pinnacle-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}

	c, err := kafka.NewConsumer(kconf)
	if err != nil {
		l.Fatal("Failed to create Kafka consumer:", err)
	}

	return &ConsumerKafka{logger: l, consumer: c}
}

func (ck *ConsumerKafka) Start(topic string) {
	ck.logger.Info("Starting Kafka consumer for topic:", topic)

	err := ck.consumer.Subscribe(topic, nil)
	if err != nil {
		ck.logger.Fatal("Failed to subscribe to topic:", err)
	}

	// Set up signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			ck.logger.Info("Caught signal", sig, ": terminating")
			run = false
		default:
			ev := ck.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				ck.processMessage(e.Value)
			case kafka.Error:
				ck.logger.Error("Kafka error:", e)
				// Terminate on all errors for now
				run = false
			default:
				// Ignore other event types
			}
		}
	}

	ck.logger.Info("Closing consumer")
	ck.consumer.Close()
}

func (ck *ConsumerKafka) processMessage(data []byte) {
	// Try to unmarshal as a Match message first
	var matchMsg kafkadata.Match
	err := sonic.Unmarshal(data, &matchMsg)

	if err == nil && matchMsg.EventType == constants.MATCH_NEW {
		ck.logger.Info("Received new match message")
		fmt.Println("New Match Message:")
		fmt.Printf("Event Type: %d\n", matchMsg.EventType)
		fmt.Printf("Source: %s\n", matchMsg.Source)
		fmt.Printf("Number of matches: %d\n", len(matchMsg.Data))

		for i, match := range matchMsg.Data {
			// Extract participant names for home and away
			var homeName, awayName string
			if len(match.Participants) >= 2 {
				homeName = match.Participants[0].Name
				awayName = match.Participants[1].Name
			}

			leagueName := ""
			if match.League != nil {
				leagueName = match.League.Name
			}

			fmt.Printf("Match #%d: ID=%d, League=%s, Home=%s, Away=%s\n",
				i+1, match.ID, leagueName, homeName, awayName)
		}
		return
	}

	// If it's not a new match message, just log the event type
	ck.logger.Info("Received non-match-new message or failed to parse")
}

func (ck *ConsumerKafka) Stop() {
	ck.consumer.Close()
}
