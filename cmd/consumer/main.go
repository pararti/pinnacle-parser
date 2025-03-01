package main

import (
	"github.com/pararti/pinnacle-parser/internal/core"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

func main() {
	// Initialize logger
	log := logger.NewLogger()
	log.Info("Starting Pinnacle Kafka Consumer")

	// Load options
	opts, err := options.NewOptions()
	if err != nil {
		log.Fatal("Failed to load options:", err)
	}

	// Create and start the consumer
	consumer := core.NewConsumerKafka(log, opts)
	consumer.Start(constants.TOPIC)
}
