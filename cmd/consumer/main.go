package main

import (
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/pararti/pinnacle-parser/internal/consumer"
	"github.com/pararti/pinnacle-parser/internal/options"
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
		return
	}

	// Initialize Sentry
	err = sentry.Init(sentry.ClientOptions{
		Dsn:         opts.ConsumerSentry,
		Environment: "production",
		Debug:       opts.TestMode,
	})

	if err != nil {
		log.Error("Sentry initialization failed:", err)
	} else {
		// Ensure all events are sent to Sentry before the program exits
		defer sentry.Flush(2 * time.Second)
		log.Info("Sentry initialized successfully")

		// Set service identifier for Sentry events
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetTag("service", "consumer")
		})
	}

	// Test Sentry with a sample message if in test mode
	if opts.TestMode {
		sentry.CaptureMessage("Consumer started in test mode")
	}

	// Create and start the consumer
	c := consumer.NewConsumerKafka(log, opts)
	c.Start(opts.KafkaTopic)
}
