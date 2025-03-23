package main

import (
	"time"

	"github.com/getsentry/sentry-go"
	app "github.com/pararti/pinnacle-parser/internal/core"
)

func main() {
	appInit := app.InitApp()

	// Set up logger path if specified
	if appInit.Opts.LogPath != "" {
		appInit.Logger.SetPath(appInit.Opts.LogPath)
	}

	// Initialize Sentry
	err := sentry.Init(sentry.ClientOptions{
		Dsn:         appInit.Opts.ProducerSentry,
		Environment: "production",
		Debug:       appInit.Opts.TestMode,
	})

	// If Sentry initialization failed, log the error
	if err != nil {
		appInit.Logger.Error("Sentry initialization failed:", err)
	} else {
		// Ensure all events are sent to Sentry before the program exits
		defer sentry.Flush(2 * time.Second)
		appInit.Logger.Info("Sentry initialized successfully")

		// Set user identifier for Sentry events
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetTag("service", "producer")
		})
	}

	// Test Sentry with a sample error if in test mode
	if appInit.Opts.TestMode {
		sentry.CaptureMessage("Producer started in test mode")
	}

	go appInit.Engine.Start(appInit.Opts)
	appInit.Sender.Start(appInit.Opts.KafkaTopic)
}
