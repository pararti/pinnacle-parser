package main

import (
	app "github.com/pararti/pinnacle-parser/internal/core"
	"github.com/pararti/pinnacle-parser/pkg/constants"
)

func main() {
	appInit := app.InitApp()

	// Set up logger path if specified
	if appInit.Opts.LogPath != "" {
		appInit.Logger.SetPath(appInit.Opts.LogPath)
	}

	if appInit.Opts.TestMode {
		// Initialize and start test mode
		testMode := app.NewTestMode(appInit.Logger, appInit.Sender)
		testMode.Start(constants.TOPIC)
		// Block main thread to keep logging
		select {}
	} else {
		// Start normal mode
		go appInit.Engine.Start(appInit.Opts)
		appInit.Sender.Start(constants.TOPIC)
	}
}
