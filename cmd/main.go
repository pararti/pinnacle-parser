package main

import (
	app "github.com/pararti/pinnacle-parser/internal/core"
)

func main() {
	appInit := app.InitApp()

	// Set up logger path if specified
	if appInit.Opts.LogPath != "" {
		appInit.Logger.SetPath(appInit.Opts.LogPath)
	}
	go appInit.Engine.Start(appInit.Opts)
	appInit.Sender.Start(appInit.Opts.KafkaTopic)
}
