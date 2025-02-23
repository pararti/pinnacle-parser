package main

import (
	app "github.com/pararti/pinnacle-parser/internal/core"
	"github.com/pararti/pinnacle-parser/pkg/constants"
)

func main() {
	appInit := app.InitApp()
	go appInit.Engine.Start(appInit.Opts)
	appInit.Logger.Info(constants.TOPIC)
	appInit.Sender.Start(constants.TOPIC)
}
