package core

import (
	"github.com/pararti/pinnacle-parser/internal/abstruct"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/internal/storage"
	"github.com/pararti/pinnacle-parser/pkg/logger"
	"os"
)

type App struct {
	Opts    *options.Options
	Logger  *logger.Logger
	Engine  abstruct.Engine
	Storage *storage.MapStorage
	Sender  abstruct.Sender
}

func InitApp() *App {
	l := logger.NewLogger()

	o, err := options.NewOptions()
	if err != nil {
		l.Fatal(err.Error())
		os.Exit(1)
	}

	s := storage.NewMapStorage()
	//sender := NewSenderKafka(l, o, s)
	var e abstruct.Engine
	var sender abstruct.Sender
	if o.TestMode {
		sender = NewTestSender(NewSenderKafka(l, o, s))
		e = NewTestMode(l, sender)
	} else {
		sender = NewSenderKafka(l, o, s)
		e = NewEngine(l, s)
	}

	return &App{Logger: l, Opts: o, Storage: s, Engine: e, Sender: sender}
}
