package core

import (
	"github.com/pararti/pinnacle-parser/internal/abstruct"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/internal/storage"
	"github.com/pararti/pinnacle-parser/pkg/logger"
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
		l.Error(err.Error())
	}

	s := storage.NewMapStorage()
	e := NewEngine(l, s)
	sender := NewSenderKafka(l, o, s)

	return &App{Logger: l, Opts: o, Storage: s, Engine: e, Sender: sender}
}
