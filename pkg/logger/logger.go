package logger

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

//TODO прикрутить вывод ошибок в тг

type Logger struct {
	Log *logrus.Logger
}

func NewLogger() *Logger {
	l := logrus.New()
	l.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})
	l.Out = os.Stdout

	return &Logger{Log: l}
}

func (l *Logger) Info(data ...any) {
	l.Log.Info(data)
}

func (l *Logger) Warn(data ...any) {
	l.Log.Warn(data)
}

func (l *Logger) Error(data ...any) {
	l.Log.Error(data)
}

func (l *Logger) Fatal(data ...any) {
	l.Log.Fatal(data)
}

func (l *Logger) SetPath(path string) {
	logFile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		l.Error("Не удалось открыть/создать файл " + path + err.Error())
	} else {
		mw := io.MultiWriter(os.Stdout, logFile)
		l.Log.SetOutput(mw)
	}

}
