package utils

import (
	"github.com/rs/zerolog"
	"os"
)

var Logger = zerolog.Nop()

func InitLog() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	Logger = zerolog.New(os.Stdout).With().Timestamp().Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true, TimeFormat: "15:04:05.000"})
}
