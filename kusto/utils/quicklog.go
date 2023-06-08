package utils

import (
	"github.com/rs/zerolog"
	"os"
)

var Logger = zerolog.Nop()

func InitLog() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}
