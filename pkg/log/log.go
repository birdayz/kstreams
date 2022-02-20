package log

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New() *zerolog.Logger {
	var output io.Writer
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		output = os.Stderr
	} else {
		output = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano

	logger := zerolog.New(output).With().Timestamp().Logger()
	return &logger
}
