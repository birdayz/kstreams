package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/streamz"
	"github.com/birdayz/streamz/serdes"
	"github.com/birdayz/streamz/stores/pebble"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"

	"net/http"
	_ "net/http"
	_ "net/http/pprof"
)

var log *zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerologr.NameFieldName = "logger"
	zerologr.NameSeparator = "/"
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	zlog := zerolog.New(output).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	log = &zlog

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

}

func main() {
	t := streamz.NewTopology()

	streamz.RegisterStore(t, streamz.WrapStore(pebble.NewStoreBuilder("/tmp/streamz", "abc"), serdes.NewString(), serdes.NewString()), "my-store")

	streamz.RegisterSource(t, "my-topic", "my-topic", serdes.StringDeserializer, serdes.StringDeserializer)
	streamz.RegisterSource(t, "my-second-topic", "my-second-topic", serdes.StringDeserializer, serdes.StringDeserializer)
	streamz.RegisterProcessor(t, NewMyProcessor, "processor-1", "my-topic", "my-store")
	streamz.RegisterProcessor(t, NewMyProcessor, "processor-2", "my-second-topic", "my-store")
	streamz.RegisterSink(t, "my-sink-topic", "my-sink-topic", serdes.StringSerializer, serdes.StringSerializer, "processor-2")

	app := streamz.New(t, streamz.WithNumRoutines(1), streamz.WithLogr(zerologr.New(log)))

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Info().Msg("Received signal. Closing app")
		app.Close()
	}()

	log.Info().Msg("Start streamz")
	app.Run()
	log.Info().Msg("App exited")
}
