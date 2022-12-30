package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/birdayz/kstreams/stores/pebble"
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
	t := kstreams.NewTopologyBuilder()

	kstreams.RegisterStore(t,
		kstreams.KVStore(
			pebble.NewStoreBackend("/tmp/kstreams"), serde.String, serde.String,
		),
		"my-store")
	kstreams.RegisterSource(t, "my-topic", "my-topic", serde.StringDeserializer, serde.StringDeserializer)
	kstreams.RegisterSource(t, "my-second-topic", "my-second-topic", serde.StringDeserializer, serde.StringDeserializer)
	kstreams.RegisterProcessor(t, NewMyProcessor, "processor-1", "my-topic", "my-store")
	kstreams.RegisterProcessor(t, NewMyProcessor, "processor-2", "my-second-topic", "my-store")
	kstreams.RegisterSink(t, "my-sink-topic", "my-sink-topic", serde.StringSerializer, serde.StringSerializer, "processor-2")

	app := kstreams.New(t.Build(), "my-app", kstreams.WithWorkersCount(1), kstreams.WithLogr(zerologr.New(log)))

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Info().Msg("Received signal. Closing app")
		app.Close()
	}()

	log.Info().Msg("Start kstreams")
	app.Run()
	log.Info().Msg("App exited")
}
