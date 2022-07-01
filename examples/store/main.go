package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serdes"
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
	t := kstreams.NewTopology()

	kstreams.RegisterStore(t,
		kstreams.KVStore(
			pebble.NewStoreBackend("/tmp/kstreams"), serdes.String, serdes.String,
		),
		"my-store")
	kstreams.RegisterSource(t, "my-topic", "my-topic", serdes.StringDeserializer, serdes.StringDeserializer)
	kstreams.RegisterSource(t, "my-second-topic", "my-second-topic", serdes.StringDeserializer, serdes.StringDeserializer)
	kstreams.RegisterProcessor(t, NewMyProcessor, "processor-1", "my-topic", "my-store")
	kstreams.RegisterProcessor(t, NewMyProcessor, "processor-2", "my-second-topic", "my-store")
	kstreams.RegisterSink(t, "my-sink-topic", "my-sink-topic", serdes.StringSerializer, serdes.StringSerializer, "processor-2")

	app := kstreams.New(t, "my-app", kstreams.WithWorkersCount(1), kstreams.WithLogr(zerologr.New(log)))

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
