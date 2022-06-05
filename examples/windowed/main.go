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
		kstreams.WindowedStore(
			pebble.NewStoreBackend("/tmp/kstreams"), serdes.String, serdes.JSON[WindowState](),
		),
		"temperature-averages")
	kstreams.RegisterSource(t, "sensor-data", "sensor-data", serdes.StringDeserializer, serdes.JSONDeserializer[SensorData]())
	kstreams.RegisterProcessor(t, NewMyProcessor, "temperature-aggregator", "sensor-data", "temperature-averages")

	// TODO, not implemented:
	// Using store as parent node, which would allow streaming its changes to
	// topic and/or other processors

	// TODO: interesting are also, avg and more sophisticated aggregations, where
	// we need to store an intermediate value in the store, which is not the
	// output value.

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
