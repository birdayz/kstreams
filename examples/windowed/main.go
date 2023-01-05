package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/processors"
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

	// Use pebble for all stores
	storeBackend := pebble.NewStoreBackend("/tmp/kstreams")

	kstreams.RegisterSource(t, "sensor-data", "sensor-data", serde.StringDeserializer, serde.JSONDeserializer[SensorData]())

	p, s := processors.NewWindowedAggregator(
		func(s string, sd SensorData) time.Time { return sd.Timestamp }, // extract timestamp from message
		time.Hour, // window size
		func() WindowState { return WindowState{} }, // initial state
		func(sd SensorData, ws WindowState) WindowState { // aggregate
			ws.Count++
			return ws
		},
		func(ws WindowState) float64 { return float64(ws.Count) }, // finalize result
		storeBackend,
		serde.String,
		serde.JSON[WindowState](), // store aggregation state as JSON. We could use something much more efficient, like apache arrow
		"my-agg-store",
	)
	kstreams.RegisterStore(t, s, "my-agg-store")
	kstreams.RegisterProcessor(t, p, "my-agg-processor", "sensor-data", "my-agg-store")

	kstreams.RegisterSink(t, "custom-agg-out", "message-count", serde.JSONSerializer[processors.WindowKey[string]](), serde.JSONSerializer[float64](), "my-agg-processor")

	app := kstreams.New(t.MustBuild(), "my-app", kstreams.WithWorkersCount(1), kstreams.WithLogr(zerologr.New(log)))

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
