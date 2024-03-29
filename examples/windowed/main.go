package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/processors"
	"github.com/birdayz/kstreams/serde"
	"github.com/birdayz/kstreams/stores/pebble"
	"github.com/lmittmann/tint"

	"net/http"
	_ "net/http"
	_ "net/http/pprof"
)

var log = slog.New(tint.NewHandler(os.Stderr, nil))

func init() {
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
	kstreams.MustRegisterProcessor(t, p, "my-agg-processor", "sensor-data", "my-agg-store")

	kstreams.MustRegisterSink(t, "custom-agg-out", "message-count", serde.JSONSerializer[processors.WindowKey[string]](), serde.JSONSerializer[float64](), "my-agg-processor")

	app := kstreams.New(t.MustBuild(), "my-app", kstreams.WithWorkersCount(1), kstreams.WithLog(log))

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Info("Received signal. Closing app")
		app.Close()
	}()

	log.Info("Start kstreams")
	app.Run()
	log.Info("App exited")
}
