package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/birdayz/kstreams"
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
