package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/internal/execution"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kserde"
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
	t := kdag.NewBuilder()

	// kdag.RegisterStore(t,
	// kstreams.KVStore(
	// 	pebble.NewStoreBackend("/tmp/kstreams"), kserde.String, kserde.String,
	// ),
	// "my-store")
	execution.RegisterSource(t, "cdc.ExampleTable", "cdc.ExampleTable", kserde.StringDeserializer, kserde.StringDeserializer)
	// execution.RegisterSource(t, "my-second-topic", "my-second-topic", kserde.StringDeserializer, kserde.StringDeserializer)
	execution.RegisterProcessor(t, NewMyProcessor, "processor-1", "cdc.ExampleTable")
	// execution.RegisterProcessor(t, NewMyProcessor, "processor-2", "my-second-topic", "my-store")
	// execution.RegisterSink(t, "my-sink-topic", "my-sink-topic", kserde.StringSerializer, kserde.StringSerializer, "processor-2")

	app := kstreams.MustNew(t.MustBuild(), "my-app", kstreams.WithWorkersCount(1), kstreams.WithLog(log))

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
