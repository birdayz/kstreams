package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var log *zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerologr.NameFieldName = "logger"
	zerologr.NameSeparator = "/"
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.000Z07:00"}
	zlog := zerolog.New(output).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	log = &zlog

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

}

func main() {
	builder := kstreams.NewTopologyBuilder()

	kstreams.MustRegisterSource(builder, "test", "test", serde.StringDeserializer, serde.StringDeserializer)
	kstreams.MustRegisterProcessor(builder,
		func() kstreams.Processor[string, string, string, string] {
			return &PrintlnProcessor{}
		},
		"printer",
		"test",
	)

	topology := builder.MustBuild()

	app := kstreams.New(topology, "my-sample-app", kstreams.WithLogr(zerologr.New(log)))
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		app.Close()
	}()

}

type PrintlnProcessor struct {
}

func (p *PrintlnProcessor) Init(stores ...kstreams.Store) error {
	return nil
}

func (p *PrintlnProcessor) Close() error {
	return nil
}

// TODO make output key WindowKey[string], and generalize this
func (p *PrintlnProcessor) Process(ctx kstreams.Context[string, string], k string, v string) error {
	fmt.Println("zzzz")
	fmt.Println(k, v)
	return nil
}
