package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serdes"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var log *zerolog.Logger

func init() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
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
	bldr := kstreams.NewTopologyBuilder()

	kstreams.MustRegisterSource(bldr, "test", "test", serdes.StringDeserializer, serdes.StringDeserializer)
	kstreams.MustRegisterProcessor(bldr,
		func() kstreams.Processor[string, string, string, string] {
			return &PrintlnProcessor{}
		},
		"printer",
	)
	if err := kstreams.SetParent(bldr, "test", "printer"); err != nil {
		panic(err)
	}

	topology := bldr.Build()

	_ = topology
	app := kstreams.New(topology, "my-sample-app", kstreams.WithLogr(zerologr.New(log)))
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		fmt.Println("Close")
		app.Close()
	}()

	fmt.Println(app.Run())
}

type PrintlnProcessor struct {
}

func (p *PrintlnProcessor) Init(stores ...kstreams.Store) error {
	fmt.Println("inited")
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
