package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log/slog"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/lmittmann/tint"
)

var log = slog.New(tint.NewHandler(os.Stderr, nil))

func init() {

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
	kstreams.MustRegisterSink(builder, "testout", "testout", serde.StringSerializer, serde.StringSerializer, "printer")

	topology := builder.MustBuild()

	app := kstreams.New(topology, "my-sample-app", kstreams.WithLog(log), kstreams.WithCommitInterval(time.Second*2))
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		app.Close()
	}()

	if err := app.Run(); err != nil {
		log.Error("App run failed", err)
	}

}

type PrintlnProcessor struct {
	processorContext kstreams.ProcessorContext[string, string]
}

func (p *PrintlnProcessor) Init(processorContext kstreams.ProcessorContext[string, string]) error {
	p.processorContext = processorContext
	return nil
}

func (p *PrintlnProcessor) Close() error {
	return nil
}

// TODO make output key WindowKey[string], and generalize this
func (p *PrintlnProcessor) Process(ctx context.Context, k string, v string) error {
	p.processorContext.Forward(ctx, k, v+"out")
	// fmt.Println("zzzz")
	// fmt.Println(k, v)
	return nil
}
