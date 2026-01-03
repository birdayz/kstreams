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
	"github.com/birdayz/kstreams/internal/execution"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/lmittmann/tint"
)

var log = slog.New(tint.NewHandler(os.Stderr, nil))

func init() {

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

}

func main() {
	builder := kdag.NewBuilder()

	execution.MustRegisterSource(builder, "test", "test", kserde.StringDeserializer, kserde.StringDeserializer)
	execution.MustRegisterProcessor(builder,
		func() kprocessor.Processor[string, string, string, string] {
			return &PrintlnProcessor{}
		},
		"printer",
		"test",
	)
	execution.MustRegisterSink(builder, "testout", "testout", kserde.StringSerializer, kserde.StringSerializer, "printer")

	topology := builder.MustBuild()

	app := kstreams.MustNew(topology, "my-sample-app", kstreams.WithLog(log), kstreams.WithCommitInterval(time.Second*2))
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
	processorContext kprocessor.ProcessorContext[string, string]
}

func (p *PrintlnProcessor) Init(processorContext kprocessor.ProcessorContext[string, string]) error {
	p.processorContext = processorContext
	return nil
}

func (p *PrintlnProcessor) Close() error {
	return nil
}

func (p *PrintlnProcessor) Process(ctx context.Context, k string, v string) error {
	p.processorContext.Forward(ctx, k, v+"out")
	return nil
}
