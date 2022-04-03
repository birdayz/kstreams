package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/streamz"
	"github.com/birdayz/streamz/sdk"
	"github.com/rs/zerolog"

	"net/http"
	_ "net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	log := zerolog.New(output).With().Timestamp().Logger()

	// Move all internal stuff to public api
	t := streamz.NewTopologyBuilder()

	streamz.RegisterSource(t, "my-topic", "my-topic", StringDeserializer, StringDeserializer)

	p := streamz.NewProcessorBuilder("processor-1", NewMyProcessor)
	p2 := streamz.NewProcessorBuilder("processor-2", func() sdk.Processor[string, string, string, string] {
		return &MyProcessor2{}
	})

	streamz.RegisterProcessor(t, p, "my-topic")
	streamz.RegisterProcessor(t, p2, "processor-1")
	streamz.RegisterProcessorFunc(t, func(ctx sdk.Context[string, string], k string, v string) error {
		fmt.Printf("This is a simple in-line processor function. Received K=%s,V=%s\n", k, v)
		return nil
	}, "processor-3", "processor-2")

	str := streamz.New(t, streamz.WithNumRoutines(1000))

	log.Info().Msg("Start streamz")
	str.Start()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info().Msg("Received signal")

	str.Close()

	log.Info().Msg("Closed")
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

func NewMyProcessor() sdk.Processor[string, string, string, string] {
	return &MyProcessor{}
}

type MyProcessor struct{}

func (p *MyProcessor) Process(ctx sdk.Context[string, string], k string, v string) error {
	v2 := v + "-modified"
	ctx.Forward(k, v2)
	return nil
}

type MyProcessor2 struct{}

func (p *MyProcessor2) Process(ctx sdk.Context[string, string], k string, v string) error {
	fmt.Printf("Just printing out the data. Key=%s, Value=%s\n", k, v)
	ctx.Forward(k, v)
	return nil
}
