package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/birdayz/streamz"
	"github.com/birdayz/streamz/internal"
	"github.com/rs/zerolog"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	log := zerolog.New(output).With().Timestamp().Logger()

	t := internal.NewTopologyBuilder()

	err := internal.AddSource(t, "my-topic", "my-topic", StringDeserializer, StringDeserializer)
	if err != nil {
		panic(err)
	}

	p := internal.Processor[string, string, string, string](&MyProcessor{})
	p2 := internal.Processor[string, string, string, string](&MyProcessor2{})

	if err := internal.AddProcessor(t, "processor-a", func() internal.Processor[string, string, string, string] { return p }); err != nil {
		panic(err)
	}

	if err := internal.AddProcessor(t, "processor-2", func() internal.Processor[string, string, string, string] { return p2 }); err != nil {
		panic(err)
	}

	// TODO: add args with the two processors. so it's statically checked for types
	if err := internal.SetParent[string, string, string, string](t, "my-topic", "processor-a"); err != nil {
		panic(err)
	}

	if err := internal.SetParent[string, string, string, string](t, "processor-a", "processor-2"); err != nil {
		panic(err)
	}

	// task, err := t.CreateTask(streamz.TopicPartition{Topic: "my-topic", Partition: 1})
	// if err != nil {
	// 	panic(err)
	// }

	//task.Process(&kgo.Record{Topic: "my-topic", Partition: 1, Key: []byte(`abc`), Value: []byte(`def`)})
	// seeds := []string{"localhost:9092"}

	str := streamz.New(t, streamz.WithNumRoutines(2))

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

type MyProcessor struct{}

func (p *MyProcessor) Process(ctx internal.Context[string, string], k string, v string) error {
	fmt.Println("LEEEEEEEEEEEL", k, v)
	v2 := v + "-modified"
	ctx.Forward(k, v2)
	return nil
}

type MyProcessor2 struct{}

func (p *MyProcessor2) Process(ctx internal.Context[string, string], k string, v string) error {
	fmt.Println("second", k, v)
	return nil
}
