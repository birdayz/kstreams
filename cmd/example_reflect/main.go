package main

import (
	"fmt"

	"github.com/birdayz/streamz"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	t := streamz.NewTopologyBuilder()

	err := streamz.AddSource(t, "my-topic", "my-topic", StringDeserializer, StringDeserializer)
	if err != nil {
		panic(err)
	}

	p := streamz.Process0r[string, string, string, string](&MyProcessor{})
	p2 := streamz.Process0r[string, string, string, string](&MyProcessor2{})

	if err := streamz.AddProcessor(t, "processor-a", func() streamz.Process0r[string, string, string, string] { return p }); err != nil {
		panic(err)
	}

	if err := streamz.AddProcessor(t, "processor-2", func() streamz.Process0r[string, string, string, string] { return p2 }); err != nil {
		panic(err)
	}

	// TODO: add args with the two processors. so it's statically checked for types
	if err := streamz.SetParent[string, string, string, string](t, "my-topic", "processor-a"); err != nil {
		panic(err)
	}

	if err := streamz.SetParent[string, string, string, string](t, "processor-a", "processor-2"); err != nil {
		panic(err)
	}

	// task, err := t.CreateTask(streamz.TopicPartition{Topic: "my-topic", Partition: 1})
	// if err != nil {
	// 	panic(err)
	// }

	//task.Process(&kgo.Record{Topic: "my-topic", Partition: 1, Key: []byte(`abc`), Value: []byte(`def`)})
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("streamzz"),
		kgo.ConsumeTopics("my-topic"),
	)
	if err != nil {
		panic(err)
	}

	sr := streamz.NewStreamRoutine(cl, t)
	sr.Loop()
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

type MyProcessor struct{}

func (p *MyProcessor) Process(ctx streamz.Context[string, string], k string, v string) error {
	fmt.Println("LEEEEEEEEEEEL", k, v)
	ctx.Forward(k, v)
	return nil
}

type MyProcessor2 struct{}

func (p *MyProcessor2) Process(ctx streamz.Context[string, string], k string, v string) error {
	fmt.Println("second", k, v)
	return nil
}
