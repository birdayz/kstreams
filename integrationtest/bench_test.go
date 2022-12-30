package integrationtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func waitForGroup(acl *kadm.Client, group string) error {
	startTime := time.Now()
	for time.Since(startTime) < time.Second*10 {
		res, err := acl.DescribeGroups(context.Background(), group)
		if err != nil {
			return err
		}

		if len(res) > 0 {
			if res[group].State == "Stable" {
				return nil
			}
		}

	}

	return fmt.Errorf("Failed to wait for group %s", group)
}

const partitionsCount = 1

func BenchmarkConsume(b *testing.B) {
	broker := &RedpandaBroker{RedpandaVersion: "latest"}
	assert.NoError(b, broker.Init())
	defer broker.Close()

	//brokers := "localhost:9092"
	brokers := broker.BootstrapServers()[0]
	kcl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	assert.NoError(b, err)
	acl := kadm.NewClient(kcl)
	_, err = acl.CreateTopics(context.Background(), partitionsCount, 1, map[string]*string{}, "source")
	assert.NoError(b, err)
	_, err = acl.CreateTopics(context.Background(), partitionsCount, 1, map[string]*string{}, "sink")
	assert.NoError(b, err)

	topology := kstreams.NewTopologyBuilder()
	kstreams.RegisterSource(topology, "source", "source", serde.StringDeserializer, serde.StringDeserializer)
	kstreams.RegisterSink(topology, "sink", "sink", serde.StringSerializer, serde.StringSerializer, "source")
	kstr := kstreams.New(topology.Build(), "bench", kstreams.WithBrokers([]string{brokers}))

	go func() { assert.NoError(b, kstr.Run()) }()

	const size = 1000000

	ver, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumerGroup("bench-check"), kgo.ConsumeTopics("sink"))
	assert.NoError(b, err)

	assert.NoError(b, waitForGroup(acl, "bench"))
	assert.NoError(b, waitForGroup(acl, "bench-check"))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(size)
		for d := 0; d < size; d++ {
			kcl.Produce(context.TODO(), &kgo.Record{Topic: "source", Key: []byte("some-key"), Value: []byte("some-val")}, func(r *kgo.Record, err error) {
				assert.NoError(b, err)
				wg.Done()
			})
		}
		wg.Wait()
		b.StartTimer()

		var received = 0
		for received < size {
			fetch := ver.PollFetches(context.TODO())
			fetch.EachRecord(func(r *kgo.Record) {
				received++
			})
		}
	}
	err = kstr.Close()
	assert.NoError(b, err)

	ver.Close()
}
