package integrationtest

import (
	"context"
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Broker interface {
	Init() error
	Close() error
	BootstrapServers() []string
}

type RedpandaBroker struct {
	RedpandaVersion  string
	bootstrapServers []string
	testcontainer    testcontainers.Container
}

func (b *RedpandaBroker) Init() error {
	ctx := context.Background()

	redpandaContainer, err := redpanda.RunContainer(ctx)
	if err != nil {
		panic(err)
	}

	b.testcontainer = redpandaContainer
	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	if err != nil {
		return err
	}

	b.bootstrapServers = []string{bootstrapServer}

	return nil
}

func (b *RedpandaBroker) Close() error {
	return b.testcontainer.Terminate(context.Background())
}

func (b *RedpandaBroker) BootstrapServers() []string {
	return b.bootstrapServers
}

func TestWithSimpleProcessor(t *testing.T) {
	var brokers = []struct {
		name   string
		broker Broker
	}{
		{
			name:   "redpanda",
			broker: &RedpandaBroker{RedpandaVersion: "latest"},
		},
	}

	for _, broker := range brokers {
		t.Run(broker.name, func(t *testing.T) {
			assert.NoError(t, broker.broker.Init())

			kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.broker.BootstrapServers()...))
			assert.NoError(t, err)
			acl := kadm.NewClient(kcl)
			_, err = acl.CreateTopics(context.Background(), 10, 1, map[string]*string{}, "source")
			assert.NoError(t, err)

			topo := kstreams.NewTopologyBuilder()
			kstreams.RegisterSource(topo, "source", "source", serde.StringDeserializer, serde.StringDeserializer)

			out := make(chan [2]string)
			kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
				return &SpyProcessor{
					out: out,
				}
			}, "my-processor", "source")

			app := kstreams.New(topo.MustBuild(), "test", kstreams.WithBrokers(broker.broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
			go func() {
				err := app.Run()
				assert.NoError(t, err)
			}()

			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{Topic: "source", Key: []byte("some-key"), Value: []byte("some-val")})
			assert.NoError(t, pr.FirstErr())

			o := <-out
			assert.Equal(t, [...]string{"some-key", "some-val"}, o)
		})
	}
}
