package integrationtest

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/docker/go-connections/nat"
	"github.com/go-logr/stdr"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
	port, err := GetFreePort()
	if err != nil {
		return err
	}
	req := testcontainers.ContainerRequest{
		Image:      fmt.Sprintf("docker.vectorized.io/vectorized/redpanda:%s", b.RedpandaVersion),
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
		User:       "root:root",
		Cmd: []string{
			"redpanda",
			"start",
			"--smp", "1",
			"--reserve-memory", "0M",
			"--overprovisioned",
			"--node-id", "0",
			"--kafka-addr", fmt.Sprintf("OUTSIDE://0.0.0.0:%d", port),
		},
	}

	req.ExposedPorts = []string{
		// Fixed port mapping for kafka
		fmt.Sprintf("%d:%d/tcp", port, port),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return err
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d", port)))
	if err != nil {
		return err
	}

	b.bootstrapServers = []string{fmt.Sprintf("%s:%d", hostIP, mappedPort.Int())}
	b.testcontainer = container

	return nil
}

func (b *RedpandaBroker) Close() error {
	return b.testcontainer.Terminate(context.Background())
}

func (b *RedpandaBroker) BootstrapServers() []string {
	return b.bootstrapServers
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
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

			app := kstreams.New(topo.Build(), "test", kstreams.WithBrokers(broker.broker.BootstrapServers()), kstreams.WithLogr(stdr.New(nil)))
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
