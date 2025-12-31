package integrationtest

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

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

// TransformProcessor transforms values by applying a function
type TransformProcessor struct {
	ctx       kstreams.ProcessorContext[string, string]
	transform func(string) string
}

func (p *TransformProcessor) Init(ctx kstreams.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *TransformProcessor) Close() error {
	return nil
}

func (p *TransformProcessor) Process(ctx context.Context, k string, v string) error {
	transformed := p.transform(v)
	p.ctx.Forward(ctx, k, transformed)
	return nil
}

// FilterProcessor filters records based on a predicate
type FilterProcessor struct {
	ctx       kstreams.ProcessorContext[string, string]
	predicate func(string, string) bool
}

func (p *FilterProcessor) Init(ctx kstreams.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *FilterProcessor) Close() error {
	return nil
}

func (p *FilterProcessor) Process(ctx context.Context, k string, v string) error {
	if p.predicate(k, v) {
		p.ctx.Forward(ctx, k, v)
	}
	// If predicate is false, record is filtered out (not forwarded)
	return nil
}

func TestWithSimpleProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

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
			defer broker.broker.Close()

			kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.broker.BootstrapServers()...))
			assert.NoError(t, err)
			defer kcl.Close()

			acl := kadm.NewClient(kcl)
			_, err = acl.CreateTopics(context.Background(), 10, 1, map[string]*string{}, "source")
			assert.NoError(t, err)

			topo := kstreams.NewTopologyBuilder()
			kstreams.RegisterSource(topo, "source", "source", serde.StringDeserializer, serde.StringDeserializer)

			out := make(chan [2]string, 1)
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

			// Give the app time to start
			time.Sleep(1 * time.Second)

			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{Topic: "source", Key: []byte("some-key"), Value: []byte("some-val")})
			assert.NoError(t, pr.FirstErr())

			select {
			case o := <-out:
				assert.Equal(t, [...]string{"some-key", "some-val"}, o)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message")
			}

			app.Close()
		})
	}
}

func TestTransformationProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("uppercase transformation", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "input")
		assert.NoError(t, err)

		topo := kstreams.NewTopologyBuilder()
		kstreams.RegisterSource(topo, "source", "input", serde.StringDeserializer, serde.StringDeserializer)

		// Add transformation processor (uppercase)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &TransformProcessor{
				transform: strings.ToUpper,
			}
		}, "transform", "source")

		// Add spy to capture output
		out := make(chan [2]string, 1)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "transform")

		app := kstreams.New(topo.MustBuild(), "test-transform", kstreams.WithBrokers(broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce message
		pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
			Topic: "input",
			Key:   []byte("key1"),
			Value: []byte("hello world"),
		})
		assert.NoError(t, pr.FirstErr())

		// Verify transformation
		select {
		case result := <-out:
			assert.Equal(t, "key1", result[0])
			assert.Equal(t, "HELLO WORLD", result[1])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for transformed message")
		}

		app.Close()
	})

	t.Run("value append transformation", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "input2")
		assert.NoError(t, err)

		topo := kstreams.NewTopologyBuilder()
		kstreams.RegisterSource(topo, "source", "input2", serde.StringDeserializer, serde.StringDeserializer)

		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &TransformProcessor{
				transform: func(v string) string { return v + "-processed" },
			}
		}, "transform", "source")

		out := make(chan [2]string, 1)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "transform")

		app := kstreams.New(topo.MustBuild(), "test-append", kstreams.WithBrokers(broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
			Topic: "input2",
			Key:   []byte("test"),
			Value: []byte("value"),
		})
		assert.NoError(t, pr.FirstErr())

		select {
		case result := <-out:
			assert.Equal(t, "test", result[0])
			assert.Equal(t, "value-processed", result[1])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		app.Close()
	})
}

func TestFilteringProcessor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("filter by value length", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "filter-input")
		assert.NoError(t, err)

		topo := kstreams.NewTopologyBuilder()
		kstreams.RegisterSource(topo, "source", "filter-input", serde.StringDeserializer, serde.StringDeserializer)

		// Filter: only pass values longer than 5 characters
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &FilterProcessor{
				predicate: func(k, v string) bool {
					return len(v) > 5
				},
			}
		}, "filter", "source")

		out := make(chan [2]string, 10)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "filter")

		app := kstreams.New(topo.MustBuild(), "test-filter", kstreams.WithBrokers(broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce multiple messages
		messages := []struct {
			key    string
			value  string
			passes bool
		}{
			{"k1", "short", false},       // len=5, should be filtered out
			{"k2", "longer value", true}, // len=12, should pass
			{"k3", "x", false},           // len=1, should be filtered out
			{"k4", "medium", true},       // len=6, should pass
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "filter-input",
				Key:   []byte(msg.key),
				Value: []byte(msg.value),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Collect results
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collecting:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 2 { // We expect exactly 2 messages to pass
					break collecting
				}
			case <-timeout:
				break collecting
			}
		}

		// Verify only long values passed through
		assert.Equal(t, 2, len(results), "expected 2 messages to pass filter")
		assert.Equal(t, "k2", results[0][0])
		assert.Equal(t, "longer value", results[0][1])
		assert.Equal(t, "k4", results[1][0])
		assert.Equal(t, "medium", results[1][1])

		app.Close()
	})

	t.Run("filter by key prefix", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "filter-key-input")
		assert.NoError(t, err)

		topo := kstreams.NewTopologyBuilder()
		kstreams.RegisterSource(topo, "source", "filter-key-input", serde.StringDeserializer, serde.StringDeserializer)

		// Filter: only pass keys starting with "allowed-"
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &FilterProcessor{
				predicate: func(k, v string) bool {
					return strings.HasPrefix(k, "allowed-")
				},
			}
		}, "filter", "source")

		out := make(chan [2]string, 10)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "filter")

		app := kstreams.New(topo.MustBuild(), "test-filter-key", kstreams.WithBrokers(broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce messages
		messages := []struct {
			key   string
			value string
		}{
			{"allowed-1", "value1"},
			{"denied-1", "value2"},
			{"allowed-2", "value3"},
			{"other", "value4"},
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "filter-key-input",
				Key:   []byte(msg.key),
				Value: []byte(msg.value),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Collect results
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collectingKeys:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 2 {
					break collectingKeys
				}
			case <-timeout:
				break collectingKeys
			}
		}

		assert.Equal(t, 2, len(results))
		assert.Equal(t, "allowed-1", results[0][0])
		assert.Equal(t, "allowed-2", results[1][0])

		app.Close()
	})
}

func TestMultiProcessorChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("transform then filter chain", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "chain-input")
		assert.NoError(t, err)

		topo := kstreams.NewTopologyBuilder()
		kstreams.RegisterSource(topo, "source", "chain-input", serde.StringDeserializer, serde.StringDeserializer)

		// Step 1: Transform to uppercase
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &TransformProcessor{
				transform: strings.ToUpper,
			}
		}, "transform", "source")

		// Step 2: Filter out values containing "SKIP"
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &FilterProcessor{
				predicate: func(k, v string) bool {
					return !strings.Contains(v, "SKIP")
				},
			}
		}, "filter", "transform")

		// Step 3: Spy to collect output
		out := make(chan [2]string, 10)
		kstreams.RegisterProcessor(topo, func() kstreams.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "filter")

		app := kstreams.New(topo.MustBuild(), "test-chain", kstreams.WithBrokers(broker.BootstrapServers()), kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Test data
		messages := []struct {
			key          string
			value        string
			shouldOutput bool
			expectedVal  string
		}{
			{"k1", "hello", true, "HELLO"},
			{"k2", "skip this", false, ""},        // Will be "SKIP THIS" after transform, then filtered
			{"k3", "world", true, "WORLD"},
			{"k4", "please skip", false, ""},      // Will be "PLEASE SKIP" after transform, then filtered
			{"k5", "final message", true, "FINAL MESSAGE"},
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "chain-input",
				Key:   []byte(msg.key),
				Value: []byte(msg.value),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Collect results
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collectingChain:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 3 { // We expect 3 messages to pass
					break collectingChain
				}
			case <-timeout:
				break collectingChain
			}
		}

		// Verify results
		assert.Equal(t, 3, len(results), "expected 3 messages through chain")
		assert.Equal(t, "k1", results[0][0])
		assert.Equal(t, "HELLO", results[0][1])
		assert.Equal(t, "k3", results[1][0])
		assert.Equal(t, "WORLD", results[1][1])
		assert.Equal(t, "k5", results[2][0])
		assert.Equal(t, "FINAL MESSAGE", results[2][1])

		app.Close()
	})
}
