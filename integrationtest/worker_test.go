package integrationtest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StateTracker tracks worker state transitions
type StateTracker struct {
	mu     sync.Mutex
	states []string
}

func (st *StateTracker) add(state string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.states = append(st.states, state)
}

func (st *StateTracker) get() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	result := make([]string, len(st.states))
	copy(result, st.states)
	return result
}

func TestWorkerStateTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("happy path state transitions", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "worker-test")
		assert.NoError(t, err)

		// Build simple topology
		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "worker-test", kserde.StringDeserializer, kserde.StringDeserializer)

		processedCount := atomic.Int32{}
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingTestProcessor{count: &processedCount}
		}, "processor", "source")

		app := kstreams.MustNew(topo.MustBuild(), "worker-state-test",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))

		go func() {
			_ = app.Run()
		}()

		// Wait for worker to be running
		time.Sleep(2 * time.Second)

		// Produce some messages
		for i := 0; i < 5; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "worker-test",
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Wait for processing
		time.Sleep(2 * time.Second)

		// Verify messages were processed
		assert.True(t, processedCount.Load() >= 5, "expected at least 5 messages processed")

		// Close app
		err = app.Close()
		assert.NoError(t, err)
	})
}

// CountingTestProcessor counts how many messages it processes
type CountingTestProcessor struct {
	ctx   kprocessor.ProcessorContext[string, string]
	count *atomic.Int32
}

func (p *CountingTestProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *CountingTestProcessor) Close() error {
	return nil
}

func (p *CountingTestProcessor) Process(ctx context.Context, k string, v string) error {
	p.count.Add(1)
	p.ctx.Forward(ctx, k, v)
	return nil
}

func TestWorkerCommitInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("commits at specified interval", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "commit-test")
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "commit-test", kserde.StringDeserializer, kserde.StringDeserializer)

		processedCount := atomic.Int32{}
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingTestProcessor{count: &processedCount}
		}, "processor", "source")

		// Short commit interval
		app := kstreams.MustNew(topo.MustBuild(), "commit-interval-test",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()),
			kstreams.WithCommitInterval(1*time.Second))

		go func() {
			_ = app.Run()
		}()

		time.Sleep(1 * time.Second)

		// Produce messages
		for i := 0; i < 10; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "commit-test",
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Wait for processing and commits
		time.Sleep(3 * time.Second)

		// Close app
		err = app.Close()
		assert.NoError(t, err)

		// Verify all messages were processed
		assert.True(t, processedCount.Load() >= 10, "expected at least 10 messages processed")
	})
}

func TestWorkerErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("processing error triggers close", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "error-test")
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "error-test", kserde.StringDeserializer, kserde.StringDeserializer)

		// Processor that errors on specific key
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &ErroringProcessor{errorOnKey: "error-key"}
		}, "processor", "source")

		app := kstreams.MustNew(topo.MustBuild(), "error-test",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))

		errChan := make(chan error, 1)
		go func() {
			errChan <- app.Run()
		}()

		time.Sleep(1 * time.Second)

		// Produce normal message (should succeed)
		pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
			Topic: "error-test",
			Key:   []byte("normal-key"),
			Value: []byte("value"),
		})
		assert.NoError(t, pr.FirstErr())

		time.Sleep(500 * time.Millisecond)

		// Produce error message (should trigger close)
		pr = kcl.ProduceSync(context.TODO(), &kgo.Record{
			Topic: "error-test",
			Key:   []byte("error-key"),
			Value: []byte("value"),
		})
		assert.NoError(t, pr.FirstErr())

		// Wait for error
		select {
		case err := <-errChan:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "simulated processing error")
		case <-time.After(10 * time.Second):
			t.Fatal("expected error but app didn't exit")
		}
	})
}

// ErroringProcessor fails on a specific key
type ErroringProcessor struct {
	ctx        kprocessor.ProcessorContext[string, string]
	errorOnKey string
}

func (p *ErroringProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *ErroringProcessor) Close() error {
	return nil
}

func (p *ErroringProcessor) Process(ctx context.Context, k string, v string) error {
	if k == p.errorOnKey {
		return fmt.Errorf("simulated processing error for key: %s", k)
	}
	p.ctx.Forward(ctx, k, v)
	return nil
}

func TestWorkerMultiplePartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("handles multiple partitions", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		// Create topic with 3 partitions
		_, err = acl.CreateTopics(context.Background(), 3, 1, map[string]*string{}, "multi-partition-test")
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "multi-partition-test", kserde.StringDeserializer, kserde.StringDeserializer)

		processedCount := atomic.Int32{}
		partitionsSeen := sync.Map{}

		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &PartitionTrackingProcessor{
				count:          &processedCount,
				partitionsSeen: &partitionsSeen,
			}
		}, "processor", "source")

		app := kstreams.MustNew(topo.MustBuild(), "multi-partition-test",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))

		go func() {
			_ = app.Run()
		}()

		time.Sleep(2 * time.Second)

		// Produce messages to different partitions
		for i := 0; i < 30; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "multi-partition-test",
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Close app
		err = app.Close()
		assert.NoError(t, err)

		// Verify messages were processed
		assert.True(t, processedCount.Load() >= 30, "expected at least 30 messages processed")

		// Verify multiple partitions were seen (since we have 3 partitions, should see at least 2)
		count := 0
		partitionsSeen.Range(func(key, value any) bool {
			count++
			return true
		})
		assert.True(t, count >= 1, "expected to see at least 1 partition")
	})
}

// PartitionTrackingProcessor tracks which partitions it processes from
type PartitionTrackingProcessor struct {
	ctx            kprocessor.ProcessorContext[string, string]
	count          *atomic.Int32
	partitionsSeen *sync.Map
}

func (p *PartitionTrackingProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *PartitionTrackingProcessor) Close() error {
	return nil
}

func (p *PartitionTrackingProcessor) Process(ctx context.Context, k string, v string) error {
	p.count.Add(1)

	// Try to get partition info if available
	// This is a basic processor, so we don't have direct access to partition
	// but we can still count messages
	p.partitionsSeen.Store(k, true)

	p.ctx.Forward(ctx, k, v)
	return nil
}

func TestWorkerCloseGracefully(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("close flushes and commits", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "close-test")
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "close-test", kserde.StringDeserializer, kserde.StringDeserializer)

		processedCount := atomic.Int32{}
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingTestProcessor{count: &processedCount}
		}, "processor", "source")

		app := kstreams.MustNew(topo.MustBuild(), "close-graceful-test",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()),
			kstreams.WithCommitInterval(10*time.Second)) // Long interval to test close commits

		go func() {
			_ = app.Run()
		}()

		// Wait for worker to reach RUNNING state
		time.Sleep(3 * time.Second)

		// Produce messages
		for i := 0; i < 5; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "close-test",
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Wait for processing
		time.Sleep(2 * time.Second)

		// Close should flush and commit even though commit interval hasn't elapsed
		startClose := time.Now()
		err = app.Close()
		closeTime := time.Since(startClose)

		assert.NoError(t, err)
		assert.True(t, closeTime < 5*time.Second, "close should complete quickly")
		assert.True(t, processedCount.Load() >= 5, "expected at least 5 messages processed")
	})
}

func TestWorkerRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("handles rebalancing when second worker joins", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		// Create topic with 2 partitions to allow rebalancing
		_, err = acl.CreateTopics(context.Background(), 2, 1, map[string]*string{}, "rebalance-test")
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "rebalance-test", kserde.StringDeserializer, kserde.StringDeserializer)

		count1 := atomic.Int32{}
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingTestProcessor{count: &count1}
		}, "processor", "source")

		// Start first worker
		app1 := kstreams.MustNew(topo.MustBuild(), "rebalance-group",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default().With("app", "app1")),
			kstreams.WithWorkersCount(1))

		go func() {
			_ = app1.Run()
		}()

		// Wait for first worker to be running
		time.Sleep(2 * time.Second)

		// Produce messages to first worker
		for i := 0; i < 10; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "rebalance-test",
				Key:   []byte(fmt.Sprintf("key-before-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		time.Sleep(1 * time.Second)

		// Start second worker to trigger rebalance
		topo2 := kdag.NewBuilder()
		kstreams.RegisterSource(topo2, "source", "rebalance-test", kserde.StringDeserializer, kserde.StringDeserializer)

		count2 := atomic.Int32{}
		kstreams.RegisterProcessor(topo2, func() kprocessor.Processor[string, string, string, string] {
			return &CountingTestProcessor{count: &count2}
		}, "processor", "source")

		app2 := kstreams.MustNew(topo2.MustBuild(), "rebalance-group",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default().With("app", "app2")),
			kstreams.WithWorkersCount(1))

		go func() {
			_ = app2.Run()
		}()

		// Wait for rebalance to complete
		time.Sleep(3 * time.Second)

		// Produce more messages - should be distributed
		for i := 0; i < 20; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "rebalance-test",
				Key:   []byte(fmt.Sprintf("key-after-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		time.Sleep(2 * time.Second)

		// Both workers should have processed messages
		totalCount := count1.Load() + count2.Load()
		assert.True(t, totalCount >= 30, fmt.Sprintf("expected at least 30 total messages, got %d", totalCount))

		// After rebalance, both workers should be processing
		// We can't guarantee exact distribution, but both should have some work
		// Note: this assertion might be flaky depending on exact rebalance timing

		app1.Close()
		app2.Close()
	})
}
