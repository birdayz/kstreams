package integrationtest

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/birdayz/kstreams/kstate"
	"github.com/birdayz/kstreams/kstate/pebble"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Int serialization helpers for testing
func intSerializer(val int) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(val))
	return buf, nil
}

func intDeserializer(data []byte) (int, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid data length for int: %d", len(data))
	}
	return int(binary.BigEndian.Uint64(data)), nil
}

// CountingProcessor counts occurrences of each key using a state store
type CountingProcessor struct {
	ctx       kprocessor.ProcessorContext[string, string]
	storeName string
	store     kstate.KeyValueStore[string, int]
}

func (p *CountingProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	store := ctx.GetStore(p.storeName)
	if store == nil {
		return fmt.Errorf("failed to get store: store not found")
	}

	kvStore, ok := store.(kstate.KeyValueStore[string, int])
	if !ok {
		return fmt.Errorf("store is not a KeyValueStore[string, int]")
	}
	p.store = kvStore
	return nil
}

func (p *CountingProcessor) Close() error {
	return nil
}

func (p *CountingProcessor) Process(ctx context.Context, k string, v string) error {
	// Get current count for this key
	count, found, err := p.store.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("failed to get count: %w", err)
	}
	if !found {
		count = 0
	}

	// Increment count
	count++

	// Store updated count
	if err := p.store.Set(ctx, k, count); err != nil {
		return fmt.Errorf("failed to set count: %w", err)
	}

	// Forward the key with updated count
	p.ctx.Forward(ctx, k, strconv.Itoa(count))
	return nil
}

// SummingProcessor sums values for each key using a state store
type SummingProcessor struct {
	ctx       kprocessor.ProcessorContext[string, string]
	storeName string
	store     kstate.KeyValueStore[string, int]
}

func (p *SummingProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	store := ctx.GetStore(p.storeName)
	if store == nil {
		return fmt.Errorf("failed to get store: store not found")
	}

	kvStore, ok := store.(kstate.KeyValueStore[string, int])
	if !ok {
		return fmt.Errorf("store is not a KeyValueStore[string, int]")
	}
	p.store = kvStore
	return nil
}

func (p *SummingProcessor) Close() error {
	return nil
}

func (p *SummingProcessor) Process(ctx context.Context, k string, v string) error {
	// Parse incoming value as integer
	val, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("failed to parse value as int: %w", err)
	}

	// Get current sum for this key
	sum, found, err := p.store.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("failed to get sum: %w", err)
	}
	if !found {
		sum = 0
	}

	// Add to sum
	sum += val

	// Store updated sum
	if err := p.store.Set(ctx, k, sum); err != nil {
		return fmt.Errorf("failed to set sum: %w", err)
	}

	// Forward the key with updated sum
	p.ctx.Forward(ctx, k, strconv.Itoa(sum))
	return nil
}

func TestStatefulAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("count by key", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "count-input")
		assert.NoError(t, err)

		// Create temporary state directory
		stateDir := t.TempDir()

		// Build topology with state store
		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "count-input", kserde.StringDeserializer, kserde.StringDeserializer)

		// Register state store for counting
		kdag.RegisterStore(topo,
			pebble.NewKeyValueStoreBuilder[string, int]("counts", stateDir).
				WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
			"counts")

		// Register counting processor
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingProcessor{storeName: "counts"}
		}, "counter", "source", "counts")

		// Add spy to capture output
		out := make(chan [2]string, 100)
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "counter")

		app := kstreams.MustNew(topo.MustBuild(), "test-count",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce test messages
		messages := []struct {
			key   string
			value string
		}{
			{"key1", "msg1"}, // count=1
			{"key2", "msg2"}, // count=1
			{"key1", "msg3"}, // count=2
			{"key1", "msg4"}, // count=3
			{"key2", "msg5"}, // count=2
			{"key3", "msg6"}, // count=1
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "count-input",
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
				if len(results) == 6 {
					break collecting
				}
			case <-timeout:
				break collecting
			}
		}

		// Verify counts
		assert.Equal(t, 6, len(results), "expected 6 count updates")

		// Verify specific counts
		expectedCounts := map[string][]string{
			"key1": {"1", "2", "3"},
			"key2": {"1", "2"},
			"key3": {"1"},
		}

		actualCounts := make(map[string][]string)
		for _, result := range results {
			actualCounts[result[0]] = append(actualCounts[result[0]], result[1])
		}

		assert.Equal(t, expectedCounts, actualCounts)

		app.Close()
	})

	t.Run("sum by key", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "sum-input")
		assert.NoError(t, err)

		stateDir := t.TempDir()

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "sum-input", kserde.StringDeserializer, kserde.StringDeserializer)

		// Register state store for summing
		kdag.RegisterStore(topo,
			pebble.NewKeyValueStoreBuilder[string, int]("sums", stateDir).
				WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
			"sums")

		// Register summing processor
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SummingProcessor{storeName: "sums"}
		}, "summer", "source", "sums")

		out := make(chan [2]string, 100)
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "summer")

		app := kstreams.MustNew(topo.MustBuild(), "test-sum",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce test messages with numeric values
		messages := []struct {
			key   string
			value string
		}{
			{"key1", "10"}, // sum=10
			{"key2", "5"},  // sum=5
			{"key1", "20"}, // sum=30
			{"key1", "5"},  // sum=35
			{"key2", "15"}, // sum=20
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "sum-input",
				Key:   []byte(msg.key),
				Value: []byte(msg.value),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Collect results
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collectingSums:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 5 {
					break collectingSums
				}
			case <-timeout:
				break collectingSums
			}
		}

		// Verify sums
		assert.Equal(t, 5, len(results), "expected 5 sum updates")

		expectedSums := map[string][]string{
			"key1": {"10", "30", "35"},
			"key2": {"5", "20"},
		}

		actualSums := make(map[string][]string)
		for _, result := range results {
			actualSums[result[0]] = append(actualSums[result[0]], result[1])
		}

		assert.Equal(t, expectedSums, actualSums)

		app.Close()
	})
}

func TestStatePersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("state persists across restarts", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "persist-input")
		assert.NoError(t, err)

		// Create persistent state directory
		stateDir := filepath.Join(t.TempDir(), "state")
		err = os.MkdirAll(stateDir, 0755)
		assert.NoError(t, err)

		// First run: process some messages
		func() {
			topo := kdag.NewBuilder()
			kstreams.RegisterSource(topo, "source", "persist-input", kserde.StringDeserializer, kserde.StringDeserializer)

			kdag.RegisterStore(topo,
				pebble.NewKeyValueStoreBuilder[string, int]("counts", stateDir).
					WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
				"counts")

			kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
				return &CountingProcessor{storeName: "counts"}
			}, "counter", "source", "counts")

			out := make(chan [2]string, 100)
			kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
				return &SpyProcessor{out: out}
			}, "spy", "counter")

			app := kstreams.MustNew(topo.MustBuild(), "test-persist",
				kstreams.WithBrokers(broker.BootstrapServers()),
				kstreams.WithLog(slog.Default()))
			go func() {
				_ = app.Run()
			}()
			time.Sleep(1 * time.Second)

			// Produce initial messages
			messages := []struct {
				key   string
				value string
			}{
				{"key1", "msg1"}, // count=1
				{"key1", "msg2"}, // count=2
				{"key2", "msg3"}, // count=1
			}

			for _, msg := range messages {
				pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
					Topic: "persist-input",
					Key:   []byte(msg.key),
					Value: []byte(msg.value),
				})
				assert.NoError(t, pr.FirstErr())
			}

			// Wait for processing
			var results [][2]string
			timeout := time.After(5 * time.Second)
		collecting1:
			for {
				select {
				case result := <-out:
					results = append(results, result)
					if len(results) == 3 {
						break collecting1
					}
				case <-timeout:
					break collecting1
				}
			}

			assert.Equal(t, 3, len(results))

			// Close app to flush state
			app.Close()
			time.Sleep(500 * time.Millisecond)
		}()

		// Second run: verify state is restored and counting continues
		func() {
			topo := kdag.NewBuilder()
			kstreams.RegisterSource(topo, "source", "persist-input", kserde.StringDeserializer, kserde.StringDeserializer)

			kdag.RegisterStore(topo,
				pebble.NewKeyValueStoreBuilder[string, int]("counts", stateDir).
					WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
				"counts")

			kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
				return &CountingProcessor{storeName: "counts"}
			}, "counter", "source", "counts")

			out := make(chan [2]string, 100)
			kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
				return &SpyProcessor{out: out}
			}, "spy", "counter")

			app := kstreams.MustNew(topo.MustBuild(), "test-persist",
				kstreams.WithBrokers(broker.BootstrapServers()),
				kstreams.WithLog(slog.Default()))
			go func() {
				_ = app.Run()
			}()
			time.Sleep(2 * time.Second)

			// Produce more messages - counts should continue from previous state
			messages := []struct {
				key           string
				value         string
				expectedCount string
			}{
				{"key1", "msg4", "3"}, // Should be 3, not 1
				{"key2", "msg5", "2"}, // Should be 2, not 1
				{"key3", "msg6", "1"}, // New key, should be 1
			}

			for _, msg := range messages {
				pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
					Topic: "persist-input",
					Key:   []byte(msg.key),
					Value: []byte(msg.value),
				})
				assert.NoError(t, pr.FirstErr())
			}

			// Collect results from second run
			var results [][2]string
			timeout := time.After(5 * time.Second)
		collecting2:
			for {
				select {
				case result := <-out:
					results = append(results, result)
					if len(results) == 3 {
						break collecting2
					}
				case <-timeout:
					break collecting2
				}
			}

			// Verify counts continued from previous state
			assert.Equal(t, 3, len(results), "expected 3 results from second run")

			resultMap := make(map[string]string)
			for _, result := range results {
				resultMap[result[0]] = result[1]
			}

			// key1 should be at count 3 (not 1), proving state was restored
			assert.Equal(t, "3", resultMap["key1"], "key1 count should continue from previous run")
			// key2 should be at count 2 (not 1)
			assert.Equal(t, "2", resultMap["key2"], "key2 count should continue from previous run")
			// key3 is new, should be 1
			assert.Equal(t, "1", resultMap["key3"], "key3 is new, should start at 1")

			app.Close()
		}()
	})
}

func TestStoreFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("flush writes state to disk", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "flush-input")
		assert.NoError(t, err)

		stateDir := filepath.Join(t.TempDir(), "flush-state")
		err = os.MkdirAll(stateDir, 0755)
		assert.NoError(t, err)

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "flush-input", kserde.StringDeserializer, kserde.StringDeserializer)

		kdag.RegisterStore(topo,
			pebble.NewKeyValueStoreBuilder[string, int]("counts", stateDir).
				WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
			"counts")

		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingProcessor{storeName: "counts"}
		}, "counter", "source", "counts")

		out := make(chan [2]string, 100)
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "counter")

		app := kstreams.MustNew(topo.MustBuild(), "test-flush",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce messages
		for i := 0; i < 5; i++ {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "flush-input",
				Key:   []byte("test-key"),
				Value: []byte(fmt.Sprintf("msg-%d", i)),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Wait for processing
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collectingFlush:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 5 {
					break collectingFlush
				}
			case <-timeout:
				break collectingFlush
			}
		}

		assert.Equal(t, 5, len(results))

		// Close app to trigger flush
		app.Close()
		time.Sleep(500 * time.Millisecond)

		// Verify state directory was created and contains data
		_, err = os.Stat(stateDir)
		assert.NoError(t, err, "state directory should exist")

		// Check that store directory exists
		storePath := filepath.Join(stateDir, "counts")
		_, err = os.Stat(storePath)
		assert.NoError(t, err, "store directory should exist")
	})
}

func TestMultipleStores(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	t.Run("processor with multiple state stores", func(t *testing.T) {
		broker := &RedpandaBroker{RedpandaVersion: "latest"}
		assert.NoError(t, broker.Init())
		defer broker.Close()

		kcl, err := kgo.NewClient(kgo.SeedBrokers(broker.BootstrapServers()...))
		assert.NoError(t, err)
		defer kcl.Close()

		acl := kadm.NewClient(kcl)
		_, err = acl.CreateTopics(context.Background(), 1, 1, map[string]*string{}, "multi-store-input")
		assert.NoError(t, err)

		stateDir := t.TempDir()

		topo := kdag.NewBuilder()
		kstreams.RegisterSource(topo, "source", "multi-store-input", kserde.StringDeserializer, kserde.StringDeserializer)

		// Register two separate stores
		kdag.RegisterStore(topo,
			pebble.NewKeyValueStoreBuilder[string, int]("store1", stateDir).
				WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
			"store1")

		kdag.RegisterStore(topo,
			pebble.NewKeyValueStoreBuilder[string, int]("store2", stateDir).
				WithSerdes(kserde.StringSerializer, kserde.StringDeserializer, intSerializer, intDeserializer),
			"store2")

		// Use first store
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &CountingProcessor{storeName: "store1"}
		}, "counter1", "source", "store1")

		// Use second store
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SummingProcessor{storeName: "store2"}
		}, "summer", "counter1", "store2")

		out := make(chan [2]string, 100)
		kstreams.RegisterProcessor(topo, func() kprocessor.Processor[string, string, string, string] {
			return &SpyProcessor{out: out}
		}, "spy", "summer")

		app := kstreams.MustNew(topo.MustBuild(), "test-multi-store",
			kstreams.WithBrokers(broker.BootstrapServers()),
			kstreams.WithLog(slog.Default()))
		go func() {
			_ = app.Run()
		}()
		time.Sleep(1 * time.Second)

		// Produce messages
		messages := []struct {
			key   string
			value string
		}{
			{"key1", "val1"}, // counter1: count=1, summer: sum=1
			{"key1", "val2"}, // counter1: count=2, summer: sum=3
			{"key1", "val3"}, // counter1: count=3, summer: sum=6
		}

		for _, msg := range messages {
			pr := kcl.ProduceSync(context.TODO(), &kgo.Record{
				Topic: "multi-store-input",
				Key:   []byte(msg.key),
				Value: []byte(msg.value),
			})
			assert.NoError(t, pr.FirstErr())
		}

		// Collect results
		var results [][2]string
		timeout := time.After(5 * time.Second)
	collectingMulti:
		for {
			select {
			case result := <-out:
				results = append(results, result)
				if len(results) == 3 {
					break collectingMulti
				}
			case <-timeout:
				break collectingMulti
			}
		}

		// Verify both stores worked correctly
		// counter1 produces: 1, 2, 3
		// summer accumulates those: 1, 3 (1+2), 6 (3+3)
		assert.Equal(t, 3, len(results))
		assert.Equal(t, "key1", results[0][0])
		assert.Equal(t, "1", results[0][1])
		assert.Equal(t, "key1", results[1][0])
		assert.Equal(t, "3", results[1][1])
		assert.Equal(t, "key1", results[2][0])
		assert.Equal(t, "6", results[2][1])

		app.Close()
	})
}
