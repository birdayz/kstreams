package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/birdayz/kstreams/kserde"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceResult holds the result of an async produce operation
type produceResult struct {
	record *kgo.Record
	err    error
}

// RuntimeSinkNode is the runtime instantiation of a sink node
// It maintains full type safety when serializing records
type RuntimeSinkNode[K, V any] struct {
	id              string // NodeID is just a string
	client          *kgo.Client
	topic           string
	keySerializer   kserde.Serializer[K]
	valueSerializer kserde.Serializer[V]

	futuresWg sync.WaitGroup
	futures   []produceResult
}

// Initial capacity for futures slice to reduce early reallocations
// This is a reasonable default; the slice will grow if needed
const defaultFuturesCapacity = 256

// NewRuntimeSinkNode creates a new runtime sink node
func NewRuntimeSinkNode[K, V any](
	id string,
	client *kgo.Client,
	topic string,
	keySerializer kserde.Serializer[K],
	valueSerializer kserde.Serializer[V],
) *RuntimeSinkNode[K, V] {
	return &RuntimeSinkNode[K, V]{
		id:              id,
		client:          client,
		topic:           topic,
		keySerializer:   keySerializer,
		valueSerializer: valueSerializer,
		futures:         make([]produceResult, 0, defaultFuturesCapacity),
	}
}

// Process serializes the key/value and produces to Kafka
func (n *RuntimeSinkNode[K, V]) Process(ctx context.Context, k K, v V) error {
	key, err := n.keySerializer(k)
	if err != nil {
		return fmt.Errorf("node %s: serialize key: %w", n.id, err)
	}

	value, err := n.valueSerializer(v)
	if err != nil {
		return fmt.Errorf("node %s: serialize value: %w", n.id, err)
	}

	n.futuresWg.Add(1)
	// Use Background context for async produce to avoid cancellation issues
	// when the processing context is canceled after Process() returns
	n.client.Produce(context.Background(), &kgo.Record{
		Key:   key,
		Value: value,
		Topic: n.topic,
	}, func(r *kgo.Record, err error) {
		pr := produceResult{
			record: r,
			err:    err,
		}
		n.futures = append(n.futures, pr)
		n.futuresWg.Done()
	})

	return nil
}

// Init initializes the sink node (no-op for sinks)
func (n *RuntimeSinkNode[K, V]) Init() error {
	return nil
}

// Close closes the sink node (no-op for sinks)
func (n *RuntimeSinkNode[K, V]) Close() error {
	return nil
}

// Flush waits for all pending produces and checks for errors
func (n *RuntimeSinkNode[K, V]) Flush(ctx context.Context) error {
	n.futuresWg.Wait()

	for _, result := range n.futures {
		if err := result.err; err != nil {
			return fmt.Errorf("node %s: produce failed: %w", n.id, result.err)
		}
	}

	// Keep allocated memory, just reset slice
	n.futures = n.futures[:0]

	return nil
}

// ProcessBatch implements batch processing for RuntimeSinkNode.
// Serializes and produces multiple records to Kafka in one operation.
func (n *RuntimeSinkNode[K, V]) ProcessBatch(ctx context.Context, keys []K, values []V) error {
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != len(values) {
		return fmt.Errorf("ProcessBatch: keys and values length mismatch (keys=%d, values=%d)", len(keys), len(values))
	}

	// Serialize all keys and values
	kgoRecords := make([]*kgo.Record, len(keys))

	for i := range keys {
		key, err := n.keySerializer(keys[i])
		if err != nil {
			return fmt.Errorf("node %s: serialize key: %w", n.id, err)
		}

		value, err := n.valueSerializer(values[i])
		if err != nil {
			return fmt.Errorf("node %s: serialize value: %w", n.id, err)
		}

		kgoRecords[i] = &kgo.Record{
			Topic: n.topic,
			Key:   key,
			Value: value,
		}
	}

	// Produce all records in one call
	// franz-go batches these internally for optimal performance
	results := n.client.ProduceSync(ctx, kgoRecords...)

	// Collect errors (if any)
	for _, res := range results {
		if res.Err != nil {
			// Store as future for Flush() to handle
			n.futures = append(n.futures, produceResult{err: res.Err})
		}
	}

	return nil
}

// Verify that RuntimeSinkNode implements Node, InputProcessor, BatchInputProcessor, and Flusher
var _ Node = (*RuntimeSinkNode[any, any])(nil)
var _ InputProcessor[any, any] = (*RuntimeSinkNode[any, any])(nil)
var _ BatchInputProcessor[any, any] = (*RuntimeSinkNode[any, any])(nil)
var _ Flusher = (*RuntimeSinkNode[any, any])(nil)
