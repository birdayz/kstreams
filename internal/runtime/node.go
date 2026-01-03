package runtime

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Node does not know about any specific types of nodes, because it would
// otherwise need to have an ounbounded number of generic types. Generic types
// are hidden inside the actual implementations using the Node interfaces.
type Node interface {
	Init() error
	Close() error
}

// InputProcessor is a partial interface covering only the generic input K/V,
// without requiring the caller to know the generic types of the output.
type InputProcessor[K any, V any] interface {
	Process(context.Context, K, V) error
}

// BatchInputProcessor is the batch version of InputProcessor.
type BatchInputProcessor[K, V any] interface {
	InputProcessor[K, V]
	ProcessBatch(ctx context.Context, keys []K, values []V) error
}

// RawRecordProcessor processes raw kgo.Record objects.
// This is used internally for source nodes.
type RawRecordProcessor interface {
	Process(ctx context.Context, m *kgo.Record) error
}

// BatchRawRecordProcessor processes batches of raw Kafka records.
// This is the batch interface for source nodes.
type BatchRawRecordProcessor interface {
	RawRecordProcessor
	ProcessBatch(ctx context.Context, records []*kgo.Record) error
}

// ChangelogCollector buffers and batches changelog records.
// This interface abstracts the RecordCollector implementation to avoid import cycles.
// Matches Kafka Streams' RecordCollector pattern.
type ChangelogCollector interface {
	// Send buffers a changelog record for later batch production.
	// Returns immediately without blocking on Kafka.
	Send(storeName, topic string, partition int32, key, value []byte, timestamp time.Time)
}

// Flusher can flush pending data
type Flusher interface {
	Flush(ctx context.Context) error
}
