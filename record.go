package kstreams

import (
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record represents a key-value record with full metadata
// This replaces the unused Record type at processor.go:24-28
type Record[K, V any] struct {
	Key      K
	Value    V
	Metadata RecordMetadata
}

// RecordMetadata contains all metadata for a Kafka record
type RecordMetadata struct {
	Topic       string
	Partition   int32
	Offset      int64
	Timestamp   time.Time
	Headers     *Headers
	LeaderEpoch int32
}

// Headers provides thread-safe access to record headers
type Headers struct {
	mu      sync.RWMutex
	headers []RecordHeader
}

// RecordHeader represents a single header key-value pair
type RecordHeader struct {
	Key   string
	Value []byte
}

// NewHeaders creates a new Headers instance
func NewHeaders() *Headers {
	return &Headers{
		headers: make([]RecordHeader, 0),
	}
}

// Get retrieves the first header value for the given key
// Returns (value, true) if found, (nil, false) if not found
func (h *Headers) Get(key string) ([]byte, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, header := range h.headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}

// GetAll retrieves all header values for the given key
func (h *Headers) GetAll(key string) [][]byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var values [][]byte
	for _, header := range h.headers {
		if header.Key == key {
			values = append(values, header.Value)
		}
	}
	return values
}

// Set sets a header value, replacing any existing values for the key
func (h *Headers) Set(key string, value []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Remove existing headers with this key
	filtered := make([]RecordHeader, 0, len(h.headers))
	for _, header := range h.headers {
		if header.Key != key {
			filtered = append(filtered, header)
		}
	}

	// Add new header
	h.headers = append(filtered, RecordHeader{Key: key, Value: value})
}

// Add appends a header value without removing existing values for the key
func (h *Headers) Add(key string, value []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.headers = append(h.headers, RecordHeader{Key: key, Value: value})
}

// Remove removes all headers with the given key
func (h *Headers) Remove(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	filtered := make([]RecordHeader, 0, len(h.headers))
	for _, header := range h.headers {
		if header.Key != key {
			filtered = append(filtered, header)
		}
	}
	h.headers = filtered
}

// All returns a copy of all headers
func (h *Headers) All() []RecordHeader {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]RecordHeader, len(h.headers))
	copy(result, h.headers)
	return result
}

// Len returns the number of headers
func (h *Headers) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.headers)
}

// convertHeaders converts kgo.RecordHeaders to our Headers type
func convertHeaders(kgoHeaders []kgo.RecordHeader) *Headers {
	headers := NewHeaders()
	for _, h := range kgoHeaders {
		headers.Add(h.Key, h.Value)
	}
	return headers
}

// newRecordFromKgo creates a Record from a kgo.Record
func newRecordFromKgo[K, V any](key K, value V, kgoRecord *kgo.Record) Record[K, V] {
	return Record[K, V]{
		Key:   key,
		Value: value,
		Metadata: RecordMetadata{
			Topic:       kgoRecord.Topic,
			Partition:   kgoRecord.Partition,
			Offset:      kgoRecord.Offset,
			Timestamp:   kgoRecord.Timestamp,
			Headers:     convertHeaders(kgoRecord.Headers),
			LeaderEpoch: kgoRecord.LeaderEpoch,
		},
	}
}
