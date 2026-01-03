package kprocessor

import (
	"time"
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

// Headers provides access to record headers.
//
// Thread Safety: Headers is NOT thread-safe by design.
// This is safe because each record is processed by exactly one goroutine
// (single-threaded per partition model). No mutex overhead in the hot path.
type Headers struct {
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
	for _, header := range h.headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}

// GetString retrieves the first header value as a string.
// Returns (value, true) if found, ("", false) if not found.
func (h *Headers) GetString(key string) (string, bool) {
	val, ok := h.Get(key)
	if !ok {
		return "", false
	}
	return string(val), true
}

// GetAll retrieves all header values for the given key
func (h *Headers) GetAll(key string) [][]byte {
	var values [][]byte
	for _, header := range h.headers {
		if header.Key == key {
			values = append(values, header.Value)
		}
	}
	return values
}

// GetAllStrings retrieves all header values for the given key as strings
func (h *Headers) GetAllStrings(key string) []string {
	vals := h.GetAll(key)
	result := make([]string, len(vals))
	for i, v := range vals {
		result[i] = string(v)
	}
	return result
}

// Set sets a header value, replacing any existing values for the key
func (h *Headers) Set(key string, value []byte) {
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

// SetString sets a header with a string value, replacing any existing values for the key
func (h *Headers) SetString(key, value string) {
	h.Set(key, []byte(value))
}

// Add appends a header value without removing existing values for the key
func (h *Headers) Add(key string, value []byte) {
	h.headers = append(h.headers, RecordHeader{Key: key, Value: value})
}

// AddString appends a string header value without removing existing values for the key
func (h *Headers) AddString(key, value string) {
	h.Add(key, []byte(value))
}

// Remove removes all headers with the given key
func (h *Headers) Remove(key string) {
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
	result := make([]RecordHeader, len(h.headers))
	copy(result, h.headers)
	return result
}

// Len returns the number of headers
func (h *Headers) Len() int {
	return len(h.headers)
}
