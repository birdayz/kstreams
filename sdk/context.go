package sdk

import (
	"context"
	"time"
)

type Context[Kout any, Vout any] interface {
	context.Context

	// Topic is the source topic of the record.
	Topic() string

	// Partition is the source partition of the record.
	Partition() int32

	// Offset is the source topic-partition's offset of the record.
	Offset() int64

	// TBD: make headers read-only, and use interceptors in src/sink, eg in sink
	// to extract from ordinary ctx, and put headers?
	Headers() Headers

	// Forward forwards the record to downstream processors. By default, it is
	// forwarded to all downstream processors. Headers are forwarde by default,
	// but can be modified with the WithHeaders option.
	Forward(k Kout, v Vout, opts ...ForwardOption)
}

type forwardOptions struct {
	processors []string
	headers    Headers
}

type ForwardOption func(*forwardOptions)

func WithHeaders(headers Headers) ForwardOption {
	return func(fo *forwardOptions) {
		fo.headers = headers
	}
}

func ToProcessors(processors ...string) ForwardOption {
	return func(fo *forwardOptions) {
		fo.processors = processors
	}
}

type RecordMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   Headers
}

type Header struct {
	Key   string
	Value []byte
}

type Headers []Header

func (h Headers) Add(key string, value []byte) {
	h = append(h, Header{
		Key:   key,
		Value: value,
	})
}

func (h Headers) First(key string) (Header, bool) {
	for _, header := range h {
		if header.Key == key {
			return header, true
		}
	}
	return Header{}, false
}
