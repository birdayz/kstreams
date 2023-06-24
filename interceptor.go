package kstreams

import "context"

type ProcessorInterceptor[K any, V any, Kout any, Vout any] func(ctx context.Context, k K, v V, processor Processor[K, V, Kout, Vout])

// How to capture Forwarded stuff here?
func init() {

	f := func(ctx context.Context, k string, v string, proc Processor[string, string, string, string]) {
	}
}
