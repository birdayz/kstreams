package kstreams

type ProcessorInterceptor[K any, V any, Kout any, Vout any] func(ctx ProcessorContext[Kout, Vout], k K, v V, processor Processor[K, V, Kout, Vout])

// How to capture Forwarded stuff here?
