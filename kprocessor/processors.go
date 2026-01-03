package kprocessor

import (
	"context"
)

// Filter creates a processor that only forwards records matching the predicate.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Filter(func(k string, v Order) bool {
//	        return v.Total > 100
//	    }),
//	    "high-value-orders",
//	    "source",
//	)
func Filter[K, V any](predicate func(k K, v V) bool) ProcessorBuilder[K, V, K, V] {
	return NewFunc(func(ctx ProcessorContext[K, V], c context.Context, k K, v V) error {
		if predicate(k, v) {
			ctx.Forward(c, k, v)
		}
		return nil
	})
}

// FilterNot creates a processor that forwards records NOT matching the predicate.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.FilterNot(func(k string, v Order) bool {
//	        return v.IsCancelled
//	    }),
//	    "active-orders",
//	    "source",
//	)
func FilterNot[K, V any](predicate func(k K, v V) bool) ProcessorBuilder[K, V, K, V] {
	return Filter(func(k K, v V) bool {
		return !predicate(k, v)
	})
}

// FilterByKey creates a processor that filters based on the key only.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.FilterByKey(func(k string) bool {
//	        return strings.HasPrefix(k, "VIP-")
//	    }),
//	    "vip-orders",
//	    "source",
//	)
func FilterByKey[K, V any](predicate func(k K) bool) ProcessorBuilder[K, V, K, V] {
	return Filter(func(k K, v V) bool {
		return predicate(k)
	})
}

// FilterByValue creates a processor that filters based on the value only.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.FilterByValue(func(v Order) bool {
//	        return v.Status == "completed"
//	    }),
//	    "completed-orders",
//	    "source",
//	)
func FilterByValue[K, V any](predicate func(v V) bool) ProcessorBuilder[K, V, K, V] {
	return Filter(func(k K, v V) bool {
		return predicate(v)
	})
}

// Map creates a processor that transforms values using the provided function.
// The key is passed through unchanged.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Map(func(k string, v string) int {
//	        return len(v)
//	    }),
//	    "string-length",
//	    "source",
//	)
func Map[Kin, Vin, Vout any](mapFunc func(k Kin, v Vin) Vout) ProcessorBuilder[Kin, Vin, Kin, Vout] {
	return NewFunc(func(ctx ProcessorContext[Kin, Vout], c context.Context, k Kin, v Vin) error {
		ctx.Forward(c, k, mapFunc(k, v))
		return nil
	})
}

// MapKey creates a processor that transforms keys using the provided function.
// The value is passed through unchanged.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.MapKey(func(k string) string {
//	        return strings.ToUpper(k)
//	    }),
//	    "uppercase-key",
//	    "source",
//	)
func MapKey[Kin, Kout, V any](mapFunc func(k Kin) Kout) ProcessorBuilder[Kin, V, Kout, V] {
	return NewFunc(func(ctx ProcessorContext[Kout, V], c context.Context, k Kin, v V) error {
		ctx.Forward(c, mapFunc(k), v)
		return nil
	})
}

// MapBoth creates a processor that transforms both key and value.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.MapBoth(func(k string, v Order) (string, OrderSummary) {
//	        return k, OrderSummary{Total: v.Total}
//	    }),
//	    "summarize",
//	    "source",
//	)
func MapBoth[Kin, Vin, Kout, Vout any](mapFunc func(k Kin, v Vin) (Kout, Vout)) ProcessorBuilder[Kin, Vin, Kout, Vout] {
	return NewFunc(func(ctx ProcessorContext[Kout, Vout], c context.Context, k Kin, v Vin) error {
		kOut, vOut := mapFunc(k, v)
		ctx.Forward(c, kOut, vOut)
		return nil
	})
}

// FlatMap creates a processor that transforms each record into zero or more records.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.FlatMap(func(k string, v string) []kprocessor.KV[string, string] {
//	        words := strings.Split(v, " ")
//	        result := make([]kprocessor.KV[string, string], len(words))
//	        for i, word := range words {
//	            result[i] = kprocessor.KV[string, string]{Key: word, Value: "1"}
//	        }
//	        return result
//	    }),
//	    "word-split",
//	    "source",
//	)
func FlatMap[Kin, Vin, Kout, Vout any](flatMapFunc func(k Kin, v Vin) []KV[Kout, Vout]) ProcessorBuilder[Kin, Vin, Kout, Vout] {
	return NewFunc(func(ctx ProcessorContext[Kout, Vout], c context.Context, k Kin, v Vin) error {
		for _, kv := range flatMapFunc(k, v) {
			ctx.Forward(c, kv.Key, kv.Value)
		}
		return nil
	})
}

// FlatMapValues creates a processor that transforms each value into zero or more values.
// The key is copied to each output record.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.FlatMapValues(func(v string) []int {
//	        return []int{len(v), len(v) * 2}
//	    }),
//	    "lengths",
//	    "source",
//	)
func FlatMapValues[K, Vin, Vout any](flatMapFunc func(v Vin) []Vout) ProcessorBuilder[K, Vin, K, Vout] {
	return NewFunc(func(ctx ProcessorContext[K, Vout], c context.Context, k K, v Vin) error {
		for _, vOut := range flatMapFunc(v) {
			ctx.Forward(c, k, vOut)
		}
		return nil
	})
}

// Peek creates a processor that invokes an action for each record without modifying it.
// Useful for debugging, logging, or side effects.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Peek(func(k string, v Order) {
//	        log.Printf("Processing order: %s, total: %.2f", k, v.Total)
//	    }),
//	    "log-orders",
//	    "source",
//	)
func Peek[K, V any](peekFunc func(k K, v V)) ProcessorBuilder[K, V, K, V] {
	return NewFunc(func(ctx ProcessorContext[K, V], c context.Context, k K, v V) error {
		peekFunc(k, v)
		ctx.Forward(c, k, v)
		return nil
	})
}

// Print creates a processor that prints each record using the provided format function.
// The record is forwarded unchanged.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.Print(func(k string, v int) string {
//	        return fmt.Sprintf("key=%s, value=%d", k, v)
//	    }),
//	    "debug",
//	    "source",
//	)
func Print[K, V any](formatFunc func(k K, v V) string) ProcessorBuilder[K, V, K, V] {
	return Peek(func(k K, v V) {
		println(formatFunc(k, v))
	})
}

// ForEach creates a terminal processor that invokes an action for each record.
// Unlike Peek, ForEach does not forward records downstream.
//
// Example:
//
//	kdag.MustRegisterProcessor(builder,
//	    kprocessor.ForEach(func(k string, v Order) {
//	        db.Save(k, v)
//	    }),
//	    "save-to-db",
//	    "source",
//	)
func ForEach[K, V any](forEachFunc func(k K, v V)) ProcessorBuilder[K, V, K, V] {
	return NewFunc(func(ctx ProcessorContext[K, V], c context.Context, k K, v V) error {
		forEachFunc(k, v)
		return nil
	})
}
