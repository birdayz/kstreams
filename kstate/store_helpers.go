package kstate

import (
	"fmt"

	"github.com/birdayz/kstreams/kprocessor"
)

// StateStoreGetter is implemented by adapters that wrap a StateStore
// This allows type-safe extraction of the underlying store without
// conflicting interface methods (kprocessor.Store vs kstate.StateStore)
type StateStoreGetter interface {
	GetStateStore() StateStore
}

// unwrapStore attempts to extract the underlying StateStore from an adapter
func unwrapStore(store kprocessor.Store) any {
	// Check if store implements StateStoreGetter (adapter pattern)
	if getter, ok := store.(StateStoreGetter); ok {
		return getter.GetStateStore()
	}
	return store
}

// GetKeyValueStore retrieves a typed KeyValueStore from the processor context.
// Returns an error if the store is not found or has the wrong type.
// Handles stores wrapped in adapters (common in kstreams architecture).
//
// Example:
//
//	func (p *MyProcessor) Init(ctx kprocessor.ProcessorContext[string, int]) error {
//	    store, err := kstate.GetKeyValueStore[string, int64](ctx, "my-store")
//	    if err != nil {
//	        return fmt.Errorf("get store: %w", err)
//	    }
//	    p.store = store
//	    return nil
//	}
func GetKeyValueStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) (KeyValueStore[K, V], error) {
	store := ctx.GetStore(name)
	if store == nil {
		return nil, fmt.Errorf("store %q not found", name)
	}

	// Unwrap adapter to get underlying StateStore, then type assert
	unwrapped := unwrapStore(store)
	if typed, ok := unwrapped.(KeyValueStore[K, V]); ok {
		return typed, nil
	}

	return nil, fmt.Errorf("store %q is not KeyValueStore[%T, %T], got %T", name, *new(K), *new(V), store)
}

// MustGetKeyValueStore retrieves a typed KeyValueStore, panicking on failure.
// Use this in Init() where failure should be fatal.
//
// Example:
//
//	func (p *MyProcessor) Init(ctx kprocessor.ProcessorContext[string, int]) error {
//	    p.store = kstate.MustGetKeyValueStore[string, int64](ctx, "my-store")
//	    return nil
//	}
func MustGetKeyValueStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) KeyValueStore[K, V] {
	store, err := GetKeyValueStore[K, V](ctx, name)
	if err != nil {
		panic(err)
	}
	return store
}

// GetBatchKeyValueStore retrieves a typed BatchKeyValueStore from the processor context.
// Returns an error if the store is not found or has the wrong type.
// Handles stores wrapped in adapters.
func GetBatchKeyValueStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) (BatchKeyValueStore[K, V], error) {
	store := ctx.GetStore(name)
	if store == nil {
		return nil, fmt.Errorf("store %q not found", name)
	}

	// Unwrap adapter to get underlying StateStore, then type assert
	unwrapped := unwrapStore(store)
	if typed, ok := unwrapped.(BatchKeyValueStore[K, V]); ok {
		return typed, nil
	}

	return nil, fmt.Errorf("store %q is not BatchKeyValueStore[%T, %T], got %T", name, *new(K), *new(V), store)
}

// MustGetBatchKeyValueStore retrieves a typed BatchKeyValueStore, panicking on failure.
func MustGetBatchKeyValueStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) BatchKeyValueStore[K, V] {
	store, err := GetBatchKeyValueStore[K, V](ctx, name)
	if err != nil {
		panic(err)
	}
	return store
}

// GetWindowedStore retrieves a typed WindowedStore from the processor context.
// Returns an error if the store is not found or has the wrong type.
// Handles stores wrapped in adapters.
func GetWindowedStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) (WindowedStore[K, V], error) {
	store := ctx.GetStore(name)
	if store == nil {
		return nil, fmt.Errorf("store %q not found", name)
	}

	// Unwrap adapter to get underlying StateStore, then type assert
	unwrapped := unwrapStore(store)
	if typed, ok := unwrapped.(WindowedStore[K, V]); ok {
		return typed, nil
	}

	return nil, fmt.Errorf("store %q is not WindowedStore[%T, %T], got %T", name, *new(K), *new(V), store)
}

// MustGetWindowedStore retrieves a typed WindowedStore, panicking on failure.
func MustGetWindowedStore[K comparable, V any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) WindowedStore[K, V] {
	store, err := GetWindowedStore[K, V](ctx, name)
	if err != nil {
		panic(err)
	}
	return store
}

// GetStore retrieves any store from the processor context and type-asserts it.
// Use this for custom store types. Returns error if store not found or wrong type.
//
// Example:
//
//	store, err := kstate.GetStore[*MyCustomStore](ctx, "custom-store")
func GetStore[S any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) (S, error) {
	store := ctx.GetStore(name)
	if store == nil {
		var zero S
		return zero, fmt.Errorf("store %q not found", name)
	}
	typed, ok := store.(S)
	if !ok {
		var zero S
		return zero, fmt.Errorf("store %q is not %T, got %T", name, zero, store)
	}
	return typed, nil
}

// MustGetStore retrieves any store and type-asserts it, panicking on failure.
func MustGetStore[S any, Kout, Vout any](ctx kprocessor.ProcessorContext[Kout, Vout], name string) S {
	store, err := GetStore[S](ctx, name)
	if err != nil {
		panic(err)
	}
	return store
}
