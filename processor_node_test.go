package kstreams

import (
	"context"
	"errors"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestProcessorNode_Process(t *testing.T) {
	t.Run("processes and forwards successfully", func(t *testing.T) {
		var processedKey string
		var processedValue string

		userProcessor := &SimpleTestProcessor{
			processFunc: func(ctx context.Context, k string, v string) error {
				processedKey = k
				processedValue = v
				// Forward to downstream
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "test-key", "test-value")
		assert.NoError(t, err)
		assert.Equal(t, "test-key", processedKey)
		assert.Equal(t, "test-value", processedValue)
	})

	t.Run("returns error from user processor", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{
			processFunc: func(ctx context.Context, k string, v string) error {
				return errors.New("processing failed")
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "key", "value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "processing failed")
	})

	t.Run("returns output errors from context", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{
			processFunc: func(ctx context.Context, k string, v string) error {
				return nil
			},
		}

		outputErr := errors.New("output error")
		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{outputErr},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "key", "value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "output error")
	})

	t.Run("returns multiple output errors joined", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{
			processFunc: func(ctx context.Context, k string, v string) error {
				return nil
			},
		}

		err1 := errors.New("error 1")
		err2 := errors.New("error 2")
		err3 := errors.New("error 3")

		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{err1, err2, err3},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "key", "value")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error 1")
		assert.Contains(t, err.Error(), "error 2")
		assert.Contains(t, err.Error(), "error 3")
	})

	t.Run("processes with type transformation", func(t *testing.T) {
		// Processor that transforms string -> int
		userProcessor := &mockUserProcessor[string, string, string, int]{
			processFunc: func(ctx context.Context, k string, v string) error {
				// Type transformation happens here (not shown in test)
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, int]{
			outputs:      map[string]InputProcessor[string, int]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		node := &ProcessorNode[string, string, string, int]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "key", "123")
		assert.NoError(t, err)
	})

	t.Run("handles empty output errors slice", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{
			processFunc: func(ctx context.Context, k string, v string) error {
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{}, // Empty
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Process(context.Background(), "key", "value")
		assert.NoError(t, err)
	})
}

func TestProcessorNode_Init(t *testing.T) {
	t.Run("initializes user processor with context", func(t *testing.T) {
		var initCalled bool
		var receivedCtx ProcessorContext[string, string]

		userProcessor := &SimpleTestProcessor{
			initFunc: func(ctx ProcessorContext[string, string]) error {
				initCalled = true
				receivedCtx = ctx
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]Store{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Init()
		assert.NoError(t, err)
		assert.True(t, initCalled)
		assert.NotZero(t, receivedCtx)
	})

	t.Run("returns error from user processor init", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{
			initFunc: func(ctx ProcessorContext[string, string]) error {
				return errors.New("init failed")
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]Store{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Init()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "init failed")
	})

	t.Run("provides access to stores via context", func(t *testing.T) {
		var receivedCtx ProcessorContext[string, string]

		userProcessor := &SimpleTestProcessor{
			initFunc: func(ctx ProcessorContext[string, string]) error {
				receivedCtx = ctx
				return nil
			},
		}

		mockStore := &mockStore{}
		processorCtx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores: map[string]Store{
				"test-store": mockStore,
			},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		node.Init()

		// Verify store is accessible
		store := receivedCtx.GetStore("test-store")
		assert.NotZero(t, store)
	})
}

func TestProcessorNode_Close(t *testing.T) {
	t.Run("close returns no error", func(t *testing.T) {
		userProcessor := &SimpleTestProcessor{}
		processorCtx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]Store{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Close()
		assert.NoError(t, err)
	})

	t.Run("close can be called multiple times", func(t *testing.T) {
		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    &TestProcessor{},
			processorContext: &InternalProcessorContext[string, string]{},
		}

		err1 := node.Close()
		err2 := node.Close()
		err3 := node.Close()

		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.NoError(t, err3)
	})
}

func TestProcessorNode_Integration(t *testing.T) {
	t.Run("full lifecycle - init, process, close", func(t *testing.T) {
		processCount := 0

		userProcessor := &SimpleTestProcessor{
			initFunc: func(ctx ProcessorContext[string, string]) error {
				return nil
			},
			processFunc: func(ctx context.Context, k string, v string) error {
				processCount++
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		// Init
		err := node.Init()
		assert.NoError(t, err)

		// Process multiple records
		for i := 0; i < 10; i++ {
			err = node.Process(context.Background(), "key", "value")
			assert.NoError(t, err)
		}

		assert.Equal(t, 10, processCount)

		// Close
		err = node.Close()
		assert.NoError(t, err)
	})

	t.Run("processor with state store", func(t *testing.T) {
		mockStore := &mockStore{
			initFunc: func() error {
				return nil
			},
		}

		userProcessor := &SimpleTestProcessor{
			initFunc: func(ctx ProcessorContext[string, string]) error {
				// Access store
				store := ctx.GetStore("test-store")
				assert.NotZero(t, store)
				return nil
			},
			processFunc: func(ctx context.Context, k string, v string) error {
				return nil
			},
		}

		processorCtx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores: map[string]Store{
				"test-store": mockStore,
			},
			outputErrors: []error{},
		}

		node := &ProcessorNode[string, string, string, string]{
			userProcessor:    userProcessor,
			processorContext: processorCtx,
		}

		err := node.Init()
		assert.NoError(t, err)

		err = node.Process(context.Background(), "key", "value")
		assert.NoError(t, err)
	})
}

// Mock user processor for testing type transformations
type mockUserProcessor[Kin, Vin, Kout, Vout any] struct {
	initFunc    func(ctx ProcessorContext[Kout, Vout]) error
	processFunc func(ctx context.Context, k Kin, v Vin) error
	closeFunc   func() error
}

func (m *mockUserProcessor[Kin, Vin, Kout, Vout]) Init(ctx ProcessorContext[Kout, Vout]) error {
	if m.initFunc != nil {
		return m.initFunc(ctx)
	}
	return nil
}

func (m *mockUserProcessor[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, k, v)
	}
	return nil
}

func (m *mockUserProcessor[Kin, Vin, Kout, Vout]) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// SimpleTestProcessor with flexible function hooks for processor_node tests
type SimpleTestProcessor struct {
	ctx         ProcessorContext[string, string]
	initFunc    func(ctx ProcessorContext[string, string]) error
	processFunc func(ctx context.Context, k string, v string) error
	closeFunc   func() error
}

func (p *SimpleTestProcessor) Init(ctx ProcessorContext[string, string]) error {
	p.ctx = ctx
	if p.initFunc != nil {
		return p.initFunc(ctx)
	}
	return nil
}

func (p *SimpleTestProcessor) Process(ctx context.Context, k string, v string) error {
	if p.processFunc != nil {
		return p.processFunc(ctx, k, v)
	}
	if p.ctx != nil {
		p.ctx.Forward(ctx, k, v)
	}
	return nil
}

func (p *SimpleTestProcessor) Close() error {
	if p.closeFunc != nil {
		return p.closeFunc()
	}
	return nil
}
