package runtime

import (
	"context"

	"github.com/birdayz/kstreams/kprocessor"
)

// mockStore implements kprocessor.Store for tests
// TODO: Generate this with mockgen from kprocessor package
type mockStore struct {
	name       string
	persistent bool
	initFunc   func() error
	flushFunc  func() error
	closeFunc  func() error
}

func (m *mockStore) Name() string {
	if m.name != "" {
		return m.name
	}
	return "mock-store"
}

func (m *mockStore) Persistent() bool { return m.persistent }

func (m *mockStore) Init() error {
	if m.initFunc != nil {
		return m.initFunc()
	}
	return nil
}

func (m *mockStore) Flush() error {
	if m.flushFunc != nil {
		return m.flushFunc()
	}
	return nil
}

func (m *mockStore) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// TestProcessor for tests - simple processor that does nothing
type TestProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func (p *TestProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *TestProcessor) Process(ctx context.Context, k string, v string) error {
	return nil
}

func (p *TestProcessor) Close() error {
	return nil
}
