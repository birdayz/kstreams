package kdag

import (
	"context"
	"reflect"

	"github.com/birdayz/kstreams/kprocessor"
)

// Test helper: simple processor for testing
type TestProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func (p *TestProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *TestProcessor) Process(ctx context.Context, k string, v string) error {
	p.ctx.Forward(ctx, k, v) //nolint:staticcheck // SA1019: using deprecated Forward in test
	return nil
}

func (p *TestProcessor) Close() error {
	return nil
}

// mockBuilder implements RuntimeBuilder for testing
type mockBuilder struct {
	kind NodeType
}

func (m *mockBuilder) BuilderKind() NodeType {
	return m.kind
}

// registerTestSource is a test helper that registers a source node directly.
// This is equivalent to the execution.RegisterSource function but for testing kdag.
func registerTestSource(b *Builder, name, topic string) error {
	var s string
	return b.AddSourceNode(name, topic, reflect.TypeOf(s), reflect.TypeOf(s), &mockBuilder{kind: NodeTypeSource})
}

// registerTestProcessor is a test helper that registers a processor node directly.
func registerTestProcessor(b *Builder, name, parent string, stores ...string) error {
	// Check for duplicate node
	if _, exists := b.GetNode(NodeID(name)); exists {
		return ErrNodeAlreadyExists
	}

	parentNode, ok := b.GetNode(NodeID(parent))
	if !ok {
		return ErrNodeNotFound
	}

	// Validate stores exist
	for _, storeName := range stores {
		if _, ok := b.graph.Stores[storeName]; !ok {
			return ErrStoreNotFound
		}
	}

	var s string
	return b.AddProcessorNode(name, parent, reflect.TypeOf(s), reflect.TypeOf(s), reflect.TypeOf(s), reflect.TypeOf(s), stores, &mockBuilder{kind: NodeTypeProcessor}, parentNode)
}

// registerTestSink is a test helper that registers a sink node directly.
func registerTestSink(b *Builder, name, topic, parent string) error {
	// Check for duplicate node
	if _, exists := b.GetNode(NodeID(name)); exists {
		return ErrNodeAlreadyExists
	}

	parentNode, ok := b.GetNode(NodeID(parent))
	if !ok {
		return ErrNodeNotFound
	}

	var s string
	return b.AddSinkNode(name, topic, parent, reflect.TypeOf(s), reflect.TypeOf(s), &mockBuilder{kind: NodeTypeSink}, parentNode)
}
