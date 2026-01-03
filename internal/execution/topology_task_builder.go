package execution

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/internal/statemgr"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kstate"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RuntimeNode is a node that can be initialized and closed.
// All runtime nodes implement this interface.
type RuntimeNode interface {
	Init() error
	Close() error
}

// StateManagerResult holds the result of state manager initialization.
type StateManagerResult struct {
	StateManager *statemgr.StateManager
	Stores       map[string]kprocessor.Store
	StateStores  map[string]kstate.StateStore
}

// NodeBuildResult holds the result of node building.
type NodeBuildResult struct {
	Sources         map[string]runtime.RawRecordProcessor
	Processors      map[string]runtime.Node
	Sinks           map[string]runtime.Flusher
	ProcessorStores map[string][]string
}

// InitializeStateManager creates and initializes the StateManager with stores.
// This is the first phase of task building - it sets up all state infrastructure.
func InitializeStateManager(
	g *kdag.Graph,
	taskID string,
	partition int32,
	client *kgo.Client,
	appID string,
	stateDir string,
	collector runtime.ChangelogCollector,
) (*StateManagerResult, error) {
	// Create StateManager
	stateManager := statemgr.NewStateManager(taskID, partition, appID, stateDir, client, nil, nil, 0)

	if err := stateManager.AcquireLock(); err != nil {
		return nil, fmt.Errorf("acquire state directory lock: %w", err)
	}

	// Build and register stores
	builtStores := make(map[string]kprocessor.Store)
	stateStores := make(map[string]kstate.StateStore)

	for name, storeDef := range g.Stores {
		sb := storeDef.Builder // Already typed as kstate.TypeErasedStoreBuilder
		store, err := sb.BuildStateStore()
		if err != nil {
			return nil, fmt.Errorf("build store %s: %w", name, err)
		}
		stateStores[name] = store

		changelogEnabled := sb.ChangelogEnabled()
		restoreCallback := sb.RestoreCallback()
		if err := stateManager.RegisterStore(store, changelogEnabled, restoreCallback, nil); err != nil {
			return nil, fmt.Errorf("register store %s: %w", name, err)
		}

		builtStores[name] = adaptStateStoreToOldStore(store)
	}

	// Initialize stores with context (using collector for changelog writes)
	storeContext := runtime.NewInternalProcessorContextWithState[any, any](nil, nil, stateManager, collector)
	for name, store := range stateStores {
		if err := store.Init(storeContext); err != nil {
			return nil, fmt.Errorf("initialize store %s: %w", name, err)
		}
	}

	return &StateManagerResult{
		StateManager: stateManager,
		Stores:       builtStores,
		StateStores:  stateStores,
	}, nil
}

// RestoreStateManager loads checkpoints and restores state from changelogs.
// This is the third phase of task building - it restores state from Kafka.
func RestoreStateManager(ctx context.Context, stateManager *statemgr.StateManager, eosEnabled bool) error {
	if err := stateManager.InitializeOffsetsFromCheckpoint(eosEnabled); err != nil {
		return fmt.Errorf("initialize checkpoints: %w", err)
	}

	if eosEnabled {
		if err := stateManager.DeleteCheckpoint(); err != nil {
			return fmt.Errorf("delete checkpoint after load (EOS): %w", err)
		}
	}

	if err := stateManager.RestoreState(ctx); err != nil {
		return fmt.Errorf("restore state: %w", err)
	}

	if !eosEnabled {
		if err := stateManager.Checkpoint(ctx); err != nil {
			return fmt.Errorf("checkpoint after restoration: %w", err)
		}
	}

	return nil
}

// BuildNodes builds all runtime nodes from the graph in reverse topological order.
// This is the second phase of task building - it creates the processing graph.
func BuildNodes(
	g *kdag.Graph,
	partition int32,
	stores map[string]kprocessor.Store,
	client *kgo.Client,
	collector runtime.ChangelogCollector,
	stateManager runtime.StateManager,
) (*NodeBuildResult, error) {
	// Build nodes in reverse topological order (sinks first, sources last)
	// This way children are built before parents, so we can wire during construction.
	order, err := g.ReverseTopologicalSort()
	if err != nil {
		return nil, fmt.Errorf("reverse topological sort: %w", err)
	}

	builtNodes := make(map[kdag.NodeID]RuntimeNode)
	builtProcessors := make(map[string]runtime.Node)
	builtSources := make(map[string]runtime.RawRecordProcessor)
	builtSinks := make(map[string]runtime.Flusher)

	for _, nodeID := range order {
		node := g.Nodes[nodeID]

		// Collect already-built children
		childNodes := make(map[string]RuntimeNode)
		for _, childID := range node.Children {
			childNodes[string(childID)] = builtNodes[childID]
		}

		// Build this node with its children
		runtimeNode, err := buildRuntimeNode(node, partition, stores, client, collector, stateManager, childNodes)
		if err != nil {
			return nil, fmt.Errorf("build node %s: %w", nodeID, err)
		}
		builtNodes[nodeID] = runtimeNode

		// Categorize by type
		switch node.Type {
		case kdag.NodeTypeSource:
			if source, ok := runtimeNode.(runtime.RawRecordProcessor); ok {
				topic := g.GetSourceTopic(nodeID)
				if topic != "" {
					builtSources[topic] = source
				}
			}
		case kdag.NodeTypeProcessor:
			if processor, ok := runtimeNode.(runtime.Node); ok {
				builtProcessors[string(nodeID)] = processor
			}
		case kdag.NodeTypeSink:
			if sink, ok := runtimeNode.(runtime.Flusher); ok {
				builtSinks[string(nodeID)] = sink
			}
		}
	}

	// Build processor-to-stores map
	processorStores := make(map[string][]string)
	for _, nodeID := range g.NodeOrder {
		node := g.Nodes[nodeID]
		if node.Type == kdag.NodeTypeProcessor && len(node.StoreNames) > 0 {
			processorStores[string(nodeID)] = node.StoreNames
		}
	}

	return &NodeBuildResult{
		Sources:         builtSources,
		Processors:      builtProcessors,
		Sinks:           builtSinks,
		ProcessorStores: processorStores,
	}, nil
}

// BuildTaskFromGraph creates a Task instance for the given topics and partition.
// Uses single-pass construction: builds nodes in reverse topological order (sinks first,
// sources last) so children are built before parents. This eliminates the need for a
// separate wiring phase.
func BuildTaskFromGraph(g *kdag.Graph, topics []string, partition int32, client *kgo.Client, appID string, stateDir string) (*Task, error) {
	taskID := fmt.Sprintf("%v-%d", topics, partition)

	// Create RecordCollector for changelog batching
	collector := NewRecordCollector(client)

	// Phase 1: Initialize StateManager and stores
	smResult, err := InitializeStateManager(g, taskID, partition, client, appID, stateDir, collector)
	if err != nil {
		return nil, fmt.Errorf("initialize state manager: %w", err)
	}

	// Phase 2: Build nodes
	nodeResult, err := BuildNodes(g, partition, smResult.Stores, client, collector, smResult.StateManager)
	if err != nil {
		return nil, fmt.Errorf("build nodes: %w", err)
	}

	// Phase 3: Restore state from changelogs
	ctx := context.Background()
	eosEnabled := false
	if err := RestoreStateManager(ctx, smResult.StateManager, eosEnabled); err != nil {
		return nil, fmt.Errorf("restore state manager: %w", err)
	}

	return NewTaskWithConfig(TaskConfig{
		TaskID:             taskID,
		Topics:             topics,
		Partition:          partition,
		RootNodes:          nodeResult.Sources,
		Stores:             smResult.Stores,
		Processors:         nodeResult.Processors,
		Sinks:              nodeResult.Sinks,
		ProcessorsToStores: nodeResult.ProcessorStores,
		StateManager:       smResult.StateManager,
		Client:             client,
		Collector:          collector,
	}), nil
}

// buildRuntimeNode builds a single runtime node from its kdag.Node definition.
// childNodes contains already-built children that will be wired to this node.
func buildRuntimeNode(
	node *kdag.Node,
	partition int32,
	stores map[string]kprocessor.Store,
	client *kgo.Client,
	collector runtime.ChangelogCollector,
	stateManager runtime.StateManager,
	childNodes map[string]RuntimeNode,
) (RuntimeNode, error) {
	switch node.Type {
	case kdag.NodeTypeSource:
		return buildSourceNode(node, childNodes)
	case kdag.NodeTypeProcessor:
		return buildProcessorNode(node, stores, collector, stateManager, childNodes)
	case kdag.NodeTypeSink:
		return buildSinkNode(node, client)
	default:
		return nil, fmt.Errorf("unknown node type: %v", node.Type)
	}
}

// buildSourceNode builds a source runtime node.
// Uses type switch on the RuntimeBuilder to get the typed data.
func buildSourceNode(node *kdag.Node, childNodes map[string]RuntimeNode) (RuntimeNode, error) {
	// The RuntimeBuilder contains type-specific data
	// We need to handle it through the typed builder pattern
	builder := node.RuntimeBuilder
	if builder == nil {
		return nil, fmt.Errorf("source node %s has no RuntimeBuilder", node.ID)
	}

	// Use the SourceBuilder interface
	if sb, ok := builder.(SourceBuilder); ok {
		return sb.Build(childNodes)
	}

	return nil, fmt.Errorf("source node %s RuntimeBuilder does not implement SourceBuilder", node.ID)
}

// buildProcessorNode builds a processor runtime node.
func buildProcessorNode(
	node *kdag.Node,
	stores map[string]kprocessor.Store,
	collector runtime.ChangelogCollector,
	stateManager runtime.StateManager,
	childNodes map[string]RuntimeNode,
) (RuntimeNode, error) {
	builder := node.RuntimeBuilder
	if builder == nil {
		return nil, fmt.Errorf("processor node %s has no RuntimeBuilder", node.ID)
	}

	if pb, ok := builder.(ProcessorBuilder); ok {
		return pb.Build(stores, collector, stateManager, childNodes)
	}

	return nil, fmt.Errorf("processor node %s RuntimeBuilder does not implement ProcessorBuilder", node.ID)
}

// buildSinkNode builds a sink runtime node.
func buildSinkNode(node *kdag.Node, client *kgo.Client) (RuntimeNode, error) {
	builder := node.RuntimeBuilder
	if builder == nil {
		return nil, fmt.Errorf("sink node %s has no RuntimeBuilder", node.ID)
	}

	if sb, ok := builder.(SinkBuilder); ok {
		return sb.Build(client)
	}

	return nil, fmt.Errorf("sink node %s RuntimeBuilder does not implement SinkBuilder", node.ID)
}

// SourceBuilder is implemented by source node data to build runtime nodes.
type SourceBuilder interface {
	Build(children map[string]RuntimeNode) (RuntimeNode, error)
}

// ProcessorBuilder is implemented by processor node data to build runtime nodes.
type ProcessorBuilder interface {
	Build(stores map[string]kprocessor.Store, collector runtime.ChangelogCollector, sm runtime.StateManager, children map[string]RuntimeNode) (RuntimeNode, error)
}

// SinkBuilder is implemented by sink node data to build runtime nodes.
type SinkBuilder interface {
	Build(client *kgo.Client) (RuntimeNode, error)
}

// adaptStateStoreToOldStore adapts kstate.StateStore to kprocessor.Store interface
func adaptStateStoreToOldStore(stateStore kstate.StateStore) kprocessor.Store {
	return &stateStoreAdapter{stateStore}
}

type stateStoreAdapter struct {
	kstate.StateStore
}

func (a *stateStoreAdapter) Init() error {
	return nil
}

func (a *stateStoreAdapter) Flush() error {
	return a.StateStore.Flush(context.Background())
}

func (a *stateStoreAdapter) Close() error {
	return a.StateStore.Close()
}

func (a *stateStoreAdapter) GetStateStore() kstate.StateStore {
	return a.StateStore
}
