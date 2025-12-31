package kstreams

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BuildTask creates a Task instance for the given topics and partition
// This is a simplified 2-pass construction replacing the old 125-line multi-pass approach
func (g *TopologyGraph) BuildTask(topics []string, partition int32, client *kgo.Client, appID string, stateDir string) (*Task, error) {
	// Create taskID (matches Kafka Streams pattern: "topics-partition")
	taskID := fmt.Sprintf("%v-%d", topics, partition)

	// Create StateManager for this task
	// TODO: Pass proper logger from topology/worker context
	// TODO: Pass custom StateRestoreListener for progress monitoring
	// TODO: Pass configurable restoration timeout from Stream/Worker config
	stateManager := NewStateManager(taskID, partition, appID, stateDir, client, nil, nil, 0)

	// CRITICAL: Acquire directory lock before any state operations
	// This prevents concurrent access to the same state directory
	// Matches Kafka Streams' StateDirectory.lock()
	if err := stateManager.AcquireLock(); err != nil {
		return nil, fmt.Errorf("acquire state directory lock: %w", err)
	}

	// Phase 1: Build all stores and register with StateManager
	// TODO: Fix this to use new StoreBuilder[StateStore] interface
	builtStores := make(map[string]Store)
	stateStores := make(map[string]StateStore) // Track actual StateStore instances

	for name, storeDef := range g.stores {
		// TEMPORARY: Store building needs refactoring for new architecture
		store := storeDef.Builder.Build() // Returns StateStore now
		stateStores[name] = store

		// Register store with StateManager
		changelogEnabled := storeDef.Builder.ChangelogEnabled()
		restoreCallback := storeDef.Builder.RestoreCallback()
		// TODO: Add CommitCallback() method to StoreBuilder interface
		// For now, pass nil (stores can implement CommitCallback if needed)
		if err := stateManager.RegisterStore(store, changelogEnabled, restoreCallback, nil); err != nil {
			return nil, fmt.Errorf("register store %s: %w", name, err)
		}

		// Need to adapt StateStore to old Store interface
		builtStores[name] = adaptStateStoreToOldStore(store)
	}

	// CRITICAL: Initialize stores BEFORE restoration
	// This sets up ProcessorContextInternal for changelog wrappers
	// Matches Kafka Streams' lifecycle: Init → Restore → Process
	// Create a minimal ProcessorContextInternal for store initialization
	storeContext := &InternalProcessorContext[any, any]{
		outputs:      nil, // Stores don't forward
		stores:       nil, // Stores don't access other stores during init
		stateManager: stateManager,
		client:       client,
	}

	for name, store := range stateStores {
		if err := store.Init(storeContext); err != nil {
			return nil, fmt.Errorf("initialize store %s: %w", name, err)
		}
	}

	// Phase 2: Build all nodes in deterministic order
	// nodeOrder was populated during graph construction
	builtNodes := make(map[NodeID]RuntimeNode)
	builtProcessors := make(map[string]Node)
	builtSources := make(map[string]RawRecordProcessor)
	builtSinks := make(map[string]Flusher)

	for _, nodeID := range g.nodeOrder {
		node := g.nodes[nodeID]
		runtimeNode, err := node.Build(partition, builtStores, client, stateManager)
		if err != nil {
			return nil, fmt.Errorf("build node %s: %w", nodeID, err)
		}
		builtNodes[nodeID] = runtimeNode

		// Categorize nodes by type for Task compatibility
		switch node.Type {
		case NodeTypeSource:
			if source, ok := runtimeNode.(RawRecordProcessor); ok {
				// The topic is found by reverse lookup in sources map
				topic := g.findTopicForSource(nodeID)
				if topic != "" {
					builtSources[topic] = source
				}
			}

		case NodeTypeProcessor:
			if processor, ok := runtimeNode.(Node); ok {
				builtProcessors[string(nodeID)] = processor
			}

		case NodeTypeSink:
			if sink, ok := runtimeNode.(Flusher); ok {
				builtSinks[string(nodeID)] = sink
			}
		}
	}

	// Phase 3: Wire connections using the captured Wire functions
	for _, nodeID := range g.nodeOrder {
		node := g.nodes[nodeID]
		parent := builtNodes[nodeID]

		for _, childID := range node.Children {
			child := builtNodes[childID]

			// Call the Wire function that was captured during registration
			// This function knows the proper types and performs safe casts
			if err := node.Wire(parent, childID, child); err != nil {
				return nil, fmt.Errorf("wire %s -> %s: %w", nodeID, childID, err)
			}
		}
	}

	// Phase 4: Build processor-to-stores map for Task
	processorStores := make(map[string][]string)
	for _, nodeID := range g.nodeOrder {
		node := g.nodes[nodeID]
		if node.Type == NodeTypeProcessor && len(node.StoreNames) > 0 {
			processorStores[string(nodeID)] = node.StoreNames
		}
	}

	// Phase 5: Initialize StateManager (restore from changelog topics)
	ctx := context.Background()
	// TODO: Pass actual EOS setting from Worker/Stream configuration
	// For now, use false - proper EOS detection will be wired through later
	eosEnabled := false
	if err := stateManager.InitializeOffsetsFromCheckpoint(eosEnabled); err != nil {
		return nil, fmt.Errorf("initialize checkpoints: %w", err)
	}

	// CRITICAL: In EOS mode, delete checkpoint after loading
	// Under EOS, checkpoint is only valid at startup
	// After loading, all offsets come from changelog writes + commits
	// Prevents stale checkpoint reuse on crash
	// Matches Kafka Streams' ProcessorStateManager.deleteCheckPointFileIfEOSEnabled()
	if eosEnabled {
		if err := stateManager.DeleteCheckpoint(); err != nil {
			return nil, fmt.Errorf("delete checkpoint after load (EOS): %w", err)
		}
	}

	if err := stateManager.RestoreState(ctx); err != nil {
		return nil, fmt.Errorf("restore state: %w", err)
	}

	// CRITICAL: Checkpoint after restoration completes
	// This ensures if process crashes before first commit, we don't re-restore everything
	// Matches Kafka Streams' StreamTask.completeRestoration() in non-EOS mode
	// In EOS mode, Kafka Streams skips this (checkpoint written on first commit)
	if !eosEnabled {
		if err := stateManager.Checkpoint(ctx); err != nil {
			return nil, fmt.Errorf("checkpoint after restoration: %w", err)
		}
	}

	// Create the Task with StateManager
	task := NewTask(
		topics,
		partition,
		builtSources,
		builtStores,
		builtProcessors,
		builtSinks,
		processorStores,
		stateManager,
		client,
		taskID,
	)

	return task, nil
}

// findTopicForSource finds the topic name for a given source node ID
// Helper function for BuildTask
func (g *TopologyGraph) findTopicForSource(nodeID NodeID) string {
	for topic, sourceNodeID := range g.sources {
		if sourceNodeID == nodeID {
			return topic
		}
	}
	return ""
}

// extractRootNodes extracts the source nodes for the given topics
// Helper function if we need it later
func extractRootNodes(topics []string, sources map[string]NodeID, builtNodes map[NodeID]RuntimeNode) map[string]RawRecordProcessor {
	rootNodes := make(map[string]RawRecordProcessor)

	for _, topic := range topics {
		if sourceID, ok := sources[topic]; ok {
			if node, ok := builtNodes[sourceID]; ok {
				if source, ok := node.(RawRecordProcessor); ok {
					rootNodes[topic] = source
				}
			}
		}
	}

	return rootNodes
}

// adaptStateStoreToOldStore is a temporary adapter to bridge new StateStore to old Store interface
// TODO: Remove this once full migration is complete
func adaptStateStoreToOldStore(stateStore StateStore) Store {
	return &stateStoreAdapter{stateStore}
}

type stateStoreAdapter struct {
	StateStore
}

func (a *stateStoreAdapter) Init() error {
	// Old Init() didn't take context
	// New Init() takes ProcessorContextInternal
	// For now, just return nil - proper init will be called later
	return nil
}

func (a *stateStoreAdapter) Flush() error {
	// Old Flush() didn't take context
	return a.StateStore.Flush(context.Background())
}

func (a *stateStoreAdapter) Close() error {
	return a.StateStore.Close()
}
