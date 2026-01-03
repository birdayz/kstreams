package kdag

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/birdayz/kstreams/internal/coordination"
)

// Validation limits to prevent pathological cases
const (
	MaxNodesPerDAG     = 10000
	MaxDepth           = 500
	MaxChildrenPerNode = 1000
)

// Validate performs all topology validations.
// This includes cycle detection, orphan detection, sink validation, and store validation.
// Returns early on first error for better UX.
func (g *Graph) Validate() error {
	// Check size limits
	if len(g.Nodes) > MaxNodesPerDAG {
		return fmt.Errorf("%w: node count %d exceeds maximum %d",
			ErrInvalidTopology, len(g.Nodes), MaxNodesPerDAG)
	}

	// 1. Cycle detection using DFS
	if err := g.detectCycles(); err != nil {
		return fmt.Errorf("DAG validation failed: %w", err)
	}

	// 2. Orphaned nodes (unreachable from sources)
	if err := g.validateNoOrphans(); err != nil {
		return fmt.Errorf("DAG validation failed: %w", err)
	}

	// 3. Sink node validation (no children)
	if err := g.validateSinks(); err != nil {
		return fmt.Errorf("DAG validation failed: %w", err)
	}

	// 4. Store existence validation
	if err := g.validateStores(); err != nil {
		return fmt.Errorf("DAG validation failed: %w", err)
	}

	return nil
}

// detectCycles uses Depth-First Search (DFS) to find cycles in the DAG.
// Returns ErrCycleDetected if any cycle is found.
// Time complexity: O(V + E) where V is vertices and E is edges.
func (g *Graph) detectCycles() error {
	visited := make(map[NodeID]bool, len(g.Nodes))
	recStack := make(map[NodeID]bool, len(g.Nodes))

	var dfs func(NodeID, []NodeID, int) error
	dfs = func(nodeID NodeID, path []NodeID, depth int) error {
		if depth > MaxDepth {
			return fmt.Errorf("%w: maximum depth %d exceeded", ErrInvalidTopology, MaxDepth)
		}

		visited[nodeID] = true
		recStack[nodeID] = true
		path = append(path, nodeID)

		node := g.Nodes[nodeID]
		if len(node.Children) > MaxChildrenPerNode {
			return fmt.Errorf("%w: node %s has %d children, exceeds maximum %d",
				ErrInvalidTopology, nodeID, len(node.Children), MaxChildrenPerNode)
		}

		for _, childID := range node.Children {
			if !visited[childID] {
				if err := dfs(childID, path, depth+1); err != nil {
					return err
				}
			} else if recStack[childID] {
				// Cycle detected!
				cyclePath := append(path, childID)
				pathStr := make([]string, len(cyclePath))
				for i, id := range cyclePath {
					pathStr[i] = string(id)
				}
				return fmt.Errorf("%w: %s", ErrCycleDetected, strings.Join(pathStr, " -> "))
			}
		}

		recStack[nodeID] = false
		return nil
	}

	// Check all nodes (handles disconnected components)
	for nodeID := range g.Nodes {
		if !visited[nodeID] {
			if err := dfs(nodeID, nil, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateNoOrphans checks that all nodes are reachable from at least one source.
// Returns ErrOrphanedNodes if unreachable nodes are found.
func (g *Graph) validateNoOrphans() error {
	// Find all reachable nodes from sources
	reachable := make(map[NodeID]bool, len(g.Nodes))

	for _, sourceID := range g.Sources {
		g.markReachable(sourceID, reachable)
	}

	// Check if any nodes are unreachable
	var orphans []NodeID
	for nodeID := range g.Nodes {
		if !reachable[nodeID] {
			orphans = append(orphans, nodeID)
		}
	}

	if len(orphans) > 0 {
		slices.Sort(orphans) // Deterministic error message
		orphanStrs := make([]string, len(orphans))
		for i, id := range orphans {
			orphanStrs[i] = string(id)
		}
		return fmt.Errorf("%w (unreachable from sources): %s",
			ErrOrphanedNodes, strings.Join(orphanStrs, ", "))
	}

	return nil
}

// markReachable recursively marks all nodes reachable from the given node.
func (g *Graph) markReachable(nodeID NodeID, reachable map[NodeID]bool) {
	if reachable[nodeID] {
		return // Already visited
	}

	reachable[nodeID] = true
	node := g.Nodes[nodeID]

	for _, childID := range node.Children {
		g.markReachable(childID, reachable)
	}
}

// validateSinks ensures that sink nodes don't have children.
func (g *Graph) validateSinks() error {
	for nodeID, node := range g.Nodes {
		if node.Type == NodeTypeSink && len(node.Children) > 0 {
			childStrs := make([]string, len(node.Children))
			for i, id := range node.Children {
				childStrs[i] = string(id)
			}
			return fmt.Errorf("%w: sink node %s has children: %s",
				ErrInvalidTopology, nodeID, strings.Join(childStrs, ", "))
		}
	}
	return nil
}

// validateStores checks that all referenced stores exist.
// Returns ErrStoreNotFound if a store is referenced but not registered.
func (g *Graph) validateStores() error {
	for nodeID, node := range g.Nodes {
		for _, storeName := range node.StoreNames {
			if _, ok := g.Stores[storeName]; !ok {
				return fmt.Errorf("%w: node %s references undefined store %q",
					ErrStoreNotFound, nodeID, storeName)
			}
		}
	}
	return nil
}

// insertSorted inserts an item into a sorted slice maintaining sort order.
// This is more efficient than repeatedly sorting the entire slice.
// Time complexity: O(log n + n) for binary search + insert.
func insertSorted(slice []NodeID, item NodeID) []NodeID {
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i] >= item
	})
	return slices.Insert(slice, idx, item)
}

// topologicalSort creates a deterministic topological ordering using Kahn's algorithm.
// This ensures that nodes are processed in dependency order and deterministically.
// Time complexity: O(V log V + E) where V is vertices and E is edges.
// The log V factor comes from maintaining sorted order for determinism.
func (g *Graph) topologicalSort() ([]NodeID, error) {
	// Calculate in-degrees (pre-allocate for all nodes)
	inDegree := make(map[NodeID]int, len(g.Nodes))
	for nodeID := range g.Nodes {
		inDegree[nodeID] = 0
	}
	for _, node := range g.Nodes {
		for _, childID := range node.Children {
			inDegree[childID]++
		}
	}

	// Queue of nodes with no incoming edges (sources)
	// Use sorted slice for deterministic ordering
	queue := make([]NodeID, 0, len(g.Nodes)/4) // Pre-allocate reasonable size
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}
	slices.Sort(queue) // Initial sort

	result := make([]NodeID, 0, len(g.Nodes))
	for len(queue) > 0 {
		// Pop first (deterministic ordering)
		nodeID := queue[0]
		queue = queue[1:]
		result = append(result, nodeID)

		node := g.Nodes[nodeID]
		// Sort children for deterministic processing
		children := make([]NodeID, len(node.Children))
		copy(children, node.Children)
		slices.Sort(children)

		for _, childID := range children {
			inDegree[childID]--
			if inDegree[childID] == 0 {
				// Insert in sorted position instead of appending and sorting entire queue
				queue = insertSorted(queue, childID)
			}
		}
	}

	// If we didn't process all nodes, there must be a cycle
	if len(result) != len(g.Nodes) {
		return nil, fmt.Errorf("%w: topological sort failed", ErrCycleDetected)
	}

	return result, nil
}

// findDescendants finds all nodes reachable from the given node.
// Used for computing partition groups.
// Sink nodes are excluded from the result.
func (g *Graph) findDescendants(nodeID NodeID) []NodeID {
	var result []NodeID
	visited := make(map[NodeID]bool, len(g.Nodes)/4) // Pre-allocate reasonable size

	var dfs func(NodeID)
	dfs = func(current NodeID) {
		if visited[current] {
			return
		}
		visited[current] = true

		node := g.Nodes[current]
		for _, childID := range node.Children {
			// Skip sink nodes in the traversal
			if g.Nodes[childID].Type != NodeTypeSink {
				result = append(result, childID)
				dfs(childID)
			}
		}
	}

	dfs(nodeID)
	return result
}

// ComputePartitionGroups creates deterministic partition groups.
// Partition groups are sub-graphs that must be co-partitioned.
// Returns groups in deterministic order based on source topic names.
func (g *Graph) ComputePartitionGroups() ([]*coordination.PartitionGroup, error) {
	// Use SORTED iteration over sources for determinism
	sourceTopics := make([]string, 0, len(g.Sources))
	for topic := range g.Sources {
		sourceTopics = append(sourceTopics, topic)
	}
	slices.Sort(sourceTopics)

	groups := make([]*coordination.PartitionGroup, 0, len(sourceTopics))
	// Reuse store map to reduce allocations
	storeSet := make(map[string]bool, 16)

	for _, topic := range sourceTopics {
		nodeID := g.Sources[topic]

		// Find all descendant processors
		processors := g.findDescendants(nodeID)

		// Collect all stores used by these processors
		clear(storeSet) // Go 1.21+ - reset map for reuse
		for _, procID := range processors {
			node := g.Nodes[procID]
			for _, storeName := range node.StoreNames {
				storeSet[storeName] = true
			}
		}

		// Convert to sorted slice (pre-allocate)
		stores := make([]string, 0, len(storeSet))
		for storeName := range storeSet {
			stores = append(stores, storeName)
		}
		slices.Sort(stores)

		// Sort processors too
		slices.Sort(processors)

		// Convert NodeID to string for PartitionGroup (pre-allocate)
		processorNames := make([]string, len(processors))
		for i, procID := range processors {
			processorNames[i] = string(procID)
		}

		group := &coordination.PartitionGroup{
			SourceTopics:   []string{topic},
			ProcessorNames: processorNames,
			StoreNames:     stores,
		}
		groups = append(groups, group)
	}

	// Merge overlapping groups
	return coordination.MergePartitionGroups(groups), nil
}
