package kstreams

import (
	"errors"
	"fmt"
	"slices"
)

// Validate performs all topology validations
// This includes cycle detection, orphan detection, sink validation, and store validation
func (g *TopologyGraph) Validate() error {
	var validationErrors []error

	// 1. Cycle detection using DFS
	if err := g.detectCycles(); err != nil {
		validationErrors = append(validationErrors, err)
	}

	// 2. Orphaned nodes (unreachable from sources)
	if err := g.validateNoOrphans(); err != nil {
		validationErrors = append(validationErrors, err)
	}

	// 3. Sink node validation (no children)
	if err := g.validateSinks(); err != nil {
		validationErrors = append(validationErrors, err)
	}

	// 4. Store existence validation
	if err := g.validateStores(); err != nil {
		validationErrors = append(validationErrors, err)
	}

	if len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return nil
}

// detectCycles uses DFS to find cycles in the DAG
// Returns an error if any cycle is detected
func (g *TopologyGraph) detectCycles() error {
	visited := make(map[NodeID]bool)
	recStack := make(map[NodeID]bool)

	var dfs func(NodeID, []NodeID) error
	dfs = func(nodeID NodeID, path []NodeID) error {
		visited[nodeID] = true
		recStack[nodeID] = true
		path = append(path, nodeID)

		node := g.nodes[nodeID]
		for _, childID := range node.Children {
			if !visited[childID] {
				if err := dfs(childID, path); err != nil {
					return err
				}
			} else if recStack[childID] {
				// Cycle detected!
				cyclePath := append(path, childID)
				return fmt.Errorf("cycle detected: %v", cyclePath)
			}
		}

		recStack[nodeID] = false
		return nil
	}

	// Check all nodes (handles disconnected components)
	for nodeID := range g.nodes {
		if !visited[nodeID] {
			if err := dfs(nodeID, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateNoOrphans checks that all nodes are reachable from at least one source
func (g *TopologyGraph) validateNoOrphans() error {
	// Find all reachable nodes from sources
	reachable := make(map[NodeID]bool)

	for _, sourceID := range g.sources {
		g.markReachable(sourceID, reachable)
	}

	// Check if any nodes are unreachable
	var orphans []NodeID
	for nodeID := range g.nodes {
		if !reachable[nodeID] {
			orphans = append(orphans, nodeID)
		}
	}

	if len(orphans) > 0 {
		slices.Sort(orphans) // Deterministic error message
		return fmt.Errorf("orphaned nodes (unreachable from sources): %v", orphans)
	}

	return nil
}

// markReachable recursively marks all nodes reachable from the given node
func (g *TopologyGraph) markReachable(nodeID NodeID, reachable map[NodeID]bool) {
	if reachable[nodeID] {
		return // Already visited
	}

	reachable[nodeID] = true
	node := g.nodes[nodeID]

	for _, childID := range node.Children {
		g.markReachable(childID, reachable)
	}
}

// validateSinks ensures that sink nodes don't have children
func (g *TopologyGraph) validateSinks() error {
	for nodeID, node := range g.nodes {
		if node.Type == NodeTypeSink && len(node.Children) > 0 {
			return fmt.Errorf("sink node %s has children: %v", nodeID, node.Children)
		}
	}
	return nil
}

// validateStores checks that all referenced stores exist
func (g *TopologyGraph) validateStores() error {
	for nodeID, node := range g.nodes {
		for _, storeName := range node.StoreNames {
			if _, ok := g.stores[storeName]; !ok {
				return fmt.Errorf("node %s references undefined store %s", nodeID, storeName)
			}
		}
	}
	return nil
}

// topologicalSort creates a deterministic topological ordering using Kahn's algorithm
// This ensures that nodes are processed in dependency order and deterministically
func (g *TopologyGraph) topologicalSort() ([]NodeID, error) {
	// Calculate in-degrees
	inDegree := make(map[NodeID]int)
	for nodeID := range g.nodes {
		inDegree[nodeID] = 0
	}
	for _, node := range g.nodes {
		for _, childID := range node.Children {
			inDegree[childID]++
		}
	}

	// Queue of nodes with no incoming edges (sources)
	// Use sorted slice for deterministic ordering
	var queue []NodeID
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}
	slices.Sort(queue) // DETERMINISTIC!

	var result []NodeID
	for len(queue) > 0 {
		// Pop first (deterministic ordering)
		nodeID := queue[0]
		queue = queue[1:]
		result = append(result, nodeID)

		node := g.nodes[nodeID]
		// Sort children for deterministic processing
		children := make([]NodeID, len(node.Children))
		copy(children, node.Children)
		slices.Sort(children) // DETERMINISTIC!

		for _, childID := range children {
			inDegree[childID]--
			if inDegree[childID] == 0 {
				queue = append(queue, childID)
				slices.Sort(queue) // Keep sorted for determinism
			}
		}
	}

	// If we didn't process all nodes, there must be a cycle
	if len(result) != len(g.nodes) {
		return nil, fmt.Errorf("graph has cycles (topological sort failed)")
	}

	return result, nil
}

// findDescendants finds all nodes reachable from the given node
// Used for computing partition groups
func (g *TopologyGraph) findDescendants(nodeID NodeID) []NodeID {
	var result []NodeID
	visited := make(map[NodeID]bool)

	var dfs func(NodeID)
	dfs = func(current NodeID) {
		if visited[current] {
			return
		}
		visited[current] = true

		node := g.nodes[current]
		for _, childID := range node.Children {
			// Skip sink nodes in the traversal
			if g.nodes[childID].Type != NodeTypeSink {
				result = append(result, childID)
				dfs(childID)
			}
		}
	}

	dfs(nodeID)
	return result
}

// ComputePartitionGroups creates deterministic partition groups
// Partition groups are sub-graphs that must be co-partitioned
func (g *TopologyGraph) ComputePartitionGroups() ([]*PartitionGroup, error) {
	// Use SORTED iteration over sources for determinism
	sourceTopics := make([]string, 0, len(g.sources))
	for topic := range g.sources {
		sourceTopics = append(sourceTopics, topic)
	}
	slices.Sort(sourceTopics) // DETERMINISTIC!

	var groups []*PartitionGroup
	for _, topic := range sourceTopics {
		nodeID := g.sources[topic]

		// Find all descendant processors
		processors := g.findDescendants(nodeID)

		// Collect all stores used by these processors
		storeSet := make(map[string]bool)
		for _, procID := range processors {
			node := g.nodes[procID]
			for _, storeName := range node.StoreNames {
				storeSet[storeName] = true
			}
		}

		// Convert to sorted slice
		stores := make([]string, 0, len(storeSet))
		for storeName := range storeSet {
			stores = append(stores, storeName)
		}
		slices.Sort(stores) // DETERMINISTIC!

		// Sort processors too
		slices.Sort(processors) // DETERMINISTIC!

		// Convert NodeID to string for PartitionGroup
		processorNames := make([]string, len(processors))
		for i, procID := range processors {
			processorNames[i] = string(procID)
		}

		group := &PartitionGroup{
			sourceTopics:   []string{topic},
			processorNames: processorNames,
			storeNames:     stores,
		}
		groups = append(groups, group)
	}

	// Merge overlapping groups (deterministic)
	return mergePartitionGroupsDeterministic(groups), nil
}

// mergePartitionGroupsDeterministic merges overlapping partition groups
// Uses deterministic ordering to ensure consistent results
func mergePartitionGroupsDeterministic(pgs []*PartitionGroup) []*PartitionGroup {
	for {
		merged := false
		for i := 0; i < len(pgs); i++ {
			for j := i + 1; j < len(pgs); j++ {
				if shouldMergeGroups(pgs[i], pgs[j]) {
					// Merge j into i
					pgs[i].sourceTopics = append(pgs[i].sourceTopics, pgs[j].sourceTopics...)
					pgs[i].processorNames = append(pgs[i].processorNames, pgs[j].processorNames...)
					pgs[i].storeNames = append(pgs[i].storeNames, pgs[j].storeNames...)

					// Remove duplicates and sort
					slices.Sort(pgs[i].sourceTopics)
					pgs[i].sourceTopics = slices.Compact(pgs[i].sourceTopics)

					slices.Sort(pgs[i].processorNames)
					pgs[i].processorNames = slices.Compact(pgs[i].processorNames)

					slices.Sort(pgs[i].storeNames)
					pgs[i].storeNames = slices.Compact(pgs[i].storeNames)

					// Remove j from list
					pgs = slices.Delete(pgs, j, j+1)
					merged = true
					break
				}
			}
			if merged {
				break
			}
		}
		if !merged {
			break
		}
	}

	// Final sort for all groups
	for _, group := range pgs {
		slices.Sort(group.sourceTopics)
		slices.Sort(group.processorNames)
		slices.Sort(group.storeNames)
	}

	return pgs
}

// shouldMergeGroups determines if two partition groups should be merged
// Groups are merged if they share any topics, processors, or stores
func shouldMergeGroups(a, b *PartitionGroup) bool {
	// Check for shared topics
	for _, topic := range a.sourceTopics {
		if slices.Contains(b.sourceTopics, topic) {
			return true
		}
	}

	// Check for shared processors
	for _, proc := range a.processorNames {
		if slices.Contains(b.processorNames, proc) {
			return true
		}
	}

	// Check for shared stores
	for _, store := range a.storeNames {
		if slices.Contains(b.storeNames, store) {
			return true
		}
	}

	return false
}
