package coordination

import "slices"

// PartitionGroup is a sub-graph of nodes that must be co-partitioned as they depend on each other.
type PartitionGroup struct {
	SourceTopics   []string
	ProcessorNames []string
	StoreNames     []string
}

// MergePartitionGroups merges overlapping partition groups deterministically.
// Groups are merged when they share any source topic, processor name, or store name.
// The algorithm uses fixed-point iteration with sorting to ensure deterministic output.
func MergePartitionGroups(pgs []*PartitionGroup) []*PartitionGroup {
	if len(pgs) <= 1 {
		return pgs
	}

	changed := true
	for changed {
		changed = false

		// Sort groups by first topic for determinism
		slices.SortFunc(pgs, func(a, b *PartitionGroup) int {
			if len(a.SourceTopics) == 0 && len(b.SourceTopics) == 0 {
				return 0
			}
			if len(a.SourceTopics) == 0 {
				return 1
			}
			if len(b.SourceTopics) == 0 {
				return -1
			}
			if a.SourceTopics[0] < b.SourceTopics[0] {
				return -1
			}
			if a.SourceTopics[0] > b.SourceTopics[0] {
				return 1
			}
			return 0
		})

		// Find first pair to merge
		for i := 0; i < len(pgs); i++ {
			for j := i + 1; j < len(pgs); j++ {
				if shouldMergeGroups(pgs[i], pgs[j]) {
					// Merge j into i
					pgs[i].SourceTopics = slices.Compact(append(pgs[i].SourceTopics, pgs[j].SourceTopics...))
					pgs[i].ProcessorNames = slices.Compact(append(pgs[i].ProcessorNames, pgs[j].ProcessorNames...))
					pgs[i].StoreNames = slices.Compact(append(pgs[i].StoreNames, pgs[j].StoreNames...))

					// Sort fields for determinism
					slices.Sort(pgs[i].SourceTopics)
					slices.Sort(pgs[i].ProcessorNames)
					slices.Sort(pgs[i].StoreNames)

					// Remove j
					pgs = slices.Delete(pgs, j, j+1)
					changed = true
					break
				}
			}
			if changed {
				break
			}
		}
	}

	return pgs
}

// shouldMergeGroups returns true if two partition groups should be merged
func shouldMergeGroups(a, b *PartitionGroup) bool {
	return containsAny(a.SourceTopics, b.SourceTopics) ||
		containsAny(a.ProcessorNames, b.ProcessorNames) ||
		containsAny(a.StoreNames, b.StoreNames)
}

// containsAny reports whether any element in 'check' is present in 's'
func containsAny[E comparable](s []E, check []E) bool {
	for _, item := range s {
		for _, v := range check {
			if item == v {
				return true
			}
		}
	}
	return false
}
