package kstreams

// Node does not know about any specific types of nodes, because it would
// otherwise need to have an ounbounded number of generic types. Generic types
// are hidden inside the actual implementations using the Node interfaces.
type Node interface {
	Init() error
	Close() error
}
