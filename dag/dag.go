package kstreams

// Can be source, sink, processor, store.
// Source: no Process()
// Sink,Processor: Execute()
type Node struct {
}

type DAG struct {
	Sources []Node
}

func (d *DAG) Build() { // Creates use-able "thing" for worker.
}
