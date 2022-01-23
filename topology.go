package streamz

// "External" type, eg a Serializer

// vs requiring types to implement it, e.g. myType.Marshal() ([]byte,error) / myType.Unmarshal([]byte) error

type Topology struct {
	source *Source[any, any]
	sink   *Sink[any, any]
}

func NewTopology() *Topology {
	return &Topology{}
}

type Record[K any, V any] struct {
	Key   K
	Value V
}

type TopologyBuilder struct{}

// func NewTopologyBuilder() *TopologyBuilder

// create is called internally whenever a new instance of the topology is needed, i.e. for a newly assigned partition
func (b *TopologyBuilder) create() {
}
