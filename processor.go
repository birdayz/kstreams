package streamz

func (s *Source[K, V]) Ingest(k []byte, v []byte) {
	key, _ := s.keyDeserializer(k)
	value, _ := s.valueDeserializer(v)
	s.forwardFunc(key, value)
}

type Source[K any, V any] struct {
	keyDeserializer   Deserializer[K]
	valueDeserializer Deserializer[V]

	forwardFunc ForwardFunc[K, V]
}

func NewSource[K any, V any](keyDeserializer Deserializer[K], valueDeserializer Deserializer[V]) *Source[K, V] {
	return &Source[K, V]{
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

type Sink[K any, V any] struct {
	keySerde   SerDe[K]
	valueSerde SerDe[V]
}

// TODO: add processorContext arg, which gives access to state stores
type ProcessFunc[K any, V any, OK any, OV any] func(K, V) (OK, OV, error)

type ForwardFunc[OK any, OV any] func(OK, OV)

type Processor[K any, V any, OK any, OV any] struct {
	processFunc ProcessFunc[K, V, OK, OV]
	forwardFunc ForwardFunc[OK, OV]
}

func NewProcessor[K, V, K2, V2 any](processFn ProcessFunc[K, V, K2, V2]) *Processor[K, V, K2, V2] {
	return &Processor[K, V, K2, V2]{
		processFunc: processFn,
	}
}

func (p *Processor[K, V, OK, OV]) Process(key K, value V) {
	outputKey, outputValue, _ := p.processFunc(key, value)
	if p.forwardFunc != nil {
		p.forwardFunc(outputKey, outputValue)
	}
}

func Link[K, V, K2, V2, K3, V3 any](prev *Processor[K, V, K2, V2], next *Processor[K2, V2, K3, V3]) {
	prev.forwardFunc = func(k K2, v V2) {
		next.Process(k, v)
	}

}
