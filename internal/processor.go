package internal

type GenericProcessor[K any, V any] interface {
	Process(K, V) error
}

type Processor[Kin any, Vin any, Kout any, Vout any] interface {
	Process(ctx Context[Kout, Vout], k Kin, v Vin) error
}

type ProcessorBuilder[Kin any, Vin any, Kout any, Vout any] interface {
	Name() string
	Build() Processor[Kin, Vin, Kout, Vout]
}

type SimpleProcessorBuilder[Kin any, Vin any, Kout any, Vout any] struct {
	name      string
	buildFunc func() Processor[Kin, Vin, Kout, Vout]
}

func (s *SimpleProcessorBuilder[Kin, Vin, Kout, Vout]) Build() Processor[Kin, Vin, Kout, Vout] {
	return s.buildFunc()
}

func (s *SimpleProcessorBuilder[Kin, Vin, Kout, Vout]) Name() string {
	return s.name
}

func NewProcessor[Kin any, Vin any, Kout any, Vout any](name string, buildFunc func() Processor[Kin, Vin, Kout, Vout]) ProcessorBuilder[Kin, Vin, Kout, Vout] {
	return &SimpleProcessorBuilder[Kin, Vin, Kout, Vout]{
		name:      name,
		buildFunc: buildFunc,
	}
}
