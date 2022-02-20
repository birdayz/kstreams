package main

import (
	"github.com/birdayz/streamz"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	sink := streamz.SinkNode[string, string]{
		KeySerializer:   StringSerializer,
		ValueSerializer: StringSerializer,
	}

	p := streamz.ProcessorNode[string, string, string, string]{
		UserFunc: func(k, v string) (k2, v2 string, err error) {
			return k, v, nil
		},
		Next: &sink,
	}

	root := streamz.SourceNode[string, string]{
		KeyDeserializer:   StringDeserializer,
		ValueDeserializer: StringDeserializer,
		Next:              &p,
	}

	task := streamz.NewTask("abc", 0, &root)

	task.Process(&kgo.Record{Key: []byte(`abc`), Value: []byte(`def`)})

}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

type MyProcessor[K, V, K2, V2 string] struct{}

func (p *MyProcessor[Kin, Vin, Kout, Vout]) Process(ctx streamz.Context[Kout, Vout], k Kin, v Vin) error {
	return nil
}
