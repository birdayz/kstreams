package main

import (
	"github.com/birdayz/streamz"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	sink := streamz.SinkNode[string, string]{}

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

	task.Process(&kgo.Record{
		Key:   []byte("test-key"),
		Value: []byte("test-value")})
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}
