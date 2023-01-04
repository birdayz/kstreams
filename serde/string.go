package serde

import "github.com/birdayz/kstreams"

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

var String = kstreams.SerDe[string]{
	Serializer:   StringSerializer,
	Deserializer: StringDeserializer,
}
