package serdes

import "github.com/birdayz/streamz/sdk"

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

func NewString() sdk.SerDe[string] {
	return sdk.SerDe[string]{
		Serializer:   StringSerializer,
		Deserializer: StringDeserializer,
	}
}
