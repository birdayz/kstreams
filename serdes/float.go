package serdes

import (
	"encoding/binary"
	"math"

	"github.com/birdayz/kstreams/sdk"
)

var Float64Deserializer = func(data []byte) (float64, error) {
	bytez := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bytez), nil
}

var Float64Serializer = func(data float64) ([]byte, error) {
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, math.Float64bits(data))
	return res, nil
}

var Float64 = sdk.SerDe[float64]{
	Serializer:   Float64Serializer,
	Deserializer: Float64Deserializer,
}
