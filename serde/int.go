package serde

import (
	"encoding/binary"

	"github.com/birdayz/kstreams"
)

// Int64Serializer serializes int64 to big-endian bytes
var Int64Serializer = func(data int64) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(data))
	return buf, nil
}

// Int64Deserializer deserializes big-endian bytes to int64
var Int64Deserializer = func(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, nil // Return 0 for empty/invalid data
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// Int64 is a SerDe for int64 values
var Int64 = kstreams.SerDe[int64]{
	Serializer:   Int64Serializer,
	Deserializer: Int64Deserializer,
}

// Int32Serializer serializes int32 to big-endian bytes
var Int32Serializer = func(data int32) ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(data))
	return buf, nil
}

// Int32Deserializer deserializes big-endian bytes to int32
var Int32Deserializer = func(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, nil
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

// Int32 is a SerDe for int32 values
var Int32 = kstreams.SerDe[int32]{
	Serializer:   Int32Serializer,
	Deserializer: Int32Deserializer,
}
