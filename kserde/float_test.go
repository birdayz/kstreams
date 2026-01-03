package kserde

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestFloat64(t *testing.T) {
	input := 1337.13
	serialized, err := Float64Serializer(input)
	assert.NoError(t, err)
	deserialized, err := Float64Deserializer(serialized)
	assert.NoError(t, err)
	assert.Equal(t, input, deserialized)
}
