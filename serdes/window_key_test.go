package serdes

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/sdk"
)

func TestWindow(t *testing.T) {
	serializer := WindowKeySerializer(StringSerializer)
	deserializer := WindowKeyDeserializer(StringDeserializer)

	input := sdk.WindowKey[string]{
		Key:  "sensor-a",
		Time: time.Now(),
	}
	serialized, err := serializer(input)
	assert.NoError(t, err)

	deserialized, err := deserializer(serialized)
	assert.NoError(t, err)
	assert.Equal(t, input.Key, deserialized.Key)
	assert.Equal(t, input.Time.UnixNano(), deserialized.Time.UnixNano())

}
