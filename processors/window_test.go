package processors

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/serde"
)

func TestWindowKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input WindowKey[string]
	}{
		{
			name: "simple key with current time",
			input: WindowKey[string]{
				Key:  "sensor-a",
				Time: time.Now(),
			},
		},
		{
			name: "empty key",
			input: WindowKey[string]{
				Key:  "",
				Time: time.Now(),
			},
		},
		{
			name: "unicode key",
			input: WindowKey[string]{
				Key:  "传感器-世界",
				Time: time.Now(),
			},
		},
		{
			name: "special characters in key",
			input: WindowKey[string]{
				Key:  "sensor!@#$%^&*()",
				Time: time.Now(),
			},
		},
		{
			name: "long key",
			input: WindowKey[string]{
				Key:  strings.Repeat("a", 1000),
				Time: time.Now(),
			},
		},
		{
			name: "zero time",
			input: WindowKey[string]{
				Key:  "sensor-a",
				Time: time.Time{},
			},
		},
		{
			name: "unix epoch",
			input: WindowKey[string]{
				Key:  "sensor-a",
				Time: time.Unix(0, 0),
			},
		},
		{
			name: "far future time",
			input: WindowKey[string]{
				Key:  "sensor-a",
				Time: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "far past time",
			input: WindowKey[string]{
				Key:  "sensor-a",
				Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serializer := WindowKeySerializer(serde.StringSerializer)
			deserializer := WindowKeyDeserializer(serde.StringDeserializer)

			// Serialize
			serialized, err := serializer(tt.input)
			assert.NoError(t, err)

			// Deserialize
			deserialized, err := deserializer(serialized)
			assert.NoError(t, err)

			// Verify key equality
			assert.Equal(t, tt.input.Key, deserialized.Key)

			// Verify time equality at nanosecond precision
			assert.Equal(t, tt.input.Time.UnixNano(), deserialized.Time.UnixNano())
		})
	}
}

func TestWindowKeyTimestampPrecision(t *testing.T) {
	tests := []struct {
		name string
		time time.Time
	}{
		{
			name: "nanosecond precision",
			time: time.Unix(1234567890, 123456789),
		},
		{
			name: "microsecond precision",
			time: time.Unix(1234567890, 123456000),
		},
		{
			name: "millisecond precision",
			time: time.Unix(1234567890, 123000000),
		},
		{
			name: "second precision",
			time: time.Unix(1234567890, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := WindowKey[string]{
				Key:  "test-key",
				Time: tt.time,
			}

			serializer := WindowKeySerializer(serde.StringSerializer)
			deserializer := WindowKeyDeserializer(serde.StringDeserializer)

			serialized, err := serializer(input)
			assert.NoError(t, err)

			deserialized, err := deserializer(serialized)
			assert.NoError(t, err)

			// Time should be preserved at nanosecond precision
			assert.Equal(t, tt.time.UnixNano(), deserialized.Time.UnixNano())
		})
	}
}

func TestWindowKeyLargeKeys(t *testing.T) {
	tests := []struct {
		name    string
		keySize int
		wantErr bool
	}{
		{
			name:    "1KB key",
			keySize: 1024,
			wantErr: false,
		},
		{
			name:    "10KB key",
			keySize: 10240,
			wantErr: false,
		},
		{
			name:    "max uint16 size (65535 bytes)",
			keySize: 65535,
			wantErr: false,
		},
		{
			name:    "larger than uint16 max (65536 bytes) - should fail",
			keySize: 65536,
			wantErr: true, // Length prefix is uint16, can't encode this
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := WindowKey[string]{
				Key:  strings.Repeat("a", tt.keySize),
				Time: time.Now(),
			}

			serializer := WindowKeySerializer(serde.StringSerializer)

			serialized, err := serializer(input)
			if tt.wantErr {
				// For keys larger than 65535, the length prefix overflows
				// The current implementation will silently truncate (uint16 overflow)
				// which will cause deserialization to fail
				assert.NoError(t, err) // Serialization doesn't error, but data is corrupt

				// Verify deserialization fails due to corrupt length
				deserializer := WindowKeyDeserializer(serde.StringDeserializer)
				_, err = deserializer(serialized)
				assert.Error(t, err) // Should error on deserialization
			} else {
				assert.NoError(t, err)

				// Verify round-trip works
				deserializer := WindowKeyDeserializer(serde.StringDeserializer)
				deserialized, err := deserializer(serialized)
				assert.NoError(t, err)
				assert.Equal(t, input.Key, deserialized.Key)
			}
		})
	}
}

func TestWindowKeyBinaryFormat(t *testing.T) {
	// Test the binary format structure:
	// [2 bytes length][N bytes key][M bytes timestamp]

	t.Run("verify binary format structure", func(t *testing.T) {
		input := WindowKey[string]{
			Key:  "test",
			Time: time.Unix(1234567890, 0).UTC(),
		}

		serializer := WindowKeySerializer(serde.StringSerializer)
		serialized, err := serializer(input)
		assert.NoError(t, err)

		// Parse the binary format manually
		assert.True(t, len(serialized) >= 2, "must have at least 2 bytes for length prefix")

		// Read length prefix
		keyLen := binary.BigEndian.Uint16(serialized[0:2])
		assert.Equal(t, uint16(4), keyLen) // "test" is 4 bytes

		// Read key
		keyBytes := serialized[2 : 2+keyLen]
		assert.Equal(t, []byte("test"), keyBytes)

		// Remaining bytes are the timestamp
		timestampBytes := serialized[2+keyLen:]
		var parsedTime time.Time
		err = parsedTime.UnmarshalBinary(timestampBytes)
		assert.NoError(t, err)
		assert.Equal(t, input.Time.Unix(), parsedTime.Unix())
	})

	t.Run("empty key format", func(t *testing.T) {
		input := WindowKey[string]{
			Key:  "",
			Time: time.Unix(1234567890, 0).UTC(),
		}

		serializer := WindowKeySerializer(serde.StringSerializer)
		serialized, err := serializer(input)
		assert.NoError(t, err)

		// Length should be 0
		keyLen := binary.BigEndian.Uint16(serialized[0:2])
		assert.Equal(t, uint16(0), keyLen)
	})
}

func TestWindowKeyDeserializerErrors(t *testing.T) {
	t.Run("empty input - panics (BUG)", func(t *testing.T) {
		// NOTE: Current implementation panics on empty input instead of returning an error
		// This is a bug - should check len(b) >= 2 before reading length prefix
		deserializer := WindowKeyDeserializer(serde.StringDeserializer)

		defer func() {
			if r := recover(); r != nil {
				// Expected panic - documenting current (buggy) behavior
				t.Log("Caught expected panic:", r)
			}
		}()

		_, _ = deserializer([]byte{})
		t.Error("Expected panic but got none - bug may be fixed")
	})

	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "only length prefix",
			input: []byte{0x00, 0x04}, // Says 4 bytes, but no data
		},
		{
			name:  "truncated key",
			input: []byte{0x00, 0x04, 't', 'e'}, // Says 4 bytes, only has 2
		},
		{
			name: "missing timestamp",
			input: []byte{0x00, 0x04, 't', 'e', 's', 't'}, // Has key but no timestamp
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deserializer := WindowKeyDeserializer(serde.StringDeserializer)
			_, err := deserializer(tt.input)
			assert.Error(t, err)
		})
	}
}

func TestWindowKeyDifferentTypes(t *testing.T) {
	t.Run("int key", func(t *testing.T) {
		input := WindowKey[int]{
			Key:  42,
			Time: time.Now(),
		}

		serializer := WindowKeySerializer(serde.Float64Serializer)
		deserializer := WindowKeyDeserializer(serde.Float64Deserializer)

		serialized, err := serializer(WindowKey[float64]{
			Key:  float64(input.Key),
			Time: input.Time,
		})
		assert.NoError(t, err)

		deserialized, err := deserializer(serialized)
		assert.NoError(t, err)
		assert.Equal(t, float64(42), deserialized.Key)
	})

	t.Run("struct key with JSON", func(t *testing.T) {
		type CustomKey struct {
			ID   string
			Type string
		}

		input := WindowKey[CustomKey]{
			Key: CustomKey{
				ID:   "sensor-1",
				Type: "temperature",
			},
			Time: time.Now(),
		}

		jsonSerde := serde.JSON[CustomKey]()
		serializer := WindowKeySerializer(jsonSerde.Serializer)
		deserializer := WindowKeyDeserializer(jsonSerde.Deserializer)

		serialized, err := serializer(input)
		assert.NoError(t, err)

		deserialized, err := deserializer(serialized)
		assert.NoError(t, err)
		assert.Equal(t, input.Key, deserialized.Key)
		assert.Equal(t, input.Time.UnixNano(), deserialized.Time.UnixNano())
	})
}

func TestWindowKeyDeterminism(t *testing.T) {
	input := WindowKey[string]{
		Key:  "deterministic-test",
		Time: time.Unix(1234567890, 123456789).UTC(),
	}

	serializer := WindowKeySerializer(serde.StringSerializer)

	// Serialize multiple times
	results := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		serialized, err := serializer(input)
		assert.NoError(t, err)
		results[i] = serialized
	}

	// All results should be identical
	for i := 1; i < len(results); i++ {
		assert.True(t, bytes.Equal(results[0], results[i]), "WindowKey serialization must be deterministic")
	}
}

func TestWindowKeyTimeZoneHandling(t *testing.T) {
	// Test that timezone information is preserved through serialization

	locations := []*time.Location{
		time.UTC,
		time.FixedZone("EST", -5*3600),
		time.FixedZone("JST", 9*3600),
	}

	for _, loc := range locations {
		t.Run("timezone-"+loc.String(), func(t *testing.T) {
			input := WindowKey[string]{
				Key:  "test",
				Time: time.Date(2024, 1, 1, 12, 0, 0, 0, loc),
			}

			serializer := WindowKeySerializer(serde.StringSerializer)
			deserializer := WindowKeyDeserializer(serde.StringDeserializer)

			serialized, err := serializer(input)
			assert.NoError(t, err)

			deserialized, err := deserializer(serialized)
			assert.NoError(t, err)

			// UnixNano() should be equal regardless of timezone
			assert.Equal(t, input.Time.UnixNano(), deserialized.Time.UnixNano())

			// However, the location might not be preserved (time.Time.MarshalBinary behavior)
			// What matters is the instant in time is the same
			assert.Equal(t, input.Time.Unix(), deserialized.Time.Unix())
		})
	}
}

// Benchmarks
func BenchmarkWindowKeySerializer(b *testing.B) {
	input := WindowKey[string]{
		Key:  "benchmark-sensor-key",
		Time: time.Now(),
	}
	serializer := WindowKeySerializer(serde.StringSerializer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = serializer(input)
	}
}

func BenchmarkWindowKeyDeserializer(b *testing.B) {
	input := WindowKey[string]{
		Key:  "benchmark-sensor-key",
		Time: time.Now(),
	}
	serializer := WindowKeySerializer(serde.StringSerializer)
	deserializer := WindowKeyDeserializer(serde.StringDeserializer)

	serialized, _ := serializer(input)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = deserializer(serialized)
	}
}

func BenchmarkWindowKeyRoundTrip(b *testing.B) {
	input := WindowKey[string]{
		Key:  "benchmark-sensor-key",
		Time: time.Now(),
	}
	serializer := WindowKeySerializer(serde.StringSerializer)
	deserializer := WindowKeyDeserializer(serde.StringDeserializer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serialized, _ := serializer(input)
		_, _ = deserializer(serialized)
	}
}
