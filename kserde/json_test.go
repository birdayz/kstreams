package kserde

import (
	"math"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// Test types for JSON serialization
type SimpleStruct struct {
	Name  string
	Age   int
	Email string
}

type NestedStruct struct {
	ID    string
	User  SimpleStruct
	Tags  []string
	Meta  map[string]interface{}
}

type EdgeCaseStruct struct {
	IntValue     int
	FloatValue   float64
	BoolValue    bool
	NilPointer   *string
	EmptySlice   []string
	EmptyMap     map[string]string
	ZeroInt      int
	ZeroFloat    float64
	ZeroBool     bool
}

func TestJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name: "simple struct",
			input: SimpleStruct{
				Name:  "John Doe",
				Age:   30,
				Email: "john@example.com",
			},
		},
		{
			name: "nested struct",
			input: NestedStruct{
				ID: "123",
				User: SimpleStruct{
					Name:  "Jane",
					Age:   25,
					Email: "jane@example.com",
				},
				Tags: []string{"tag1", "tag2", "tag3"},
				Meta: map[string]interface{}{
					"key1": "value1",
					"key2": 42.0,
					"key3": true,
				},
			},
		},
		{
			name: "empty struct",
			input: SimpleStruct{},
		},
		{
			name: "edge cases",
			input: EdgeCaseStruct{
				IntValue:     123,
				FloatValue:   3.14159,
				BoolValue:    true,
				NilPointer:   nil,
				EmptySlice:   []string{},
				EmptyMap:     map[string]string{},
				ZeroInt:      0,
				ZeroFloat:    0.0,
				ZeroBool:     false,
			},
		},
		{
			name:  "string",
			input: "simple string",
		},
		{
			name:  "int",
			input: 42,
		},
		{
			name:  "float",
			input: 3.14159,
		},
		{
			name:  "bool true",
			input: true,
		},
		{
			name:  "bool false",
			input: false,
		},
		{
			name:  "slice of strings",
			input: []string{"a", "b", "c"},
		},
		{
			name: "map",
			input: map[string]int{
				"one":   1,
				"two":   2,
				"three": 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create serializer and deserializer for the input type
			switch v := tt.input.(type) {
			case SimpleStruct:
				testRoundTrip(t, v)
			case NestedStruct:
				testRoundTrip(t, v)
			case EdgeCaseStruct:
				testRoundTrip(t, v)
			case string:
				testRoundTrip(t, v)
			case int:
				testRoundTrip(t, v)
			case float64:
				testRoundTrip(t, v)
			case bool:
				testRoundTrip(t, v)
			case []string:
				testRoundTrip(t, v)
			case map[string]int:
				testRoundTrip(t, v)
			default:
				t.Fatalf("unsupported type: %T", v)
			}
		})
	}
}

// Helper function for round-trip testing
func testRoundTrip[T any](t *testing.T, input T) {
	serializer := JSONSerializer[T]()
	deserializer := JSONDeserializer[T]()

	// Serialize
	serialized, err := serializer(input)
	assert.NoError(t, err)

	// Deserialize
	deserialized, err := deserializer(serialized)
	assert.NoError(t, err)

	// Round-trip equality
	assert.Equal(t, input, deserialized)
}

func TestJSONSerializerEdgeCases(t *testing.T) {
	t.Run("special float values", func(t *testing.T) {
		type FloatStruct struct {
			Value float64
		}

		tests := []struct {
			name    string
			input   FloatStruct
			wantErr bool
		}{
			{
				name:    "NaN",
				input:   FloatStruct{Value: math.NaN()},
				wantErr: true, // JSON doesn't support NaN
			},
			{
				name:    "+Inf",
				input:   FloatStruct{Value: math.Inf(1)},
				wantErr: true, // JSON doesn't support Infinity
			},
			{
				name:    "-Inf",
				input:   FloatStruct{Value: math.Inf(-1)},
				wantErr: true, // JSON doesn't support -Infinity
			},
			{
				name:    "max float64",
				input:   FloatStruct{Value: math.MaxFloat64},
				wantErr: false,
			},
			{
				name:    "min float64",
				input:   FloatStruct{Value: -math.MaxFloat64},
				wantErr: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				serializer := JSONSerializer[FloatStruct]()
				_, err := serializer(tt.input)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("large int values", func(t *testing.T) {
		type IntStruct struct {
			Value int64
		}

		tests := []struct {
			name  string
			input IntStruct
		}{
			{
				name:  "max int64",
				input: IntStruct{Value: math.MaxInt64},
			},
			{
				name:  "min int64",
				input: IntStruct{Value: math.MinInt64},
			},
			{
				name:  "zero",
				input: IntStruct{Value: 0},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				testRoundTrip(t, tt.input)
			})
		}
	})
}

func TestJSONDeserializerErrors(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "invalid JSON",
			input: []byte("{invalid json}"),
		},
		{
			name:  "truncated JSON",
			input: []byte(`{"name": "John"`), // Missing closing brace
		},
		{
			name:  "wrong type",
			input: []byte(`"string"`), // Expecting struct, got string
		},
		{
			name:  "empty input",
			input: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deserializer := JSONDeserializer[SimpleStruct]()
			_, err := deserializer(tt.input)
			assert.Error(t, err)
		})
	}
}

func TestJSONSerdeDeterminism(t *testing.T) {
	input := SimpleStruct{
		Name:  "Test",
		Age:   25,
		Email: "test@example.com",
	}

	serializer := JSONSerializer[SimpleStruct]()

	// Serialize multiple times
	results := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		serialized, err := serializer(input)
		assert.NoError(t, err)
		results[i] = serialized
	}

	// All results should be identical (JSON encoding is deterministic for simple types)
	for i := 1; i < len(results); i++ {
		assert.Equal(t, results[0], results[i], "JSON serialization must be deterministic")
	}
}

func TestJSONSerdeNilSafety(t *testing.T) {
	t.Run("deserialize nil bytes", func(t *testing.T) {
		deserializer := JSONDeserializer[SimpleStruct]()
		_, err := deserializer(nil)
		assert.Error(t, err) // JSON unmarshaling nil should error
	})

	t.Run("deserialize empty bytes", func(t *testing.T) {
		deserializer := JSONDeserializer[SimpleStruct]()
		_, err := deserializer([]byte{})
		assert.Error(t, err) // Empty JSON should error
	})

	t.Run("nil pointer in struct", func(t *testing.T) {
		type StructWithPointer struct {
			Value *string
		}

		input := StructWithPointer{Value: nil}
		testRoundTrip(t, input)
	})
}

func TestJSONEmptyCollections(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "empty slice",
			input: []string{},
		},
		{
			name:  "empty map",
			input: map[string]string{},
		},
		{
			name: "struct with empty collections",
			input: struct {
				Slice []int
				Map   map[string]int
			}{
				Slice: []int{},
				Map:   map[string]int{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.input.(type) {
			case []string:
				testRoundTrip(t, v)
			case map[string]string:
				testRoundTrip(t, v)
			default:
				// Use reflection-based testing for anonymous struct
				serializer := JSONSerializer[struct {
					Slice []int
					Map   map[string]int
				}]()
				deserializer := JSONDeserializer[struct {
					Slice []int
					Map   map[string]int
				}]()

				input := v.(struct {
					Slice []int
					Map   map[string]int
				})

				serialized, err := serializer(input)
				assert.NoError(t, err)
				deserialized, err := deserializer(serialized)
				assert.NoError(t, err)
				assert.Equal(t, input, deserialized)
			}
		})
	}
}

func TestJSONWithSerDe(t *testing.T) {
	// Test the JSON() helper function
	serde := JSON[SimpleStruct]()

	input := SimpleStruct{
		Name:  "Test",
		Age:   30,
		Email: "test@example.com",
	}

	// Serialize
	serialized, err := serde.Serializer(input)
	assert.NoError(t, err)

	// Deserialize
	deserialized, err := serde.Deserializer(serialized)
	assert.NoError(t, err)

	// Verify
	assert.Equal(t, input, deserialized)
}

// Benchmarks
func BenchmarkJSONSerializerSimple(b *testing.B) {
	input := SimpleStruct{
		Name:  "John Doe",
		Age:   30,
		Email: "john@example.com",
	}
	serializer := JSONSerializer[SimpleStruct]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = serializer(input)
	}
}

func BenchmarkJSONDeserializerSimple(b *testing.B) {
	input := SimpleStruct{
		Name:  "John Doe",
		Age:   30,
		Email: "john@example.com",
	}
	serializer := JSONSerializer[SimpleStruct]()
	deserializer := JSONDeserializer[SimpleStruct]()

	serialized, _ := serializer(input)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = deserializer(serialized)
	}
}

func BenchmarkJSONRoundTripSimple(b *testing.B) {
	input := SimpleStruct{
		Name:  "John Doe",
		Age:   30,
		Email: "john@example.com",
	}
	serializer := JSONSerializer[SimpleStruct]()
	deserializer := JSONDeserializer[SimpleStruct]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serialized, _ := serializer(input)
		_, _ = deserializer(serialized)
	}
}

func BenchmarkJSONSerializerNested(b *testing.B) {
	input := NestedStruct{
		ID: "123",
		User: SimpleStruct{
			Name:  "Jane",
			Age:   25,
			Email: "jane@example.com",
		},
		Tags: []string{"tag1", "tag2", "tag3"},
		Meta: map[string]interface{}{
			"key1": "value1",
			"key2": 42.0,
		},
	}
	serializer := JSONSerializer[NestedStruct]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = serializer(input)
	}
}
