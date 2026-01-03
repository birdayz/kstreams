package kserde

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestStringRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "simple string",
			input: "hello world",
		},
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "unicode characters",
			input: "Hello 世界 🌍",
		},
		{
			name:  "special characters",
			input: "!@#$%^&*()_+-=[]{}|;':\",./<>?",
		},
		{
			name:  "newlines and tabs",
			input: "line1\nline2\ttabbed",
		},
		{
			name:  "very long string",
			input: string(make([]byte, 10000)), // 10KB of null bytes
		},
		{
			name:  "json-like string",
			input: `{"key": "value", "number": 123}`,
		},
		{
			name:  "xml-like string",
			input: `<root><child>value</child></root>`,
		},
		{
			name:  "only spaces",
			input: "     ",
		},
		{
			name:  "only tabs",
			input: "\t\t\t",
		},
		{
			name:  "mixed whitespace",
			input: " \t\n\r ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized, err := StringSerializer(tt.input)
			assert.NoError(t, err)

			// Deserialize
			deserialized, err := StringDeserializer(serialized)
			assert.NoError(t, err)

			// Round-trip equality
			assert.Equal(t, tt.input, deserialized)
		})
	}
}

func TestStringSerializer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
	}{
		{
			name:     "simple",
			input:    "hello",
			expected: []byte("hello"),
		},
		{
			name:     "empty",
			input:    "",
			expected: []byte{},
		},
		{
			name:     "unicode",
			input:    "世界",
			expected: []byte("世界"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StringSerializer(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringDeserializer(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "simple",
			input:    []byte("hello"),
			expected: "hello",
		},
		{
			name:     "empty",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "nil bytes",
			input:    nil,
			expected: "",
		},
		{
			name:     "unicode",
			input:    []byte("世界"),
			expected: "世界",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StringDeserializer(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestStringSerdeDeterminism verifies that serialization is deterministic
func TestStringSerdeDeterminism(t *testing.T) {
	input := "test string"

	// Serialize multiple times
	results := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		serialized, err := StringSerializer(input)
		assert.NoError(t, err)
		results[i] = serialized
	}

	// All results should be identical
	for i := 1; i < len(results); i++ {
		assert.Equal(t, results[0], results[i], "serialization must be deterministic")
	}
}

// TestStringSerdeNilSafety verifies nil handling
func TestStringSerdeNilSafety(t *testing.T) {
	// Deserialize nil should not panic
	result, err := StringDeserializer(nil)
	assert.NoError(t, err)
	assert.Equal(t, "", result)

	// Deserialize empty should not panic
	result, err = StringDeserializer([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

// BenchmarkStringSerializer benchmarks string serialization
func BenchmarkStringSerializer(b *testing.B) {
	input := "benchmark test string with some length to it"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = StringSerializer(input)
	}
}

// BenchmarkStringDeserializer benchmarks string deserialization
func BenchmarkStringDeserializer(b *testing.B) {
	input := []byte("benchmark test string with some length to it")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = StringDeserializer(input)
	}
}

// BenchmarkStringRoundTrip benchmarks full round-trip
func BenchmarkStringRoundTrip(b *testing.B) {
	input := "benchmark test string"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serialized, _ := StringSerializer(input)
		_, _ = StringDeserializer(serialized)
	}
}
