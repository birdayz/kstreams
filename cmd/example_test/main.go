package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/birdayz/streamz"
)

func main() {
	first := streamz.NewProcessor(func(k, v string) (k2 string, v2 int, err error) {
		fmt.Println(k, v)
		number, err := strconv.Atoi(v)
		return k, number, err
	})

	second := streamz.NewProcessor(func(k string, v int) (k2 string, v2 int, err error) {
		return k, v * 2, nil
	})

	third := streamz.NewProcessor(func(k string, v int) (string, int, error) {
		fmt.Println(k, v)
		return k, v, nil
	})

	// TODO change return to a func call ?

	streamz.Link(first, second)
	streamz.Link(second, third)

	first.Process("first-device", "22")

	// topology.submit(Source, processor, .. processor, .. Sink)

	// --- Complex types --

	source := streamz.NewSource(StringDeserializer, ComplexValueDeserializer)

	processor := streamz.NewProcessor(func(k string, v ComplexValue) (string, ComplexValue, error) {
		fmt.Println("test", k, v)
		return k, v, nil
	})

	streamz.LinkSource(source, processor)

	source.Ingest([]byte("sensor-1"), []byte(`{"device_id":"abc","city":"NYC"}`))

}

type ComplexValue struct {
	DeviceID string `json:"device_id"`
	City     string `json:"city"`
}

var ComplexValueDeserializer = func(data []byte) (ComplexValue, error) {
	var parsed ComplexValue
	if err := json.Unmarshal(data, &parsed); err != nil {
		return ComplexValue{}, err
	}
	return parsed, nil
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}
