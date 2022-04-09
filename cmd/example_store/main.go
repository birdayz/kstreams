package main

import (
	"fmt"

	"github.com/birdayz/streamz/sdk"
	"github.com/birdayz/streamz/stores"
)

func main() {

	st, err := stores.NewPersistent("/tmp/stores", "my-store", 0)
	if err != nil {
		panic(err)
	}

	store := sdk.NewTypedStateStore(st, StringSerializer, StringSerializer, StringDeserializer, StringDeserializer)

	if err := store.Set("key-a", "value!"); err != nil {
		panic(err)
	}

	v, err := store.Get("key-a")
	if err != nil {
		panic(err)
	}

	fmt.Println("Got", v)

	st.Close()
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}
