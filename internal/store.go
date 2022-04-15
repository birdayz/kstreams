package internal

import "github.com/birdayz/streamz/sdk"

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder, name string) {
	t.stores[name] = storeBuilder
}
