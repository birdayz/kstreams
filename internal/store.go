package internal

import "github.com/birdayz/kstreams/sdk"

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder, name string) {
	t.stores[name] = storeBuilder
}
