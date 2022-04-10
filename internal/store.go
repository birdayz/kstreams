package internal

import "github.com/birdayz/streamz/sdk"

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder) {

	// TODO: check if store already exists?
	t.stores[storeBuilder.Name()] = storeBuilder
}
