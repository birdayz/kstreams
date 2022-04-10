package streamz

import (
	"github.com/birdayz/streamz/internal"
	"github.com/birdayz/streamz/sdk"
)

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder) {
	internal.RegisterStore(t, storeBuilder)

}
