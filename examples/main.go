package main

import "github.com/birdayz/kstreams"

func main() {
	bldr := kstreams.NewTopologyBuilder()
	topology := bldr.Build()

	_ = topology
	app := kstreams.New(topology, "my-sample-app")
	_ = app
}
