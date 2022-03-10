package streamz

import "github.com/birdayz/streamz/internal"

func MustAddProcessor[Kin, Vin, Kout, Vout any](t *internal.TopologyBuilder, p internal.ProcessorBuilder[Kin, Vin, Kout, Vout]) {
	internal.MustAddProcessor(t, p)
}
