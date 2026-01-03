module github.com/birdayz/kstreams/examples

go 1.24.0

require (
	github.com/birdayz/kstreams v0.0.0-20220410233832-8efff200c41e
	github.com/lmittmann/tint v1.0.3
)

require (
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go v1.20.6 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.17.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.12.0 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
)

replace github.com/birdayz/kstreams/kstate/pebble => ../kstate/pebble

replace github.com/birdayz/kstreams/kstate/s3 => ../kstate/s3

replace github.com/birdayz/kstreams => ../
