module github.com/birdayz/streamz/cmd/example_store

go 1.18

require (
	github.com/birdayz/streamz v0.0.0-20220407000846-92fbed7c989e
	github.com/birdayz/streamz/stores v0.0.0-20220407000846-92fbed7c989e
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/pebble v0.0.0-20220408205130-c55c1d80c374 // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)

replace github.com/birdayz/streamz/stores => ../../stores

replace github.com/birdayz/streamz => ../../