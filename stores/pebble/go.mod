module github.com/birdayz/kstreams/stores/pebble

go 1.18

require (
	github.com/birdayz/kstreams v0.0.0-20220407000846-92fbed7c989e
	github.com/cockroachdb/pebble v0.0.0-20220506213004-f8897076324b
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/twmb/franz-go v1.4.3-0.20220501054214-8148c55adcd2 // indirect
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20220215213838-c67ef7e57058 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.1.0 // indirect
	github.com/twmb/go-rbtree v1.0.0 // indirect
	golang.org/x/exp v0.0.0-20220414153411-bcd21879b8fd // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/birdayz/kstreams => ../../

replace github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.2
