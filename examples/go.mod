module github.com/birdayz/kstreams/examples

go 1.19

require (
	github.com/birdayz/kstreams v0.0.0-20220410233832-8efff200c41e
	github.com/birdayz/kstreams/stores/pebble v0.0.0-20220410233832-8efff200c41e
	github.com/go-logr/zerologr v1.2.1
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/rs/zerolog v1.26.1
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/pebble v0.0.0-20221222183300-eb5e1039627d // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/go-logr/logr v1.2.4-0.20221203165057-4da5305ff29a // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.12.0 // indirect
	github.com/prometheus/client_model v0.2.1-0.20210607210712-147c58e9608a // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/twmb/franz-go v1.10.4 // indirect
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20220215213838-c67ef7e57058 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20220414153411-bcd21879b8fd // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.3.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/birdayz/kstreams/stores/pebble => ../stores/pebble

replace github.com/birdayz/kstreams/stores/s3 => ../stores/s3

replace github.com/birdayz/kstreams => ../

replace gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.0
