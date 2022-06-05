# kstreams
kstreams is a stream processing library for go, inspired by kafka streams. This is currently under work, and a 
prototype is being written. Goals are still under work and not yet fully clear.

## Goals
- (Go 1.18 Generics-)Typed Processors, Stores, SerDes
- State stores
- Object storage backed state stores. Changelog topics as backup will be available as well. We encourage object storage for operational simplicity.
- At-least-once-delivery guarantee
- Minimal dependencies (little core deps except franz-go, specific stores and plugins like prometheus are separate)
- Add-ons available for popular stores (bigtable,redis,...) and data formats (proto, json, avro, msgpack, ...), Prometheus
- Emphasis on parallelism: Strong async and bulk processing support. Be MUCH better at this that kafka streams, and compete with parallel-consumer.
- Best developer experience. Be nice to use, but do not hide important details from the user
- Processors for windowing, including calendar based windowing (https://github.com/confluentinc/kafka-streams-examples/blob/21d3079eb8de91619cc873148655cdf607135846/src/test/java/io/confluent/examples/streams/window/CustomWindowTest.java#L192)
- Joins

## Non-Goals
- Exactly-once-delivery
- Kafka streams-like DSL is out of the question until Go generics support method type parameters.

## Next steps

### Object storage/S3 backend
- Minio testcontainer
- Tests S3
- Add GCS, generify some code so both s3 and gcs can make use of
		write-back cache or whatever we add to make it performant

### Windowing
- Generify, add aggregation processor, which outputs with
		Key=WindowKey[K], Value=Type of user, uses allows for "state" type
		which is used in store.
- Add support for calendar-based aggregations; i.e. properly aggregate
		time ranges that do not match UTC-day, or span days where daylight
		savings change the timezone.
- Performance: add benchmarks
- Cleanup. When are value deleted from db? Can we just delegate it to
		the store ? (Pebble TTL, cleanup policy of bucket)
- Add record timestamp to context

## Stores 
- How to allow read access to stores / make available via gRPC or other
		protocol

## General
- Explore tombstone handling, some things have to be improved probably
		so tombstones work "nicely".
