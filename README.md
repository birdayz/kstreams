# Streamz
streamz is a stream processing library for go, inspired by kafka streams. This is currently under work, and a 
prototype is being written. Goals are still under work and not yet fully clear.

### The mission
## Goals
- (Go 1.18 Generics-)Typed Processors, Stores, SerDes
- State stores
- At-least-once-delivery guarantee
- Minimal dependencies (little core deps except franz-go, specific stores and plugins like prometheus are separate)
- Add-ons available for popular stores (bigtable,redis,...) and data formats (proto, json, avro, msgpack, ...)

### Non-Goals
- Exactly-once-delivery

### To be decided
- Log to logur or zerolog directly

## TODO

- [ ] AddProcessor no error returned
- [ ] SetParent -> no error, and type safe -> Give processor interface a Name func() string
- [ ] Simple processor which only takes a func
- [ ] Topology, lifecycle mgmt
- [ ] Commit, Flush
- [ ] Support Bulk Streams!
- [ ] Async support plan
- [ ] Context overhaul, logging, ...
- [ ] Prometheus support. -> Possibly in integration/plugin folder with own go.mod, tracing
- [ ] Fanout,dynamic routing/sink topics
- [ ] Change topology, incrementally - restart only parts => can be used to build higher level systems
- [ ] Commit periodically, commit in OnRevoke
