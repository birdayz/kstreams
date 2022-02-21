WIP: some experiments about go generics and implementing kafka streams

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
