package kstreams

type Store interface {
	Init() error
	Flush() error
	Close() error
}
