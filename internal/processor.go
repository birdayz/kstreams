package internal

import "context"

type GenericProcessor[K any, V any] interface {
	Process(context.Context, K, V) error
}
