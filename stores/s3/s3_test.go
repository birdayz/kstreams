package s3

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestS3(t *testing.T) {
	// Skip until testcontainer is implemented. this works for now locally
	// only
	t.Skip()
	store, err := newS3Store("mystore", 0)
	assert.NoError(t, err)

	err = store.Set([]byte("my-key"), []byte("my-value"))
	assert.NoError(t, err)
}
