package serdes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/birdayz/kstreams/sdk"
)

// TODO: consider making these internal again. this representation, is it useful
// for anybody?
func WindowKeySerializer[K any](serializer sdk.Serializer[K]) sdk.Serializer[sdk.WindowKey[K]] {
	return func(wk sdk.WindowKey[K]) ([]byte, error) {
		buf := bytes.NewBuffer(nil)

		// It might be interesting, if serializers are not just functions but
		// interfaces, and can optionally implement "MarshalTo", which directly
		// writes to an io.Writer.
		serializedKey, err := serializer(wk.Key)
		if err != nil {
			return nil, err
		}

		lnPrefix := make([]byte, 2)
		binary.BigEndian.PutUint16(lnPrefix, uint16(len(serializedKey))) // TODO careful
		if _, err := buf.Write(lnPrefix); err != nil {
			return nil, err
		}

		if _, err := buf.Write(serializedKey); err != nil {
			return nil, err
		}

		ts, err := wk.Time.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(ts); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}
}

func WindowKeyDeserializer[K any](deserializer sdk.Deserializer[K]) sdk.Deserializer[sdk.WindowKey[K]] {
	return func(b []byte) (key sdk.WindowKey[K], err error) {
		length := binary.BigEndian.Uint16(b)
		if len(b) < int(length)+1+8 {
			return sdk.WindowKey[K]{}, fmt.Errorf("eof")
		}

		b = b[2:]

		deserialized, err := deserializer(b[:length])
		if err != nil {
			return sdk.WindowKey[K]{}, err
		}

		b = b[length:]

		var t time.Time
		err = t.UnmarshalBinary(b)
		if err != nil {
			return sdk.WindowKey[K]{}, err
		}

		return sdk.WindowKey[K]{
			Key:  deserialized,
			Time: t,
		}, nil
	}
}
