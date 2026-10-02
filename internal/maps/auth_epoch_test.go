package maps

import (
	"errors"
	"io"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMonMapAuthenticationEpoch(t *testing.T) {
	front := monAuthFixture(20, 19)
	blob := wire.NewDecoder(front).Bytes()
	// Replace the fixture's empty v8/v9 routing metadata with nonempty sets
	// to verify the auth_epoch offset against Tentacle MonMap::encode.
	prefix := blob[6 : len(blob)-38]
	tail := wire.Encoder{}
	tail.U32(2)
	tail.U32(1)
	tail.U32(2)
	tail.U8(3)
	tail.U32(1)
	tail.String("c")
	tail.U8(1)
	tail.String("a")
	tail.U32(1)
	tail.String("b")
	authOffset := len(tail.Data)
	tail.U32(19)
	encode := func(version byte, suffix []byte) []byte {
		body := append(append([]byte(nil), prefix...), suffix...)
		mapBlob := wire.Encoder{}
		mapBlob.Struct(version, 6, body)
		message := wire.Encoder{}
		message.Bytes(mapBlob.Data)
		return message.Data
	}
	mon, err := DecodeMon(encode(10, tail.Data))
	if err != nil || mon.AuthEpoch != 19 || mon.Epoch != 7 {
		t.Fatal("Tentacle authentication epoch was lost", mon, err)
	}
	mon, err = DecodeMon(encode(9, tail.Data[:authOffset]))
	if err != nil || mon.AuthEpoch != 0 {
		t.Fatal("pre-epoch Tentacle map did not default to epoch zero", mon, err)
	}
	for n := 0; n < 4; n++ {
		if _, err := DecodeMon(encode(10, tail.Data[:authOffset+n])); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("complete map missing authentication epoch was accepted", n, err)
		}
	}
}
