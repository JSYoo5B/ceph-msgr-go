package wire

import (
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestEncodingFixture(t *testing.T) {
	// Direct little-endian field layout, independent of the decoder.
	e := Encoder{}
	e.U8(1)
	e.U16(0x2345)
	e.U32(0x6789abcd)
	e.U64(0x0123456789abcdef)
	e.String("mon")
	const want = "014523cdab8967efcdab8967452301030000006d6f6e"
	if hex.EncodeToString(e.Data) != want {
		t.Fatalf("got %x", e.Data)
	}
}

func TestMalformedLength(t *testing.T) {
	for _, tc := range []struct {
		input string
		limit uint32
		want  error
	}{
		{"ffffffff", DefaultLimit, ErrLimit},
		{"050000006162", DefaultLimit, io.ErrUnexpectedEOF},
		{"01000000", 3, ErrLimit},
	} {
		p, _ := hex.DecodeString(tc.input)
		d := NewDecoderLimit(p, tc.limit)
		d.Bytes()
		if !errors.Is(d.Err(), tc.want) {
			t.Fatalf("%s: %v", tc.input, d.Err())
		}
		before := d.Remaining()
		d.U64()
		if d.Remaining() != before {
			t.Fatal("read moved after error")
		}
	}
}

func TestVersionedEnvelope(t *testing.T) {
	p, _ := hex.DecodeString("030102000000aabb7f")
	d := NewDecoder(p)
	version, child := d.Struct(2)
	if version != 3 || child.U8() != 0xaa || d.U8() != 0x7f || d.Done() != nil {
		t.Fatal("future compatible envelope")
	}
	p[1] = 3
	d = NewDecoder(p)
	_, child = d.Struct(2)
	if !errors.Is(child.Err(), ErrVersion) || !errors.Is(d.Err(), ErrVersion) {
		t.Fatal("accepted incompatible encoding")
	}
}

func TestCollectionBounds(t *testing.T) {
	p, _ := hex.DecodeString("ffffffff")
	d := NewDecoder(p)
	if d.Count(4, 1024) != 0 || !errors.Is(d.Err(), ErrLimit) {
		t.Fatal("unbounded collection")
	}
}

func FuzzDecoder(f *testing.F) {
	f.Add([]byte{1, 1, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, p []byte) {
		d := NewDecoder(p)
		_, sub := d.Struct(3)
		sub.String()
		sub.Count(4, 1024)
		d.Bytes()
		d.Done()
	})
}
