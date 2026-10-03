package maps

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func osdMessageBytes(t testing.TB, name string) []byte {
	t.Helper()
	p, err := os.ReadFile("testdata/osd-message/" + name + ".hex")
	if err != nil {
		t.Fatal(err)
	}
	p, err = hex.DecodeString(strings.TrimSpace(string(p)))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func osdMessageFixture(t testing.TB, version uint16) msgr.MessageData {
	t.Helper()
	name, compat := "v1.front", uint16(1)
	if version == 4 {
		name, compat = "v4.front", 3
	}
	return msgr.MessageData{Type: osdMapMessage, Version: version, CompatVersion: compat, Front: osdMessageBytes(t, name)}
}

func TestDecodeNativeValidatedOSDMessages(t *testing.T) {
	for _, version := range []uint16{1, 4} {
		input := osdMessageFixture(t, version)
		r, err := DecodeOSDMessage(input, 0)
		if err != nil {
			t.Fatal(version, err)
		}
		var fsid [16]byte
		copy(fsid[:], []byte{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255})
		if r.FSID != fsid || len(r.FullMaps) != 1 || len(r.IncrementalMaps) != 2 || r.FullMaps[0].Epoch != 10 || r.IncrementalMaps[0].Epoch != 11 || r.IncrementalMaps[1].Epoch != 12 {
			t.Fatal("lost envelope metadata", version, r)
		}
		if version == 1 && (r.OldestMap != 0 || r.NewestMap != 0) || version == 4 && (r.OldestMap != 10 || r.NewestMap != 12) {
			t.Fatal("lost version-specific bounds", version, r)
		}
		if !bytes.Equal(r.RawFront, input.Front) || !bytes.Equal(r.FullMaps[0].Data, osdMessageBytes(t, "full")) || !bytes.Equal(r.IncrementalMaps[0].Data, osdMessageBytes(t, "incremental-11")) || !bytes.Equal(r.IncrementalMaps[1].Data, osdMessageBytes(t, "incremental-12")) {
			t.Fatal("opaque bytes differ from independently native-validated fixture", version)
		}
		input.Front[28] ^= 1
		if bytes.Equal(r.RawFront, input.Front) || !bytes.Equal(r.IncrementalMaps[0].Data, osdMessageBytes(t, "incremental-11")) {
			t.Fatal("decoded map borrowed mutable input")
		}
	}
}

func TestOSDMessageBoundsAndUnsupportedPayloads(t *testing.T) {
	for _, version := range []uint16{1, 4} {
		m := osdMessageFixture(t, version)
		limit := uint32(41 + len(m.Front))
		if _, err := DecodeOSDMessage(m, limit); err != nil {
			t.Fatal("exact complete message budget", version, err)
		}
		if _, err := DecodeOSDMessage(m, limit-1); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("Messenger header absent from budget", version, err)
		}
		for n := 0; n < len(m.Front); n++ {
			input := m
			input.Front = m.Front[:n]
			if _, err := DecodeOSDMessage(input, limit); err == nil {
				t.Fatal("accepted truncated native-validated envelope", version, n)
			}
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*msgr.MessageData)
		want error
	}{
		{"type", func(m *msgr.MessageData) { m.Type = 42 }, msgr.ErrFrame},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{0} }, msgr.ErrFrame},
		{"data", func(m *msgr.MessageData) { m.Data = []byte{0} }, msgr.ErrFrame},
		{"new version", func(m *msgr.MessageData) { m.Version = 5 }, wire.ErrVersion},
		{"unsupported branch", func(m *msgr.MessageData) { m.Version = 2; m.CompatVersion = 2 }, wire.ErrVersion},
		{"compat", func(m *msgr.MessageData) { m.CompatVersion = 4 }, wire.ErrVersion},
		{"empty fsid", func(m *msgr.MessageData) { clear(m.Front[:16]) }, msgr.ErrFrame},
		{"count overflow", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[16:20], math.MaxUint32) }, wire.ErrLimit},
		{"byte length overflow", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[24:28], math.MaxUint32) }, wire.ErrLimit},
		{"zero epoch", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[20:24], 0) }, msgr.ErrFrame},
		{"duplicate epoch", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[156:160], 11) }, msgr.ErrFrame},
		{"unordered epoch", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[156:160], 9) }, msgr.ErrFrame},
		{"inverted trim bounds", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[len(m.Front)-12:], 13) }, msgr.ErrFrame},
		{"removed snapshots", func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[len(m.Front)-4:], 1) }, wire.ErrVersion},
		{"trailing bytes", func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := osdMessageFixture(t, 4)
			tc.edit(&m)
			_, err := DecodeOSDMessage(m, wire.DefaultLimit)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
}

func TestOSDMessageAggregateCountAndOpaqueBlobs(t *testing.T) {
	e := wire.Encoder{}
	e.Raw([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	e.U32(MaxOSDMapBlobs)
	for epoch := uint32(1); epoch <= MaxOSDMapBlobs; epoch++ {
		e.U32(epoch)
		e.Bytes([]byte{255}) // Deliberately opaque, not an inner-map codec test.
	}
	e.U32(0)
	m := msgr.MessageData{Type: osdMapMessage, Version: 1, CompatVersion: 1, Front: e.Data}
	r, err := DecodeOSDMessage(m, wire.DefaultLimit)
	if err != nil || len(r.IncrementalMaps) != MaxOSDMapBlobs || !bytes.Equal(r.IncrementalMaps[0].Data, []byte{255}) {
		t.Fatal("opaque blobs or exact count limit", err)
	}
	// The independent total count limit also applies across both containers.
	binary.LittleEndian.PutUint32(e.Data[len(e.Data)-4:], 1)
	e.U32(MaxOSDMapBlobs + 1)
	e.Bytes([]byte{255})
	m.Front = e.Data
	if _, err := DecodeOSDMessage(m, wire.DefaultLimit); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate full+incremental count exceeded bound", err)
	}
}

func FuzzOSDMessage(f *testing.F) {
	for _, version := range []uint16{1, 4} {
		m := osdMessageFixture(f, version)
		f.Add(m.Front, m.Version, m.CompatVersion)
	}
	f.Fuzz(func(t *testing.T, front []byte, version, compat uint16) {
		r, err := DecodeOSDMessage(msgr.MessageData{Type: osdMapMessage, Version: version, CompatVersion: compat, Front: front}, 64*1024)
		if err != nil {
			return
		}
		if r.FSID == [16]byte{} || len(r.FullMaps)+len(r.IncrementalMaps) > MaxOSDMapBlobs || !bytes.Equal(r.RawFront, front) {
			t.Fatal("accepted envelope lost identity, bytes or count bound")
		}
		for _, blobs := range [][]OSDBlob{r.FullMaps, r.IncrementalMaps} {
			var previous uint32
			for _, blob := range blobs {
				if blob.Epoch <= previous || len(blob.Data) == 0 {
					t.Fatal("accepted envelope lost ordered nonempty blobs")
				}
				previous = blob.Epoch
			}
		}
	})
}
