package msgr

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// serviceMapShapeFront encodes the native ServiceMap layout with the given
// collection sizes, independently of the decoder: every service holds the
// same number of daemons and every daemon the same number of metadata and
// task-status entries. Names are distinct; value is every metadata value.
func serviceMapShapeFront(services, daemons, metadata, tasks int, value string) []byte {
	entries := func(e *wire.Encoder, prefix string, n int) {
		e.U32(uint32(n))
		for i := range n {
			e.String(fmt.Sprintf("%s%05x", prefix, i))
			e.String(value)
		}
	}
	daemon := wire.Encoder{}
	daemon.U64(4242)
	Address{}.Encode(&daemon)
	daemon.U32(9)
	daemon.U32(1700000000)
	daemon.U32(5)
	entries(&daemon, "m", metadata)
	entries(&daemon, "t", tasks)
	svc := wire.Encoder{}
	svc.U32(uint32(daemons))
	for i := range daemons {
		svc.String(fmt.Sprintf("d%05x", i))
		svc.Struct(2, 1, daemon.Data)
	}
	svc.String("")
	body := wire.Encoder{}
	body.U32(12)
	body.U32(1700000100)
	body.U32(7)
	body.U32(uint32(services))
	for i := range services {
		body.String(fmt.Sprintf("s%05x", i))
		body.Struct(1, 1, svc.Data)
	}
	e := wire.Encoder{}
	e.Struct(1, 1, body.Data)
	return e.Data
}

// Each ServiceMap collection admits exactly 65536 entries and refuses one
// more as a limit error. Native ServiceMap encoding has no such cap, so a
// native map above it is terminal for the decoding session, not truncated.
func TestDecodeServiceMapCollectionCaps(t *testing.T) {
	const limit = 1 << 16
	for name, shape := range map[string]func(n int) []byte{
		"services":    func(n int) []byte { return serviceMapShapeFront(n, 0, 0, 0, "") },
		"daemons":     func(n int) []byte { return serviceMapShapeFront(1, n, 0, 0, "") },
		"metadata":    func(n int) []byte { return serviceMapShapeFront(1, 1, n, 0, "") },
		"task status": func(n int) []byte { return serviceMapShapeFront(1, 1, 0, n, "") },
	} {
		t.Run(name, func(t *testing.T) {
			admitted, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: shape(limit)}, wire.DefaultLimit)
			if err != nil {
				t.Fatal("collection at its cap refused", err)
			}
			counts := map[string]int{"services": len(admitted.Services)}
			for _, s := range admitted.Services {
				counts["daemons"] = len(s.Daemons)
				for _, d := range s.Daemons {
					counts["metadata"], counts["task status"] = len(d.Metadata), len(d.TaskStatus)
				}
				break
			}
			if counts[name] != limit {
				t.Fatal("collection at its cap lost entries", counts)
			}
			over, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: shape(limit + 1)}, wire.DefaultLimit)
			if !errors.Is(err, wire.ErrLimit) || !errors.Is(err, ErrFrame) || over.Raw != nil || over.Services != nil {
				t.Fatal("collection above its cap was not a limit refusal with an empty result", err, len(over.Raw))
			}
		})
	}
}

// The frame bound counts the front plus 41 bytes of message header. A map at
// the bound is admitted whole; one byte more is refused before decoding.
func TestDecodeServiceMapFrameBound(t *testing.T) {
	front := serviceMapShapeFront(1, 1, 1, 0, string(bytes.Repeat([]byte{'v'}, 200<<10)))
	m := MessageData{Type: ServiceMapMessage, Front: front}
	limit := uint32(len(front)) + 41
	got, err := DecodeServiceMap(m, limit)
	if err != nil || len(got.Raw) != len(front) {
		t.Fatal("map at the frame bound refused", err)
	}
	if over, err := DecodeServiceMap(m, limit-1); !errors.Is(err, wire.ErrLimit) || !errors.Is(err, ErrFrame) || over.Raw != nil {
		t.Fatal("map above the frame bound was not a limit refusal", err)
	}
}

// Raw metadata, the daemon GID and the raw map are preserved byte for byte,
// including values that are not text, and the result shares no memory with
// the received frame.
func TestDecodeServiceMapPreservesRawMetadataAndOwnsItsBytes(t *testing.T) {
	value := string(append(bytes.Repeat([]byte{0, 0xff, 'x', '\n'}, 64<<10), 0x80))
	front := serviceMapShapeFront(2, 3, 4, 2, value)
	original := bytes.Clone(front)
	got, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: front}, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	for i := range front {
		front[i] ^= 0xa5
	}
	if !bytes.Equal(got.Raw, original) {
		t.Fatal("raw map aliases the received frame")
	}
	if len(got.Services) != 2 {
		t.Fatal("services", len(got.Services))
	}
	for name, s := range got.Services {
		if len(s.Daemons) != 3 {
			t.Fatal("daemons", name, len(s.Daemons))
		}
		for daemonName, d := range s.Daemons {
			if d.GID != 4242 || d.StartEpoch != 9 || len(d.Metadata) != 4 || len(d.TaskStatus) != 2 {
				t.Fatal("daemon identity or collection sizes", name, daemonName, d.GID, len(d.Metadata), len(d.TaskStatus))
			}
			for key, v := range d.Metadata {
				if v != value {
					t.Fatal("metadata value changed", name, daemonName, key, len(v))
				}
			}
		}
	}
	again, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: got.Raw}, wire.DefaultLimit)
	if err != nil || again.Epoch != got.Epoch || len(again.Services) != 2 || !bytes.Equal(again.Raw, original) {
		t.Fatal("retained raw map does not decode to the same map", err)
	}
}
