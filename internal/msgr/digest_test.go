package msgr

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Two independent bufferlists, not a JSON parser or a product encoder round
// trip: MON status {"name":"a"}, then distinct opaque health bytes ff/NUL/LF.
const digestVector = `0c000000 7b226e616d65223a2261227d 03000000 ff000a`

func TestDigestSubscriptionIndependentVector(t *testing.T) {
	want := configLiteral(t, `
03000000
09000000 6d6772646967657374 0000000000000000 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00
00000000
`)
	message := DigestSubscribe("")
	if message.Type != 15 || message.Version != 3 || message.CompatVersion != 1 || message.Priority != 127 || message.Transaction != 0 || !bytes.Equal(message.Front, want) || len(message.Middle) != 0 || len(message.Data) != 0 {
		t.Fatal("digest subscription changed its continuous start, maps or empty hostname")
	}
}

func TestDecodeDigestIndependentVectorOwnsRawBuffers(t *testing.T) {
	front := configLiteral(t, digestVector)
	// Message's default version is 1; encode resolves default compatibility
	// zero to version 1. This literal Messenger header carries that wire form.
	header := configLiteral(t, `0000000000000000000000000000000005077f00010000000000000000000000000000000101000000`)
	message, err := DecodeMessage(Frame{Tag: Message, Segments: [][]byte{header, front}})
	if err != nil || message.Type != 0x705 || message.Version != 1 || message.CompatVersion != 1 {
		t.Fatal("native digest header changed", message, err)
	}
	digest, err := DecodeDigest(message, uint32(len(front)+41))
	if err != nil || !bytes.Equal(digest.MonStatus, []byte(`{"name":"a"}`)) || !bytes.Equal(digest.Health, []byte{0xff, 0, '\n'}) {
		t.Fatal("digest bufferlist order or raw bytes changed", digest, err)
	}
	clear(front)
	if !bytes.Equal(digest.MonStatus, []byte(`{"name":"a"}`)) || !bytes.Equal(digest.Health, []byte{0xff, 0, '\n'}) {
		t.Fatal("digest retained the Messenger receive buffer")
	}
	digest.MonStatus[0], digest.Health[0] = 'x', 'y'
	next, err := DecodeDigest(MessageData{Type: 0x705, Version: 1, CompatVersion: 1, Front: configLiteral(t, digestVector)}, 4096)
	if err != nil || !bytes.Equal(next.MonStatus, []byte(`{"name":"a"}`)) || !bytes.Equal(next.Health, []byte{0xff, 0, '\n'}) {
		t.Fatal("caller mutation changed another digest", next, err)
	}
	empty, err := DecodeDigest(MessageData{Type: 0x705, Version: 1, CompatVersion: 1, Front: configLiteral(t, `00000000 00000000`)}, 49)
	if err != nil || len(empty.MonStatus) != 0 || len(empty.Health) != 0 {
		t.Fatal("empty bufferlists acquired a JSON/schema requirement", empty, err)
	}
}

func TestDecodeDigestFrontContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		modify func(*MessageData)
		limit  uint32
		cause  error
	}{
		{"wrong message type", func(m *MessageData) { m.Type = 0x703 }, 4096, nil},
		{"unsupported compatibility", func(m *MessageData) { m.Version, m.CompatVersion = 2, 2 }, 4096, wire.ErrVersion},
		{"short second length", func(m *MessageData) { m.Front = m.Front[:19] }, 4096, io.ErrUnexpectedEOF},
		{"short second buffer", func(m *MessageData) { m.Front = m.Front[:len(m.Front)-1] }, 4096, io.ErrUnexpectedEOF},
		{"trailing bytes", func(m *MessageData) { m.Front = append(m.Front, 0) }, 4096, nil},
		{"unexpected middle", func(m *MessageData) { m.Middle = []byte{0} }, 4096, nil},
		{"unexpected data", func(m *MessageData) { m.Data = []byte{0} }, 4096, nil},
		{"logical frame limit", func(m *MessageData) {}, 63, wire.ErrLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := MessageData{Type: 0x705, Version: 1, CompatVersion: 1, Front: configLiteral(t, digestVector)}
			test.modify(&message)
			digest, err := DecodeDigest(message, test.limit)
			if digest.MonStatus != nil || digest.Health != nil || !errors.Is(err, ErrFrame) || (test.cause != nil && !errors.Is(err, test.cause)) {
				t.Fatal("invalid front exposed a partial digest or lost its wire cause", digest, err)
			}
		})
	}
}
