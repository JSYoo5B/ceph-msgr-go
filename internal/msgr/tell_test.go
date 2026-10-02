package msgr

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// MCommand.h:47-56 encodes UUID bytes and vector<string> without Paxos fields.
// Message.h:120-121 and 340 use request97/reply98 and version1/compatibility0.
func TestTellCommandPinnedVector(t *testing.T) {
	fsid := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	front := []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		3, 0, 0, 0, // vector count
		1, 0, 0, 0, 'x', // byte length + first command
		2, 0, 0, 0, 0xc3, 0xa9, // UTF-8 byte length, not rune count
		0, 0, 0, 0, // empty final command
	}
	input := []byte{0, 0xff, 1, 0}
	m := TellCommand(fsid, []string{"x", "é", ""}, input)
	if TellCommandMessage != 97 || TellCommandReplyMessage != 98 || m.Type != 97 || m.Version != 1 || m.CompatVersion != 0 || m.Priority != 127 || m.Transaction != 0 || len(m.Middle) != 0 || !bytes.Equal(m.Front, front) || !bytes.Equal(m.Data, input) {
		t.Fatalf("tell request disagrees with pinned byte vector: %+v", m)
	}
	// TID is session-owned, distinct from both the command front and raw input.
	m.Sequence, m.Transaction, m.AckSequence = 5, 0x0807060504030201, 3
	header := []byte{
		5, 0, 0, 0, 0, 0, 0, 0,
		1, 2, 3, 4, 5, 6, 7, 8,
		97, 0, 127, 0, 1, 0,
		0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0,
	}
	f := m.Frame()
	if len(f.Segments) != 4 || !bytes.Equal(f.Segments[0], header) || !bytes.Equal(f.Segments[1], front) || len(f.Segments[2]) != 0 || !bytes.Equal(f.Segments[3], input) {
		t.Fatal("tell framing moved TID, front, or binary input")
	}
	var output bytes.Buffer
	const total = uint32(41 + 35 + 4) // fixed header, independently specified front, data
	if err := NewWriter(&output, total).Write(f); err != nil {
		t.Fatal("exact frame limit rejected tell input", err)
	}
	if err := NewWriter(io.Discard, total-1).Write(f); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("tell input exceeded its frame limit without rejection", err)
	}
}

func TestTellCommandEmptyVectorStillCarriesAuthenticatedFSID(t *testing.T) {
	fsid := [16]byte{1, 2, 3, 4}
	want := []byte{1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if m := TellCommand(fsid, nil, nil); !bytes.Equal(m.Front, want) || len(m.Data) != 0 {
		t.Fatal("empty vector omitted its count or changed the FSID", m)
	}
}

// Pinned Ceph 7f793731f1b39eb4f465e960113d2363c311b964:
// src/messages/MCommandReply.h:49-58 encodes int32 code + string status.
// MSG_COMMAND_REPLY=98, TID lives in Message's header, and raw output is data.
// This vector is independent of CommandMessage, TellCommand and wire.Encoder.
func TestTellReplyPinnedVector(t *testing.T) {
	header := []byte{
		9, 0, 0, 0, 0, 0, 0, 0, // sequence
		1, 2, 3, 4, 5, 6, 7, 8, // transaction
		98, 0, 127, 0, 1, 0, // type, priority, version
		0, 0, 0, 0, 0, 0, // data offset and reserved
		0, 0, 0, 0, 0, 0, 0, 0, // ACK sequence
		1, 0, 0, 0, 0, // complete flag, compatibility, reserved
	}
	front := []byte{0xea, 0xff, 0xff, 0xff, 5, 0, 0, 0, 'b', 'a', 'd', 0, 0xff}
	data := []byte{0, 0xff, 0x7a, 0}
	m, err := DecodeMessage(Frame{Tag: Message, Segments: [][]byte{header, front, nil, data}})
	if err != nil || m.Transaction != 0x0807060504030201 || m.Sequence != 9 || m.Type != 98 || m.Version != 1 || m.CompatVersion != 0 {
		t.Fatalf("pinned reply header lost identity: message=%+v err=%v", m, err)
	}
	code, status, err := CommandReply(m, 1024)
	if err != nil || code != -22 || status != "bad\x00\xff" || !bytes.Equal(m.Data, data) {
		t.Fatalf("pinned tell reply lost signed/raw result: code=%d status=%q data=%x err=%v", code, status, m.Data, err)
	}
}

func TestTellReplyRejectsMalformedBoundedFront(t *testing.T) {
	valid := []byte{0, 0, 0, 0, 2, 0, 0, 0, 'o', 'k'}
	for _, tc := range []struct {
		name   string
		front  []byte
		limit  uint32
		compat uint16
		cause  error
	}{
		{"truncated-code", valid[:3], 1024, 0, io.ErrUnexpectedEOF},
		{"truncated-status-count", valid[:7], 1024, 0, io.ErrUnexpectedEOF},
		{"truncated-status", valid[:9], 1024, 0, io.ErrUnexpectedEOF},
		{"configured-front-limit", valid, 9, 0, wire.ErrLimit},
		{"nested-status-limit", []byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, 1024, 0, wire.ErrLimit},
		{"future-compatibility", valid, 1024, 2, wire.ErrVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := MessageData{Type: 98, Version: 2, CompatVersion: tc.compat, Front: tc.front}
			if _, _, err := CommandReply(m, tc.limit); !errors.Is(err, tc.cause) {
				t.Fatalf("malformed tell reply accepted or misclassified: got=%v want=%v", err, tc.cause)
			}
		})
	}
	// MCommandReply has no echoed vector. Accepting an extra count would mask
	// the MON/MGR command-ack layout being accidentally used for local tell.
	trailing := append(append([]byte(nil), valid...), 0, 0, 0, 0)
	if _, _, err := CommandReply(MessageData{Type: 98, Front: trailing}, 1024); err == nil {
		t.Fatal("tell reply accepted an unexpected echoed-command count")
	}
	if _, _, err := CommandReply(MessageData{Type: 97, Front: valid}, 1024); !errors.Is(err, ErrFrame) {
		t.Fatal("request type was accepted as a reply", err)
	}
}

func TestTellReplyUsesConfiguredLimitAndKeepsRawData(t *testing.T) {
	// This request-independent front has a status longer than DefaultLimit.
	// Neither the wire decoder's default nor the code sign limits this string.
	status := strings.Repeat("s", wire.DefaultLimit+1)
	front := []byte{0, 0, 0, 0x80, 1, 0, 0, 1} // min int32; status length=16MiB+1
	front = append(front, status...)
	data := []byte{0xff, 0, 1, 0xff}
	m := MessageData{Type: 98, Version: 1, Front: front, Data: data}
	code, text, err := CommandReply(m, 32<<20)
	if err != nil || code != -2147483648 || text != status || !bytes.Equal(m.Data, data) {
		t.Fatalf("configured tell reply lost result: code=%d statusBytes=%d err=%v", code, len(text), err)
	}
	if _, _, err := CommandReply(m, wire.DefaultLimit); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("tell reply ignored the smaller configured limit", err)
	}
}
