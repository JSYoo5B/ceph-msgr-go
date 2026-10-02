package msgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Assemble the pinned MMonCommandAck/MMgrCommandReply payload layout directly,
// rather than using CommandMessage or the command reply decoder as an oracle.
func commandReplyPayload(mon bool, code int32, status string, commands ...string) []byte {
	var front []byte
	if mon {
		front = make([]byte, 18) // PaxosServiceMessage
	}
	front = binary.LittleEndian.AppendUint32(front, uint32(code))
	front = binary.LittleEndian.AppendUint32(front, uint32(len(status)))
	front = append(front, status...)
	if mon {
		front = binary.LittleEndian.AppendUint32(front, uint32(len(commands)))
		for _, command := range commands {
			front = binary.LittleEndian.AppendUint32(front, uint32(len(command)))
			front = append(front, command...)
		}
	}
	return front
}

func TestCommandReplyUsesConfiguredFrameLimit(t *testing.T) {
	const frameLimit = 32 << 20
	for _, mon := range []bool{true, false} {
		name, typ := "MGR/status", MgrCommandReplyMessage
		status, commands := strings.Repeat("s", wire.DefaultLimit+1), []string(nil)
		if mon {
			name, typ = "MON/echoed-command", MonCommandReplyMessage
			// MMonCommandAck echoes the original command, including whitespace.
			status = "server rejected command"
			commands = []string{`{"prefix":"status"}` + strings.Repeat(" ", wire.DefaultLimit)}
		}
		t.Run(name, func(t *testing.T) {
			data := []byte{0, 0xff, 0, 1}
			m := MessageData{Type: typ, Version: 1, CompatVersion: 1, Front: commandReplyPayload(mon, -22, status, commands...), Data: data}
			var stream bytes.Buffer
			if err := NewWriter(&stream, frameLimit).Write(m.Frame()); err != nil {
				t.Fatal("reply should fit the configured frame limit", err)
			}
			f, err := NewReader(&stream, frameLimit).Read()
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeMessage(f)
			if err != nil {
				t.Fatal(err)
			}
			code, text, err := CommandReply(got, frameLimit)
			if err != nil || code != -22 || text != status || !bytes.Equal(got.Data, data) {
				t.Fatalf("valid bounded reply lost its server result: code=%d status-length=%d err=%v", code, len(text), err)
			}
			if _, _, err := CommandReply(got, wire.DefaultLimit); !errors.Is(err, wire.ErrLimit) {
				t.Fatal("smaller configured limit was ignored", err)
			}
		})
	}
}

func TestCommandReplyRejectsTruncatedEchoedCommand(t *testing.T) {
	front := commandReplyPayload(true, 0, "ok", "echo")
	m := MessageData{Type: MonCommandReplyMessage, Version: 1, Front: front[:len(front)-1]}
	if _, _, err := CommandReply(m, 32<<20); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("truncated command was accepted", err)
	}
	// A permitted frame size still bounds the nested command length.
	commandLengthOffset := len(front) - len("echo") - 4
	binary.LittleEndian.PutUint32(m.Front[commandLengthOffset:], (32<<20)+1)
	if _, _, err := CommandReply(m, 32<<20); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("oversized nested command was accepted", err)
	}
}
