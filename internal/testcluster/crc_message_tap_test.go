package testcluster

import (
	"bytes"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func tapFrame(t *testing.T, f msgr.Frame) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := msgr.NewWriter(&out, 1<<20).Write(f); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestCRCMessageTapFragmentedFramesFilterAuthAndOwnSnapshots(t *testing.T) {
	tap := &CRCMessageTapConn{}
	auth := tapFrame(t, msgr.MessageData{Type: msgr.AuthMessage, Version: 1, Front: []byte("private-auth-frame-not-recorded")}.Frame())
	tap.capture("outgoing", auth, false)
	tap.Arm()
	request := msgr.MessageData{Type: 42, Version: 9, CompatVersion: 3, Transaction: 77, Front: []byte{0, 255, 128}, Data: []byte("fixture")}
	stream := append(auth, tapFrame(t, request.Frame())...)
	for len(stream) > 0 {
		n := 3
		if len(stream) < n {
			n = len(stream)
		}
		tap.capture("outgoing", stream[:n], true)
		stream = stream[n:]
	}
	reply := msgr.MessageData{Type: 43, Version: 8, CompatVersion: 2, Transaction: 77, Front: []byte{1}}
	tap.capture("incoming", tapFrame(t, reply.Frame()), true)
	records, err := tap.Snapshot()
	if err != nil || len(records) != 2 || records[0].Direction != "outgoing" || records[0].Message.Version != 9 || records[0].Message.Transaction != 77 || !bytes.Equal(records[0].Message.Front, request.Front) || records[1].Direction != "incoming" || records[1].Message.Version != 8 {
		t.Fatal("actual CRC frames or direction lost", len(records), err)
	}
	records[0].Message.Front[0] = 7
	fresh, _ := tap.Snapshot()
	if fresh[0].Message.Front[0] != 0 {
		t.Fatal("tap snapshot aliases owned frame")
	}
}
func TestCRCMessageTapRejectsCorruptOrUnboundedObservation(t *testing.T) {
	raw := tapFrame(t, msgr.MessageData{Type: 42, Version: 9, CompatVersion: 3, Front: []byte("fixture")}.Frame())
	raw[31] ^= 1 // Corrupt the preamble CRC, not an unused trailing segment CRC.
	tap := &CRCMessageTapConn{}
	tap.capture("outgoing", raw, true)
	if _, err := tap.Snapshot(); err == nil {
		t.Fatal("corrupt captured frame accepted")
	}
	tap = &CRCMessageTapConn{}
	tap.capture("outgoing", make([]byte, (1<<20)+1), true)
	if _, err := tap.Snapshot(); err == nil {
		t.Fatal("unbounded pending capture accepted")
	}
}
