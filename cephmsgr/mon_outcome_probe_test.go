package cephmsgr

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

type monOutcomeSink struct {
	net.Conn
	partial bool
}

func (c monOutcomeSink) Write(p []byte) (int, error) {
	if c.partial {
		return len(p) / 2, io.ErrUnexpectedEOF
	}
	return len(p), nil
}

func TestMonOutcomeProbeBindsCRCRequestAndPartialWrite(t *testing.T) {
	message := msgr.CommandMessage(false, [16]byte{1}, []string{`{"prefix":"config-key set","key":"owned"}`}, []byte{0, 0xff})
	message.Transaction, message.Sequence = 19, 7
	var encoded bytes.Buffer
	if err := msgr.NewWriter(&encoded, 128<<10).Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
	for _, partial := range []bool{false, true} {
		probe := &monOutcomeWrites{}
		probe.enabled.Store(true)
		conn := &monOutcomeConn{Conn: monOutcomeSink{partial: partial}, record: probe, connection: 3, peer: "mon.b"}
		_, _ = conn.Write(encoded.Bytes())
		writes, err := probe.matching(message.Front, message.Data)
		if err != nil || len(writes) != 1 || writes[0].Transaction != 19 || writes[0].Sequence != 7 || writes[0].Connection != 3 || writes[0].Peer != "mon.b" || writes[0].Complete == partial {
			t.Fatal("CRC identity/partial-write observation", writes, err)
		}
		if other, _ := probe.matching(message.Front, []byte("other mutation")); len(other) != 0 {
			t.Fatal("different request data matched the original submission")
		}
	}
}

func TestMonOutcomeProbeRejectsCRCFrameCorruption(t *testing.T) {
	message := msgr.CommandMessage(false, [16]byte{1}, []string{`{"prefix":"status"}`}, nil)
	var encoded bytes.Buffer
	if err := msgr.NewWriter(&encoded, 128<<10).Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	// Corrupt an actual front byte, not an unused trailing-segment CRC slot.
	data[32+41+4] ^= 1
	probe := &monOutcomeWrites{}
	probe.enabled.Store(true)
	conn := &monOutcomeConn{Conn: monOutcomeSink{}, record: probe}
	_, _ = conn.Write(data)
	if writes, err := probe.matching(message.Front, message.Data); err == nil || len(writes) != 0 {
		t.Fatal("corrupt frame supplied trustworthy transaction evidence")
	}
}

// The embedded net.Conn only supplies unused methods; no socket is opened.
var _ net.Conn = monOutcomeSink{}

func TestMonOutcomeIdentityReceiptReadableAcrossFixtureUIDs(t *testing.T) {
	control := t.TempDir()
	identity := monOutcomeIdentity{OriginalGlobalID: 51, Submitted: monOutcomeWrite{
		Connection: 1, Peer: "mon.a", Transaction: 3, Sequence: 5, Complete: true}, OutcomeUnknown: true}
	writeMonOutcomeIdentity(t, control, identity)
	path := filepath.Join(control, "mon-command-outcome-identity.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0044 != 0044 {
		t.Fatal("non-secret identity metadata is unreadable across host/container UIDs", info, err)
	}
	data, err := os.ReadFile(path)
	var observed monOutcomeIdentity
	if err != nil || json.Unmarshal(data, &observed) != nil || observed.OriginalGlobalID != 51 || observed.Submitted.Transaction != 3 || !observed.OutcomeUnknown {
		t.Fatal("identity receipt lost original submission or unknown outcome", err)
	}
}
