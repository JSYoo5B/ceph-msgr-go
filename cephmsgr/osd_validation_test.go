package cephmsgr

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func osdValidationFixture(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	read := func(suffix string) []byte {
		p, err := os.ReadFile("../internal/osd/testdata/" + name + suffix + ".hex")
		if err != nil {
			t.Fatal(err)
		}
		p, err = hex.DecodeString(strings.TrimSpace(string(p)))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Front: read(".front")}
	if name == "reply-v6-success" {
		m.Data = read(".data")
	}
	return m
}

func osdValidationRequest() OSDRequest {
	return OSDRequest{MapEpoch: 11, Object: OSDObject{Pool: 7, Name: "osd-codec", Key: "route-key", Namespace: "tenant", Hash: 0},
		Operations: []OSDOperation{{Code: OSDStat}, {Code: OSDRead, Offset: 2, Length: 4}}}
}

func TestOSDReplyValidationRetainsCompletionAndRejectsMismatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		edit    func(*msgr.MessageData)
		invalid bool
		want    error
	}{
		{name: "success", fixture: "reply-v6-success"},
		{name: "short read", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint64(m.Front[114:122], 2)
			binary.LittleEndian.PutUint32(m.Front[134:138], 2)
			m.Data = m.Data[:18]
		}},
		{name: "failed original read length", fixture: "reply-v6-enoent", edit: func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint64(m.Front[114:122], 4)
		}, want: &OSDError{Code: -2}},
		{name: "unexecuted stat in failed vector", fixture: "reply-v6-enoent", edit: func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint32(m.Front[142:146], 0)
		}, want: &OSDError{Code: -2}},
		{name: "redirect retains identity", fixture: "reply-v6-redirect", want: ErrOSDRedirect},
		{name: "redirect wrong object", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) { m.Front[4] ^= 1 }, invalid: true},
		{name: "redirect wrong pool", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[14:22], 8) }, invalid: true},
		{name: "redirect wrong raw hash", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[22:26], 1) }, invalid: true},
		{name: "redirect wrong attempt", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[138:142], 1) }, invalid: true},
		{name: "redirect wrong count", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) {
			front := append([]byte(nil), m.Front[:100]...)
			front = append(front, m.Front[138:146]...)
			m.Front = append(front, m.Front[150:]...)
			binary.LittleEndian.PutUint32(m.Front[58:62], 1)
		}, invalid: true},
		{name: "redirect wrong opcode", fixture: "reply-v6-redirect", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint16(m.Front[100:102], uint16(OSDStat)) }, invalid: true},
		{name: "read wrong offset", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[106:114], 3) }, invalid: true},
		{name: "read length too large", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[114:122], 5) }, invalid: true},
		{name: "read data too large", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint32(m.Front[134:138], 5)
			m.Data = append(m.Data, 0)
		}, invalid: true},
		{name: "read data differs from extent", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[114:122], 3) }, invalid: true},
		{name: "successful stat missing mtime", fixture: "reply-v6-success", edit: func(m *msgr.MessageData) {
			binary.LittleEndian.PutUint32(m.Front[96:100], 8)
			m.Data = append(m.Data[:8:8], m.Data[16:]...)
		}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type result struct {
				reply OSDReply
				err   error
			}
			resultCh := make(chan result, 1)
			go func() { reply, err := o.Request(ctx, osdValidationRequest()); resultCh <- result{reply, err} }()
			request := peer.next(t)
			m := osdValidationFixture(t, tc.fixture)
			if tc.edit != nil {
				tc.edit(&m)
			}
			peer.seq++
			m.Sequence, m.Transaction = peer.seq, request.Transaction
			if err := peer.writer.Write(m.Frame()); err != nil {
				t.Fatal(err)
			}
			got := <-resultCh
			if got.reply.Transaction != request.Transaction || len(got.reply.RawFront) == 0 {
				t.Fatal("reply identity or raw output was discarded", got.reply)
			}
			if tc.invalid {
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.Is(got.err, ErrOSDRedirect) {
					t.Fatal("invalid reply accepted as completed result", got.err)
				}
				if o.session.Err() == nil {
					t.Fatal("invalid reply did not fail the OSD session")
				}
			} else if tc.want == nil {
				if got.err != nil {
					t.Fatal(got.err)
				}
			} else if expected, ok := tc.want.(*OSDError); ok {
				var actual *OSDError
				if !errors.As(got.err, &actual) || actual.Code != expected.Code {
					t.Fatal("OSD error code was discarded", got.err)
				}
			} else if !errors.Is(got.err, tc.want) {
				t.Fatal(got.err)
			}
		})
	}
}
