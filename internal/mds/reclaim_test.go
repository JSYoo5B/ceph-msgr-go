package mds

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestEncodeReclaimMatchesTheNativeLayout(t *testing.T) {
	m, err := EncodeReclaim("uuid-é", ReclaimReset)
	if err != nil {
		t.Fatal(err)
	}
	// std::string (u32 length, bytes) then the u32 flags, header v1/compat 1.
	want := append([]byte{7, 0, 0, 0}, []byte("uuid-é")...)
	want = append(want, 1, 0, 0, 0)
	if m.Type != 27 || m.Version != 1 || m.CompatVersion != 1 || m.Priority != 127 || !bytes.Equal(m.Front, want) || len(m.Data) != 0 || len(m.Middle) != 0 {
		t.Fatalf("reclaim message %+v", m)
	}
	finish, err := EncodeReclaim("", ReclaimFinish)
	if err != nil || !bytes.Equal(finish.Front, []byte{0, 0, 0, 0, 0, 0, 0, 0x80}) {
		t.Fatalf("finish message %x %v", finish.Front, err)
	}
	for name, args := range map[string]struct {
		uuid  string
		flags uint32
	}{
		"empty uuid reset":     {"", ReclaimReset},
		"finish with uuid":     {"x", ReclaimFinish},
		"finish with reset":    {"", ReclaimFinish | ReclaimReset},
		"no flags":             {"x", 0},
		"unknown flag":         {"x", 2},
		"oversized uuid reset": {string(make([]byte, MaxReclaimUUID+1)), ReclaimReset},
	} {
		if _, err := EncodeReclaim(args.uuid, args.flags); !errors.Is(err, ErrSession) {
			t.Fatal("invalid reclaim accepted", name, err)
		}
	}
}

func reclaimReplyMessage(result int32, epoch uint32, addrs []msgr.Address) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(uint32(result))
	e.U32(epoch)
	msgr.EncodeAddresses(&e, addrs)
	return msgr.MessageData{Type: ClientReclaimReplyMessage, Version: 1, CompatVersion: 1, Front: e.Data}
}

func TestDecodeReclaimReply(t *testing.T) {
	addr := msgr.Address{Type: 2, Nonce: 99, Endpoint: netip.MustParseAddrPort("192.0.2.9:0")}
	got, err := DecodeReclaimReply(reclaimReplyMessage(0, 41, []msgr.Address{addr}), 0)
	if err != nil || got.Result != 0 || got.Epoch != 41 || len(got.Addresses) != 1 || got.Addresses[0] != addr {
		t.Fatalf("reply %+v %v", got, err)
	}
	empty, err := DecodeReclaimReply(reclaimReplyMessage(-22, 0, nil), 0)
	if err != nil || empty.Result != -22 || len(empty.Addresses) != 0 {
		t.Fatalf("empty reply %+v %v", empty, err)
	}
	bad := reclaimReplyMessage(0, 1, nil)
	bad.Front = append(bad.Front, 0)
	wrongType := reclaimReplyMessage(0, 1, nil)
	wrongType.Type = ClientSessionMessage
	wrongVersion := reclaimReplyMessage(0, 1, nil)
	wrongVersion.Version = 2
	middle := reclaimReplyMessage(0, 1, nil)
	middle.Middle = []byte{1}
	truncated := reclaimReplyMessage(0, 1, nil)
	truncated.Front = truncated.Front[:6]
	for name, m := range map[string]msgr.MessageData{"trailing": bad, "type": wrongType, "version": wrongVersion, "middle": middle, "truncated": truncated} {
		if _, err := DecodeReclaimReply(m, 0); err == nil {
			t.Fatal("malformed reply accepted", name)
		}
	}
	if _, err := DecodeReclaimReply(reclaimReplyMessage(0, 1, nil), 20); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("frame limit not applied", err)
	}
}
