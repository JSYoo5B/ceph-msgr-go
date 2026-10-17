package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMapVersionRoundTripAndRefusals(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, fsid))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, kind := range []MapKind{OSDMapKind, MonMapKind} {
		got, err := c.MapVersion(ctx, kind)
		if err != nil || got != (MapVersionResult{Newest: 42, Oldest: 3}) {
			t.Fatal(kind, got, err)
		}
	}
	if _, err := c.MapVersion(ctx, "pgmap"); err == nil {
		t.Fatal("unsupported map kind sent")
	}
	waitCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	var unknown *OutcomeUnknownError
	if _, err := c.MapVersion(waitCtx, FSMapKind); !errors.As(err, &unknown) {
		t.Fatal("an unanswered request is not an unknown outcome", err)
	}
	if _, err := c.MapVersion(ctx, MDSMapKind); !errors.As(err, &unknown) {
		t.Fatal("a reply with another handle was accepted", err)
	}
}

func TestMapVersionCodec(t *testing.T) {
	m := msgr.MonGetVersion(0x0102030405060708, "osdmap")
	want := []byte{8, 7, 6, 5, 4, 3, 2, 1, 6, 0, 0, 0, 'o', 's', 'd', 'm', 'a', 'p'}
	if m.Type != 19 || m.Version != 1 || string(m.Front) != string(want) {
		t.Fatalf("request wire %+v", m)
	}
	e := wire.Encoder{}
	e.U64(9)
	e.U64(50)
	v1 := msgr.MessageData{Type: msgr.MonGetVersionReplyMessage, Version: 1, Front: e.Data}
	if r, err := msgr.DecodeMonGetVersionReply(v1, 0); err != nil || r.Handle != 9 || r.Newest != 50 || r.Oldest != 0 {
		t.Fatal("v1 reply", r, err)
	}
	e.U64(2)
	v2 := msgr.MessageData{Type: msgr.MonGetVersionReplyMessage, Version: 2, Front: e.Data}
	if r, err := msgr.DecodeMonGetVersionReply(v2, 0); err != nil || r.Oldest != 2 {
		t.Fatal("v2 reply", r, err)
	}
	v1.Front = e.Data // trailing oldest under a v1 header
	if _, err := msgr.DecodeMonGetVersionReply(v1, 0); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	v2.Version = 3
	if _, err := msgr.DecodeMonGetVersionReply(v2, 0); err == nil {
		t.Fatal("unknown version accepted")
	}
}
