package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestStatfsClusterAndPool(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, fsid))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	want := StatfsResult{Version: 41, KB: 1000, KBUsed: 300, KBAvail: 700, Objects: 12}
	got, err := c.Statfs(ctx, nil)
	if err != nil || got != want {
		t.Fatalf("cluster statfs %+v %v", got, err)
	}
	pool := int64(4)
	if got, err := c.Statfs(ctx, &pool); err != nil || got != want {
		t.Fatalf("pool statfs %+v %v", got, err)
	}
	negative := int64(-1)
	if _, err := c.Statfs(ctx, &negative); err == nil {
		t.Fatal("negative pool accepted")
	}
}

func TestStatfsEncodingAndReplyDecoding(t *testing.T) {
	pool := int64(0x0102030405060708)
	m := msgr.Statfs([16]byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}, &pool)
	want := append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0}, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6, 1, 8, 7, 6, 5, 4, 3, 2, 1)
	if m.Type != 13 || m.Version != 2 || m.CompatVersion != 1 || string(m.Front) != string(want) {
		t.Fatalf("statfs wire %+v", m)
	}
	cluster := msgr.Statfs([16]byte{}, nil)
	if len(cluster.Front) != 18+16+1 || cluster.Front[34] != 0 {
		t.Fatalf("cluster statfs wire %x", cluster.Front)
	}
	if _, err := msgr.DecodeStatfsReply(msgr.MessageData{Type: msgr.StatfsReplyMessage, Front: make([]byte, 55)}, 0); err == nil {
		t.Fatal("short reply accepted")
	}
	if _, err := msgr.DecodeStatfsReply(msgr.MessageData{Type: msgr.StatfsReplyMessage, Front: make([]byte, 57)}, 0); err == nil {
		t.Fatal("long reply accepted")
	}
	if _, err := msgr.DecodeStatfsReply(msgr.MessageData{Type: msgr.StatfsReplyMessage, Front: make([]byte, 56)}, 0); err != nil {
		t.Fatal(err)
	}
}

func TestStatfsIgnoredRequestIsAnUnknownOutcome(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, fsid))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// The MON has no error reply for a removed pool or missing capability: it
	// sends nothing, so the wait ends by its context as an unknown outcome.
	ignored := int64(0x7fffffffffffffff)
	waitCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	_, err = c.Statfs(waitCtx, &ignored)
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatal("an unanswered statfs is not an unknown outcome", err)
	}
	if _, err := c.Statfs(ctx, nil); err != nil {
		t.Fatal("the session did not survive", err)
	}
}
