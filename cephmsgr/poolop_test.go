package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestPoolOpSelfManagedAndPoolSnapshots(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seen []msgr.MessageData
	options := mockOptions(t, 20, fsid)
	dial := options.DialContext
	_ = dial
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = seen
	created, err := c.PoolOp(ctx, PoolOpRequest{Pool: 4, Op: SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	id, err := created.SnapshotID()
	if err != nil || id != 7 || created.Code != 0 || created.Epoch != 5 {
		t.Fatalf("self-managed create %+v %d %v", created, id, err)
	}
	if removed, err := c.PoolOp(ctx, PoolOpRequest{Pool: 4, Op: SelfManagedSnapshotRemove, Snapshot: 7}); err != nil || len(removed.Data) != 0 {
		t.Fatal("self-managed remove", removed, err)
	}
	if _, err := removedID(c, ctx); err == nil {
		t.Fatal("a reply without data decoded as a snapshot id")
	}
	if r, err := c.PoolOp(ctx, PoolOpRequest{Pool: 4, Op: PoolSnapshotCreate, Name: "snap-é"}); err != nil || r.Code != 0 {
		t.Fatal("pool snapshot create", r, err)
	}
	var server *PoolOpError
	r, err := c.PoolOp(ctx, PoolOpRequest{Pool: 4, Op: PoolSnapshotRemove, Name: "deny"})
	if !errors.As(err, &server) || server.Code != -1 || r.Code != -1 || r.Epoch != 5 {
		t.Fatal("known server refusal not preserved", r, err)
	}
}

func removedID(c *Client, ctx context.Context) (uint64, error) {
	r, err := c.PoolOp(ctx, PoolOpRequest{Pool: 4, Op: SelfManagedSnapshotRemove, Snapshot: 9})
	if err != nil {
		return 0, err
	}
	return r.SnapshotID()
}

func TestPoolOpWireAndRefusalsAndUnknownOutcome(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, fsid))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for name, request := range map[string]PoolOpRequest{
		"unknown op":            {Pool: 1, Op: 0x01},
		"snapshot without name": {Pool: 1, Op: PoolSnapshotCreate},
		"nul in name":           {Pool: 1, Op: PoolSnapshotCreate, Name: "a\x00b"},
		"oversized name":        {Pool: 1, Op: PoolSnapshotRemove, Name: string(make([]byte, MaxPoolSnapshotName+1))},
		"name on unmanaged":     {Pool: 1, Op: SelfManagedSnapshotCreate, Name: "x"},
		"id on create":          {Pool: 1, Op: SelfManagedSnapshotCreate, Snapshot: 3},
		"remove without id":     {Pool: 1, Op: SelfManagedSnapshotRemove},
		"remove snapdir":        {Pool: 1, Op: SelfManagedSnapshotRemove, Snapshot: ^uint64(0)},
		"id on pool snapshot":   {Pool: 1, Op: PoolSnapshotCreate, Name: "x", Snapshot: 1},
		"oversized pool id":     {Pool: 1 << 31, Op: SelfManagedSnapshotCreate},
	} {
		if _, err := c.PoolOp(ctx, request); !errors.Is(err, osd.ErrRequest) {
			t.Fatal("invalid pool op accepted", name, err)
		}
	}
	// "block" is never answered: the cancelled wait is an unknown outcome and
	// nothing is replayed.
	waitCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	_, err = c.PoolOp(waitCtx, PoolOpRequest{Pool: 1, Op: PoolSnapshotCreate, Name: "block"})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatal("unanswered pool op is not an unknown outcome", err)
	}
	if _, err := c.PoolOp(ctx, PoolOpRequest{Pool: 1, Op: SelfManagedSnapshotCreate}); err != nil {
		t.Fatal("the session did not survive a cancelled pool op", err)
	}
}

func TestPoolOpEncodingMatchesTheNativeLayout(t *testing.T) {
	m := msgr.PoolOp([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 0x01020304, msgr.PoolOpDeleteUnmanagedSnap, 0x1112131415161718, "né")
	want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0} // paxos version, session -1, tid
	want = append(want, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16) // fsid
	want = append(want, 4, 3, 2, 1)                                            // pool
	want = append(want, 0x22, 0, 0, 0)                                         // op
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 0)                                // auid
	want = append(want, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11)        // snapid
	want = append(want, 3, 0, 0, 0, 'n', 0xc3, 0xa9)                           // name
	want = append(want, 0, 0, 0)                                               // pad, crush_rule
	if m.Type != 49 || m.Version != 4 || m.CompatVersion != 2 || m.Priority != 127 || string(m.Front) != string(want) {
		t.Fatalf("pool op wire %+v", m)
	}
}
