package mds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestCapsRoundTripAllBodies(t *testing.T) {
	base := Caps{Ino: 0x10000000001, Realm: 1, CapID: 7, Seq: 2, IssueSeq: 1, Caps: 0x55, Wanted: 0x15, Dirty: 0, MigrateSeq: 3, SnapFollows: 4,
		UID: 1000, GID: 1001, Mode: 0o100644, NLink: 1, XattrVersion: 2, Size: 12, MaxSize: 4 << 20, TruncateSize: ^uint64(0), TruncateSeq: 1,
		MTime: Time{1700000000, 1}, ATime: Time{1700000000, 2}, CTime: Time{1700000000, 3}, Layout: FileLayout{StripeUnit: 4 << 20, StripeCount: 1, ObjectSize: 4 << 20, Pool: 2, PoolNamespace: "ns"},
		TimeWarpSeq: 1, SnapTrace: []byte{1, 2, 3}, Xattrs: []byte{0, 0, 0, 0}, Flock: []byte{9}, InlineVersion: ^uint64(0), OSDEpochBarrier: 17, OldestFlushTID: 5,
		CallerUID: 1000, CallerGID: 1001, BTime: Time{1600000000, 0}, ChangeAttr: 8, Flags: 1, Files: -1, Subdirs: -1, FscryptAuth: []byte{1}}
	for _, op := range []uint32{CapOpGrant, CapOpImport, CapOpExport, CapOpFlush} {
		c := base
		c.Op = op
		if op == CapOpImport || op == CapOpExport {
			c.Peer = CapPeer{CapID: 9, IssueSeq: 2, MSeq: 3, MDS: 1, Flags: 1}
		}
		if op == CapOpExport {
			c.Size, c.MaxSize, c.TruncateSize, c.TruncateSeq, c.TimeWarpSeq = 0, 0, 0, 0, 0
			c.MTime, c.ATime, c.CTime = Time{}, Time{}, Time{}
			c.Layout = FileLayout{PoolNamespace: "ns"}
		}
		m := EncodeCaps(c, 33)
		if m.Version != 12 || m.CompatVersion != 1 || m.Transaction != 33 || len(m.Middle) != 4 {
			t.Fatal("caps envelope", m)
		}
		if len(m.Front) < capsHeadBytes+capsBodyBytes {
			t.Fatal("short caps front")
		}
		got, err := DecodeCaps(m, 0)
		if err != nil {
			t.Fatal(op, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("op %d\n got %+v\nwant %+v", op, got, c)
		}
	}
	m := EncodeCaps(base, 0)
	m.Middle = nil
	if _, err := DecodeCaps(m, 0); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("xattr length must match middle", err)
	}
	m = EncodeCaps(base, 0)
	m.Version = 10
	if _, err := DecodeCaps(m, 0); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("old caps version accepted", err)
	}
	export := base
	export.Op = CapOpExport
	m = EncodeCaps(export, 0)
	m.Front[capsHeadBytes+capPeerBytes] = 1
	if _, err := DecodeCaps(m, 0); err == nil {
		t.Fatal("nonzero export padding accepted")
	}
}

func TestCapReleaseLeaseSnapQuota(t *testing.T) {
	items := []CapItem{{Ino: 1, CapID: 2, MigrateSeq: 3, IssueSeq: 4}, {Ino: 5, CapID: 6}}
	m, err := EncodeCapRelease(items, 9)
	if err != nil || m.Version != 2 || len(m.Front) != 4+2*capItemBytes+4 {
		t.Fatal(m, err)
	}
	gotItems, barrier, err := DecodeCapRelease(m, 0)
	if err != nil || barrier != 9 || !reflect.DeepEqual(gotItems, items) {
		t.Fatal(gotItems, barrier, err)
	}
	lease := Lease{Action: LeaseRevoke, Mask: 1, Ino: 1, First: 2, Last: NoSnap, Seq: 3, DurationMS: 0, DentryName: "f"}
	lm := EncodeLease(lease)
	if len(lm.Front) != leaseHeadBytes+5 {
		t.Fatal("lease size", len(lm.Front))
	}
	if got, err := DecodeLease(lm, 0); err != nil || got != lease {
		t.Fatal(got, err)
	}
	e := wire.Encoder{}
	e.U32(SnapOpSplit)
	e.U64(0x10000000002)
	e.U32(1)
	e.U32(1)
	e.U32(2)
	e.U64(0x10000000003)
	e.U64(0x10000000004)
	e.Raw([]byte{7, 8})
	snap, err := DecodeSnap(msgr.MessageData{Type: ClientSnapMessage, Version: 1, CompatVersion: 1, Front: e.Data}, 0)
	if err != nil || snap.Op != SnapOpSplit || !reflect.DeepEqual(snap.SplitInos, []uint64{0x10000000003}) || !reflect.DeepEqual(snap.Trace, []byte{7, 8}) {
		t.Fatal(snap, err)
	}
	if _, err := DecodeSnap(msgr.MessageData{Type: ClientSnapMessage, Version: 1, CompatVersion: 1, Front: e.Data[:len(e.Data)-1]}, 0); err == nil {
		t.Fatal("truncated snap accepted")
	}
	q := wire.Encoder{}
	q.U64(1)
	q.U32(1700000000)
	q.U32(0)
	q.U64(10)
	q.U64(2)
	q.U64(1)
	quota := wire.Encoder{}
	quota.U64(100)
	quota.U64(0)
	q.Struct(1, 1, quota.Data)
	got, err := DecodeQuota(msgr.MessageData{Type: ClientQuotaMessage, Version: 1, CompatVersion: 1, Front: q.Data}, 0)
	if err != nil || got.RBytes != 10 || got.MaxBytes != 100 || got.RFiles != 2 {
		t.Fatal(got, err)
	}
}

func FuzzDecodeCaps(f *testing.F) {
	m := EncodeCaps(Caps{Op: CapOpImport, SnapTrace: []byte{1}}, 0)
	f.Add(m.Front, m.Middle)
	f.Fuzz(func(t *testing.T, front, middle []byte) {
		DecodeCaps(msgr.MessageData{Type: ClientCapsMessage, Version: 12, CompatVersion: 1, Front: front, Middle: middle}, 1<<20)
		DecodeSnap(msgr.MessageData{Type: ClientSnapMessage, Version: 1, CompatVersion: 1, Front: front}, 1<<20)
		DecodeLease(msgr.MessageData{Type: ClientLeaseMessage, Version: 1, CompatVersion: 1, Front: front}, 1<<20)
	})
}
