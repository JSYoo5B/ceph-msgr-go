package mds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

var allMDSFeatures = []byte{0xff, 0xff, 0xff, 0, 0, 0, 0, 0}

func TestClientRequestRoundTripAndHeadLayout(t *testing.T) {
	r := ClientRequest{OldestClientTID: 5, MDSMapEpoch: 9, Flags: RequestFlagWantDentry, Retry: 0x1ff, Forward: 2, Op: OpMkdir,
		CallerUID: 1000, CallerGID: 1001, Ino: 0, OwnerUID: 1000, OwnerGID: 1001,
		Path: FilePath{Ino: 1, Path: "a/b"}, Path2: FilePath{Path: "x"},
		Releases: []RequestRelease{{Ino: 0x10000000000, CapID: 7, Caps: 1, Wanted: 2, Seq: 3, IssueSeq: 4, MSeq: 5, DentrySeq: 6, DentryName: "b"}},
		Stamp:    Time{1700000000, 7}, GIDs: []uint64{1001, 1002}, AlternateName: []byte{0, 1}, FscryptAuth: []byte{9}}
	r.Args[0] = 0xed
	m, err := EncodeClientRequest(r, allMDSFeatures)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != ClientRequestMessage || m.Version != 6 || m.CompatVersion != 1 || m.Priority != 127 {
		t.Fatal("request envelope", m)
	}
	head := wire.NewDecoder(m.Front)
	if head.U16() != 3 || head.U64() != 5 || head.U32() != 9 || head.U32() != RequestFlagWantDentry || head.U8() != 0xff || head.U8() != 2 || head.U16() != 1 {
		t.Fatal("head prefix")
	}
	if got := len(m.Front); got < RequestHeadBytes {
		t.Fatal("short head", got)
	}
	got, err := DecodeClientRequest(m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, r) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, r)
	}
	if _, err := EncodeClientRequest(r, []byte{0xff, 0xff, 0x0b}); !errors.Is(err, ErrRequest) {
		t.Fatal("MDS without HAS_OWNER_UIDGID accepted", err)
	}
	if _, err := EncodeClientRequest(r, nil); !errors.Is(err, ErrRequest) {
		t.Fatal("MDS without 32-bit retry/forward accepted", err)
	}
}

func encodeTestInode(e *wire.Encoder, ino uint64, symlink string) {
	b := wire.Encoder{}
	b.U64(ino)
	b.U64(NoSnap)
	b.U32(0)
	b.U64(3)
	b.U64(1)
	b.U32(0x55) // cap
	b.U32(0)
	b.U64(9)
	b.U32(1)
	b.U32(0)
	b.U64(1)
	b.U8(CapFlagAuth)
	for _, v := range []uint32{4 << 20, 1, 4 << 20, 0, 0, 0, 2} {
		b.U32(v)
	}
	for range 3 {
		b.U32(1700000000)
		b.U32(1)
	}
	b.U32(0)
	b.U64(12)
	b.U64(0)
	b.U64(0)
	b.U32(1)
	b.U32(0o100644)
	b.U32(1000)
	b.U32(1000)
	b.U32(1)
	for range 5 {
		b.U64(0)
	}
	b.U32(0)
	b.U32(0)
	b.U32(1) // fragtree
	b.U32(0x01000000)
	b.U32(2)
	b.String(symlink)
	b.Raw([]byte{2, 0, 0, 0, 0, 0, 0, 0})
	x := wire.Encoder{}
	x.U32(1)
	x.String("user.a")
	x.Bytes([]byte{0, 1})
	b.Bytes(x.Data)
	b.U64(1)
	b.Bytes(nil)
	q := wire.Encoder{}
	q.U64(100)
	q.U64(10)
	b.Struct(1, 1, q.Data)
	b.String("ns")
	b.U32(1700000000)
	b.U32(2)
	b.U64(77)
	b.U32(^uint32(0))
	b.U64(0)
	b.U64(0)
	b.U32(1)
	b.String("k")
	b.String("v")
	b.U8(0)
	b.Bytes(nil)
	b.Bytes(nil)
	opt := wire.Encoder{}
	opt.U32(0)
	b.Struct(1, 1, opt.Data)
	e.Struct(8, 1, b.Data)
}

func TestDecodeTraceAndReaddir(t *testing.T) {
	e := wire.Encoder{}
	encodeTestInode(&e, 1, "")
	dir := wire.Encoder{}
	dir.U32(0)
	dir.U32(0)
	dir.U32(2)
	dir.U32(0)
	dir.U32(1)
	e.Struct(1, 1, dir.Data)
	e.String("file")
	lease := wire.Encoder{}
	lease.U16(1)
	lease.U32(30000)
	lease.U32(4)
	lease.Bytes(nil)
	e.Struct(2, 1, lease.Data)
	encodeTestInode(&e, 0x10000000001, "target")
	trace, err := DecodeTrace(e.Data, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if trace.DirInode.Ino != 1 || !reflect.DeepEqual(trace.Dir.Distribution, []int32{0, 1}) || trace.DentryName != "file" || trace.Lease.DurationMS != 30000 || trace.Target.Symlink != "target" {
		t.Fatal("trace", trace)
	}
	s := trace.Target
	if s.Cap.Caps != 0x55 || s.Cap.Flags != CapFlagAuth || s.Layout.Pool != 2 || s.Layout.PoolNamespace != "ns" || s.Size != 12 || s.Mode != 0o100644 || s.QuotaMaxBytes != 100 || s.DirPin != -1 || s.ChangeAttr != 77 || s.SnapMetadata["k"] != "v" || !reflect.DeepEqual(s.FragTree, []FragSplit{{0x01000000, 2}}) || s.DirHash != 2 {
		t.Fatalf("inode %+v", s)
	}
	xattrs, err := DecodeXattrs(s.Xattrs)
	if err != nil || !reflect.DeepEqual(xattrs, map[string][]byte{"user.a": {0, 1}}) {
		t.Fatal("xattrs", xattrs, err)
	}
	if _, err := DecodeTrace(e.Data, true, false); err == nil {
		t.Fatal("trailing target accepted")
	}
	if _, err := DecodeTrace(nil, false, false); err != nil {
		t.Fatal("empty trace", err)
	}
	extra := wire.Encoder{}
	extra.Struct(1, 1, dir.Data)
	extra.U32(1)
	extra.U16(ReaddirFragEnd | ReaddirFragComplete)
	extra.String("file")
	extra.Struct(2, 1, lease.Data)
	encodeTestInode(&extra, 0x10000000001, "")
	readdir, err := DecodeReaddir(extra.Data)
	if err != nil || len(readdir.Entries) != 1 || readdir.Flags != ReaddirFragEnd|ReaddirFragComplete || readdir.Entries[0].Name != "file" || readdir.Entries[0].Inode.Ino != 0x10000000001 {
		t.Fatal("readdir", readdir, err)
	}
	bad := wire.Encoder{}
	bad.Struct(1, 1, []byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0})
	if _, err := DecodeTrace(append(bad.Data, 0), false, false); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}

func TestDecodeReplyEnvelopeForwardAndSnapTrace(t *testing.T) {
	e := wire.Encoder{}
	e.U32(OpLookup)
	e.U32(uint32(-2 & 0xffffffff))
	e.U32(5)
	e.U8(1)
	e.U8(0)
	e.U8(1)
	e.Bytes([]byte{1})
	e.Bytes(nil)
	e.Bytes([]byte{2, 3})
	r, err := DecodeClientReply(msgr.MessageData{Type: ClientReplyMessage, Version: 1, CompatVersion: 1, Front: e.Data}, 0)
	if err != nil || r.Result != -2 || !r.Safe || r.IsDentry || !r.IsTarget || r.MDSMapEpoch != 5 || len(r.Trace) != 1 || r.Extra != nil || len(r.Snap) != 2 {
		t.Fatal("reply", r, err)
	}
	if _, err := DecodeClientReply(msgr.MessageData{Type: ClientReplyMessage, Version: 2, CompatVersion: 1, Front: e.Data}, 0); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("reply version", err)
	}
	f := wire.Encoder{}
	f.U32(1)
	f.U32(3)
	f.U8(1)
	fwd, err := DecodeRequestForward(msgr.MessageData{Type: ClientRequestForwardMessage, Version: 1, CompatVersion: 1, Front: f.Data}, 0)
	if err != nil || fwd != (RequestForward{DestinationMDS: 1, Forward: 3, MustResend: true}) {
		t.Fatal("forward", fwd, err)
	}
	snap := wire.Encoder{}
	for _, ino := range []uint64{0x10000000000, 1} {
		b := wire.Encoder{}
		b.U64(ino)
		b.U64(2)
		b.U64(1)
		b.U64(0)
		b.U64(3)
		b.U32(1)
		b.U32(0)
		b.U64(3)
		b.U32(1700000000)
		b.U32(0)
		b.U64(4)
		snap.Struct(1, 1, b.Data)
	}
	realms, err := DecodeSnapTrace(snap.Data)
	if err != nil || len(realms) != 2 || realms[0].Ino != 0x10000000000 || !reflect.DeepEqual(realms[0].Snaps, []uint64{3}) || realms[1].ChangeAttr != 4 {
		t.Fatal("snap trace", realms, err)
	}
	if _, err := DecodeSnapTrace(snap.Data[:len(snap.Data)-1]); err == nil {
		t.Fatal("truncated snap trace accepted")
	}
}

func FuzzDecodeTrace(f *testing.F) {
	e := wire.Encoder{}
	encodeTestInode(&e, 1, "x")
	f.Add(e.Data, false, true)
	f.Fuzz(func(t *testing.T, raw []byte, dentry, target bool) {
		DecodeTrace(raw, dentry, target)
		DecodeReaddir(raw)
		DecodeSnapTrace(raw)
		DecodeXattrs(raw)
	})
}
