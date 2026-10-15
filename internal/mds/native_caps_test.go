package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Fresh native Tentacle MClientCaps, MClientCapRelease, MClientLease,
// MClientSnap and MClientQuota vectors produced by
// tools/mds_caps_native_encoder.cc; see mds-caps-native-provenance.json.
// Every vector is a canonical native Message::encode of a fresh object with
// the FLOCK, EXPORT_PEER and MDS_INLINE_DATA features (MClientCaps v12 keeps
// its xattr blob in the middle segment). The receipts are what separate
// default-constructed native objects decoded from those segments.

const nativeCapsFlushTID = 77

type nativeLayoutReceipt struct {
	StripeUnit  uint32 `json:"stripe_unit"`
	StripeCount uint32 `json:"stripe_count"`
	ObjectSize  uint32 `json:"object_size"`
	PoolID      int64  `json:"pool_id"`
	PoolNS      string `json:"pool_ns"`
}

type nativeCapPeerReceipt struct {
	CapID    uint64 `json:"cap_id"`
	IssueSeq uint32 `json:"issue_seq"`
	MSeq     uint32 `json:"mseq"`
	MDS      int32  `json:"mds"`
	Flags    uint8  `json:"flags"`
}

type nativeXattrReceipt struct {
	Name  string `json:"name"`
	Value string `json:"value_hex"`
}

type nativeCapsReceipt struct {
	Type            uint16                `json:"message_type"`
	Version         uint16                `json:"version"`
	TID             uint64                `json:"tid"`
	Op              uint32                `json:"op"`
	Ino             uint64                `json:"ino"`
	Realm           uint64                `json:"realm"`
	CapID           uint64                `json:"cap_id"`
	Seq             uint32                `json:"seq"`
	IssueSeq        uint32                `json:"issue_seq"`
	Caps            uint32                `json:"caps"`
	Wanted          uint32                `json:"wanted"`
	Dirty           uint32                `json:"dirty"`
	MigrateSeq      uint32                `json:"migrate_seq"`
	SnapFollows     uint64                `json:"snap_follows"`
	SnapTraceLen    uint32                `json:"snap_trace_len"`
	UID             uint32                `json:"uid"`
	GID             uint32                `json:"gid"`
	Mode            uint32                `json:"mode"`
	NLink           uint32                `json:"nlink"`
	XattrLen        uint32                `json:"xattr_len"`
	XattrVersion    uint64                `json:"xattr_version"`
	Size            uint64                `json:"size"`
	MaxSize         uint64                `json:"max_size"`
	TruncateSize    uint64                `json:"truncate_size"`
	TruncateSeq     uint32                `json:"truncate_seq"`
	MTime           nativeTime            `json:"mtime"`
	ATime           nativeTime            `json:"atime"`
	CTime           nativeTime            `json:"ctime"`
	Layout          nativeLayoutReceipt   `json:"layout"`
	TimeWarpSeq     uint32                `json:"time_warp_seq"`
	Peer            *nativeCapPeerReceipt `json:"peer"`
	SnapBlob        string                `json:"snapbl_hex"`
	SnapRealms      []nativeRealmReceipt  `json:"snap_realms"`
	XattrBlob       string                `json:"xattrbl_hex"`
	Xattrs          []nativeXattrReceipt  `json:"xattrs"`
	FlockBlob       string                `json:"flockbl_hex"`
	InlineVersion   uint64                `json:"inline_version"`
	InlineData      string                `json:"inline_data_hex"`
	OSDEpochBarrier uint32                `json:"osd_epoch_barrier"`
	OldestFlushTID  uint64                `json:"oldest_flush_tid"`
	CallerUID       uint32                `json:"caller_uid"`
	CallerGID       uint32                `json:"caller_gid"`
	BTime           nativeTime            `json:"btime"`
	ChangeAttr      uint64                `json:"change_attr"`
	Flags           uint32                `json:"flags"`
	Files           int64                 `json:"nfiles"`
	Subdirs         int64                 `json:"nsubdirs"`
	FscryptAuth     string                `json:"fscrypt_auth_hex"`
	FscryptFile     string                `json:"fscrypt_file_hex"`
}

type nativeCapReleaseReceipt struct {
	Type    uint16 `json:"message_type"`
	Version uint16 `json:"version"`
	TID     uint64 `json:"tid"`
	Num     uint32 `json:"num"`
	Caps    []struct {
		Ino        uint64 `json:"ino"`
		CapID      uint64 `json:"cap_id"`
		MigrateSeq uint32 `json:"migrate_seq"`
		IssueSeq   uint32 `json:"issue_seq"`
	} `json:"caps"`
	OSDEpochBarrier uint32 `json:"osd_epoch_barrier"`
}

type nativeLeaseMessageReceipt struct {
	Type       uint16 `json:"message_type"`
	Version    uint16 `json:"version"`
	TID        uint64 `json:"tid"`
	Action     uint8  `json:"action"`
	Mask       uint16 `json:"mask"`
	Ino        uint64 `json:"ino"`
	First      uint64 `json:"first"`
	Last       uint64 `json:"last"`
	Seq        uint32 `json:"seq"`
	DurationMS uint32 `json:"duration_ms"`
	DentryName string `json:"dname"`
}

type nativeSnapReceipt struct {
	Type        uint16               `json:"message_type"`
	Version     uint16               `json:"version"`
	TID         uint64               `json:"tid"`
	Op          uint32               `json:"op"`
	Split       uint64               `json:"split"`
	NumInos     uint32               `json:"num_split_inos"`
	NumRealms   uint32               `json:"num_split_realms"`
	TraceLen    uint32               `json:"trace_len"`
	SplitInos   []uint64             `json:"split_inos"`
	SplitRealms []uint64             `json:"split_realms"`
	Trace       string               `json:"trace_hex"`
	SnapRealms  []nativeRealmReceipt `json:"snap_realms"`
}

type nativeQuotaReceipt struct {
	Type     uint16     `json:"message_type"`
	Version  uint16     `json:"version"`
	TID      uint64     `json:"tid"`
	Ino      uint64     `json:"ino"`
	RCTime   nativeTime `json:"rctime"`
	RBytes   int64      `json:"rbytes"`
	RFiles   int64      `json:"rfiles"`
	RSubdirs int64      `json:"rsubdirs"`
	MaxBytes int64      `json:"max_bytes"`
	MaxFiles int64      `json:"max_files"`
}

type nativeCapsFixture struct {
	Front      string          `json:"front_sha256"`
	Middle     string          `json:"middle_sha256"`
	Data       string          `json:"data_sha256"`
	Header     string          `json:"header_sha256"`
	Caps       json.RawMessage `json:"decoded_caps"`
	CapRelease json.RawMessage `json:"decoded_cap_release"`
	Lease      json.RawMessage `json:"decoded_lease"`
	Snap       json.RawMessage `json:"decoded_snap"`
	Quota      json.RawMessage `json:"decoded_quota"`
}

type nativeCapsProof struct {
	Commit     string                       `json:"ceph_commit"`
	Wrapper    string                       `json:"wrapper_sha256"`
	Driver     string                       `json:"build_driver_sha256"`
	Launcher   string                       `json:"launcher_sha256"`
	Terminated bool                         `json:"builder_termination_completed"`
	Canonical  bool                         `json:"canonical_message_encode_verified"`
	Decoder    bool                         `json:"native_payload_decoder_verified"`
	Fixtures   map[string]nativeCapsFixture `json:"fixtures"`
}

func readNativeCapsProof(t *testing.T) nativeCapsProof {
	t.Helper()
	data, err := os.ReadFile("testdata/mds-caps-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof nativeCapsProof
	if err = json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

// nativeCapsReceipt decodes one receipt strictly: every field the native
// decoder reported must be modelled by the comparison structure.
func strictNativeReceipt(t *testing.T, name string, raw json.RawMessage, v any) {
	t.Helper()
	if len(raw) == 0 || string(raw) == "null" {
		t.Fatal("native caps-family receipt missing", name)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		t.Fatal(name, err)
	}
}

func nativeCapsMessage(t *testing.T, name string, typ, version uint16, tid uint64) msgr.MessageData {
	t.Helper()
	header, middle := nativeMapBytes(t, name, "header"), nativeMapBytes(t, name, "middle")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, nativeMapBytes(t, name, "front"), middle, nativeMapBytes(t, name, "data")}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sequence != 1 || m.Transaction != tid || m.AckSequence != 3 || m.Priority != 127 || !bytes.Equal(m.Middle, middle) || len(m.Data) != 0 || !bytes.Equal(header, m.Frame().Segments[0]) {
		t.Fatal("native offline caps-family header lost", m)
	}
	if m.Type != typ || m.Version != version || m.CompatVersion != 1 {
		t.Fatal("native canonical caps-family version/compat differs", m)
	}
	return m
}

// capsFromReceipt maps the native decoder's view of MClientCaps onto the Go
// structure, independently of the Go decoder. The native decoder converts
// the legacy ceph_file_layout with file_layout_t::from_legacy: a zeroed
// legacy layout becomes pool -1, while the Go codec keeps the raw wire pool
// 0. cas_hash, object_stripe_unit and unused are written as zero by
// file_layout_t::to_legacy and are not part of the native view.
func capsFromReceipt(t *testing.T, r *nativeCapsReceipt) Caps {
	t.Helper()
	l := r.Layout
	pool := int32(l.PoolID)
	if l.PoolID == -1 {
		if l.StripeUnit|l.StripeCount|l.ObjectSize != 0 {
			t.Fatal("native pool -1 with a populated layout cannot come from from_legacy")
		}
		pool = 0
	}
	c := Caps{Op: r.Op, Ino: r.Ino, Realm: r.Realm, CapID: r.CapID, Seq: r.Seq, IssueSeq: r.IssueSeq,
		Caps: r.Caps, Wanted: r.Wanted, Dirty: r.Dirty, MigrateSeq: r.MigrateSeq, SnapFollows: r.SnapFollows,
		UID: r.UID, GID: r.GID, Mode: r.Mode, NLink: r.NLink, XattrVersion: r.XattrVersion,
		Size: r.Size, MaxSize: r.MaxSize, TruncateSize: r.TruncateSize, TruncateSeq: r.TruncateSeq,
		MTime: r.MTime.time(), ATime: r.ATime.time(), CTime: r.CTime.time(),
		Layout:      FileLayout{StripeUnit: l.StripeUnit, StripeCount: l.StripeCount, ObjectSize: l.ObjectSize, Pool: pool, PoolNamespace: l.PoolNS},
		TimeWarpSeq: r.TimeWarpSeq, SnapTrace: nilIfEmpty(mustHex(t, r.SnapBlob)), Xattrs: nilIfEmpty(mustHex(t, r.XattrBlob)),
		Flock: nilIfEmpty(mustHex(t, r.FlockBlob)), InlineVersion: r.InlineVersion, InlineData: nilIfEmpty(mustHex(t, r.InlineData)),
		OSDEpochBarrier: r.OSDEpochBarrier, OldestFlushTID: r.OldestFlushTID, CallerUID: r.CallerUID, CallerGID: r.CallerGID,
		BTime: r.BTime.time(), ChangeAttr: r.ChangeAttr, Flags: r.Flags, Files: r.Files, Subdirs: r.Subdirs,
		FscryptAuth: nilIfEmpty(mustHex(t, r.FscryptAuth)), FscryptFile: nilIfEmpty(mustHex(t, r.FscryptFile))}
	// The native decoder assigns the peer only for EXPORT and IMPORT.
	if (r.Peer != nil) != (r.Op == CapOpExport || r.Op == CapOpImport) {
		t.Fatal("native peer receipt presence differs from the op", r.Op)
	}
	if r.Peer != nil {
		c.Peer = CapPeer{CapID: r.Peer.CapID, IssueSeq: r.Peer.IssueSeq, MSeq: r.Peer.MSeq, MDS: r.Peer.MDS, Flags: r.Peer.Flags}
	}
	if int(r.SnapTraceLen) != len(c.SnapTrace) || int(r.XattrLen) != len(c.Xattrs) {
		t.Fatal("native caps blob lengths differ from their head lengths", r.SnapTraceLen, r.XattrLen)
	}
	return c
}

func realmsFromReceipt(t *testing.T, receipts []nativeRealmReceipt) []SnapRealm {
	t.Helper()
	var realms []SnapRealm
	for _, n := range receipts {
		if int(n.NumSnaps) != len(n.Snaps) || int(n.NumPrior) != len(n.Prior) {
			t.Fatal("native realm counts differ", n)
		}
		r := SnapRealm{Ino: n.Ino, Created: n.Created, Parent: n.Parent, ParentSince: n.ParentSince, Seq: n.Seq,
			LastModified: n.LastModified.time(), ChangeAttr: n.ChangeAttr}
		if len(n.Snaps) != 0 {
			r.Snaps = n.Snaps
		}
		if len(n.Prior) != 0 {
			r.PriorParentSnaps = n.Prior
		}
		realms = append(realms, r)
	}
	return realms
}

func checkNativeCapsBlobs(t *testing.T, got Caps, r *nativeCapsReceipt) {
	t.Helper()
	realms, err := DecodeSnapTrace(got.SnapTrace)
	if err != nil {
		t.Fatal(err)
	}
	if want := realmsFromReceipt(t, r.SnapRealms); !reflect.DeepEqual(realms, want) {
		t.Fatalf("snap trace\n got %+v\nnative %+v", realms, want)
	}
	xattrs, err := DecodeXattrs(got.Xattrs)
	if err != nil {
		t.Fatal(err)
	}
	if len(xattrs) != len(r.Xattrs) {
		t.Fatalf("xattrs %v, native %+v", xattrs, r.Xattrs)
	}
	for _, x := range r.Xattrs {
		if v, ok := xattrs[x.Name]; !ok || !bytes.Equal(v, mustHex(t, x.Value)) {
			t.Fatalf("xattr %q = %x, native %s", x.Name, v, x.Value)
		}
	}
}

// checkNativeEnvelope compares a Go-encoded message with every native
// segment and the native 41-byte header for the same offline inputs.
func checkNativeEnvelope(t *testing.T, name string, got, native msgr.MessageData) {
	t.Helper()
	if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion || got.Priority != native.Priority || got.Transaction != native.Transaction || len(got.Data) != 0 {
		t.Fatalf("envelope = %+v, native %+v", got, native)
	}
	if !bytes.Equal(got.Front, native.Front) {
		t.Fatalf("front\n got %x\nnative %x", got.Front, native.Front)
	}
	if !bytes.Equal(got.Middle, native.Middle) {
		t.Fatalf("middle\n got %x\nnative %x", got.Middle, native.Middle)
	}
	got.Sequence, got.AckSequence = 1, 3
	if header := nativeMapBytes(t, name, "header"); !bytes.Equal(got.Frame().Segments[0], header) {
		t.Fatalf("header\n got %x\nnative %x", got.Frame().Segments[0], header)
	}
}

func nativeXattrBlob(entries ...string) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(entries) / 2))
	for i := 0; i < len(entries); i += 2 {
		e.String(entries[i])
		e.String(entries[i+1])
	}
	return e.Data
}

// The client UPDATE is written here as Client::send_cap fills it, without
// consulting the native bytes or receipt; Go must reproduce the native
// front, middle and header exactly.
func TestEncodeClientCapsUpdateMatchesFreshNative(t *testing.T) {
	proof := readNativeCapsProof(t)
	name := "mds-caps-native-caps-update"
	update := Caps{Op: CapOpUpdate, Ino: 0x10000000001, CapID: 0x7777, Seq: 11, IssueSeq: 13,
		Caps: 0x3d55, Wanted: 0x1800, Dirty: 0x1080, MigrateSeq: 12, SnapFollows: 5,
		UID: 1000, GID: 1001, Mode: 0o100600, NLink: 1, XattrVersion: 8,
		Size: 20000, MaxSize: 8 << 20, TruncateSize: 100, TruncateSeq: 5,
		MTime: Time{1700000500, 123}, ATime: Time{1700000400, 456}, CTime: Time{1700000500, 124}, TimeWarpSeq: 4,
		Xattrs: nativeXattrBlob("user.flushed", "yes"), InlineVersion: ^uint64(0), OSDEpochBarrier: 21, OldestFlushTID: 76,
		CallerUID: ^uint32(0), CallerGID: ^uint32(0), BTime: Time{1700000000, 9}, ChangeAttr: 0x9a, Flags: 4,
		// The native client leaves the dirstat counts at their -1 defaults.
		Files: -1, Subdirs: -1}
	native := nativeCapsMessage(t, name, ClientCapsMessage, 12, nativeCapsFlushTID)
	checkNativeEnvelope(t, name, EncodeCaps(update, nativeCapsFlushTID), native)
	decoded, err := DecodeCaps(native, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, update) {
		t.Fatalf("round trip\n got %+v\nwant %+v", decoded, update)
	}
	var r nativeCapsReceipt
	strictNativeReceipt(t, name, proof.Fixtures[name].Caps, &r)
	if r.Type != ClientCapsMessage || r.Version != 12 || r.TID != nativeCapsFlushTID {
		t.Fatal("native caps receipt envelope", r.Type, r.Version, r.TID)
	}
	if want := capsFromReceipt(t, &r); !reflect.DeepEqual(decoded, want) {
		t.Fatalf("Go decode differs from native decode\n got %+v\nnative %+v", decoded, want)
	}
	checkNativeCapsBlobs(t, decoded, &r)
}

// Server-originated MClientCaps: Go decodes the native segments to exactly
// what the native decoder reported, and re-encodes them byte-for-byte.
func TestDecodeFreshNativeServerCaps(t *testing.T) {
	proof := readNativeCapsProof(t)
	for _, tc := range []struct {
		name          string
		op            uint32
		tid           uint64
		realms, xattr int
	}{
		{"caps-grant", CapOpGrant, 0, 1, 2},
		// EXPORT: peer only, zero-padded body, -1 dirstat defaults.
		{"caps-export", CapOpExport, 0, 0, 0},
		// IMPORT of a directory: zeroed legacy layout and CEPH_INLINE_NONE.
		{"caps-import", CapOpImport, 0, 2, 0},
		{"caps-flush-ack", CapOpFlushAck, nativeCapsFlushTID, 0, 0},
		{"caps-update", CapOpUpdate, nativeCapsFlushTID, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-caps-native-" + tc.name
			m := nativeCapsMessage(t, name, ClientCapsMessage, 12, tc.tid)
			got, err := DecodeCaps(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			var r nativeCapsReceipt
			strictNativeReceipt(t, name, proof.Fixtures[name].Caps, &r)
			if r.Type != ClientCapsMessage || r.Version != 12 || r.TID != m.Transaction {
				t.Fatal("native caps receipt envelope", r.Type, r.Version, r.TID)
			}
			if want := capsFromReceipt(t, &r); !reflect.DeepEqual(got, want) {
				t.Fatalf("Go decode differs from native decode\n got %+v\nnative %+v", got, want)
			}
			if got.Op != tc.op || len(r.SnapRealms) != tc.realms || len(r.Xattrs) != tc.xattr {
				t.Fatalf("vector identity op %d realms %d xattrs %d", got.Op, len(r.SnapRealms), len(r.Xattrs))
			}
			checkNativeCapsBlobs(t, got, &r)
			checkNativeEnvelope(t, name, EncodeCaps(got, m.Transaction), m)
		})
	}
}

// The GRANT vector sets every field to a nonzero value, so a misplaced Go
// field cannot pass on zero defaults.
func TestNativeCapsGrantFieldsAreDistinctive(t *testing.T) {
	m := nativeCapsMessage(t, "mds-caps-native-caps-grant", ClientCapsMessage, 12, 0)
	got, err := DecodeCaps(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	v := reflect.ValueOf(got)
	for i := range v.NumField() {
		field := v.Type().Field(i)
		switch field.Name {
		case "Op", "Peer":
			// GRANT is op 0 and carries no peer.
		case "Layout":
			sub := v.Field(i)
			for j := range sub.NumField() {
				if name := sub.Type().Field(j).Name; sub.Field(j).IsZero() && name != "CASHash" && name != "ObjectStripeUnit" && name != "Unused" {
					t.Errorf("grant Layout.%s is zero", name)
				}
			}
		default:
			if v.Field(i).IsZero() {
				t.Errorf("grant %s is zero", field.Name)
			}
		}
	}
}

func TestEncodeCapReleaseMatchesFreshNative(t *testing.T) {
	proof := readNativeCapsProof(t)
	name := "mds-caps-native-cap-release"
	items := []CapItem{{Ino: 0x10000000003, CapID: 0x31, MigrateSeq: 2, IssueSeq: 7}, {Ino: 0x10000000004, CapID: 0x32, IssueSeq: 1}}
	native := nativeCapsMessage(t, name, ClientCapReleaseMessage, 2, 0)
	got, err := EncodeCapRelease(items, 21)
	if err != nil {
		t.Fatal(err)
	}
	checkNativeEnvelope(t, name, got, native)
	decoded, barrier, err := DecodeCapRelease(native, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	var r nativeCapReleaseReceipt
	strictNativeReceipt(t, name, proof.Fixtures[name].CapRelease, &r)
	if r.Type != ClientCapReleaseMessage || r.Version != 2 || r.TID != 0 || int(r.Num) != len(r.Caps) {
		t.Fatal("native cap release receipt", r)
	}
	var want []CapItem
	for _, item := range r.Caps {
		want = append(want, CapItem{Ino: item.Ino, CapID: item.CapID, MigrateSeq: item.MigrateSeq, IssueSeq: item.IssueSeq})
	}
	if !reflect.DeepEqual(decoded, want) || !reflect.DeepEqual(decoded, items) || barrier != r.OSDEpochBarrier || barrier != 21 {
		t.Fatalf("cap release %+v/%d, native %+v", decoded, barrier, r)
	}
}

func TestLeaseMatchesFreshNative(t *testing.T) {
	proof := readNativeCapsProof(t)
	for _, tc := range []struct {
		name   string
		action uint8
	}{{"lease-revoke", LeaseRevoke}, {"lease-release", LeaseRelease}} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-caps-native-" + tc.name
			native := nativeCapsMessage(t, name, ClientLeaseMessage, 1, 0)
			// Client::handle_lease answers a revoke with the same fields
			// and the constructor's zero duration.
			lease := Lease{Action: tc.action, Mask: 3, Ino: 0x10000000000, First: 2, Last: NoSnap, Seq: 4, DentryName: "file"}
			checkNativeEnvelope(t, name, EncodeLease(lease), native)
			got, err := DecodeLease(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			var r nativeLeaseMessageReceipt
			strictNativeReceipt(t, name, proof.Fixtures[name].Lease, &r)
			want := Lease{Action: r.Action, Mask: r.Mask, Ino: r.Ino, First: r.First, Last: r.Last, Seq: r.Seq, DurationMS: r.DurationMS, DentryName: r.DentryName}
			if r.Type != ClientLeaseMessage || r.Version != 1 || r.TID != 0 || got != want || got != lease {
				t.Fatalf("lease %+v, native %+v", got, r)
			}
		})
	}
}

func TestDecodeFreshNativeSnapSplit(t *testing.T) {
	proof := readNativeCapsProof(t)
	name := "mds-caps-native-snap-split"
	m := nativeCapsMessage(t, name, ClientSnapMessage, 1, 0)
	got, err := DecodeSnap(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	var r nativeSnapReceipt
	strictNativeReceipt(t, name, proof.Fixtures[name].Snap, &r)
	if r.Type != ClientSnapMessage || r.Version != 1 || r.TID != 0 || int(r.NumInos) != len(r.SplitInos) || int(r.NumRealms) != len(r.SplitRealms) || int(r.TraceLen) != len(mustHex(t, r.Trace)) {
		t.Fatal("native snap receipt", r)
	}
	want := Snap{Op: r.Op, Split: r.Split, SplitInos: r.SplitInos, SplitRealms: r.SplitRealms, Trace: nilIfEmpty(mustHex(t, r.Trace))}
	if !reflect.DeepEqual(got, want) || got.Op != SnapOpSplit || len(got.SplitInos) != 2 || len(got.SplitRealms) != 1 {
		t.Fatalf("snap\n got %+v\nnative %+v", got, want)
	}
	realms, err := DecodeSnapTrace(got.Trace)
	if err != nil {
		t.Fatal(err)
	}
	if want := realmsFromReceipt(t, r.SnapRealms); !reflect.DeepEqual(realms, want) || len(realms) != 2 {
		t.Fatalf("snap trace\n got %+v\nnative %+v", realms, want)
	}
}

func TestDecodeFreshNativeQuota(t *testing.T) {
	proof := readNativeCapsProof(t)
	name := "mds-caps-native-quota"
	m := nativeCapsMessage(t, name, ClientQuotaMessage, 1, 0)
	got, err := DecodeQuota(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	var r nativeQuotaReceipt
	strictNativeReceipt(t, name, proof.Fixtures[name].Quota, &r)
	want := Quota{Ino: r.Ino, RCTime: r.RCTime.time(), RBytes: r.RBytes, RFiles: r.RFiles, RSubdirs: r.RSubdirs, MaxBytes: r.MaxBytes, MaxFiles: r.MaxFiles}
	if r.Type != ClientQuotaMessage || r.Version != 1 || r.TID != 0 || got != want ||
		got != (Quota{Ino: 0x10000000000, RCTime: Time{1700000400, 5}, RBytes: 1 << 20, RFiles: 12, RSubdirs: 3, MaxBytes: 1 << 30, MaxFiles: 1000}) {
		t.Fatalf("quota %+v, native %+v", got, r)
	}
}

func TestNativeCapsProvenanceMatchesCurrentSourcesAndSegments(t *testing.T) {
	proof := readNativeCapsProof(t)
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Terminated || !proof.Decoder || !proof.Canonical || len(proof.Fixtures) != 10 {
		t.Fatal("native caps proof lacks pinned source, cleanup or decoder")
	}
	for _, producer := range []struct{ hash, file string }{{proof.Wrapper, "mds_caps_native_encoder.cc"}, {proof.Driver, "mds_caps_native_encoder.py"}, {proof.Launcher, "mds_caps_native_encoder.py"}} {
		raw, err := os.ReadFile("../../tools/" + producer.file)
		if err != nil {
			t.Fatal(err)
		}
		if producer.hash != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("native caps producer hash differs", producer.file)
		}
	}
	for name, fixture := range proof.Fixtures {
		receipts := 0
		for _, raw := range []json.RawMessage{fixture.Caps, fixture.CapRelease, fixture.Lease, fixture.Snap, fixture.Quota} {
			if len(raw) != 0 {
				receipts++
			}
		}
		if receipts != 1 {
			t.Fatal("native caps vector lacks exactly one decoder receipt", name)
		}
		for segment, digest := range map[string]string{"header": fixture.Header, "front": fixture.Front, "middle": fixture.Middle, "data": fixture.Data} {
			if digest != fmt.Sprintf("%x", sha256.Sum256(nativeMapBytes(t, name, segment))) {
				t.Fatal("native caps segment differs", name, segment)
			}
		}
	}
}
