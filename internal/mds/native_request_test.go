package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Fresh native Tentacle MClientRequest, MClientRequestForward and
// MClientReply vectors produced by tools/mds_request_native_encoder.cc; see
// mds-request-native-provenance.json. Request and forward/reply envelopes
// are canonical native encodings. Reply trace/readdir bytes restate the MDS
// REPLY_ENCODING field order (CInode::encode_inodestat needs a live MDS) and
// are proven by the unchanged native client decoders, whose decoded fields
// form the receipts compared here.

const nativeRequestTID = 41

type nativeTime struct {
	Sec  uint32 `json:"sec"`
	Nsec uint32 `json:"nsec"`
}

func (t nativeTime) time() Time { return Time{Seconds: t.Sec, Nanoseconds: t.Nsec} }

type nativeRequestReceipt struct {
	Type              uint16     `json:"message_type"`
	Version           uint16     `json:"version"`
	TID               uint64     `json:"tid"`
	SourceHeadVersion uint16     `json:"source_head_version"`
	SourceMDSFeatures string     `json:"source_mds_features_hex"`
	HeadVersion       uint16     `json:"head_version"`
	OldestClientTID   uint64     `json:"oldest_client_tid"`
	MDSMapEpoch       uint32     `json:"mdsmap_epoch"`
	Flags             uint32     `json:"flags"`
	NumRetry          uint8      `json:"num_retry"`
	NumForward        uint8      `json:"num_fwd"`
	NumReleases       uint16     `json:"num_releases"`
	Op                uint32     `json:"op"`
	CallerUID         uint32     `json:"caller_uid"`
	CallerGID         uint32     `json:"caller_gid"`
	Ino               uint64     `json:"ino"`
	Args              string     `json:"args_hex"`
	ExtRetry          uint32     `json:"ext_num_retry"`
	ExtForward        uint32     `json:"ext_num_fwd"`
	StructLen         uint32     `json:"struct_len"`
	OwnerUID          uint32     `json:"owner_uid"`
	OwnerGID          uint32     `json:"owner_gid"`
	PathIno           uint64     `json:"path_ino"`
	Path              string     `json:"path"`
	Path2Ino          uint64     `json:"path2_ino"`
	Path2             string     `json:"path2"`
	Stamp             nativeTime `json:"stamp"`
	GIDs              []uint64   `json:"gid_list"`
	AlternateName     string     `json:"alternate_name_hex"`
	FscryptAuth       string     `json:"fscrypt_auth_hex"`
	FscryptFile       string     `json:"fscrypt_file_hex"`
	Releases          []struct {
		Ino       uint64 `json:"ino"`
		CapID     uint64 `json:"cap_id"`
		Caps      uint32 `json:"caps"`
		Wanted    uint32 `json:"wanted"`
		Seq       uint32 `json:"seq"`
		IssueSeq  uint32 `json:"issue_seq"`
		MSeq      uint32 `json:"mseq"`
		DentrySeq uint32 `json:"dname_seq"`
		DentryLen uint32 `json:"dname_len"`
		Dentry    string `json:"dname"`
	} `json:"releases"`
}

type nativeForwardReceipt struct {
	Type       uint16 `json:"message_type"`
	Version    uint16 `json:"version"`
	TID        uint64 `json:"tid"`
	Dest       int32  `json:"dest_mds"`
	Forward    int32  `json:"num_fwd"`
	MustResend bool   `json:"must_resend"`
}

type nativeInodeReceipt struct {
	Ino          uint64 `json:"ino"`
	SnapID       uint64 `json:"snapid"`
	Rdev         uint32 `json:"rdev"`
	Version      uint64 `json:"version"`
	XattrVersion uint64 `json:"xattr_version"`
	Cap          struct {
		Caps   uint32 `json:"caps"`
		Wanted uint32 `json:"wanted"`
		CapID  uint64 `json:"cap_id"`
		Seq    uint32 `json:"seq"`
		MSeq   uint32 `json:"mseq"`
		Realm  uint64 `json:"realm"`
		Flags  uint8  `json:"flags"`
	} `json:"cap"`
	Layout struct {
		StripeUnit  uint32 `json:"stripe_unit"`
		StripeCount uint32 `json:"stripe_count"`
		ObjectSize  uint32 `json:"object_size"`
		PoolID      int64  `json:"pool_id"`
		PoolNS      string `json:"pool_ns"`
	} `json:"layout"`
	CTime        nativeTime `json:"ctime"`
	MTime        nativeTime `json:"mtime"`
	ATime        nativeTime `json:"atime"`
	TimeWarpSeq  uint32     `json:"time_warp_seq"`
	Size         uint64     `json:"size"`
	MaxSize      uint64     `json:"max_size"`
	TruncateSize uint64     `json:"truncate_size"`
	TruncateSeq  uint32     `json:"truncate_seq"`
	Mode         uint32     `json:"mode"`
	UID          uint32     `json:"uid"`
	GID          uint32     `json:"gid"`
	NLink        uint32     `json:"nlink"`
	Files        int64      `json:"nfiles"`
	Subdirs      int64      `json:"nsubdirs"`
	RBytes       int64      `json:"rbytes"`
	RFiles       int64      `json:"rfiles"`
	RSubdirs     int64      `json:"rsubdirs"`
	RCTime       nativeTime `json:"rctime"`
	FragTree     []struct {
		Frag uint32 `json:"frag"`
		Bits int32  `json:"bits"`
	} `json:"fragtree"`
	Symlink   string `json:"symlink"`
	DirLayout string `json:"dir_layout_hex"`
	XattrBlob string `json:"xattrbl_hex"`
	Xattrs    []struct {
		Name  string `json:"name"`
		Value string `json:"value_hex"`
	} `json:"xattrs"`
	InlineVersion uint64 `json:"inline_version"`
	InlineData    string `json:"inline_data_hex"`
	Quota         struct {
		MaxBytes int64 `json:"max_bytes"`
		MaxFiles int64 `json:"max_files"`
	} `json:"quota"`
	BTime            nativeTime        `json:"btime"`
	ChangeAttr       uint64            `json:"change_attr"`
	DirPin           int32             `json:"dir_pin"`
	SnapBTime        nativeTime        `json:"snap_btime"`
	RSnaps           int64             `json:"rsnaps"`
	SnapMetadata     map[string]string `json:"snap_metadata"`
	FscryptFlag      bool              `json:"fscrypt_flag_encoded"`
	FscryptAuth      string            `json:"fscrypt_auth_hex"`
	FscryptFile      string            `json:"fscrypt_file_hex"`
	OptMetadataCount uint32            `json:"optmetadata_count"`
}

type nativeDirStatReceipt struct {
	Frag uint32  `json:"frag"`
	Auth int32   `json:"auth"`
	Dist []int32 `json:"dist"`
}

type nativeLeaseReceipt struct {
	Mask          uint16 `json:"mask"`
	DurationMS    uint32 `json:"duration_ms"`
	Seq           uint32 `json:"seq"`
	AlternateName string `json:"alternate_name_hex"`
}

type nativeRealmReceipt struct {
	Ino          uint64     `json:"ino"`
	Created      uint64     `json:"created"`
	Parent       uint64     `json:"parent"`
	ParentSince  uint64     `json:"parent_since"`
	Seq          uint64     `json:"seq"`
	NumSnaps     uint32     `json:"num_snaps"`
	NumPrior     uint32     `json:"num_prior_parent_snaps"`
	Snaps        []uint64   `json:"my_snaps"`
	Prior        []uint64   `json:"prior_parent_snaps"`
	LastModified nativeTime `json:"last_modified"`
	ChangeAttr   uint64     `json:"change_attr"`
}

type nativeReplyReceipt struct {
	Type        uint16 `json:"message_type"`
	Version     uint16 `json:"version"`
	TID         uint64 `json:"tid"`
	Op          uint32 `json:"op"`
	Result      int32  `json:"result"`
	MDSMapEpoch uint32 `json:"mdsmap_epoch"`
	Safe        uint8  `json:"safe"`
	IsDentry    uint8  `json:"is_dentry"`
	IsTarget    uint8  `json:"is_target"`
	TraceBytes  int    `json:"trace_bytes"`
	ExtraBytes  int    `json:"extra_bytes"`
	SnapBytes   int    `json:"snap_bytes"`
	Trace       *struct {
		DirInode   *nativeInodeReceipt   `json:"dir_inode"`
		Dir        *nativeDirStatReceipt `json:"dir"`
		DentryName *string               `json:"dentry_name"`
		Lease      *nativeLeaseReceipt   `json:"lease"`
		Target     *nativeInodeReceipt   `json:"target"`
	} `json:"trace"`
	Readdir *struct {
		Dir     nativeDirStatReceipt `json:"dir"`
		NumDN   uint32               `json:"numdn"`
		Flags   uint16               `json:"flags"`
		Entries []struct {
			Name  string             `json:"name"`
			Lease nativeLeaseReceipt `json:"lease"`
			Inode nativeInodeReceipt `json:"inode"`
		} `json:"entries"`
	} `json:"readdir"`
	SnapRealms []nativeRealmReceipt `json:"snap_realms"`
}

type nativeRequestFixture struct {
	Front   string                `json:"front_sha256"`
	Data    string                `json:"data_sha256"`
	Header  string                `json:"header_sha256"`
	Request *nativeRequestReceipt `json:"decoded_request"`
	Forward *nativeForwardReceipt `json:"decoded_forward"`
	Reply   *nativeReplyReceipt   `json:"decoded_reply"`
}

type nativeRequestProof struct {
	Commit     string                          `json:"ceph_commit"`
	Wrapper    string                          `json:"wrapper_sha256"`
	Driver     string                          `json:"build_driver_sha256"`
	Launcher   string                          `json:"launcher_sha256"`
	Terminated bool                            `json:"builder_termination_completed"`
	Canonical  bool                            `json:"canonical_message_encode_verified"`
	Decoder    bool                            `json:"native_payload_decoder_verified"`
	Fixtures   map[string]nativeRequestFixture `json:"fixtures"`
}

func readNativeRequestProof(t *testing.T) nativeRequestProof {
	t.Helper()
	data, err := os.ReadFile("testdata/mds-request-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof nativeRequestProof
	if err = json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func nativeRequestMessage(t *testing.T, name string, typ, version uint16) msgr.MessageData {
	t.Helper()
	header := nativeMapBytes(t, name, "header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, nativeMapBytes(t, name, "front"), nil, nativeMapBytes(t, name, "data")}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sequence != 1 || m.Transaction != nativeRequestTID || m.AckSequence != 3 || m.Priority != 127 || len(m.Middle) != 0 || len(m.Data) != 0 || !bytes.Equal(header, m.Frame().Segments[0]) {
		t.Fatal("native offline request-family header lost", m)
	}
	if m.Type != typ || m.Version != version || m.CompatVersion != 1 {
		t.Fatal("native canonical request-family version/compat differs", m)
	}
	return m
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// requestFromReceipt maps the native decoder's view of a request onto the
// Go structure, independently of the Go decoder.
func requestFromReceipt(t *testing.T, r *nativeRequestReceipt) ClientRequest {
	t.Helper()
	if r.HeadVersion != RequestHeadVersion || r.SourceHeadVersion != RequestHeadVersion || r.StructLen != RequestHeadBytes ||
		uint32(r.NumRetry) != r.ExtRetry&0xff || uint32(r.NumForward) != r.ExtForward&0xff || int(r.NumReleases) != len(r.Releases) {
		t.Fatalf("native request head receipt %+v", r)
	}
	req := ClientRequest{OldestClientTID: r.OldestClientTID, MDSMapEpoch: r.MDSMapEpoch, Flags: r.Flags, Retry: r.ExtRetry, Forward: r.ExtForward,
		Op: r.Op, CallerUID: r.CallerUID, CallerGID: r.CallerGID, Ino: r.Ino, OwnerUID: r.OwnerUID, OwnerGID: r.OwnerGID,
		Path: FilePath{Ino: r.PathIno, Path: r.Path}, Path2: FilePath{Ino: r.Path2Ino, Path: r.Path2}, Stamp: r.Stamp.time(),
		AlternateName: nilIfEmpty(mustHex(t, r.AlternateName)), FscryptAuth: nilIfEmpty(mustHex(t, r.FscryptAuth)), FscryptFile: nilIfEmpty(mustHex(t, r.FscryptFile))}
	if copy(req.Args[:], mustHex(t, r.Args)) != RequestArgsBytes {
		t.Fatal("native args size differs")
	}
	if len(r.GIDs) != 0 {
		req.GIDs = r.GIDs
	}
	for _, rel := range r.Releases {
		if rel.DentryLen != uint32(len(rel.Dentry)) {
			t.Fatal("native release dname_len differs")
		}
		req.Releases = append(req.Releases, RequestRelease{Ino: rel.Ino, CapID: rel.CapID, Caps: rel.Caps, Wanted: rel.Wanted, Seq: rel.Seq,
			IssueSeq: rel.IssueSeq, MSeq: rel.MSeq, DentrySeq: rel.DentrySeq, DentryName: rel.Dentry})
	}
	return req
}

func TestEncodeClientRequestMatchesFreshNative(t *testing.T) {
	proof := readNativeRequestProof(t)
	args := func(words ...uint32) (a [RequestArgsBytes]byte) {
		for i, w := range words {
			binary.LittleEndian.PutUint32(a[4*i:], w)
		}
		return a
	}
	for _, tc := range []struct {
		name string
		req  ClientRequest
	}{
		{"request-lookup", ClientRequest{OldestClientTID: 40, MDSMapEpoch: 9, Flags: RequestFlagWantDentry, Op: OpLookup,
			CallerUID: 1000, CallerGID: 1001, Args: args(0x355), OwnerUID: 1000, OwnerGID: 1001,
			Path: FilePath{Ino: 1, Path: "dir/file"}, Stamp: Time{1700000000, 123456000}, GIDs: []uint64{1001, 1002}}},
		// Retry 0x101 proves the legacy u8 copy is the truncated low byte.
		{"request-mkdir-release", ClientRequest{OldestClientTID: 40, MDSMapEpoch: 9, Flags: RequestFlagWantDentry, Retry: 0x101, Forward: 2, Op: OpMkdir,
			CallerUID: 1000, CallerGID: 1001, Args: args(0o755), OwnerUID: 2000, OwnerGID: 2001,
			Path:     FilePath{Ino: 0x10000000000, Path: "newdir"},
			Releases: []RequestRelease{{Ino: 0x10000000000, CapID: 0x1234, Caps: 0x0d55, Wanted: 5, Seq: 7, IssueSeq: 6, MSeq: 2, DentrySeq: 3, DentryName: "newdir"}},
			Stamp:    Time{1700000001, 5}, GIDs: []uint64{1001}, AlternateName: []byte("alt"), FscryptAuth: []byte{1, 2}, FscryptFile: []byte{3}}},
		// The native constructor's default owner is -1/-1.
		{"request-rename", ClientRequest{OldestClientTID: 40, MDSMapEpoch: 10, Flags: RequestFlagWantDentry | RequestFlagReplay, Retry: 1, Op: OpRename,
			Ino: 0x10000000002, OwnerUID: ^uint32(0), OwnerGID: ^uint32(0),
			Path: FilePath{Ino: 0x10000000000, Path: "dst"}, Path2: FilePath{Ino: 0x10000000001, Path: "src"}, Stamp: Time{1700000002, 999999999}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-request-native-" + tc.name
			native := nativeRequestMessage(t, name, ClientRequestMessage, 6)
			got, err := EncodeClientRequest(tc.req, allMDSFeatures)
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion || got.Priority != native.Priority || len(got.Middle) != 0 || len(got.Data) != 0 {
				t.Fatalf("envelope = %+v, native %+v", got, native)
			}
			if !bytes.Equal(got.Front, native.Front) {
				t.Fatalf("front\n got %x\nnative %x", got.Front, native.Front)
			}
			got.Sequence, got.Transaction, got.AckSequence = 1, nativeRequestTID, 3
			if header := nativeMapBytes(t, name, "header"); !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatalf("header\n got %x\nnative %x", got.Frame().Segments[0], header)
			}
			decoded, err := DecodeClientRequest(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, tc.req) {
				t.Fatalf("round trip\n got %+v\nwant %+v", decoded, tc.req)
			}
			receipt := proof.Fixtures[name].Request
			if receipt == nil || receipt.Type != ClientRequestMessage || receipt.Version != 6 || receipt.TID != nativeRequestTID {
				t.Fatal("native request receipt missing", receipt)
			}
			// The CEPHFS_FEATURES_ALL image the native source carried; the Go
			// encoder needs the same two bits to select head version 3.
			if receipt.SourceMDSFeatures != "08000000"+hex.EncodeToString(allMDSFeatures) {
				t.Fatal("native source MDS features differ", receipt.SourceMDSFeatures)
			}
			if want := requestFromReceipt(t, receipt); !reflect.DeepEqual(decoded, want) {
				t.Fatalf("Go decode differs from native decode\n got %+v\nnative %+v", decoded, want)
			}
		})
	}
}

func TestDecodeFreshNativeRequestForward(t *testing.T) {
	proof := readNativeRequestProof(t)
	name := "mds-request-native-forward"
	m := nativeRequestMessage(t, name, ClientRequestForwardMessage, 1)
	got, err := DecodeRequestForward(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	r := proof.Fixtures[name].Forward
	if r == nil || r.Type != ClientRequestForwardMessage || r.Version != 1 || r.TID != m.Transaction ||
		got != (RequestForward{DestinationMDS: r.Dest, Forward: r.Forward, MustResend: r.MustResend}) ||
		got != (RequestForward{DestinationMDS: 1, Forward: 3, MustResend: true}) {
		t.Fatalf("forward %+v, native receipt %+v", got, r)
	}
}

// inodeFromReceipt maps the native InodeStat decoder output onto the Go
// structure. Two native conversions are applied explicitly: the native
// decoder turns the legacy ceph_file_layout into file_layout_t (a zeroed
// legacy layout becomes pool -1, while the Go codec keeps the raw wire pool
// 0), and it discards the fscrypt flag, which the receipt records as the
// MDS rule !fscrypt_auth.empty(). The legacy cas_hash, object_stripe_unit
// and unused words are written as zero by file_layout_t::to_legacy.
func inodeFromReceipt(t *testing.T, n *nativeInodeReceipt) InodeStat {
	t.Helper()
	if n.OptMetadataCount != 0 {
		t.Fatal("receipt carries optmetadata options this comparison does not model", n.OptMetadataCount)
	}
	pool := int32(n.Layout.PoolID)
	if n.Layout.PoolID < 0 {
		pool = 0
	}
	s := InodeStat{Ino: n.Ino, SnapID: n.SnapID, Rdev: n.Rdev, Version: n.Version, XattrVersion: n.XattrVersion,
		Cap:    ReplyCap{Caps: n.Cap.Caps, Wanted: n.Cap.Wanted, CapID: n.Cap.CapID, Seq: n.Cap.Seq, MSeq: n.Cap.MSeq, Realm: n.Cap.Realm, Flags: n.Cap.Flags},
		Layout: FileLayout{StripeUnit: n.Layout.StripeUnit, StripeCount: n.Layout.StripeCount, ObjectSize: n.Layout.ObjectSize, Pool: pool, PoolNamespace: n.Layout.PoolNS},
		CTime:  n.CTime.time(), MTime: n.MTime.time(), ATime: n.ATime.time(), TimeWarpSeq: n.TimeWarpSeq,
		Size: n.Size, MaxSize: n.MaxSize, TruncateSize: n.TruncateSize, TruncateSeq: n.TruncateSeq,
		Mode: n.Mode, UID: n.UID, GID: n.GID, NLink: n.NLink,
		Files: n.Files, Subdirs: n.Subdirs, RBytes: n.RBytes, RFiles: n.RFiles, RSubdirs: n.RSubdirs, RCTime: n.RCTime.time(),
		Symlink: n.Symlink, Xattrs: nilIfEmpty(mustHex(t, n.XattrBlob)), InlineVersion: n.InlineVersion, InlineData: nilIfEmpty(mustHex(t, n.InlineData)),
		QuotaMaxBytes: n.Quota.MaxBytes, QuotaMaxFiles: n.Quota.MaxFiles, BTime: n.BTime.time(), ChangeAttr: n.ChangeAttr, DirPin: n.DirPin,
		SnapBTime: n.SnapBTime.time(), RSnaps: n.RSnaps, Fscrypt: n.FscryptFlag,
		FscryptAuth: nilIfEmpty(mustHex(t, n.FscryptAuth)), FscryptFile: nilIfEmpty(mustHex(t, n.FscryptFile)),
		// A zero-option multiton: the u32 option count only.
		OptionalMetadata: binary.LittleEndian.AppendUint32(nil, n.OptMetadataCount)}
	if n.Layout.PoolID == -1 && (n.Layout.StripeUnit|n.Layout.StripeCount|n.Layout.ObjectSize) != 0 {
		t.Fatal("native pool -1 with a populated layout cannot come from from_legacy")
	}
	for _, split := range n.FragTree {
		s.FragTree = append(s.FragTree, FragSplit{Frag: split.Frag, Bits: split.Bits})
	}
	if copy(s.DirLayout[:], mustHex(t, n.DirLayout)) != 8 {
		t.Fatal("native dir layout size differs")
	}
	s.DirHash = s.DirLayout[0]
	if len(n.SnapMetadata) != 0 {
		s.SnapMetadata = n.SnapMetadata
	}
	return s
}

func checkNativeInode(t *testing.T, what string, got *InodeStat, n *nativeInodeReceipt) {
	t.Helper()
	if got == nil || n == nil {
		t.Fatalf("%s presence differs: Go %v native %v", what, got != nil, n != nil)
	}
	if want := inodeFromReceipt(t, n); !reflect.DeepEqual(*got, want) {
		t.Fatalf("%s differs from native decode\n got %+v\nnative %+v", what, *got, want)
	}
	xattrs, err := DecodeXattrs(got.Xattrs)
	if err != nil {
		t.Fatal(what, err)
	}
	if len(xattrs) != len(n.Xattrs) {
		t.Fatalf("%s xattrs %v, native %+v", what, xattrs, n.Xattrs)
	}
	for _, x := range n.Xattrs {
		if v, ok := xattrs[x.Name]; !ok || !bytes.Equal(v, mustHex(t, x.Value)) {
			t.Fatalf("%s xattr %q = %x, native %s", what, x.Name, v, x.Value)
		}
	}
}

func dirStatFromReceipt(n *nativeDirStatReceipt) DirStat {
	s := DirStat{Frag: n.Frag, Auth: n.Auth}
	if len(n.Dist) != 0 {
		s.Distribution = n.Dist
	}
	return s
}

func leaseFromReceipt(t *testing.T, n *nativeLeaseReceipt) LeaseStat {
	return LeaseStat{Mask: n.Mask, DurationMS: n.DurationMS, Seq: n.Seq, AlternateName: nilIfEmpty(mustHex(t, n.AlternateName))}
}

func TestDecodeFreshNativeClientReplies(t *testing.T) {
	proof := readNativeRequestProof(t)
	for _, tc := range []struct {
		name                     string
		op                       uint32
		result                   int32
		safe, isDentry, isTarget bool
		entries, realms          int
	}{
		{"reply-lookup", OpLookup, 0, true, true, true, 0, 2},
		{"reply-readdir", OpReaddir, 0, true, false, true, 2, 1},
		// ENOENT: the native errorcode32_t host-to-wire mapping is the
		// identity on the Linux build host, so the wire carries -2.
		{"reply-unsafe-error", OpUnlink, -2, false, false, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-request-native-" + tc.name
			m := nativeRequestMessage(t, name, ClientReplyMessage, 1)
			reply, err := DecodeClientReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			r := proof.Fixtures[name].Reply
			if r == nil || r.Type != ClientReplyMessage || r.Version != 1 || r.TID != m.Transaction {
				t.Fatal("native reply receipt missing", r)
			}
			wantHead := ReplyHead{Op: r.Op, Result: r.Result, MDSMapEpoch: r.MDSMapEpoch, Safe: r.Safe == 1, IsDentry: r.IsDentry == 1, IsTarget: r.IsTarget == 1}
			if reply.ReplyHead != wantHead || wantHead != (ReplyHead{Op: tc.op, Result: tc.result, MDSMapEpoch: 9, Safe: tc.safe, IsDentry: tc.isDentry, IsTarget: tc.isTarget}) ||
				len(reply.Trace) != r.TraceBytes || len(reply.Extra) != r.ExtraBytes || len(reply.Snap) != r.SnapBytes {
				t.Fatalf("reply head %+v, native receipt %+v", reply.ReplyHead, r)
			}

			trace, err := DecodeTrace(reply.Trace, reply.IsDentry, reply.IsTarget)
			if err != nil {
				t.Fatal(err)
			}
			if r.Trace == nil {
				if reply.Trace != nil || trace != (Trace{}) {
					t.Fatal("trace without a native trace receipt", trace)
				}
			} else {
				if reply.IsDentry {
					checkNativeInode(t, "dir inode", trace.DirInode, r.Trace.DirInode)
					if r.Trace.Dir == nil || r.Trace.Lease == nil || r.Trace.DentryName == nil ||
						!reflect.DeepEqual(*trace.Dir, dirStatFromReceipt(r.Trace.Dir)) || trace.DentryName != *r.Trace.DentryName ||
						!reflect.DeepEqual(*trace.Lease, leaseFromReceipt(t, r.Trace.Lease)) {
						t.Fatalf("dentry trace %+v / %+v / %q, native %+v", trace.Dir, trace.Lease, trace.DentryName, r.Trace)
					}
				} else if trace.DirInode != nil || r.Trace.DirInode != nil || r.Trace.Dir != nil || r.Trace.Lease != nil || r.Trace.DentryName != nil {
					t.Fatal("unexpected dentry records")
				}
				if reply.IsTarget {
					checkNativeInode(t, "target inode", trace.Target, r.Trace.Target)
				} else if trace.Target != nil || r.Trace.Target != nil {
					t.Fatal("unexpected target record")
				}
			}

			if r.Readdir == nil {
				if reply.Extra != nil || tc.entries != 0 {
					t.Fatal("extra without a native readdir receipt")
				}
			} else {
				readdir, err := DecodeReaddir(reply.Extra)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(readdir.Dir, dirStatFromReceipt(&r.Readdir.Dir)) || readdir.Flags != r.Readdir.Flags ||
					readdir.Flags != ReaddirFragEnd|ReaddirFragComplete || len(readdir.Entries) != int(r.Readdir.NumDN) || len(readdir.Entries) != tc.entries {
					t.Fatalf("readdir %+v, native %+v", readdir, r.Readdir)
				}
				for i, e := range r.Readdir.Entries {
					got := readdir.Entries[i]
					if got.Name != e.Name || !reflect.DeepEqual(got.Lease, leaseFromReceipt(t, &e.Lease)) {
						t.Fatalf("readdir entry %d %+v, native %+v", i, got, e)
					}
					checkNativeInode(t, "readdir inode "+e.Name, &got.Inode, &e.Inode)
				}
			}

			realms, err := DecodeSnapTrace(reply.Snap)
			if err != nil {
				t.Fatal(err)
			}
			if len(realms) != len(r.SnapRealms) || len(realms) != tc.realms {
				t.Fatalf("snap realms %+v, native %+v", realms, r.SnapRealms)
			}
			for i, n := range r.SnapRealms {
				if int(n.NumSnaps) != len(n.Snaps) || int(n.NumPrior) != len(n.Prior) {
					t.Fatal("native realm counts differ", n)
				}
				want := SnapRealm{Ino: n.Ino, Created: n.Created, Parent: n.Parent, ParentSince: n.ParentSince, Seq: n.Seq,
					LastModified: n.LastModified.time(), ChangeAttr: n.ChangeAttr}
				if len(n.Snaps) != 0 {
					want.Snaps = n.Snaps
				}
				if len(n.Prior) != 0 {
					want.PriorParentSnaps = n.Prior
				}
				if !reflect.DeepEqual(realms[i], want) {
					t.Fatalf("realm %d\n got %+v\nnative %+v", i, realms[i], want)
				}
			}
		})
	}
}

// The distinctive lookup target exercises every InodeStat field with a
// nonzero value, so a misplaced Go field cannot pass on zero defaults.
func TestNativeReplyTargetFieldsAreDistinctive(t *testing.T) {
	m := nativeRequestMessage(t, "mds-request-native-reply-lookup", ClientReplyMessage, 1)
	reply, err := DecodeClientReply(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := DecodeTrace(reply.Trace, reply.IsDentry, reply.IsTarget)
	if err != nil {
		t.Fatal(err)
	}
	v := reflect.ValueOf(*trace.Target)
	for i := range v.NumField() {
		field := v.Type().Field(i)
		switch field.Name {
		case "Layout", "Cap":
			sub := v.Field(i)
			for j := range sub.NumField() {
				// cas_hash, object_stripe_unit and unused are always zero
				// from file_layout_t::to_legacy.
				if name := sub.Type().Field(j).Name; sub.Field(j).IsZero() && name != "CASHash" && name != "ObjectStripeUnit" && name != "Unused" {
					t.Errorf("target %s.%s is zero", field.Name, name)
				}
			}
		case "OptionalMetadata":
			// Zero-option multiton by design.
		default:
			if v.Field(i).IsZero() {
				t.Errorf("target %s is zero", field.Name)
			}
		}
	}
}

func TestNativeRequestProvenanceMatchesCurrentSourcesAndSegments(t *testing.T) {
	proof := readNativeRequestProof(t)
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Terminated || !proof.Decoder || !proof.Canonical || len(proof.Fixtures) != 7 {
		t.Fatal("native request proof lacks pinned source, cleanup or decoder")
	}
	for _, producer := range []struct{ hash, file string }{{proof.Wrapper, "mds_request_native_encoder.cc"}, {proof.Driver, "mds_request_native_encoder.py"}, {proof.Launcher, "mds_request_native_encoder.py"}} {
		raw, err := os.ReadFile("../../tools/" + producer.file)
		if err != nil {
			t.Fatal(err)
		}
		if producer.hash != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("native request producer hash differs", producer.file)
		}
	}
	for name, fixture := range proof.Fixtures {
		receipts := 0
		for _, present := range []bool{fixture.Request != nil, fixture.Forward != nil, fixture.Reply != nil} {
			if present {
				receipts++
			}
		}
		if receipts != 1 {
			t.Fatal("native request vector lacks exactly one decoder receipt", name)
		}
		for segment, digest := range map[string]string{"header": fixture.Header, "front": fixture.Front, "data": fixture.Data} {
			if digest != fmt.Sprintf("%x", sha256.Sum256(nativeMapBytes(t, name, segment))) {
				t.Fatal("native request segment differs", name, segment)
			}
		}
	}
}
