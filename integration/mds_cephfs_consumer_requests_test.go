package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// nativeMDSCephFSConsumerPlan is the consumer-request plan of the completion
// fixture (integration/fixture/mds_cephfs_consumer.go).
type nativeMDSCephFSConsumerPlan struct {
	Dir        string   `json:"dir"`
	File       string   `json:"file"`
	Snapshots  []string `json:"snapshots"`
	ObjectSize uint64   `json:"object_size"`
	Objects    uint64   `json:"objects"`
	Overwrite  struct {
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
	} `json:"overwrite"`
	Append struct {
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
	} `json:"append"`
	MaxObjects uint64 `json:"max_objects"`
	Vxattrs    []struct {
		Target string `json:"target"`
		Name   string `json:"name"`
	} `json:"vxattrs"`
}

type nativeMDSCephFSConsumerOutput struct {
	Action         string                `json:"action"`
	Path           string                `json:"path"`
	NativeGlobalID uint64                `json:"native_global_id"`
	DiffGlobalID   uint64                `json:"diff_global_id"`
	Stat           *nativeMDSCephFSStat  `json:"stat"`
	FileStat       *nativeMDSCephFSStat  `json:"file_stat"`
	Snapshots      []nativeMDSCephFSStat `json:"snapshot_file_stats"`
	Written        [][2]uint64           `json:"written"`
	Vxattrs        []struct {
		Target   string `json:"target"`
		Name     string `json:"name"`
		Errno    int    `json:"errno"`
		ValueHex string `json:"value_hex"`
	} `json:"vxattrs"`
	Pages []struct {
		Return int         `json:"return"`
		Blocks [][2]uint64 `json:"blocks"`
	} `json:"pages"`
}

// cephfsErrNoData is the Linux ENODATA the MDS answers for an absent vxattr.
const cephfsErrNoData = -61

// cephfsBlockdiffArgs packs ceph_mds_request_args.blockdiff: the scan index
// and the number of data objects to scan (the MDS caps it at its
// mds_file_blockdiff_max_concurrent_object_scans).
func cephfsBlockdiffArgs(scanIdx, maxObjects uint64) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint64(a[0:], scanIdx)
	binary.LittleEndian.PutUint64(a[8:], maxObjects)
	return a
}

// decodeCephFSVxattrExtra is the consumer's decoder of a GETVXATTR reply's
// extra: a v1/compat1 envelope around one string
// (Server::handle_client_getvxattr at the pinned tag).
func decodeCephFSVxattrExtra(extra []byte) (string, error) {
	if len(extra) < 10 || extra[0] != 1 || extra[1] != 1 {
		return "", errors.New("GETVXATTR extra lacks its v1/compat1 envelope")
	}
	if n := binary.LittleEndian.Uint32(extra[2:]); uint64(n) != uint64(len(extra)-6) {
		return "", errors.New("GETVXATTR envelope length differs from the extra")
	}
	if n := binary.LittleEndian.Uint32(extra[6:]); uint64(n) != uint64(len(extra)-10) {
		return "", errors.New("GETVXATTR string length differs from the envelope")
	}
	return string(extra[10:]), nil
}

// cephfsBlockdiffPage is one decoded FILE_BLOCKDIFF reply extra (BlockDiff:
// rval, the next scan index and the changed extents as an interval set).
type cephfsBlockdiffPage struct {
	Rval    int32
	ScanIdx uint64
	Blocks  [][2]uint64
}

// decodeCephFSBlockdiffExtra is the consumer's decoder of a FILE_BLOCKDIFF
// reply's extra: v1/compat1, s32 rval, u64 scan_idx and the interval set as a
// u32 count of (u64 offset, u64 length) pairs (BlockDiff::encode,
// interval_set encoding at the pinned tag). The extents must be ordered,
// disjoint and nonempty, as an interval set is.
func decodeCephFSBlockdiffExtra(extra []byte) (cephfsBlockdiffPage, error) {
	var page cephfsBlockdiffPage
	if len(extra) < 22 || extra[0] != 1 || extra[1] != 1 {
		return page, errors.New("FILE_BLOCKDIFF extra lacks its v1/compat1 envelope")
	}
	if n := binary.LittleEndian.Uint32(extra[2:]); uint64(n) != uint64(len(extra)-6) {
		return page, errors.New("FILE_BLOCKDIFF envelope length differs from the extra")
	}
	page.Rval, page.ScanIdx = int32(binary.LittleEndian.Uint32(extra[6:])), binary.LittleEndian.Uint64(extra[10:])
	count := binary.LittleEndian.Uint32(extra[18:])
	if uint64(count) != uint64(len(extra)-22)/16 || (len(extra)-22)%16 != 0 {
		return page, errors.New("FILE_BLOCKDIFF extent count differs from the extra")
	}
	page.Blocks = [][2]uint64{}
	var end uint64
	for i := range int(count) {
		at := 22 + 16*i
		b := [2]uint64{binary.LittleEndian.Uint64(extra[at:]), binary.LittleEndian.Uint64(extra[at+8:])}
		if b[1] == 0 || b[0]+b[1] < b[0] || i > 0 && b[0] <= end {
			return page, errors.New("FILE_BLOCKDIFF extents are not an interval set")
		}
		end = b[0] + b[1]
		page.Blocks = append(page.Blocks, b)
	}
	return page, nil
}

// TestCephFSConsumerReplyExtraDecoders checks the test's GETVXATTR and
// FILE_BLOCKDIFF extra decoders on hand-built bytes laid out as the pinned
// encoders write them, and their refusals.
func TestCephFSConsumerReplyExtraDecoders(t *testing.T) {
	vx := []byte{1, 1, 5, 0, 0, 0, 1, 0, 0, 0, '0'}
	if s, err := decodeCephFSVxattrExtra(vx); err != nil || s != "0" {
		t.Fatal("vxattr extra", s, err)
	}
	for _, bad := range [][]byte{nil, vx[:9], append([]byte{2}, vx[1:]...), append(bytes.Clone(vx), 0), {1, 1, 5, 0, 0, 0, 2, 0, 0, 0, '0'}} {
		if _, err := decodeCephFSVxattrExtra(bad); err == nil {
			t.Fatalf("vxattr extra %x accepted", bad)
		}
	}
	page := func(rval int32, scan uint64, blocks ...[2]uint64) []byte {
		b := []byte{1, 1, 0, 0, 0, 0}
		b = binary.LittleEndian.AppendUint32(b, uint32(rval))
		b = binary.LittleEndian.AppendUint64(b, scan)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(blocks)))
		for _, x := range blocks {
			b = binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(b, x[0]), x[1])
		}
		binary.LittleEndian.PutUint32(b[2:], uint32(len(b)-6))
		return b
	}
	got, err := decodeCephFSBlockdiffExtra(page(1, 2, [2]uint64{73728, 4096}, [2]uint64{262144, 65536}))
	if err != nil || !reflect.DeepEqual(got, cephfsBlockdiffPage{1, 2, [][2]uint64{{73728, 4096}, {262144, 65536}}}) {
		t.Fatal("blockdiff extra", got, err)
	}
	if got, err := decodeCephFSBlockdiffExtra(page(0, 4)); err != nil || got.Rval != 0 || got.ScanIdx != 4 || len(got.Blocks) != 0 {
		t.Fatal("empty blockdiff extra", got, err)
	}
	truncated := page(1, 2, [2]uint64{1, 2})
	for _, bad := range [][]byte{truncated[:len(truncated)-1], page(1, 2, [2]uint64{8, 0}), page(1, 2, [2]uint64{8, 8}, [2]uint64{12, 1}), page(1, 2, [2]uint64{^uint64(0), 2}),
		append([]byte{1, 2}, truncated[2:]...)} {
		if _, err := decodeCephFSBlockdiffExtra(bad); err == nil {
			t.Fatalf("blockdiff extra %x accepted", bad)
		}
	}
}

// consumerReply is one Go request and its reply as received.
type consumerReply struct {
	Name        string
	Op          uint32
	Transaction uint64
	Result      int32
	Safe        bool
	ExtraHex    string `json:",omitempty"`
	Error       string `json:",omitempty"`
}

// TestCephMDSCephFSConsumerRequestsIntegration sends GETVXATTR and
// FILE_BLOCKDIFF from a default CephFS session, through the raw Submit path,
// against a live Tentacle MDS, with the stock client as the independent
// oracle. The session declares neither op_getvxattr (bit 17) nor blockdiff
// (bit 23), as its native session record shows, and the MDS still answers
// both: at the pinned tag it reads neither bit, while the stock client
// refuses each request locally unless the MDS advertises it, which this
// MDS's OPEN does. The library adds nothing for either request: the consumer
// packs the vxattr name as the second path, the blockdiff scan index and page
// size into the request arguments, the snapshot files as
// <directory inode>/<empty snapdir component>/<snapshot>/<file> paths, and
// decodes each reply's extra itself.
//
// The native oracle sets a 64 KiB object layout on an empty Go-created
// directory, writes four objects of a file, takes a snapshot, overwrites a
// range inside the second object, appends a fifth object and takes a second
// snapshot. Go reads nine vxattrs of the directory and the file; each value
// equals the stock client's getxattr of it (which the stock client also asks
// the MDS for), and the two the MDS reports absent are -ENODATA for both,
// as are a non-ceph name and an unknown ceph name. Go then pages the diff
// between the snapshots two objects at a time; every page (rval, extents)
// equals the corresponding page the stock client's ceph_file_blockdiff
// returned with the same page size, the scan index advances by the page size,
// and the union of the extents is exactly the overwritten range and the
// appended tail. A diff of the same snapshot twice ends at once with no
// extents, and a diff of the snapshotted directories is -EINVAL. The session
// record is unchanged afterwards; the oracle removes the snapshots and the
// file and Go removes both directories.
//
// Not exercised: the MDS-side feature advertisement missing (a Tentacle MDS
// always advertises both bits), a diff across a layout change, holes, files
// larger than one page per object scan bound, vxattrs that need a
// filesystem setting (pins, quiesce, subvolumes) and multiple ranks.
//
//scenario:row mds-cephfs-consumer-requests.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsconsumerrequests key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-consumer-requests
//scenario:row mds-cephfs-consumer-requests.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsconsumerrequests key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-consumer-requests
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated writes a file, takes two snapshots and sets a directory layout in the owned filesystem; a failed step leaves them
func TestCephMDSCephFSConsumerRequestsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned CephFS consumer-request acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("consumer-request acceptance requires the owned completion fixture")
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(stop)
	var oracle nativeMDSCephFSCompletionOracle
	var fresh nativeMDSFresh
	var current nativeMDSCephFSTarget
	for _, read := range []struct {
		name   string
		target any
	}{{"mds-cephfs-completion-oracle.json", &oracle}, {"mds-fresh-oracle.json", &fresh}, {"mds-cephfs-completion-target.json", &current}} {
		if err := readMDSFreshJSON(filepath.Join(dir, read.name), read.target); err != nil {
			t.Fatal(err)
		}
	}
	identity, key := os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE")
	plan := oracle.Plan.Consumer
	const object = 64 << 10
	if oracle.Source != mdsFreshSource || oracle.Token != fresh.Token || len(oracle.Token) != 32 || oracle.FSID != fresh.FSID || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fresh.FSName || identity != "client.mds-cephfs-completion-"+oracle.Token || oracle.ClientIdentity != identity ||
		oracle.ClientCaps["mds"] != "allow rw fsname="+fresh.FSName || oracle.NativeCaps["mds"] != "allow rwps fsname="+fresh.FSName || oracle.Plan.Directory != "ceph-msgr-completion-"+oracle.Token ||
		plan.Dir != "bdiff" || plan.File != "file" || len(plan.Snapshots) != 2 || plan.ObjectSize != object || plan.Objects != 4 || plan.MaxObjects != 2 ||
		plan.Overwrite.Offset != object+8<<10 || plan.Overwrite.Length != 4<<10 || plan.Append.Offset != 4*object || plan.Append.Length != object || len(plan.Vxattrs) != 9 ||
		current.Source != mdsFreshSource || current.Token != oracle.Token || current.Adopted != 0 || current.MDS.GID != fresh.MDS.GID || current.MDS.Nonce != fresh.MDS.Nonce {
		t.Fatal("consumer-request oracle, native caps or closed plan differs")
	}
	control := &mdsCephFSCompletionControl{dir: dir, token: oracle.Token, fresh: fresh, mds: current.MDS, goGIDs: map[uint64]bool{}}
	target := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", current.MDS.Endpoint, current.MDS.Nonce)}
	dials := newRecoveryDials(false, current.MDS.Endpoint)
	client := recoveryClient(t, ctx, identity, key, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS = true, true })
	snap := client.Snapshot()
	control.goGIDs[snap.GlobalID] = true
	declared := client.MDSClientFeatures()
	if !bytes.Equal(declared, cephmsgr.MDSCephFSClientFeatures()) || cephmsgr.MDSFeatureBitsetHas(declared, 17) || cephmsgr.MDSFeatureBitsetHas(declared, 23) {
		t.Fatal("the default CephFS client declares op_getvxattr or blockdiff", declared)
	}
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-consumer", "root": "/"}
	var session *cephmsgr.MDSSession
	var agent *cephfsCapAgent
	var epoch uint32
	var dirIno, bdiffIno, fileIno uint64
	facts := map[string]any{"global_id": snap.GlobalID, "declared_hex": hex.EncodeToString(declared)}
	var replies []consumerReply
	t.Cleanup(func() {
		facts["mds_dials"], facts["osd_dials"] = dials.attempts(), dials.OSD.Load()
		if agent != nil {
			facts["agent"] = agent.snapshot()
		}
		data, err := json.Marshal(map[string]any{"source_commit": mdsFreshSource, "plan": plan, "facts": facts, "replies": replies, "native": control.receipts})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("consumer-request evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-consumer-requests-results.json"), data)
		}
		if err != nil {
			t.Error("preserve consumer-request evidence", err)
		}
	})
	req := func(t *testing.T, action string, args ...string) nativeMDSCephFSCompletionReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("consumer-request control", action, err)
		}
		return r
	}
	observe := func(t *testing.T) *nativeMDSSession {
		t.Helper()
		r := req(t, "observe", fmt.Sprint(snap.GlobalID))
		epoch = r.Map.Epoch
		return r.session(snap.GlobalID)
	}
	// submit sends one request and waits for its first reply, then makes a
	// successful unsafe reply safe. The caller checks the result.
	submit := func(t *testing.T, name string, r cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		r.MDSMapEpoch = epoch
		e := consumerReply{Name: name, Op: r.Op}
		call, err := session.Submit(ctx, r)
		if err != nil {
			e.Error = err.Error()
			replies = append(replies, e)
			t.Fatal("submit", name, err)
		}
		e.Transaction = call.Transaction()
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		first, err := call.Wait(wait)
		e.Result, e.Safe, e.ExtraHex = first.Result, first.Safe, hex.EncodeToString(first.Extra)
		if err != nil {
			e.Error = err.Error()
			replies = append(replies, e)
			t.Fatal("first reply", name, err)
		}
		replies = append(replies, e)
		trace, _, err := agent.applyReply(name, first)
		if err != nil {
			t.Fatal("decode reply", name, err)
		}
		if first.Result == 0 && !first.Safe {
			if err := session.FlushJournal(wait); err != nil {
				t.Fatal(err)
			}
			if safe, err := call.WaitSafe(wait); err != nil || safe.Result != 0 || !safe.Safe {
				t.Fatal("safe reply", name, safe.Result, err)
			}
		}
		return first, trace
	}
	vxattr := func(t *testing.T, ino uint64, name string) (string, int32) {
		t.Helper()
		first, _ := submit(t, "getvxattr "+name, cephmsgr.MDSRequest{Op: mds.OpGetVxattr, Path: cephmsgr.MDSFilePath{Ino: ino}, Path2: cephmsgr.MDSFilePath{Path: name}})
		if first.Op != mds.OpGetVxattr || !first.Safe {
			t.Fatal("GETVXATTR reply op or safety differs", name, first.Op, first.Safe)
		}
		if first.Result != 0 {
			if len(first.Extra) != 0 {
				t.Fatal("a refused GETVXATTR carries an extra", name, first.Extra)
			}
			return "", first.Result
		}
		value, err := decodeCephFSVxattrExtra(first.Extra)
		if err != nil {
			t.Fatal(name, err)
		}
		return value, 0
	}
	snapPath := func(i int, file bool) cephmsgr.MDSFilePath {
		// Client::file_blockdiff sends make_nosnap_relative_path: the live
		// directory inode, the empty snapdir component, the snapshot name and
		// the file.
		p := "/" + plan.Snapshots[i]
		if file {
			p += "/" + plan.File
		}
		return cephmsgr.MDSFilePath{Ino: bdiffIno, Path: p}
	}
	blockdiff := func(t *testing.T, label string, a, b cephmsgr.MDSFilePath) ([]cephfsBlockdiffPage, int32) {
		t.Helper()
		var pages []cephfsBlockdiffPage
		var scan uint64
		for len(pages) < 64 {
			first, _ := submit(t, fmt.Sprintf("blockdiff %s page %d", label, len(pages)), cephmsgr.MDSRequest{Op: mds.OpFileBlockdiff, Args: cephfsBlockdiffArgs(scan, plan.MaxObjects), Path: a, Path2: b})
			if first.Op != mds.OpFileBlockdiff || !first.Safe {
				t.Fatal("FILE_BLOCKDIFF reply op or safety differs", first.Op, first.Safe)
			}
			if first.Result != 0 {
				return pages, first.Result
			}
			page, err := decodeCephFSBlockdiffExtra(first.Extra)
			if err != nil {
				t.Fatal(label, err)
			}
			pages = append(pages, page)
			if page.Rval != 1 {
				return pages, 0
			}
			scan = page.ScanIdx
		}
		t.Fatal("FILE_BLOCKDIFF did not end within 64 pages", label)
		return nil, 0
	}

	if !t.Run("default_session_without_the_bits", func(t *testing.T) {
		connection, err := client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		if session, err = connection.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024}); err != nil {
			t.Fatal(err)
		}
		agent = startCephFSCapAgent(ctx, session)
		// The stock client sends each request only when the MDS advertises
		// its bit; a consumer gating the same way finds both here.
		advertised := session.Status().Open.SupportedFeatures
		if !cephmsgr.MDSFeatureBitsetHas(advertised, 17) || !cephmsgr.MDSFeatureBitsetHas(advertised, 23) {
			t.Fatal("the MDS does not advertise op_getvxattr and blockdiff", advertised)
		}
		native := observe(t)
		if err := checkNativeCephFSSession(native, identity, metadata); err != nil {
			t.Fatal(err)
		}
		facts["advertised_hex"], facts["native_session"] = hex.EncodeToString(advertised), native
		first, trace := submit(t, "mkdir owned directory", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: oracle.Plan.Directory}})
		if first.Result != 0 || trace.Target == nil {
			t.Fatal("owned directory mkdir differs", first.Result)
		}
		dirIno = trace.Target.Ino
		first, trace = submit(t, "mkdir blockdiff directory", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.Dir}})
		if first.Result != 0 || trace.Target == nil || trace.DentryName != plan.Dir {
			t.Fatal("blockdiff directory mkdir differs", first.Result)
		}
		bdiffIno = trace.Target.Ino
		facts["owned_dir_ino"], facts["bdiff_ino"] = dirIno, bdiffIno
	}) {
		return
	}

	var nativeRead *nativeMDSCephFSConsumerOutput
	if !t.Run("native_file_and_snapshots", func(t *testing.T) {
		prepared := req(t, "consumer-prepare").Consumer
		size := plan.Objects*object + plan.Append.Length
		want := [][2]uint64{{0, plan.Objects * object}, {plan.Overwrite.Offset, plan.Overwrite.Length}, {plan.Append.Offset, plan.Append.Length}}
		if prepared.Stat.Ino != bdiffIno || prepared.FileStat.Size != size || !reflect.DeepEqual(prepared.Written, want) || len(prepared.Snapshots) != 2 ||
			prepared.Snapshots[0].Size != plan.Objects*object || prepared.Snapshots[1].Size != size {
			t.Fatal("native preparation differs", prepared.Stat, prepared.FileStat, prepared.Written, prepared.Snapshots)
		}
		first, trace := submit(t, "lookup file", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: bdiffIno, Path: plan.File}})
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != prepared.FileStat.Ino || trace.Target.Size != size {
			t.Fatal("Go lookup of the native file differs", first.Result)
		}
		fileIno = trace.Target.Ino
		nativeRead = req(t, "consumer-read").Consumer
		if nativeRead.Stat.Ino != bdiffIno || nativeRead.FileStat.Ino != fileIno || len(nativeRead.Vxattrs) != len(plan.Vxattrs) {
			t.Fatal("native readback differs", nativeRead.Stat, nativeRead.FileStat)
		}
		facts["file_ino"] = fileIno
	}) {
		return
	}

	t.Run("getvxattr_equals_native", func(t *testing.T) {
		values := map[string]string{}
		for i, v := range plan.Vxattrs {
			ino := bdiffIno
			if v.Target == "file" {
				ino = fileIno
			}
			value, result := vxattr(t, ino, v.Name)
			n := nativeRead.Vxattrs[i]
			nativeValue, err := hex.DecodeString(n.ValueHex)
			if err != nil || n.Name != v.Name || n.Target != v.Target {
				t.Fatal("native vxattr record differs", n)
			}
			switch {
			case n.Errno == 61 && result != cephfsErrNoData, n.Errno == 0 && (result != 0 || value != string(nativeValue)):
				t.Errorf("%s %s: Go %d %q, stock client errno %d %q", v.Target, v.Name, result, value, n.Errno, nativeValue)
			}
			values[v.Target+" "+v.Name] = value
		}
		// The plan's own controls: the MDS reports a charmap only on a
		// directory with one, and a directory vxattr only on a directory.
		if values["dir ceph.dir.charmap"] != "" || nativeRead.Vxattrs[5].Errno != 61 || values["file ceph.dir.layout.json"] != "" || nativeRead.Vxattrs[8].Errno != 61 {
			t.Error("the ENODATA controls of the plan did not answer ENODATA")
		}
		if !strings.Contains(values["dir ceph.dir.layout.json"], `"object_size": 65536`) || values["dir ceph.dir.subvolume"] != "0" || values["dir ceph.dir.layout.pool_name"] != oracle.DataPoolName ||
			values["dir ceph.dir.layout.pool_id"] != fmt.Sprint(oracle.DataPool) {
			t.Error("the directory layout vxattrs do not show the native layout", values)
		}
		// Names the stock client would not send to the MDS: a non-ceph name
		// and a ceph name the MDS does not know.
		for _, name := range []string{"user.ceph-msgr", "ceph.dir.ceph-msgr-unknown"} {
			if _, result := vxattr(t, bdiffIno, name); result != cephfsErrNoData {
				t.Errorf("GETVXATTR %s answered %d, not -ENODATA", name, result)
			}
		}
		facts["vxattrs"] = values
	})

	t.Run("blockdiff_pages_equal_native", func(t *testing.T) {
		pages, result := blockdiff(t, "snapshots", snapPath(0, true), snapPath(1, true))
		if result != 0 || len(pages) != len(nativeRead.Pages) {
			t.Fatal("Go blockdiff page count differs from the stock client", result, len(pages), len(nativeRead.Pages))
		}
		var union [][2]uint64
		for i, page := range pages {
			n := nativeRead.Pages[i]
			if int(page.Rval) != n.Return || !reflect.DeepEqual(page.Blocks, append([][2]uint64{}, n.Blocks...)) {
				t.Errorf("page %d: Go rval %d extents %v, stock client %d %v", i, page.Rval, page.Blocks, n.Return, n.Blocks)
			}
			if next := min(uint64(i+1)*plan.MaxObjects, plan.Objects); page.ScanIdx != next {
				t.Errorf("page %d: scan index %d, want %d", i, page.ScanIdx, next)
			}
			union = append(union, page.Blocks...)
		}
		want := [][2]uint64{{plan.Overwrite.Offset, plan.Overwrite.Length}, {plan.Append.Offset, plan.Append.Length}}
		if !reflect.DeepEqual(union, want) || len(pages) != 3 {
			t.Error("the diff is not exactly the overwritten range and the appended tail", union)
		}
		same, result := blockdiff(t, "same snapshot", snapPath(0, true), snapPath(0, true))
		if result != 0 || len(same) != 1 || same[0].Rval != 0 || len(same[0].Blocks) != 0 || same[0].ScanIdx != 0 {
			t.Error("a diff of one snapshot with itself is not one empty final page", result, same)
		}
		if _, result := blockdiff(t, "directories", snapPath(0, false), snapPath(1, false)); result != -22 {
			t.Error("a diff of the snapshotted directories is not -EINVAL", result)
		}
		facts["blockdiff_pages"] = pages
		native := observe(t)
		if err := checkNativeCephFSSession(native, identity, metadata); err != nil {
			t.Error("the session record changed after the requests", err)
		}
	})

	t.Run("cleanup_remove_release_close", func(t *testing.T) {
		removed := req(t, "consumer-remove").Consumer
		if removed.Stat.Ino != bdiffIno || removed.FileStat.Exists {
			t.Fatal("native removal differs", removed.Stat, removed.FileStat)
		}
		for _, step := range []struct {
			base uint64
			name string
		}{{dirIno, plan.Dir}, {1, oracle.Plan.Directory}} {
			if first, _ := submit(t, "rmdir "+step.name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: step.base, Path: step.name}}); first.Result != 0 {
				t.Fatal("rmdir", step.name, first.Result)
			}
		}
		native, err := control.stat(ctx, "dir")
		if err != nil || native.Exists || native.Errno != 2 {
			t.Fatal("native owned directory remained", native, err)
		}
		items, barrier := agent.releaseAll()
		if len(items) != 0 {
			if err := session.ReleaseCaps(ctx, items, barrier); err != nil {
				t.Fatal(err)
			}
		}
		closing, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = session.Close(closing)
		cancel()
		if err != nil || session.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("consumer-request close unconfirmed", err)
		}
		if observe(t) != nil {
			t.Fatal("consumer-request session remains in the native map")
		}
		if dials.OSD.Load() != 0 {
			t.Fatal("the consumer-request test issued object I/O")
		}
	})
}
