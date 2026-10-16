package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSDataIntegration accepts CephFS file data I/O through the
// Go client's MDS session and its own OSD requests against live Tentacle
// daemons, compared with a native LibCephFS client: striped writes with a cap
// flush, a native sparse file read through the layout, a native truncate and
// its TRUNC push, a buffered write flushed on a concurrent reader's revoke,
// and a write after a snapshot that clones the objects. File layout striping,
// object naming, snap contexts, size and time bookkeeping and cap flush
// decisions are test-only consumer logic. See
// integration/fixture/MDS_CEPHFS_DATA_FIXTURE.md and
// MDS_CEPHFS_DATA_VALIDATION.md.

// cephfsStriping restates the striping fields of file_layout_t (test-only).
type cephfsStriping struct{ StripeUnit, StripeCount, ObjectSize uint64 }

// cephfsBufferExtent is a range of the file request, relative to its start.
type cephfsBufferExtent struct{ Offset, Length uint64 }

// cephfsObjectExtent is one object range and the request ranges it carries.
type cephfsObjectExtent struct {
	ObjectNo, Offset, Length uint64
	Buffers                  []cephfsBufferExtent
}

// fileToExtents restates Striper::file_to_extents at the pinned source: the
// range is cut at stripe-unit boundaries, each block lands in object
// objectset*stripe_count+stripepos at (stripeno%stripes_per_object)*su, and
// a block continuing an object's last extent extends it. Extents stay sorted
// by object number (upper_bound insertion), as natively.
func (l cephfsStriping) fileToExtents(offset, length uint64) []cephfsObjectExtent {
	su, sc, os := l.StripeUnit, l.StripeCount, l.ObjectSize
	if sc == 1 {
		su = os
	}
	stripesPerObject := os / su
	var out []cephfsObjectExtent
	for cur, left := offset, length; left > 0; {
		blockno := cur / su
		stripeno, stripepos := blockno/sc, blockno%sc
		objectno := stripeno/stripesPerObject*sc + stripepos
		blockOff := cur % su
		xOffset, xLen := (stripeno%stripesPerObject)*su+blockOff, min(left, su-blockOff)
		i := sort.Search(len(out), func(i int) bool { return out[i].ObjectNo > objectno })
		if i > 0 && out[i-1].ObjectNo == objectno && out[i-1].Offset+out[i-1].Length == xOffset {
			out[i-1].Length += xLen
			out[i-1].Buffers = append(out[i-1].Buffers, cephfsBufferExtent{cur - offset, xLen})
		} else {
			out = slices.Insert(out, i, cephfsObjectExtent{ObjectNo: objectno, Offset: xOffset, Length: xLen, Buffers: []cephfsBufferExtent{{cur - offset, xLen}}})
		}
		left -= xLen
		cur += xLen
	}
	return out
}

// extentToFile restates Striper::extent_to_file: the file ranges one object
// range covers.
func (l cephfsStriping) extentToFile(objectno, off, length uint64) [][2]uint64 {
	su, sc := l.StripeUnit, l.StripeCount
	stripesPerObject := l.ObjectSize / su
	var out [][2]uint64
	offInBlock := off % su
	for length > 0 {
		stripepos, objectsetno := objectno%sc, objectno/sc
		stripeno := off/su + objectsetno*stripesPerObject
		blockno := stripeno*sc + stripepos
		n := min(length, su-offInBlock)
		out = append(out, [2]uint64{blockno*su + offInBlock, n})
		offInBlock = 0
		off += n
		length -= n
	}
	return out
}

func cephfsObjectName(ino, objectno uint64) string { return fmt.Sprintf("%x.%08x", ino, objectno) }

// cephfsTrunc is an inode's truncate state (InodeStat or a TRUNC push), the
// truncate_size and truncate_seq the native client sends with every extent
// operation (Client::_read_sync and Client::_write -> Filer::read_trunc and
// Filer::write_trunc).
type cephfsTrunc struct {
	Size uint64
	Seq  uint32
}

// objectTruncateSize restates Striper::object_truncate_size (Striper.cc
// 309-347): the file truncate size as an in-object size. 0 and all ones are
// passed unchanged.
func (l cephfsStriping) objectTruncateSize(objectno, size uint64) uint64 {
	if size == 0 || size == math.MaxUint64 {
		return size
	}
	stripesPerObject := l.ObjectSize / l.StripeUnit
	objectSet := objectno / l.StripeCount
	truncSet := size / l.ObjectSize / l.StripeCount
	switch {
	case objectSet > truncSet:
		return 0
	case objectSet < truncSet:
		return l.ObjectSize
	}
	block := size / l.StripeUnit
	stripe := block / l.StripeCount
	truncObject := truncSet*l.StripeCount + block%l.StripeCount
	switch {
	case objectno < truncObject:
		return (stripe%stripesPerObject + 1) * l.StripeUnit
	case objectno > truncObject:
		return stripe % stripesPerObject * l.StripeUnit
	}
	return stripe%stripesPerObject*l.StripeUnit + size%l.StripeUnit
}

// wire is the pair the Go client sends for one object. A never-truncated
// inode carries truncate_size all ones and truncate_seq 1 (Server.cc
// 3639-3640), which the native client sends unchanged and the OSD rewrites
// to (0, 0) before executing (PrimaryLogPG.cc 6088-6093). cephmsgr refuses
// that pair, so the test sends the rewritten (0, 0) the OSD executes.
func (c cephfsTrunc) wire(l cephfsStriping, objectno uint64) (uint64, uint32) {
	if c.Seq == 1 && c.Size == math.MaxUint64 {
		return 0, 0
	}
	return l.objectTruncateSize(objectno, c.Size), c.Seq
}

// cephfsDataStream is the plan payload: SHA-256 of the seed, then of each
// previous block, concatenated (the fixture's native driver computes the
// same bytes).
func cephfsDataStream(seed string, n uint64) []byte {
	out := make([]byte, 0, n+sha256.Size)
	block := sha256.Sum256([]byte(seed))
	for uint64(len(out)) < n {
		out = append(out, block[:]...)
		block = sha256.Sum256(block[:])
	}
	return out[:n]
}

// cephfsSnapContext restates the client SnapRealm::build_snap_context over
// decoded realms: prior parent snaps, the parent's snaps since parent_since
// and the realm's own snaps, descending, with the larger sequence.
func cephfsSnapContext(realms map[uint64]cephmsgr.MDSSnapRealm, ino uint64) (cephmsgr.OSDSnapContext, error) {
	var walk func(ino uint64, depth int) (cephmsgr.OSDSnapContext, error)
	walk = func(ino uint64, depth int) (cephmsgr.OSDSnapContext, error) {
		r, ok := realms[ino]
		if !ok || depth > 16 {
			return cephmsgr.OSDSnapContext{}, fmt.Errorf("no snap trace describes realm %#x", ino)
		}
		set := map[uint64]bool{}
		seq := r.Seq
		for _, s := range r.PriorParentSnaps {
			set[s] = true
		}
		if r.Parent != 0 {
			parent, err := walk(r.Parent, depth+1)
			if err != nil {
				return parent, err
			}
			for _, s := range parent.Snapshots {
				if s >= r.ParentSince {
					set[s] = true
				}
			}
			seq = max(seq, parent.Sequence)
		}
		for _, s := range r.Snaps {
			set[s] = true
		}
		var snaps []uint64
		for s := range set {
			snaps = append(snaps, s)
		}
		slices.SortFunc(snaps, func(a, b uint64) int { return -compareUint64(a, b) })
		return cephmsgr.OSDSnapContext{Sequence: seq, Snapshots: snaps}, nil
	}
	return walk(ino, 0)
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cephfsDataDigest(data []byte) nativeMDSCephFSDataDigest {
	sum := sha256.Sum256(data)
	d := nativeMDSCephFSDataDigest{Length: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	for i := 0; i < len(data); i += 16384 {
		chunk := sha256.Sum256(data[i:min(len(data), i+16384)])
		d.Chunks = append(d.Chunks, hex.EncodeToString(chunk[:]))
	}
	return d
}

func cephfsMDSTime(t time.Time) cephmsgr.MDSTime {
	return cephmsgr.MDSTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
}

func TestCephFSStriperRestatement(t *testing.T) {
	l := cephfsStriping{16384, 2, 65536}
	// 300 KiB + 123 bytes: object sets 0 and 1 in full, set 2 partially.
	ext := l.fileToExtents(0, 307323)
	if len(ext) != 6 {
		t.Fatal("object count differs", ext)
	}
	want := []struct{ obj, off, length uint64 }{{0, 0, 65536}, {1, 0, 65536}, {2, 0, 65536}, {3, 0, 65536}, {4, 0, 28795}, {5, 0, 16384}}
	for i, w := range want {
		if ext[i].ObjectNo != w.obj || ext[i].Offset != w.off || ext[i].Length != w.length {
			t.Fatal("object extent differs", i, ext[i], w)
		}
	}
	if !slices.Equal(ext[0].Buffers, []cephfsBufferExtent{{0, 16384}, {32768, 16384}, {65536, 16384}, {98304, 16384}}) ||
		!slices.Equal(ext[4].Buffers, []cephfsBufferExtent{{262144, 16384}, {294912, 12411}}) {
		t.Fatal("buffer extents differ", ext[0].Buffers, ext[4].Buffers)
	}
	// An unaligned range inside one object set: blocks 6 to 10.
	ext = l.fileToExtents(100000, 80000)
	got := map[uint64][2]uint64{}
	for _, e := range ext {
		got[e.ObjectNo] = [2]uint64{e.Offset, e.Length}
	}
	if len(ext) != 4 || got[0] != [2]uint64{50848, 14688} || got[1] != [2]uint64{49152, 16384} || got[2] != [2]uint64{0, 32544} || got[3] != [2]uint64{0, 16384} {
		t.Fatal("unaligned extents differ", ext)
	}
	// Every byte maps to exactly one object byte and back.
	for _, layout := range []cephfsStriping{l, {4096, 3, 16384}, {65536, 1, 65536}} {
		for _, r := range [][2]uint64{{0, 1}, {1, 200000}, {12345, 99999}, {65535, 2}} {
			covered := uint64(0)
			for _, e := range layout.fileToExtents(r[0], r[1]) {
				var total uint64
				for _, b := range e.Buffers {
					total += b.Length
				}
				back := layout.extentToFile(e.ObjectNo, e.Offset, e.Length)
				if total != e.Length || len(back) != len(e.Buffers) {
					t.Fatal("extent and buffer lengths differ", layout, r, e, back)
				}
				for i, b := range e.Buffers {
					if back[i] != [2]uint64{r[0] + b.Offset, b.Length} {
						t.Fatal("extent_to_file is not the inverse", layout, r, e, back)
					}
				}
				covered += e.Length
			}
			if covered != r[1] {
				t.Fatal("range coverage differs", layout, r, covered)
			}
		}
	}
	// Striper::object_truncate_size for the acceptance layout and the
	// native truncate to 280000: objects of earlier object sets keep the
	// object size, object 2 holds the cut (17856), the rest 0.
	acceptance := cephfsStriping{65536, 2, 131072}
	for objectno, want := range []uint64{131072, 131072, 17856, 0, 0, 0, 0, 0, 0, 0} {
		if got := acceptance.objectTruncateSize(uint64(objectno), 280000); got != want {
			t.Fatal("object truncate size differs", objectno, got, want)
		}
		for _, end := range []uint64{0, math.MaxUint64} {
			if acceptance.objectTruncateSize(uint64(objectno), end) != end {
				t.Fatal("0 and all-ones truncate sizes are not passed unchanged")
			}
		}
	}
	// Inside object set 0: block 6 (stripe 3, position 0) of l at 100000;
	// object 0 holds the cut (the extent start above), object 1 three full
	// stripe units, set 1 nothing.
	for objectno, want := range []uint64{50848, 49152, 0, 0, 0} {
		if got := l.objectTruncateSize(uint64(objectno), 100000); got != want {
			t.Fatal("object truncate size inside a set differs", objectno, got, want)
		}
	}
	if size, seq := (cephfsTrunc{math.MaxUint64, 1}).wire(acceptance, 2); size != 0 || seq != 0 {
		t.Fatal("never-truncated inode not sent as the OSD's (0, 0)")
	}
	if size, seq := (cephfsTrunc{280000, 2}).wire(acceptance, 2); size != 17856 || seq != 2 {
		t.Fatal("truncated inode wire fields differ")
	}
	if s := cephfsDataStream("seed", 70); len(s) != 70 || !bytes.Equal(s[32:64], func() []byte { b := sha256.Sum256(s[:32]); return b[:] }()) {
		t.Fatal("payload chain differs")
	}
	realms := map[uint64]cephmsgr.MDSSnapRealm{
		1:     {Ino: 1, Seq: 1},
		0x100: {Ino: 0x100, Parent: 1, ParentSince: 2, Seq: 4, Snaps: []uint64{4, 2}, PriorParentSnaps: []uint64{1}},
	}
	if c, err := cephfsSnapContext(realms, 0x100); err != nil || c.Sequence != 4 || !slices.Equal(c.Snapshots, []uint64{4, 2, 1}) {
		t.Fatal("snap context restatement differs", c, err)
	}
	if _, err := cephfsSnapContext(realms, 7); err == nil {
		t.Fatal("an unknown realm produced a snap context")
	}
}

type nativeMDSCephFSDataPlan struct {
	Directory      string            `json:"directory"`
	Snapshot       string            `json:"snapshot"`
	Files          []string          `json:"files"`
	StripeUnit     uint64            `json:"stripe_unit"`
	StripeCount    uint64            `json:"stripe_count"`
	ObjectSize     uint64            `json:"object_size"`
	Seeds          map[string]string `json:"seeds"`
	GoFileSize     uint64            `json:"go_file_size"`
	NativeExtents  [][2]uint64       `json:"native_extents"`
	NativeSize     uint64            `json:"native_size"`
	TruncWriteSize uint64            `json:"trunc_write_size"`
	TruncSize      uint64            `json:"trunc_size"`
	SharedSize     uint64            `json:"shared_size"`
	SnapV1Size     uint64            `json:"snap_v1_size"`
	SnapV2Offset   uint64            `json:"snap_v2_offset"`
	SnapV2Size     uint64            `json:"snap_v2_size"`
	ObjectCounts   map[string]int    `json:"object_counts"`
}

type nativeMDSCephFSDataOracle struct {
	Source            string                   `json:"source_commit"`
	Token             string                   `json:"token"`
	FSID              string                   `json:"fsid"`
	FSCID             int32                    `json:"fscid"`
	FSName            string                   `json:"fs_name"`
	MDSGID            uint64                   `json:"mds_gid"`
	ClientIdentity    string                   `json:"client_identity"`
	NativeIdentity    string                   `json:"native_identity"`
	ClientCaps        map[string]string        `json:"client_caps"`
	NativeCaps        map[string]string        `json:"native_caps"`
	Plan              nativeMDSCephFSDataPlan  `json:"plan"`
	BaselineBlocklist []nativeMDSSessionsBlock `json:"baseline_blocklist"`
	DataPool          int64                    `json:"data_pool"`
	DataPoolName      string                   `json:"data_pool_name"`
	DirLayout         string                   `json:"dir_layout"`
}

type nativeMDSCephFSDataStat struct {
	Exists  bool   `json:"exists"`
	Errno   int    `json:"errno"`
	Ino     uint64 `json:"ino"`
	Mode    uint32 `json:"mode"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
	NLink   uint32 `json:"nlink"`
	Size    uint64 `json:"size"`
	MTime   int64  `json:"mtime"`
	Version uint64 `json:"version"`
}

type nativeMDSCephFSDataDigest struct {
	Length uint64   `json:"length"`
	SHA256 string   `json:"sha256"`
	Chunks []string `json:"chunks"`
}

type nativeMDSCephFSDataClone struct {
	Head      bool        `json:"head"`
	ID        uint64      `json:"id"`
	Snapshots []uint64    `json:"snapshots"`
	Size      uint64      `json:"size"`
	Overlaps  [][2]uint64 `json:"overlaps"`
}

type nativeMDSCephFSDataObject struct {
	ObjNo  int    `json:"objno"`
	Name   string `json:"name"`
	Exists bool   `json:"exists"`
	Size   uint64 `json:"size"`
	Snaps  *struct {
		Seq    uint64                     `json:"seq"`
		Clones []nativeMDSCephFSDataClone `json:"clones"`
	} `json:"snaps"`
}

type nativeMDSCephFSDataOutput struct {
	Action         string                      `json:"action"`
	Path           string                      `json:"path"`
	NativeGlobalID uint64                      `json:"native_global_id"`
	Stat           *nativeMDSCephFSDataStat    `json:"stat"`
	Layout         string                      `json:"layout"`
	LayoutAbsent   bool                        `json:"layout_absent"`
	Written        uint64                      `json:"written"`
	Read           *nativeMDSCephFSDataDigest  `json:"read"`
	TimingsNS      map[string]int64            `json:"timings_ns"`
	Entries        []nativeMDSCephFSEntry      `json:"entries"`
	Objects        []nativeMDSCephFSDataObject `json:"objects"`
}

type nativeMDSCephFSDataReceipt struct {
	Source          string                     `json:"source_commit"`
	Token           string                     `json:"token"`
	RequestID       int64                      `json:"request_id"`
	Action          string                     `json:"action"`
	Target          string                     `json:"target"`
	GlobalID        uint64                     `json:"global_id"`
	FSID            string                     `json:"fsid"`
	FSCID           int32                      `json:"fscid"`
	FSName          string                     `json:"fs_name"`
	MDSGID          uint64                     `json:"mds_gid"`
	MDSEndpoint     string                     `json:"mds_endpoint"`
	MDSNonce        uint32                     `json:"mds_nonce"`
	MDSIncarnation  int32                      `json:"mds_incarnation"`
	Rank            int32                      `json:"rank"`
	IdentityChecked bool                       `json:"identity_checked"`
	Map             *nativeMDSSessionsMap      `json:"map"`
	DaemonEpoch     uint32                     `json:"daemon_mdsmap_epoch"`
	Sessions        []nativeMDSSession         `json:"sessions"`
	Blocklist       []nativeMDSSessionsBlock   `json:"blocklist"`
	Native          *nativeMDSCephFSDataOutput `json:"native"`
	Process         json.RawMessage            `json:"native_process,omitempty"`
	Mutation        *nativeMDSCephFSMutation   `json:"mutation"`
	Complete        bool                       `json:"complete"`
	Error           string                     `json:"error"`
	ElapsedMillis   int64                      `json:"elapsed_ms"`
	ObservedUTC     string                     `json:"observed_utc"`
}

func (r nativeMDSCephFSDataReceipt) session(gid uint64) *nativeMDSSession {
	for i := range r.Sessions {
		if r.Sessions[i].ID == gid {
			return &r.Sessions[i]
		}
	}
	return nil
}

// mdsCephFSDataControl drives the closed fixture control. One request is
// outstanding at a time.
type mdsCephFSDataControl struct {
	mu       sync.Mutex
	dir      string
	oracle   nativeMDSCephFSDataOracle
	fresh    nativeMDSFresh
	incarn   int32
	last     int64
	goGID    uint64
	receipts []nativeMDSCephFSDataReceipt
}

func (x *mdsCephFSDataControl) request(ctx context.Context, action, arg string) (nativeMDSCephFSDataReceipt, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	ctx, stop := context.WithTimeout(ctx, 130*time.Second)
	defer stop()
	var result nativeMDSCephFSDataReceipt
	path := filepath.Join(x.dir, "mds-cephfs-data-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	args := []string{x.oracle.Token, action}
	if arg != "" {
		args = append(args, arg)
	}
	if err := testcluster.RequestControl(ctx, x.dir, "mds-cephfs-data", "mds-cephfs-data-completed", args...); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	x.receipts = append(x.receipts, result)
	f := x.fresh
	if result.Source != mdsFreshSource || result.Token != x.oracle.Token || result.RequestID <= x.last || result.Action != action ||
		result.FSID != f.FSID || result.FSCID != f.FSCID || result.FSName != f.FSName || result.MDSGID != f.MDS.GID || result.MDSEndpoint != f.MDS.Endpoint || result.MDSNonce != f.MDS.Nonce ||
		result.MDSIncarnation != x.incarn || result.Rank != 0 || !result.IdentityChecked || !result.Complete || result.Error != "" || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
		return result, fmt.Errorf("native MDS CephFS data receipt scope or unchanged daemon identity differs: %s", result.Error)
	}
	if action == "observe" {
		if result.GlobalID != x.goGID || result.Sessions == nil {
			return result, errors.New("native MDS CephFS data observation scope differs")
		}
	} else if result.Native == nil || result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
		return result, errors.New("native MDS CephFS data oracle output missing or bound to the Go client")
	}
	x.last = result.RequestID
	return result, nil
}

// cephfsDataObjectIO is one OSD request of the Go client on a file object.
type cephfsDataObjectIO struct {
	Leaf          string
	Op            string
	Object        string
	ObjectNo      uint64
	Offset        uint64  `json:",omitempty"`
	Length        uint64  `json:",omitempty"`
	TruncateSize  uint64  `json:",omitempty"`
	TruncateSeq   uint32  `json:",omitempty"`
	ReplyLength   *uint64 `json:",omitempty"`
	ReadSnapshot  uint64  `json:",omitempty"`
	SnapContext   *cephmsgr.OSDSnapContext
	MapEpoch      uint32
	Hash          uint32
	Code          int32
	OSDError      int32  `json:",omitempty"`
	Error         string `json:",omitempty"`
	Transaction   uint64
	ReplayVersion cephmsgr.OSDVersion
	UserVersion   uint64
	DataBytes     int                       `json:",omitempty"`
	StatSize      uint64                    `json:",omitempty"`
	Snapshots     *cephmsgr.OSDSnapshotList `json:",omitempty"`
	Micros        int64
}

// cephfsDataIO issues the Go client's object requests for file data. The
// object locator (pool, name, raw hash) and the target OSD come from the
// MON's `osd map` for each object (test-only interpretation of native JSON on
// the legacy single-OSD fixture).
type cephfsDataIO struct {
	ctx       context.Context
	client    *cephmsgr.Client
	pool      int64
	poolName  string
	addresses map[int32]string
	osd       *cephmsgr.OSDConnection
	osdID     int32
	leaf      string
	mu        sync.Mutex
	log       []cephfsDataObjectIO
}

func (d *cephfsDataIO) request(name string, objectno uint64, op string, request cephmsgr.OSDRequest) (cephmsgr.OSDReply, cephfsDataObjectIO, error) {
	e := cephfsDataObjectIO{Leaf: d.leaf, Op: op, Object: name, ObjectNo: objectno, ReadSnapshot: request.ReadSnapshot}
	if len(request.Operations) == 1 {
		e.Offset, e.Length, e.TruncateSize, e.TruncateSeq = request.Operations[0].Offset, request.Operations[0].Length, request.Operations[0].TruncateSize, request.Operations[0].TruncateSequence
	}
	defer func() {
		d.mu.Lock()
		d.log = append(d.log, e)
		d.mu.Unlock()
	}()
	p, err := recoveryObjectPlacement(d.ctx, d.client, d.poolName, name)
	if err == nil && (p.PoolID != d.pool || len(p.Acting) != 1) {
		err = fmt.Errorf("object %s placement differs: %+v", name, p)
	}
	if err == nil && d.osd == nil {
		d.osd, err = d.client.OpenOSD(d.ctx, cephmsgr.OSDTarget{ID: p.Primary, Address: d.addresses[p.Primary]})
		d.osdID = p.Primary
	}
	if err == nil && p.Primary != d.osdID {
		err = fmt.Errorf("object %s primary osd.%d differs from osd.%d", name, p.Primary, d.osdID)
	}
	if err != nil {
		e.Error = err.Error()
		return cephmsgr.OSDReply{}, e, err
	}
	request.MapEpoch, request.Object = p.Epoch, cephmsgr.OSDObject{Pool: d.pool, Name: name, Hash: p.Hash}
	if len(request.SnapContext.Snapshots) != 0 || request.SnapContext.Sequence != 0 {
		snapc := request.SnapContext
		e.SnapContext = &snapc
	}
	e.MapEpoch, e.Hash = p.Epoch, p.Hash
	begin := time.Now()
	ctx, stop := context.WithTimeout(d.ctx, 30*time.Second)
	defer stop()
	reply, err := d.osd.Request(ctx, request)
	e.Micros = time.Since(begin).Microseconds()
	e.Code, e.Transaction, e.ReplayVersion, e.UserVersion = reply.Code, reply.Transaction, reply.ReplayVersion, reply.UserVersion
	if len(reply.Operations) == 1 && err == nil {
		length := reply.Operations[0].Length
		e.ReplyLength = &length
	}
	if err != nil {
		e.Error = err.Error()
		var osdErr *cephmsgr.OSDError
		if errors.As(err, &osdErr) {
			e.OSDError = osdErr.Code
		}
	}
	return reply, e, err
}

// last is the most recent object request record.
func (d *cephfsDataIO) last() cephfsDataObjectIO {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.log[len(d.log)-1]
}

// reset closes a failed OSD connection; the next request opens a new one.
func (d *cephfsDataIO) reset() {
	if d.osd != nil {
		d.osd.Close()
		d.osd = nil
	}
}

// write writes data at a file offset: one request per object extent with the
// caller's snap context, modification time and the inode's truncate state
// (Filer::write_trunc). A reply whose extent the OSD trimmed is an error here.
func (d *cephfsDataIO) write(ino uint64, l cephfsStriping, offset uint64, data []byte, snapc cephmsgr.OSDSnapContext, mtime cephmsgr.MDSTime, trunc cephfsTrunc) error {
	for _, ext := range l.fileToExtents(offset, uint64(len(data))) {
		buf := make([]byte, 0, ext.Length)
		for _, b := range ext.Buffers {
			buf = append(buf, data[b.Offset:b.Offset+b.Length]...)
		}
		name := cephfsObjectName(ino, ext.ObjectNo)
		size, seq := trunc.wire(l, ext.ObjectNo)
		reply, e, err := d.request(name, ext.ObjectNo, "write", cephmsgr.OSDRequest{ModificationTime: cephmsgr.OSDTime{Seconds: mtime.Seconds, Nanoseconds: mtime.Nanoseconds},
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWrite, Offset: ext.Offset, Length: ext.Length, Data: buf, TruncateSize: size, TruncateSequence: seq}}, SnapContext: snapc})
		if err != nil || e.Code != 0 {
			return fmt.Errorf("write %s at %d+%d: %w", name, ext.Offset, ext.Length, err)
		}
		if reply.Operations[0].Length != ext.Length {
			return fmt.Errorf("write %s at %d+%d was trimmed by the OSD to %d bytes", name, ext.Offset, ext.Length, reply.Operations[0].Length)
		}
	}
	return nil
}

// read reads [offset, offset+length) of a file at a snapshot (0 for the
// head) as the native striped read assembles it: a missing object or a short
// object read leaves zeros (StripedReadResult zero fill).
func (d *cephfsDataIO) read(ino uint64, l cephfsStriping, offset, length, snap uint64, trunc cephfsTrunc) ([]byte, map[uint64]string, error) {
	out := make([]byte, length)
	holes := map[uint64]string{}
	for _, ext := range l.fileToExtents(offset, length) {
		name := cephfsObjectName(ino, ext.ObjectNo)
		size, seq := trunc.wire(l, ext.ObjectNo)
		reply, e, err := d.request(name, ext.ObjectNo, "read", cephmsgr.OSDRequest{ReadSnapshot: snap,
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDRead, Offset: ext.Offset, Length: ext.Length, TruncateSize: size, TruncateSequence: seq}}})
		var data []byte
		switch {
		case e.OSDError == -2:
			holes[ext.ObjectNo] = "missing"
		case err != nil:
			return nil, nil, fmt.Errorf("read %s: %w", name, err)
		default:
			data = reply.Operations[0].Data
			if uint64(len(data)) < ext.Length {
				holes[ext.ObjectNo] = fmt.Sprintf("short %d of %d", len(data), ext.Length)
			}
		}
		d.mu.Lock()
		d.log[len(d.log)-1].DataBytes = len(data)
		d.mu.Unlock()
		pos := 0
		for _, b := range ext.Buffers {
			if pos < len(data) {
				copy(out[b.Offset:b.Offset+b.Length], data[pos:min(len(data), pos+int(b.Length))])
			}
			pos += int(b.Length)
		}
	}
	return out, holes, nil
}

// stat returns whether an object exists and its size.
func (d *cephfsDataIO) stat(ino, objectno uint64) (bool, uint64, error) {
	name := cephfsObjectName(ino, objectno)
	reply, e, err := d.request(name, objectno, "stat", cephmsgr.OSDRequest{Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDStat}}})
	if e.OSDError == -2 {
		return false, 0, nil
	}
	if err != nil || len(reply.Operations) != 1 || len(reply.Operations[0].Data) != 16 {
		return false, 0, fmt.Errorf("stat %s: %w", name, err)
	}
	size := binary.LittleEndian.Uint64(reply.Operations[0].Data[:8])
	d.mu.Lock()
	d.log[len(d.log)-1].StatSize = size
	d.mu.Unlock()
	return true, size, nil
}

// listSnaps returns an object's clones.
func (d *cephfsDataIO) listSnaps(ino, objectno uint64) (cephmsgr.OSDSnapshotList, error) {
	name := cephfsObjectName(ino, objectno)
	reply, _, err := d.request(name, objectno, "list-snaps", cephmsgr.OSDRequest{Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}}})
	if err != nil || len(reply.Operations) != 1 {
		return cephmsgr.OSDSnapshotList{}, fmt.Errorf("list snaps %s: %w", name, err)
	}
	list := reply.Operations[0].Snapshots
	d.mu.Lock()
	d.log[len(d.log)-1].Snapshots = &list
	d.mu.Unlock()
	return list, nil
}

// cephfsObjectSizes is the object size each object of a file written as
// [offset, offset+n) ranges reaches from empty objects (0 for an object
// never written, which then does not exist).
func cephfsObjectSizes(l cephfsStriping, ranges [][2]uint64) map[uint64]uint64 {
	sizes := map[uint64]uint64{}
	for _, r := range ranges {
		for _, e := range l.fileToExtents(r[0], r[1]) {
			sizes[e.ObjectNo] = max(sizes[e.ObjectNo], e.Offset+e.Length)
		}
	}
	return sizes
}

// cephfsDataReply is one MDS reply as the Go client received it.
type cephfsDataReply struct {
	Name         string
	Transaction  uint64
	Op           uint32
	Result       int32
	Safe         bool
	SafeAfter    bool `json:",omitempty"`
	Arrival      uint64
	MDSMapEpoch  uint32
	ExtraHex     string `json:",omitempty"`
	Target       *mdsCephFSInode
	Realms       []cephmsgr.MDSSnapRealm `json:",omitempty"`
	FirstMicros  int64
	SafeMicros   int64 `json:",omitempty"`
	PushesBefore uint64
	PushesAfter  uint64
}

type cephfsDataFileEvidence struct {
	Name           string
	Ino            uint64
	Layout         cephmsgr.MDSFileLayout
	ReplyCaps      string
	ReplyCap       cephmsgr.MDSReplyCap
	ReplyMaxSize   uint64
	SnapContext    cephmsgr.OSDSnapContext
	MaxSizeRequest *cephmsgr.MDSCaps           `json:",omitempty"`
	MaxSizeGrants  []cephfsMaxSizeEvent        `json:",omitempty"`
	MTime          cephmsgr.MDSTime            `json:",omitempty"`
	FlushMessage   *cephmsgr.MDSCaps           `json:",omitempty"`
	FlushTID       uint64                      `json:",omitempty"`
	FlushAck       *cephfsFlushAck             `json:",omitempty"`
	FlushAckMicros int64                       `json:",omitempty"`
	GetattrSize    uint64                      `json:",omitempty"`
	GetattrMTime   cephmsgr.MDSTime            `json:",omitempty"`
	GetattrChange  uint64                      `json:",omitempty"`
	ObjectSizes    map[string]uint64           `json:",omitempty"`
	Holes          map[uint64]string           `json:",omitempty"`
	NativeStat     *nativeMDSCephFSDataStat    `json:",omitempty"`
	NativeLayout   string                      `json:",omitempty"`
	NativeRead     *nativeMDSCephFSDataDigest  `json:",omitempty"`
	NativeObjects  []nativeMDSCephFSDataObject `json:",omitempty"`
	CloseUpdate    *cephmsgr.MDSCaps           `json:",omitempty"`
	// Truncate is the inode truncate state the Go client sent with its
	// extent operations.
	Truncate *cephfsTrunc `json:",omitempty"`
}

func TestCephMDSCephFSDataIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_DATA") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_DATA=1 for owned native CephFS data acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS") == "1" ||
		os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" ||
		os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("CephFS data acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSDataOracle
	var fresh nativeMDSFresh
	var incarnation struct {
		MDS struct {
			Incarnation *int32 `json:"incarnation"`
		} `json:"mds"`
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-data-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &incarnation); err != nil || incarnation.MDS.Incarnation == nil {
		t.Fatal("owned MDS incarnation missing", err)
	}
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_DATA_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_DATA_KEY_FILE")
	plan, fsName := oracle.Plan, fresh.FSName
	layout := cephfsStriping{plan.StripeUnit, plan.StripeCount, plan.ObjectSize}
	wantLayout := fmt.Sprintf("stripe_unit=%d stripe_count=%d object_size=%d pool=%s", plan.StripeUnit, plan.StripeCount, plan.ObjectSize, oracle.DataPoolName)
	if oracle.Source != mdsFreshSource || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || oracle.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" || fresh.MDS.Nonce == 0 ||
		identity != "client.mds-cephfs-data-"+oracle.Token || oracle.ClientIdentity != identity || oracle.NativeIdentity != "client.mds-cephfs-data-native-"+oracle.Token ||
		oracle.ClientCaps["mon"] != "allow r" || oracle.ClientCaps["mds"] != "allow rw fsname="+fsName || oracle.ClientCaps["osd"] != "allow rw tag cephfs data="+fsName ||
		oracle.NativeCaps["mds"] != "allow rwps fsname="+fsName || oracle.NativeCaps["osd"] != "allow rw tag cephfs data="+fsName ||
		plan.Directory != "ceph-msgr-data-"+oracle.Token || plan.Snapshot != "data-"+oracle.Token[:8] ||
		!slices.Equal(plan.Files, []string{"go-file", "native-file", "trunc-file", "shared-file", "snap-file"}) || layout != (cephfsStriping{65536, 2, 131072}) ||
		oracle.DataPoolName != fsName+"-data" || oracle.DataPool <= 0 || oracle.DirLayout != wantLayout || len(plan.Seeds) != 6 ||
		plan.GoFileSize != 614523 || plan.NativeSize != 1620000 || plan.TruncWriteSize != 1228800 || plan.TruncSize != 280000 || plan.SharedSize != 200000 ||
		plan.SnapV1Size != 600000 || plan.SnapV2Offset != 400000 || plan.SnapV2Size != 320000 {
		t.Fatal("owned CephFS data oracle, plan or identities differ")
	}
	for purpose, seed := range plan.Seeds {
		if seed != "ceph-msgr-cephfs-data-"+purpose+":"+oracle.Token {
			t.Fatal("payload seed is not bound to the run", purpose, seed)
		}
	}
	// Expected file images, computed independently of both clients.
	goPayload := cephfsDataStream(plan.Seeds["go-file"], plan.GoFileSize)
	nativeStream := cephfsDataStream(plan.Seeds["native-file"], plan.NativeSize)
	nativeImage := make([]byte, plan.NativeSize)
	for _, e := range plan.NativeExtents {
		copy(nativeImage[e[0]:e[0]+e[1]], nativeStream[e[0]:e[0]+e[1]])
	}
	truncPayload := cephfsDataStream(plan.Seeds["trunc-file"], plan.TruncWriteSize)
	sharedPayload := cephfsDataStream(plan.Seeds["shared-file"], plan.SharedSize)
	snapV1 := cephfsDataStream(plan.Seeds["snap-file-v1"], plan.SnapV1Size)
	snapV2 := cephfsDataStream(plan.Seeds["snap-file-v2"], plan.SnapV2Size)
	snapHead := append(slices.Clone(snapV1[:plan.SnapV2Offset]), snapV2...)

	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-data", "root": "/"}
	target := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)}
	dials := newRecoveryDials(true, fresh.MDS.Endpoint)
	client := recoveryClient(t, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = true, true, true })
	gid := client.Snapshot().GlobalID
	control := &mdsCephFSDataControl{dir: dir, oracle: oracle, fresh: fresh, incarn: *incarnation.MDS.Incarnation, goGID: gid}
	osdIO := &cephfsDataIO{ctx: ctx, client: client, pool: oracle.DataPool, poolName: oracle.DataPoolName}

	var (
		session   *cephmsgr.MDSSession
		agent     *cephfsCapAgent
		epoch     uint32
		dirIno    uint64
		requests  []cephfsDataReply
		files     = map[string]*cephfsDataFileEvidence{}
		facts     = map[string]any{}
		findings  []string
		caseTimes = map[string]int64{}
		realms    = map[uint64]cephmsgr.MDSSnapRealm{}
		started   = time.Now()
		// truncExpected is the object size the native truncate leaves on each
		// object of trunc-file (after the MDS trimmed them).
		truncExpected map[uint64]uint64
	)
	t.Cleanup(func() {
		var snapshot *cephfsAgentSnapshot
		if agent != nil {
			s := agent.snapshot()
			snapshot = &s
		}
		osdIO.mu.Lock()
		objects := slices.Clone(osdIO.log)
		osdIO.mu.Unlock()
		data, err := json.Marshal(struct {
			Source       string                             `json:"source_commit"`
			Oracle       nativeMDSCephFSDataOracle          `json:"oracle"`
			Target       cephmsgr.MDSTarget                 `json:"target"`
			GlobalID     uint64                             `json:"global_id"`
			CephFSNonce  uint32                             `json:"cephfs_nonce"`
			Requests     []cephfsDataReply                  `json:"requests"`
			Files        map[string]*cephfsDataFileEvidence `json:"files"`
			Objects      []cephfsDataObjectIO               `json:"object_requests"`
			Agent        *cephfsAgentSnapshot               `json:"capability_agent"`
			Facts        map[string]any                     `json:"facts"`
			Findings     []string                           `json:"product_findings"`
			Native       []nativeMDSCephFSDataReceipt       `json:"native"`
			Dials        [2]uint32                          `json:"mds_osd_dials"`
			CaseMillis   map[string]int64                   `json:"case_ms"`
			ElapsedMilli int64                              `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, target, gid, client.Snapshot().CephFSNonce, requests, files, objects, snapshot, facts, findings, control.receipts,
			[2]uint32{dials.MDS.Load(), dials.OSD.Load()}, caseTimes, time.Since(started).Milliseconds()})
		if err == nil && len(data) > 16<<20 {
			err = errors.New("MDS CephFS data evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-data-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS data public evidence", err)
		}
	})
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		osdIO.leaf = name
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		if agent != nil {
			if s := agent.snapshot(); len(s.Errors) != 0 {
				t.Errorf("capability agent errors after %s: %v", name, s.Errors)
				return false
			}
		}
		return ok
	}
	req := func(t *testing.T, action, arg string) nativeMDSCephFSDataReceipt {
		t.Helper()
		r, err := control.request(ctx, action, arg)
		if err != nil {
			t.Fatal("native control", action, arg, err)
		}
		return r
	}
	observe := func(t *testing.T) (nativeMDSCephFSDataReceipt, *nativeMDSSession) {
		t.Helper()
		r := req(t, "observe", strconv.FormatUint(gid, 10))
		epoch = r.Map.Epoch
		return r, r.session(gid)
	}
	// submit sends one request, applies its first reply to the agent and
	// waits for the safe reply (asking the MDS to flush its journal).
	submit := func(t *testing.T, name string, request cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		request.MDSMapEpoch = epoch
		e := cephfsDataReply{Name: name, PushesBefore: session.Status().PushSequence}
		begin := time.Now()
		call, err := session.Submit(ctx, request)
		if err != nil {
			t.Fatal("submit", name, err)
		}
		wait, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		first, err := call.Wait(wait)
		if err != nil {
			t.Fatal("first reply", name, err)
		}
		e.Transaction, e.Op, e.Result, e.Safe, e.Arrival, e.MDSMapEpoch, e.FirstMicros = call.Transaction(), first.Op, first.Result, first.Safe, first.Arrival, first.MDSMapEpoch, time.Since(begin).Microseconds()
		if len(first.Extra) <= 64 {
			e.ExtraHex = hex.EncodeToString(first.Extra)
		}
		trace, _, err := agent.applyReply(name, first)
		if err != nil {
			t.Fatal("decode reply trace", name, err)
		}
		e.Target = summarizeInode(trace.Target)
		if e.Realms, err = first.DecodeSnapTrace(); err != nil {
			t.Fatal("decode snap trace", name, err)
		}
		for _, r := range e.Realms {
			realms[r.Ino] = r
		}
		if !first.Safe {
			if err := session.FlushJournal(ctx); err != nil {
				t.Fatal("flush journal", name, err)
			}
			safe, err := call.WaitSafe(wait)
			if err != nil || safe.Result != first.Result {
				t.Fatal("safe reply", name, err, safe.Result)
			}
			e.SafeAfter, e.SafeMicros = true, time.Since(begin).Microseconds()
		}
		e.PushesAfter = session.Status().PushSequence
		requests = append(requests, e)
		return first, trace
	}
	lookup := func(t *testing.T, parent uint64, name string) *cephmsgr.MDSInodeStat {
		t.Helper()
		first, trace := submit(t, "lookup "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}})
		if first.Result != 0 || trace.Target == nil {
			t.Fatalf("lookup %s: %d", name, first.Result)
		}
		return trace.Target
	}
	// open restates the native open: the agent records the open file mode
	// before the request (get_open_ref), as the revoke acceptance does.
	open := func(t *testing.T, ino uint64, mode int, name string) *cephmsgr.MDSInodeStat {
		t.Helper()
		agent.open(ino, mode)
		flags := uint32(cephOpenReadOnly)
		if mode == cephFileModeRDWR {
			flags = cephOpenReadWrite
		}
		first, trace := submit(t, "open "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(flags, 0, 0), Path: cephmsgr.MDSFilePath{Ino: ino}})
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != ino {
			t.Fatalf("open %s differs: %d", name, first.Result)
		}
		return trace.Target
	}
	create := func(t *testing.T, name string) *cephmsgr.MDSInodeStat {
		t.Helper()
		first, trace := submit(t, "create "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
		st := trace.Target
		if first.Result != 0 || st == nil || trace.DentryName != name || len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != st.Ino {
			t.Fatalf("create %s differs: %d", name, first.Result)
		}
		agent.open(st.Ino, cephFileModeRDWR)
		return st
	}
	waitCap := func(t *testing.T, ino uint64, what string, limit time.Duration, done func(cephfsCap) bool) cephfsCap {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			c, _ := agent.cap(ino)
			if done(c) {
				return c
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not happen on %#x: issued %s max_size %d size %d", what, ino, cephCapString(c.Issued), c.MaxSize, c.Size)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// snapc is the snap context of the file's cap realm, from the newest
	// realm records of replies and snap pushes.
	snapc := func(t *testing.T, ino uint64) cephmsgr.OSDSnapContext {
		t.Helper()
		c, _ := agent.cap(ino)
		known := maps.Clone(realms)
		for _, s := range agent.snapshot().Snaps {
			for _, r := range s.Realms {
				if old, ok := known[r.Ino]; !ok || r.Seq >= old.Seq {
					known[r.Ino] = r
				}
			}
		}
		context, err := cephfsSnapContext(known, c.Realm)
		if err != nil {
			t.Fatal(err)
		}
		return context
	}
	// truncOf is the inode's truncate state as the agent holds it (from
	// InodeStats and TRUNC pushes), sent with every extent operation.
	truncOf := func(ino uint64) cephfsTrunc {
		c, _ := agent.cap(ino)
		return cephfsTrunc{c.TruncateSize, c.TruncateSeq}
	}
	// neverTruncated is the truncate state of an inode the MDS created
	// (Server.cc 3639-3640).
	neverTruncated := cephfsTrunc{math.MaxUint64, 1}
	// ensureMaxSize requests a larger max_size as get_caps does and waits
	// for the MDS to grant it.
	ensureMaxSize := func(t *testing.T, f *cephfsDataFileEvidence, endoff uint64) {
		t.Helper()
		before := len(agent.snapshot().MaxSizes)
		msg, sent, err := agent.requestMaxSize(f.Ino, endoff)
		if err != nil {
			t.Fatal(err)
		}
		if sent {
			f.MaxSizeRequest = &msg
		}
		waitCap(t, f.Ino, fmt.Sprintf("max_size %d", endoff), 15*time.Second, func(c cephfsCap) bool { return c.MaxSize >= endoff })
		f.MaxSizeGrants = append(f.MaxSizeGrants, agent.snapshot().MaxSizes[before:]...)
	}
	// newMTime is the writer's clock for a write, kept after the inode's
	// current mtime so the MDS takes it (_update_cap_fields).
	newMTime := func(ino uint64) cephmsgr.MDSTime {
		c, _ := agent.cap(ino)
		m := cephfsMDSTime(time.Now())
		if !cephfsTimeAfter(m, c.MTime) {
			m = cephmsgr.MDSTime{Seconds: c.MTime.Seconds + 1}
		}
		return m
	}
	// flush sends the dirty metadata with FLAG_SYNC and waits for its
	// FLUSH_ACK.
	flush := func(t *testing.T, f *cephfsDataFileEvidence) {
		t.Helper()
		sent := time.Now()
		msg, tid, err := agent.flushDirty(f.Ino)
		if err != nil {
			t.Fatal(err)
		}
		f.FlushMessage, f.FlushTID = &msg, tid
		deadline := time.Now().Add(15 * time.Second)
		for f.FlushAck == nil {
			for _, ack := range agent.snapshot().FlushAcks {
				if ack.Transaction == tid {
					a := ack
					f.FlushAck, f.FlushAckMicros = &a, ack.Received.Sub(sent).Microseconds()
				}
			}
			if f.FlushAck == nil && time.Now().After(deadline) {
				t.Fatal("no FLUSH_ACK for flush tid", tid)
			}
			time.Sleep(10 * time.Millisecond)
		}
		a := f.FlushAck.Caps
		if a.Op != cephmsgr.MDSCapOpFlushAck || a.Ino != f.Ino || a.Dirty != cephCapFileWR || a.CapID != msg.CapID {
			t.Fatalf("FLUSH_ACK differs: %+v", f.FlushAck)
		}
	}
	nativeObjects := func(t *testing.T, name string, ino uint64) []nativeMDSCephFSDataObject {
		t.Helper()
		r := req(t, "objects", name)
		if r.Native.Stat == nil || !r.Native.Stat.Exists || r.Native.Stat.Ino != ino || len(r.Native.Objects) != plan.ObjectCounts[name] {
			t.Fatal("native object view differs", name, r.Native.Stat)
		}
		return r.Native.Objects
	}
	// checkObjects compares Go OSD stats and the native object view with the
	// expected object sizes (absent means the object must not exist).
	checkObjects := func(t *testing.T, f *cephfsDataFileEvidence, expected map[uint64]uint64) {
		t.Helper()
		native := nativeObjects(t, f.Name, f.Ino)
		f.NativeObjects, f.ObjectSizes = native, map[string]uint64{}
		for i := range plan.ObjectCounts[f.Name] {
			objno := uint64(i)
			exists, size, err := osdIO.stat(f.Ino, objno)
			if err != nil {
				t.Fatal(err)
			}
			want, wantExists := expected[objno]
			if exists {
				f.ObjectSizes[cephfsObjectName(f.Ino, objno)] = size
			}
			n := native[i]
			if exists != wantExists || size != want || n.Exists != exists || n.Size != size || n.Name != cephfsObjectName(f.Ino, objno) {
				t.Fatalf("object %s differs: go exists %v size %d, native %+v, expected exists %v size %d", cephfsObjectName(f.Ino, objno), exists, size, n, wantExists, want)
			}
		}
	}
	nativeRead := func(t *testing.T, f *cephfsDataFileEvidence, target string, want []byte) nativeMDSCephFSDataReceipt {
		t.Helper()
		r := req(t, "read", target)
		got, expected := *r.Native.Read, cephfsDataDigest(want)
		if got.Length != expected.Length || got.SHA256 != expected.SHA256 || !slices.Equal(got.Chunks, expected.Chunks) || r.Native.Stat.Size != uint64(len(want)) {
			for i := range min(len(got.Chunks), len(expected.Chunks)) {
				if got.Chunks[i] != expected.Chunks[i] {
					t.Fatalf("native read of %s differs from 16 KiB chunk %d on (length %d, want %d)", target, i, got.Length, expected.Length)
				}
			}
			t.Fatalf("native read of %s differs: length %d size %d, want %d", target, got.Length, r.Native.Stat.Size, len(want))
		}
		if f != nil {
			f.NativeRead = &got
		}
		return r
	}
	closeFile := func(t *testing.T, f *cephfsDataFileEvidence, mode int) {
		t.Helper()
		msg, sent, err := agent.close(f.Ino, mode)
		if err != nil {
			t.Fatal("close", f.Name, err)
		}
		if sent {
			f.CloseUpdate = &msg
		}
	}
	checkLayout := func(t *testing.T, f *cephfsDataFileEvidence, st *cephmsgr.MDSInodeStat) {
		t.Helper()
		l := st.Layout
		f.Layout = l
		if uint64(l.StripeUnit) != plan.StripeUnit || uint64(l.StripeCount) != plan.StripeCount || uint64(l.ObjectSize) != plan.ObjectSize || int64(l.Pool) != oracle.DataPool || l.PoolNamespace != "" {
			t.Fatalf("%s layout from the reply differs from the directory layout: %+v", f.Name, l)
		}
	}
	t.Cleanup(func() {
		if session != nil {
			closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			session.Close(closing)
		}
	})

	if !leaf("open_data_session_and_layout", func(t *testing.T) {
		conn, err := client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal("open explicit MDS target", err)
		}
		session, err = conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
		if err != nil {
			t.Fatal("open CephFS session", err)
		}
		agent = startCephFSCapAgent(ctx, session)
		_, n := observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
			t.Fatal(err)
		}
		nonce := client.Snapshot().CephFSNonce
		if nonce == 0 || !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(nonce), 10)) || n.Caps != 0 {
			t.Fatalf("native session %s does not carry the shared nonce %d or already holds caps", n.Address, nonce)
		}
		facts["native_session_address"] = n.Address
		set := req(t, "layout-set", "")
		if set.Mutation == nil || set.Mutation.State != "applied" || set.Native.Layout != wantLayout {
			t.Fatal("native directory layout was not applied", set.Mutation, set.Native.Layout)
		}
		st := lookup(t, 1, plan.Directory)
		dirIno = st.Ino
		facts["directory"] = map[string]any{"ino": dirIno, "native_ino": set.Native.Stat.Ino, "native_layout": set.Native.Layout, "go_dir_layout": st.Layout}
		if set.Native.Stat.Ino != dirIno || uint64(st.Layout.StripeUnit) != plan.StripeUnit || uint64(st.Layout.StripeCount) != plan.StripeCount ||
			uint64(st.Layout.ObjectSize) != plan.ObjectSize || int64(st.Layout.Pool) != oracle.DataPool {
			t.Fatalf("Go directory inode or layout differs from the native layout: %+v", st.Layout)
		}
		osdIO.addresses = osdDumpAddresses(t, ctx, client)
	}) {
		return
	}

	if !leaf("go_striped_write_flush", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "go-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		if c, _ := agent.cap(f.Ino); c.Issued&cephCapFileWR == 0 {
			t.Fatal("the created file was not issued Fw", cephCapString(c.Issued))
		}
		ensureMaxSize(t, f, plan.GoFileSize)
		f.SnapContext = snapc(t, f.Ino)
		f.MTime = newMTime(f.Ino)
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		if trunc != neverTruncated {
			t.Fatalf("the created inode's truncate state differs: %+v", trunc)
		}
		if err := osdIO.write(f.Ino, layout, 0, goPayload, f.SnapContext, f.MTime, trunc); err != nil {
			t.Fatal(err)
		}
		if err := agent.markDirty(f.Ino, plan.GoFileSize, f.MTime, f.SnapContext.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		flush(t, f)
		if m := f.FlushMessage; m.Op != cephmsgr.MDSCapOpUpdate || m.Dirty != cephCapFileWR || m.Size != plan.GoFileSize || m.MTime != f.MTime || m.SnapFollows != f.SnapContext.Sequence ||
			m.Flags != 1 || m.OldestFlushTID != f.FlushTID {
			t.Fatalf("flush message differs: %+v", m)
		}
		first, trace := submit(t, "getattr go-file", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: f.Ino}})
		if first.Result != 0 || trace.Target == nil {
			t.Fatal("getattr after the flush", first.Result)
		}
		f.GetattrSize, f.GetattrMTime, f.GetattrChange = trace.Target.Size, trace.Target.MTime, trace.Target.ChangeAttr
		if trace.Target.Size != plan.GoFileSize || trace.Target.MTime != f.MTime {
			t.Fatalf("MDS size or mtime after the flush differ: %d %+v", trace.Target.Size, trace.Target.MTime)
		}
		back, holes, err := osdIO.read(f.Ino, layout, 0, plan.GoFileSize, 0, truncOf(f.Ino))
		if err != nil || len(holes) != 0 || !bytes.Equal(back, goPayload) {
			t.Fatal("Go read-back of its own write differs", err, holes)
		}
		closeFile(t, f, cephFileModeRDWR)
		ns := req(t, "stat", f.Name)
		f.NativeStat, f.NativeLayout = ns.Native.Stat, ns.Native.Layout
		if !ns.Native.Stat.Exists || ns.Native.Stat.Ino != f.Ino || ns.Native.Stat.Size != plan.GoFileSize || ns.Native.Stat.MTime != int64(f.MTime.Seconds) || ns.Native.Layout != wantLayout {
			t.Fatalf("native stat of the Go-written file differs: %+v %s", ns.Native.Stat, ns.Native.Layout)
		}
		nativeRead(t, f, f.Name, goPayload)
		checkObjects(t, f, cephfsObjectSizes(layout, [][2]uint64{{0, plan.GoFileSize}}))
	}) {
		return
	}

	if !leaf("native_sparse_file_read", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "native-file"}
		files[f.Name] = f
		w := req(t, "write-native", "")
		if w.Mutation == nil || w.Mutation.State != "applied" || w.Native.Stat.Size != plan.NativeSize {
			t.Fatal("native sparse write was not applied", w.Mutation)
		}
		f.Ino = lookup(t, dirIno, f.Name).Ino
		st := open(t, f.Ino, cephFileModeRD, f.Name)
		f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		if f.Ino != w.Native.Stat.Ino || st.Size != plan.NativeSize {
			t.Fatalf("Go view of the native file differs: ino %#x size %d", st.Ino, st.Size)
		}
		waitCap(t, f.Ino, "Fr", 10*time.Second, func(c cephfsCap) bool { return c.Issued&cephCapFileRD != 0 })
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		if trunc != neverTruncated {
			t.Fatalf("the native file's truncate state differs: %+v", trunc)
		}
		data, holes, err := osdIO.read(f.Ino, layout, 0, st.Size, 0, trunc)
		if err != nil {
			t.Fatal(err)
		}
		f.Holes = holes
		if !bytes.Equal(data, nativeImage) {
			t.Fatal("Go read of the sparse native file differs from the planned image")
		}
		nativeRead(t, f, f.Name, nativeImage)
		written := cephfsObjectSizes(layout, plan.NativeExtents)
		checkObjects(t, f, written)
		// Objects the native writer never wrote read as zeros (missing); a
		// written object shorter than its extent is zero-filled (short).
		missing := 0
		for _, e := range layout.fileToExtents(0, plan.NativeSize) {
			if _, ok := written[e.ObjectNo]; !ok {
				missing++
				if holes[e.ObjectNo] != "missing" {
					t.Fatalf("hole object %d was not read as missing: %v", e.ObjectNo, holes)
				}
			} else if holes[e.ObjectNo] == "missing" {
				t.Fatalf("written object %d was read as missing: %v", e.ObjectNo, holes)
			}
		}
		if missing == 0 {
			t.Fatal("the planned sparse file has no missing object")
		}
		closeFile(t, f, cephFileModeRD)
	}) {
		return
	}

	if !leaf("native_truncate_trunc_push", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "trunc-file"}
		files[f.Name] = f
		w := req(t, "write-trunc", "")
		if w.Mutation == nil || w.Mutation.State != "applied" {
			t.Fatal("native write of the truncated file was not applied", w.Mutation)
		}
		f.Ino = lookup(t, dirIno, f.Name).Ino
		st := open(t, f.Ino, cephFileModeRD, f.Name)
		f.ReplyCap, f.ReplyCaps = st.Cap, cephCapString(st.Cap.Caps)
		checkLayout(t, f, st)
		before := waitCap(t, f.Ino, "Fr", 10*time.Second, func(c cephfsCap) bool { return c.Issued&cephCapFileRD != 0 })
		if (cephfsTrunc{before.TruncateSize, before.TruncateSeq}) != neverTruncated {
			t.Fatalf("the native file's truncate state before the truncate differs: %d seq %d", before.TruncateSize, before.TruncateSeq)
		}
		data, _, err := osdIO.read(f.Ino, layout, 0, before.Size, 0, truncOf(f.Ino))
		if err != nil || before.Size != plan.TruncWriteSize || !bytes.Equal(data, truncPayload) {
			t.Fatal("Go read before the truncate differs", err, before.Size)
		}
		preSizes := cephfsObjectSizes(layout, [][2]uint64{{0, plan.TruncWriteSize}})
		facts["trunc_before"] = map[string]any{"size": before.Size, "truncate_seq": before.TruncateSeq, "truncate_size": before.TruncateSize, "issued": cephCapString(before.Issued)}
		snap := agent.snapshot()
		tr := req(t, "truncate", "")
		if tr.Mutation == nil || tr.Mutation.State != "applied" || tr.Native.Stat.Size != plan.TruncSize {
			t.Fatal("native truncate was not applied", tr.Mutation)
		}
		deadline := time.Now().Add(15 * time.Second)
		var trunc cephfsTruncEvent
		for {
			s := agent.snapshot()
			if len(s.Truncs) > len(snap.Truncs) {
				trunc = s.Truncs[len(s.Truncs)-1]
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no TRUNC push arrived for the native truncate")
			}
			time.Sleep(10 * time.Millisecond)
		}
		m := trunc.Caps
		facts["trunc_push"] = trunc
		if m.Ino != f.Ino || m.Size != plan.TruncSize || m.TruncateSize != plan.TruncSize || m.TruncateSeq != before.TruncateSeq+1 || trunc.SizeAfter != plan.TruncSize ||
			trunc.TruncateSeqAfter != m.TruncateSeq {
			t.Fatalf("TRUNC push differs: %+v", trunc)
		}
		var around []cephfsCapRecord
		for _, r := range agent.snapshot().Records[len(snap.Records):] {
			if r.Ino == f.Ino {
				around = append(around, r)
			}
		}
		facts["trunc_cap_records"] = around
		after := waitCap(t, f.Ino, "Fr after the truncate", 15*time.Second, func(c cephfsCap) bool { return c.Issued&cephCapFileRD != 0 && c.Size == plan.TruncSize })
		// The Go read honours the TRUNC: it reads up to the new size and sends
		// the new truncate_size/truncate_seq per object as the native client
		// does (object 2: 17856, sequence 2).
		state := truncOf(f.Ino)
		f.Truncate = &state
		if state != (cephfsTrunc{plan.TruncSize, m.TruncateSeq}) {
			t.Fatalf("the truncate state after the TRUNC push differs: %+v", state)
		}
		data, _, err = osdIO.read(f.Ino, layout, 0, after.Size, 0, state)
		if err != nil || !bytes.Equal(data, truncPayload[:plan.TruncSize]) {
			t.Fatal("Go read after the TRUNC differs", err)
		}
		nativeRead(t, f, f.Name, truncPayload[:plan.TruncSize])
		// Object trimming: Filer::truncate sends TRIMTRUNC to every object of
		// [size, old size) rounded up to the period, truncating each to the
		// in-object offset where that range starts; objects wholly beyond
		// stay as empty objects.
		expected := maps.Clone(preSizes)
		period := plan.StripeCount * plan.ObjectSize
		end := (plan.TruncWriteSize + period - 1) / period * period
		for _, e := range layout.fileToExtents(plan.TruncSize, end-plan.TruncSize) {
			if _, ok := expected[e.ObjectNo]; ok {
				// TRUNCATE sets the object size to the extent's start even
				// when that grows the object; a missing object stays absent.
				expected[e.ObjectNo] = e.Offset
			}
		}
		truncExpected = expected
		var timeline []map[uint64]uint64
		deadline = time.Now().Add(30 * time.Second)
		flushed := false
		for {
			current := map[uint64]uint64{}
			for objno := range preSizes {
				exists, size, err := osdIO.stat(f.Ino, objno)
				if err != nil {
					t.Fatal(err)
				}
				if exists {
					current[objno] = size
				}
			}
			timeline = append(timeline, current)
			if maps.Equal(current, expected) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("object trimming did not reach the expected sizes: %v, want %v", current, expected)
			}
			if !flushed {
				// Journal flush: the MDS trims objects after the setattr commits.
				if err := session.FlushJournal(ctx); err != nil {
					t.Fatal(err)
				}
				flushed = true
			}
			time.Sleep(200 * time.Millisecond)
		}
		facts["trunc_object_timeline"], facts["trunc_object_expected"] = timeline, expected
		checkObjects(t, f, expected)
		closeFile(t, f, cephFileModeRD)
	}) {
		return
	}

	// The native client sends the inode's truncate_size (per object,
	// Striper::object_truncate_size) and truncate_seq with every extent
	// operation (Filer::write_trunc, Filer::read_trunc); so does the Go
	// client. After the native truncate the MDS's TRIMTRUNC gave every
	// trimmed object the truncate sequence 2. The leaves below write at the
	// new end of file with that sequence, replay a write with the previous
	// sequence (PrimaryLogPG trims it at the object size), read with a newer
	// sequence than an object's (the OSD clips at truncate_size) and send
	// sequenced TRUNCATEs that are not newer (no-ops) and one that is.
	const probeBytes = 1000
	var (
		truncObject, truncObjectStart, truncObjectSize uint64
		truncImage                                     []byte
		truncObjects                                   map[uint64]uint64
	)
	// objectBytes is the expected content of an object of trunc-file at
	// in-object [0, n), from the expected file image.
	objectBytes := func(objectno, n uint64) []byte {
		var out []byte
		for _, r := range layout.extentToFile(objectno, 0, n) {
			out = append(out, truncImage[r[0]:r[0]+r[1]]...)
		}
		return out
	}
	// checkTruncFile compares both object views, the Go read and the native
	// read with the expected state of trunc-file.
	checkTruncFile := func(t *testing.T, f *cephfsDataFileEvidence) {
		t.Helper()
		checkObjects(t, f, truncObjects)
		got, holes, err := osdIO.read(f.Ino, layout, 0, uint64(len(truncImage)), 0, truncOf(f.Ino))
		if err != nil || !bytes.Equal(got, truncImage) {
			t.Fatal("Go read of trunc-file differs from the expected image", err, holes)
		}
		nativeRead(t, f, "trunc-file", truncImage)
	}
	if !leaf("write_after_truncate_with_truncate_seq", func(t *testing.T) {
		base := files["trunc-file"]
		f := &cephfsDataFileEvidence{Name: "trunc-file", Ino: base.Ino}
		files["trunc-file-extend"] = f
		st := open(t, f.Ino, cephFileModeRDWR, f.Name+" for write")
		f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		c := waitCap(t, f.Ino, "Fw", 10*time.Second, func(c cephfsCap) bool { return c.Issued&cephCapFileWR != 0 })
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		if c.Size != plan.TruncSize || trunc != (cephfsTrunc{plan.TruncSize, 2}) || st.TruncateSeq != trunc.Seq || st.TruncateSize != trunc.Size {
			t.Fatalf("the reopened file's size or truncate state differs: %d %+v (InodeStat %d seq %d)", c.Size, trunc, st.TruncateSize, st.TruncateSeq)
		}
		ensureMaxSize(t, f, plan.TruncSize+probeBytes)
		probe := cephfsDataStream("ceph-msgr-cephfs-data-probe:"+oracle.Token, probeBytes)
		f.SnapContext, f.MTime = snapc(t, f.Ino), newMTime(f.Ino)
		ext := layout.fileToExtents(plan.TruncSize, probeBytes)
		if len(ext) != 1 || truncExpected == nil || ext[0].Offset != truncExpected[ext[0].ObjectNo] || layout.objectTruncateSize(ext[0].ObjectNo, trunc.Size) != ext[0].Offset {
			t.Fatalf("the probe does not start at the trimmed end of one object: %+v %v", ext, truncExpected)
		}
		truncObject, truncObjectStart = ext[0].ObjectNo, ext[0].Offset
		if err := osdIO.write(f.Ino, layout, plan.TruncSize, probe, f.SnapContext, f.MTime, trunc); err != nil {
			t.Fatal(err)
		}
		w := osdIO.last()
		if w.TruncateSize != truncObjectStart || w.TruncateSeq != trunc.Seq || w.Code != 0 || w.ReplyLength == nil || *w.ReplyLength != probeBytes {
			t.Fatalf("the sequenced write was not executed in full: %+v", w)
		}
		newSize := plan.TruncSize + probeBytes
		if err := agent.markDirty(f.Ino, newSize, f.MTime, f.SnapContext.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		flush(t, f)
		if m := f.FlushMessage; m.Size != newSize || m.Dirty != cephCapFileWR || m.TruncateSeq != trunc.Seq {
			t.Fatalf("flush message differs: %+v", m)
		}
		first, tr := submit(t, "getattr trunc-file after the sequenced write", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: f.Ino}})
		if first.Result != 0 || tr.Target == nil || tr.Target.Size != newSize || tr.Target.TruncateSeq != trunc.Seq || tr.Target.TruncateSize != trunc.Size {
			t.Fatal("MDS size or truncate state after the sequenced write differ", first.Result)
		}
		f.GetattrSize, f.GetattrMTime, f.GetattrChange = tr.Target.Size, tr.Target.MTime, tr.Target.ChangeAttr
		truncImage = append(slices.Clone(truncPayload[:plan.TruncSize]), probe...)
		truncObjectSize = truncObjectStart + probeBytes
		truncObjects = maps.Clone(truncExpected)
		truncObjects[truncObject] = truncObjectSize
		checkTruncFile(t, f)
		facts["truncate_seq_write"] = map[string]any{"object": cephfsObjectName(f.Ino, truncObject), "object_offset": truncObjectStart, "truncate": trunc, "request": w,
			"getattr_size": tr.Target.Size, "getattr_truncate_seq": tr.Target.TruncateSeq, "getattr_truncate_size": tr.Target.TruncateSize}
		closeFile(t, f, cephFileModeRDWR)
	}) {
		return
	}

	// A write issued under the previous truncate sequence (here replayed
	// after the truncate) that extends past the trimmed object is cut by
	// PrimaryLogPG at the object size (PrimaryLogPG.cc 6768-6777). The OSD
	// answers result 0 with the shortened extent; the product reports it as
	// a success whose Length is the number of bytes written.
	if !leaf("stale_truncate_seq_write_trimmed", func(t *testing.T) {
		const staleBytes = 1000
		f := &cephfsDataFileEvidence{Name: "trunc-file", Ino: files["trunc-file"].Ino}
		files["trunc-file-stale"] = f
		st := open(t, f.Ino, cephFileModeRDWR, f.Name+" for a stale write")
		f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		waitCap(t, f.Ino, "Fw", 10*time.Second, func(c cephfsCap) bool { return c.Issued&cephCapFileWR != 0 })
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		stale := cephfsTrunc{trunc.Size, trunc.Seq - 1}
		offset := uint64(len(truncImage)) - staleBytes/2
		ext := layout.fileToExtents(offset, staleBytes)
		if len(ext) != 1 || ext[0].ObjectNo != truncObject || ext[0].Offset != truncObjectSize-staleBytes/2 || st.Size != uint64(len(truncImage)) {
			t.Fatalf("the stale write does not straddle the end of object %d: %+v (size %d)", truncObject, ext, st.Size)
		}
		size, seq := stale.wire(layout, truncObject)
		payload := cephfsDataStream("ceph-msgr-cephfs-data-stale:"+oracle.Token, staleBytes)
		f.SnapContext, f.MTime = snapc(t, f.Ino), newMTime(f.Ino)
		name := cephfsObjectName(f.Ino, truncObject)
		reply, e, err := osdIO.request(name, truncObject, "write", cephmsgr.OSDRequest{ModificationTime: cephmsgr.OSDTime{Seconds: f.MTime.Seconds, Nanoseconds: f.MTime.Nanoseconds},
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWrite, Offset: ext[0].Offset, Length: staleBytes, Data: payload, TruncateSize: size, TruncateSequence: seq}}, SnapContext: f.SnapContext})
		kept := truncObjectSize - ext[0].Offset
		facts["stale_truncate_seq_write"] = map[string]any{"object": name, "object_offset": ext[0].Offset, "requested": staleBytes, "kept": kept, "truncate_size": size, "truncate_seq": seq,
			"object_truncate_seq": trunc.Seq, "request": e}
		if err != nil || reply.Code != 0 || len(reply.Operations) != 1 {
			t.Fatalf("the stale write did not complete: %v (%+v)", err, e)
		}
		op := reply.Operations[0]
		if op.Result != 0 || op.Offset != ext[0].Offset || op.Length != kept || op.TruncateSize != size || op.TruncateSequence != seq || seq != 1 || size != truncObjectStart {
			t.Fatalf("the stale write reply differs: %+v", op)
		}
		copy(truncImage[offset:], payload[:kept])
		checkTruncFile(t, f)
		findings = append(findings, fmt.Sprintf("A Go WRITE of %d bytes to %s with the previous truncate_seq %d (object truncate_seq %d) was trimmed by the OSD to %d bytes at the object size %d; cephmsgr returned success (result 0, user version %d) with the shortened Length %d. A caller must compare the reply Length with the request to see the trim.",
			staleBytes, name, seq, trunc.Seq, kept, truncObjectSize, reply.UserVersion, op.Length))
		closeFile(t, f, cephFileModeRDWR)
	}) {
		return
	}

	// A READ whose truncate_seq is newer than the object's and that extends
	// past truncate_size is clipped there (PrimaryLogPG.cc 5849-5853): a
	// reader that already holds a newer truncation than the object. Object 0
	// was written natively with the never-truncated pair (OSD sequence 0);
	// the trimmed object carries sequence 2.
	if !leaf("read_with_newer_truncate_seq_clips", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "trunc-file", Ino: files["trunc-file"].Ino}
		files["trunc-file-newer-read"] = f
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		newer := trunc.Seq + 1
		var records []any
		for _, tc := range []struct {
			objectno, size, clip uint64
		}{{0, truncObjects[0], 65536}, {truncObject, truncObjectSize, 10000}} {
			name := cephfsObjectName(f.Ino, tc.objectno)
			want := objectBytes(tc.objectno, tc.size)
			for _, seq := range []uint32{newer, trunc.Seq} {
				size := tc.clip
				if seq == trunc.Seq {
					size, seq = trunc.wire(layout, tc.objectno)
				}
				reply, e, err := osdIO.request(name, tc.objectno, "read", cephmsgr.OSDRequest{Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDRead, Length: tc.size, TruncateSize: size, TruncateSequence: seq}}})
				records = append(records, e)
				if err != nil || len(reply.Operations) != 1 {
					t.Fatalf("read of %s with truncate_seq %d failed: %v", name, seq, err)
				}
				op := reply.Operations[0]
				wantLength := tc.size
				if seq == newer {
					wantLength = tc.clip
				}
				if op.Length != wantLength || !bytes.Equal(op.Data, want[:wantLength]) || op.TruncateSize != size || op.TruncateSequence != seq {
					t.Fatalf("read of %s with truncate_seq %d (size %d) returned %d bytes, want %d", name, seq, size, len(op.Data), wantLength)
				}
			}
		}
		facts["newer_truncate_seq_reads"] = records
		checkTruncFile(t, f)
	}) {
		return
	}

	// A sequenced TRUNCATE (offset = truncate_size, as Objecter::trunc sends
	// it) that is not newer than the object's sequence is a no-op
	// (PrimaryLogPG.cc 6986-6992). One that is newer applies: on the empty
	// object beyond the end of file it raises the object's sequence, and a
	// write with the inode's sequence is then cut to nothing.
	if !leaf("sequenced_truncate_noop_when_not_newer", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "trunc-file", Ino: files["trunc-file"].Ino}
		files["trunc-file-sequenced-truncate"] = f
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		f.SnapContext, f.MTime = snapc(t, f.Ino), newMTime(f.Ino)
		mtime := cephmsgr.OSDTime{Seconds: f.MTime.Seconds, Nanoseconds: f.MTime.Nanoseconds}
		var records []any
		truncate := func(objectno, offset uint64, seq uint32) cephmsgr.OSDReply {
			t.Helper()
			name := cephfsObjectName(f.Ino, objectno)
			reply, e, err := osdIO.request(name, objectno, "truncate", cephmsgr.OSDRequest{ModificationTime: mtime, SnapContext: f.SnapContext,
				Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDTruncate, Offset: offset, TruncateSize: offset, TruncateSequence: seq}}})
			records = append(records, e)
			if err != nil || reply.Code != 0 || len(reply.Operations) != 1 {
				t.Fatalf("sequenced TRUNCATE of %s failed: %v", name, err)
			}
			if op := reply.Operations[0]; op.Result != 0 || op.Offset != offset || op.Length != 0 || op.TruncateSize != offset || op.TruncateSequence != seq {
				t.Fatalf("sequenced TRUNCATE reply differs: %+v", op)
			}
			return reply
		}
		for _, seq := range []uint32{trunc.Seq, trunc.Seq - 1} {
			truncate(truncObject, truncObjectStart, seq)
			exists, size, err := osdIO.stat(f.Ino, truncObject)
			if err != nil || !exists || size != truncObjectSize {
				t.Fatalf("TRUNCATE to %d with truncate_seq %d changed object %d: exists %v size %d %v", truncObjectStart, seq, truncObject, exists, size, err)
			}
		}
		// The last object of the old size is empty and beyond the end of file.
		last := uint64(len(truncObjects) - 1)
		if size, ok := truncObjects[last]; !ok || size != 0 || last <= truncObject {
			t.Fatalf("no empty object beyond the end of file: %v", truncObjects)
		}
		truncate(last, 0, trunc.Seq+1)
		name := cephfsObjectName(f.Ino, last)
		reply, e, err := osdIO.request(name, last, "write", cephmsgr.OSDRequest{ModificationTime: mtime, SnapContext: f.SnapContext,
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWrite, Length: 100, Data: make([]byte, 100), TruncateSize: 0, TruncateSequence: trunc.Seq}}})
		records = append(records, e)
		if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Length != 0 {
			t.Fatalf("the write after the newer TRUNCATE was not cut to nothing: %v %+v", err, reply.Operations)
		}
		facts["sequenced_truncates"] = records
		checkTruncFile(t, f)
	}) {
		return
	}

	if !leaf("concurrent_reader_flushes_buffered_write", func(t *testing.T) {
		const hold = 2 * time.Second
		f := &cephfsDataFileEvidence{Name: "shared-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		if c, _ := agent.cap(f.Ino); c.Issued&(cephCapFileWR|cephCapFileBuffer) != cephCapFileWR|cephCapFileBuffer {
			t.Fatal("the created file was not issued Fwb", cephCapString(c.Issued))
		}
		ensureMaxSize(t, f, plan.SharedSize)
		f.SnapContext = snapc(t, f.Ino)
		f.MTime = newMTime(f.Ino)
		var writeback struct {
			sync.Mutex
			began, done time.Time
		}
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		err := agent.markDirty(f.Ino, plan.SharedSize, f.MTime, f.SnapContext.Sequence, true, hold, func() error {
			writeback.Lock()
			writeback.began = time.Now()
			writeback.Unlock()
			err := osdIO.write(f.Ino, layout, 0, sharedPayload, f.SnapContext, f.MTime, trunc)
			writeback.Lock()
			writeback.done = time.Now()
			writeback.Unlock()
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		before := agent.snapshot()
		requested := time.Now()
		r := req(t, "read-shared", "")
		answered := time.Now()
		d, _ := agent.dirtyState(f.Ino)
		facts["shared_dirty_events"] = d.Events
		writeback.Lock()
		began, done := writeback.began, writeback.done
		writeback.Unlock()
		facts["shared_timing"] = map[string]any{"requested": requested, "writeback_began": began, "writeback_done": done, "native_answered": answered, "native_timings_ns": r.Native.TimingsNS}
		if began.IsZero() || done.IsZero() || done.After(answered) {
			t.Fatal("the buffered data was not written back while the native reader waited", began, done)
		}
		nativeRead2 := *r.Native.Read
		f.NativeRead, f.NativeStat = &nativeRead2, r.Native.Stat
		expected := cephfsDataDigest(sharedPayload)
		if nativeRead2.SHA256 != expected.SHA256 || nativeRead2.Length != plan.SharedSize || r.Native.Stat.Size != plan.SharedSize {
			t.Fatalf("the concurrent native reader did not see the flushed bytes: %d bytes, size %d", nativeRead2.Length, r.Native.Stat.Size)
		}
		// The bytes reached the OSDs only in the held writeback, so the native
		// process cannot have finished its read before the hold elapsed.
		if read := time.Duration(r.Native.TimingsNS["read"]); read < hold {
			t.Fatalf("the native read completed %v after its start, before the %v held writeback", read, hold)
		}
		// Order, as check_caps and handle_cap_grant run natively: the first
		// revoke is answered with the dirty metadata flush (FILE_WR dirty
		// travels with any cap message); the revoke of Fb, which is in use,
		// starts the held writeback, and only the cap message after the
		// writeback drops Fb. The FLUSH_ACK answers the metadata flush.
		var revoke, metadata, revokeBuffer, written, completed, acked int = -1, -1, -1, -1, -1, -1
		for i, ev := range d.Events {
			switch {
			case ev.Kind == "revoke" && revoke < 0:
				revoke = i
			}
			switch {
			case ev.Kind == "revoke" && revokeBuffer < 0 && strings.Contains(ev.Revoked, "b"):
				revokeBuffer = i
			case ev.Kind == "cap-update" && ev.TID != 0 && metadata < 0:
				metadata = i
				f.FlushTID = ev.TID
			case ev.Kind == "data-writeback" && written < 0:
				written = i
				if ev.Error != "" {
					t.Fatal("writeback failed", ev.Error)
				}
			case ev.Kind == "cap-update" && written >= 0 && completed < 0:
				completed = i
			case ev.Kind == "flush-ack" && ev.TID == f.FlushTID && acked < 0:
				acked = i
			}
		}
		if revoke < 0 || metadata < revoke || revokeBuffer < 0 || written < revokeBuffer || written < metadata || completed < written || acked < metadata {
			t.Fatalf("revoke, flush and writeback order differs: %+v", d.Events)
		}
		for _, ev := range d.Events[revokeBuffer+1 : written] {
			if ev.Kind == "cap-update" && !strings.Contains(ev.Caps, "b") {
				t.Fatalf("Fb was released before the buffered data was written back: %+v", ev)
			}
		}
		if strings.Contains(d.Events[completed].Caps, "b") {
			t.Fatalf("the revocation ack still holds Fb: %+v", d.Events[completed])
		}
		facts["shared_order"] = map[string]int{"first_revoke": revoke, "metadata_flush": metadata, "buffer_revoke": revokeBuffer, "writeback": written, "completed": completed, "flush_ack": acked}
		for _, a := range agent.snapshot().FlushAcks[len(before.FlushAcks):] {
			if a.Transaction == f.FlushTID {
				ack := a
				f.FlushAck = &ack
			}
		}
		if f.FlushAck == nil || f.FlushAck.Caps.Dirty != cephCapFileWR {
			t.Fatal("no FLUSH_ACK for the metadata flush", f.FlushTID)
		}
		checkObjects(t, f, cephfsObjectSizes(layout, [][2]uint64{{0, plan.SharedSize}}))
		closeFile(t, f, cephFileModeRDWR)
	}) {
		return
	}

	snapCreated := false
	if !leaf("snapshot_write_clones_objects", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "snap-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		ensureMaxSize(t, f, plan.SnapV1Size)
		before := snapc(t, f.Ino)
		mtime1 := newMTime(f.Ino)
		trunc := truncOf(f.Ino)
		f.Truncate = &trunc
		if err := osdIO.write(f.Ino, layout, 0, snapV1, before, mtime1, trunc); err != nil {
			t.Fatal(err)
		}
		if err := agent.markDirty(f.Ino, plan.SnapV1Size, mtime1, before.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		flush(t, f)
		v1Flush := map[string]any{"tid": f.FlushTID, "message": f.FlushMessage, "ack": f.FlushAck, "snap_context": before, "mtime": mtime1}
		closeFile(t, f, cephFileModeRDWR)
		touched := cephfsObjectSizes(layout, [][2]uint64{{plan.SnapV2Offset, plan.SnapV2Size}})
		oldSizes := cephfsObjectSizes(layout, [][2]uint64{{0, plan.SnapV1Size}})
		preLists := map[uint64]cephmsgr.OSDSnapshotList{}
		for objno := range oldSizes {
			list, err := osdIO.listSnaps(f.Ino, objno)
			if err != nil {
				t.Fatal(err)
			}
			preLists[objno] = list
			if len(list.Clones) != 1 || list.Clones[0].ID != cephmsgr.OSDHeadSnapshot || list.Sequence != before.Sequence {
				t.Fatalf("object %d already has clones before the snapshot: %+v", objno, list)
			}
		}
		snapsBefore := agent.snapshot()
		created := req(t, "snapshot-create", "")
		if created.Mutation == nil || created.Mutation.State != "applied" || !created.Native.Stat.Exists {
			t.Fatal("native snapshot creation differs", created.Mutation)
		}
		snapCreated = true
		var split cephfsSnapEvent
		deadline := time.Now().Add(15 * time.Second)
		for split.Received.IsZero() {
			for _, ev := range agent.snapshot().Snaps[len(snapsBefore.Snaps):] {
				if ev.Op == cephmsgr.MDSSnapOpSplit && ev.Split == dirIno {
					split = ev
				}
			}
			if split.Received.IsZero() && time.Now().After(deadline) {
				t.Fatal("no snap SPLIT made the directory a realm")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if len(split.Realms) == 0 || split.Realms[0].Ino != dirIno || len(split.Realms[0].Snaps) != 1 || split.Realms[0].Seq != split.Realms[0].Snaps[0] || split.Realms[0].Parent != 1 {
			t.Fatalf("the split realm differs: %+v", split)
		}
		snapID := split.Realms[0].Snaps[0]
		st = open(t, f.Ino, cephFileModeRDWR, f.Name+" after snapshot")
		if st.Cap.Realm != dirIno {
			t.Fatalf("the reopened file's cap realm %#x is not the snapshotted directory %#x", st.Cap.Realm, dirIno)
		}
		ensureMaxSize(t, f, plan.SnapV2Offset+plan.SnapV2Size)
		f.SnapContext = snapc(t, f.Ino)
		if f.SnapContext.Sequence != snapID || !slices.Equal(f.SnapContext.Snapshots, []uint64{snapID}) || f.SnapContext.Sequence <= before.Sequence {
			t.Fatalf("the snap context after the snapshot differs: %+v (snapshot %d, before %+v)", f.SnapContext, snapID, before)
		}
		f.MTime = newMTime(f.Ino)
		if err := osdIO.write(f.Ino, layout, plan.SnapV2Offset, snapV2, f.SnapContext, f.MTime, truncOf(f.Ino)); err != nil {
			t.Fatal(err)
		}
		if err := agent.markDirty(f.Ino, plan.SnapV2Offset+plan.SnapV2Size, f.MTime, f.SnapContext.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		f.FlushAck = nil
		flush(t, f)
		if f.FlushMessage.SnapFollows != snapID {
			t.Fatalf("the flush after the snapshot follows %d, not the snapshot %d", f.FlushMessage.SnapFollows, snapID)
		}
		closeFile(t, f, cephFileModeRDWR)
		// Every object the second write touched was cloned at the snapshot:
		// the clone keeps the old size, and its overlap with the head is the
		// old extent minus the rewritten range (make_writeable).
		lists := map[string]any{}
		for objno, oldSize := range oldSizes {
			list, err := osdIO.listSnaps(f.Ino, objno)
			if err != nil {
				t.Fatal(err)
			}
			lists[cephfsObjectName(f.Ino, objno)] = list
			newSize := max(oldSize, touched[objno])
			if _, rewritten := touched[objno]; !rewritten {
				if len(list.Clones) != 1 || list.Clones[0].ID != cephmsgr.OSDHeadSnapshot {
					t.Fatalf("untouched object %d was cloned: %+v", objno, list)
				}
				continue
			}
			var modified [2]uint64
			for _, e := range layout.fileToExtents(plan.SnapV2Offset, plan.SnapV2Size) {
				if e.ObjectNo == objno {
					modified = [2]uint64{e.Offset, e.Length}
				}
			}
			var overlap []cephmsgr.OSDExtent
			if modified[0] > 0 {
				overlap = append(overlap, cephmsgr.OSDExtent{Offset: 0, Length: min(modified[0], oldSize)})
			}
			if end := modified[0] + modified[1]; end < oldSize {
				overlap = append(overlap, cephmsgr.OSDExtent{Offset: end, Length: oldSize - end})
			}
			if len(list.Clones) != 2 || list.Sequence != snapID {
				t.Fatalf("object %d clones differ: %+v", objno, list)
			}
			clone, head := list.Clones[0], list.Clones[1]
			if clone.ID != snapID || !slices.Equal(clone.Snapshots, []uint64{snapID}) || clone.Size != oldSize || !slices.Equal(clone.Overlap, overlap) ||
				head.ID != cephmsgr.OSDHeadSnapshot || head.Size != newSize {
				t.Fatalf("object %d clone differs: %+v (want clone %d size %d overlap %+v, head size %d)", objno, list, snapID, oldSize, overlap, newSize)
			}
		}
		facts["snapshot"] = map[string]any{"snap_id": snapID, "split": split, "v1_flush": v1Flush, "pre_snapshot_lists": preLists, "lists": lists, "native_snapshot": created.Native.Stat}
		// The snapshot through the OSD (snap id reads) and natively.
		old, holes, err := osdIO.read(f.Ino, layout, 0, plan.SnapV1Size, snapID, truncOf(f.Ino))
		if err != nil || len(holes) != 0 || !bytes.Equal(old, snapV1) {
			t.Fatal("Go snapshot read differs from the pre-snapshot bytes", err, holes)
		}
		nativeRead(t, nil, "snapshot-file", snapV1)
		nativeRead(t, f, f.Name, snapHead)
		ss := req(t, "stat", "snapshot-file")
		facts["native_snapshot_file"] = ss.Native.Stat
		if !ss.Native.Stat.Exists || ss.Native.Stat.Size != plan.SnapV1Size || ss.Native.Stat.MTime != int64(mtime1.Seconds) {
			t.Fatalf("native stat of the snapshot file differs: %+v", ss.Native.Stat)
		}
		native := nativeObjects(t, f.Name, f.Ino)
		f.NativeObjects = native
		for i, n := range native {
			goList := lists[cephfsObjectName(f.Ino, uint64(i))].(cephmsgr.OSDSnapshotList)
			if n.Snaps == nil || n.Snaps.Seq != goList.Sequence || len(n.Snaps.Clones) != len(goList.Clones) {
				t.Fatalf("native listsnaps of object %d differs: %+v vs %+v", i, n.Snaps, goList)
			}
			for j, c := range n.Snaps.Clones {
				g := goList.Clones[j]
				var overlap [][2]uint64
				for _, o := range g.Overlap {
					overlap = append(overlap, [2]uint64{o.Offset, o.Length})
				}
				if c.Head != (g.ID == cephmsgr.OSDHeadSnapshot) || !c.Head && c.ID != g.ID || c.Size != g.Size || !slices.Equal(c.Snapshots, g.Snapshots) ||
					len(c.Overlaps) != len(overlap) || len(overlap) != 0 && !slices.Equal(c.Overlaps, overlap) {
					t.Fatalf("native clone %d of object %d differs: %+v vs %+v", j, i, c, g)
				}
			}
		}
	}) {
		return
	}

	t.Run("cleanup_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		if session == nil || session.Status().State != cephmsgr.MDSSessionOpen {
			t.Fatal("the Go session is not open for cleanup")
		}
		if snapCreated {
			removed := req(t, "snapshot-remove", "")
			if removed.Mutation == nil || removed.Mutation.State != "applied" || removed.Native.Stat.Exists {
				t.Fatal("native snapshot removal differs", removed.Mutation)
			}
		}
		for _, name := range plan.Files {
			if files[name] == nil {
				continue
			}
			first, _ := submit(t, "unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
			if first.Result != 0 {
				t.Fatalf("unlink %s: %d", name, first.Result)
			}
		}
		cleared := req(t, "layout-clear", "")
		facts["layout_cleared"] = cleared.Native
		if cleared.Mutation == nil || cleared.Mutation.State != "applied" || !cleared.Native.LayoutAbsent {
			t.Fatal("the directory layout was not removed natively", cleared.Mutation)
		}
		first, _ := submit(t, "rmdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}})
		if first.Result != 0 {
			t.Fatal("rmdir", first.Result)
		}
		if st := req(t, "stat", "dir").Native.Stat; st.Exists || st.Errno != 2 {
			t.Fatal("native stat still finds the data directory", st)
		}
		_, n := observe(t)
		if n == nil {
			t.Fatal("Go session vanished before close")
		}
		before := n.Caps
		items, barrier := agent.releaseAll()
		release, stop := context.WithTimeout(ctx, 10*time.Second)
		err := session.ReleaseCaps(release, items, barrier)
		stop()
		if err != nil {
			t.Fatal("release caps", err)
		}
		samples := []int64{before}
		for range 10 {
			_, n = observe(t)
			samples = append(samples, n.Caps)
			if n.Caps == 0 {
				break
			}
		}
		facts["release"] = map[string]any{"items": items, "barrier": barrier, "num_caps": samples}
		if n.Caps != 0 {
			t.Fatal("native num_caps did not reach zero after the release", samples)
		}
		closing, stopClose := context.WithTimeout(ctx, 15*time.Second)
		closeErr := session.Close(closing)
		stopClose()
		if closeErr != nil || session.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("Close was not confirmed by the MDS", closeErr, session.Err())
		}
		select {
		case <-agent.done:
		case <-time.After(10 * time.Second):
			t.Fatal("capability agent did not observe the session end")
		}
		if r, _ := observe(t); r.session(gid) != nil {
			t.Fatal("closed session remains in the native session map")
		}
		s := agent.snapshot()
		if len(s.Errors) != 0 || s.Pending != 0 {
			t.Fatal("capability agent ended with errors or unapplied pushes", s.Errors, s.Pending)
		}
		facts["dials"] = map[string]any{"mds": dials.MDS.Load(), "osd": dials.OSD.Load(), "mds_dials": dials.attempts()}
	})
}
