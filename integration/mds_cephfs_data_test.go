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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSDataIntegration accepts CephFS file data I/O through the
// Go client's MDS session and its own OSD requests against live Tentacle
// daemons, compared with a native LibCephFS client: striped writes with a cap
// flush, a native sparse file read through the layout, a native truncate and
// its TRUNC push, a buffered write flushed on a concurrent reader's revoke,
// a write after a snapshot that clones the objects, and pending dirty cap
// snapshots explicitly flushed after an owned MDS restart. File layout striping,
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
	CapSnapSizes   [3]uint64         `json:"cap_snap_sizes"`
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
	BaselineConfig    map[string]string        `json:"baseline_config"`
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
	Source          string                        `json:"source_commit"`
	Token           string                        `json:"token"`
	RequestID       int64                         `json:"request_id"`
	Action          string                        `json:"action"`
	Target          string                        `json:"target"`
	GlobalID        uint64                        `json:"global_id"`
	FSID            string                        `json:"fsid"`
	FSCID           int32                         `json:"fscid"`
	FSName          string                        `json:"fs_name"`
	MDSGID          uint64                        `json:"mds_gid"`
	MDSEndpoint     string                        `json:"mds_endpoint"`
	MDSNonce        uint32                        `json:"mds_nonce"`
	MDSIncarnation  int32                         `json:"mds_incarnation"`
	Rank            int32                         `json:"rank"`
	IdentityChecked bool                          `json:"identity_checked"`
	Map             *nativeMDSSessionsMap         `json:"map"`
	DaemonEpoch     uint32                        `json:"daemon_mdsmap_epoch"`
	Sessions        []nativeMDSSession            `json:"sessions"`
	Blocklist       []nativeMDSSessionsBlock      `json:"blocklist"`
	Native          *nativeMDSCephFSDataOutput    `json:"native"`
	Process         json.RawMessage               `json:"native_process,omitempty"`
	Mutation        *nativeMDSCephFSMutation      `json:"mutation"`
	Restart         *nativeMDSCephFSRestart       `json:"restart,omitempty"`
	RankStatus      *nativeMDSCephFSRankStatus    `json:"rank_status,omitempty"`
	Runtime         json.RawMessage               `json:"runtime,omitempty"`
	Config          map[string]string             `json:"config,omitempty"`
	CapLog          *nativeMDSCephFSCompletionLog `json:"cap_log,omitempty"`
	FlushJournal    *struct {
		ReturnCode int64  `json:"return_code"`
		Message    string `json:"message"`
	} `json:"flush_journal,omitempty"`
	OSDDrop       *nativeMDSCephFSDataOSDDrop    `json:"osd_drop,omitempty"`
	OSDLog        *nativeMDSCephFSDataOSDDropLog `json:"osd_log,omitempty"`
	Reader        *nativeMDSCephFSDataReader     `json:"reader,omitempty"`
	Complete      bool                           `json:"complete"`
	Error         string                         `json:"error"`
	ElapsedMillis int64                          `json:"elapsed_ms"`
	ObservedUTC   string                         `json:"observed_utc"`
}

// nativeMDSCephFSDataOSDDrop is the fixture's single OSD client-drop
// override of the writeback-fence recipe.
type nativeMDSCephFSDataOSDDrop struct {
	OSD             string            `json:"osd"`
	RequestID       int64             `json:"request_id"`
	State           string            `json:"state"`
	Settings        map[string]string `json:"settings"`
	Baseline        map[string]string `json:"baseline"`
	Readback        map[string]string `json:"readback"`
	LogMark         int               `json:"log_mark"`
	RestoreReadback map[string]string `json:"restore_readback"`
	Error           string            `json:"error"`
}

// nativeMDSCephFSDataReader is the fixture's background native reader of
// shared-file (writeback-fence recipe only).
type nativeMDSCephFSDataReader struct {
	RequestID     int64  `json:"request_id"`
	State         string `json:"state"`
	StartedUTC    string `json:"started_utc"`
	ElapsedMillis int64  `json:"elapsed_ms"`
	WaitRequestID int64  `json:"wait_request_id"`
	Error         string `json:"error"`
}

// nativeMDSCephFSDataOSDDropLog is attributed OSD drop evidence: pairs of a
// received MOSDOp from the registered client and its blackhole line.
type nativeMDSCephFSDataOSDDropLog struct {
	Mark     int      `json:"mark"`
	Total    int      `json:"total"`
	Lines    []string `json:"lines"`
	Dropped  int      `json:"dropped"`
	GlobalID uint64   `json:"global_id"`
	Nonce    uint32   `json:"nonce"`
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

func (x *mdsCephFSDataControl) request(ctx context.Context, action string, arguments ...string) (nativeMDSCephFSDataReceipt, error) {
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
	for _, arg := range arguments {
		if arg != "" {
			args = append(args, arg)
		}
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
		result.FSID != f.FSID || result.FSCID != f.FSCID || result.FSName != f.FSName || result.Rank != 0 || !result.Complete || result.Error != "" {
		return result, fmt.Errorf("native MDS CephFS data receipt scope differs: %s", result.Error)
	}
	expected := nativeMDSCephFSDaemon{Name: f.MDS.Name, GID: f.MDS.GID, Rank: 0, State: f.MDS.State,
		Endpoint: f.MDS.Endpoint, Nonce: f.MDS.Nonce, Incarnation: x.incarn}
	if action == "restart-adopt" {
		r := result.Restart
		if r == nil || r.State != "adopted" || r.Adopted == nil || r.Armed.GID != expected.GID || r.Armed.Nonce != expected.Nonce {
			return result, errors.New("native data restart adoption receipt differs")
		}
		a := *r.Adopted
		if a.Name != expected.Name || a.GID == 0 || a.GID == expected.GID || a.Nonce == 0 || a.Nonce == expected.Nonce || a.Endpoint == "" ||
			a.Rank != 0 || a.State != "up:active" || a.Incarnation == expected.Incarnation {
			return result, errors.New("native data restart did not adopt a new owned MDS incarnation")
		}
		expected = a
	}
	if result.MDSGID != expected.GID || result.MDSEndpoint != expected.Endpoint || result.MDSNonce != expected.Nonce || result.MDSIncarnation != expected.Incarnation {
		return result, errors.New("native data receipt names a different MDS incarnation")
	}
	switch action {
	case "restart-fire", "restart-status":
		if result.Restart == nil || result.Restart.Armed.GID != f.MDS.GID || action == "restart-status" && result.RankStatus == nil {
			return result, errors.New("native data restart transition receipt incomplete")
		}
	default:
		if !result.IdentityChecked || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
			return result, errors.New("native data receipt lacks stable daemon identity proof")
		}
	}
	switch action {
	case "observe":
		if result.GlobalID != x.goGID || result.Sessions == nil {
			return result, errors.New("native MDS CephFS data observation scope differs")
		}
	case "restart-arm":
		if result.Restart == nil || result.Restart.State != "armed" || result.Restart.Armed.GID != f.MDS.GID || result.Restart.Armed.Nonce != f.MDS.Nonce || len(result.Restart.ArmedAddresses) == 0 {
			return result, errors.New("native data restart arm receipt differs")
		}
	case "restart-fire", "restart-status":
	case "restart-adopt":
		if !maps.Equal(result.Config, x.oracle.BaselineConfig) {
			return result, errors.New("adopted data MDS runtime differs from its baseline")
		}
	case "debug-on", "debug-off", "log-pause", "log-resume", "blackhole-on", "client-drop-on":
		var runtime struct {
			Kind            string            `json:"kind"`
			State           string            `json:"state"`
			IncarnationGID  uint64            `json:"incarnation_gid"`
			Settings        map[string]string `json:"settings"`
			Baseline        map[string]string `json:"baseline"`
			Readback        map[string]string `json:"readback"`
			RestoreReadback map[string]string `json:"restore_readback"`
			Error           string            `json:"error"`
		}
		if json.Unmarshal(result.Runtime, &runtime) != nil || runtime.IncarnationGID != expected.GID || runtime.Error != "" || len(runtime.Settings) == 0 {
			return result, errors.New("native data runtime receipt lacks confirmed state/config")
		}
		restoring := action == "debug-off" || action == "log-resume"
		for name, value := range runtime.Settings {
			if runtime.Baseline[name] != x.oracle.BaselineConfig[name] || runtime.Baseline[name] == "" ||
				!restoring && runtime.Readback[name] != value || restoring && runtime.RestoreReadback[name] != runtime.Baseline[name] {
				return result, errors.New("native data runtime readback differs from its owned baseline/setting")
			}
		}
		if restoring && runtime.State != "restored" || !restoring && runtime.State != "applied" {
			return result, errors.New("native data runtime action did not confirm its outcome")
		}
		if action == "log-pause" && (runtime.Kind != "log-pause" || runtime.Readback["mds_log_pause"] != "true") {
			return result, errors.New("native data journal pause was not read back")
		}
		if action == "log-resume" && (runtime.Kind != "log-pause" || runtime.RestoreReadback["mds_log_pause"] != "false") ||
			action == "blackhole-on" && (runtime.Kind != "blackhole-on" || runtime.Readback["ms_blackhole_client"] != "true") {
			return result, errors.New("native data ACK-loss control was not read back")
		}
		if action == "client-drop-on" && (runtime.Kind != "client-drop-on" || len(runtime.Settings) != 2 || runtime.Readback["ms_blackhole_client"] != "true" || runtime.Readback["debug_ms"] != "10/10") {
			return result, errors.New("native data incoming client drop was not read back")
		}
	case "flush-journal":
		if result.FlushJournal == nil || result.FlushJournal.ReturnCode != 0 {
			return result, errors.New("native data journal barrier did not complete")
		}
	case "cap-log-mark":
		if result.CapLog == nil || result.CapLog.Mark < 0 {
			return result, errors.New("native data cap log mark missing")
		}
	case "osd-drop-on", "osd-drop-off":
		d := result.OSDDrop
		if d == nil || d.Error != "" || d.OSD == "" || d.Baseline["ms_blackhole_client"] != "false" || d.Baseline["debug_ms"] == "" ||
			d.Settings["ms_blackhole_client"] != "true" || d.Settings["debug_ms"] != "10/10" || len(d.Settings) != 2 {
			return result, errors.New("native data OSD drop receipt differs")
		}
		if action == "osd-drop-on" && (d.State != "applied" || !maps.Equal(d.Readback, d.Settings)) ||
			action == "osd-drop-off" && (d.State != "restored" || !maps.Equal(d.RestoreReadback, d.Baseline)) {
			return result, errors.New("native data OSD drop was not read back")
		}
	case "osd-log":
		if len(arguments) != 2 || arguments[0] != strconv.FormatUint(x.goGID, 10) || result.OSDLog == nil || result.OSDLog.GlobalID != x.goGID ||
			strconv.FormatUint(uint64(result.OSDLog.Nonce), 10) != arguments[1] || result.OSDLog.Dropped != 0 || len(result.OSDLog.Lines)%2 != 0 {
			return result, errors.New("native data OSD drop log source scope or bounded completeness differs")
		}
	case "evict":
		m := result.Mutation
		if len(arguments) != 1 || m == nil || m.Kind != "eviction" || m.State != "evicted" || m.Until == "" || m.Address == "" ||
			strconv.FormatUint(m.GlobalID, 10) != arguments[0] || result.session(m.GlobalID) != nil {
			return result, errors.New("native data eviction receipt differs")
		}
	case "reader-start", "reader-status":
		if r := result.Reader; r == nil || r.RequestID == 0 || r.Error != "" || r.State != "running" && (action == "reader-start" || r.State != "completed") {
			return result, errors.New("native data background reader receipt differs")
		}
	case "reader-wait":
		if r := result.Reader; r == nil || r.State != "completed" || r.WaitRequestID != result.RequestID || result.Native == nil || result.Native.Read == nil ||
			result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
			return result, errors.New("native data background reader result differs")
		}
	case "cap-log":
		if len(arguments) != 2 || arguments[0] != strconv.FormatUint(x.goGID, 10) || result.CapLog == nil ||
			result.CapLog.GlobalID != x.goGID || strconv.FormatUint(uint64(result.CapLog.Nonce), 10) != arguments[1] || result.CapLog.Dropped != 0 {
			return result, errors.New("native data cap log source scope or bounded completeness differs")
		}
	default:
		if result.Native == nil || result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
			return result, errors.New("native MDS CephFS data oracle output missing or bound to the Go client")
		}
	}
	x.fresh.MDS.GID, x.fresh.MDS.Endpoint, x.fresh.MDS.Nonce, x.fresh.MDS.State = expected.GID, expected.Endpoint, expected.Nonce, expected.State
	x.incarn = expected.Incarnation
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
	ReplyFlags    uint64
	Started       time.Time
	Completed     time.Time
	RequestBytes  int    `json:",omitempty"`
	RequestSHA256 string `json:",omitempty"`
	UserVersion   uint64
	DataBytes     int                       `json:",omitempty"`
	StatSize      uint64                    `json:",omitempty"`
	Snapshots     *cephmsgr.OSDSnapshotList `json:",omitempty"`
	// Submission is the local request identity kept when a request ends
	// with an error (not proof of execution).
	Submission *cephmsgr.OSDSubmission `json:",omitempty"`
	Micros     int64
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
	// wait bounds one request's local wait (30 s when zero). A bounded
	// writeback that ends before a reply leaves an unknown outcome.
	wait time.Duration
	mu   sync.Mutex
	log  []cephfsDataObjectIO
}

func (d *cephfsDataIO) request(name string, objectno uint64, op string, request cephmsgr.OSDRequest) (cephmsgr.OSDReply, cephfsDataObjectIO, error) {
	e := cephfsDataObjectIO{Leaf: d.leaf, Op: op, Object: name, ObjectNo: objectno, ReadSnapshot: request.ReadSnapshot}
	if len(request.Operations) == 1 {
		e.Offset, e.Length, e.TruncateSize, e.TruncateSeq = request.Operations[0].Offset, request.Operations[0].Length, request.Operations[0].TruncateSize, request.Operations[0].TruncateSequence
		if data := request.Operations[0].Data; len(data) != 0 {
			e.RequestBytes = len(data)
			hash := sha256.Sum256(data)
			e.RequestSHA256 = hex.EncodeToString(hash[:])
		}
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
	e.Started = begin
	wait := d.wait
	if wait == 0 {
		wait = 30 * time.Second
	}
	ctx, stop := context.WithTimeout(d.ctx, wait)
	defer stop()
	reply, err := d.osd.Request(ctx, request)
	e.Completed, e.ReplyFlags = time.Now(), reply.Flags
	e.Micros = time.Since(begin).Microseconds()
	e.Code, e.Transaction, e.ReplayVersion, e.UserVersion = reply.Code, reply.Transaction, reply.ReplayVersion, reply.UserVersion
	if len(reply.Operations) == 1 && err == nil {
		length := reply.Operations[0].Length
		e.ReplyLength = &length
	}
	if err != nil {
		e.Error = err.Error()
		submission := reply.Submission
		e.Submission = &submission
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

// cephfsDataRestartRecipe selects the single owned MDS restart of one data
// fixture run. Every other leaf is shared.
type cephfsDataRestartRecipe int

const (
	// cephfsDataCapSnapRestart: dirty cap-snap generations, a dropped durable
	// FLUSHSNAP ACK and completed-flush deduplication.
	cephfsDataCapSnapRestart cephfsDataRestartRecipe = iota
	// cephfsDataCapSendRestart: an admitted head UPDATE dropped before
	// dispatch and a second UPDATE refused by the Lost session, both
	// recovered explicitly after reconnect.
	cephfsDataCapSendRestart
	// cephfsDataWritebackFenceRestart: an object write interrupted with an
	// unknown outcome, an MDS restart, a capability revoke and an eviction
	// whose blocklist entry fences the client's later object write.
	cephfsDataWritebackFenceRestart
)

func TestCephMDSCephFSDataIntegration(t *testing.T) {
	runCephMDSCephFSDataAcceptance(t, cephfsDataCapSnapRestart)
}

// TestCephMDSCephFSCapSendRecoveryIntegration runs the same owned data
// fixture with the cap-send recovery restart instead of the cap-snap one.
func TestCephMDSCephFSCapSendRecoveryIntegration(t *testing.T) {
	runCephMDSCephFSDataAcceptance(t, cephfsDataCapSendRestart)
}

// TestCephMDSCephFSWritebackFenceIntegration runs the same owned data fixture
// with the interrupted-writeback, revoke, restart and eviction recipe instead
// of the cap-snap restart.
func TestCephMDSCephFSWritebackFenceIntegration(t *testing.T) {
	runCephMDSCephFSDataAcceptance(t, cephfsDataWritebackFenceRestart)
}

func runCephMDSCephFSDataAcceptance(t *testing.T, recipe cephfsDataRestartRecipe) {
	// top owns clients that must outlive a leaf.
	top := t
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
		!slices.Equal(plan.Files, []string{"go-file", "native-file", "trunc-file", "shared-file", "snap-file", "capsnap-file"}) || layout != (cephfsStriping{65536, 2, 131072}) ||
		oracle.DataPoolName != fsName+"-data" || oracle.DataPool <= 0 || oracle.DirLayout != wantLayout || len(plan.Seeds) != 9 ||
		len(oracle.BaselineConfig) != 4 || oracle.BaselineConfig["mds_log_pause"] != "false" || oracle.BaselineConfig["ms_blackhole_client"] != "false" || oracle.BaselineConfig["debug_mds"] == "" || oracle.BaselineConfig["debug_ms"] == "" ||
		plan.GoFileSize != 614523 || plan.NativeSize != 1620000 || plan.TruncWriteSize != 1228800 || plan.TruncSize != 280000 || plan.SharedSize != 200000 ||
		plan.CapSnapSizes != ([3]uint64{200000, 220000, 240000}) || plan.SnapV1Size != 600000 || plan.SnapV2Offset != 400000 || plan.SnapV2Size != 320000 {
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
		connection *cephmsgr.MDSConnection
		session    *cephmsgr.MDSSession
		agent      *cephfsCapAgent
		epoch      uint32
		dirIno     uint64
		requests   []cephfsDataReply
		files      = map[string]*cephfsDataFileEvidence{}
		facts      = map[string]any{}
		findings   []string
		caseTimes  = map[string]int64{}
		realms     = map[uint64]cephmsgr.MDSSnapRealm{}
		started    = time.Now()
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
			// A latched writeback must be resolved by an explicit decision
			// inside the leaf that caused it.
			if latched := agent.latchedFiles(); len(latched) != 0 {
				t.Errorf("buffered writeback still latched after %s: %v", name, latched)
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
		var err error
		connection, err = client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal("open explicit MDS target", err)
		}
		session, err = connection.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
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

	capSnapsCreated := [2]bool{}
	facts["restart_recipe"] = map[cephfsDataRestartRecipe]string{cephfsDataCapSnapRestart: "cap-snap", cephfsDataCapSendRestart: "cap-send", cephfsDataWritebackFenceRestart: "writeback-fence"}[recipe]
	if recipe == cephfsDataCapSendRestart && !leaf("uncertain_and_refused_cap_sends_recovered_across_mds_restart", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "capsnap-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		sizes := [2]uint64{plan.CapSnapSizes[0], plan.CapSnapSizes[1]}
		ensureMaxSize(t, f, sizes[1])
		if c, _ := agent.cap(f.Ino); c.Issued&cephCapFileWR == 0 {
			t.Fatalf("file lacks Fw: %s", cephCapString(c.Issued))
		}
		trunc := truncOf(f.Ino)
		context := snapc(t, f.Ino)
		gen1 := cephfsDataStream(plan.Seeds["capsnap-file-a"], sizes[0])
		gen2 := cephfsDataStream(plan.Seeds["capsnap-file-b"], sizes[1])
		image := append(slices.Clone(gen1), gen2[sizes[0]:]...)
		var times [2]cephmsgr.MDSTime
		// Each data write completes (ONDISK) before its metadata flush is
		// prepared; recovery never repeats it.
		times[0] = newMTime(f.Ino)
		if err := osdIO.write(f.Ino, layout, 0, gen1, context, times[0], trunc); err != nil {
			t.Fatal(err)
		}
		uncertain, err := agent.prepareHeadFlush(f.Ino, sizes[0], times[0], context.Sequence)
		if err != nil {
			t.Fatal("prepare the first UPDATE", err)
		}
		prepareRefused := func() *cephfsHeadFlush {
			times[1] = newMTime(f.Ino)
			if err := osdIO.write(f.Ino, layout, sizes[0], gen2[sizes[0]:], context, times[1], trunc); err != nil {
				t.Fatal(err)
			}
			refused, err := agent.prepareHeadFlush(f.Ino, sizes[1], times[1], context.Sequence)
			if err != nil {
				t.Fatal("prepare the second UPDATE", err)
			}
			return refused
		}
		osdIO.mu.Lock()
		for _, op := range osdIO.log {
			if op.Leaf == osdIO.leaf && (op.Op != "write" || op.Code != 0 || op.ReplyFlags&cephmsgr.OSDReplyOnDisk == 0) {
				osdIO.mu.Unlock()
				t.Fatal("first data generation lacks ONDISK completion", op)
			}
		}
		osdIO.mu.Unlock()
		recoveryFacts := map[string]any{"context": context, "sizes": sizes}
		facts["cap_send_recovery"] = recoveryFacts
		var refused *cephfsHeadFlush
		connection, session, agent, target, refused = recoverUncertainCapSends(t, ctx, control, client, dials, connection, session, agent, realms, metadata, osdIO, uncertain, prepareRefused, recoveryFacts)
		osdIO.mu.Lock()
		var written []cephfsDataObjectIO
		for _, op := range osdIO.log {
			if op.Leaf == osdIO.leaf && op.Op == "write" {
				written = append(written, op)
			}
		}
		osdIO.mu.Unlock()
		recoveryFacts["writeback"] = written
		for _, op := range written {
			if op.Code != 0 || op.ReplyFlags&cephmsgr.OSDReplyOnDisk == 0 || op.RequestBytes == 0 || op.RequestSHA256 == "" {
				t.Fatal("data generation lacks independent ONDISK completion metadata", op)
			}
		}
		var acks [2]*cephfsFlushAck
		deadline := time.Now().Add(15 * time.Second)
		for acks[0] == nil || acks[1] == nil {
			for _, ack := range agent.snapshot().FlushAcks {
				for i, flush := range []*cephfsHeadFlush{uncertain, refused} {
					if ack.Transaction == flush.Transaction && acks[i] == nil {
						a := ack
						acks[i] = &a
					}
				}
			}
			if (acks[0] == nil || acks[1] == nil) && time.Now().After(deadline) {
				t.Fatalf("recovered UPDATE FLUSH_ACKs missing: %+v", agent.snapshot().FlushAcks)
			}
			time.Sleep(10 * time.Millisecond)
		}
		current, _ := agent.cap(f.Ino)
		for i, flush := range []*cephfsHeadFlush{uncertain, refused} {
			last := flush.Attempts[len(flush.Attempts)-1]
			a := acks[i]
			if a.Caps.Op != cephmsgr.MDSCapOpFlushAck || a.Caps.Ino != f.Ino || a.Caps.Dirty != cephCapFileWR || a.Caps.CapID != current.CapID ||
				!last.Admitted || last.Generation != session.Status().Generation || a.Received.Before(last.Started) {
				t.Fatalf("FLUSH_ACK %d does not correlate with its explicit recovery send: %+v %+v", i, a, last)
			}
		}
		if acks[1].Arrival <= acks[0].Arrival {
			t.Fatal("recovered FLUSH_ACKs arrived out of flush order", acks)
		}
		if k0, k1 := uncertain.Attempts[len(uncertain.Attempts)-1].Kind, refused.Attempts[len(refused.Attempts)-1].Kind; k0 != "resubmit" || k1 != "first-send" ||
			len(uncertain.Attempts) != 2 || len(refused.Attempts) != 2 || refused.Attempts[0].Admitted || !uncertain.Attempts[0].Admitted {
			t.Fatal("recovery attempt classes differ", uncertain.Attempts, refused.Attempts)
		}
		recoveryFacts["flushes"] = []cephfsHeadFlush{cloneCephFSHeadFlush(uncertain), cloneCephFSHeadFlush(refused)}
		recoveryFacts["acks"] = acks
		f.MTime = times[1]
		closeFile(t, f, cephFileModeRDWR)
		head, holes, err := osdIO.read(f.Ino, layout, 0, sizes[1], cephmsgr.OSDHeadSnapshot, trunc)
		if err != nil || len(holes) != 0 || !bytes.Equal(head, image) {
			t.Fatal("Go head bytes differ after cap-send recovery", err, holes)
		}
		nativeRead(t, f, f.Name, image)
		stat := req(t, "stat", f.Name).Native.Stat
		if stat == nil || !stat.Exists || stat.Ino != f.Ino || stat.Size != sizes[1] || stat.MTime != int64(times[1].Seconds) {
			t.Fatal("native metadata after cap-send recovery differs", stat)
		}
		f.NativeStat = stat
		recoveryFacts["native_stat"] = stat
	}) {
		return
	}
	// revoke carries the revoke-time writeback into the fence leaf, which
	// evicts the same client instance.
	var revoke *cephfsRevokeWriteback
	if recipe == cephfsDataWritebackFenceRestart && !leaf("revoke_time_writeback_interrupted_latched_and_evicted", func(t *testing.T) {
		shared := files["shared-file"]
		if shared == nil || shared.Ino == 0 {
			t.Fatal("the shared file of the revoke-time writeback is missing")
		}
		f := &cephfsDataFileEvidence{Name: shared.Name, Ino: shared.Ino}
		rw := map[string]any{"file": f}
		facts["revoke_writeback"] = rw
		st := open(t, f.Ino, cephFileModeRDWR, f.Name)
		f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		waitCap(t, f.Ino, "Fwb for the buffered write", 15*time.Second, func(c cephfsCap) bool {
			return c.Issued&(cephCapFileWR|cephCapFileBuffer) == cephCapFileWR|cephCapFileBuffer
		})
		ensureMaxSize(t, f, plan.SharedSize)
		trunc := truncOf(f.Ino)
		context := snapc(t, f.Ino)
		f.Truncate, f.SnapContext = &trunc, context
		// A overwrites file range [0, 64 KiB), the first extent of object 0;
		// B overwrites [64 KiB, 128 KiB), the first extent of object 1. Both
		// stay inside the file, so the size does not tell the branches apart;
		// their bytes do.
		su := plan.StripeUnit
		extA, extB := layout.fileToExtents(0, su), layout.fileToExtents(su, su)
		if len(extA) != 1 || len(extB) != 1 || extA[0].ObjectNo == extB[0].ObjectNo || su*2 > plan.SharedSize {
			t.Fatal("A and B must each be one extent of a distinct object", extA, extB)
		}
		dataA := cephfsDataStream("ceph-msgr-cephfs-data-revoke-a:"+oracle.Token, su)
		dataB := cephfsDataStream("ceph-msgr-cephfs-data-revoke-b:"+oracle.Token, su)
		imageA := slices.Clone(sharedPayload)
		copy(imageA[:su], dataA)
		imageAB := slices.Clone(imageA)
		copy(imageAB[su:2*su], dataB)
		ledger := &cephfsWritebackLedger{}
		rw["ledger"], rw["context"], rw["extents"] = ledger, context, map[string]any{"a": extA[0], "b": extB[0]}
		identity := client.Snapshot()
		gidArg, nonceArg := strconv.FormatUint(identity.GlobalID, 10), strconv.FormatUint(uint64(identity.CephFSNonce), 10)
		leafWrites := func() []cephfsDataObjectIO {
			osdIO.mu.Lock()
			defer osdIO.mu.Unlock()
			var writes []cephfsDataObjectIO
			for _, op := range osdIO.log {
				if op.Leaf == osdIO.leaf && op.Op == "write" {
					writes = append(writes, op)
				}
			}
			return writes
		}

		// 1. A completes (ONDISK) and its metadata flush is acknowledged.
		timeA := newMTime(f.Ino)
		if out := ledger.writeback(osdIO, "revoke-a", f.Ino, layout, 0, dataA, context, timeA, trunc, 30*time.Second); !slices.Equal(out, []string{cephfsWriteCompleted}) {
			t.Fatal("A did not complete", out, ledger.Writes)
		}
		if err := agent.markDirty(f.Ino, plan.SharedSize, timeA, context.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		f.MTime = timeA
		flush(t, f)
		rw["accepted_flush"] = map[string]any{"tid": f.FlushTID, "message": f.FlushMessage, "ack": f.FlushAck}

		// 2. B is buffered; only the revoke callback writes it back, while the
		// owned OSD drops client messages after Messenger receipt.
		_, n := observe(t)
		if n == nil || !strings.HasSuffix(n.Address, ":0/"+nonceArg) {
			t.Fatal("native session address does not carry the shared nonce", n)
		}
		rw["osd_drop"] = req(t, "osd-drop-on", "").OSDDrop
		timeB := newMTime(f.Ino)
		buffered := time.Now()
		if err := agent.markDirty(f.Ino, plan.SharedSize, timeB, context.Sequence, true, 0,
			ledger.bufferedWriteback(osdIO, "revoke-b", f.Ino, layout, su, dataB, context, timeB, trunc, 5*time.Second)); err != nil {
			t.Fatal(err)
		}

		// 3. A native reader starts behind the drop. Its open makes the MDS
		// revoke FILE_BUFFER, and the callback's writeback of B is dropped.
		rw["reader_started"] = req(t, "reader-start", "").Reader
		var latched cephfsDirtyFile
		deadline := time.Now().Add(45 * time.Second)
		for latched.Latch == nil {
			latched, _ = agent.dirtyState(f.Ino)
			if latched.Latch == nil && time.Now().After(deadline) {
				t.Fatalf("the revoke-time writeback did not latch: %+v", latched)
			}
			time.Sleep(20 * time.Millisecond)
		}
		rw["latch"] = latched.Latch
		if l := latched.Latch; l.Source != "revoke-callback" || l.Outcome != cephfsWriteUncertain || !slices.Equal(l.Extents, []string{cephfsWriteUncertain}) ||
			!latched.Buffered || l.Began.Before(buffered) {
			t.Fatalf("the interrupted revoke-time writeback is not an uncertain latch: %+v", l)
		}
		if len(ledger.Writes) != 2 || ledger.Writes[1].Phase != "revoke-b" || ledger.Writes[1].Outcome != cephfsWriteUncertain {
			t.Fatal("B's writeback ledger differs", ledger.Writes)
		}
		uncertain := ledger.Writes[1].IO
		if uncertain.Submission == nil || uncertain.Submission.GlobalID != identity.GlobalID || uncertain.Submission.MapEpoch == 0 || uncertain.ObjectNo != extB[0].ObjectNo ||
			uncertain.Offset != extB[0].Offset || uncertain.Length != extB[0].Length || uncertain.RequestSHA256 == "" {
			t.Fatalf("uncertain write identity differs: %+v", uncertain)
		}
		rw["uncertain_write"] = uncertain
		var dropLog *nativeMDSCephFSDataOSDDropLog
		deadline = time.Now().Add(15 * time.Second)
		for {
			r, err := control.request(ctx, "osd-log", gidArg, nonceArg)
			if err != nil {
				t.Fatal("OSD drop log", err)
			}
			dropLog = r.OSDLog
			if len(dropLog.Lines) != 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("owned OSD did not record the attributed drop of the revoke-time write")
			}
			time.Sleep(200 * time.Millisecond)
		}
		rw["osd_drop_log"] = dropLog
		if len(dropLog.Lines) != 2 || !strings.Contains(dropLog.Lines[0], " from=client."+gidArg+" type=42 ") {
			t.Fatal("the OSD did not drop exactly one object request of this client", dropLog.Lines)
		}
		pending := req(t, "reader-status", "").Reader
		rw["reader_pending_during_drop"] = pending
		if pending.State != "running" {
			t.Fatal("the native reader ended while FILE_BUFFER was held", pending)
		}
		rw["osd_drop_restored"] = req(t, "osd-drop-off", "").OSDDrop
		restored := time.Now()

		// 4. The obligation outlives the drop: the reader still waits, B is not
		// resubmitted and FILE_BUFFER is still implemented.
		const held = 3 * time.Second
		time.Sleep(held)
		still := req(t, "reader-status", "").Reader
		rw["reader_pending_after_restore"] = still
		if still.State != "running" {
			t.Fatal("the native reader ended before the explicit decision", still)
		}
		if d, _ := agent.dirtyState(f.Ino); d.Latch == nil || !d.Buffered || len(ledger.Writes) != 2 || len(leafWrites()) != 2 {
			t.Fatal("the latched writeback was resubmitted or resolved without a decision", d.Latch, len(ledger.Writes))
		}
		if c, _ := agent.cap(f.Ino); c.Implemented&cephCapFileBuffer == 0 {
			t.Fatal("FILE_BUFFER was released before the decision", cephCapString(c.Implemented))
		}

		// 5. The consumer's explicit decision evicts the uncertain buffered
		// data; the revoke completes and the reader reaches a terminal result.
		evictedAt := time.Now()
		msg, sent, err := agent.evictBuffered(f.Ino)
		if err != nil || !sent || msg.Caps&cephCapFileBuffer != 0 {
			t.Fatal("eviction did not release FILE_BUFFER", msg, sent, err)
		}
		rw["eviction_update"] = msg
		result := req(t, "reader-wait", "")
		rw["reader_result"], rw["reader_timings_ns"] = result.Reader, result.Native.TimingsNS
		got, want := *result.Native.Read, cephfsDataDigest(imageA)
		if got.Length != want.Length || got.SHA256 != want.SHA256 || !slices.Equal(got.Chunks, want.Chunks) || result.Native.Stat == nil || result.Native.Stat.Size != plan.SharedSize {
			branch := "neither"
			if got.SHA256 == cephfsDataDigest(imageAB).SHA256 {
				branch = "the A+B"
			}
			t.Fatalf("the native reader did not see the A branch (it saw %s branch): length %d", branch, got.Length)
		}
		f.NativeRead, f.NativeStat = &got, result.Native.Stat
		if read := time.Duration(result.Native.TimingsNS["read"]); read < evictedAt.Sub(restored) {
			t.Fatalf("the native read completed %v after its start, before the %v the obligation was held after the drop", read, evictedAt.Sub(restored))
		}

		// 6. Independent observation: native object sizes and Go head bytes
		// show A landed and B did not.
		checkObjects(t, f, cephfsObjectSizes(layout, [][2]uint64{{0, plan.SharedSize}}))
		head, holes, err := osdIO.read(f.Ino, layout, 0, plan.SharedSize, cephmsgr.OSDHeadSnapshot, trunc)
		if err != nil || len(holes) != 0 || !bytes.Equal(head, imageA) {
			t.Fatal("Go head bytes differ from the A branch", err, holes)
		}
		d, _ := agent.dirtyState(f.Ino)
		rw["dirty_events"] = d.Events
		if d.Latch != nil || d.Buffered || len(d.Resolved) != 1 || d.Resolved[0].Decision != "evicted" || d.Resolved[0].Outcome != cephfsWriteUncertain {
			t.Fatalf("the eviction lost the uncertain outcome: %+v", d)
		}
		writebacks, release := 0, -1
		for i, ev := range d.Events {
			if ev.At.Before(buffered) {
				continue
			}
			switch {
			case ev.Kind == "data-writeback":
				writebacks++
				if ev.Error == "" {
					t.Fatal("a revoke-time writeback was reported complete", ev)
				}
			case ev.Kind == "cap-update" && ev.At.Before(evictedAt) && !strings.Contains(ev.Caps, "b"):
				t.Fatalf("FILE_BUFFER was released before the decision: %+v", ev)
			case ev.Kind == "cap-update" && !ev.At.Before(evictedAt) && release < 0:
				release = i
			}
		}
		if writebacks != 1 || release < 0 || strings.Contains(d.Events[release].Caps, "b") {
			t.Fatalf("writeback attempts or the release after the decision differ: %d %+v", writebacks, d.Events)
		}
		if ledger.mayResend(1) == nil || len(leafWrites()) != 2 {
			t.Fatal("the uncertain B became resendable or was resubmitted")
		}
		rw["object_writes"] = leafWrites()
		closeFile(t, f, cephFileModeRDWR)
		revoke = &cephfsRevokeWriteback{Ino: f.Ino, Offset: su, Length: su, Context: context, Trunc: trunc, ImageA: imageA, Ledger: ledger}
	}) {
		return
	}
	if recipe == cephfsDataWritebackFenceRestart && !leaf("interrupted_writeback_fenced_across_mds_restart_and_eviction", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "capsnap-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		sizes := plan.CapSnapSizes
		ensureMaxSize(t, f, sizes[2])
		if c, _ := agent.cap(f.Ino); c.Issued&cephCapFileWR == 0 {
			t.Fatalf("file lacks Fw: %s", cephCapString(c.Issued))
		}
		trunc := truncOf(f.Ino)
		context := snapc(t, f.Ino)
		genA := cephfsDataStream(plan.Seeds["capsnap-file-a"], sizes[0])
		genB := cephfsDataStream(plan.Seeds["capsnap-file-b"], sizes[1])
		genC := cephfsDataStream(plan.Seeds["capsnap-file-c"], sizes[2])
		ledger := &cephfsWritebackLedger{}
		wb := map[string]any{"context": context, "sizes": sizes, "ledger": ledger}
		facts["writeback_fence"] = wb
		identityBefore := client.Snapshot()
		gidArg, nonceArg := strconv.FormatUint(identityBefore.GlobalID, 10), strconv.FormatUint(uint64(identityBefore.CephFSNonce), 10)
		expectedA := cephfsObjectSizes(layout, [][2]uint64{{0, sizes[0]}})

		// 1. Generation A: every object write completes (ONDISK) before its
		// metadata flush, which reports only the committed end.
		timeA := newMTime(f.Ino)
		if out := ledger.writeback(osdIO, "a", f.Ino, layout, 0, genA, context, timeA, trunc, 30*time.Second); !slices.Equal(out, []string{cephfsWriteCompleted, cephfsWriteCompleted}) {
			t.Fatal("generation A writeback did not complete", out, ledger.Writes)
		}
		if end := ledger.committedEnd(0); end != sizes[0] {
			t.Fatal("generation A does not cover its size", end)
		}
		if err := agent.markDirty(f.Ino, ledger.committedEnd(0), timeA, context.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		f.MTime = timeA
		flush(t, f)
		if m := f.FlushMessage; m.Size != sizes[0] || m.MTime != timeA || m.SnapFollows != context.Sequence {
			t.Fatalf("accepted flush differs: %+v", m)
		}
		acceptedAt := f.FlushAck.Received
		wb["accepted_flush"] = map[string]any{"tid": f.FlushTID, "message": f.FlushMessage, "ack": f.FlushAck}

		// 2. The writeback of B's tail is interrupted: the owned OSD drops
		// client messages after Messenger receipt, so the bounded local wait
		// ends without a reply. The consumer records an unknown outcome and
		// never reports B's size in a metadata flush.
		_, n := observe(t)
		if n == nil || !strings.HasSuffix(n.Address, ":0/"+nonceArg) {
			t.Fatal("native session address does not carry the shared nonce", n)
		}
		goAddress := n.Address
		wb["go_session_address"] = goAddress
		wb["osd_drop"] = req(t, "osd-drop-on", "").OSDDrop
		timeB := newMTime(f.Ino)
		uncertainAt := time.Now()
		out := ledger.writeback(osdIO, "b", f.Ino, layout, sizes[0], genB[sizes[0]:], context, timeB, trunc, 5*time.Second)
		if !slices.Equal(out, []string{cephfsWriteUncertain}) {
			t.Fatal("interrupted writeback was not an unknown outcome", out, ledger.Writes[len(ledger.Writes)-1])
		}
		uncertain := ledger.Writes[len(ledger.Writes)-1].IO
		tail := layout.fileToExtents(sizes[0], sizes[1]-sizes[0])
		if len(tail) != 1 || uncertain.Submission == nil || uncertain.Submission.GlobalID != identityBefore.GlobalID || uncertain.Submission.MapEpoch == 0 ||
			uncertain.ObjectNo != tail[0].ObjectNo || uncertain.Offset != tail[0].Offset || uncertain.Length != tail[0].Length || uncertain.RequestSHA256 == "" {
			t.Fatalf("uncertain write identity differs: %+v", uncertain)
		}
		var dropLog *nativeMDSCephFSDataOSDDropLog
		deadline := time.Now().Add(15 * time.Second)
		for {
			r, err := control.request(ctx, "osd-log", gidArg, nonceArg)
			if err != nil {
				t.Fatal("OSD drop log", err)
			}
			dropLog = r.OSDLog
			if len(dropLog.Lines) != 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("owned OSD did not record the attributed drop of the interrupted write")
			}
			time.Sleep(200 * time.Millisecond)
		}
		wb["osd_drop_log"] = dropLog
		if len(dropLog.Lines) != 2 || !strings.Contains(dropLog.Lines[0], " from=client."+gidArg+" type=42 ") {
			t.Fatal("the OSD did not drop exactly one object request of this client", dropLog.Lines)
		}
		wb["osd_drop_restored"] = req(t, "osd-drop-off", "").OSDDrop
		if end := ledger.committedEnd(0); end != sizes[0] {
			t.Fatal("the committed end moved past completed data", end)
		}
		if d, _ := agent.dirtyState(f.Ino); d.Dirty != 0 || d.Flushing != 0 || d.Buffered {
			t.Fatal("the uncertain write left dirty metadata", d)
		}
		// Independent observation: B never reached its object, which keeps
		// generation A's size.
		checkObjects(t, f, expectedA)
		wb["objects_after_interrupt"] = f.ObjectSizes

		// 3. Owned MDS restart with the uncertain write outstanding.
		restartFacts := map[string]any{}
		wb["restart"] = restartFacts
		preRestart := agent
		connection, session, agent, target = restartAcrossInterruptedWriteback(t, ctx, control, client, dials, connection, session, agent, realms, metadata, osdIO, f.Ino, restartFacts)
		after := req(t, "stat", f.Name).Native.Stat
		if after == nil || !after.Exists || after.Ino != f.Ino || after.Size != sizes[0] || after.MTime != int64(timeA.Seconds) {
			t.Fatal("the accepted flush did not survive the restart", after)
		}
		wb["native_stat_after_restart"] = after

		// 4. Capability revoke by a native reader: it sees only completed data.
		nativeRead(t, f, f.Name, genA)
		var revokes []cephfsDirtyEvent
		for _, a := range []*cephfsCapAgent{preRestart, agent} {
			d, _ := a.dirtyState(f.Ino)
			for _, ev := range d.Events {
				if ev.Kind == "revoke" && !ev.At.Before(uncertainAt) && !slices.ContainsFunc(revokes, func(r cephfsDirtyEvent) bool { return r.At.Equal(ev.At) && r.Seq == ev.Seq }) {
					revokes = append(revokes, ev)
				}
			}
			if tids := a.dirtyFlushesSince(f.Ino, acceptedAt); len(tids) != 0 {
				t.Fatal("a metadata flush followed the accepted one without completed writeback", tids)
			}
		}
		wb["revokes"] = revokes
		if len(revokes) == 0 {
			t.Fatal("no capability revoke reached the writer while its write was uncertain")
		}

		// 5. Eviction fences this address instance: the client's later object
		// write is refused by the OSD and is known not applied.
		lostSeen := make(chan time.Time, 1)
		go func(s *cephmsgr.MDSSession) {
			<-s.Done()
			lostSeen <- time.Now()
		}(session)
		evictedAt := time.Now()
		evicted, err := control.request(ctx, "evict", gidArg)
		if err != nil {
			t.Fatal("evict the Go session", err)
		}
		wb["eviction"] = evicted.Mutation
		if evicted.Mutation.Address != goAddress {
			t.Fatal("eviction names another address", evicted.Mutation)
		}
		var entry *nativeMDSSessionsBlock
		for i := range evicted.Blocklist {
			if evicted.Blocklist[i].Address == goAddress && !evicted.Blocklist[i].Range {
				entry = &evicted.Blocklist[i]
			}
		}
		if entry == nil || entry.Until != evicted.Mutation.Until {
			t.Fatal("the evicted address instance is not blocklisted", evicted.Blocklist)
		}
		select {
		case lost := <-lostSeen:
			wb["lost_after_evict_ms"] = lost.Sub(evictedAt).Milliseconds()
		case <-time.After(40 * time.Second):
			t.Fatal("evicted CephFS session did not end")
		}
		wb["evicted_session"], wb["evicted_session_error"] = session.Status(), describeRecoveryError(session.Err())
		if session.Status().State != cephmsgr.MDSSessionLost {
			t.Fatal("evicted session did not end Lost", session.Status(), session.Err())
		}
		select {
		case <-agent.done:
		case <-time.After(10 * time.Second):
			t.Fatal("evicted session's agent did not stop")
		}
		timeC := cephfsMDSTime(time.Now())
		if !cephfsTimeAfter(timeC, timeB) {
			timeC = cephmsgr.MDSTime{Seconds: timeB.Seconds + 1}
		}
		out = ledger.writeback(osdIO, "c", f.Ino, layout, sizes[1], genC[sizes[1]:], context, timeC, trunc, 30*time.Second)
		if !slices.Equal(out, []string{cephfsWriteFenced}) {
			t.Fatal("the evicted client's object write was not fenced", out, ledger.Writes[len(ledger.Writes)-1])
		}
		for i, w := range ledger.Writes {
			if err := ledger.mayResend(i); (err == nil) != (w.Outcome == cephfsWriteFenced) {
				t.Fatal("recovery rule differs", i, w.Outcome, err)
			}
		}
		wantWrites := 4
		if revoke != nil {
			// The same fenced instance probes the revoke-time writeback's B
			// extent with new bytes (never B's uncertain write again): the
			// OSD refuses it with -EBLOCKLISTED, so it is known not applied.
			probe := cephfsDataStream("ceph-msgr-cephfs-data-revoke-fence:"+oracle.Token, revoke.Length)
			out := revoke.Ledger.writeback(osdIO, "revoke-fence", revoke.Ino, layout, revoke.Offset, probe, revoke.Context, timeC, revoke.Trunc, 30*time.Second)
			fenced := revoke.Ledger.Writes[len(revoke.Ledger.Writes)-1]
			if !slices.Equal(out, []string{cephfsWriteFenced}) || fenced.IO.OSDError != cephEBLOCKLISTED {
				t.Fatal("the evicted client's probe of the revoke-time extent was not fenced", out, fenced)
			}
			wb["revoke_fence_probe"] = fenced
			wantWrites++
		}

		// 6. Recovery through a new client instance: it repeats no write,
		// resends nothing uncertain and finds only generation A's data.
		wb["evicted_agent"] = agent.snapshot()
		osdIO.reset()
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		dials = newRecoveryDials(true, control.fresh.MDS.Endpoint)
		client = recoveryClient(top, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = true, true, true })
		replacement := client.Snapshot()
		if replacement.GlobalID == identityBefore.GlobalID || replacement.CephFSNonce == 0 || replacement.CephFSNonce == identityBefore.CephFSNonce {
			t.Fatal("replacement client reuses the fenced identity", replacement.GlobalID, replacement.CephFSNonce)
		}
		gid, control.goGID, osdIO.client = replacement.GlobalID, replacement.GlobalID, client
		wb["fenced_client"] = map[string]any{"global_id": identityBefore.GlobalID, "cephfs_nonce": identityBefore.CephFSNonce}
		wb["replacement_client"] = map[string]any{"global_id": replacement.GlobalID, "cephfs_nonce": replacement.CephFSNonce}
		connection, err = client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal("open the replacement client's MDS", err)
		}
		session, err = connection.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
		if err != nil {
			t.Fatal("open the replacement CephFS session", err)
		}
		agent = startCephFSCapAgent(ctx, session)
		_, n = observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil || !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(replacement.CephFSNonce), 10)) {
			t.Fatal("replacement native session differs", n, err)
		}
		checkObjects(t, f, expectedA)
		wb["objects_after_recovery"] = f.ObjectSizes
		found := lookup(t, dirIno, f.Name)
		if found.Ino != f.Ino || found.Size != sizes[0] || found.MTime != timeA {
			t.Fatalf("MDS metadata after eviction differs: size %d mtime %+v", found.Size, found.MTime)
		}
		// The inode was never truncated: the replacement sends the same state.
		head, holes, err := osdIO.read(f.Ino, layout, 0, sizes[0], cephmsgr.OSDHeadSnapshot, trunc)
		if err != nil || len(holes) != 0 || !bytes.Equal(head, genA) {
			t.Fatal("Go head bytes after recovery differ", err, holes)
		}
		nativeRead(t, f, f.Name, genA)
		stat := req(t, "stat", f.Name).Native.Stat
		if stat == nil || !stat.Exists || stat.Ino != f.Ino || stat.Size != sizes[0] || stat.MTime != int64(timeA.Seconds) {
			t.Fatal("native metadata after recovery differs", stat)
		}
		f.NativeStat = stat
		if revoke != nil {
			// Neither the evicted B nor the fenced probe reached shared-file.
			wb["revoke_native_after_recovery"] = nativeRead(t, nil, "shared-file", revoke.ImageA).Native.Read
			head, holes, err := osdIO.read(revoke.Ino, layout, 0, uint64(len(revoke.ImageA)), cephmsgr.OSDHeadSnapshot, revoke.Trunc)
			if err != nil || len(holes) != 0 || !bytes.Equal(head, revoke.ImageA) {
				t.Fatal("Go head bytes of the revoke-time file differ after recovery", err, holes)
			}
		}
		osdIO.mu.Lock()
		var writes []cephfsDataObjectIO
		for _, op := range osdIO.log {
			if op.Leaf == osdIO.leaf && op.Op == "write" {
				writes = append(writes, op)
			}
		}
		osdIO.mu.Unlock()
		wb["object_writes"] = writes
		if len(writes) != wantWrites || len(ledger.Writes) != 4 || ledger.count(cephfsWriteCompleted, "a") != 2 || ledger.count(cephfsWriteUncertain, "b") != 1 ||
			ledger.count(cephfsWriteFenced, "c") != 1 {
			t.Fatal("object writes were repeated or misclassified", len(writes), ledger.Writes)
		}
	}) {
		return
	}
	if recipe == cephfsDataCapSnapRestart && !leaf("dirty_data_capsnaps_across_two_snapshots_and_mds_restart", func(t *testing.T) {
		f := &cephfsDataFileEvidence{Name: "capsnap-file"}
		files[f.Name] = f
		st := create(t, f.Name)
		f.Ino, f.ReplyCap, f.ReplyCaps, f.ReplyMaxSize = st.Ino, st.Cap, cephCapString(st.Cap.Caps), st.MaxSize
		checkLayout(t, f, st)
		ensureMaxSize(t, f, plan.CapSnapSizes[2])
		c, _ := agent.cap(f.Ino)
		if c.Issued&(cephCapFileWR|cephCapFileBuffer) != (cephCapFileWR | cephCapFileBuffer) {
			t.Fatalf("file lacks Fwb: %s", cephCapString(c.Issued))
		}
		agent.enableCapSnaps(realms)
		var images [3][]byte
		var times [3]cephmsgr.MDSTime
		var contexts [3]cephmsgr.OSDSnapContext
		var ids [2]uint64
		trunc := truncOf(f.Ino)
		for generation, letter := range []string{"a", "b"} {
			images[generation] = cephfsDataStream(plan.Seeds["capsnap-file-"+letter], plan.CapSnapSizes[generation])
			times[generation] = newMTime(f.Ino)
			// Capture values, not the loop variables, in this retained writeback.
			payload, mtime := images[generation], times[generation]
			context, err := agent.capSnapContext(f.Ino)
			if err != nil {
				t.Fatal(err)
			}
			contexts[generation] = context
			if err := agent.markDirty(f.Ino, uint64(len(payload)), mtime, context.Sequence, true, 0, func() error {
				return osdIO.write(f.Ino, layout, 0, payload, context, mtime, trunc)
			}); err != nil {
				t.Fatal(err)
			}
			created := req(t, "capsnap-create-"+letter, "")
			if created.Mutation == nil || created.Mutation.State != "applied" || !created.Native.Stat.Exists {
				t.Fatal("cap snapshot creation differs", created)
			}
			capSnapsCreated[generation] = true
			deadline := time.Now().Add(15 * time.Second)
			for {
				state := agent.snapshot()
				found := slices.ContainsFunc(state.CapSnaps, func(s cephfsCapSnap) bool {
					return s.Ino == f.Ino && s.Context.Sequence == context.Sequence && s.Buffered
				})
				if found {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("dirty cap snap %s was not queued: %+v", letter, state)
				}
				time.Sleep(10 * time.Millisecond)
			}
			after, err := agent.capSnapContext(f.Ino)
			if err != nil || after.Sequence <= context.Sequence || len(after.Snapshots) != len(context.Snapshots)+1 {
				t.Fatal("snapshot context did not advance", after, context, err)
			}
			ids[generation] = after.Sequence
		}
		// Both data generations are retained before either FLUSHSNAP is sent.
		// This establishes dirty generation overlap, not simultaneous pending ACKs.
		queued := agent.snapshot().CapSnaps
		if len(queued) != 2 || !queued[0].Buffered || !queued[1].Buffered || queued[0].Transaction != 0 || queued[1].Transaction != 0 {
			t.Fatalf("two immutable dirty generations were not retained: %+v", queued)
		}
		facts["capsnap_queued"] = queued
		osdIO.mu.Lock()
		beforeWriteback := len(osdIO.log)
		osdIO.mu.Unlock()
		if err := agent.prepareCapSnaps(f.Ino); err != nil {
			t.Fatal("prepare cap-snap writeback and original flush identities", err)
		}
		prepared := agent.snapshot().CapSnaps
		for _, s := range prepared {
			if s.Transaction == 0 || s.Buffered || !s.Sent.IsZero() || !s.Acked.IsZero() || s.WritebackCompleted.IsZero() || s.Error != "" {
				t.Fatal("cap snap was sent or did not finish writeback before restart", s)
			}
		}
		osdIO.mu.Lock()
		written := slices.Clone(osdIO.log[beforeWriteback:])
		osdIO.mu.Unlock()
		if len(written) != 4 {
			t.Fatal("expected two object writes per prepared snapshot generation", written)
		}
		for _, op := range written {
			if op.Op != "write" || op.Code != 0 || op.ReplyFlags&cephmsgr.OSDReplyOnDisk == 0 || op.RequestBytes == 0 || op.RequestSHA256 == "" {
				t.Fatal("prepared generation lacks independent ONDISK completion metadata", op)
			}
		}
		recoveryFacts := map[string]any{"writeback": written}
		facts["capsnap_recovery"] = recoveryFacts
		connection, session, agent, target = reconnectPreparedCapSnaps(t, ctx, control, client, dials, connection, session, agent, realms, metadata, osdIO, f.Ino, ids[0], recoveryFacts)
		_, nativeSession := observe(t)
		if err := checkNativeCephFSSession(nativeSession, identity, metadata); err != nil || nativeSession.Caps == 0 ||
			!strings.HasSuffix(nativeSession.Address, ":0/"+strconv.FormatUint(uint64(client.Snapshot().CephFSNonce), 10)) {
			t.Fatal("native session after cap-snap reconnect differs", nativeSession, err)
		}
		recoveryFacts["native_session_after"] = nativeSession
		deadline := time.Now().Add(15 * time.Second)
		for {
			snaps := agent.snapshot().CapSnaps
			if len(snaps) == 2 && !snaps[0].Acked.IsZero() && !snaps[1].Acked.IsZero() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("cap-snap ACKs missing: %+v", agent.snapshot())
			}
			time.Sleep(10 * time.Millisecond)
		}
		flushed := agent.snapshot().CapSnaps
		for i, s := range flushed {
			if s.Caps.Size != plan.CapSnapSizes[i] || s.Caps.MTime != times[i] || s.Caps.Dirty != cephCapFileWR || s.Context.Sequence != contexts[i].Sequence ||
				s.WritebackCompleted.Before(s.WritebackStarted) || s.Sent.Before(s.WritebackCompleted) || s.Acked.Before(s.WritebackCompleted) || s.Transaction != prepared[i].Transaction || s.Buffered || s.Error != "" {
				t.Fatalf("cap snap %d differs: %+v", i, s)
			}
		}
		if flushed[1].Transaction <= flushed[0].Transaction {
			t.Fatal("cap-snap flush TIDs did not increase", flushed)
		}
		if err := validateMixedCapSnapRecovery(flushed, session.Status()); err != nil {
			t.Fatal("mixed original-TID resubmission and first-send evidence differs", err, flushed)
		}
		images[2] = cephfsDataStream(plan.Seeds["capsnap-file-c"], plan.CapSnapSizes[2])
		times[2] = newMTime(f.Ino)
		context, err := agent.capSnapContext(f.Ino)
		if err != nil || !slices.Contains(context.Snapshots, ids[0]) || !slices.Contains(context.Snapshots, ids[1]) {
			t.Fatal("head context omits snapshots", context, err)
		}
		contexts[2] = context
		if err := osdIO.write(f.Ino, layout, 0, images[2], context, times[2], trunc); err != nil {
			t.Fatal(err)
		}
		if err := agent.markDirty(f.Ino, plan.CapSnapSizes[2], times[2], context.Sequence, false, 0, nil); err != nil {
			t.Fatal(err)
		}
		f.MTime = times[2]
		flush(t, f)
		closeFile(t, f, cephFileModeRDWR)
		lists := map[uint64]cephmsgr.OSDSnapshotList{}
		for objno := uint64(0); objno < 2; objno++ {
			list, err := osdIO.listSnaps(f.Ino, objno)
			if err != nil || len(list.Clones) != 3 || list.Sequence != context.Sequence {
				t.Fatal("two-snapshot clone list differs", objno, list, err)
			}
			for i, clone := range list.Clones {
				expectedID := cephmsgr.OSDHeadSnapshot
				if i < 2 {
					expectedID = ids[i]
				}
				expectedSize := cephfsObjectSizes(layout, [][2]uint64{{0, plan.CapSnapSizes[i]}})[objno]
				if clone.ID != expectedID || clone.Size != expectedSize || len(clone.Overlap) != 0 || i < 2 && !slices.Equal(clone.Snapshots, []uint64{ids[i]}) {
					t.Fatalf("clone %d object %d differs: %+v", i, objno, clone)
				}
			}
			lists[objno] = list
		}
		native := nativeObjects(t, f.Name, f.Ino)
		f.NativeObjects = native
		for _, obj := range native {
			list := lists[uint64(obj.ObjNo)]
			if obj.Snaps == nil || obj.Snaps.Seq != list.Sequence || len(obj.Snaps.Clones) != 3 {
				t.Fatal("native two-snapshot clones differ", obj)
			}
			for i, clone := range obj.Snaps.Clones {
				g := list.Clones[i]
				if clone.Head != (g.ID == cephmsgr.OSDHeadSnapshot) || !clone.Head && clone.ID != g.ID || clone.Size != g.Size || !slices.Equal(clone.Snapshots, g.Snapshots) || len(clone.Overlaps) != 0 {
					t.Fatal("native cap-snap clone differs", obj.ObjNo, i, clone, g)
				}
			}
		}
		for i, letter := range []string{"a", "b"} {
			got, holes, err := osdIO.read(f.Ino, layout, 0, plan.CapSnapSizes[i], ids[i], trunc)
			if err != nil || len(holes) != 0 || !bytes.Equal(got, images[i]) {
				t.Fatal("Go cap-snap bytes differ", letter, err, holes)
			}
			target := "capsnap-" + letter + "-file"
			nativeRead(t, nil, target, images[i])
			stat := req(t, "stat", target).Native.Stat
			if stat == nil || !stat.Exists || stat.Size != plan.CapSnapSizes[i] || stat.MTime != int64(times[i].Seconds) {
				t.Fatal("native cap-snap metadata differs", letter, stat)
			}
		}
		head, holes, err := osdIO.read(f.Ino, layout, 0, plan.CapSnapSizes[2], cephmsgr.OSDHeadSnapshot, trunc)
		if err != nil || len(holes) != 0 || !bytes.Equal(head, images[2]) {
			t.Fatal("Go cap-snap head differs", err, holes)
		}
		nativeRead(t, f, f.Name, images[2])
		facts["dirty_cap_snaps"] = map[string]any{"snapshot_ids": ids, "contexts": contexts, "completed": flushed, "lists": lists, "native_objects": native}
	}) {
		return
	}

	t.Run("cleanup_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		if session == nil || session.Status().State != cephmsgr.MDSSessionOpen {
			t.Fatal("the Go session is not open for cleanup")
		}
		for i, letter := range []string{"a", "b"} {
			if capSnapsCreated[i] {
				removed := req(t, "capsnap-remove-"+letter, "")
				if removed.Mutation == nil || removed.Mutation.State != "applied" || removed.Native.Stat.Exists {
					t.Fatal("native cap snapshot removal differs", removed)
				}
			}
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
