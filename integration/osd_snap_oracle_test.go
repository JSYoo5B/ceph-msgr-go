package integration_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Native self-managed snapshot oracle helpers. Every observation comes from
// tools/osd_omap_write_native.py (librados C API, the pinned rados CLI and
// ceph-dencoder) inside the disposable fixture. Nothing here is product API.

// snapOracleHead is LIBRADOS_SNAP_HEAD: nativeSnapRead at the head object.
const snapOracleHead = ^uint64(0) - 1

// snapOracleContext is a write SnapContext. Snaps keep the caller's order
// (Ceph expects descending); librados set_write_ctx validates it natively.
type snapOracleContext struct {
	Seq   uint64
	Snaps []uint64
}

func (c snapOracleContext) MarshalJSON() ([]byte, error) {
	snaps := c.Snaps
	if snaps == nil {
		snaps = []uint64{}
	}
	return json.Marshal(struct {
		Seq   uint64   `json:"seq"`
		Snaps []uint64 `json:"snaps"`
	}{c.Seq, snaps})
}

// snapOracleAction extends the shared OMAP write action with the data/xattr
// operations admitted only in snapwrite: writefull, write, setxattr, rmxattr
// and delete. Name is the raw xattr name (nonempty, NUL-free natively).
type snapOracleAction struct {
	omapWriteAction
	Data   []byte `json:"data,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
	Name   []byte `json:"name,omitempty"`
}

func snapWriteFull(data []byte) snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "writefull"}, Data: data}
}

func snapWrite(offset uint64, data []byte) snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "write"}, Offset: offset, Data: data}
}

func snapSetXattr(name, value []byte) snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "setxattr", Value: value}, Name: name}
}

func snapRemoveXattr(name []byte) snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "rmxattr"}, Name: name}
}

func snapDelete() snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "delete"}}
}

func snapOmapSet(entries ...omapNativeEntry) snapOracleAction {
	return snapOracleAction{omapWriteAction: omapWriteAction{Op: "set", Entries: entries}}
}

// snapOracleInput is the osd-omap-write-input.json schema of the snapshot
// modes: snapcreate {count}; snapwrite {object, snapc, ordersnap, operations,
// mtime, inventory_keys, object_info}; snapread {object, snap,
// inventory_keys, object_info}; snaprollback {object, snap, snapc,
// inventory_keys, object_info}; listsnaps {object and/or raw}.
type snapOracleInput struct {
	Object        string             `json:"object,omitempty"`
	Count         int                `json:"count,omitempty"`
	Snap          *uint64            `json:"snap,omitempty"`
	SnapContext   *snapOracleContext `json:"snapc,omitempty"`
	OrderSnap     bool               `json:"ordersnap,omitempty"`
	Operations    []snapOracleAction `json:"operations,omitempty"`
	MTime         *omapWriteMTime    `json:"mtime,omitempty"`
	InventoryKeys [][]byte           `json:"inventory_keys,omitempty"`
	ObjectInfo    bool               `json:"object_info,omitempty"`
	Raw           *[]byte            `json:"raw,omitempty"`
}

// snapObjectInfo is the committed object_info_t that ceph-dencoder decodes
// from the empty-name xattr. OID.Snap is CEPH_NOSNAP (-2) for a head and the
// clone id at a snap. Version strings are eversion_t "epoch'version".
type snapObjectInfo struct {
	OID struct {
		Name      string `json:"oid"`
		Key       string `json:"key"`
		Snap      int64  `json:"snapid"`
		Hash      int64  `json:"hash"`
		Pool      int64  `json:"pool"`
		Namespace string `json:"namespace"`
	} `json:"oid"`
	Version            string   `json:"version"`
	PriorVersion       string   `json:"prior_version"`
	UserVersion        uint64   `json:"user_version"`
	Size               uint64   `json:"size"`
	MTime              string   `json:"mtime"`
	Flags              []string `json:"flags"`
	ExpectedObjectSize uint64   `json:"expected_object_size"`
	ExpectedWriteSize  uint64   `json:"expected_write_size"`
	AllocHintFlags     uint32   `json:"alloc_hint_flags"`
}

// parseSnapEversion splits an eversion_t dump ("epoch'version").
func parseSnapEversion(text string) (epoch uint32, version uint64, err error) {
	left, right, ok := strings.Cut(text, "'")
	if !ok {
		return 0, 0, fmt.Errorf("invalid native eversion %q", text)
	}
	e, err := strconv.ParseUint(left, 10, 32)
	if err != nil {
		return 0, 0, err
	}
	version, err = strconv.ParseUint(right, 10, 64)
	return uint32(e), version, err
}

type snapOracleEntry struct {
	Name  []byte `json:"name"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// snapOracleReadOp is one per-op result of the compound snapshot read:
// index 0 stat, 1 read, 2 getxattrs, 3 omap_vals, 4 omap_bykeys.
type snapOracleReadOp struct {
	Index   int               `json:"index"`
	Op      string            `json:"op"`
	Opcode  uint16            `json:"opcode"`
	Prval   int32             `json:"prval"`
	Size    *uint64           `json:"size"`
	Seconds *int64            `json:"seconds"`
	Nanos   *int64            `json:"nanos"`
	Data    []byte            `json:"data"`
	More    *bool             `json:"more"`
	Entries []snapOracleEntry `json:"entries"`
}

type snapOracleRead struct {
	Aggregate      int32              `json:"aggregate"`
	Version        uint64             `json:"version"` // rados_get_last_version after the read
	Operations     []snapOracleReadOp `json:"operations"`
	ObjectInfoCode *int32             `json:"object_info_code"`
	ObjectInfo     *snapObjectInfo    `json:"object_info"`
}

// Xattrs and Omap interpret a successful snapshot read. BYKEYS covers the
// empty key and the requested inventory; the bounded page must agree.
func (r snapOracleRead) Xattrs() map[string][]byte {
	result := map[string][]byte{}
	for _, entry := range r.Operations[2].Entries {
		result[string(entry.Name)] = entry.Value
	}
	return result
}

func (r snapOracleRead) Omap(t *testing.T) map[string][]byte {
	t.Helper()
	page, keyed := r.Operations[3], r.Operations[4]
	if r.Aggregate != 0 || page.Prval != 0 || keyed.Prval != 0 || page.More == nil || *page.More {
		t.Fatal("native snapshot OMAP inventory is incomplete", r.Aggregate, page.Prval, keyed.Prval)
	}
	result := map[string][]byte{}
	var nonempty []omapNativeEntry
	for _, entry := range keyed.Entries {
		result[string(entry.Key)] = entry.Value
		if len(entry.Key) != 0 {
			nonempty = append(nonempty, omapNativeEntry{Key: entry.Key, Value: entry.Value})
		}
	}
	var listed []omapNativeEntry
	for _, entry := range page.Entries {
		listed = append(listed, omapNativeEntry{Key: entry.Key, Value: entry.Value})
	}
	if !equalOmapEntries(listed, nonempty) {
		t.Fatal("native snapshot OMAP page has keys outside the known inventory")
	}
	return result
}

// snapToolResult preserves a native tool run: exit status, the errno parsed
// from cpp_strerror text on failure, the decoded JSON, and bounded stderr.
type snapToolResult struct {
	Exit   int             `json:"exit"`
	Code   *int32          `json:"code"`
	JSON   json.RawMessage `json:"json"`
	Stderr string          `json:"stderr"`
}

// snapCLIID is the rados CLI clone id: "head" or an unsigned clone id.
type snapCLIID struct {
	Head bool
	ID   uint64
}

func (id *snapCLIID) UnmarshalJSON(data []byte) error {
	if string(data) == `"head"` {
		*id = snapCLIID{Head: true}
		return nil
	}
	id.Head = false
	return json.Unmarshal(data, &id.ID)
}

// snapCLIListing is `rados listsnaps --format json` (rados.cc:3660-3750).
// The head entry has no overlaps; self-managed snaps have no names.
type snapCLIListing struct {
	Name   string `json:"name"`
	Seq    int64  `json:"seq"`
	Clones []struct {
		ID        snapCLIID `json:"id"`
		Snapshots []struct {
			ID   uint64 `json:"id"`
			Name string `json:"name"`
		} `json:"snapshots"`
		Size     uint64 `json:"size"`
		Overlaps []struct {
			Start  uint64 `json:"start"`
			Length uint64 `json:"length"`
		} `json:"overlaps"`
	} `json:"clones"`
}

func (r snapToolResult) listing(t *testing.T) snapCLIListing {
	t.Helper()
	var listing snapCLIListing
	if r.Exit != 0 || json.Unmarshal(r.JSON, &listing) != nil {
		t.Fatalf("native rados listsnaps failed: exit=%d code=%v %s", r.Exit, r.Code, r.Stderr)
	}
	return listing
}

// snapDencoderListing is ceph-dencoder's obj_list_snap_response_t dump.
// Pinned osd_types.h dumps seq INSIDE the open "clones" array, so the JSON
// formatter emits it as a trailing bare number in that array.
type snapDencoderListing struct {
	Seq    uint64
	Clones []snapDencoderClone
}

type snapDencoderClone struct {
	Head      bool
	ID        uint64
	Snapshots []uint64
	Overlaps  [][2]uint64
	Size      uint64
}

func (r snapToolResult) dencoderListing(t *testing.T) snapDencoderListing {
	t.Helper()
	var raw struct {
		Clones []json.RawMessage `json:"clones"`
	}
	if r.Exit != 0 || json.Unmarshal(r.JSON, &raw) != nil || len(raw.Clones) == 0 {
		t.Fatalf("ceph-dencoder obj_list_snap_response_t failed: exit=%d code=%v %s %s", r.Exit, r.Code, r.Stderr, r.JSON)
	}
	var listing snapDencoderListing
	if err := json.Unmarshal(raw.Clones[len(raw.Clones)-1], &listing.Seq); err != nil {
		t.Fatal("native dencoder dump lacks its trailing seq", err)
	}
	for _, element := range raw.Clones[:len(raw.Clones)-1] {
		var clone struct {
			ID        json.RawMessage `json:"cloneid"`
			Snapshots []struct {
				ID uint64 `json:"id"`
			} `json:"snapshots"`
			Overlaps []struct {
				Offset uint64 `json:"offset"`
				Length uint64 `json:"length"`
			} `json:"overlaps"`
			Size uint64 `json:"size"`
		}
		if err := json.Unmarshal(element, &clone); err != nil {
			t.Fatal("native dencoder clone entry", err)
		}
		decoded := snapDencoderClone{Size: clone.Size, Head: string(clone.ID) == `"HEAD"`}
		if !decoded.Head && json.Unmarshal(clone.ID, &decoded.ID) != nil {
			t.Fatalf("native dencoder clone id %s", clone.ID)
		}
		for _, snap := range clone.Snapshots {
			decoded.Snapshots = append(decoded.Snapshots, snap.ID)
		}
		for _, overlap := range clone.Overlaps {
			decoded.Overlaps = append(decoded.Overlaps, [2]uint64{overlap.Offset, overlap.Length})
		}
		listing.Clones = append(listing.Clones, decoded)
	}
	return listing
}

type snapOracleScope struct {
	Independent  bool `json:"independent_native_c_api"`
	HeaderCLI    bool `json:"header_via_pinned_rados_cli"`
	RawWire      bool `json:"raw_wire_payload_available"`
	PerOp        bool `json:"native_write_per_op_results_available"`
	CmpPrval     bool `json:"native_cmp_per_op_prval_available"`
	ListSnapsCLI bool `json:"listsnaps_via_pinned_rados_cli"`
	SnapHeader   bool `json:"snap_read_header_available"`
	RollbackTime bool `json:"rollback_mtime_explicit"`
}

type snapOracleCleanup struct {
	Objects []struct {
		Object string `json:"object"`
		Code   int32  `json:"code"`
	} `json:"objects"`
	Removed bool `json:"objects_removed"`
	Snaps   []struct {
		Snap uint64 `json:"snap"`
		Code int32  `json:"code"`
	} `json:"snaps"`
	SnapsRemoved bool `json:"snaps_removed"`
}

// snapOracleReceipt is osd-omap-write-result.json for the snapshot modes and
// cleanup. Fields shadow the narrower omapWriteReceipt ones of the same name.
type snapOracleReceipt struct {
	omapWriteReceipt
	Scope      snapOracleScope   `json:"scope"`
	ObjectInfo *snapObjectInfo   `json:"object_info"`
	Cleanup    snapOracleCleanup `json:"cleanup"`
	// snapcreate: one entry per MON allocation, stopping at a refusal.
	Snaps []struct {
		Code int32   `json:"code"`
		Snap *uint64 `json:"snap"`
	} `json:"snaps"`
	OwnedSnaps []uint64 `json:"owned_snaps"`
	// snapwrite/snaprollback: the set_write_ctx code; nonzero means the
	// native vector or rollback was not sent (Aggregate/Rollback.Code nil).
	SnapContext *struct {
		Seq   uint64   `json:"seq"`
		Snaps []uint64 `json:"snaps"`
		Code  int32    `json:"code"`
	} `json:"snapc"`
	OrderSnap    bool            `json:"ordersnap"`
	OperateFlags uint32          `json:"operate_flags"`
	LastVersion  *uint64         `json:"last_version"`
	Snap         *uint64         `json:"snap"`
	Read         *snapOracleRead `json:"read"`
	Rollback     *struct {
		Snap uint64 `json:"snap"`
		Code *int32 `json:"code"`
	} `json:"rollback"`
	ListSnaps *snapToolResult `json:"listsnaps"`
	RawLength *int            `json:"raw_length"`
	Decoded   *snapToolResult `json:"decoded"`
}

// nativeSnapOracle runs one snapshot-capable oracle mode on the shared input
// and result files of the OMAP write oracle and checks its provenance.
func nativeSnapOracle(t *testing.T, ctx context.Context, base osdFixtureOracle, mode string, input snapOracleInput) snapOracleReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil || len(data) > 1<<20 {
		t.Fatal("bounded snapshot oracle input", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-omap-write-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-omap-write-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-omap-write", mode); err != nil {
		t.Fatal("native snapshot oracle "+mode, err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-omap-write-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err = io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("snapshot oracle receipt exceeds bound", err)
	}
	var result snapOracleReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if result.Schema != 1 || result.Commit != source || result.Mode != mode || result.Fixture.ID != base.Pool || result.Fixture.Object != base.Name || !strings.Contains(result.DaemonVersion, source) || !strings.Contains(result.DaemonVersion, "20.2.4") {
		t.Fatal("native snapshot oracle receipt provenance differs")
	}
	s := result.Scope
	if !s.Independent || !s.HeaderCLI || s.RawWire || s.PerOp || !s.CmpPrval || !s.ListSnapsCLI || s.SnapHeader || s.RollbackTime {
		t.Fatal("native snapshot oracle fabricated wire, header or per-op evidence")
	}
	return result
}

// nativeSnapCreate allocates owned self-managed snap ids; any MON refusal is fatal.
func nativeSnapCreate(t *testing.T, ctx context.Context, base osdFixtureOracle, count int) []uint64 {
	t.Helper()
	result := nativeSnapOracle(t, ctx, base, "snapcreate", snapOracleInput{Count: count})
	var snaps []uint64
	for _, entry := range result.Snaps {
		if entry.Code != 0 || entry.Snap == nil {
			t.Fatal("native self-managed snap allocation refused", entry.Code)
		}
		snaps = append(snaps, *entry.Snap)
	}
	if len(snaps) != count {
		t.Fatal("native self-managed snap allocation count differs", result.Snaps)
	}
	return snaps
}

// nativeSnapWrite runs one native write vector under snapc; keys extend the
// before/after head OMAP inventory beyond the empty key.
func nativeSnapWrite(t *testing.T, ctx context.Context, base osdFixtureOracle, object string, keys [][]byte, snapc snapOracleContext, orderSnap bool, mtime omapWriteMTime, ops ...snapOracleAction) snapOracleReceipt {
	t.Helper()
	return nativeSnapOracle(t, ctx, base, "snapwrite", snapOracleInput{Object: object, SnapContext: &snapc, OrderSnap: orderSnap, MTime: &mtime, Operations: ops, InventoryKeys: keys, ObjectInfo: true})
}

func nativeSnapRead(t *testing.T, ctx context.Context, base osdFixtureOracle, object string, snap uint64, keys [][]byte) snapOracleRead {
	t.Helper()
	result := nativeSnapOracle(t, ctx, base, "snapread", snapOracleInput{Object: object, Snap: &snap, InventoryKeys: keys, ObjectInfo: true})
	if result.Read == nil || len(result.Read.Operations) != 5 || result.Snap == nil || *result.Snap != snap {
		t.Fatal("native snapshot read receipt is incomplete")
	}
	return *result.Read
}

func nativeSnapRollback(t *testing.T, ctx context.Context, base osdFixtureOracle, object string, keys [][]byte, snap uint64, snapc snapOracleContext) snapOracleReceipt {
	t.Helper()
	return nativeSnapOracle(t, ctx, base, "snaprollback", snapOracleInput{Object: object, Snap: &snap, SnapContext: &snapc, InventoryKeys: keys, ObjectInfo: true})
}

func nativeListSnaps(t *testing.T, ctx context.Context, base osdFixtureOracle, object string) snapToolResult {
	t.Helper()
	result := nativeSnapOracle(t, ctx, base, "listsnaps", snapOracleInput{Object: object})
	if result.ListSnaps == nil {
		t.Fatal("native listsnaps receipt lacks the CLI result")
	}
	return *result.ListSnaps
}

// nativeDecodeSnapList decodes raw LIST_SNAPS output bytes with ceph-dencoder.
func nativeDecodeSnapList(t *testing.T, ctx context.Context, base osdFixtureOracle, raw []byte) snapToolResult {
	t.Helper()
	result := nativeSnapOracle(t, ctx, base, "listsnaps", snapOracleInput{Raw: &raw})
	if result.Decoded == nil || result.RawLength == nil || *result.RawLength != len(raw) {
		t.Fatal("native dencoder receipt lacks the decoded result")
	}
	return *result.Decoded
}

// Test-only encoding used solely to exercise the native dencoder path.
func snapListResponse(seq uint64, clones []snapDencoderClone) []byte {
	le := binary.LittleEndian
	var body []byte
	body = le.AppendUint32(body, uint32(len(clones)))
	for _, clone := range clones {
		id := clone.ID
		if clone.Head {
			id = snapOracleHead
		}
		var c []byte
		c = le.AppendUint64(c, id)
		c = le.AppendUint32(c, uint32(len(clone.Snapshots)))
		for _, snap := range clone.Snapshots {
			c = le.AppendUint64(c, snap)
		}
		c = le.AppendUint32(c, uint32(len(clone.Overlaps)))
		for _, overlap := range clone.Overlaps {
			c = le.AppendUint64(le.AppendUint64(c, overlap[0]), overlap[1])
		}
		c = le.AppendUint64(c, clone.Size)
		body = append(le.AppendUint32(append(body, 1, 1), uint32(len(c))), c...)
	}
	body = le.AppendUint64(body, seq)
	return append(le.AppendUint32([]byte{2, 1}, uint32(len(body))), body...)
}

// cleanupSnapOracle removes owned heads and then owned snap ids. Removed snaps
// make the PG trim clones (active+clean+snaptrim) and then head whiteouts:
// observe completion natively, then leave a replicated fixture exactly
// active+clean for later routing tests.
func cleanupSnapOracle(t *testing.T, base osdFixtureOracle) {
	t.Helper()
	cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
	defer stop()
	result := nativeSnapOracle(t, cleanup, base, "cleanup", snapOracleInput{})
	if !result.Cleanup.Removed || !result.Cleanup.SnapsRemoved {
		t.Error("owned native snapshot cleanup failed", result.Cleanup)
	}
	t.Logf("cleanup objects=%+v snaps=%+v", result.Cleanup.Objects, result.Cleanup.Snaps)
	started := time.Now()
	for _, object := range result.Cleanup.Objects {
		for {
			listing := nativeListSnaps(t, cleanup, base, object.Object)
			if listing.Exit != 0 && listing.Code != nil && *listing.Code == -2 {
				break
			}
			if listing.Exit != 0 || time.Since(started) > 45*time.Second {
				t.Fatalf("native snap trim of %s did not complete: exit=%d %s %s", object.Object, listing.Exit, listing.Stderr, listing.JSON)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	t.Logf("native snap trim removed every owned clone and whiteout after %s", time.Since(started).Round(time.Millisecond))
	started = time.Now()
	waitOSDFixtureClean(t, cleanup, base)
	t.Logf("fixture clean wait after snap removal: %s", time.Since(started).Round(time.Millisecond))
}
