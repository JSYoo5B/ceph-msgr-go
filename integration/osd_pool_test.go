package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// poolReceipt is the closed result of the owned-pool native oracle.
type poolReceipt struct {
	Schema        int                 `json:"schema_version"`
	Commit        string              `json:"source_commit"`
	Mode          string              `json:"mode"`
	DaemonVersion string              `json:"fixture_daemon_version"`
	Pool          string              `json:"pool"`
	PoolID        int64               `json:"pool_id"`
	Flags         int64               `json:"flags"`
	Size          int                 `json:"size"`
	MinSize       int                 `json:"min_size"`
	PGNum         int                 `json:"pg_num"`
	State         nativeXattrSnapshot `json:"state"`
	Placement     osdFixtureOracle    `json:"placement"`
	SnapID        uint64              `json:"snap_id"`
	PoolsRemoved  bool                `json:"pools_removed"`
	FlagNames     string              `json:"flag_names"`
	ServiceDump   json.RawMessage     `json:"service_dump"`
	Epochs        map[string]uint64   `json:"epochs"`
	PoolType      int                 `json:"pool_type"`
	CrushRule     json.RawMessage     `json:"crush_rule"`
	ECOptimized   bool                `json:"ec_optimized"`
	// fullratios mode.
	NearfullRatio     float64           `json:"nearfull_ratio"`
	BackfillfullRatio float64           `json:"backfillfull_ratio"`
	FullRatio         float64           `json:"full_ratio"`
	Epoch             uint32            `json:"epoch"`
	FullOSDs          []int32           `json:"full_osds"`
	OwnedFlags        map[string]string `json:"owned_flags"`
	OSDMapFlags       []string          `json:"osdmap_flags"`
	// NonprimaryShards is the pool's raw nonprimary_shards dump.
	NonprimaryShards json.RawMessage `json:"nonprimary_shards"`
	KB               uint64          `json:"kb"`
	KBUsed           uint64          `json:"kb_used"`
	KBAvail          uint64          `json:"kb_avail"`
	NumObjects       uint64          `json:"num_objects"`
	SnapSeq          uint64          `json:"snap_seq"`
	SnapMode         string          `json:"snap_mode"`
	PoolSnaps        []struct {
		ID   uint64 `json:"snapid"`
		Name string `json:"name"`
	} `json:"pool_snaps"`
	RemovedSnaps      string            `json:"removed_snaps"`
	RemovedSnapsQueue []json.RawMessage `json:"removed_snaps_queue"`
}

func nativePool(t *testing.T, ctx context.Context, mode string, input any) poolReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	raw, err := json.Marshal(map[string]any{})
	if input != nil {
		raw, err = json.Marshal(input)
	}
	if err != nil || len(raw) > 1<<20 {
		t.Fatal("bounded pool oracle input", err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-pool-input.json"), raw); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-pool-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-pool", mode); err != nil {
		t.Fatal("native owned-pool oracle "+mode, err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-pool-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("pool oracle receipt exceeds bound", err)
	}
	var receipt poolReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if receipt.Schema != 1 || receipt.Commit != source || receipt.Mode != mode || !strings.Contains(receipt.DaemonVersion, source) {
		t.Fatal("pool oracle receipt provenance differs", receipt)
	}
	return receipt
}

type ownedPools struct {
	ids     map[int]int64
	request func(osdFixtureOracle, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
}

// createOwnedPools builds disposable pools in the fixture's own shape and
// removes them on cleanup. Its pools never alias the fixture data pool.
func createOwnedPools(t *testing.T, ctx context.Context, base osdFixtureOracle, slots ...int) ownedPools {
	t.Helper()
	pg, hash := 1, false
	request := osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		pg, hash, request = 8, true, replicatedPGRequest
	}
	pools := ownedPools{ids: map[int]int64{}, request: request}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned pools were not removed")
		}
	})
	for _, slot := range slots {
		r := nativePool(t, ctx, "create", map[string]any{"slot": slot, "pg_num": pg, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
		if r.Pool == "" || r.PoolID == base.Pool || r.PoolID <= 0 {
			t.Fatal("owned pool identity differs", r)
		}
		pools.ids[slot] = r.PoolID
	}
	return pools
}

func b64(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func poolSeed(t *testing.T, ctx context.Context, slot int, object, namespace string, data []byte, attrs []nativeXattrEntry) nativeXattrSnapshot {
	t.Helper()
	seed := []map[string]string{}
	for _, a := range attrs {
		seed = append(seed, map[string]string{"name": b64(a.Name), "value": b64(a.Value)})
	}
	r := nativePool(t, ctx, "seed", map[string]any{"slot": slot, "object": object, "namespace": namespace, "data": b64(data), "attributes": seed})
	if r.State.Code != 0 || !bytes.Equal(r.State.Data, data) {
		t.Fatal("native pool seed differs", r.State.Code)
	}
	return r.State
}

func poolObserve(t *testing.T, ctx context.Context, slot int, object, namespace, snap string) nativeXattrSnapshot {
	t.Helper()
	return poolObserveSnap(t, ctx, slot, object, namespace, snap, 0)
}

// poolObserveSnap reads at a pool snapshot name or a self-managed snapshot id.
func poolObserveSnap(t *testing.T, ctx context.Context, slot int, object, namespace, snap string, snapID uint64) nativeXattrSnapshot {
	t.Helper()
	in := map[string]any{"slot": slot, "object": object, "namespace": namespace}
	if snap != "" {
		in["snap"] = snap
	}
	if snapID != 0 {
		in["snap_id"] = snapID
	}
	return nativePool(t, ctx, "observe", in).State
}

func poolPlace(t *testing.T, ctx context.Context, slot int, object, namespace string) osdFixtureOracle {
	t.Helper()
	return poolPlaceOn(t, ctx, slot, object, namespace, nil)
}

// poolPlaceOn routes to an explicit acting-set member (a replica read target).
func poolPlaceOn(t *testing.T, ctx context.Context, slot int, object, namespace string, osd *int32) osdFixtureOracle {
	t.Helper()
	in := map[string]any{"slot": slot, "object": object, "namespace": namespace}
	if osd != nil {
		in["osd"] = *osd
	}
	r := nativePool(t, ctx, "place", in).Placement
	if r.Name != object || r.Namespace != namespace || r.ID < 0 || r.Address == "" {
		t.Fatal("native owned-pool placement differs", r)
	}
	return r
}

func attrNames(a nativeXattrSnapshot) map[string]string { return xattrInventory(a) }

func TestCephOSDCrossPoolAndNamespaceCopyFromIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1, 2)
	data := []byte{0, 0xff, 'x', 'p', 'o', 'o', 'l', 10, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	attrs := []nativeXattrEntry{{Name: []byte("pool-attr"), Value: []byte{0xff, 0, 'P'}}}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	copyTo := func(slot int, object, namespace string, in cephmsgr.OSDCopyFromInput) (osdFixtureOracle, cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, slot, object, namespace)
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		r := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: in})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000600}
		reply, err := connections[route.ID].Request(ctx, r)
		return route, reply, err
	}
	for _, tc := range []struct {
		name                string
		srcSlot, dstSlot    int
		srcNS, dstNS        string
		copySrcNS           string // namespace named in the copy; differs from srcNS for the refusal
		want                int32  // aggregate result
		source, destination string
		expectCopy          bool
	}{
		{name: "pool 1 to pool 2", srcSlot: 1, dstSlot: 2, srcNS: "", dstNS: "", copySrcNS: "", source: "pool-src-a", destination: "pool-dst-a", expectCopy: true},
		{name: "pool 2 to pool 1 across namespaces", srcSlot: 2, dstSlot: 1, srcNS: "ns-src", dstNS: "ns-dst", copySrcNS: "ns-src", source: "pool-src-b", destination: "pool-dst-b", expectCopy: true},
		{name: "same pool across namespaces", srcSlot: 1, dstSlot: 1, srcNS: "ns-one", dstNS: "ns-two", copySrcNS: "ns-one", source: "pool-src-c", destination: "pool-dst-c", expectCopy: true},
		{name: "wrong source namespace", srcSlot: 1, dstSlot: 2, srcNS: "ns-real", dstNS: "", copySrcNS: "ns-other", source: "pool-src-d", destination: "pool-dst-d", want: -2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seeded := poolSeed(t, ctx, tc.srcSlot, tc.source, tc.srcNS, data, attrs)
			in := cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[tc.srcSlot], SourceName: tc.source, SourceNamespace: tc.copySrcNS}
			route, reply, err := copyTo(tc.dstSlot, tc.destination, tc.dstNS, in)
			after := poolObserve(t, ctx, tc.dstSlot, tc.destination, tc.dstNS, "")
			source := poolObserve(t, ctx, tc.srcSlot, tc.source, tc.srcNS, "")
			if source.Version != seeded.Version || !bytes.Equal(source.Data, data) {
				t.Fatal("copy altered the source")
			}
			if tc.expectCopy {
				if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Code != 0 {
					t.Fatal("cross-pool copy", err)
				}
				if after.Code != 0 || !bytes.Equal(after.Data, data) || !reflect.DeepEqual(attrNames(after), attrNames(seeded)) || reply.UserVersion != after.Version {
					t.Fatalf("destination differs from source: code=%d data=%x", after.Code, after.Data)
				}
				if reply.PG.Pool != uint64(route.Pool) || route.Pool != pools.ids[tc.dstSlot] {
					t.Fatal("reply or route pool differs", reply.PG, route.Pool)
				}
				return
			}
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != tc.want || after.Code != -2 {
				t.Fatal("expected a known refusal that leaves no destination", err, after.Code)
			}
		})
	}
}

// Pool snapshots are the other snapshot mode: the daemon takes the SnapContext
// from the pool, so the client sends none for writes and names the pool snap
// id only when reading or rolling back.
func TestCephOSDPoolSnapshotIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "pool-snap-object"
	original := []byte{0, 0xff, 'o', 'r', 'i', 'g', 10, 1, 2, 3, 4, 5, 6, 7, 8}
	attrs := []nativeXattrEntry{{Name: []byte("snap-attr"), Value: []byte{0xff, 0, 'S'}}}
	seeded := poolSeed(t, ctx, 1, object, "", original, attrs)
	snap := nativePool(t, ctx, "snapshot", map[string]any{"slot": 1, "name": "owned-pool-snap", "action": "create"})
	if snap.SnapID == 0 {
		t.Fatal("native pool snapshot id missing")
	}
	route := poolPlace(t, ctx, 1, object, "")
	connection := mutationConnection(t, c, ctx, route)
	defer connection.Close()
	send := func(configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := pools.request(route, ops...)
		if ops[0].Code != cephmsgr.OSDRead && ops[0].Code != cephmsgr.OSDListSnapshots {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000700}
		}
		if configure != nil {
			configure(&r)
		}
		return connection.Request(ctx, r)
	}
	newHead := []byte("new head after the pool snapshot\x00\xff")
	if _, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(newHead)), Data: newHead}); err != nil {
		t.Fatal("head overwrite in a pool-snapshot pool", err)
	}
	head := poolObserve(t, ctx, 1, object, "", "")
	clone := poolObserve(t, ctx, 1, object, "", "owned-pool-snap")
	if !bytes.Equal(head.Data, newHead) || !bytes.Equal(clone.Data, original) || !reflect.DeepEqual(attrNames(clone), attrNames(seeded)) {
		t.Fatalf("native head/clone differ: head=%x clone=%x", head.Data, clone.Data)
	}
	t.Run("read at the pool snapshot", func(t *testing.T) {
		reply, err := send(func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap.SnapID }, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(original) + 16)})
		if err != nil {
			t.Fatal("snapshot read", err)
		}
		if !bytes.Equal(reply.Operations[0].Data, original) {
			t.Fatalf("snapshot read returned %x", reply.Operations[0].Data)
		}
	})
	t.Run("list snapshots", func(t *testing.T) {
		reply, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
		if err != nil {
			t.Fatal(err)
		}
		list := reply.Operations[0].Snapshots
		found := false
		for _, clone := range list.Clones {
			for _, id := range clone.Snapshots {
				found = found || id == snap.SnapID
			}
		}
		if !found {
			t.Fatalf("pool snapshot id %d missing from the clone list: %+v", snap.SnapID, list)
		}
	})
	t.Run("client SnapContext in a pool-snapshot pool", func(t *testing.T) {
		before := poolObserve(t, ctx, 1, object, "", "")
		reply, err := send(func(r *cephmsgr.OSDRequest) {
			r.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap.SnapID + 1, Snapshots: []uint64{snap.SnapID + 1}}
		}, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 4, Data: []byte("ctx!")})
		after := poolObserve(t, ctx, 1, object, "", "")
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) {
			t.Fatal("unexpected unknown outcome", err)
		}
		t.Logf("explicit SnapContext in a pool-snapshot pool: result=%d err=%v head before=%q after=%q", reply.Code, err, before.Data, after.Data)
		// The daemon takes the context from the pool, so the write commits as an
		// ordinary head overwrite; it neither clones for the client's sequence
		// nor refuses the request. Recorded as observed server behavior.
		if err != nil || !bytes.Equal(after.Data, []byte("ctx!")) {
			t.Fatal("pool-snapshot pool did not accept the head write", err)
		}
	})
	t.Run("rollback to the pool snapshot", func(t *testing.T) {
		reply, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDRollback, Snapshot: snap.SnapID})
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("rollback", err)
		}
		after := poolObserve(t, ctx, 1, object, "", "")
		if after.Code != 0 || !bytes.Equal(after.Data, original) || !reflect.DeepEqual(attrNames(after), attrNames(seeded)) {
			t.Fatalf("head after rollback is not the original: %x", after.Data)
		}
	})
}
