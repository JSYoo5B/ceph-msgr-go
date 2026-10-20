package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

const enforceSnapshotName = "enforce-snap-S"

var enforceSnapshotA = []byte("A\x00\xffsnapshot-original")
var enforceSnapshotB = []byte("B\x00\x80snapshot-head")
var enforceSnapshotC = []byte("C\x00\xffenforced-snapshot")

type enforceSnapNative struct {
	Schema   int    `json:"schema_version"`
	Source   string `json:"source_commit"`
	Action   string `json:"action"`
	Identity struct {
		Source string `json:"source_commit"`
		FSID   string `json:"fsid"`
		Pool   int64  `json:"pool_id"`
		Object string `json:"object"`
		Snap   uint64 `json:"snap_id"`
	} `json:"identity"`
	NativeIdentity struct {
		Name     string `json:"name"`
		GlobalID uint64 `json:"global_id"`
	} `json:"native_identity"`
	Epoch             uint32              `json:"pool_epoch"`
	NativeEnforceFlag bool                `json:"native_enforce_flag_available"`
	Head              nativeXattrSnapshot `json:"head"`
	Snapshot          nativeXattrSnapshot `json:"snapshot"`
	List              snapCLIListing      `json:"list_snaps"`
	ListRaw           []byte              `json:"list_snaps_raw"`
	OrdinaryWrite     *struct {
		Code           int32           `json:"code"`
		Version        uint64          `json:"version"`
		NativeIdentity json.RawMessage `json:"native_identity"`
	} `json:"ordinary_write"`
}

// TestCephOSDEnforceSnapContextIntegration observes the server's distinction
// between pool-provided and caller-enforced SnapContext. Native librados does
// not expose ENFORCE_SNAPC: it supplies ordinary WRITEFULL+ORDERSNAP and
// independent selected-snapshot STAT2/READ/LISTSNAPS observations only.
func TestCephOSDEnforceSnapContextIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	client, _, base, _ := fixture(t)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stop()
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	const controlHead, enforcedHead = "enforce-snap-control", "enforce-snap-enforced"
	heads := []string{controlHead, enforcedHead}
	facts := []map[string]any{}
	record := func(action string, value any) {
		facts = append(facts, map[string]any{"action": action, "value": value})
	}
	t.Cleanup(func() {
		encoded, err := json.MarshalIndent(map[string]any{"source_commit": mdsFreshSource, "native_enforce_flag_available": false, "native_comparison": "ordinary control write and independent selected-snapshot observations; -85 is the Go server result", "facts": facts}, "", "  ")
		if err == nil && len(encoded) > 1<<20 {
			err = errors.New("ENFORCE_SNAPC evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-enforce-snap-context-results.json"), append(encoded, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	})
	for _, name := range heads {
		record("native seed "+name, poolSeed(t, ctx, 1, name, "", enforceSnapshotA, nil))
	}
	made, err := client.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.PoolSnapshotCreate, Name: enforceSnapshotName})
	record("public MON pool snapshot create", made)
	if err != nil || made.Code != 0 {
		t.Fatal("create owned pool snapshot", err)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		removed, err := client.PoolOp(clean, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.PoolSnapshotRemove, Name: enforceSnapshotName})
		record("public MON pool snapshot remove", removed)
		if err != nil || removed.Code != 0 {
			t.Error("remove owned pool snapshot", err)
		}
	})
	info := nativePool(t, ctx, "poolinfo", map[string]any{"slot": 1})
	record("native pool snapshot identity", info)
	if info.SnapMode != "pool" || len(info.PoolSnaps) != 1 || info.PoolSnaps[0].Name != enforceSnapshotName || info.PoolSnaps[0].ID == 0 || info.SnapSeq != info.PoolSnaps[0].ID {
		t.Fatal("owned pool snapshot identity differs", info)
	}
	snap := info.PoolSnaps[0].ID
	native := func(t *testing.T, action, name string) enforceSnapNative {
		t.Helper()
		input, _ := json.Marshal(map[string]any{"object": name, "pool_id": pools.ids[1], "snap_id": snap})
		if err := testcluster.WritePublicEvidence(filepath.Join(directory, "osd-enforce-snap-context-input.json"), input); err != nil {
			t.Fatal(err)
		}
		if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-enforce-snap-context", action); err != nil {
			t.Fatal("native snapshot observation", err)
		}
		raw, err := os.ReadFile(filepath.Join(directory, "osd-enforce-snap-context-result.json"))
		var value enforceSnapNative
		if err != nil || len(raw) > 512<<10 || json.Unmarshal(raw, &value) != nil {
			t.Fatal("bounded native snapshot receipt", err)
		}
		if value.Schema != 1 || value.Source != mdsFreshSource || value.Identity.Source != mdsFreshSource || value.Identity.Pool != pools.ids[1] || value.Identity.Object != name || value.Identity.Snap != snap || value.Identity.FSID != client.Snapshot().FSID || value.Action != action || value.NativeEnforceFlag || value.NativeIdentity.Name != "client.test" || value.NativeIdentity.GlobalID == 0 || value.Epoch == 0 {
			t.Fatal("native receipt identity differs", value)
		}
		record("native "+action+" "+name, map[string]any{"receipt": value, "sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "bytes": len(raw)})
		return value
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	send := func(t *testing.T, c *cephmsgr.Client, name string, data []byte, enforce bool, snapc cephmsgr.OSDSnapContext, tick uint32) (cephmsgr.OSDReply, error) {
		t.Helper()
		route := poolPlace(t, ctx, 1, name, "")
		var o *cephmsgr.OSDConnection
		if c == client {
			o = connections[route.ID]
			if o == nil {
				o = mutationConnection(t, c, ctx, route)
				connections[route.ID] = o
			}
		} else {
			o = mutationConnection(t, c, ctx, route)
			defer o.Close()
		}
		request := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		request.EnforceSnapContext = enforce
		request.OrderSnapshots = true
		request.SnapContext = snapc
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000900 + tick, Nanoseconds: tick * 11}
		reply, err := o.Request(ctx, request)
		text := ""
		if err != nil {
			text = err.Error()
		}
		record("Go WRITEFULL "+name, map[string]any{"request": request, "reply": reply, "error": text, "route": route})
		var unknown *cephmsgr.OutcomeUnknownError
		expectedSeed := route.Hash
		if request.Placement != nil {
			expectedSeed = request.Placement.PG.Seed
		}
		if errors.As(err, &unknown) || reply.Submission.Transaction == 0 || reply.Submission.GlobalID != c.Snapshot().GlobalID || reply.Object != name || reply.PG.Pool != uint64(route.Pool) || reply.PG.Seed != expectedSeed || reply.MapEpoch < request.MapEpoch || len(reply.RawFront) == 0 {
			t.Fatal("Go request lost identity or has unknown outcome", err)
		}
		return reply, err
	}
	requireSuccess := func(t *testing.T, r cephmsgr.OSDReply, err error) {
		t.Helper()
		if err != nil || r.Code != 0 || len(r.Operations) != 1 || r.Operations[0].Result != 0 || r.Flags&uint64(cephmsgr.OSDReplyOnDisk) == 0 {
			t.Fatal("expected known ONDISK write", r.Code, err)
		}
	}
	requireRefusal := func(t *testing.T, r cephmsgr.OSDReply, err error, code int32) {
		t.Helper()
		var remote *cephmsgr.OSDError
		if !errors.As(err, &remote) || remote.Code != code || r.Code != code || len(r.Operations) != 1 || r.Operations[0].Result != code && r.Operations[0].Result != 0 {
			t.Fatal("raw signed server refusal differs", code, r.Code, err)
		}
	}
	checkClone := func(t *testing.T, n enforceSnapNative) {
		t.Helper()
		if n.Head.Code != 0 || n.Snapshot.Code != 0 || !bytes.Equal(n.Snapshot.Data, enforceSnapshotA) || n.Snapshot.Version == 0 || n.List.Name != n.Identity.Object || n.List.Seq != int64(snap) {
			t.Fatal("native selected snapshot differs", n)
		}
		found := false
		for _, clone := range n.List.Clones {
			if !clone.ID.Head {
				for _, id := range clone.Snapshots {
					if id.ID == snap {
						found = true
						if clone.Size != uint64(len(enforceSnapshotA)) {
							t.Fatal("native clone size differs")
						}
					}
				}
			}
		}
		if !found {
			t.Fatal("owned snapshot missing from native clone listing")
		}
	}
	baseline := map[string]enforceSnapNative{}
	if !t.Run("ordinary_empty_context_uses_pool_snapshot", func(t *testing.T) {
		for _, name := range heads {
			r, err := send(t, client, name, enforceSnapshotB, false, cephmsgr.OSDSnapContext{}, 1)
			requireSuccess(t, r, err)
			n := native(t, "observe", name)
			checkClone(t, n)
			if !bytes.Equal(n.Head.Data, enforceSnapshotB) || r.UserVersion != n.Head.Version || n.Head.Seconds != 1700000901 || n.Head.Nanos != 11 {
				t.Fatal("ordinary write/readback differs", name)
			}
			baseline[name] = n
		}
	}) {
		return
	}
	if !t.Run("native_and_Go_ordinary_controls_commit_but_enforced_stale_context_refuses", func(t *testing.T) {
		n := native(t, "ordinary-write", controlHead)
		checkClone(t, n)
		if n.OrdinaryWrite == nil || n.OrdinaryWrite.Code != 0 || n.OrdinaryWrite.Version != n.Head.Version || !bytes.Equal(n.Head.Data, enforceSnapshotC) || n.Head.Version <= baseline[controlHead].Head.Version || n.Head.Seconds != 1700000902 || n.Head.Nanos != 22 || !reflect.DeepEqual(n.Snapshot, baseline[controlHead].Snapshot) {
			t.Fatal("native ordinary control differs")
		}
		before := n
		r, err := send(t, client, controlHead, enforceSnapshotC, false, cephmsgr.OSDSnapContext{}, 3)
		requireSuccess(t, r, err)
		n = native(t, "observe", controlHead)
		checkClone(t, n)
		if r.UserVersion != n.Head.Version || n.Head.Version <= before.Head.Version || !bytes.Equal(n.Head.Data, enforceSnapshotC) || n.Head.Seconds != 1700000903 || n.Head.Nanos != 33 || !reflect.DeepEqual(n.Snapshot, baseline[controlHead].Snapshot) {
			t.Fatal("Go ordinary control differs")
		}
		r, err = send(t, client, enforcedHead, enforceSnapshotC, true, cephmsgr.OSDSnapContext{}, 4)
		requireRefusal(t, r, err, -85)
		n = native(t, "observe", enforcedHead)
		if !reflect.DeepEqual(n.Head, baseline[enforcedHead].Head) || !reflect.DeepEqual(n.Snapshot, baseline[enforcedHead].Snapshot) || !reflect.DeepEqual(n.List, baseline[enforcedHead].List) {
			t.Fatal("EOLDSNAPC changed head, selected clone or snapset")
		}
	}) {
		return
	}
	if !t.Run("explicit_current_context_commits_and_readonly_refusal_preserves_state", func(t *testing.T) {
		r, err := send(t, client, enforcedHead, enforceSnapshotC, true, cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}, 5)
		requireSuccess(t, r, err)
		n := native(t, "observe", enforcedHead)
		checkClone(t, n)
		if !bytes.Equal(n.Head.Data, enforceSnapshotC) || r.UserVersion != n.Head.Version || n.Head.Version <= baseline[enforcedHead].Head.Version || n.Head.Seconds != 1700000905 || n.Head.Nanos != 55 || !reflect.DeepEqual(n.Snapshot, baseline[enforcedHead].Snapshot) {
			t.Fatal("current caller context did not commit/preserve snapshot")
		}
		options := integrationOptions(t)
		options.EnableOSD = true
		options.Identity = "client.readonly"
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		key, err := os.ReadFile(filepath.Join(directory, "readonly.key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(string(bytes.TrimSpace(key)))
		if err != nil {
			t.Fatal(err)
		}
		ro, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer ro.Close()
		r, err = send(t, ro, enforcedHead, []byte("forbidden"), true, cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}, 6)
		requireRefusal(t, r, err, -1)
		after := native(t, "observe", enforcedHead)
		if !reflect.DeepEqual(after.Head, n.Head) || !reflect.DeepEqual(after.Snapshot, n.Snapshot) || !reflect.DeepEqual(after.List, n.List) {
			t.Fatal("readonly refusal changed owned head or clone")
		}
	}) {
		return
	}
}
