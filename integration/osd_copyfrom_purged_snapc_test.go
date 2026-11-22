package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// copyPurgedTrimState is one native snaptrim observation of the owned pool
// (tools/osd_pool_mode_snaptrim_oracle.py).
type copyPurgedTrimState struct {
	PGID             string                         `json:"pgid"`
	State            string                         `json:"state"`
	Epoch            int64                          `json:"epoch"`
	OSDMapEpoch      int64                          `json:"osdmap_epoch"`
	PoolID           int64                          `json:"pool_id"`
	SnapSeq          uint64                         `json:"snap_seq"`
	SnapTrimQueue    []uint64                       `json:"snap_trimq"`
	PurgedSnaps      []uint64                       `json:"purged_snaps"`
	RemovedSnapQueue []uint64                       `json:"removed_snaps_queue"`
	ListSnaps        map[string]copyPurgedListSnaps `json:"listsnaps"`
}

type copyPurgedListSnaps struct {
	Exit   int             `json:"exit"`
	Stderr string          `json:"stderr"`
	JSON   json.RawMessage `json:"json"`
}

func (l copyPurgedListSnaps) listing(t *testing.T, name string) snapCLIListing {
	t.Helper()
	var listing snapCLIListing
	if l.Exit != 0 || json.Unmarshal(l.JSON, &listing) != nil {
		t.Fatalf("native listsnaps of %s failed: exit=%d %s", name, l.Exit, l.Stderr)
	}
	return listing
}

// copyPurgedPhase says where the removed snapshot is in one observation:
// "queued" (in the OSDMap removed_snaps_queue only: the PG has not applied
// the removal map), "pending" (also in the PG's snap_trimq), "purged"
// (trimmed into the PG's purged_snaps while the OSDMap still queues it),
// "unqueued" (still in purged_snaps after the MON stopped queueing it: the PG
// has not applied that map), "pruned" (in none of them), or "" for any other
// combination.
func copyPurgedPhase(s copyPurgedTrimState, snap uint64) string {
	trim, purged, queued := slices.Contains(s.SnapTrimQueue, snap), slices.Contains(s.PurgedSnaps, snap), slices.Contains(s.RemovedSnapQueue, snap)
	switch {
	case !trim && !purged && queued:
		return "queued"
	case trim && !purged && queued:
		return "pending"
	case !trim && purged && queued:
		return "purged"
	case !trim && purged && !queued:
		return "unqueued"
	case !trim && !purged && !queued:
		return "pruned"
	}
	return ""
}

func TestCopyPurgedPhase(t *testing.T) {
	for _, tc := range []struct {
		trim, purged, queue []uint64
		want                string
	}{
		{nil, nil, []uint64{5}, "queued"},
		{[]uint64{5}, nil, []uint64{5}, "pending"},
		{nil, []uint64{4, 5}, []uint64{5}, "purged"},
		{nil, []uint64{5}, []uint64{4}, "unqueued"},
		{nil, []uint64{4}, nil, "pruned"},
		{[]uint64{5}, []uint64{5}, []uint64{5}, ""},
		{[]uint64{5}, nil, nil, ""},
		{[]uint64{5}, []uint64{5}, nil, ""},
	} {
		s := copyPurgedTrimState{SnapTrimQueue: tc.trim, PurgedSnaps: tc.purged, RemovedSnapQueue: tc.queue}
		if got := copyPurgedPhase(s, 5); got != tc.want {
			t.Fatalf("%+v: phase %q, want %q", tc, got, tc.want)
		}
	}
}

// TestCephOSDCopyFromPurgedSnapContextIntegration crosses both copy opcodes
// with destination SnapContexts that name a removed snapshot after its trim.
// A removed self-managed snapshot moves from the PG's snap_trimq to its
// info.purged_snaps when the trimmer is done (PrimaryLogPG.cc SnapTrimmer);
// the MON drops it from the OSDMap removed_snaps_queue only once the MGR
// digest reports it purged (src/mon/OSDMonitor.cc try_prune_purged_snaps),
// and the PG then drops it from purged_snaps too (src/osd/PG.cc
// on_active_advmap). filter_snapc removes a snapshot that is in the
// snap_trimq or purged_snaps (src/osd/PG.cc), so the two later states differ:
//
//   - purged: both MGRs are paused under fixture-owned leases (pauseDaemon)
//     before the removal, so no digest can report the trim. The state is
//     reached when the native `pg query` shows the snapshot in purged_snaps
//     and not in the snap_trimq while `osd dump` still queues it, and the same
//     observation after the cells confirms it held. A context naming only the
//     removed snapshot clones nothing but raises the snapset seq; one naming
//     it over a live snapshot clones the pre-copy head under the removed id
//     holding only the live one.
//   - pruned: after the MGRs resume, the native observation shows the
//     snapshot in neither set and no longer queued. filter_snapc no longer
//     knows it: a context naming it clones the pre-copy head under it, and the
//     clone lists the removed snapshot and is readable at its id at the
//     post-prune observation. Its later lifetime is not checked; the owned
//     pool is deleted afterwards.
//
// Every cell copies the same native head into a fresh destination seeded by
// Go, checked by independent native reads and rados listsnaps. The waits end
// on these native observations, never on elapsed time alone. Sources:
// ceph/ceph/blob/v20.2.4/src/osd/PG.cc (filter_snapc, on_active_advmap),
// src/osd/PrimaryLogPG.cc (execute_ctx, make_writeable, SnapTrimmer),
// src/mon/OSDMonitor.cc. Not verified: more than one PG, a removal while a
// PG is inactive (PeeringState::activate), the OSD's purged-snaps scrub that
// can requeue such a clone, and profiles other than this row's and the
// legacy-v6 osd.aes.ip4.crc suite that also selects it.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfrompurgedsnapcontext key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=replicated-pg-reply-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses both MGRs and allocates and removes self-managed snapshots in an owned pool it deletes
func TestCephOSDCopyFromPurgedSnapContextIntegration(t *testing.T) {
	// The owned pool takes the selected profile's HASHPSPOOL policy, which
	// the fixture sets as the global pool default before the daemons start
	// (osd_pool_default_flag_hashpspool): the legacy-v6 client sends the raw
	// object hash, and the owned-pool oracle checks the native flag against
	// the request.
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	type cell struct {
		Name        string                  `json:"name"`
		Code        cephmsgr.OSDOpcode      `json:"opcode"`
		SnapContext cephmsgr.OSDSnapContext `json:"snap_context"`
		Seed        cephmsgr.OSDReply       `json:"seed_reply"`
		Reply       cephmsgr.OSDReply       `json:"reply"`
		Error       string                  `json:"error,omitempty"`
		After       nativeXattrSnapshot     `json:"after"`
		Clone       *nativeXattrSnapshot    `json:"destination_clone,omitempty"`
	}
	receipt := struct {
		Pool         int64                          `json:"pool_id"`
		Snaps        map[string]uint64              `json:"snaps"`
		Source       nativeXattrSnapshot            `json:"source"`
		Managers     []string                       `json:"paused_managers"`
		Removal      cephmsgr.PoolOpResult          `json:"snap_removal"`
		States       map[string]copyPurgedTrimState `json:"trim_states"`
		Observations map[string]int                 `json:"observations"`
		Cells        []cell                         `json:"cells"`
	}{Snaps: map[string]uint64{}, States: map[string]copyPurgedTrimState{}, Observations: map[string]int{}}
	t.Cleanup(func() {
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "osd-copyfrom-purged-snapc-results.json"), raw)
		}
		if err != nil {
			t.Error("purged SnapContext copy evidence", err)
		}
	})

	// One owned PG under either hash policy, so the observed PG is the PG of
	// every owned object.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned pool was not removed")
		}
	})
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": 1, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	if created.PoolID <= 0 || created.PoolID == base.Pool || created.PGNum != 1 {
		t.Fatal("owned pool identity differs", created)
	}
	receipt.Pool = created.PoolID
	const src = "copy-purged-source"
	source := poolSeed(t, ctx, 1, src, "", []byte("purged-source\x00\xff"), []nativeXattrEntry{{Name: []byte("marker"), Value: []byte("source")}})
	receipt.Source = source
	codes := []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2}
	destination := func(code cephmsgr.OSDOpcode, name string) string { return fmt.Sprintf("copy-purged-%x-%s", code, name) }
	var watched []string
	for _, code := range codes {
		for _, name := range []string{"purged-only", "purged-newest", "pruned-only", "pruned-newest"} {
			watched = append(watched, destination(code, name))
		}
	}
	observe := func(t *testing.T, label string, objects []string) copyPurgedTrimState {
		t.Helper()
		var result struct {
			SnapTrim copyPurgedTrimState `json:"snap_trim"`
		}
		nativePoolResult(t, ctx, "snaptrim", map[string]any{"slot": 1, "object": src, "objects": objects}, &result)
		s := result.SnapTrim
		if s.PoolID != receipt.Pool || s.PGID != fmt.Sprintf("%d.0", receipt.Pool) || len(s.ListSnaps) != len(objects) {
			t.Fatalf("%s: native snaptrim observation is not the owned PG's: %+v", label, s)
		}
		receipt.States[label] = s
		return s
	}
	allocate := func(label string) uint64 {
		r := nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "create"})
		if r.SnapID == 0 {
			t.Fatal("native self-managed snapshot allocation", r)
		}
		receipt.Snaps[label] = r.SnapID
		return r.SnapID
	}
	live, removed := allocate("live"), allocate("removed")
	if live >= removed {
		t.Fatal("native snap ids must increase", live, removed)
	}
	if s := observe(t, "initial", nil); len(s.SnapTrimQueue)+len(s.PurgedSnaps)+len(s.RemovedSnapQueue) != 0 || s.SnapSeq != removed || s.State != "active+clean" {
		t.Fatalf("owned pool does not start without removed snapshots: %+v", s)
	}

	// Pause both MGRs before the removal, so the MON cannot learn the trim.
	var dump struct {
		ActiveName string `json:"active_name"`
		Standbys   []struct {
			Name string `json:"name"`
		} `json:"standbys"`
	}
	raw, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
	if err != nil || json.Unmarshal(raw.Data, &dump) != nil || dump.ActiveName == "" {
		t.Fatal("mgr dump", err)
	}
	managers := []string{dump.ActiveName}
	for _, s := range dump.Standbys {
		managers = append(managers, s.Name)
	}
	// Each MGR is paused under its own lease; a lease's cleanup resumes it
	// after a failure.
	leases := map[string]*testcluster.DaemonLease{}
	resume := func(ctx context.Context) error {
		var errs []error
		for _, name := range managers {
			if lease := leases[name]; lease != nil {
				if _, err := resumeDaemon(ctx, lease); err != nil {
					errs = append(errs, err)
				} else {
					delete(leases, name)
				}
			}
		}
		return errors.Join(errs...)
	}
	for _, name := range managers {
		leases[name] = pauseDaemon(t, ctx, control, "mgr", name, leaseUntil(ctx))
	}
	receipt.Managers = managers
	receipt.Removal, err = c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(receipt.Pool), Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: removed})
	if err != nil || receipt.Removal.Code != 0 || receipt.Removal.Epoch == 0 {
		t.Fatal("remove self-managed snap", removed, receipt.Removal, err)
	}
	// wait observes until the removed snapshot reaches phase want; a phase
	// outside passing (the states on the way) fails.
	wait := func(t *testing.T, label string, passing []string, want string, bound time.Duration) copyPurgedTrimState {
		t.Helper()
		started := time.Now()
		for count := 1; ; count++ {
			s := observe(t, label, nil)
			receipt.Observations[label] = count
			phase := copyPurgedPhase(s, removed)
			if phase == want {
				t.Logf("%s after %s and %d native observations: %+v", label, time.Since(started).Round(time.Millisecond), count, s)
				return s
			}
			if !slices.Contains(passing, phase) {
				t.Fatalf("%s: unexpected trim phase %q: %+v", label, phase, s)
			}
			if time.Since(started) > bound {
				t.Fatalf("%s: the removed snapshot did not reach %s: %+v", label, want, s)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	wait(t, "purged", []string{"queued", "pending"}, "purged", 60*time.Second)

	seq := uint32(0)
	run := func(t *testing.T, code cephmsgr.OSDOpcode, name string, epoch uint32, snapc cephmsgr.OSDSnapContext) cell {
		t.Helper()
		dst := destination(code, name)
		route := poolPlace(t, ctx, 1, dst, "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		label := []byte("seed-" + dst)
		seq++
		seedReq := request(route,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(label)), Data: label},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: label})
		seedReq.ModificationTime = cephmsgr.OSDTime{Seconds: 1700096000, Nanoseconds: seq}
		seedReply, err := o.Request(ctx, seedReq)
		if err != nil || seedReply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("destination seed", dst, seedReply, err)
		}
		r := request(route, cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: receipt.Pool, SourceName: src}})
		r.SnapContext = snapc
		r.MapEpoch = max(r.MapEpoch, epoch)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700097000}
		reply, err := o.Request(ctx, r)
		entry := cell{Name: fmt.Sprintf("%x/%s", code, name), Code: code, SnapContext: snapc, Seed: seedReply, Reply: reply, After: poolObserve(t, ctx, 1, dst, "", "")}
		if err != nil {
			entry.Error = err.Error()
		}
		t.Logf("%s: aggregate=%d tid=%d uv=%d epoch=%d err=%v", entry.Name, reply.Code, reply.Transaction, reply.UserVersion, reply.MapEpoch, err)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Transaction == 0 || reply.Object != dst ||
			len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || reply.Operations[0].Code != code {
			t.Fatalf("%s: copy receipt %+v %v", entry.Name, reply, err)
		}
		a := entry.After
		if a.Code != 0 || !bytes.Equal(a.Data, source.Data) || a.Version != source.Version || reply.UserVersion != a.Version ||
			!reflect.DeepEqual(xattrInventory(a), xattrInventory(source)) || a.Seconds != source.Seconds || a.Nanos != source.Nanos {
			t.Fatalf("%s: native destination is not the copied source: %+v", entry.Name, a)
		}
		// The pre-copy head, if cloned, is readable at the live snapshot.
		if len(snapc.Snapshots) > 1 {
			clone := poolObserveSnap(t, ctx, 1, dst, "", "", live)
			entry.Clone = &clone
			if clone.Code != 0 || !bytes.Equal(clone.Data, label) || clone.Version != seedReply.UserVersion {
				t.Fatalf("%s: native read at the live snapshot is not the pre-copy head: %+v", entry.Name, clone)
			}
		}
		receipt.Cells = append(receipt.Cells, entry)
		return entry
	}
	// check compares the destination's native listsnaps with the model: the
	// snapset seq is the context seq, and a clone (when made) has the removed
	// id and the listed snapshots, sized like the seed.
	check := func(t *testing.T, s copyPurgedTrimState, code cephmsgr.OSDOpcode, name string, clone []uint64) {
		t.Helper()
		dst := destination(code, name)
		l := s.ListSnaps[dst].listing(t, dst)
		head := l.Clones[len(l.Clones)-1]
		if l.Seq != int64(removed) || !head.ID.Head || head.Size != uint64(len(source.Data)) {
			t.Fatalf("%s: native listsnaps head or seq differs: %s", dst, s.ListSnaps[dst].JSON)
		}
		if clone == nil {
			if len(l.Clones) != 1 {
				t.Fatalf("%s: a clone was made: %s", dst, s.ListSnaps[dst].JSON)
			}
			return
		}
		var ids []uint64
		if len(l.Clones) == 2 {
			for _, snap := range l.Clones[0].Snapshots {
				ids = append(ids, snap.ID)
			}
		}
		slices.Sort(ids)
		if len(l.Clones) != 2 || l.Clones[0].ID.Head || l.Clones[0].ID.ID != removed || !slices.Equal(ids, clone) ||
			l.Clones[0].Size != uint64(len("seed-"+dst)) || len(l.Clones[0].Overlaps) != 0 {
			t.Fatalf("%s: native clone differs from id %d holding %v: %s", dst, removed, clone, s.ListSnaps[dst].JSON)
		}
	}
	only := cephmsgr.OSDSnapContext{Sequence: removed, Snapshots: []uint64{removed}}
	newest := cephmsgr.OSDSnapContext{Sequence: removed, Snapshots: []uint64{removed, live}}
	t.Run("purged", func(t *testing.T) {
		for _, code := range codes {
			run(t, code, "purged-only", receipt.Removal.Epoch, only)
			run(t, code, "purged-newest", receipt.Removal.Epoch, newest)
		}
		s := observe(t, "purged-after", nil)
		if copyPurgedPhase(s, removed) != "purged" {
			t.Fatalf("the purged state did not hold through the cells: %+v", s)
		}
	})
	if t.Failed() {
		return
	}
	var names []string
	for _, code := range codes {
		names = append(names, destination(code, "purged-only"), destination(code, "purged-newest"))
	}
	listed := observe(t, "purged-listed", names)
	if copyPurgedPhase(listed, removed) != "purged" {
		t.Fatalf("the purged state did not hold through the listing: %+v", listed)
	}
	for _, code := range codes {
		check(t, listed, code, "purged-only", nil)
		check(t, listed, code, "purged-newest", []uint64{live})
	}

	if err := resume(ctx); err != nil {
		t.Fatal("resume the MGRs", err)
	}
	pruned := wait(t, "pruned", []string{"purged", "unqueued"}, "pruned", 180*time.Second)
	epoch := uint32(max(pruned.OSDMapEpoch, pruned.Epoch))
	t.Run("pruned", func(t *testing.T) {
		for _, code := range codes {
			run(t, code, "pruned-only", epoch, only)
			run(t, code, "pruned-newest", epoch, newest)
		}
	})
	if t.Failed() {
		return
	}
	names = names[:0]
	for _, code := range codes {
		names = append(names, destination(code, "pruned-only"), destination(code, "pruned-newest"))
	}
	listed = observe(t, "pruned-listed", names)
	if copyPurgedPhase(listed, removed) != "pruned" {
		t.Fatalf("the pruned state did not hold through the cells: %+v", listed)
	}
	for _, code := range codes {
		check(t, listed, code, "pruned-only", []uint64{removed})
		check(t, listed, code, "pruned-newest", []uint64{live, removed})
		// The removed snapshot is no longer queued, so its clone reads.
		dst := destination(code, "pruned-only")
		if at := poolObserveSnap(t, ctx, 1, dst, "", "", removed); at.Code != 0 || !bytes.Equal(at.Data, []byte("seed-"+dst)) {
			t.Fatalf("%s: native read at the pruned snapshot is not the pre-copy head: %+v", dst, at)
		}
	}
}
