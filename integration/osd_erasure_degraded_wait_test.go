package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A write to an object that a returned shard of an optimized isa pool
// (k=2,m=2, k=3,m=1 or k=4,m=2) is missing, while the norecover and nobackfill flags keep the PG from
// recovering it and osd_backoff_on_degraded keeps its default (false). The
// pinned source (Ceph v20.2.4) gives the expected native behaviour:
//
//   - do_op decodes the ops first and resets the op's description, so the
//     op tracker prints the object and the ops from then on
//     (src/osd/PrimaryLogPG.cc:2008-2011, src/messages/MOSDOp.h:622-641).
//   - An object is degraded while an acting shard that is not an async
//     recovery target has it in its missing set (src/osd/PrimaryLogPG.cc:655-696
//     is_degraded_or_backfilling_object). A write-ordered op on it, without
//     the backoff setting, waits in waiting_for_degraded_object and reports
//     the delay as its flag point (src/osd/PrimaryLogPG.cc:2225-2234,
//     :705-712 wait_for_degraded_object; src/osd/OpRequest.h:116-126,
//     :161-163), before any execution event.
//   - The wait starts that object's recovery at once (src/osd/PrimaryLogPG.cc:618-643
//     maybe_kick_recovery), which the norecover flag does not stop; when the
//     recovered object is complete on every shard, on_global_recover requeues
//     the waiters (src/osd/PrimaryLogPG.cc:499-537), and the write runs as any
//     other.
//
// The OSD of the returned shard is paused under a fixture-owned lease while
// the write waits, so the push of the recovered shard cannot complete and
// the wait lasts until the test ends the pause. The lease ends by
// degradedWaitPauseBudget, well inside the 20 s default
// osd_heartbeat_grace, from a deadline fixed before the pause is requested,
// and the fixture itself resumes the OSD at that deadline (heldPause,
// integration/fixture/daemon_lease.go). Every control while it lasts (the
// in-flight snapshots, the STAT, the primary's kill) runs under the pause's
// held phase; one that outlives it fails the test, and the lease still ends
// by its deadline. The test requires that its own resume ended the lease
// before the deadline. Each control also requires natively that the paused
// OSD was never marked down. These consumer helpers are test-only.

// degradedWaitEvent is the delay do_op marks for a write that waits for a
// degraded object (src/osd/PrimaryLogPG.cc:705-712).
const degradedWaitEvent = "waiting for degraded object"

// degradedWaitPauseBudget bounds how long the returned shard's OSD stays
// paused, counted from before the pause is requested until the resume is
// acknowledged: below the 20 s default osd_heartbeat_grace, so that its
// peers cannot report it before the pause ends.
const degradedWaitPauseBudget = 15 * time.Second

// degradedWaitResumeReserve is the part of the pause budget left to the
// resume: every control of the held phase must end before it.
const degradedWaitResumeReserve = 3 * time.Second

// checkDegradedWait judges one snapshot of the primary's op tracker,
// independently of the oracle's parse: a complete receipt between identical
// placement records that equal route but for an epoch not older than its
// own, answered by route's primary, holding exactly one op of the client
// entity and incarnation, for route's PG and shard at route's epoch, with its
// operations decoded for route's head object, delayed waiting for the
// degraded object right after its arrival at the PG, and with no execution
// event. It returns that op.
func checkDegradedWait(s opsInFlightSnapshot, route osdFixtureOracle, gid uint64, incarnation int32) (opsInFlightOp, error) {
	if !s.Complete || len(s.Failures) != 0 || s.Code != 0 || s.AbsenceEstablished {
		return opsInFlightOp{}, fmt.Errorf("incomplete in-flight snapshot: code %d, %q", s.Code, s.Failures)
	}
	if !samePlacement(s.Before, s.After) || s.Before.Epoch != s.After.Epoch || s.Before.Address != s.After.Address {
		return opsInFlightOp{}, fmt.Errorf("the placement records around the dump differ: %+v vs %+v", s.Before, s.After)
	}
	if !samePlacement(s.Before, route) || s.Before.Epoch < route.Epoch || s.OSD != route.PrimaryID || s.PGID != route.PGID {
		return opsInFlightOp{}, fmt.Errorf("the dump of osd.%d for %s at %+v is not the route's primary: %+v", s.OSD, s.PGID, s.Before, route)
	}
	if want := fmt.Sprintf("client.%d.%d", gid, incarnation); s.Client != want || len(s.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want 1 of %s)", len(s.Ops), s.Client, want)
	}
	op := s.Ops[0]
	if op.Reqid != fmt.Sprintf("client.%d.%d:%d", gid, incarnation, op.TID) || op.TID == 0 {
		return op, fmt.Errorf("op identity %s, tid %d", op.Reqid, op.TID)
	}
	if op.PGID != route.PGID+"s"+strconv.Itoa(int(route.Shard)) || op.Epoch != route.Epoch {
		return op, fmt.Errorf("op for %s at epoch %d; want %ss%d at %d", op.PGID, op.Epoch, route.PGID, route.Shard, route.Epoch)
	}
	if strings.Contains(op.Description, "(undecoded)") || !strings.Contains(op.Description, " "+hobjectHead(route)+" [") {
		return op, fmt.Errorf("op description %q does not show the decoded operations of %s", op.Description, hobjectHead(route))
	}
	reached := slices.Index(op.Events, "reached_pg")
	if op.FlagPoint != degradedWaitEvent || reached < 0 || !slices.Equal(op.Events[reached+1:], []string{degradedWaitEvent}) || slices.Contains(op.Events, "started") {
		return op, fmt.Errorf("op is at %q with events %q; want %q right after reached_pg and nothing executed", op.FlagPoint, op.Events, degradedWaitEvent)
	}
	return op, nil
}

// degradedObject is one object that the returned last parity shard is
// missing, with another object of the same PG that it is not.
type degradedObject struct {
	object string
	// route is the healthy placement before the outage, since the start of
	// its interval.
	route osdFixtureOracle
	// now is the degraded placement on the full acting set after the
	// return, in the interval that started at nowSince.
	now      osdFixtureOracle
	nowSince uint32
	// other is the placement of another object of the same PG.
	other osdFixtureOracle
	// written is the reply of the overwrite while the shard was away, and
	// pre the native object after the return.
	written cephmsgr.OSDReply
	pre     nativeXattrSnapshot
	data    []byte
	// returned is the OSD of the shard that is missing the object.
	returned int32
	// flagged reports that the recovery flags are set; the run's cleanup
	// unsets them while it is true.
	flagged *bool
}

// degradedObject seeds object and another object of its PG, kills the OSD of
// the last parity shard, overwrites object while it is away, sets norecover
// and nobackfill and starts the OSD again, and waits until the PG is active
// and degraded on its full placement (degradedOn).
func (r *ecEpochRun) degradedObject(object string, seeded, degradedAt uint32) degradedObject {
	t := r.t
	t.Helper()
	d := degradedObject{object: object, flagged: new(bool)}
	route, _ := r.healthy(object)
	other := ""
	for i := range 64 {
		name := object + "-other-" + strconv.Itoa(i)
		if p := r.place(name); p.PGID == route.PGID {
			other = name
			break
		}
	}
	if other == "" {
		t.Fatal("no name of 64 candidates maps to the PG of", object, route.PGID)
	}
	r.write("seed", route, bytes.Repeat([]byte{0x44, 0, 0xff, 'W'}, 1500), seeded, cephmsgr.OSDSnapContext{})
	r.write("seed the other object", r.place(other), []byte("another object of the same PG\x00\xff"), seeded, cephmsgr.OSDSnapContext{})
	route, since := r.healthy(object)
	d.route = route

	last := r.facts.size - 1
	parity := r.kill(route.Acting[last])
	absent := r.waitState("active without the last parity shard", object, func(o pgIntervalObservation) error { return o.activeWithout(route, []int{last}, since) })
	d.data = bytes.Repeat([]byte("written while the parity shard is away\x02"), 100)
	d.written = r.write("overwrite while the shard is away", *absent.After, d.data, degradedAt, cephmsgr.OSDSnapContext{})

	// Registered after the run's own cleanups, so it runs before their
	// recovery, which waits for clean PGs.
	t.Cleanup(func() {
		if *d.flagged {
			cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			nativePool(t, cleanup, "osdflags", map[string]any{"action": "unset"})
		}
	})
	*d.flagged = true
	flags := nativePool(t, r.ctx, "osdflags", map[string]any{"action": "set"})
	if !slices.Contains(flags.OSDMapFlags, "norecover") || !slices.Contains(flags.OSDMapFlags, "nobackfill") {
		t.Fatal("recovery flags were not set", flags.OSDMapFlags)
	}
	r.start(parity)
	d.returned = parity.osd
	seen := r.waitState("degraded on the returned shard", object, func(o pgIntervalObservation) error { return o.degradedOn(route, absent.Query.SameIntervalSince) })
	d.now, d.nowSince = *seen.After, seen.Query.SameIntervalSince
	d.pre = poolObserve(t, r.ctx, 1, object, "", "")
	if d.pre.Code != 0 || !bytes.Equal(d.pre.Data, d.data) || d.pre.Version != d.written.UserVersion || d.pre.Seconds != int64(degradedAt) {
		t.Fatal("native state before the held write differs", d.pre.Code, d.pre.Version, d.written.UserVersion)
	}
	d.other = r.place(other)
	if d.other.PGID != d.now.PGID || d.other.ID != d.now.ID || d.other.Shard != d.now.Shard {
		t.Fatal("the other object left the PG or its primary", d.other, d.now)
	}
	return d
}

// unsetFlags clears norecover and nobackfill.
func (r *ecEpochRun) unsetFlags(d degradedObject) {
	r.t.Helper()
	nativePool(r.t, r.ctx, "osdflags", map[string]any{"action": "unset"})
	*d.flagged = false
}

// pause pauses the OSD under a lease that the fixture ends by
// degradedWaitPauseBudget from now (heldPause), its held phase ending
// degradedWaitResumeReserve before; the lease's cleanup ends it after a
// failure.
func (r *ecEpochRun) pause(id int32) *heldPause {
	r.t.Helper()
	return startHeldPause(r.t, r.ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd", strconv.FormatInt(int64(id), 10), time.Now().Add(degradedWaitPauseBudget), degradedWaitResumeReserve)
}

// checkHeld fails the test when step ended past the held phase.
func (r *ecEpochRun) checkHeld(p *heldPause, step string) {
	r.t.Helper()
	if err := p.check(step); err != nil {
		r.t.Fatal(err)
	}
}

// resume ends the pause and fails the test when it did not end within the
// budget.
func (r *ecEpochRun) resume(p *heldPause) {
	r.t.Helper()
	left, err := p.resume()
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Logf("%s resumed %v before the end of its %v pause budget", p.daemon(), left.Round(100*time.Millisecond), p.budget)
}

// waitDegraded takes bounded snapshots of the primary's op tracker, each
// under the pause's held phase, until one holds the write of client gid
// under incarnation waiting for the degraded object, and returns that op.
func (r *ecEpochRun) waitDegraded(d degradedObject, p *heldPause, call heldCall, gid uint64, incarnation int32) opsInFlightOp {
	r.t.Helper()
	err := errors.New("no snapshot taken")
	for attempt := 1; attempt <= 10 && p.held.Err() == nil; attempt++ {
		s := primaryOpsInFlight(r.t, p.held, 1, d.object, gid, incarnation)
		r.checkHeld(p, fmt.Sprintf("in-flight snapshot %d", attempt))
		var op opsInFlightOp
		if op, err = checkDegradedWait(s, d.now, gid, incarnation); err == nil {
			r.t.Logf("held at osd.%d (snapshot %d, %d ops in flight): %s, flag point %s, events %q", s.OSD, attempt, s.NumOps, op.Description, op.FlagPoint, op.Events)
			select {
			case early := <-call.finished:
				r.t.Fatal("the held write ended", early.reply.Code, early.err)
			default:
			}
			return op
		}
		r.t.Logf("snapshot %d: %v", attempt, err)
		select {
		case <-p.held.Done():
		case <-time.After(300 * time.Millisecond):
		}
	}
	r.t.Fatal("the primary did not hold the write for the degraded object within the held phase of the pause:", err)
	return opsInFlightOp{}
}

// cleanInInterval unsets nothing; it waits for every PG to be clean and for
// one observation of the PG active+clean on route's placement in the
// interval that started at since: no OSD was marked down in between.
func (r *ecEpochRun) cleanInInterval(object string, route osdFixtureOracle, since uint32) {
	r.t.Helper()
	r.clean()
	r.waitState("clean in the same interval", object, func(o pgIntervalObservation) error {
		if err := o.cleanOn(); err != nil {
			return err
		}
		if !samePlacement(*o.Before, route) || o.Query.SameIntervalSince != since {
			return fmt.Errorf("the placement %+v in the interval since %d is not %+v in the interval since %d", *o.Before, o.Query.SameIntervalSince, route, since)
		}
		return nil
	})
}

// osdMapState is one OSD's entry of one native osd dump (oracle mode
// osdstate, tools/osd_pool_mode_osdstate_oracle.py).
type osdMapState struct {
	Epoch  uint32 `json:"epoch"`
	OSD    int32  `json:"osd"`
	Up     bool   `json:"up"`
	In     bool   `json:"in"`
	UpFrom uint32 `json:"up_from"`
	UpThru uint32 `json:"up_thru"`
	DownAt uint32 `json:"down_at"`
}

// osdState reads osd id's entry of the native OSD map.
func (r *ecEpochRun) osdState(id int32) osdMapState {
	r.t.Helper()
	var result struct {
		State *osdMapState `json:"osd_state"`
	}
	nativePoolResult(r.t, r.ctx, "osdstate", map[string]any{"osd": id}, &result)
	if result.State == nil || result.State.OSD != id {
		r.t.Fatal("no native OSD map entry of osd", id, result.State)
	}
	return *result.State
}

// checkStayedUp requires two entries of one OSD, before and after a window
// that lasts at least until epoch through, to show it up in both with the
// same up_from and down_at: the pinned OSDMap::apply_incremental sets
// down_at when a map marks an up OSD down and up_from when one marks it up
// (src/osd/OSDMap.cc:2484-2521), so no map in between marked it down.
func checkStayedUp(before, after osdMapState, through uint32) error {
	if before.OSD != after.OSD || after.Epoch < through || after.Epoch < before.Epoch {
		return fmt.Errorf("the entries of osd.%d at epoch %d and osd.%d at epoch %d do not span the window through epoch %d", before.OSD, before.Epoch, after.OSD, after.Epoch, through)
	}
	if !before.Up || !after.Up || before.UpFrom != after.UpFrom || before.DownAt != after.DownAt {
		return fmt.Errorf("osd.%d did not stay up from epoch %d to %d: up %v -> %v, up_from %d -> %d, down_at %d -> %d",
			before.OSD, before.Epoch, after.Epoch, before.Up, after.Up, before.UpFrom, after.UpFrom, before.DownAt, after.DownAt)
	}
	return nil
}

// stayedUp reads the OSD's entry again and requires it to have stayed up
// since before (checkStayedUp).
func (r *ecEpochRun) stayedUp(before osdMapState, through uint32) {
	r.t.Helper()
	after := r.osdState(before.OSD)
	if err := checkStayedUp(before, after, through); err != nil {
		r.t.Fatal(err)
	}
	r.t.Logf("osd.%d stayed up from epoch %d to %d: up_from %d, up_thru %d -> %d, down_at %d", after.OSD, before.Epoch, after.Epoch, after.UpFrom, before.UpThru, after.UpThru, after.DownAt)
}

// degradedWaitOps is the held write: an APPEND of tail and a SETXATTR.
func degradedWaitOps(tail, value []byte, attr string) []cephmsgr.OSDOperation {
	return []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
	}
}

// erasureDegradedWriteWaitScenario: a write
// during recovery of its object on an optimized EC PG. The OSD of the last
// parity shard is killed, the object is overwritten through the unchanged
// shard-0 primary while it is away, and the norecover and nobackfill flags
// keep the returned shard from recovering it: the PG is active and degraded
// on its full placement. With the returned OSD paused, one Go
// APPEND+SETXATTR of that object must be held at the primary: a bracketed
// dump_ops_in_flight shows exactly that op, decoded for the object at its
// PG shard and epoch, waiting for the degraded object, with nothing
// executed, and a STAT of another object of the same PG on the same
// connection completes meanwhile. After the pause ends, the primary
// completes the object's recovery and the held write must complete once
// with ONDISK under its original transaction, with the flags still set; the
// native bytes, xattr, user version and mtime must show one application.
// The flags are restored and every PG is clean again in the same interval,
// so the pause never marked the OSD down.
//
// Not verified: a write that waits for an unreadable object (missing on the
// primary), an op held inside the EC backend for sub-write commits, a pause
// past the heartbeat grace, backfill and async recovery targets.
func erasureDegradedWriteWaitScenario(t *testing.T, width erasureWidth) {
	r := startECEpochRun(t, width, 900*time.Second)
	const (
		object          = "ec-degraded-wait"
		attr            = "ec-degraded-wait"
		heldIncarnation = 71
		seeded          = 1700012000
		degradedAt      = 1700012001
		mutated         = 1700012002
	)
	d := r.degradedObject(object, seeded, degradedAt)

	client := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer client.Close()
	gid := client.Snapshot().GlobalID
	o := mutationConnection(t, client.Client, r.ctx, d.now)
	defer o.Close()
	tail := []byte("degraded-wait\x00\xff" + object)
	value := []byte{0, 0xff, 'w'}
	held := r.request(d.now, degradedWaitOps(tail, value, attr)...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}

	p := r.pause(d.returned)
	call := startHeldCall(r.ctx, o, held)
	op := r.waitDegraded(d, p, call, gid, heldIncarnation)
	stat := r.request(d.other, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	stat.ClientIncarnation = heldIncarnation + 1
	statReply, err := o.Request(p.held, stat)
	r.checkHeld(p, "the STAT of another object of the PG")
	if err != nil || statReply.Code != 0 || len(statReply.Operations) != 1 {
		t.Fatal("STAT of another object of the PG while the write waits", err, statReply.Code)
	}
	select {
	case early := <-call.finished:
		t.Fatal("the held write ended while the returned OSD was paused", early.reply.Code, early.err)
	default:
	}
	r.resume(p)

	var done heldResult
	select {
	case done = <-call.finished:
	case <-time.After(60 * time.Second):
		call.cancel()
		t.Fatal("the held write did not complete within 60 s after the pause ended")
	}
	reply := done.reply
	t.Logf("held write completed: tid %d code %d flags %#x user version %d replay %s epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.MapEpoch)
	if done.err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Transaction != op.TID || reply.RetryAttempt != 0 ||
		reply.UserVersion <= d.pre.Version || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != heldIncarnation {
		t.Fatal("the held write's reply differs", done.err, reply.Code, reply.Flags, reply.Transaction, op.TID, reply.UserVersion)
	}
	if !versionAfter(reply.ReplayVersion, d.written.ReplayVersion) {
		t.Fatalf("the held write applied at %s, not after the overwrite at %s", replayVersion(reply), replayVersion(d.written))
	}
	final := poolObserve(t, r.ctx, 1, object, "", "")
	want := append(bytes.Clone(d.data), tail...)
	if final.Code != 0 || !bytes.Equal(final.Data, want) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion || final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the held write differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	t.Logf("applied once after the object's recovery: version %d -> %d", d.pre.Version, final.Version)

	r.unsetFlags(d)
	r.cleanInInterval(object, d.route, d.nowSince)
	recovered := poolObserve(t, r.ctx, 1, object, "", "")
	if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || recovered.Seconds != final.Seconds || !maps.Equal(xattrInventory(recovered), xattrInventory(final)) {
		t.Fatal("native state after recovery differs", recovered.Version, final.Version)
	}
}

// TestCephOSDErasureOptimizedK2M2DegradedWriteWaitIntegration is erasureDegradedWriteWaitScenario on optimized isa
// k=2,m=2 (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2degradedwritewait key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses an OSD and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2DegradedWriteWaitIntegration(t *testing.T) {
	erasureDegradedWriteWaitScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1DegradedWriteWaitIntegration is erasureDegradedWriteWaitScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1degradedwritewait key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses an OSD and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK3M1DegradedWriteWaitIntegration(t *testing.T) {
	erasureDegradedWriteWaitScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2DegradedWriteWaitIntegration is erasureDegradedWriteWaitScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2degradedwritewait key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses an OSD and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2DegradedWriteWaitIntegration(t *testing.T) {
	erasureDegradedWriteWaitScenario(t, erasureK4M2)
}

// versionAfter reports whether a is a newer PG version than b.
func versionAfter(a, b cephmsgr.OSDVersion) bool {
	return a.Epoch > b.Epoch || a.Epoch == b.Epoch && a.Version > b.Version
}

// erasureDegradedWaitAtDyingPrimaryScenario: a
// request held at the primary after do_op decoded it, when that primary's
// OSD dies. The object is degraded as in
// erasureDegradedWriteWaitScenario and the
// returned OSD is paused, so the Go APPEND+SETXATTR waits for the degraded
// object, shown by a bracketed dump_ops_in_flight. The primary's OSD is then
// killed through the fixture's forced kill while the returned OSD is still
// paused, and the pause ends. The native OSD map must show the returned OSD
// up with the up_from and down_at it had before the pause, once the map shows
// the primary gone and again after the primary's return, so the pause did not
// add a second outage. The call must end with an OutcomeUnknownError
// that keeps its original identity, and the client must open no further OSD
// connection. The flags are unset; after the primary's restart and clean
// PGs the native object must be unchanged, and the returned primary's PG log
// must hold the overwrite's identity but not the held one. The caller then
// resubmits the same operations once under the original transaction, which
// must apply once and appear once in that log.
//
// The held op lived only in the dead primary's waiting_for_degraded_object;
// no other shard had it. Not verified: an op held inside the EC backend
// after its sub-writes were sent, which other shards may have applied, a
// genuinely lagging primary, an orderly stop or partition, and other
// shards' logs.
func erasureDegradedWaitAtDyingPrimaryScenario(t *testing.T, width erasureWidth) {
	r := startECEpochRun(t, width, 900*time.Second)
	const (
		object          = "ec-degraded-wait-dying"
		attr            = "ec-degraded-wait-dying"
		heldIncarnation = 81
		seeded          = 1700013000
		degradedAt      = 1700013001
		mutated         = 1700013002
	)
	d := r.degradedObject(object, seeded, degradedAt)

	endpoint := osdEndpoint(t, d.now.Address)
	fc := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer fc.Close()
	gid := fc.Snapshot().GlobalID
	o := mutationConnection(t, fc.Client, r.ctx, d.now)
	defer o.Close()
	dialed := fc.dialSnapshot()
	if dialed[endpoint] != 1 {
		t.Fatal("the held write's connection was not dialed exactly once", dialed)
	}
	tail := []byte("degraded-wait-dying\x00\xff" + object)
	value := []byte{0, 0xff, 'd'}
	ops := degradedWaitOps(tail, value, attr)
	held := r.request(d.now, ops...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}

	returned := r.osdState(d.returned)
	p := r.pause(d.returned)
	call := startHeldCall(r.ctx, o, held)
	op := r.waitDegraded(d, p, call, gid, heldIncarnation)
	primary := r.killWithin(p.held, d.now.ID)
	r.checkHeld(p, "the kill of the primary's OSD")
	r.resume(p)

	// The killed daemon's socket closes, so the session ends the call; the
	// caller's context stays live.
	lost, ended, late := endWithin(call.finished, 30*time.Second, 5*time.Second, call.cancel)
	if !ended {
		t.Fatal("the session did not end the held write within 30 s after the primary's OSD was killed;", late)
	}
	var unknown *cephmsgr.OutcomeUnknownError
	var server *cephmsgr.OSDError
	sub := lost.reply.Submission
	t.Logf("held write ended by the session: %v; submission tid %d global id %d incarnation %d epoch %d attempt %d placement %+v",
		lost.err, sub.Transaction, sub.GlobalID, sub.ClientIncarnation, sub.MapEpoch, sub.Attempt, sub.Placement)
	if !errors.As(lost.err, &unknown) || errors.As(lost.err, &server) || errors.Is(lost.err, context.Canceled) {
		t.Fatal("expected an unknown outcome without a server result or a local cancellation", lost.err)
	}
	if sub.Transaction != op.TID || sub.GlobalID != gid || sub.ClientIncarnation != heldIncarnation || sub.MapEpoch != d.now.Epoch ||
		sub.Attempt != 0 || sub.Placement == nil || sub.Placement.Primary != d.now.ID || int8(sub.Placement.Shard) != d.now.Shard {
		t.Fatal("the unknown outcome lost the identity the primary held", sub, op.TID)
	}
	o.Close()
	// The map marks the killed OSD down a moment after its socket closed.
	away := r.place(object)
	for attempt := 1; attempt < 30 && (away.Epoch <= d.now.Epoch || slices.Contains(away.Acting, d.now.ID)); attempt++ {
		time.Sleep(time.Second)
		away = r.place(object)
	}
	t.Logf("without the primary: epoch %d primary osd.%d shard %d acting %v pg_temp %v", away.Epoch, away.ID, away.Shard, away.Acting, away.PGTemp)
	if away.Epoch <= d.now.Epoch || slices.Contains(away.Acting, d.now.ID) {
		t.Fatal("the placement does not reflect the killed primary within 30 s", away)
	}
	r.stayedUp(returned, away.Epoch)

	r.unsetFlags(d)
	back, _ := r.primaryBack(object, d.now, primary, d.nowSince)
	t.Logf("restored: epoch %d primary osd.%d shard %d acting %v at %s (was %s)", back.Epoch, back.ID, back.Shard, back.Acting, back.Address, d.now.Address)
	r.stayedUp(returned, back.Epoch)
	n := poolObserve(t, r.ctx, 1, object, "", "")
	if n.Code != 0 || n.Version != d.pre.Version || !bytes.Equal(n.Data, d.data) || n.Seconds != d.pre.Seconds || n.Nanos != d.pre.Nanos || !maps.Equal(xattrInventory(n), xattrInventory(d.pre)) {
		t.Fatal("native state after the return differs from the pre-state", n.Version, d.pre.Version, len(n.Data), n.Seconds)
	}
	if extra := newDials(dialed, fc.dialSnapshot(), endpoint); len(extra) != 0 {
		t.Fatal("the client connected again before the caller's decision:", extra)
	}
	ws := d.written.Submission
	writtenKey := pgHistoryTarget{Type: "client", Num: ws.GlobalID, Inc: int32(ws.ClientIncarnation), TID: ws.Transaction}
	heldKey := pgHistoryTarget{Type: "client", Num: gid, Inc: heldIncarnation, TID: sub.Transaction}
	nearMiss := pgHistoryTarget{Type: "client", Num: gid, Inc: heldIncarnation + 1, TID: sub.Transaction}
	keys := []pgHistoryTarget{writtenKey, heldKey, nearMiss}
	history := primaryPGHistory(t, r.ctx, 1, object, keys)
	requirePGHistoryOnRoute(t, history, back)
	if got := history.Targets[0].Locations; len(got) != 1 || got[0].Tier != "log" || got[0].Object != hobjectHead(back) || got[0].Version != replayVersion(d.written) {
		t.Fatalf("the returned primary's log does not hold the overwrite once at %s: %+v", replayVersion(d.written), got)
	}
	if len(history.Targets[1].Locations) != 0 || len(history.Targets[2].Locations) != 0 {
		t.Fatalf("the returned primary's log holds the held identity: %+v %+v", history.Targets[1].Locations, history.Targets[2].Locations)
	}

	// The caller's explicit decision: resubmit the same operations once
	// under the original identity to the returned primary.
	ro := mutationConnection(t, fc.Client, r.ctx, back)
	defer ro.Close()
	retry := r.request(back, ops...)
	retry.ClientIncarnation = heldIncarnation
	retry.ModificationTime = held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	retryCtx, stop := context.WithTimeout(r.ctx, 30*time.Second)
	reply, err := ro.Request(retryCtx, retry)
	stop()
	if err != nil {
		t.Fatal("explicit resubmission to the returned primary:", err)
	}
	t.Logf("resubmission: tid %d code %d flags %#x user version %d replay %s attempt %d epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.RetryAttempt, reply.MapEpoch)
	if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= d.pre.Version ||
		reply.Submission.Transaction != sub.Transaction || reply.Submission.MapEpoch != back.Epoch || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != heldIncarnation {
		t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	final := poolObserve(t, r.ctx, 1, object, "", "")
	if final.Code != 0 || !bytes.Equal(final.Data, append(bytes.Clone(d.data), tail...)) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion ||
		final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	after := primaryPGHistory(t, r.ctx, 1, object, keys)
	requirePGHistoryOnRoute(t, after, back)
	if got := after.Targets[1].Locations; len(got) != 1 || got[0].Tier != "log" || got[0].Version != replayVersion(reply) || got[0].Object != hobjectHead(back) ||
		len(after.Targets[0].Locations) != 1 || len(after.Targets[2].Locations) != 0 {
		t.Fatalf("the returned primary's log after the resubmission: overwrite %+v held %+v near miss %+v", after.Targets[0].Locations, got, after.Targets[2].Locations)
	}
	if err := checkOneDial(dialed, fc.dialSnapshot(), endpoint, osdEndpoint(t, back.Address)); err != nil {
		t.Fatal("the resubmission did not open exactly its one connection:", err)
	}
	r.heldThrough("the log reads and the resubmission at the returned primary", object, back)
	t.Logf("applied once by the resubmission at %s, version %d -> %d; the original outcome stays unknown", replayVersion(reply), d.pre.Version, final.Version)
}

// TestCephOSDErasureOptimizedK2M2DegradedWaitAtDyingPrimaryIntegration is erasureDegradedWaitAtDyingPrimaryScenario on optimized isa
// k=2,m=2 (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2degradedwaitatdyingprimary key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses OSDs and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2DegradedWaitAtDyingPrimaryIntegration(t *testing.T) {
	erasureDegradedWaitAtDyingPrimaryScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1DegradedWaitAtDyingPrimaryIntegration is erasureDegradedWaitAtDyingPrimaryScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1degradedwaitatdyingprimary key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses OSDs and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK3M1DegradedWaitAtDyingPrimaryIntegration(t *testing.T) {
	erasureDegradedWaitAtDyingPrimaryScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2DegradedWaitAtDyingPrimaryIntegration is erasureDegradedWaitAtDyingPrimaryScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2degradedwaitatdyingprimary key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated kills and pauses OSDs and sets norecover/nobackfill; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2DegradedWaitAtDyingPrimaryIntegration(t *testing.T) {
	erasureDegradedWaitAtDyingPrimaryScenario(t, erasureK4M2)
}

// degradedWaitSnapshot builds one synthetic receipt of PG 3.7's primary
// osd.3 holding client.4171.71:5, decoded, waiting for the degraded object
// at the placement's epoch 25. No daemon produced it.
func degradedWaitSnapshot() (opsInFlightSnapshot, osdFixtureOracle) {
	s, route := heldSnapshot()
	s.Client = "client.4171.71"
	op := &s.Ops[0]
	op.Reqid, op.Epoch, op.FlagPoint = "client.4171.71:5", 25, degradedWaitEvent
	op.Events = []string{"initiated", "throttled", "header_read", "all_read", "dispatched", "queued_for_pg", "reached_pg", degradedWaitEvent}
	op.Description = "osd_op(client.4171.71:5 3.7s0 " + hobjectHead(route) + " [append 0~13,setxattr ec (3)] snapc 0=[] ondisk+write+known_if_redirected e25)"
	return s, route
}

func TestCheckDegradedWait(t *testing.T) {
	s, route := degradedWaitSnapshot()
	if op, err := checkDegradedWait(s, route, 4171, 71); err != nil || op.TID != 5 {
		t.Fatal("held write refused:", op, err)
	}
	newer, _ := degradedWaitSnapshot()
	newer.Before.Epoch, newer.After.Epoch = 26, 26
	if _, err := checkDegradedWait(newer, route, 4171, 71); err != nil {
		t.Fatal("held write in a dump at a newer epoch of the same placement refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot){
		"incomplete":         func(s *opsInFlightSnapshot) { s.Complete = false },
		"a recorded failure": func(s *opsInFlightSnapshot) { s.Failures = []string{"placement or epoch changed"} },
		"refused":            func(s *opsInFlightSnapshot) { s.Code = -22 },
		"claims absence":     func(s *opsInFlightSnapshot) { s.AbsenceEstablished = true },
		"placement moved":    func(s *opsInFlightSnapshot) { s.After.Epoch = 26 },
		"acting moved":       func(s *opsInFlightSnapshot) { s.After.Acting = []int32{3, 2, 1, crushItemNone} },
		"an older epoch":     func(s *opsInFlightSnapshot) { s.Before.Epoch, s.After.Epoch = 24, 24 },
		"another primary":    func(s *opsInFlightSnapshot) { s.OSD = 2 },
		"another PG":         func(s *opsInFlightSnapshot) { s.PGID = "3.6" },
		"another address": func(s *opsInFlightSnapshot) {
			s.Before.Address, s.After.Address = "v2:192.0.2.7:6800/2", "v2:192.0.2.7:6800/2"
		},
		"another client":          func(s *opsInFlightSnapshot) { s.Client = "client.4171.70" },
		"no op":                   func(s *opsInFlightSnapshot) { s.Ops = nil },
		"two ops":                 func(s *opsInFlightSnapshot) { s.Ops = append(s.Ops, s.Ops[0]) },
		"identity of another tid": func(s *opsInFlightSnapshot) { s.Ops[0].Reqid = "client.4171.71:6" },
		"zero tid":                func(s *opsInFlightSnapshot) { s.Ops[0].Reqid, s.Ops[0].TID = "client.4171.71:0", 0 },
		"another shard":           func(s *opsInFlightSnapshot) { s.Ops[0].PGID = "3.7s2" },
		"no shard":                func(s *opsInFlightSnapshot) { s.Ops[0].PGID = "3.7" },
		"another epoch":           func(s *opsInFlightSnapshot) { s.Ops[0].Epoch = 26 },
		"undecoded": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = "osd_op(client.4171.71:5 3.7s0 3.d6c2a1e7 (undecoded) ondisk+write+known_if_redirected e25)"
		},
		"another object": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = strings.Replace(s.Ops[0].Description, ":::obj:head", ":::other:head", 1)
		},
		"a snapshot object": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = strings.Replace(s.Ops[0].Description, ":::obj:head", ":::obj:4", 1)
		},
		"not delayed":         func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = "started" },
		"waiting for missing": func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = "waiting for missing object" },
		"waiting for the map": func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = heldEvent },
		"never reached the PG": func(s *opsInFlightSnapshot) {
			s.Ops[0].Events = []string{"initiated", "queued_for_pg", degradedWaitEvent}
		},
		"another delay event":     func(s *opsInFlightSnapshot) { s.Ops[0].Events[len(s.Ops[0].Events)-1] = "waiting for scrub" },
		"executed after the wait": func(s *opsInFlightSnapshot) { s.Ops[0].Events = append(s.Ops[0].Events, "started") },
		"executed before the wait": func(s *opsInFlightSnapshot) {
			s.Ops[0].Events = slices.Insert(s.Ops[0].Events, 6, "started")
		},
		"requeued and held again": func(s *opsInFlightSnapshot) {
			s.Ops[0].Events = append(s.Ops[0].Events, "reached_pg", degradedWaitEvent)
		},
	} {
		changed, _ := degradedWaitSnapshot()
		edit(&changed)
		if _, err := checkDegradedWait(changed, route, 4171, 71); err == nil {
			t.Error("snapshot accepted:", name)
		}
	}
	for name, c := range map[string]struct {
		gid uint64
		inc int32
	}{
		"another entity":      {4172, 71},
		"another incarnation": {4171, 72},
	} {
		if _, err := checkDegradedWait(s, route, c.gid, c.inc); err == nil {
			t.Error("request accepted:", name)
		}
	}
	moved := route
	moved.Epoch = 26
	if _, err := checkDegradedWait(s, moved, 4171, 71); err == nil {
		t.Error("a dump older than the route accepted")
	}
}

func TestVersionAfter(t *testing.T) {
	for _, c := range []struct {
		a, b cephmsgr.OSDVersion
		want bool
	}{
		{cephmsgr.OSDVersion{Epoch: 27, Version: 3}, cephmsgr.OSDVersion{Epoch: 27, Version: 2}, true},
		{cephmsgr.OSDVersion{Epoch: 28, Version: 1}, cephmsgr.OSDVersion{Epoch: 27, Version: 2}, true},
		{cephmsgr.OSDVersion{Epoch: 27, Version: 2}, cephmsgr.OSDVersion{Epoch: 27, Version: 2}, false},
		{cephmsgr.OSDVersion{Epoch: 26, Version: 9}, cephmsgr.OSDVersion{Epoch: 27, Version: 2}, false},
	} {
		if got := versionAfter(c.a, c.b); got != c.want {
			t.Errorf("versionAfter(%+v, %+v) = %v", c.a, c.b, got)
		}
	}
}

func TestCheckStayedUp(t *testing.T) {
	before := osdMapState{Epoch: 35, OSD: 3, Up: true, In: true, UpFrom: 31, UpThru: 33, DownAt: 28}
	after := before
	after.Epoch, after.UpThru = 41, 39
	if err := checkStayedUp(before, after, 40); err != nil {
		t.Fatal("an OSD that stayed up refused:", err)
	}
	for name, edit := range map[string]func(b, a *osdMapState){
		"marked down":              func(b, a *osdMapState) { a.Up, a.DownAt = false, 38 },
		"marked down and up again": func(b, a *osdMapState) { a.UpFrom, a.DownAt = 39, 38 },
		"up again only":            func(b, a *osdMapState) { a.UpFrom = 39 },
		"down mark only":           func(b, a *osdMapState) { a.DownAt = 38 },
		"down before the window":   func(b, a *osdMapState) { b.Up = false },
		"another OSD":              func(b, a *osdMapState) { a.OSD = 2 },
		"before the fault epoch":   func(b, a *osdMapState) { a.Epoch = 39 },
		"an older dump after":      func(b, a *osdMapState) { b.Epoch, a.Epoch = 42, 41 },
	} {
		b, a := before, after
		edit(&b, &a)
		if err := checkStayedUp(b, a, 40); err == nil {
			t.Error("accepted:", name)
		}
	}
}
