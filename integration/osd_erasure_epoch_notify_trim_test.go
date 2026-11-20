package integration_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDErasureOptimizedK2M2DisconnectedNotifyIntegration: notifications
// to a Go watch registration of the optimized pool while no connection holds
// it. The registration is made at the shard-0 primary, whose OSD is then
// killed; the replacement under the primary-first pg_temp loads it as a
// disconnected watch (src/osd/PrimaryLogPG.cc populate_obc_watchers), and the
// native watcher list still lists it. A notify through the replacement with a
// 5-second timeout must complete with ETIMEDOUT and name the registration as
// missed (src/osd/Watch.cc Notify::do_timeout). A second notify with a longer
// timeout, started before the caller RECONNECTs the cookie to the
// replacement, must be delivered on the reconnected handle, which sends the
// notifications still in progress (Watch::connect), and complete with the
// caller's ACK. The timed-out notification is not delivered. After the
// UNWATCH the list is empty at the replacement and at the returned primary.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2disconnectednotify key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills the primary OSD; restart waits osd-ready clean, which does not prove no pg_temp
func TestCephOSDErasureOptimizedK2M2DisconnectedNotifyIntegration(t *testing.T) {
	r := startECEpochRun(t, 900*time.Second)
	const (
		object      = "ec-disconnected-notify"
		incarnation = 71
		// Long enough that the disconnected registration outlives both
		// notifications and the RECONNECT.
		watchTimeout = 120
	)
	route, _ := r.healthy(object)
	r.write("seed", route, bytes.Repeat([]byte{0x6e, 0, 0xff, 'N'}, 1024), 1700012000, cephmsgr.OSDSnapContext{})
	route, since := r.healthy(object)

	watcher := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer watcher.Close()
	gid := watcher.Snapshot().GlobalID
	o := mutationConnection(t, watcher.Client, r.ctx, route)
	request := r.request(route)
	request.ClientIncarnation = incarnation
	watch, initial, err := o.WatchObject(r.ctx, request, cephmsgr.OSDWatchOptions{TimeoutSeconds: watchTimeout, RegistrationGeneration: 3})
	if err != nil {
		t.Fatal("watch at the original primary", err)
	}
	cookie := watch.Cookie()
	r.watchers("at the original primary", object, 1, gid, cookie)

	fresh, outage := r.replacePrimary(object, route, since)
	o.Close()
	r.watchers("under the replacement, disconnected", object, 1, gid, cookie)
	n := mutationConnection(t, r.c, r.ctx, fresh)
	defer n.Close()

	// A notify that times out while the registration is disconnected.
	missedPayload := []byte{0, 0xff, 'm'}
	missed, missedReply, err := n.NotifyObject(r.ctx, r.request(fresh), missedPayload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
	if err != nil {
		t.Fatal("notify through the replacement", err)
	}
	waited, stop := context.WithTimeout(r.ctx, 60*time.Second)
	timedOut, err := missed.Next(waited)
	stop()
	missed.Close()
	if err != nil || timedOut.Opcode != cephmsgr.OSDWatchCompletion || timedOut.ReturnCode != -110 || len(timedOut.Replies) != 0 ||
		len(timedOut.Missed) != 1 || timedOut.Missed[0].GlobalID != gid || timedOut.Missed[0].Cookie != cookie {
		t.Fatal("the notify to the disconnected registration did not time out naming it as missed", timedOut, err)
	}
	t.Logf("notify tid %d (notify id %d) through osd.%d timed out: missed %+v", missedReply.Transaction, timedOut.NotifyID, fresh.ID, timedOut.Missed)

	// A notify in progress when the caller RECONNECTs.
	payload, ackData := []byte{0, 0xff, 'd'}, []byte{1, 'd'}
	pending, pendingReply, err := n.NotifyObject(r.ctx, r.request(fresh), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 60})
	if err != nil {
		t.Fatal("second notify through the replacement", err)
	}
	defer pending.Close()
	ro := mutationConnection(t, watcher.Client, r.ctx, fresh)
	again := r.request(fresh)
	again.ClientIncarnation = incarnation
	moved, reconnect, err := ro.ReconnectWatch(r.ctx, watch, again)
	if err != nil {
		t.Fatal("RECONNECT to the replacement primary", reconnect, err)
	}
	if moved.Cookie() != cookie || reconnect.Operations[0].Watch.Generation != initial.Operations[0].Watch.Generation+1 || reconnect.Submission.Placement.PrimaryFirst == nil {
		t.Fatal("RECONNECT changed the cookie, kept the generation or lost the primary-first placement", reconnect)
	}
	delivered, stop := context.WithTimeout(r.ctx, 30*time.Second)
	event, err := moved.Next(delivered)
	stop()
	if err != nil || event.Opcode != cephmsgr.OSDWatchNotification || !bytes.Equal(event.Payload, payload) || event.Cookie != cookie || event.Source.ID != fresh.ID ||
		event.NotifierGlobalID != r.c.Snapshot().GlobalID || event.NotifyID == timedOut.NotifyID {
		t.Fatal("the notification in progress was not delivered after the RECONNECT", event, err)
	}
	if _, err := moved.Acknowledge(r.ctx, fresh.Epoch, event, ackData); err != nil {
		t.Fatal("acknowledge", err)
	}
	completed, stop := context.WithTimeout(r.ctx, 30*time.Second)
	done, err := pending.Next(completed)
	stop()
	if err != nil || done.ReturnCode != 0 || done.NotifyID != event.NotifyID || len(done.Replies) != 1 || done.Replies[0].GlobalID != gid || done.Replies[0].Cookie != cookie ||
		!bytes.Equal(done.Replies[0].Data, ackData) || len(done.Missed) != 0 {
		t.Fatal("notify completion after the RECONNECT", done, err)
	}
	t.Logf("notify tid %d (notify id %d) delivered after the RECONNECT (generation %d) and completed with one ACK", pendingReply.Transaction, done.NotifyID, reconnect.Operations[0].Watch.Generation)
	quiet, stop := context.WithTimeout(r.ctx, 3*time.Second)
	extra, err := moved.Next(quiet)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("another event after the delivered notification", extra, err)
	}

	if _, err := moved.Unwatch(r.ctx, fresh.Epoch); err != nil {
		t.Fatal("unwatch", err)
	}
	ro.Close()
	r.watchers("after the UNWATCH at the replacement", object, 0, gid, cookie)
	replaced := r.state(object).Query
	if replaced == nil {
		t.Fatal("no pg query under the replacement before the return")
	}
	r.primaryBack(object, route, outage, replaced.SameIntervalSince)
	r.watchers("at the returned primary", object, 0, gid, cookie)
}

// TestCephOSDErasureOptimizedK2M2SnapTrimFailoverIntegration: a self-managed
// snapshot removed while the optimized pool's PG is led by a replacement
// primary is trimmed only after the PG is clean again. A clone for the first
// snapshot id is made through the shard-0 primary; its OSD is killed, and
// with the PG active under the first parity shard's OSD with the
// primary-first pg_temp, a second clone is made through the replacement and
// the first snapshot is removed. The replacement must hold the removed id in
// its snap_trimq, not in purged_snaps, while the PG is not clean, and both
// clones stay listed in Go and native LIST_SNAPS: the trimmer needs a clean
// PG (src/osd/PrimaryLogPG.cc kick_snap_trim, NotTrimming::react). After the
// original primary returns and every PG is clean, the returned primary must
// trim it: only the second clone and the head remain, with their bytes and
// the head's user version, in Go and native reads.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2snaptrimfailover key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills the primary OSD; restart waits osd-ready clean, which does not prove no pg_temp
func TestCephOSDErasureOptimizedK2M2SnapTrimFailoverIntegration(t *testing.T) {
	r := startECEpochRun(t, 900*time.Second)
	const object = "ec-snaptrim-failover"
	first := bytes.Repeat([]byte("ec-trim-first\x00\xff"), 400)
	second := bytes.Repeat([]byte("ec-trim-second\x01"), 300)
	third := bytes.Repeat([]byte("ec-trim-third\xfe\x00"), 500)
	route, _ := r.healthy(object)
	r.write("first write", route, first, 1700013000, cephmsgr.OSDSnapContext{})
	s1 := r.allocateSnap()
	r.write("overwrite under the first snapshot", route, second, 1700013001, cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}})
	route, since := r.healthy(object)

	trim := func() copyPurgedTrimState {
		t.Helper()
		var result struct {
			SnapTrim copyPurgedTrimState `json:"snap_trim"`
		}
		nativePoolResult(t, r.ctx, "snaptrim", map[string]any{"slot": 1, "object": object, "objects": []string{object}}, &result)
		return result.SnapTrim
	}
	nativeClones := func(label string, s copyPurgedTrimState, want ...uint64) {
		t.Helper()
		listing := s.ListSnaps[object].listing(t, object)
		var got []uint64
		for _, c := range listing.Clones {
			if !c.ID.Head {
				got = append(got, c.ID.ID)
			}
		}
		if !slices.Equal(got, want) || len(listing.Clones) != len(want)+1 {
			t.Fatalf("%s: native listsnaps clones %v (%d entries); want %v and the head", label, got, len(listing.Clones), want)
		}
	}
	if s := trim(); copyPurgedPhase(s, s1) != "pruned" || s.State != "active+clean" {
		t.Fatalf("before the outage the first snapshot is %q in %+v", copyPurgedPhase(s, s1), s)
	}

	fresh, outage := r.replacePrimary(object, route, since)
	s2 := r.allocateSnap()
	if s2 <= s1 {
		t.Fatal("the second snapshot id is not newer", s1, s2)
	}
	written := r.write("overwrite under the second snapshot through the replacement", fresh, third, 1700013002, cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}})
	both := map[uint64][]byte{s1: first, s2: second, cephmsgr.OSDHeadSnapshot: third}
	checkECClones(t, "before the removal", r.readSnaps("before the removal", object, fresh, both, written.UserVersion).list, map[uint64]int{s1: len(first), s2: len(second)}, s2)

	removed, err := r.c.PoolOp(r.ctx, cephmsgr.PoolOpRequest{Pool: uint32(r.facts.pool), Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: s1})
	if err != nil || removed.Code != 0 {
		t.Fatal("remove the first snapshot", err, removed.Code)
	}
	lead := fresh.PGID + "s" + strconv.Itoa(int(fresh.Shard))
	var pending copyPurgedTrimState
	for attempt := 1; ; attempt++ {
		pending = trim()
		if copyPurgedPhase(pending, s1) == "pending" {
			break
		}
		if attempt == shardOutageObservations {
			t.Fatalf("the removed snapshot never reached the replacement's snap_trimq: %q in %+v", copyPurgedPhase(pending, s1), pending)
		}
		time.Sleep(shardOutagePause)
	}
	if pending.PGID != lead || slices.Contains(pgStates(pending.State), "clean") || !slices.Contains(pgStates(pending.State), "undersized") {
		t.Fatalf("the removed snapshot is pending in %s (%s), not on the replacement's undersized PG %s", pending.PGID, pending.State, lead)
	}
	nativeClones("pending under the replacement", pending, s1, s2)
	list := r.readSnaps("pending under the replacement", object, fresh, map[uint64][]byte{s2: second, cephmsgr.OSDHeadSnapshot: third}, written.UserVersion).list
	checkECClones(t, "pending under the replacement", list, map[uint64]int{s1: len(first), s2: len(second)}, s2)
	if again := trim(); copyPurgedPhase(again, s1) != "pending" || slices.Contains(pgStates(again.State), "clean") {
		t.Fatalf("the removed snapshot is %q in %+v after the reads under the replacement", copyPurgedPhase(again, s1), again)
	}
	t.Logf("snapshot %d removed under osd.%d (shard %d): pending in %s %s at epoch %d, trim queue %v, purged %v",
		s1, fresh.ID, fresh.Shard, pending.PGID, pending.State, pending.Epoch, pending.SnapTrimQueue, pending.PurgedSnaps)

	replaced := r.state(object).Query
	if replaced == nil {
		t.Fatal("no pg query under the replacement before the return")
	}
	back, _ := r.primaryBack(object, route, outage, replaced.SameIntervalSince)
	var trimmed copyPurgedTrimState
	for attempt := 1; ; attempt++ {
		trimmed = trim()
		if phase := copyPurgedPhase(trimmed, s1); phase == "purged" || phase == "unqueued" || phase == "pruned" {
			break
		}
		if attempt == shardOutageObservations {
			t.Fatalf("the returned primary never trimmed the removed snapshot: %q in %+v", copyPurgedPhase(trimmed, s1), trimmed)
		}
		time.Sleep(shardOutagePause)
	}
	if trimmed.PGID != back.PGID+"s0" || trimmed.State != "active+clean" {
		t.Fatalf("the removed snapshot was trimmed in %s (%s), not on the returned primary's clean PG", trimmed.PGID, trimmed.State)
	}
	nativeClones("after the trim", trimmed, s2)
	after := r.readSnaps("after the trim", object, back, map[uint64][]byte{s2: second, cephmsgr.OSDHeadSnapshot: third}, written.UserVersion)
	checkECClones(t, "after the trim", after.list, map[uint64]int{s2: len(second)}, s2)
	t.Logf("snapshot %d trimmed by osd.%d (%s, %s at epoch %d): purged %v, queue %v; clone %d and the head remain",
		s1, back.ID, trimmed.PGID, copyPurgedPhase(trimmed, s1), trimmed.Epoch, trimmed.PurgedSnaps, trimmed.RemovedSnapQueue, s2)
}
