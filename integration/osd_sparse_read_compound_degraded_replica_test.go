package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephOSDSparseReadCompoundDegradedReplicaIntegration sends the
// SPARSE_READ and READ replica-read compound (BALANCE_READS and
// LOCALIZE_READS, at the head and at a pool snapshot) to a replica that
// rejoined the acting set missing the object, while the norecover and
// nobackfill flags hold its recovery, and to the other members.
//
// An owned three-replica pool gets a subject object written with holes and
// a pool snapshot, then one replica's OSD is killed and the subject's head
// is rewritten, which leaves its clone and head missing on that OSD; a
// companion object of the same PG is not written while it is away. After the
// OSD rejoins, the native pg query of the primary must report the PG
// degraded and that peer's last_complete behind its last_update. Until the
// primary's read lease reaches the rejoined replica, it answers every replica
// read with -EAGAIN (do_op's check_laggy), so the companion is asked for
// until it is served, and each subject bounce below is bracketed by the
// companion served by the same replica. The
// replica then answers both compounds of the subject with -EAGAIN before any
// operation runs: the pinned do_op (PrimaryLogPG.cc at v20.2.4) replies
// -EAGAIN to a replica read of an unreadable head before
// find_object_context, so a read snap does not exempt the clone. That is a
// known server code with untouched per-op results, no output, retry attempt
// 0 and the next transaction ID on the same connection: the client neither
// resends nor routes it elsewhere. A Go write of another object in the same
// PG after the rejoin rules out the unstable-write bounce of
// TestCephOSDSparseReadCompoundReplicaReadIntegration. The primary and the other
// replica serve the subject; each served compound's per-op unions, results,
// extent maps, bytes and user version must equal the native rados mapext
// map and bytes read through the primary at the same snapshot, and each
// SPARSE_READ the same operation sent alone to the same member. After the
// flags are cleared and the fixture is clean the primary reports the PG
// active+clean, and after the next write in the PG every member serves
// both compounds of the subject with the native bytes. Native heads and the
// clone are unchanged by the reads.
//
// Not verified: a replica caught while a push of the object is in flight
// (no stock control holds a push, so that window is a timing race), a
// backfill or async-recovery target (not in the acting set), EC, and
// MemStore.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdsparsereadcompounddegradedreplica key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdsparsereadcompounddegradedreplica key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated kills and restarts a replica OSD, sets norecover/nobackfill; restart waits osd-ready clean, no pg_temp proof
func TestCephOSDSparseReadCompoundDegradedReplicaIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("SPARSE_READ compound acceptance requires a replicated pool; EC is outside this subset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	const (
		subject  = "compound-degraded-subject"
		snapName = "compound-degraded-snap"
	)
	first := poolPlace(t, ctx, 1, subject, "")
	if first.PoolType != 1 || first.ID != first.PrimaryID || len(first.Acting) != 3 {
		t.Fatal("degraded compound target is not a healthy three-replica placement", first)
	}
	// The companion and the fillers share the subject's PG.
	names := []string{}
	for i := 0; len(names) < 8; i++ {
		if i == 600 {
			t.Fatal("too few object names map to the subject's PG")
		}
		name := fmt.Sprintf("compound-degraded-mate-%d", i)
		if poolPlace(t, ctx, 1, name, "").PGID == first.PGID {
			names = append(names, name)
		}
	}
	companion, fillers := names[0], names[1:]
	objects := []string{subject, companion}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	connect := func(route osdFixtureOracle) *cephmsgr.OSDConnection {
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		return connections[route.ID]
	}
	// route is the native route of name to member id, or to the primary.
	route := func(name string, id *int32) osdFixtureOracle {
		r := poolPlaceOn(t, ctx, 1, name, "", id)
		if r.PGID != first.PGID || r.Address == "" || id != nil && r.ID != *id {
			t.Fatal("native member routing differs", r)
		}
		return r
	}
	request := func(r osdFixtureOracle, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		q := pools.request(r, ops...)
		if q.Placement != nil {
			q.Placement.Primary = r.PrimaryID
		}
		if configure != nil {
			configure(&q)
		}
		return q
	}
	tick := uint32(0)
	write := func(name string, op cephmsgr.OSDOperation) {
		t.Helper()
		tick++
		r := route(name, nil)
		reply, err := connect(r).Request(ctx, request(r, func(q *cephmsgr.OSDRequest) {
			q.ModificationTime = cephmsgr.OSDTime{Seconds: 1700012000 + tick, Nanoseconds: tick}
		}, op))
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("owned degraded compound write", name, reply.Code, err)
		}
	}
	data := func(offset uint64, label byte, n int) cephmsgr.OSDOperation {
		b := bytes.Repeat([]byte{label, 0, 255, byte(offset >> 12)}, (n+3)/4)[:n]
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(n), Data: b}
	}
	fill := 0
	fillPG := func() {
		t.Helper()
		write(fillers[fill%len(fillers)], cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 6, Data: []byte("filler")})
		fill++
	}
	for _, offset := range []uint64{8192, 0x11000, 0x20000} {
		write(subject, data(offset, 'p', 4))
	}
	write(companion, data(0, 'u', 64))
	write(companion, data(0x9000, 'u', 16))
	snap := nativePool(t, ctx, "snapshot", map[string]any{"slot": 1, "name": snapName, "action": "create"})
	if snap.SnapID == 0 {
		t.Fatal("native pool snapshot id missing")
	}

	victim := first.Acting[1]
	if victim == first.ID {
		victim = first.Acting[2]
	}
	name := strconv.Itoa(int(victim))
	// Recorded and registered first: a set or kill whose wait failed may still
	// run, and the restore runs after it.
	flagsSet, started := true, true
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 200*time.Second)
		defer stop()
		if !started {
			testcluster.ControlDaemon(restore, dir, "start", "osd", name)
		}
		if flagsSet {
			nativePool(t, restore, "osdflags", map[string]any{"action": "unset"})
		}
		waitOSDFixtureClean(t, restore, base)
	}()
	if flags := nativePool(t, ctx, "osdflags", map[string]any{"action": "set"}); !strings.Contains(strings.Join(flags.OSDMapFlags, ","), "norecover") {
		t.Fatal("native norecover flag not set", flags.OSDMapFlags)
	}
	started = false
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for slicesContains(route(subject, nil).Acting, victim) {
		if time.Now().After(deadline) {
			t.Fatal("victim never left the acting set")
		}
		time.Sleep(2 * time.Second)
	}
	// The head is rewritten and extended while the victim is away; its first
	// write after the snapshot makes the clone.
	write(subject, data(0, 'h', 16))
	write(subject, data(0x10000, 'h', 64))
	write(subject, data(0x24000, 'h', 4))
	fillPG()
	if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
		t.Fatal(err)
	}
	started = true
	for !slicesContains(route(subject, nil).Acting, victim) {
		if time.Now().After(deadline.Add(120 * time.Second)) {
			t.Fatal("victim never rejoined the acting set")
		}
		time.Sleep(2 * time.Second)
	}

	sr := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: offset, Length: length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: uint32(length) + 4096}}
	}
	rd := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length}
	}
	librbd := []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0x10000, 0x1000), sr(0x11000, 0x20000)}
	flags := []struct {
		name string
		set  func(*cephmsgr.OSDRequest)
	}{
		{"balance", func(r *cephmsgr.OSDRequest) { r.BalanceReads = true }},
		{"localize", func(r *cephmsgr.OSDRequest) { r.LocalizeReads = true }},
	}
	at := func(flag func(*cephmsgr.OSDRequest), readSnap uint64) func(*cephmsgr.OSDRequest) {
		return func(r *cephmsgr.OSDRequest) { flag(r); r.ReadSnapshot = readSnap }
	}
	// send targets one member with a bounded wait: a request the rejoining
	// replica holds while it peers ends as an unknown outcome there.
	send := func(r osdFixtureOracle, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		callCtx, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		return connect(r).Request(callCtx, request(r, configure, ops...))
	}
	// bounced requires -EAGAIN before any operation ran, without a resend,
	// and the next transaction ID on the same connection for the next request.
	bounced := func(t *testing.T, r osdFixtureOracle, configure func(*cephmsgr.OSDRequest), why string) {
		t.Helper()
		reply, err := send(r, configure, librbd...)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -11 || reply.Code != -11 || reply.RetryAttempt != 0 || len(reply.RawData) != 0 ||
			len(reply.Operations) != len(librbd) || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != subject {
			t.Fatalf("osd.%d %s: expected a known -EAGAIN without a resend, got code %d: %v", r.ID, why, reply.Code, err)
		}
		for i, got := range reply.Operations {
			if got.Code != librbd[i].Code || got.Result != 0 || got.PayloadLength != 0 || got.Offset != librbd[i].Offset || got.Length != librbd[i].Length || got.SparseRead.Extents != nil {
				t.Fatal("a bounced compound claimed an executed operation", r.ID, i, got)
			}
		}
		probe, err := send(r, flags[0].set, rd(0, 1))
		if probe.Transaction != reply.Transaction+1 || probe.Submission.Transaction != probe.Transaction {
			t.Fatal("the bounce consumed another transaction ID: a hidden resend", r.ID, reply.Transaction, probe.Transaction, err)
		}
	}
	// served compares one compound served by one member with the native map.
	served := func(t *testing.T, r osdFixtureOracle, object string, configure func(*cephmsgr.OSDRequest), native poolSparseMap, ops []cephmsgr.OSDOperation) {
		t.Helper()
		m := native.SparseMap
		reply, err := send(r, configure, ops...)
		if err != nil || reply.Code != 0 || len(reply.Operations) != len(ops) || reply.RetryAttempt != 0 || reply.Object != object {
			t.Fatal("Go replica-read SPARSE_READ compound failed", r.ID, object, reply.Code, err)
		}
		if reply.UserVersion != m.State.Version || reply.Transaction != reply.Submission.Transaction {
			t.Fatal("compound version/identity differs from independent native stat", r.ID, reply.UserVersion, m.State.Version)
		}
		var raw []byte
		for i, op := range ops {
			got := reply.Operations[i]
			length, extents, want := sparseReadCompoundExpect(op, m.Full, m.State.Data)
			if got.Code != op.Code || got.Result != 0 || got.Offset != op.Offset || got.Length != length || got.PayloadLength != uint32(len(got.Data)) {
				t.Fatal("per-op union/result differs from the native model", r.ID, object, i, got.Length, length, got.Result)
			}
			if op.Code == cephmsgr.OSDRead {
				if !bytes.Equal(got.Data, want) {
					t.Fatal("READ bytes differ from native bytes", r.ID, object, i)
				}
			} else {
				if !reflect.DeepEqual(got.SparseRead.Extents, extents) || !bytes.Equal(got.SparseRead.Data, want) {
					t.Fatal("SPARSE_READ map/bytes differ from the native map and bytes", r.ID, object, i, got.SparseRead.Extents, extents)
				}
				alone, err := send(r, configure, op)
				if err != nil || !reflect.DeepEqual(alone.Operations[0].SparseRead, got.SparseRead) || !bytes.Equal(alone.Operations[0].Data, got.Data) {
					t.Fatal("compound SPARSE_READ differs from standalone on the same member", r.ID, object, i, err)
				}
			}
			raw = append(raw, got.Data...)
		}
		if !bytes.Equal(reply.RawData, raw) {
			t.Fatal("per-op payloads do not exactly partition the raw reply data")
		}
	}
	// peerComplete reports, from one native pg query of the primary, whether
	// the victim's peer_info has last_complete equal to last_update, and the
	// PG state (tools/osd_pool_mode_pgpeers_oracle.py). peer_info is the
	// peer's state as the primary learned it in peering.
	peerComplete := func(t *testing.T) (bool, string) {
		t.Helper()
		var seen struct {
			Peers struct {
				State  string  `json:"state"`
				Acting []int32 `json:"acting"`
				Peers  []struct {
					Peer         string `json:"peer"`
					LastUpdate   string `json:"last_update"`
					LastComplete string `json:"last_complete"`
				} `json:"peers"`
			} `json:"pg_peers"`
		}
		primary := route(subject, nil)
		nativePoolResult(t, ctx, "pgpeers", map[string]any{"pgid": primary.PGID, "osd": primary.ID}, &seen)
		for _, peer := range seen.Peers.Peers {
			if peer.Peer == name {
				return peer.LastComplete == peer.LastUpdate, seen.Peers.State
			}
		}
		t.Fatal("native pg query lists no peer_info of the victim", name, seen.Peers.Peers)
		return false, ""
	}

	victimRoute := route(subject, &victim)
	// A write in the PG after the rejoin, replicated to the victim too, rules
	// out the unstable-write cause.
	fillPG()
	if complete, state := peerComplete(t); complete || !strings.Contains(state, "degraded") || !strings.HasPrefix(state, "active") {
		t.Fatalf("native PG state %q does not show the victim missing objects", state)
	}
	models := map[string]poolSparseMap{}
	for _, object := range objects {
		models[object] = poolSparseMapAt(t, ctx, object, "")
		models[object+"@snap"] = poolSparseMapAt(t, ctx, object, snapName)
	}
	if reflect.DeepEqual(models[subject].SparseMap.Full, models[subject+"@snap"].SparseMap.Full) || models[subject+"@snap"].SparseMap.SnapID != snap.SnapID {
		t.Fatal("native clone is not distinct from the rewritten head")
	}
	// At the snapshot the companion, unchanged since, resolves to its head.
	companionOps := []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0, 16), sr(0x8ffe, 8)}
	companionRoute := route(companion, &victim)
	// A rejoined replica answers every replica read with -EAGAIN until the
	// primary's read lease reaches it (do_op's check_laggy precedes the
	// missing-object check), so the caller asks for the companion, which the
	// replica holds, until it is served: from then on the replica's lease and
	// committed point serve reads, and only a missing object bounces. Reads
	// are idempotent; each bounce is a known -EAGAIN.
	for i := 0; ; i++ {
		reply, err := send(companionRoute, flags[0].set, companionOps...)
		if err == nil {
			break
		}
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || reply.Code != -11 || i == 30 {
			t.Fatalf("rejoined replica osd.%d never served the companion: code %d %v", victim, reply.Code, err)
		}
		time.Sleep(time.Second)
	}
	t.Run("missing-object-bounces-between-served-reads", func(t *testing.T) {
		// Each bounce is bracketed by the companion served by the same
		// replica, so the lease and committed point held around it.
		for _, flag := range flags {
			served(t, companionRoute, companion, at(flag.set, 0), models[companion], companionOps)
			bounced(t, victimRoute, at(flag.set, 0), "head missing while recovery is held "+flag.name)
			bounced(t, victimRoute, at(flag.set, snap.SnapID), "clone and head missing while recovery is held "+flag.name)
			served(t, companionRoute, companion, at(flag.set, snap.SnapID), models[companion+"@snap"], companionOps)
		}
	})
	t.Run("other-members-serve-the-subject", func(t *testing.T) {
		for _, id := range first.Acting {
			if id == victim {
				continue
			}
			id := id
			r := route(subject, &id)
			for _, flag := range flags {
				served(t, r, subject, at(flag.set, 0), models[subject], librbd)
				served(t, r, subject, at(flag.set, snap.SnapID), models[subject+"@snap"], librbd)
			}
		}
	})
	if t.Failed() {
		return
	}
	nativePool(t, ctx, "osdflags", map[string]any{"action": "unset"})
	flagsSet = false
	waitOSDFixtureClean(t, ctx, base)
	// The primary's peer_info is the peers' state as of peering and is not
	// what recovery reports, so only the PG state is read here.
	if _, state := peerComplete(t); state != "active+clean" {
		t.Fatalf("native PG state %q after recovery is not active+clean", state)
	}
	// Recovery starts a new interval; the recovered replica may bounce until
	// a write in it. Reads are idempotent, so the caller asks again after one.
	fillPG()
	for i := 0; ; i++ {
		reply, err := send(victimRoute, flags[0].set, librbd...)
		if err == nil {
			break
		}
		if reply.Code != -11 || i == 10 {
			t.Fatalf("recovered replica osd.%d: code %d %v", victim, reply.Code, err)
		}
		t.Logf("recovered replica osd.%d bounced again (attempt %d)", victim, i)
		time.Sleep(2 * time.Second)
	}
	t.Run("recovered-replica-serves", func(t *testing.T) {
		for _, id := range first.Acting {
			id := id
			r := route(subject, &id)
			for _, flag := range flags {
				served(t, r, subject, at(flag.set, 0), models[subject], librbd)
				served(t, r, subject, at(flag.set, snap.SnapID), models[subject+"@snap"], librbd)
			}
		}
	})
	for _, object := range objects {
		if after := poolSparseMapAt(t, ctx, object, ""); !reflect.DeepEqual(after.SparseMap.State, models[object].SparseMap.State) || !reflect.DeepEqual(after.SparseMap.Full, models[object].SparseMap.Full) {
			t.Fatal("replica reads changed a native head", object)
		}
	}
	if after := poolSparseMapAt(t, ctx, subject, snapName); !reflect.DeepEqual(after.SparseMap, models[subject+"@snap"].SparseMap) {
		t.Fatal("replica reads changed the native clone")
	}
	t.Logf("osd.%d bounced the subject's head and snapshot compounds with -EAGAIN while missing it and served the companion; other members served the subject; after recovery every member served it", victim)
}
