package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephOSDSparseReadCompoundReplicaReadIntegration sends the SPARSE_READ
// and READ compound with BALANCE_READS or LOCALIZE_READS to every member of
// the acting set, at the head and at a pool snapshot, as librbd sends its
// object read under rbd_read_from_replica_policy or rbd_balance_snap_reads
// and rbd_localize_snap_reads (ceph/ceph/blob/v20.2.4/src/librbd/io/ObjectRequest.cc
// ObjectReadRequest::read_object and src/librbd/ImageCtx.cc get_read_flags).
// An owned replicated pool gets one native pool snapshot; Go writes before
// and after it leave a clone with holes under a rewritten and extended head,
// an object unchanged since the snapshot and one created after it.
//
// A replica serves the vector only while no log entry of the object's head is
// newer than its pg_committed_to (pinned PrimaryLogPG.cc do_op and
// PeeringState.cc can_serve_replica_read; PGLog.h has_write_since compares
// heads, so a clone read is no exception). The primary sends that point with
// each later write in the PG, so a Go write of another object in the same PG,
// acknowledged after every subject write, is the barrier before the replica
// cells: once it is acknowledged every replica has applied its log entry and
// with it the newer committed point.
//
// Each cell compares every per-op union, result, extent map and byte with the
// intersection of the pinned rados mapext full map at the same snapshot with
// the operation's range, plus native bytes, stat and user version (native
// observation through the primary; the native client does not execute the
// compound). Every SPARSE_READ must equal the same operation sent alone with
// the same flag to the same member. At the snapshot the clone is read, the
// unchanged object and a snap id above every allocated one resolve to the
// head, and the object created after the snapshot is -ENOENT before any
// operation runs, with every per-op result untouched.
//
// Bounce: after a further Go write of the clone's head, which is then the
// last write of its PG, every replica answers both the head and the snapshot
// compound with -EAGAIN before any operation runs, a known server code with
// untouched per-op results and no resend, while the primary serves the new
// head with the flag. After the next write in the PG every replica serves
// the new head and the unchanged clone. A compound without a flag at a
// replica is refused locally on the replicated-PG profiles, whose placement
// names the primary, and on the legacy profile is dropped unanswered by the
// replica (an unknown outcome at the bounded wait). Refused shapes with a
// flag stay unsent with consecutive transaction IDs, and native heads and
// the clone are unchanged afterwards.
//
// The rows are the two cells of the three-replica v8 request/v6 reply
// profile, a CRC/AES IPv4 host and a secure AES256K IPv6 container; the
// legacy three-replica BlueStore suites run it too. The request-v9 fixture
// admits one OSD only, so v9 replica reads have native codec vectors
// (TestSparseReadCompoundReplicaMatches12FreshNativeRequests) and no daemon
// evidence. A replica that rejoined missing the object is
// TestCephOSDSparseReadCompoundDegradedReplicaIntegration's. Not verified:
// EC, MemStore allocation maps, a replica during a push of the object, the
// pool option pct_update_delay, faults and resubmission; no stock daemon
// answers this compound with a later operation's error
// (TestCephOSDReadCompoundLaterErrorIntegration).
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdsparsereadcompoundreplicaread key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdsparsereadcompoundreplicaread key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:shareable pool snapshot, writes and replica reads in one owned pool only; checked owned-pool cleanup removes it
func TestCephOSDSparseReadCompoundReplicaReadIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" {
		t.Skip("requires three replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("SPARSE_READ compound acceptance requires a replicated pool; EC is outside this subset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const (
		clone, unchanged, created = "compound-replica-clone", "compound-replica-unchanged", "compound-replica-created"
		snapName                  = "compound-replica-snap"
	)
	objects := []string{clone, unchanged, created}
	// routes[name][osd] is the native route of name to one acting member.
	routes := map[string]map[int32]osdFixtureOracle{}
	primaries := map[string]osdFixtureOracle{}
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
	place := func() {
		for _, name := range objects {
			primary := poolPlace(t, ctx, 1, name, "")
			if primary.PoolType != 1 || primary.Shard != -1 || primary.ID != primary.PrimaryID || len(primary.Acting) != 3 {
				t.Fatal("replica compound target is not a healthy three-replica placement", primary)
			}
			primaries[name] = primary
			routes[name] = map[int32]osdFixtureOracle{}
			for _, id := range primary.Acting {
				id := id
				route := poolPlaceOn(t, ctx, 1, name, "", &id)
				if route.ID != id || route.PrimaryID != primary.ID || route.PGID != primary.PGID || route.Address == "" {
					t.Fatal("native member routing differs", route)
				}
				routes[name][id] = route
				connect(route)
			}
		}
	}
	place()
	// request targets one member; the placement still names the PG's primary.
	request := func(route osdFixtureOracle, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		r := pools.request(route, ops...)
		if r.Placement != nil {
			r.Placement.Primary = route.PrimaryID
		}
		if configure != nil {
			configure(&r)
		}
		return r
	}
	send := func(route osdFixtureOracle, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		return connect(route).Request(ctx, request(route, configure, ops...))
	}
	tick := uint32(0)
	write := func(route osdFixtureOracle, op cephmsgr.OSDOperation) {
		t.Helper()
		tick++
		reply, err := send(route, func(r *cephmsgr.OSDRequest) {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700011000 + tick, Nanoseconds: tick}
		}, op)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("owned replica compound write", route.Name, reply.Code, err)
		}
	}
	data := func(offset uint64, label byte, n int) cephmsgr.OSDOperation {
		b := bytes.Repeat([]byte{label, 0, 255, byte(offset >> 12)}, (n+3)/4)[:n]
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(n), Data: b}
	}
	// Before the snapshot: holes between three allocated ranges on the clone
	// object, a dense head and one range on the unchanged object.
	for _, offset := range []uint64{8192, 0x11000, 0x20000} {
		write(primaries[clone], data(offset, 'p', 4))
	}
	write(primaries[unchanged], data(0, 'u', 64))
	write(primaries[unchanged], data(0x9000, 'u', 16))
	snap := nativePool(t, ctx, "snapshot", map[string]any{"slot": 1, "name": snapName, "action": "create"})
	if snap.SnapID == 0 {
		t.Fatal("native pool snapshot id missing")
	}
	// Routes read after the snapshot carry a map epoch that includes it.
	place()
	write(primaries[clone], data(0, 'h', 16))
	write(primaries[clone], data(0x10000, 'h', 64))
	write(primaries[clone], data(0x24000, 'h', 4))
	write(primaries[created], data(0, 'c', 32))

	// fillers[pgid] is an owned object of that PG; its write after every
	// subject write is the replicas' committed-point barrier.
	fillers := map[string]osdFixtureOracle{}
	fill := 0
	fillPG := func(t *testing.T, pgid string) {
		t.Helper()
		if fillers[pgid].Name == "" {
			for ; ; fill++ {
				if fill == 400 {
					t.Fatal("no further object name maps to PG", pgid)
				}
				name := fmt.Sprintf("compound-replica-filler-%d", fill)
				if r := poolPlace(t, ctx, 1, name, ""); r.PGID == pgid {
					fillers[pgid] = r
					fill++
					break
				}
			}
		}
		tick++
		reply, err := send(fillers[pgid], func(r *cephmsgr.OSDRequest) {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700011000 + tick}
		}, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 6, Data: []byte("filler")})
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("PG committed-point filler write", pgid, reply.Code, err)
		}
	}
	settle := func(t *testing.T) {
		t.Helper()
		done := map[string]bool{}
		for _, name := range objects {
			if pgid := primaries[name].PGID; !done[pgid] {
				fillPG(t, pgid)
				done[pgid] = true
			}
		}
	}
	settle(t)

	sr := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: offset, Length: length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: uint32(length) + 4096}}
	}
	rd := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length}
	}
	hint := func(op cephmsgr.OSDOperation, flags uint32) cephmsgr.OSDOperation {
		op.Flags = cephmsgr.OSDOperationFlags(flags)
		return op
	}
	const (
		random, sequential, willNeed, dontNeed, noCache = 0x4, 0x8, 0x10, 0x20, 0x40
	)
	flags := []struct {
		name string
		set  func(*cephmsgr.OSDRequest)
	}{
		{"balance", func(r *cephmsgr.OSDRequest) { r.BalanceReads = true }},
		{"localize", func(r *cephmsgr.OSDRequest) { r.LocalizeReads = true }},
	}
	librbd := []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0x10000, 0x1000), hint(sr(0x11000, 0x20000), sequential)}
	fadvise := []cephmsgr.OSDOperation{hint(sr(0, 0x9000), random), hint(rd(4096, 4096), noCache), hint(sr(0x10ffe, 8), dontNeed), hint(rd(0x20000, 16), willNeed)}
	type cell struct {
		name, object string
		snap         uint64
		// observe names the native observation the snap id resolves to: the
		// pool snapshot, or the head.
		observe string
		ops     []cephmsgr.OSDOperation
	}
	cells := []cell{
		{"head-librbd-mixed", clone, 0, "", librbd},
		{"head-fadvise-each", clone, 0, "", fadvise},
		{"head-unchanged", unchanged, 0, "", []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0, 16), sr(0x8ffe, 8)}},
		{"snap-clone-librbd-mixed", clone, snap.SnapID, snapName, librbd},
		{"snap-clone-size-clips", clone, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0x20000, 0x10000), rd(0x20002, 100), sr(0x24000, 16), rd(0x24000, 4)}},
		{"snap-unchanged-resolves-head", unchanged, snap.SnapID, snapName, []cephmsgr.OSDOperation{sr(0, 0x10000), rd(0, 16), sr(0x8ffe, 8)}},
		{"snap-above-seq-resolves-head", clone, snap.SnapID + 1000, "", librbd},
	}
	// check sends one cell with one flag to one member and compares it with
	// the native model.
	check := func(t *testing.T, tc cell, route osdFixtureOracle, flag func(*cephmsgr.OSDRequest), native poolSparseMap) {
		t.Helper()
		at := func(r *cephmsgr.OSDRequest) {
			flag(r)
			r.ReadSnapshot = tc.snap
		}
		m := native.SparseMap
		reply, err := send(route, at, tc.ops...)
		if err != nil || reply.Code != 0 || len(reply.Operations) != len(tc.ops) {
			t.Fatal("Go replica-read SPARSE_READ compound failed", route.ID, reply.Code, err)
		}
		if reply.UserVersion != m.State.Version || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != tc.object || reply.RetryAttempt != 0 {
			t.Fatal("compound version/identity differs from independent native stat", route.ID, reply.UserVersion, m.State.Version)
		}
		var raw []byte
		for i, op := range tc.ops {
			got := reply.Operations[i]
			length, extents, want := sparseReadCompoundExpect(op, m.Full, m.State.Data)
			if got.Code != op.Code || got.Flags != uint32(op.Flags) || got.Result != 0 || got.Offset != op.Offset || got.Length != length || got.PayloadLength != uint32(len(got.Data)) {
				t.Fatal("per-op union/result differs from the native model", route.ID, i, got.Code, got.Length, length, got.Result)
			}
			if op.Code == cephmsgr.OSDRead {
				if !bytes.Equal(got.Data, want) {
					t.Fatal("READ bytes differ from native bytes", route.ID, i)
				}
			} else {
				if !reflect.DeepEqual(got.SparseRead.Extents, extents) || !bytes.Equal(got.SparseRead.Data, want) || uint32(len(got.Data)) > op.SparseRead.MaxOutputBytes {
					t.Fatal("SPARSE_READ map/bytes differ from the native map and bytes", route.ID, i, got.SparseRead.Extents, extents)
				}
				alone, err := send(route, at, op)
				if err != nil || !reflect.DeepEqual(alone.Operations[0].SparseRead, got.SparseRead) || !bytes.Equal(alone.Operations[0].Data, got.Data) {
					t.Fatal("compound SPARSE_READ differs from standalone on the same member", route.ID, i, err)
				}
			}
			raw = append(raw, got.Data...)
		}
		if !bytes.Equal(reply.RawData, raw) {
			t.Fatal("per-op payloads do not exactly partition the raw reply data")
		}
	}
	// bounced requires a replica's -EAGAIN before any operation ran.
	bounced := func(t *testing.T, tc cell, route osdFixtureOracle, flag func(*cephmsgr.OSDRequest)) {
		t.Helper()
		reply, err := send(route, func(r *cephmsgr.OSDRequest) { flag(r); r.ReadSnapshot = tc.snap }, tc.ops...)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -11 || reply.Code != -11 || reply.RetryAttempt != 0 || len(reply.RawData) != 0 || len(reply.Operations) != len(tc.ops) || reply.Submission.Transaction == 0 {
			t.Fatalf("osd.%d %s: expected a known -EAGAIN without a resend, got code %d: %v", route.ID, tc.name, reply.Code, err)
		}
		for i, got := range reply.Operations {
			if got.Result != 0 || got.PayloadLength != 0 || got.Offset != tc.ops[i].Offset || got.Length != tc.ops[i].Length || got.SparseRead.Extents != nil {
				t.Fatal("a bounced compound claimed an executed operation", route.ID, i, got)
			}
		}
	}
	models := map[string]poolSparseMap{}
	model := func(object, observe string) poolSparseMap {
		key := object + "@" + observe
		if _, ok := models[key]; !ok {
			models[key] = poolSparseMapAt(t, ctx, object, observe)
		}
		return models[key]
	}
	headBefore := map[string]poolSparseMap{}
	for _, name := range objects {
		headBefore[name] = model(name, "")
	}
	cloneMap := model(clone, snapName)
	if cloneMap.SparseMap.Code != 0 || reflect.DeepEqual(cloneMap.SparseMap.Full, headBefore[clone].SparseMap.Full) || bytes.Equal(cloneMap.SparseMap.State.Data, headBefore[clone].SparseMap.State.Data) || cloneMap.SparseMap.SnapID != snap.SnapID {
		t.Fatal("native clone is not distinct from the rewritten head", cloneMap.SparseMap.Full, headBefore[clone].SparseMap.Full)
	}
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			native := model(tc.object, tc.observe)
			if native.SparseMap.Code != 0 || len(native.SparseMap.Full) == 0 {
				t.Fatal("independent native allocation map missing", native.SparseMap.Code)
			}
			for _, id := range primaries[tc.object].Acting {
				for _, flag := range flags {
					check(t, tc, routes[tc.object][id], flag.set, native)
				}
			}
			t.Logf("%s: snap %d, served by acting %v with both flags; native map %v, version %d; scope=native full-object mapext and read through the primary, per-op intersection model", tc.name, tc.snap, primaries[tc.object].Acting, native.SparseMap.Full, native.SparseMap.State.Version)
		})
	}
	t.Run("created-after-snapshot-ambiguous", func(t *testing.T) {
		native := poolSparseMapAt(t, ctx, created, snapName).SparseMap
		if native.Code != -2 || headBefore[created].SparseMap.Code != 0 {
			t.Fatal("independent native absence at the snapshot not observed", native.Code)
		}
		ops := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8), sr(8, 8)}
		for _, id := range primaries[created].Acting {
			for _, flag := range flags {
				route := routes[created][id]
				reply, err := send(route, func(r *cephmsgr.OSDRequest) { flag.set(r); r.ReadSnapshot = snap.SnapID }, ops...)
				var remote *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if errors.As(err, &unknown) || !errors.As(err, &remote) || remote.Code != -2 || reply.Code != -2 || len(reply.RawData) != 0 || len(reply.Operations) != len(ops) {
					t.Fatal("snapshot-absent replica compound differs from native ENOENT", id, flag.name, reply.Code, err)
				}
				for i, got := range reply.Operations {
					if got.Result != 0 || got.PayloadLength != 0 || got.Offset != ops[i].Offset || got.Length != ops[i].Length || got.SparseRead.Extents != nil {
						t.Fatal("early aggregate error claimed an executed prefix", id, i, got)
					}
				}
				head, err := send(route, flag.set, ops...)
				if err != nil || head.Code != 0 {
					t.Fatal("signed snapshot error poisoned the session or the head read", id, head.Code, err)
				}
			}
		}
	})
	t.Run("unstable-head-bounces-head-and-snapshot", func(t *testing.T) {
		// The clone's head gets one more write, which is then the last of its
		// PG: no replica's committed point covers it.
		write(primaries[clone], data(0x30000, 'b', 8))
		after := poolSparseMapAt(t, ctx, clone, "")
		if reflect.DeepEqual(after.SparseMap.State, headBefore[clone].SparseMap.State) {
			t.Fatal("native head did not change with the bouncing write")
		}
		head := cell{"bounce-head", clone, 0, "", librbd}
		clipped := cell{"bounce-head-eof", clone, 0, "", []cephmsgr.OSDOperation{sr(0x24000, 0x10000), rd(0x30000, 16)}}
		atSnap := cell{"bounce-snap-clone", clone, snap.SnapID, snapName, librbd}
		for _, id := range primaries[clone].Acting {
			route := routes[clone][id]
			for _, flag := range flags {
				if id == primaries[clone].ID {
					// The primary never checks the committed point.
					check(t, head, route, flag.set, after)
					check(t, clipped, route, flag.set, after)
					check(t, atSnap, route, flag.set, cloneMap)
					continue
				}
				bounced(t, head, route, flag.set)
				bounced(t, atSnap, route, flag.set)
			}
		}
		// The next write in the PG carries the committed point past it.
		fillPG(t, primaries[clone].PGID)
		for _, id := range primaries[clone].Acting {
			for _, flag := range flags {
				check(t, head, routes[clone][id], flag.set, after)
				check(t, clipped, routes[clone][id], flag.set, after)
				check(t, atSnap, routes[clone][id], flag.set, cloneMap)
			}
		}
		headBefore[clone] = after
		t.Logf("replicas of %v bounced head and snapshot compounds with -EAGAIN until the next write of PG %s; primary osd.%d served them", primaries[clone].Acting, primaries[clone].PGID, primaries[clone].ID)
	})
	var replica osdFixtureOracle
	for _, id := range primaries[clone].Acting {
		if id != primaries[clone].ID {
			replica = routes[clone][id]
			break
		}
	}
	t.Run("no-flag-at-a-replica", func(t *testing.T) {
		// A separately authenticated client: OpenOSD reuses one handle per
		// OSD on a client, and an unanswered request must not touch the
		// connections of the cells above.
		options := integrationOptions(t)
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		independent, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer independent.Close()
		separate := mutationConnection(t, independent, ctx, replica)
		defer separate.Close()
		r := request(replica, nil, librbd...)
		callCtx, stop := context.WithTimeout(ctx, 4*time.Second)
		defer stop()
		reply, err := separate.Request(callCtx, r)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if isReplicatedPGFixtureProfile() {
			// The placement names another primary: refused before submission.
			if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) {
				t.Fatal("a flagless compound to a replica was transmitted", reply.Submission, err)
			}
			return
		}
		// The legacy request has no placement to check; the replica treats it
		// as misdirected and never answers.
		if !errors.As(err, &unknown) || errors.As(err, &server) || reply.Submission.Transaction == 0 {
			t.Fatal("a flagless compound at a replica was answered", reply.Code, err)
		}
	})
	t.Run("refused-shapes-not-sent", func(t *testing.T) {
		balance := func(r *cephmsgr.OSDRequest) { r.BalanceReads = true }
		baseline := []cephmsgr.OSDOperation{sr(0, 8), rd(0, 8)}
		first, err := send(replica, balance, baseline...)
		if err != nil {
			t.Fatal(err)
		}
		for name, tc := range map[string]struct {
			configure func(*cephmsgr.OSDRequest)
			ops       []cephmsgr.OSDOperation
		}{
			"snapdir": {func(r *cephmsgr.OSDRequest) { balance(r); r.ReadSnapshot = ^uint64(0) }, baseline},
			"snapcontext": {func(r *cephmsgr.OSDRequest) {
				balance(r)
				r.ReadSnapshot = snap.SnapID
				r.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap.SnapID, Snapshots: []uint64{snap.SnapID}}
			}, baseline},
			"retry":   {func(r *cephmsgr.OSDRequest) { balance(r); r.RetryAttempt = 1 }, baseline},
			"failok":  {balance, []cephmsgr.OSDOperation{hint(sr(0, 8), uint32(cephmsgr.OSDFailOK)), rd(0, 8)}},
			"stat":    {balance, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDStat}}},
			"mapext":  {func(r *cephmsgr.OSDRequest) { r.LocalizeReads = true }, []cephmsgr.OSDOperation{{Code: cephmsgr.OSDMapExt, Length: 8, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 64}}, sr(0, 8)}},
			"isdirty": {balance, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDIsdirty}}},
			"write":   {balance, []cephmsgr.OSDOperation{sr(0, 8), {Code: cephmsgr.OSDWrite, Length: 1, Data: []byte{1}}}},
		} {
			reply, err := send(replica, tc.configure, tc.ops...)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
				t.Fatal("unreviewed replica compound was transmitted", name, reply.Submission, err)
			}
		}
		next, err := send(replica, balance, baseline...)
		if err != nil || next.Transaction != first.Transaction+1 || !bytes.Equal(next.RawData, first.RawData) {
			t.Fatal("a refusal consumed a TID or broke the connection", first.Transaction, next.Transaction, err)
		}
	})
	for _, name := range objects {
		if after := poolSparseMapAt(t, ctx, name, ""); !reflect.DeepEqual(after.SparseMap.State, headBefore[name].SparseMap.State) || !reflect.DeepEqual(after.SparseMap.Full, headBefore[name].SparseMap.Full) {
			t.Fatal("replica reads changed a native head", name)
		}
	}
	if after := poolSparseMapAt(t, ctx, clone, snapName); !reflect.DeepEqual(after.SparseMap, cloneMap.SparseMap) {
		t.Fatal("replica reads changed the native clone")
	}
}
