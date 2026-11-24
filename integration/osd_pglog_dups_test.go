package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Duplicate-tier control of the PG log. In the pinned source (Ceph v20.2.4)
// a primary answers a write whose full request identity (entity,
// incarnation, TID) it still finds in its log with the original result and
// versions instead of executing it (src/osd/PrimaryLogPG.cc:2274-2300 do_op,
// src/osd/PG.cc:781-794, src/osd/PGLog.h:401-463 get_request: the ordinary
// entries, their extra request IDs, then the duplicate records). A trimmed
// entry becomes a duplicate record, and each of its extra request IDs a
// separate record with the entry's own version, while it is within
// osd_pg_log_dups_tracked versions of the newest entry; the oldest records
// are dropped while more than that many remain (src/osd/PGLog.cc:55-160). A
// request dropped from every tier executes again when it is resubmitted.
// The live `pg <pgid> log` command dumps the primary's in-memory log, whose
// records that lookup indexes (src/osd/PrimaryLogPG.cc:1076-1083); it is
// answered by the primary alone. These consumer helpers are test-only.

// pgLogRecord is one request identity of a PG-log dump, in dump order:
// the duplicate records, then each ordinary entry followed by its extra
// request IDs.
type pgLogRecord struct {
	Tier    string // "dups", "log" or "extra_reqids"
	Reqid   string
	Version string
	// Op and Object are dumped for ordinary entries only.
	Op, Object string
	// UserVersion is dumped for duplicate records and extra request IDs
	// only (src/osd/osd_types.cc pg_log_dup_t::dump and
	// pg_log_entry_t::dump); it is nil for an ordinary entry.
	UserVersion *uint64
}

// parsePGLogRecords reads the request identities of one raw `pg_log_t`
// dump with Go's own decoder, independently of the Python parser.
func parsePGLogRecords(raw []byte) (head, tail string, records []pgLogRecord, err error) {
	var document struct {
		Log *struct {
			Head    string `json:"head"`
			Tail    string `json:"tail"`
			Entries []struct {
				Reqid   string `json:"reqid"`
				Version string `json:"version"`
				Op      string `json:"op"`
				Object  string `json:"object"`
				Extras  []struct {
					Reqid       string `json:"reqid"`
					UserVersion string `json:"user_version"`
				} `json:"extra_reqids"`
			} `json:"log"`
			Dups []struct {
				Reqid       string `json:"reqid"`
				Version     string `json:"version"`
				UserVersion string `json:"user_version"`
			} `json:"dups"`
		} `json:"pg_log_t"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return "", "", nil, err
	}
	if document.Log == nil || document.Log.Head == "" || document.Log.Tail == "" {
		return "", "", nil, errors.New("dump has no pg_log_t head and tail")
	}
	number := func(text string) (*uint64, error) {
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("user version %q: %w", text, err)
		}
		return &value, nil
	}
	for _, dup := range document.Log.Dups {
		uv, err := number(dup.UserVersion)
		if err != nil {
			return "", "", nil, err
		}
		records = append(records, pgLogRecord{Tier: "dups", Reqid: dup.Reqid, Version: dup.Version, UserVersion: uv})
	}
	for _, entry := range document.Log.Entries {
		records = append(records, pgLogRecord{Tier: "log", Reqid: entry.Reqid, Version: entry.Version, Op: entry.Op, Object: entry.Object})
		for _, extra := range entry.Extras {
			uv, err := number(extra.UserVersion)
			if err != nil {
				return "", "", nil, err
			}
			records = append(records, pgLogRecord{Tier: "extra_reqids", Reqid: extra.Reqid, Version: entry.Version, UserVersion: uv})
		}
	}
	return document.Log.Head, document.Log.Tail, records, nil
}

// dupHistoryRequest is one identity the test made the PG log hold: a
// request it sent, at its reply's replay and user versions, or the source
// write a COPY_FROM2 carried as its entry's extra request ID (Extra; same
// version as that entry, the source's own user version).
type dupHistoryRequest struct {
	Label       string
	Reqid       string
	Version     string
	UserVersion uint64
	Extra       bool
	// Probe marks the owned-pool create's own write and removal of its probe
	// object, which the test takes from the first dump; their user
	// versions are not compared.
	Probe bool
}

// dupHistoryPosition says where one complete dump begins in the requests
// the test made, in order: requests before First are in no tier, those from
// First up to Log are duplicate records, and the rest are ordinary entries
// with their extra request IDs.
type dupHistoryPosition struct {
	First, Log int
}

// tier is where request i is in that dump: "evicted", "dups" or "log".
func (p dupHistoryPosition) tier(i int) string {
	switch {
	case i < p.First:
		return "evicted"
	case i < p.Log:
		return "dups"
	}
	return "log"
}

// matchDupHistory requires one dump's records to be exactly the newest
// requests of want, from some request on and in the same order, with
// nothing else: every duplicate record before every ordinary entry, each
// request at its own version and user version, an extra request ID right
// after its entry, and the head at the last request's version. The dump
// thus holds every later request and none from another writer.
//
// A persisted store has one value per `dup_<version>` key
// (src/osd/osd_types.cc:5200-5207). With collapse, the duplicate records of
// an entry and its extra request IDs, which share that version, must be
// reduced to the entry's own record: the pinned writer encodes the records
// that a trim adds newest first into one key map, so the entry, encoded
// last, keeps the key (src/osd/PGLog.cc:832-850). Without collapse every
// record must be there, as in the live log.
func matchDupHistory(head, tail string, records []pgLogRecord, want []dupHistoryRequest, collapse bool) (dupHistoryPosition, error) {
	if len(want) == 0 {
		return dupHistoryPosition{}, errors.New("no requests to match")
	}
	if last := want[len(want)-1]; head != last.Version {
		return dupHistoryPosition{}, fmt.Errorf("head %s; the last request %s is at %s", head, last.Label, last.Version)
	}
	if len(records) == 0 {
		return dupHistoryPosition{}, errors.New("dump holds no request")
	}
	first := slices.IndexFunc(want, func(r dupHistoryRequest) bool { return r.Reqid == records[0].Reqid && r.Version == records[0].Version })
	if first < 0 {
		return dupHistoryPosition{}, fmt.Errorf("the oldest record %s at %s is not a request of this test", records[0].Reqid, records[0].Version)
	}
	position := dupHistoryPosition{First: first, Log: -1}
	i := first
	for n, record := range records {
		if record.Tier != "dups" && position.Log < 0 {
			position.Log = i
		}
		if record.Tier == "dups" && position.Log >= 0 {
			return dupHistoryPosition{}, fmt.Errorf("record %d is a duplicate after an ordinary entry", n)
		}
		if i >= len(want) {
			return dupHistoryPosition{}, fmt.Errorf("record %d (%s %s at %s) is newer than every request", n, record.Tier, record.Reqid, record.Version)
		}
		r := want[i]
		tier := "log"
		switch {
		case record.Tier == "dups":
			tier = "dups"
		case r.Extra:
			tier = "extra_reqids"
		}
		if record.Reqid != r.Reqid || record.Version != r.Version || record.Tier != tier {
			return dupHistoryPosition{}, fmt.Errorf("record %d is %s %s at %s; want %s %s (%s) at %s", n, record.Tier, record.Reqid, record.Version, tier, r.Reqid, r.Label, r.Version)
		}
		if !r.Probe && (record.UserVersion != nil && *record.UserVersion != r.UserVersion || record.UserVersion == nil && record.Tier != "log") {
			return dupHistoryPosition{}, fmt.Errorf("record %d (%s) has user version %v; want %d", n, r.Label, record.UserVersion, r.UserVersion)
		}
		if newer, err := eversionAfter(record.Version, tail); err != nil || newer != (record.Tier != "dups") {
			return dupHistoryPosition{}, fmt.Errorf("record %d (%s %s at %s) is on the wrong side of the tail %s: %v", n, record.Tier, r.Label, record.Version, tail, err)
		}
		i++
		if collapse && record.Tier == "dups" {
			// The extra request IDs of this entry shared its key.
			for i < len(want) && want[i].Extra && want[i].Version == record.Version {
				i++
			}
		}
	}
	if i != len(want) {
		return dupHistoryPosition{}, fmt.Errorf("the dump ends at request %d of %d (%s)", i, len(want), want[i].Label)
	}
	if position.Log < 0 {
		position.Log = len(want)
	}
	return position, nil
}

// eversionAfter reports whether eversion a ("<epoch>'<version>") is newer
// than b.
func eversionAfter(a, b string) (bool, error) {
	parse := func(text string) ([2]uint64, error) {
		var v [2]uint64
		if _, err := fmt.Sscanf(text, "%d'%d", &v[0], &v[1]); err != nil || fmt.Sprintf("%d'%d", v[0], v[1]) != text {
			return v, fmt.Errorf("eversion %q", text)
		}
		return v, nil
	}
	x, err := parse(a)
	if err != nil {
		return false, err
	}
	y, err := parse(b)
	if err != nil {
		return false, err
	}
	return x[0] > y[0] || x[0] == y[0] && x[1] > y[1], nil
}

// createProbe reads the two entries the owned-pool create leaves in a new
// pool's log before any request of the test (tools/osd_pool_oracle.py
// mode_create: one WRITEFULL and one removal of the probe object through
// the oracle's own librados client): the first two records of the first
// dump, an ordinary modify and then a delete of that object by another
// client than this test's.
func createProbe(records []pgLogRecord, gid uint64) ([]dupHistoryRequest, error) {
	if len(records) < 2 {
		return nil, errors.New("fewer than two records")
	}
	var probe []dupHistoryRequest
	for i, op := range []string{"modify", "delete"} {
		r := records[i]
		if r.Tier != "log" || r.Op != op || r.Object == "" || !bytes.HasSuffix([]byte(r.Object), []byte(":::ceph-msgr-owned-pool-probe:head")) ||
			bytes.HasPrefix([]byte(r.Reqid), []byte(fmt.Sprintf("client.%d.", gid))) {
			return nil, fmt.Errorf("record %d is %s %s of %s by %s", i, r.Tier, r.Op, r.Object, r.Reqid)
		}
		probe = append(probe, dupHistoryRequest{Label: "create probe " + op, Reqid: r.Reqid, Version: r.Version, Probe: true})
	}
	return probe, nil
}

// pgLogBoundsReceipt is the owned-pool oracle's pglogbounds result
// (tools/osd_pool_mode_pglogbounds_oracle.py).
type pgLogBoundsReceipt struct {
	Bounds struct {
		Action    string                    `json:"action"`
		Bounds    map[string]int            `json:"bounds"`
		Claimed   bool                      `json:"claimed"`
		Failures  []string                  `json:"failures"`
		Prior     map[string]*string        `json:"prior"`
		Entries   map[string]*string        `json:"entries"`
		Effective map[string]map[string]int `json:"effective"`
		Lowered   bool                      `json:"lowered"`
		Restored  bool                      `json:"restored"`
	} `json:"pglog_bounds"`
}

// dupHistoryBounds are the values the oracle sets: the PG log keeps four
// entries, trims on every write past them, and keeps at most twelve
// duplicate records.
var dupHistoryBounds = map[string]int{"osd_min_pg_log_entries": 4, "osd_max_pg_log_entries": 4, "osd_pg_log_trim_min": 1, "osd_pg_log_dups_tracked": 12}

// dupHistoryRun is one run of the scenario on one owned pool.
type dupHistoryRun struct {
	t       *testing.T
	ctx     context.Context
	c       *cephmsgr.Client
	request func(osdFixtureOracle, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
	gid     uint64
	slot    int
	route   osdFixtureOracle
	conn    *cephmsgr.OSDConnection
	want    []dupHistoryRequest
	epoch   uint32
	// probed: want begins with the create's probe entries.
	probed bool
	// last is the most recent primary snapshot and its position.
	last     pgHistorySnapshot
	position dupHistoryPosition
}

func (r *dupHistoryRun) key(incarnation uint32, reply cephmsgr.OSDReply) pgHistoryTarget {
	return pgHistoryTarget{Type: "client", Num: r.gid, Inc: int32(incarnation), TID: reply.Transaction}
}

// send issues one Go request exactly once on conn and requires an on-disk
// success for this client's identity. A resubmission keeps its TID.
func (r *dupHistoryRun) send(label string, conn *cephmsgr.OSDConnection, message cephmsgr.OSDRequest) cephmsgr.OSDReply {
	r.t.Helper()
	reply, err := conn.Request(r.ctx, message)
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		r.t.Fatalf("%s: err=%v code=%d flags=%#x; not resent", label, err, reply.Code, reply.Flags)
	}
	tid := reply.Transaction
	if message.Resubmit != nil {
		tid = message.Resubmit.Transaction
	}
	if reply.Transaction == 0 || reply.Transaction != tid || reply.Submission.Transaction != tid || reply.Submission.GlobalID != r.gid ||
		reply.Submission.ClientIncarnation != message.ClientIncarnation || reply.RetryAttempt != message.RetryAttempt || reply.Object != message.Object.Name {
		r.t.Fatal("reply identity differs from its submission", label, reply.Transaction, reply.RetryAttempt, reply.Submission)
	}
	return reply
}

// reopen ends the run's session with the primary and opens a fresh one,
// which later requests use too. The client holds one session per OSD, so a
// resubmission goes out only after the session of the original has ended
// (cephmsgr OSDRequest.Resubmit).
func (r *dupHistoryRun) reopen() *cephmsgr.OSDConnection {
	r.t.Helper()
	if err := r.conn.Close(); err != nil {
		r.t.Fatal("closing the original session", err)
	}
	r.conn = mutationConnection(r.t, r.c, r.ctx, r.route)
	return r.conn
}

// write sends one new request for the object of route on the run's
// connection and records it.
func (r *dupHistoryRun) write(label string, route osdFixtureOracle, incarnation, seconds uint32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
	r.t.Helper()
	message := r.request(route, ops...)
	message.ClientIncarnation = incarnation
	message.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
	reply := r.send(label, r.conn, message)
	if reply.Submission.Attempt != 0 || reply.RetryAttempt != 0 {
		r.t.Fatal("a new request carries a retry attempt", label, reply.Submission)
	}
	r.want = append(r.want, dupHistoryRequest{Label: label, Reqid: r.key(incarnation, reply).String(), Version: replayVersion(reply), UserVersion: reply.UserVersion})
	return reply
}

// snapshot takes one bracketed primary snapshot of the PG and matches it to
// the requests made so far. The map epoch must not move during the run: a
// placement or epoch change would leave the observation unattributed.
func (r *dupHistoryRun) snapshot(object string, targets []pgHistoryTarget) (pgHistorySnapshot, dupHistoryPosition) {
	r.t.Helper()
	s := primaryPGHistory(r.t, r.ctx, r.slot, object, targets)
	requirePGHistoryOnRoute(r.t, s, r.route)
	if s.Before.Epoch != r.epoch {
		r.t.Fatalf("map epoch moved from %d to %d during the run; the observation is unattributed", r.epoch, s.Before.Epoch)
	}
	raw, _ := base64.StdEncoding.DecodeString(s.Raw)
	head, tail, records, err := parsePGLogRecords(raw)
	if err != nil || head != s.Head || tail != s.Tail {
		r.t.Fatalf("Go reading of the dump differs: %v, head %s tail %s vs %s %s", err, head, tail, s.Head, s.Tail)
	}
	if !r.probed {
		probe, err := createProbe(records, r.gid)
		if err != nil {
			r.t.Fatalf("the first dump does not begin with the owned-pool create's probe: %v", err)
		}
		r.want, r.probed = append(probe, r.want...), true
	}
	p, err := matchDupHistory(head, tail, records, r.want, false)
	if err != nil {
		r.t.Fatalf("primary PG log is not exactly the newest requests of this test: %v", err)
	}
	if p.First < r.position.First || p.Log < r.position.Log {
		r.t.Fatalf("history moved backwards: from %+v to %+v", r.position, p)
	}
	if s.DupEntries != p.Log-p.First || s.LogEntries+s.ExtraReqidEntries != len(r.want)-p.Log {
		r.t.Fatalf("receipt counts %d dups, %d entries and %d extras; the Go reading places requests %+v of %d", s.DupEntries, s.LogEntries, s.ExtraReqidEntries, p, len(r.want))
	}
	r.last, r.position = s, p
	return s, p
}

// index is the position of the request labelled label in r.want.
func (r *dupHistoryRun) index(label string) int {
	i := slices.IndexFunc(r.want, func(w dupHistoryRequest) bool { return w.Label == label })
	if i < 0 {
		r.t.Fatal("no request", label)
	}
	return i
}

// requireLocations requires a target's Python-parsed positions to be exactly
// one position in tier at version, or none for "evicted".
func requireLocations(t *testing.T, s pgHistorySnapshot, target int, tier, version string) {
	t.Helper()
	got := s.Targets[target]
	if tier == "evicted" {
		if len(got.Locations) != 0 {
			t.Fatalf("%s is still in the primary's log: %+v", got.Reqid, got.Locations)
		}
		return
	}
	if len(got.Locations) != 1 || got.Locations[0].Tier != tier || got.Locations[0].Version != version {
		t.Fatalf("%s occurs at %+v; want once in %s at %s", got.Reqid, got.Locations, tier, version)
	}
}

// dupHistoryScenario: with the OSDs' PG-log bounds lowered through the
// fixture, the Go client issues a seed WRITEFULL and a known APPEND K of
// object A under their own incarnations, and one COPY_FROM2 P of a source
// written once in an owned replicated pool, whose entry carries the source
// write S as its extra request ID. Every later request is a WRITEFULL of
// another object of the same one-PG pool. After each one the live primary's
// PG log must be exactly the newest requests of this test (matchDupHistory),
// so where each identity is follows from one complete dump.
//
// Phase 1: K, P and S start as ordinary entries and must move into the
// duplicate records; P and S must then be two records with P's version.
// With shards, every acting shard's store is then copied and read offline.
// Each copy must sit exactly where the primary's live log does, with K, P
// and S trimmed into the duplicate records (shardPersistedVerdict): the
// primary sends every acting shard, its own included, the same trim bound
// with each write. K and P must be persisted duplicate records on every
// shard and S must not be, because S and P share one key. The primary's
// copy must be its live log with that one record fewer. K is then
// resubmitted explicitly under its original identity on a fresh connection:
// the OSD must answer it from the duplicate record with K's original
// versions, the native object must be unchanged and the live log must be
// byte for byte the same.
//
// Phase 2: more writes push K out of every tier of the live primary log.
// The first dump without K must begin exactly at K's successor P, with S
// beside it as a duplicate record, and hold every later request
// (evictionBoundary): K left alone, and P and S are the surviving positive
// control. With shards, every acting shard's store is copied again and must
// sit at that live position (shardEvictedVerdict): no record of K in the
// copy, P persisted at its key. K is then resubmitted explicitly once more: the
// OSD has no record of it and executes the APPEND again at a new version,
// the native object holds the tail twice, and the live log holds K as a new
// ordinary entry. That is the loss of request history after eviction.
//
// Not claimed: anything about a shard's live duplicate index, the shards'
// stores after the second resubmission, an OSD restart (which reloads the persisted
// records), a PG interval change, the default bounds, or any other
// operation than APPEND and COPY_FROM2.
func dupHistoryScenario(t *testing.T, width *erasureWidth) {
	shards, topology := width != nil, "3"
	if shards {
		topology = width.topology
	}
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("needs the BlueStore OSD topology %q", topology)
	}
	if shards && os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned pools were not removed")
		}
		if shards && slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	const (
		sourceSlot, slot = 1, 2
		source           = "dup-history-source"
		known            = "dup-history-known"
		destination      = "dup-history-copy"
		filler           = "dup-history-filler"
		seedIncarnation  = 6
		knownIncarnation = 7
		sourceInc        = 5
		copyIncarnation  = 9
		fillIncarnation  = 11
		maxFills         = 40
	)
	replicated := nativePool(t, ctx, "create", map[string]any{"slot": sourceSlot, "pg_num": 1, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	if replicated.PoolID <= 0 || replicated.PoolID == base.Pool || replicated.PoolType != 1 {
		t.Fatal("owned source pool is not a separate replicated pool", replicated.PoolID, replicated.PoolType)
	}
	// One PG holds every subject object.
	var created poolReceipt
	if shards {
		created = nativePool(t, ctx, "create", map[string]any{"slot": slot, "pg_num": 1, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	} else {
		created = nativePool(t, ctx, "create", map[string]any{"slot": slot, "pg_num": 1, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	}
	if created.PoolID == replicated.PoolID || created.PGNum != 1 {
		t.Fatal("subject pool is not a separate one-PG pool", created.PoolID, created.PGNum)
	}
	var facts erasureFacts
	if shards {
		if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
			t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
		}
		facts = erasurePoolMapFacts(t, ctx, c, created, *width, true)
	} else if created.PoolType != 1 {
		t.Fatal("subject pool is not replicated", created.PoolType)
	}
	for _, object := range []string{known, destination, filler} {
		if poolObserve(t, ctx, slot, object, "", "").Code != -2 {
			t.Fatal("subject object already exists", object)
		}
	}
	route := poolPlace(t, ctx, slot, known, "")
	if shards {
		facts.checkPlacement(t, route)
	}
	routes := map[string]osdFixtureOracle{known: route}
	for _, object := range []string{destination, filler} {
		other := poolPlace(t, ctx, slot, object, "")
		if other.PGID != route.PGID || other.PrimaryID != route.PrimaryID || other.Epoch != route.Epoch || !slices.Equal(other.Acting, route.Acting) || other.Address != route.Address {
			t.Fatalf("%s is not in the subject PG %s: %+v", object, route.PGID, other)
		}
		routes[object] = other
	}
	sourceRoute := poolPlace(t, ctx, sourceSlot, source, "")
	if sourceRoute.Pool != replicated.PoolID || sourceRoute.PoolType != 1 {
		t.Fatal("source is not in the owned replicated pool", sourceRoute)
	}

	// The bounds, restored by this test's own cleanup before the pools are
	// removed.
	lowered := false
	t.Cleanup(func() {
		if !lowered {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		var restored pgLogBoundsReceipt
		nativePoolResult(t, cleanup, "pglogbounds", map[string]any{"action": "restore"}, &restored)
		if restored.Bounds.Claimed || !restored.Bounds.Restored {
			t.Errorf("PG-log bounds were not restored: %q, central entries %v", restored.Bounds.Failures, restored.Bounds.Entries)
		}
	})
	lowered = true
	var set pgLogBoundsReceipt
	nativePoolResult(t, ctx, "pglogbounds", map[string]any{"action": "lower"}, &set)
	if !set.Bounds.Lowered || len(set.Bounds.Failures) != 0 || len(set.Bounds.Effective) < route.Replicas {
		t.Fatalf("PG-log bounds were not lowered: %q", set.Bounds.Failures)
	}
	for osd, values := range set.Bounds.Effective {
		if !maps.Equal(values, dupHistoryBounds) {
			t.Fatalf("osd.%s applies %v; want %v", osd, values, dupHistoryBounds)
		}
	}
	t.Logf("PG-log bounds %v applied on osd %v (prior central entries %v)", dupHistoryBounds, slices.Sorted(maps.Keys(set.Bounds.Effective)), set.Bounds.Prior)

	r := &dupHistoryRun{t: t, ctx: ctx, c: c, request: request, gid: c.Snapshot().GlobalID, slot: slot, route: route, epoch: route.Epoch}
	r.conn = mutationConnection(t, c, ctx, route)
	defer func() { r.conn.Close() }()
	sourceConn := mutationConnection(t, c, ctx, sourceRoute)
	defer sourceConn.Close()

	// S: the source write in the replicated pool (another PG, not recorded
	// in the subject's requests until the copy carries it).
	sourceData := []byte("dup-history\x00\xff source bytes")
	sourceMessage := request(sourceRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(sourceData)), Data: sourceData})
	sourceMessage.ClientIncarnation, sourceMessage.ModificationTime = sourceInc, cephmsgr.OSDTime{Seconds: 1700012000}
	written := r.send("source write", sourceConn, sourceMessage)
	if native := poolObserve(t, ctx, sourceSlot, source, "", ""); native.Code != 0 || !bytes.Equal(native.Data, sourceData) || native.Version != written.UserVersion || written.UserVersion == 0 {
		t.Fatal("native source differs from the one Go write", native.Code, native.Version, written.UserVersion)
	}

	seed := bytes.Repeat([]byte{0x5a, 0, 0xff, 'd'}, 256)
	tail := []byte("appended once\x00\xff" + known)
	seeded := r.write("seed", routes[known], seedIncarnation, 1700012001, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(seed)), Data: seed})
	appendOp := cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail}
	k := r.write("K", routes[known], knownIncarnation, 1700012002, appendOp)
	p := r.write("P", routes[destination], copyIncarnation, 1700012003,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: replicated.PoolID, SourceName: source, SourceVersion: written.UserVersion}})
	if got := p.Operations[0]; got.Result != 0 || got.CopyFromVersion != written.UserVersion {
		t.Fatal("copy operation result differs", got.Result, got.CopyFromVersion)
	}
	sKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: sourceInc, TID: written.Transaction}
	r.want = append(r.want, dupHistoryRequest{Label: "S", Reqid: sKey.String(), Version: replayVersion(p), UserVersion: written.UserVersion, Extra: true})
	once := append(bytes.Clone(seed), tail...)
	if native := poolObserve(t, ctx, slot, known, "", ""); native.Code != 0 || !bytes.Equal(native.Data, once) || native.Version != k.UserVersion {
		t.Fatal("native object differs from one seed and one APPEND", native.Code, len(native.Data), native.Version, k.UserVersion)
	}
	if native := poolObserve(t, ctx, slot, destination, "", ""); native.Code != 0 || !bytes.Equal(native.Data, sourceData) || native.Version != p.UserVersion {
		t.Fatal("native copy differs from the source", native.Code, native.Version, p.UserVersion)
	}

	kKey := r.key(knownIncarnation, k)
	pKey := r.key(copyIncarnation, p)
	seedKey := r.key(seedIncarnation, seeded)
	targets := func(last pgHistoryTarget) []pgHistoryTarget {
		return []pgHistoryTarget{kKey, pKey, sKey, seedKey, last,
			{Type: "client", Num: r.gid, Inc: knownIncarnation + 1, TID: k.Transaction},
			{Type: "client", Num: r.gid, Inc: knownIncarnation, TID: k.Transaction + 1<<20},
			{Type: "client", Num: r.gid + 1, Inc: knownIncarnation, TID: k.Transaction}}
	}
	const kTarget, pTarget, sTarget, lastTarget = 0, 1, 2, 4
	requireMisses := func(s pgHistorySnapshot) {
		t.Helper()
		for _, got := range s.Targets[5:] {
			if len(got.Locations) != 0 {
				t.Fatalf("near-miss identity %s matched %+v", got.Reqid, got.Locations)
			}
		}
	}
	// Before any trim, K, P and S are ordinary entries and S is P's extra.
	// This first dump also supplies the create's probe entries.
	s, at := r.snapshot(known, targets(pKey))
	ki, pi, si := r.index("K"), r.index("P"), r.index("S")
	if at.First != 0 || at.Log != 0 {
		t.Fatalf("history was trimmed before the first filler: %+v", at)
	}
	requireLocations(t, s, kTarget, "log", r.want[ki].Version)
	requireLocations(t, s, sTarget, "extra_reqids", r.want[pi].Version)
	if err := copyFromDestinationBinding(s.Targets[pTarget].Locations, s.Targets[sTarget].Locations,
		copyFromHistoryWant{object: hobjectHead(routes[destination]), version: r.want[pi].Version, sourceUserVersion: written.UserVersion}); err != nil {
		t.Fatal(err)
	}
	requireMisses(s)

	fills, data := 0, func(n int) []byte { return bytes.Repeat([]byte{byte(n), 'f', 0, 0xff}, 64) }
	fill := func() pgHistoryTarget {
		t.Helper()
		fills++
		if fills > 2*maxFills {
			t.Fatalf("%d fillers did not reach the expected tier", 2*maxFills)
		}
		reply := r.write(fmt.Sprintf("F%d", fills), routes[filler], fillIncarnation, 1700012100+uint32(fills),
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 256, Data: data(fills)})
		return r.key(fillIncarnation, reply)
	}

	// Phase 1: until K, P and S are duplicate records.
	var last pgHistoryTarget
	for at.Log <= si {
		if fills >= maxFills {
			t.Fatalf("after %d fillers the newest duplicate record precedes S: %+v", fills, at)
		}
		last = fill()
		previous := at
		s, at = r.snapshot(filler, targets(last))
		requireMisses(s)
		for _, i := range []int{ki, pi, si} {
			if at.tier(i) == "evicted" {
				t.Fatalf("after F%d %s left every tier before the others became duplicates: %+v", fills, r.want[i].Label, at)
			}
		}
		if previous.Log <= ki && at.tier(ki) == "dups" {
			t.Logf("after F%d K moved from the ordinary entries to the duplicate records", fills)
		}
	}
	requireLocations(t, s, kTarget, "dups", r.want[ki].Version)
	requireLocations(t, s, pTarget, "dups", r.want[pi].Version)
	requireLocations(t, s, sTarget, "dups", r.want[pi].Version)
	requireLocations(t, s, lastTarget, "log", r.want[len(r.want)-1].Version)
	t.Logf("after F%d: K %s, P %s and S %s are duplicate records at K's %s and P's %s; dump %+v of %d requests, %d duplicate records",
		fills, kKey, pKey, sKey, r.want[ki].Version, r.want[pi].Version, at, len(r.want), s.DupEntries)

	if shards {
		dupHistoryShards(t, r, width, s, at, targets(last), ki, pi, si, false)
	}

	// The explicit resubmission while K is a duplicate record.
	before := r.last
	again := request(route, appendOp)
	again.ClientIncarnation, again.ModificationTime = knownIncarnation, cephmsgr.OSDTime{Seconds: 1700012002}
	again.Resubmit, again.RetryAttempt = &k.Submission, 1
	answered := r.send("K resubmitted from the duplicate record", r.reopen(), again)
	if answered.ReplayVersion != k.ReplayVersion || answered.UserVersion != k.UserVersion {
		t.Fatalf("the duplicate answer is at %s user version %d; K was %s user version %d", replayVersion(answered), answered.UserVersion, replayVersion(k), k.UserVersion)
	}
	if native := poolObserve(t, ctx, slot, known, "", ""); native.Code != 0 || !bytes.Equal(native.Data, once) || native.Version != k.UserVersion {
		t.Fatal("the duplicate answer changed the native object", native.Code, len(native.Data), native.Version)
	}
	s, at = r.snapshot(filler, targets(last))
	if s.SHA256 != before.SHA256 || s.Bytes != before.Bytes {
		t.Fatalf("the live PG log changed across the duplicate answer: %s (%d bytes) to %s (%d bytes)", before.SHA256, before.Bytes, s.SHA256, s.Bytes)
	}
	requireLocations(t, s, kTarget, "dups", r.want[ki].Version)
	t.Logf("K resubmitted with attempt 1 was answered from the duplicate record at %s user version %d; object and live log unchanged", replayVersion(answered), answered.UserVersion)

	// Phase 2: until K is in no tier of the live primary log.
	for at.First <= ki {
		if fills >= 2*maxFills {
			t.Fatalf("after %d fillers K is still a duplicate record: %+v", fills, at)
		}
		last = fill()
		s, at = r.snapshot(filler, targets(last))
		requireMisses(s)
	}
	requireLocations(t, s, kTarget, "evicted", "")
	requireLocations(t, s, lastTarget, "log", r.want[len(r.want)-1].Version)
	if err := evictionBoundary(at, ki, pi, si); err != nil {
		t.Fatalf("after F%d: %v", fills, err)
	}
	requireLocations(t, s, pTarget, "dups", r.want[pi].Version)
	requireLocations(t, s, sTarget, "dups", r.want[pi].Version)
	oldest := r.want[at.First]
	t.Logf("after F%d K is in no tier of the live primary log; %d duplicate records remain, the oldest K's successor %s (%s at %s) with S beside it, and every later request is there",
		fills, at.Log-at.First, oldest.Label, oldest.Reqid, oldest.Version)
	if shards {
		dupHistoryShards(t, r, width, s, at, targets(last), ki, pi, si, true)
	}
	if native := poolObserve(t, ctx, slot, known, "", ""); native.Code != 0 || !bytes.Equal(native.Data, once) || native.Version != k.UserVersion {
		t.Fatal("the native object changed before the second resubmission", native.Code, len(native.Data), native.Version)
	}

	// The explicit resubmission after the eviction.
	head := r.want[len(r.want)-1].Version
	again.RetryAttempt = 2
	replayed := r.send("K resubmitted after its eviction", r.reopen(), again)
	if replayed.ReplayVersion.Epoch < k.ReplayVersion.Epoch || replayed.ReplayVersion.Version <= lastVersion(t, head) || replayed.UserVersion <= k.UserVersion {
		t.Fatalf("the resubmission after eviction is at %s user version %d; the log head was %s and K was %s user version %d",
			replayVersion(replayed), replayed.UserVersion, head, replayVersion(k), k.UserVersion)
	}
	twice := append(bytes.Clone(once), tail...)
	if native := poolObserve(t, ctx, slot, known, "", ""); native.Code != 0 || !bytes.Equal(native.Data, twice) || native.Version != replayed.UserVersion {
		t.Fatalf("native object after the second resubmission: code %d, %d bytes, version %d; want the tail twice at %d", native.Code, len(native.Data), native.Version, replayed.UserVersion)
	}
	r.want = append(r.want, dupHistoryRequest{Label: "K again", Reqid: kKey.String(), Version: replayVersion(replayed), UserVersion: replayed.UserVersion})
	s, _ = r.snapshot(known, targets(last))
	requireLocations(t, s, kTarget, "log", replayVersion(replayed))
	t.Logf("K resubmitted with attempt 2 after its eviction executed again: new entry at %s user version %d (first execution %s user version %d); the object holds the tail twice",
		replayVersion(replayed), replayed.UserVersion, replayVersion(k), k.UserVersion)
}

// evictionBoundary judges the first live dump without K (position p): its
// oldest retained record must be K's successor P, with S, P's extra request
// ID, also a duplicate record. A suffix that begins later, so that more than
// K left together, is refused. Each filler adds one duplicate record and the
// trim drops the oldest while more than osd_pg_log_dups_tracked remain
// (src/osd/PGLog.cc:83-108, 141-151).
func evictionBoundary(p dupHistoryPosition, ki, pi, si int) error {
	if pi != ki+1 || si != pi+1 {
		return fmt.Errorf("requests K %d, P %d and S %d are not consecutive", ki, pi, si)
	}
	if p.tier(ki) != "evicted" {
		return fmt.Errorf("K is still %s: %+v", p.tier(ki), p)
	}
	if p.First != pi || p.tier(pi) != "dups" || p.tier(si) != "dups" {
		return fmt.Errorf("the oldest retained record is request %d, not K's successor P (%d) with S (%d) beside it as duplicate records: %+v", p.First, pi, si, p)
	}
	return nil
}

// shardPersistedVerdict judges one shard copy (position p, with the records
// of one version reduced to one) taken right after the live primary dump at
// position live. The primary builds one sub-write per acting shard, its own
// included, each with the same trim bound (src/osd/ECCommon.cc:812-889), and
// every shard passes it to its own log (src/osd/ECBackend.cc:383-442). Every
// shard has applied all the test's writes before their replies, so each copy
// must be at the live position, with K, P and S in the duplicate range.
func shardPersistedVerdict(p, live dupHistoryPosition, ki, pi, si int) error {
	if p != live {
		return fmt.Errorf("persisted history %+v differs from the live primary's %+v", p, live)
	}
	for _, i := range []int{ki, pi, si} {
		if p.tier(i) != "dups" {
			return fmt.Errorf("request %d is %s, not a persisted duplicate record: %+v", i, p.tier(i), p)
		}
	}
	return nil
}

// shardEvictedVerdict judges one shard copy (position p, with the records
// of one version reduced to one) taken right after the first live primary
// dump without K (position live). A trim that drops the oldest duplicate
// record also removes its `dup_<version>` key from the store
// (src/osd/PGLog.cc:141-151 adds the key to trimmed_dups, and :898-899 and
// :1034-1035 remove those keys with the write), and every shard
// applies the same trim bound with each sub-write, so each copy must be at
// the live position: K in no tier, P and S duplicate records.
func shardEvictedVerdict(p, live dupHistoryPosition, ki, pi, si int) error {
	if p != live {
		return fmt.Errorf("persisted history %+v differs from the live primary's %+v", p, live)
	}
	if p.tier(ki) != "evicted" {
		return fmt.Errorf("K is still %s in the store: %+v", p.tier(ki), p)
	}
	for _, i := range []int{pi, si} {
		if p.tier(i) != "dups" {
			return fmt.Errorf("request %d is %s, not a persisted duplicate record: %+v", i, p.tier(i), p)
		}
	}
	return nil
}

// dupHistoryShards copies every acting shard's store right after the
// primary snapshot s (position at) and matches each offline log to the
// requests, with the duplicate records of one version reduced to one. Every
// copy is read and classified on its own. After phase 1 (evicted false) each
// must pass shardPersistedVerdict and hold K and P as duplicate records;
// after the eviction (evicted true) each must pass shardEvictedVerdict and
// hold P but no record of K. S is never persisted beside P at their shared
// key.
func dupHistoryShards(t *testing.T, r *dupHistoryRun, width *erasureWidth, s pgHistorySnapshot, at dupHistoryPosition, targets []pgHistoryTarget, ki, pi, si int, evicted bool) {
	t.Helper()
	verdict := shardPersistedVerdict
	if evicted {
		verdict = shardEvictedVerdict
	}
	captured := shardPGHistory(t, r.ctx, r.slot, "dup-history-filler", r.route, targets, nil)
	if captured.Before.Epoch != s.After.Epoch || captured.Before.Epoch != r.epoch {
		t.Fatalf("map epoch moved from %d to %d between the primary snapshot and the shard capture", s.After.Epoch, captured.Before.Epoch)
	}
	if len(captured.Shards) != width.size() {
		t.Fatalf("%d shards captured; the width has %d", len(captured.Shards), width.size())
	}
	for i, shard := range captured.Shards {
		raw, _ := base64.StdEncoding.DecodeString(shard.Raw)
		head, tail, records, err := parsePGLogRecords(raw)
		if err != nil || head != shard.Head || tail != shard.Tail {
			t.Fatalf("Go reading of %s differs: %v", shard.SPG, err)
		}
		p, err := matchDupHistory(head, tail, records, r.want, true)
		if err != nil {
			t.Fatalf("shard %s (osd.%d) does not hold exactly the newest requests: %v", shard.SPG, shard.OSD, err)
		}
		for _, got := range shard.Targets[5:] {
			if len(got.Locations) != 0 {
				t.Fatalf("near-miss identity %s matched on %s: %+v", got.Reqid, shard.SPG, got.Locations)
			}
		}
		if err := verdict(p, at, ki, pi, si); err != nil {
			t.Fatalf("shard %s (osd.%d): %v", shard.SPG, shard.OSD, err)
		}
		if evicted {
			if len(shard.Targets[0].Locations) != 0 {
				t.Fatalf("shard %s still holds K after its eviction from the live log: %+v", shard.SPG, shard.Targets[0].Locations)
			}
		} else {
			requireShardLocation(t, shard, 0, "dups", r.want[ki].Version)
		}
		requireShardLocation(t, shard, 1, "dups", r.want[pi].Version)
		if len(shard.Targets[2].Locations) != 0 {
			t.Fatalf("shard %s persisted S beside P at one key: %+v", shard.SPG, shard.Targets[2].Locations)
		}
		collision := "dup_" + r.want[pi].Version + " holds P only"
		if i == int(r.route.Shard) {
			if shard.OSD != s.OSD || p != at || shard.Head != s.Head || shard.Tail != s.Tail || shard.DupEntries != s.DupEntries-1 || shard.LogEntries != s.LogEntries {
				t.Fatalf("the primary's store copy %s (%+v, %d duplicate records) differs from its live log (%+v, %d duplicate records) by more than the shared key",
					shard.SPG, p, shard.DupEntries, at, s.DupEntries)
			}
		}
		t.Logf("shard %s (osd.%d): persisted history %+v of %d requests, K %s, %d duplicate records; same-version key: %s",
			shard.SPG, shard.OSD, p, len(r.want), p.tier(ki), shard.DupEntries, collision)
	}
}

func requireShardLocation(t *testing.T, shard shardHistoryShard, target int, tier, version string) {
	t.Helper()
	got := shard.Targets[target]
	if len(got.Locations) != 1 || got.Locations[0].Tier != tier || got.Locations[0].Version != version {
		t.Fatalf("shard %s: %s occurs at %+v; want once in %s at %s", shard.SPG, got.Reqid, got.Locations, tier, version)
	}
}

func lastVersion(t *testing.T, eversion string) uint64 {
	t.Helper()
	var epoch, version uint64
	if _, err := fmt.Sscanf(eversion, "%d'%d", &epoch, &version); err != nil {
		t.Fatal("eversion", eversion, err)
	}
	return version
}

// TestCephOSDErasureOptimizedK2M2DupHistoryIntegration is dupHistoryScenario
// on optimized isa k=2,m=2 (four OSDs), with every acting shard's store read
// offline after phase 1 and again after K's eviction.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2duphistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated pauses each acting OSD for a store copy and lowers every OSD's PG-log bounds for the run
func TestCephOSDErasureOptimizedK2M2DupHistoryIntegration(t *testing.T) {
	dupHistoryScenario(t, &erasureK2M2)
}

// TestCephOSDReplicatedDupHistoryIntegration is dupHistoryScenario on an
// owned replicated pool of the fixture's replica count (three BlueStore
// OSDs), live primary only.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdreplicatedduphistory key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdreplicatedduphistory key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:shareable I/O in its own pools only; checked restores put back the PG-log bounds on every OSD and remove the pools
func TestCephOSDReplicatedDupHistoryIntegration(t *testing.T) {
	dupHistoryScenario(t, nil)
}

func TestMatchDupHistory(t *testing.T) {
	want := []dupHistoryRequest{
		{Label: "seed", Reqid: "client.4700.6:1", Version: "30'1", UserVersion: 1},
		{Label: "K", Reqid: "client.4700.7:2", Version: "30'2", UserVersion: 2},
		{Label: "P", Reqid: "client.4700.9:3", Version: "30'3", UserVersion: 3},
		{Label: "S", Reqid: "client.4700.5:1", Version: "30'3", UserVersion: 1, Extra: true},
		{Label: "F1", Reqid: "client.4700.11:4", Version: "30'4", UserVersion: 4},
		{Label: "F2", Reqid: "client.4700.11:5", Version: "30'5", UserVersion: 5},
	}
	// Synthetic dump text; no daemon produced it.
	entry := func(r dupHistoryRequest, extras ...dupHistoryRequest) string {
		text := `{"op":"modify","object":"x","version":"` + r.Version + `","prior_version":"0'0","reqid":"` + r.Reqid + `","extra_reqids":[`
		for i, e := range extras {
			if i > 0 {
				text += ","
			}
			text += fmt.Sprintf(`{"reqid":"%s","user_version":"%d"}`, e.Reqid, e.UserVersion)
		}
		return text + `],"mtime":"0.0","return_code":0}`
	}
	dup := func(r dupHistoryRequest) string {
		return fmt.Sprintf(`{"reqid":"%s","version":"%s","user_version":"%d","return_code":"0"}`, r.Reqid, r.Version, r.UserVersion)
	}
	dump := func(head, tail string, dups []string, log []string) []byte {
		join := func(items []string) string {
			text := ""
			for i, item := range items {
				if i > 0 {
					text += ","
				}
				text += item
			}
			return text
		}
		return []byte(`{"pg_log_t":{"head":"` + head + `","tail":"` + tail + `","log":[` + join(log) + `],"dups":[` + join(dups) + `]}}`)
	}
	seed, k, p, s, f1, f2 := want[0], want[1], want[2], want[3], want[4], want[5]
	for name, c := range map[string]struct {
		raw      []byte
		collapse bool
		want     dupHistoryPosition
	}{
		"nothing trimmed":        {dump("30'5", "0'0", nil, []string{entry(seed), entry(k), entry(p, s), entry(f1), entry(f2)}), false, dupHistoryPosition{0, 0}},
		"K is a duplicate":       {dump("30'5", "30'2", []string{dup(seed), dup(k)}, []string{entry(p, s), entry(f1), entry(f2)}), false, dupHistoryPosition{0, 2}},
		"P and S are duplicates": {dump("30'5", "30'3", []string{dup(seed), dup(k), dup(p), dup(s)}, []string{entry(f1), entry(f2)}), false, dupHistoryPosition{0, 4}},
		"K evicted":              {dump("30'5", "30'4", []string{dup(p), dup(s), dup(f1)}, []string{entry(f2)}), false, dupHistoryPosition{2, 5}},
		"S outlives P":           {dump("30'5", "30'4", []string{dup(s), dup(f1)}, []string{entry(f2)}), false, dupHistoryPosition{3, 5}},
		"persisted, one key":     {dump("30'5", "30'3", []string{dup(seed), dup(k), dup(p)}, []string{entry(f1), entry(f2)}), true, dupHistoryPosition{0, 4}},
		"persisted, then more":   {dump("30'5", "30'4", []string{dup(k), dup(p), dup(f1)}, []string{entry(f2)}), true, dupHistoryPosition{1, 5}},
		"persisted, untrimmed P": {dump("30'5", "30'2", []string{dup(k)}, []string{entry(p, s), entry(f1), entry(f2)}), true, dupHistoryPosition{1, 2}},
	} {
		head, tail, records, err := parsePGLogRecords(c.raw)
		if err != nil {
			t.Fatal(name, err)
		}
		got, err := matchDupHistory(head, tail, records, want, c.collapse)
		if err != nil || got != c.want {
			t.Errorf("%s: %+v %v; want %+v", name, got, err, c.want)
		}
	}
	other := dupHistoryRequest{Reqid: "client.4800.11:4", Version: "30'4", UserVersion: 4}
	stale := k
	stale.UserVersion = 9
	moved := f1
	moved.Version = "30'6"
	for name, c := range map[string]struct {
		raw      []byte
		collapse bool
	}{
		"live log without S":           {dump("30'5", "30'3", []string{dup(k), dup(p)}, []string{entry(f1), entry(f2)}), false},
		"persisted S beside P":         {dump("30'5", "30'3", []string{dup(k), dup(p), dup(s)}, []string{entry(f1), entry(f2)}), true},
		"a request is missing":         {dump("30'5", "30'2", []string{dup(seed), dup(k)}, []string{entry(p, s), entry(f2)}), false},
		"a duplicate is missing":       {dump("30'5", "30'3", []string{dup(seed), dup(p), dup(s)}, []string{entry(f1), entry(f2)}), false},
		"a foreign writer":             {dump("30'5", "30'3", []string{dup(k), dup(p), dup(s)}, []string{entry(other), entry(f2)}), false},
		"a foreign oldest record":      {dump("30'5", "30'4", []string{dup(other)}, []string{entry(f2)}), false},
		"out of order":                 {dump("30'5", "30'2", []string{dup(k), dup(seed)}, []string{entry(p, s), entry(f1), entry(f2)}), false},
		"another user version":         {dump("30'5", "30'2", []string{dup(seed), dup(stale)}, []string{entry(p, s), entry(f1), entry(f2)}), false},
		"S as its own entry":           {dump("30'5", "0'0", nil, []string{entry(seed), entry(k), entry(p), entry(s), entry(f1), entry(f2)}), false},
		"S on another entry":           {dump("30'5", "0'0", nil, []string{entry(seed), entry(k), entry(p), entry(f1, s), entry(f2)}), false},
		"the last request is missing":  {dump("30'4", "0'0", nil, []string{entry(seed), entry(k), entry(p, s), entry(f1)}), false},
		"head past the last request":   {dump("30'6", "0'0", nil, []string{entry(seed), entry(k), entry(p, s), entry(f1), entry(f2)}), false},
		"a record newer than all":      {dump("30'6", "0'0", nil, []string{entry(seed), entry(k), entry(p, s), entry(f1), entry(f2), entry(moved)}), false},
		"a duplicate after an entry":   {dump("30'5", "30'2", []string{dup(seed)}, []string{entry(k), entry(p, s), entry(f1), entry(f2), dup(f2)}), false},
		"a duplicate newer than tail":  {dump("30'5", "30'1", []string{dup(seed), dup(k)}, []string{entry(p, s), entry(f1), entry(f2)}), false},
		"an entry not after the tail":  {dump("30'5", "30'3", []string{dup(seed), dup(k)}, []string{entry(p, s), entry(f1), entry(f2)}), false},
		"an empty dump":                {dump("30'5", "30'5", nil, nil), false},
		"an unparsable tail":           {dump("30'5", "30-2", []string{dup(seed), dup(k)}, []string{entry(p, s), entry(f1), entry(f2)}), false},
		"the newest request a version": {dump("30'5", "0'0", nil, []string{entry(seed), entry(k), entry(p, s), entry(f1), entry(moved)}), false},
	} {
		head, tail, records, err := parsePGLogRecords(c.raw)
		if err != nil {
			t.Fatal(name, err)
		}
		if got, err := matchDupHistory(head, tail, records, want, c.collapse); err == nil {
			t.Errorf("%s: accepted as %+v", name, got)
		}
	}
	if _, _, _, err := parsePGLogRecords([]byte(`{"pg_log_t":{"head":"30'1","tail":"0'0","log":[],"dups":[{"reqid":"client.1.1:1","version":"0'1","user_version":"x"}]}}`)); err == nil {
		t.Error("an unparsable user version was read")
	}
	if _, _, _, err := parsePGLogRecords([]byte(`{"op_log":{}}`)); err == nil {
		t.Error("a dump without pg_log_t was read")
	}
	// The copy's extra request ID is the newest request: the head is its
	// entry's version.
	head, tail, records, err := parsePGLogRecords(dump("30'3", "0'0", nil, []string{entry(seed), entry(k), entry(p, s)}))
	if got, err2 := matchDupHistory(head, tail, records, want[:4], false); err != nil || err2 != nil || got != (dupHistoryPosition{0, 0}) {
		t.Error("a dump ending in an extra request ID refused", got, err, err2)
	}
	probe := `{"op":"%s","object":"4:50ecdf02:::ceph-msgr-owned-pool-probe:head","version":"%s","prior_version":"0'0","reqid":"%s","extra_reqids":[],"mtime":"0.0","return_code":0}`
	modify, remove := fmt.Sprintf(probe, "modify", "25'1", "client.14632.0:1"), fmt.Sprintf(probe, "delete", "25'2", "client.14632.0:2")
	_, _, records, _ = parsePGLogRecords(dump("30'5", "0'0", nil, []string{modify, remove, entry(seed)}))
	if got, err := createProbe(records, 4700); err != nil || len(got) != 2 || got[0].Reqid != "client.14632.0:1" || got[1].Version != "25'2" || !got[0].Probe {
		t.Error("the create probe was not read", got, err)
	}
	for name, raw := range map[string][]byte{
		"by this client":  []byte(fmt.Sprintf(`{"pg_log_t":{"head":"25'2","tail":"0'0","log":[%s,%s],"dups":[]}}`, fmt.Sprintf(probe, "modify", "25'1", "client.4700.0:1"), remove)),
		"removal missing": dump("30'5", "0'0", nil, []string{modify, entry(seed)}),
		"another object":  dump("30'5", "0'0", nil, []string{entry(seed), remove}),
		"order reversed":  dump("30'5", "0'0", nil, []string{remove, modify}),
		"one record only": dump("30'5", "0'0", nil, []string{modify}),
		"already trimmed": dump("30'5", "25'2", []string{dup(seed), dup(k)}, []string{entry(p, s)}),
	} {
		_, _, records, _ := parsePGLogRecords(raw)
		if got, err := createProbe(records, 4700); err == nil {
			t.Errorf("create probe %s accepted: %+v", name, got)
		}
	}
	p0 := dupHistoryPosition{First: 2, Log: 5}
	for i, tier := range []string{"evicted", "evicted", "dups", "dups", "dups", "log"} {
		if p0.tier(i) != tier {
			t.Errorf("request %d: %s, want %s", i, p0.tier(i), tier)
		}
	}
}

func TestDupHistoryBoundaries(t *testing.T) {
	// Requests: seed 0, K 1, P 2, S 3 (P's extra), F1 4, F2 5, F3 6.
	const ki, pi, si = 1, 2, 3
	for name, c := range map[string]struct {
		p  dupHistoryPosition
		ok bool
	}{
		"P oldest, S beside it":        {dupHistoryPosition{First: 2, Log: 5}, true},
		"F1 oldest, K, P and S gone":   {dupHistoryPosition{First: 4, Log: 6}, false},
		"S oldest, P gone with K":      {dupHistoryPosition{First: 3, Log: 6}, false},
		"F2 oldest, no duplicate left": {dupHistoryPosition{First: 5, Log: 5}, false},
		"K still a duplicate":          {dupHistoryPosition{First: 1, Log: 5}, false},
		"P still an ordinary entry":    {dupHistoryPosition{First: 2, Log: 2}, false},
	} {
		if err := evictionBoundary(c.p, ki, pi, si); (err == nil) != c.ok {
			t.Errorf("eviction boundary %s %+v: %v", name, c.p, err)
		}
	}
	if evictionBoundary(dupHistoryPosition{First: 3, Log: 5}, ki, 3, 4) == nil {
		t.Error("eviction boundary accepted a P that is not K's successor")
	}
	live := dupHistoryPosition{First: 0, Log: 6}
	for name, c := range map[string]struct {
		p  dupHistoryPosition
		ok bool
	}{
		"at the live position":           {live, true},
		"P and S still ordinary entries": {dupHistoryPosition{First: 0, Log: 2}, false},
		"K still an ordinary entry":      {dupHistoryPosition{First: 0, Log: 1}, false},
		"trimmed less, K a duplicate":    {dupHistoryPosition{First: 0, Log: 5}, false},
		"trimmed past the primary":       {dupHistoryPosition{First: 1, Log: 7}, false},
		"K evicted on the shard":         {dupHistoryPosition{First: 2, Log: 6}, false},
	} {
		if err := shardPersistedVerdict(c.p, live, ki, pi, si); (err == nil) != c.ok {
			t.Errorf("shard verdict %s %+v: %v", name, c.p, err)
		}
	}
	if shardPersistedVerdict(dupHistoryPosition{First: 0, Log: 2}, dupHistoryPosition{First: 0, Log: 2}, ki, pi, si) == nil {
		t.Error("shard verdict accepted P and S as ordinary entries at an equal live position")
	}
	// After the eviction the live log begins at P.
	gone := dupHistoryPosition{First: 2, Log: 5}
	for name, c := range map[string]struct {
		p  dupHistoryPosition
		ok bool
	}{
		"at the live position":           {gone, true},
		"K still persisted":              {dupHistoryPosition{First: 1, Log: 5}, false},
		"P gone from the store as well":  {dupHistoryPosition{First: 3, Log: 5}, false},
		"trimmed less, F1 an entry":      {dupHistoryPosition{First: 2, Log: 4}, false},
		"trimmed past the primary":       {dupHistoryPosition{First: 2, Log: 6}, false},
		"the phase-1 position persisted": {dupHistoryPosition{First: 0, Log: 6}, false},
	} {
		if err := shardEvictedVerdict(c.p, gone, ki, pi, si); (err == nil) != c.ok {
			t.Errorf("evicted shard verdict %s %+v: %v", name, c.p, err)
		}
	}
	for name, live := range map[string]dupHistoryPosition{
		"K still a live duplicate": {First: 1, Log: 5},
		"P evicted with K":         {First: 3, Log: 5},
	} {
		if shardEvictedVerdict(live, live, ki, pi, si) == nil {
			t.Errorf("evicted shard verdict accepted an equal live position where %s: %+v", name, live)
		}
	}
	// The F1-oldest suffix is a valid suffix to the matcher; only the
	// boundary check refuses it.
	want := []dupHistoryRequest{
		{Label: "seed", Reqid: "client.4700.6:1", Version: "30'1", UserVersion: 1},
		{Label: "K", Reqid: "client.4700.7:2", Version: "30'2", UserVersion: 2},
		{Label: "P", Reqid: "client.4700.9:3", Version: "30'3", UserVersion: 3},
		{Label: "S", Reqid: "client.4700.5:1", Version: "30'3", UserVersion: 1, Extra: true},
		{Label: "F1", Reqid: "client.4700.11:4", Version: "30'4", UserVersion: 4},
		{Label: "F2", Reqid: "client.4700.11:5", Version: "30'5", UserVersion: 5},
	}
	raw := []byte(`{"pg_log_t":{"head":"30'5","tail":"30'4","log":[{"op":"modify","object":"x","version":"30'5","prior_version":"30'4","reqid":"client.4700.11:5","extra_reqids":[],"mtime":"0.0","return_code":0}],` +
		`"dups":[{"reqid":"client.4700.11:4","version":"30'4","user_version":"4","return_code":"0"}]}}`)
	head, tail, records, err := parsePGLogRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, err := matchDupHistory(head, tail, records, want, false)
	if err != nil || p != (dupHistoryPosition{First: 4, Log: 5}) {
		t.Fatal("the F1-oldest suffix", p, err)
	}
	if evictionBoundary(p, ki, pi, si) == nil {
		t.Error("eviction boundary accepted the F1-oldest suffix")
	}
}
