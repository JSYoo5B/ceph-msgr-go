package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Shard-local PG-log control for a partial overwrite of an optimized
// erasure-coded object. The primary of the pinned source (Ceph v20.2.4)
// builds one sub-write per acting shard and hands the operation's log
// entries to the shard inside it, but it leaves a shard out before building
// that sub-write when the shard's transaction is empty
// (src/osd/ECCommon.cc:812-889, src/osd/ECBackend.cc:919-928). An overwrite
// that changes neither the size nor a whiteout names the parity shards,
// shard 0 and the data shards it touches (src/osd/ECTransaction.cc:597-603,
// :803-830), so an overwrite confined to shard 0's chunk leaves a nonprimary
// data shard without a transaction and without the entry. These consumer
// helpers are test-only and nothing is exported.

// shardPartialWrite is one partial_writes_last_complete record of a shard's
// own pg_info_t: a shard that the holder saw left out of the writes in
// (From, To].
type shardPartialWrite struct {
	Shard int    `json:"shard"`
	From  string `json:"from"`
	To    string `json:"to"`
}

// shardLogEntry is one ordinary entry of a shard's own log: its version and
// the shards it names. An empty set is the dump's form for every shard.
type shardLogEntry struct {
	Version       string `json:"version"`
	WrittenShards []int  `json:"written_shards"`
}

// partialWritePlan restates which shards the pinned primary names for one
// overwrite of an existing optimized EC object that changes neither its size
// nor a whiteout and is not the first write of the interval: the parity
// shards, shard 0 and every data shard holding a byte of the range
// (src/osd/ECTransaction.cc:597-603; the write plan adds the touched shards
// at :803-830). It applies to a plugin without a chunk mapping, where raw
// shard and shard are equal, and it refuses a pool whose native
// nonprimary_shards differ from the data shards 1..k-1 that this
// restatement would otherwise assume.
func partialWritePlan(k, m int, nonprimary []int, stripeWidth, size, offset, length uint64) (written, skipped []int, err error) {
	if k < 2 || m < 1 || k+m > 8 || stripeWidth == 0 || stripeWidth%uint64(k) != 0 {
		return nil, nil, fmt.Errorf("unsupported geometry k=%d m=%d stripe_width=%d", k, m, stripeWidth)
	}
	want := make([]int, 0, k-1)
	for shard := 1; shard < k; shard++ {
		want = append(want, shard)
	}
	if !slices.Equal(nonprimary, want) {
		return nil, nil, fmt.Errorf("native nonprimary shards %v are not the data shards %v", nonprimary, want)
	}
	if length == 0 || offset+length < offset || offset+length > size {
		return nil, nil, fmt.Errorf("overwrite [%d,+%d) is not inside the %d-byte object", offset, length, size)
	}
	chunk := stripeWidth / uint64(k)
	touched := map[int]bool{0: true}
	for at := offset - offset%chunk; at < offset+length; at += chunk {
		touched[int(at/chunk%uint64(k))] = true
	}
	for shard := 0; shard < k+m; shard++ {
		if shard >= k || touched[shard] {
			written = append(written, shard)
		} else {
			skipped = append(skipped, shard)
		}
	}
	return written, skipped, nil
}

// erasureShardRole names a shard of a pool without a chunk mapping from the
// native profile width and the native nonprimary_shards.
func erasureShardRole(shard, k int, nonprimary []int) string {
	switch {
	case shard >= k:
		return "parity"
	case slices.Contains(nonprimary, shard):
		return "nonprimary data"
	}
	return "data"
}

// partialWriteWant is what one acknowledged full write followed by one
// acknowledged overwrite must leave in a PG that holds nothing newer.
type partialWriteWant struct {
	object      string
	full, patch string
	// shards is the pool size; written are the shards the overwrite names.
	shards  int
	written []int
}

func (w partialWriteWant) skipped() []int {
	var shards []int
	for shard := 0; shard < w.shards; shard++ {
		if !slices.Contains(w.written, shard) {
			shards = append(shards, shard)
		}
	}
	return shards
}

// partialWriteShardHistory is what one acting shard's own log shows for the
// overwrite at capture time. A shard without the entry is an observation of
// that shard's log only; it says nothing about eviction or loss.
type partialWriteShardHistory struct {
	// Patch: the overwrite's entry is held, naming the expected shards.
	Patch bool
	// Recorded: the shard's info holds the partial-write record of every
	// shard the overwrite left out. It cannot be true without Patch.
	Recorded bool
}

func (h partialWriteShardHistory) String() string {
	switch {
	case !h.Patch:
		return "overwrite entry not held, log ends at the full write"
	case !h.Recorded:
		return "overwrite entry held, no partial-write record"
	}
	return "overwrite entry held, partial-write record present"
}

// checkShardDumps rechecks one shard's info dump against its digest and
// reads both raw dumps independently of the Python parser: the info's shard
// ID, last_update, last_complete and partial_writes_last_complete records,
// the log's head, its last entry and every ordinary log entry's version and
// written_shards text must equal the receipt. A head newer than the last
// entry must be reported as such and be spanned by the shard's own
// partial-write record.
func checkShardDumps(shard shardHistoryShard) error {
	info, err := base64.StdEncoding.DecodeString(shard.InfoRaw)
	digest := sha256.Sum256(info)
	if err != nil || len(info) == 0 || len(info) != shard.InfoBytes || shard.InfoBytes > shard.LimitBytes || hex.EncodeToString(digest[:]) != shard.InfoSHA256 {
		return fmt.Errorf("info dump differs from its receipt: %v, %d of %d bytes", err, len(info), shard.InfoBytes)
	}
	var document struct {
		PGID         string `json:"pgid"`
		Shard        string `json:"shard"`
		LastUpdate   string `json:"last_update"`
		LastComplete string `json:"last_complete"`
		Records      []struct {
			ID   *int   `json:"id"`
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"partial_writes_last_complete"`
	}
	if err := json.Unmarshal(info, &document); err != nil || document.PGID != shard.SPG || len(document.Records) != len(shard.PartialWrites) {
		return fmt.Errorf("info dump is for %q with %d partial-write records; the receipt has %s with %d (%v)", document.PGID, len(document.Records), shard.SPG, len(shard.PartialWrites), err)
	}
	// The store's own shard ID, kept apart from the OSD's position among the
	// wire targets: it must be the acting index the capture selected.
	if shard.Shard == nil || *shard.Shard != shard.Index || document.Shard != strconv.Itoa(shard.Index) {
		return fmt.Errorf("info dump names shard %q; the capture selected acting index %d", document.Shard, shard.Index)
	}
	if document.LastUpdate == "" || document.LastUpdate != shard.LastUpdate || document.LastComplete != shard.LastComplete {
		return fmt.Errorf("info dump is at last_update %q last_complete %q; the receipt has %s and %s", document.LastUpdate, document.LastComplete, shard.LastUpdate, shard.LastComplete)
	}
	for i, record := range document.Records {
		if got := shard.PartialWrites[i]; record.ID == nil || *record.ID != got.Shard || record.From != got.From || record.To != got.To {
			return fmt.Errorf("partial-write record %d differs from the info dump: %+v", i, got)
		}
	}
	raw, err := base64.StdEncoding.DecodeString(shard.Raw)
	var log struct {
		Log struct {
			Head    string `json:"head"`
			Tail    string `json:"tail"`
			Entries []struct {
				Version       string  `json:"version"`
				WrittenShards *string `json:"written_shards"`
			} `json:"log"`
		} `json:"pg_log_t"`
	}
	if err != nil || json.Unmarshal(raw, &log) != nil || len(log.Log.Entries) != len(shard.Entries) || len(shard.Entries) != shard.LogEntries {
		return fmt.Errorf("log dump holds %d entries; the receipt lists %d of %d (%v)", len(log.Log.Entries), len(shard.Entries), shard.LogEntries, err)
	}
	for i, entry := range log.Log.Entries {
		got := shard.Entries[i]
		text := make([]string, len(got.WrittenShards))
		for j, written := range got.WrittenShards {
			text[j] = strconv.Itoa(written)
		}
		if entry.WrittenShards == nil || entry.Version != got.Version || *entry.WrittenShards != "{"+strings.Join(text, ",")+"}" {
			return fmt.Errorf("log entry %d differs from the dump: receipt %+v", i, got)
		}
	}
	// The log's bounds, read from the dump itself. A log without entries
	// ends at its tail.
	last := log.Log.Tail
	if len(log.Log.Entries) != 0 {
		last = log.Log.Entries[len(log.Log.Entries)-1].Version
	}
	if log.Log.Head == "" || log.Log.Head != shard.Head || log.Log.Tail != shard.Tail || last != shard.LastEntry || shard.HeadPastEntries != (last != log.Log.Head) {
		return fmt.Errorf("log dump runs from %q to head %q with last entry %q; the receipt has tail %s, head %s, last entry %s, head past entries %t",
			log.Log.Tail, log.Log.Head, last, shard.Tail, shard.Head, shard.LastEntry, shard.HeadPastEntries)
	}
	if shard.HeadPastEntries {
		// Only a partial-write range of this very shard explains a head
		// without its entry: the range must end at the head and start at the
		// last entry or earlier, and the log must hold an entry.
		own := slices.IndexFunc(shard.PartialWrites, func(record shardPartialWrite) bool { return record.Shard == shard.Index })
		if own < 0 || shard.PartialWrites[own].To != shard.Head || !versionAtOrBefore(shard.PartialWrites[own].From, last) || len(log.Log.Entries) == 0 {
			return fmt.Errorf("log head %s is newer than the last entry %s without this shard's partial-write record between them: %+v", shard.Head, last, shard.PartialWrites)
		}
	}
	return nil
}

// versionAtOrBefore compares two eversion_t print forms ("epoch'version").
// An unparsable version is never at or before another.
func versionAtOrBefore(a, b string) bool {
	parse := func(text string) (epoch, version uint64, ok bool) {
		left, right, found := strings.Cut(text, "'")
		epoch, errEpoch := strconv.ParseUint(left, 10, 32)
		version, errVersion := strconv.ParseUint(right, 10, 64)
		return epoch, version, found && errEpoch == nil && errVersion == nil
	}
	aEpoch, aVersion, aOK := parse(a)
	bEpoch, bVersion, bOK := parse(b)
	return aOK && bOK && (aEpoch < bEpoch || aEpoch == bEpoch && aVersion <= bVersion)
}

// writeEntry requires an identity to be exactly one ordinary successful
// modify entry of the object at the version and returns its log index.
func writeEntry(locations []pgHistoryLocation, object, version string) (int, error) {
	if len(locations) != 1 {
		return 0, fmt.Errorf("the identity occurs %d times; want one ordinary entry: %+v", len(locations), locations)
	}
	entry := locations[0]
	if entry.Tier != "log" || entry.ExtraIndex != nil || entry.Op != "modify" || entry.ReturnCode == nil || *entry.ReturnCode != 0 ||
		entry.Object != object || entry.Version != version {
		return 0, fmt.Errorf("the identity is not the successful modify of %s at %s: %+v", object, version, entry)
	}
	return entry.Index, nil
}

// partialWriteShardObservation classifies one shard's own log and info. The
// full write created the head, so its entry must be held and must name every
// shard. The overwrite's entry is either not held, with the log ending at
// the full write, or held directly after the full write, naming exactly the
// expected shards, with the log ending there. A held entry may come with the
// partial-write record of every left-out shard, from the full write to the
// overwrite; a shard without the entry must hold no record. Every other
// shape contradicts the two acknowledged writes and is refused.
func partialWriteShardObservation(shard shardHistoryShard, full, patch []pgHistoryLocation, want partialWriteWant) (partialWriteShardHistory, error) {
	none := partialWriteShardHistory{}
	first, err := writeEntry(full, want.object, want.full)
	if err != nil {
		return none, fmt.Errorf("full write: %w", err)
	}
	if first >= len(shard.Entries) || shard.Entries[first].Version != want.full || len(shard.Entries[first].WrittenShards) != 0 {
		return none, fmt.Errorf("the full write's entry %d does not name every shard: %+v", first, shard.Entries)
	}
	if len(patch) == 0 {
		if shard.Head != want.full || first != len(shard.Entries)-1 {
			return none, fmt.Errorf("the overwrite's entry is not held but the log ends at %s, not at the full write %s", shard.Head, want.full)
		}
		if len(shard.PartialWrites) != 0 {
			return none, fmt.Errorf("a shard without the overwrite's entry holds partial-write records: %+v", shard.PartialWrites)
		}
		return none, nil
	}
	second, err := writeEntry(patch, want.object, want.patch)
	if err != nil {
		return none, fmt.Errorf("overwrite: %w", err)
	}
	named := want.written
	if len(named) == want.shards {
		// The pinned writer stores the empty set for every shard.
		named = nil
	}
	if second != first+1 || second != len(shard.Entries)-1 || shard.Head != want.patch || shard.Entries[second].Version != want.patch {
		return none, fmt.Errorf("the overwrite's entry %d is not the log's last entry directly after the full write's entry %d (head %s)", second, first, shard.Head)
	}
	if !slices.Equal(shard.Entries[second].WrittenShards, named) {
		return none, fmt.Errorf("the overwrite's entry names shards %v; want %v", shard.Entries[second].WrittenShards, named)
	}
	if len(shard.PartialWrites) == 0 {
		return partialWriteShardHistory{Patch: true}, nil
	}
	var records []shardPartialWrite
	for _, skipped := range want.skipped() {
		records = append(records, shardPartialWrite{Shard: skipped, From: want.full, To: want.patch})
	}
	if !slices.Equal(shard.PartialWrites, records) {
		return none, fmt.Errorf("partial-write records %+v; want %+v", shard.PartialWrites, records)
	}
	return partialWriteShardHistory{Patch: true, Recorded: true}, nil
}

// nativeStripeWidth reads the pool's stripe_width from the Go client's own
// authenticated MON map dump.
func nativeStripeWidth(dump []byte, pool int64) (uint64, error) {
	var osdMap struct {
		Pools []struct {
			ID          int64  `json:"pool"`
			StripeWidth uint64 `json:"stripe_width"`
		} `json:"pools"`
	}
	if err := json.Unmarshal(dump, &osdMap); err != nil {
		return 0, err
	}
	for _, p := range osdMap.Pools {
		if p.ID == pool && p.StripeWidth != 0 && p.StripeWidth <= 1<<22 {
			return p.StripeWidth, nil
		}
	}
	return 0, errors.New("the pool has no bounded stripe_width in the authenticated map")
}

const (
	partialWriteSlot   = 2
	partialWriteObject = "ec-partial-write-history"
	// The partial overwrite starts a few bytes into the object, inside shard
	// 0's chunk of the first stripe for every chunk size above 32 bytes.
	partialWriteOffset = 5
)

// partialWriteRun is one owned optimized isa pool with eight PGs and
// the single object that a partial-overwrite control writes through the Go
// client. Every request is sent once; a failed or unknown result stops the
// test and nothing is resent.
type partialWriteRun struct {
	t       *testing.T
	ctx     context.Context
	client  *cephmsgr.Client
	request func(osdFixtureOracle, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
	width   erasureWidth
	facts   erasureFacts
	// stripe is the pool's stripe_width from the Go client's own map dump.
	stripe uint64
	gid    uint64
	// route is the native placement the requests use and connection the
	// session to its primary.
	route      osdFixtureOracle
	connection *cephmsgr.OSDConnection
	// data is the object's expected content after the acknowledged writes.
	data []byte
}

// startPartialWriteRun creates the pool, reads its geometry from native
// sources only, requires the head to be absent and connects to the native
// primary. The pool and its profile are removed on cleanup.
func startPartialWriteRun(t *testing.T, width erasureWidth, timeout time.Duration) *partialWriteRun {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	if os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	fixture, hash := osdFixture, false
	run := &partialWriteRun{t: t, width: width, request: osdObjectRequest}
	if isReplicatedPGFixtureProfile() {
		fixture, run.request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	run.client, _, _, _ = fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	run.ctx = ctx
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned pools were not removed")
		}
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	t.Cleanup(cancel)
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": partialWriteSlot, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	run.facts = erasurePoolMapFacts(t, ctx, run.client, created, width, true)
	// The geometry comes from the native map and profile: the stripe width
	// from the Go client's authenticated map dump, the width and plugin from
	// the profile the oracle read back, and the shards that may not be
	// primary from the pool. isa defines no chunk mapping, and the profile
	// must not carry one, so shard i holds raw chunk i.
	dump, err := run.client.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("authenticated osd dump", err)
	}
	if run.stripe, err = nativeStripeWidth(dump.Data, created.PoolID); err != nil {
		t.Fatal(err)
	}
	if _, mapped := created.ECProfile["mapping"]; mapped || created.ECProfile["plugin"] != "isa" ||
		created.ECProfile["k"] != strconv.Itoa(width.k) || created.ECProfile["m"] != strconv.Itoa(width.m) {
		t.Fatalf("native profile %v is not an unmapped isa %s profile", created.ECProfile, width)
	}
	t.Logf("pool %d: stripe_width %d, chunk %d, profile k=%s m=%s plugin=%s, nonprimary shards %v",
		created.PoolID, run.stripe, run.stripe/uint64(width.k), created.ECProfile["k"], created.ECProfile["m"], created.ECProfile["plugin"], run.facts.nonprimary)
	if poolObserve(t, ctx, partialWriteSlot, partialWriteObject, "", "").Code != -2 {
		t.Fatal("the head already exists")
	}
	run.route = poolPlace(t, ctx, partialWriteSlot, partialWriteObject, "")
	if run.route.Pool != created.PoolID {
		t.Fatal("the object is not in the owned pool", run.route.PGID)
	}
	run.facts.checkPlacement(t, run.route)
	run.connection = mutationConnection(t, run.client, ctx, run.route)
	t.Cleanup(func() { run.connection.Close() })
	run.gid = run.client.Snapshot().GlobalID
	return run
}

// plan restates which shards the pinned primary names for an overwrite of
// the object's current bytes that is not the first write of an interval.
func (r *partialWriteRun) plan(offset uint64, length int) (written, skipped []int) {
	r.t.Helper()
	written, skipped, err := partialWritePlan(r.width.k, r.width.m, r.facts.nonprimary, r.stripe, uint64(len(r.data)), offset, uint64(length))
	if err != nil {
		r.t.Fatal(err)
	}
	return written, skipped
}

// send issues one Go request exactly once under its own incarnation and
// requires an acknowledged ONDISK reply that keeps the submission's identity.
func (r *partialWriteRun) send(label string, incarnation uint32, seconds uint32, op cephmsgr.OSDOperation) cephmsgr.OSDReply {
	r.t.Helper()
	message := r.request(r.route, op)
	message.ClientIncarnation = incarnation
	message.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
	reply, err := r.connection.Request(r.ctx, message)
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		r.t.Fatalf("%s: err=%v code=%d flags=%#x; not resent", label, err, reply.Code, reply.Flags)
	}
	if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != r.gid || reply.Submission.ClientIncarnation != incarnation ||
		reply.Submission.Attempt != 0 || reply.RetryAttempt != 0 || reply.Object != r.route.Name || reply.PG.Pool != uint64(r.route.Pool) || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 {
		r.t.Fatal("reply identity differs from its submission", label, reply.Transaction, reply.Submission)
	}
	return reply
}

// observe is the independent native readback: the object holds exactly the
// expected bytes at the reply's user version.
func (r *partialWriteRun) observe(label string, reply cephmsgr.OSDReply) {
	r.t.Helper()
	if native := poolObserve(r.t, r.ctx, partialWriteSlot, partialWriteObject, "", ""); native.Code != 0 || !bytes.Equal(native.Data, r.data) || native.Version != reply.UserVersion || reply.UserVersion == 0 {
		r.t.Fatalf("native object after the %s differs: code=%d, %d bytes, version %d, reply version %d", label, native.Code, len(native.Data), native.Version, reply.UserVersion)
	}
}

// overwrite sends one WRITE inside the object and reads the result back
// natively.
func (r *partialWriteRun) overwrite(label string, incarnation uint32, seconds uint32, offset uint64, patch []byte) cephmsgr.OSDReply {
	r.t.Helper()
	reply := r.send(label, incarnation, seconds, cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: uint64(len(patch)), Data: patch})
	copy(r.data[offset:], patch)
	r.observe(label, reply)
	return reply
}

func (r *partialWriteRun) key(incarnation int32, reply cephmsgr.OSDReply) pgHistoryTarget {
	return pgHistoryTarget{Type: "client", Num: r.gid, Inc: incarnation, TID: reply.Transaction}
}

func replayVersion(reply cephmsgr.OSDReply) string {
	return fmt.Sprintf("%d'%d", reply.ReplayVersion.Epoch, reply.ReplayVersion.Version)
}

// capture takes one bracketed snapshot of the live primary's log and then one
// shard-local capture of every acting shard's own store, with no subject
// request in between. The map epoch must not move between the two. None of
// the near-miss identities may have a position anywhere, each shard's dumps
// are rechecked in Go, nothing may have been trimmed or copied, and the
// primary's store copy must repeat its live answer. past lists the shards
// whose log head may be newer than their last entry; nil admits none.
func (r *partialWriteRun) capture(keys, misses []pgHistoryTarget, past []int) (pgHistorySnapshot, shardHistorySnapshot) {
	r.t.Helper()
	targets := append(slices.Clone(keys), misses...)
	primary := primaryPGHistory(r.t, r.ctx, partialWriteSlot, partialWriteObject, targets)
	requirePGHistoryOnRoute(r.t, primary, r.route)
	for i := range misses {
		if got := primary.Targets[len(keys)+i]; len(got.Locations) != 0 {
			r.t.Fatalf("near-miss identity %s matched on the primary: %+v", got.Reqid, got.Locations)
		}
	}
	shards := shardPGHistory(r.t, r.ctx, partialWriteSlot, partialWriteObject, r.route, targets, past)
	if shards.Before.Epoch != primary.After.Epoch {
		r.t.Fatalf("map epoch moved from %d to %d between the primary snapshot and the shard capture", primary.After.Epoch, shards.Before.Epoch)
	}
	for i, shard := range shards.Shards {
		if err := checkShardDumps(shard); err != nil {
			r.t.Fatalf("shard %s (osd.%d): %v", shard.SPG, shard.OSD, err)
		}
		if info, _ := base64.StdEncoding.DecodeString(shard.InfoRaw); len(info) <= 16<<10 {
			r.t.Logf("complete offline info dump of %s: %s", shard.SPG, info)
		}
		for j := range misses {
			if got := shard.Targets[len(keys)+j]; len(got.Locations) != 0 {
				r.t.Fatalf("near-miss identity %s matched on shard %s: %+v", got.Reqid, shard.SPG, got.Locations)
			}
		}
		if shard.DupEntries != 0 || shard.ExtraReqidEntries != 0 {
			r.t.Fatalf("shard %s holds %d duplicates and %d extra request IDs; nothing was trimmed or copied", shard.SPG, shard.DupEntries, shard.ExtraReqidEntries)
		}
		if i == int(r.route.Shard) {
			// The primary's offline copy must repeat its live answer.
			if shard.OSD != primary.OSD || shard.Head != primary.Head || shard.Tail != primary.Tail || shard.LogEntries != primary.LogEntries ||
				shard.DupEntries != primary.DupEntries || !reflect.DeepEqual(shard.Targets, primary.Targets) {
				r.t.Fatalf("the primary's store copy %s differs from its live PG log: %+v vs live head %s tail %s", shard.SPG, shard.Targets, primary.Head, primary.Tail)
			}
		}
	}
	return primary, shards
}

// partialOverwrite is the acknowledged state every partial-overwrite control
// starts from: one full write and one overwrite confined to shard 0's chunk.
type partialOverwrite struct {
	full, patched     cephmsgr.OSDReply
	fullKey, patchKey pgHistoryTarget
	// misses are near-miss identities that must have no position anywhere.
	misses []pgHistoryTarget
	want   partialWriteWant
	// shards is the capture taken after the overwrite.
	shards shardHistorySnapshot
}

// partialOverwrite writes two full stripes to the fresh head and then
// overwrites a few bytes inside shard 0's chunk of the first stripe. Native
// readback checks the bytes after each write and the live primary must hold
// both requests by their full identity. Then every acting shard's store is
// copied while its container is paused and read offline, between two
// identical native placement records, and the interval the requests were
// sent in must still hold after the capture (shardPGHistory). The full write's entry must be held by
// every shard and name every shard. The overwrite's entry must be held by
// shard 0 and by the parity shards and name exactly those; the nonprimary
// data shard, which the overwrite does not touch, must not hold it in this
// capture and its own log must end at the full write.
func (r *partialWriteRun) partialOverwrite() partialOverwrite {
	r.t.Helper()
	const (
		fullIncarnation  = 7
		patchIncarnation = 9
	)
	patch := []byte("partial\x00\xffpatch")
	data := make([]byte, 2*r.stripe)
	for i := range data {
		data[i] = byte(i*7 + i>>8)
	}
	full := r.send("full write", fullIncarnation, 1700009100, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
	r.data = data
	r.observe("full write", full)
	written, skipped := r.plan(partialWriteOffset, len(patch))
	if !slices.Equal(skipped, r.facts.nonprimary) || len(skipped) == 0 {
		r.t.Fatalf("the overwrite plan names shards %v and leaves out %v; this control needs the nonprimary shards %v left out", written, skipped, r.facts.nonprimary)
	}
	r.t.Logf("overwrite [%d,+%d) of %d bytes names shards %v and leaves out %v", partialWriteOffset, len(patch), len(data), written, skipped)
	// Independent native readback of the final bytes at the overwrite's
	// user version.
	patched := r.overwrite("partial overwrite", patchIncarnation, 1700009101, partialWriteOffset, patch)
	if patched.UserVersion <= full.UserVersion {
		r.t.Fatal("the overwrite did not advance the user version", full.UserVersion, patched.UserVersion)
	}
	state := partialOverwrite{full: full, patched: patched, fullKey: r.key(fullIncarnation, full), patchKey: r.key(patchIncarnation, patched)}
	if full.Transaction == patched.Transaction {
		r.t.Fatal("the two requests share a transaction ID", state.fullKey, state.patchKey)
	}
	// Near misses: each request's real TID under the other request's
	// incarnation, then one changed field of the overwrite's key.
	state.misses = []pgHistoryTarget{
		{Type: "client", Num: r.gid, Inc: fullIncarnation, TID: patched.Transaction},
		{Type: "client", Num: r.gid, Inc: patchIncarnation, TID: full.Transaction},
		{Type: "client", Num: r.gid + 1, Inc: patchIncarnation, TID: patched.Transaction},
		{Type: "osd", Num: r.gid, Inc: patchIncarnation, TID: patched.Transaction},
	}
	state.want = partialWriteWant{object: hobjectHead(r.route), full: replayVersion(full), patch: replayVersion(patched), shards: r.facts.size, written: written}
	want := state.want
	r.t.Logf("full write %s at %s, user version %d; partial overwrite %s at %s, user version %d", state.fullKey, want.full, full.UserVersion, state.patchKey, want.patch, patched.UserVersion)

	// The live primary first: both requests are ordinary entries of the
	// head. Then every acting shard's own store.
	primary, shards := r.capture([]pgHistoryTarget{state.fullKey, state.patchKey}, state.misses, nil)
	for i, expected := range []string{want.full, want.patch} {
		if _, err := writeEntry(primary.Targets[i].Locations, want.object, expected); err != nil {
			r.t.Fatalf("live primary, %s: %v", primary.Targets[i].Reqid, err)
		}
	}
	if primary.Head != want.patch {
		r.t.Fatalf("the live primary's log ends at %s, not at the overwrite %s", primary.Head, want.patch)
	}
	for i, shard := range shards.Shards {
		// The capture selected and the store returned collection <pgid>s<i>,
		// so acting index i is shard i; the role follows from the native
		// width and nonprimary shards, never from the OSD's position among
		// the wire targets.
		role := erasureShardRole(i, r.width.k, r.facts.nonprimary)
		observed, err := partialWriteShardObservation(shard, shard.Targets[0].Locations, shard.Targets[1].Locations, want)
		if err != nil {
			r.t.Fatalf("shard %s (osd.%d, %s) contradicts the two acknowledged writes: %v", shard.SPG, shard.OSD, role, err)
		}
		r.t.Logf("shard %s (osd.%d, %s): %s; log head %s, last_update %s, entries %+v, partial-write records %+v at epoch %d",
			shard.SPG, shard.OSD, role, observed, shard.Head, shard.LastUpdate, shard.Entries, shard.PartialWrites, shard.PartialWritesEpoch)
		// The overwrite names shard 0, the parity shards and the data shards
		// it touches; the primary builds a sub-write for each of them and the
		// reply waits for every one (pinned src/osd/ECCommon.cc:836-846). A
		// shard it names must therefore hold the entry. A shard it does not
		// name has an empty transaction and gets no sub-write, so its own
		// log still ends at the full write.
		if named := slices.Contains(written, i); named != observed.Patch {
			r.t.Fatalf("shard %s (osd.%d, %s): %s; the overwrite names shards %v", shard.SPG, shard.OSD, role, observed, written)
		}
	}
	state.shards = shards
	return state
}

// erasurePartialWriteShardHistoryScenario: the Go client writes two full
// stripes to a fresh head of an owned optimized isa pool and then overwrites
// a few bytes inside shard 0's chunk of the first stripe, which leaves every
// nonprimary data shard out (one on k=2, two on k=3, three on k=4);
// partialOverwrite states what is required of the primary and of every
// acting shard's own log afterwards. This is a control of what each shard's
// log holds after an acknowledged partial write: it stops before any
// trimming, peering or resubmission and claims nothing about eviction, loss
// or replay. One overwrite runs per width (a 14-byte overwrite at offset 5
// of a two-stripe object, the stripe width from the native map); other
// offsets, appends, truncates, a write that leaves out only some nonprimary
// shards and plugins with a chunk mapping are not run natively.
func erasurePartialWriteShardHistoryScenario(t *testing.T, width erasureWidth) {
	startPartialWriteRun(t, width, 420*time.Second).partialOverwrite()
}

// TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration is erasurePartialWriteShardHistoryScenario on optimized isa
// k=2,m=2 (four OSDs; nonprimary shard {1}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwriteshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated pauses each acting OSD for a store copy; no wait for every PG clean without pg_temp afterwards
func TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration(t *testing.T) {
	erasurePartialWriteShardHistoryScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1PartialWriteShardHistoryIntegration is erasurePartialWriteShardHistoryScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1partialwriteshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated pauses each acting OSD for a store copy; no wait for every PG clean without pg_temp afterwards
func TestCephOSDErasureOptimizedK3M1PartialWriteShardHistoryIntegration(t *testing.T) {
	erasurePartialWriteShardHistoryScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2PartialWriteShardHistoryIntegration is erasurePartialWriteShardHistoryScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2partialwriteshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated pauses each acting OSD for a store copy; no wait for every PG clean without pg_temp afterwards
func TestCephOSDErasureOptimizedK4M2PartialWriteShardHistoryIntegration(t *testing.T) {
	erasurePartialWriteShardHistoryScenario(t, erasureK4M2)
}

func TestPartialWritePlan(t *testing.T) {
	// Chunk 4096: stripe 0 is shard 0 [0,4096) and shard 1 [4096,8192).
	for name, c := range map[string]struct {
		k, m             int
		nonprimary       []int
		stripe, size     uint64
		offset, length   uint64
		written, skipped []int
	}{
		"inside shard 0's chunk":        {2, 2, []int{1}, 8192, 16384, 5, 13, []int{0, 2, 3}, []int{1}},
		"last byte of shard 0's chunk":  {2, 2, []int{1}, 8192, 16384, 4095, 1, []int{0, 2, 3}, []int{1}},
		"shard 0's chunk, next stripe":  {2, 2, []int{1}, 8192, 16384, 8192, 4096, []int{0, 2, 3}, []int{1}},
		"first byte of shard 1's chunk": {2, 2, []int{1}, 8192, 16384, 4096, 1, []int{0, 1, 2, 3}, nil},
		"shard 1's chunk, next stripe":  {2, 2, []int{1}, 8192, 16384, 12288, 4096, []int{0, 1, 2, 3}, nil},
		"across both chunks":            {2, 2, []int{1}, 8192, 16384, 4090, 13, []int{0, 1, 2, 3}, nil},
		"across a stripe boundary":      {2, 2, []int{1}, 8192, 16384, 8190, 4, []int{0, 1, 2, 3}, nil},
		"whole object":                  {2, 2, []int{1}, 8192, 16384, 0, 16384, []int{0, 1, 2, 3}, nil},
		"k=4 touching shard 2 only":     {4, 2, []int{1, 2, 3}, 16384, 16384, 8192, 4096, []int{0, 2, 4, 5}, []int{1, 3}},
		"k=4 touching shards 1 and 2":   {4, 2, []int{1, 2, 3}, 16384, 16384, 8191, 2, []int{0, 1, 2, 4, 5}, []int{3}},
		"k=2,m=1":                       {2, 1, []int{1}, 8192, 8192, 0, 1, []int{0, 2}, []int{1}},
		"k=3 inside shard 0's chunk":    {3, 1, []int{1, 2}, 12288, 24576, 5, 13, []int{0, 3}, []int{1, 2}},
		"k=3 shard 1's chunk only":      {3, 1, []int{1, 2}, 12288, 24576, 4101, 13, []int{0, 1, 3}, []int{2}},
		"k=4 inside shard 0's chunk":    {4, 2, []int{1, 2, 3}, 16384, 32768, 5, 13, []int{0, 4, 5}, []int{1, 2, 3}},
	} {
		written, skipped, err := partialWritePlan(c.k, c.m, c.nonprimary, c.stripe, c.size, c.offset, c.length)
		if err != nil || !slices.Equal(written, c.written) || !slices.Equal(skipped, c.skipped) {
			t.Errorf("%s: written %v skipped %v err %v", name, written, skipped, err)
		}
	}
	for name, c := range map[string]struct {
		k, m           int
		nonprimary     []int
		stripe, size   uint64
		offset, length uint64
	}{
		"no nonprimary shard":        {2, 2, nil, 8192, 16384, 5, 13},
		"another nonprimary shard":   {2, 2, []int{2}, 8192, 16384, 5, 13},
		"nonprimary shard 0":         {2, 2, []int{0, 1}, 8192, 16384, 5, 13},
		"k=4 with one nonprimary":    {4, 2, []int{1}, 16384, 16384, 5, 13},
		"empty overwrite":            {2, 2, []int{1}, 8192, 16384, 5, 0},
		"append":                     {2, 2, []int{1}, 8192, 16384, 16384, 1},
		"overwrite growing the size": {2, 2, []int{1}, 8192, 16384, 16380, 5},
		"offset overflow":            {2, 2, []int{1}, 8192, 16384, 1<<64 - 1, 2},
		"no stripe width":            {2, 2, []int{1}, 0, 16384, 5, 13},
		"stripe width not k chunks":  {2, 2, []int{1}, 8191, 16384, 5, 13},
		"one data shard":             {1, 2, nil, 4096, 16384, 5, 13},
		"no parity shard":            {2, 0, []int{1}, 8192, 16384, 5, 13},
		"more shards than the bound": {6, 3, []int{1, 2, 3, 4, 5}, 24576, 24576, 5, 13},
	} {
		if written, skipped, err := partialWritePlan(c.k, c.m, c.nonprimary, c.stripe, c.size, c.offset, c.length); err == nil || written != nil || skipped != nil {
			t.Errorf("plan accepted: %s: %v %v", name, written, skipped)
		}
	}
	for shard, role := range []string{"data", "nonprimary data", "parity", "parity"} {
		if got := erasureShardRole(shard, 2, []int{1}); got != role {
			t.Errorf("shard %d role %q, want %q", shard, got, role)
		}
	}
	if got := erasureShardRole(1, 2, nil); got != "data" {
		t.Error("a data shard of a pool without nonprimary shards:", got)
	}
	if width, err := nativeStripeWidth([]byte(`{"pools":[{"pool":3,"stripe_width":4096},{"pool":4,"stripe_width":8192}]}`), 4); err != nil || width != 8192 {
		t.Error("stripe width of pool 4:", width, err)
	}
	for name, dump := range map[string]string{
		"another pool":   `{"pools":[{"pool":3,"stripe_width":8192}]}`,
		"replicated":     `{"pools":[{"pool":4,"stripe_width":0}]}`,
		"no stripe":      `{"pools":[{"pool":4}]}`,
		"over the bound": `{"pools":[{"pool":4,"stripe_width":4194305}]}`,
		"not JSON":       `{"pools":[`,
	} {
		if width, err := nativeStripeWidth([]byte(dump), 4); err == nil || width != 0 {
			t.Error("stripe width accepted:", name, width)
		}
	}
}

// partialWriteShard builds one synthetic shard receipt; no daemon produced
// it. held selects whether the overwrite's entry is in the shard's log.
func partialWriteShard(held, recorded bool) (shardHistoryShard, []pgHistoryLocation, []pgHistoryLocation) {
	zero := 0
	object := "4:53c99ff5:::obj:head"
	full := []pgHistoryLocation{{Tier: "log", Index: 2, Version: "28'3", Op: "modify", Object: object, ReturnCode: &zero}}
	index := 1
	shard := shardHistoryShard{Index: index, Shard: &index, OSD: 3, SPG: "4.2s1", Head: "28'3", Tail: "0'0", LastEntry: "28'3", LastUpdate: "28'3", LastComplete: "28'3", LogEntries: 3, LimitBytes: 64 << 10,
		Entries: []shardLogEntry{{Version: "28'1", WrittenShards: []int{}}, {Version: "28'2", WrittenShards: []int{}}, {Version: "28'3", WrittenShards: []int{}}}}
	var patch []pgHistoryLocation
	log := `{"version":"28'1","written_shards":"{}"},{"version":"28'2","written_shards":"{}"},{"version":"28'3","written_shards":"{}"}`
	records := ""
	if held {
		patch = []pgHistoryLocation{{Tier: "log", Index: 3, Version: "28'4", Op: "modify", Object: object, ReturnCode: &zero}}
		shard.Head, shard.LastEntry, shard.LastUpdate, shard.LastComplete, shard.LogEntries = "28'4", "28'4", "28'4", "28'4", 4
		shard.Entries = append(shard.Entries, shardLogEntry{Version: "28'4", WrittenShards: []int{0, 2, 3}})
		log += `,{"version":"28'4","written_shards":"{0,2,3}"}`
	}
	if recorded {
		shard.PartialWrites, shard.PartialWritesEpoch = []shardPartialWrite{{Shard: 1, From: "28'3", To: "28'4"}}, 28
		records = `{"id":1,"from":"28'3","to":"28'4"}`
	}
	setShardDumps(&shard, `{"pg_log_t":{"head":"`+shard.Head+`","tail":"0'0","log":[`+log+`]}}`,
		`{"pgid":"4.2s1","shard":"1","last_update":"`+shard.Head+`","last_complete":"`+shard.Head+`","partial_writes_last_complete":[`+records+`]}`)
	return shard, full, patch
}

// advancedShard builds the synthetic receipt of a nonprimary shard after a
// peering that followed a partial write it was left out of: its info and its
// log head are at the overwrite, its last entry is the full write and its
// own info holds the range between them. No daemon produced it.
func advancedShard() shardHistoryShard {
	shard, _, _ := partialWriteShard(false, false)
	shard.Head, shard.LastUpdate, shard.LastComplete, shard.HeadPastEntries = "28'4", "28'4", "28'4", true
	shard.PartialWrites, shard.PartialWritesEpoch = []shardPartialWrite{{Shard: 1, From: "28'3", To: "28'4"}}, 30
	setShardDumps(&shard, advancedShardLog, advancedShardInfo)
	return shard
}

const (
	advancedShardLog  = `{"pg_log_t":{"head":"28'4","tail":"0'0","log":[{"version":"28'1","written_shards":"{}"},{"version":"28'2","written_shards":"{}"},{"version":"28'3","written_shards":"{}"}]}}`
	advancedShardInfo = `{"pgid":"4.2s1","shard":"1","last_update":"28'4","last_complete":"28'4","partial_writes_last_complete":[{"id":1,"from":"28'3","to":"28'4"}]}`
)

func setShardDumps(shard *shardHistoryShard, log, info string) {
	digest := sha256.Sum256([]byte(info))
	shard.Raw = base64.StdEncoding.EncodeToString([]byte(log))
	shard.InfoRaw, shard.InfoBytes, shard.InfoSHA256 = base64.StdEncoding.EncodeToString([]byte(info)), len(info), hex.EncodeToString(digest[:])
}

func TestPartialWriteShardObservation(t *testing.T) {
	want := partialWriteWant{object: "4:53c99ff5:::obj:head", full: "28'3", patch: "28'4", shards: 4, written: []int{0, 2, 3}}
	if !slices.Equal(want.skipped(), []int{1}) {
		t.Fatal("left-out shards", want.skipped())
	}
	// The three observations a shard may show.
	for name, c := range map[string]struct {
		held, recorded bool
		want           partialWriteShardHistory
		text           string
	}{
		"not held":          {false, false, partialWriteShardHistory{}, "overwrite entry not held, log ends at the full write"},
		"held":              {true, false, partialWriteShardHistory{Patch: true}, "overwrite entry held, no partial-write record"},
		"held and recorded": {true, true, partialWriteShardHistory{Patch: true, Recorded: true}, "overwrite entry held, partial-write record present"},
	} {
		shard, full, patch := partialWriteShard(c.held, c.recorded)
		got, err := partialWriteShardObservation(shard, full, patch, want)
		if err != nil || got != c.want || got.String() != c.text {
			t.Errorf("%s: %+v %q %v", name, got, got, err)
		}
	}
	// An overwrite that names every shard is dumped with the empty set.
	every := partialWriteWant{object: want.object, full: "28'3", patch: "28'4", shards: 4, written: []int{0, 1, 2, 3}}
	shard, full, patch := partialWriteShard(true, false)
	shard.Entries[3].WrittenShards = []int{}
	if got, err := partialWriteShardObservation(shard, full, patch, every); err != nil || !got.Patch || got.Recorded {
		t.Error("an overwrite naming every shard:", got, err)
	}
	if got, err := partialWriteShardObservation(shard, full, patch, want); err == nil || got.Patch {
		t.Error("an entry naming every shard accepted for a three-shard plan")
	}
	failed, one := -5, 1
	type edit func(*shardHistoryShard, *[]pgHistoryLocation, *[]pgHistoryLocation)
	// Everything else contradicts the two acknowledged writes and is never
	// reported as an entry that is merely not held.
	for name, c := range map[string]struct {
		held, recorded bool
		edit           edit
	}{
		"full write missing":                {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { *f = nil }},
		"full write twice":                  {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { *f = append(*f, (*f)[0]) }},
		"full write only a duplicate":       {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].Tier = "dups" }},
		"full write as an extra request ID": {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].ExtraIndex = &one }},
		"full write failed":                 {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].ReturnCode = &failed }},
		"full write without a return code":  {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].ReturnCode = nil }},
		"full write is a delete":            {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].Op = "delete" }},
		"full write on another object":      {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].Object = "4:53c99ff5:::other:head" }},
		"full write at another version":     {false, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].Version = "28'2" }},
		"full write names three shards":     {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[2].WrittenShards = []int{0, 2, 3} }},
		"full write beyond the entry list":  {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries = s.Entries[:2] }},
		"entry list at another version":     {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[2].Version = "28'2" }},
		"not held, log ends later":          {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Head = "28'4" }},
		"not held, log ends earlier":        {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Head = "28'2" }},
		"not held, another entry follows": {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) {
			s.Entries = append(s.Entries, shardLogEntry{Version: "28'5"})
		}},
		"not held, but a record is": {false, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) {
			s.PartialWrites = []shardPartialWrite{{Shard: 1, From: "28'3", To: "28'4"}}
		}},
		"overwrite twice":                 {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { *p = append(*p, (*p)[0]) }},
		"overwrite only a duplicate":      {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { (*p)[0].Tier = "dups" }},
		"overwrite failed":                {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { (*p)[0].ReturnCode = &failed }},
		"overwrite on another object":     {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { (*p)[0].Object = "4:53c99ff5:::other:head" }},
		"overwrite at another version":    {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { (*p)[0].Version = "28'5" }},
		"overwrite before the full write": {true, false, func(_ *shardHistoryShard, _, p *[]pgHistoryLocation) { (*p)[0].Index = 1 }},
		"overwrite not directly after":    {true, false, func(_ *shardHistoryShard, f, _ *[]pgHistoryLocation) { (*f)[0].Index = 1 }},
		"another entry before the overwrite": {true, false, func(s *shardHistoryShard, _, p *[]pgHistoryLocation) {
			s.Entries = []shardLogEntry{s.Entries[0], s.Entries[1], s.Entries[2], s.Entries[2], s.Entries[3]}
			(*p)[0].Index = 4
		}},
		"overwrite is not the last entry": {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) {
			s.Entries = append(s.Entries, shardLogEntry{Version: "28'5"})
		}},
		"log ends later":                  {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Head = "28'5" }},
		"entry list at another version 2": {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[3].Version = "28'5" }},
		"overwrite names every shard":     {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[3].WrittenShards = []int{} }},
		"overwrite names the left-out shard": {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) {
			s.Entries[3].WrittenShards = []int{0, 1, 2, 3}
		}},
		"overwrite names another set":  {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[3].WrittenShards = []int{0, 1, 3} }},
		"overwrite names fewer shards": {true, false, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.Entries[3].WrittenShards = []int{0, 2} }},
		"record for another shard":     {true, true, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.PartialWrites[0].Shard = 2 }},
		"record from another version":  {true, true, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.PartialWrites[0].From = "28'2" }},
		"record to another version":    {true, true, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.PartialWrites[0].To = "28'5" }},
		"record of an empty range":     {true, true, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) { s.PartialWrites[0].From = "28'4" }},
		"a second record": {true, true, func(s *shardHistoryShard, _, _ *[]pgHistoryLocation) {
			s.PartialWrites = append(s.PartialWrites, shardPartialWrite{Shard: 3, From: "28'3", To: "28'4"})
		}},
	} {
		shard, full, patch := partialWriteShard(c.held, c.recorded)
		c.edit(&shard, &full, &patch)
		if got, err := partialWriteShardObservation(shard, full, patch, want); err == nil || got != (partialWriteShardHistory{}) {
			t.Errorf("contradictory shard history classified as %q: %s", got, name)
		}
	}
}

func TestCheckShardDumps(t *testing.T) {
	for _, c := range [][2]bool{{false, false}, {true, false}, {true, true}} {
		shard, _, _ := partialWriteShard(c[0], c[1])
		if err := checkShardDumps(shard); err != nil {
			t.Errorf("consistent receipt (held %t, recorded %t) refused: %v", c[0], c[1], err)
		}
	}
	const (
		log  = `{"pg_log_t":{"head":"28'4","tail":"0'0","log":[{"version":"28'1","written_shards":"{}"},{"version":"28'2","written_shards":"{}"},{"version":"28'3","written_shards":"{}"},{"version":"28'4","written_shards":"{0,2,3}"}]}}`
		info = `{"pgid":"4.2s1","shard":"1","last_update":"28'4","last_complete":"28'4","partial_writes_last_complete":[{"id":1,"from":"28'3","to":"28'4"}]}`
	)
	for name, edit := range map[string]func(*shardHistoryShard){
		"info is not base64":             func(s *shardHistoryShard) { s.InfoRaw += "%" },
		"info is empty":                  func(s *shardHistoryShard) { setShardDumps(s, log, "") },
		"info length differs":            func(s *shardHistoryShard) { s.InfoBytes++ },
		"info digest differs":            func(s *shardHistoryShard) { s.InfoSHA256 = strings.Repeat("0", 64) },
		"info over its bound":            func(s *shardHistoryShard) { s.LimitBytes = s.InfoBytes - 1 },
		"info replaced after the digest": func(s *shardHistoryShard) { s.InfoRaw = base64.StdEncoding.EncodeToString([]byte(info + " ")) },
		"info is not JSON":               func(s *shardHistoryShard) { setShardDumps(s, log, info[:len(info)-1]) },
		"info of another shard":          func(s *shardHistoryShard) { setShardDumps(s, log, strings.Replace(info, "4.2s1", "4.2s0", 1)) },
		"info names another shard": func(s *shardHistoryShard) {
			setShardDumps(s, log, strings.Replace(info, `"shard":"1"`, `"shard":"0"`, 1))
		},
		"info names no shard":            func(s *shardHistoryShard) { setShardDumps(s, log, strings.Replace(info, `"shard":"1",`, "", 1)) },
		"receipt names another shard":    func(s *shardHistoryShard) { zero := 0; s.Shard = &zero },
		"receipt names no shard":         func(s *shardHistoryShard) { s.Shard = nil },
		"capture selected another index": func(s *shardHistoryShard) { s.Index = 2 },
		"parser missed a record":         func(s *shardHistoryShard) { s.PartialWrites = nil },
		"parser invented a record":       func(s *shardHistoryShard) { s.PartialWrites = append(s.PartialWrites, s.PartialWrites[0]) },
		"record shard differs":           func(s *shardHistoryShard) { s.PartialWrites[0].Shard = 2 },
		"record start differs":           func(s *shardHistoryShard) { s.PartialWrites[0].From = "28'2" },
		"record end differs":             func(s *shardHistoryShard) { s.PartialWrites[0].To = "28'5" },
		"dump record without a shard":    func(s *shardHistoryShard) { setShardDumps(s, log, strings.Replace(info, `"id":1,`, "", 1)) },
		"log is not base64":              func(s *shardHistoryShard) { s.Raw += "%" },
		"log is not JSON":                func(s *shardHistoryShard) { setShardDumps(s, log[:len(log)-1], info) },
		"parser missed an entry":         func(s *shardHistoryShard) { s.Entries = s.Entries[:3] },
		"parser invented an entry":       func(s *shardHistoryShard) { s.Entries = append(s.Entries, s.Entries[3]) },
		"entry count differs":            func(s *shardHistoryShard) { s.LogEntries = 3 },
		"entry version differs":          func(s *shardHistoryShard) { s.Entries[3].Version = "28'5" },
		"parser dropped a named shard":   func(s *shardHistoryShard) { s.Entries[3].WrittenShards = []int{0, 2} },
		"parser read every shard":        func(s *shardHistoryShard) { s.Entries[3].WrittenShards = nil },
		"parser invented a named shard":  func(s *shardHistoryShard) { s.Entries[2].WrittenShards = []int{0} },
		"dump entry without written_shards": func(s *shardHistoryShard) {
			setShardDumps(s, strings.Replace(log, `,"written_shards":"{0,2,3}"`, "", 1), info)
		},
		"dump holds one entry more": func(s *shardHistoryShard) {
			setShardDumps(s, strings.Replace(log, "]}}", `,{"version":"28'5","written_shards":"{}"}]}}`, 1), info)
		},
		"dump holds one entry less": func(s *shardHistoryShard) {
			setShardDumps(s, strings.Replace(log, `,{"version":"28'4","written_shards":"{0,2,3}"}`, "", 1), info)
		},
		"dump entry in another order": func(s *shardHistoryShard) { setShardDumps(s, strings.Replace(log, "{0,2,3}", "{2,0,3}", 1), info) },
		// The bounds, read from the dumps themselves.
		"info at another last_update": func(s *shardHistoryShard) {
			setShardDumps(s, log, strings.Replace(info, `"last_update":"28'4"`, `"last_update":"28'5"`, 1))
		},
		"info at another last_complete": func(s *shardHistoryShard) {
			setShardDumps(s, log, strings.Replace(info, `"last_complete":"28'4"`, `"last_complete":"28'3"`, 1))
		},
		"info without last_update": func(s *shardHistoryShard) {
			setShardDumps(s, log, strings.Replace(info, `"last_update":"28'4",`, "", 1))
		},
		"receipt at another update":   func(s *shardHistoryShard) { s.LastUpdate = "28'3" },
		"receipt at another complete": func(s *shardHistoryShard) { s.LastComplete = "28'3" },
		"dump at another head": func(s *shardHistoryShard) {
			setShardDumps(s, strings.Replace(log, `"head":"28'4"`, `"head":"28'5"`, 1), info)
		},
		"dump without a head": func(s *shardHistoryShard) { setShardDumps(s, strings.Replace(log, `"head":"28'4",`, "", 1), info) },
		"dump at another tail": func(s *shardHistoryShard) {
			setShardDumps(s, strings.Replace(log, `"tail":"0'0"`, `"tail":"28'1"`, 1), info)
		},
		"receipt at another head":       func(s *shardHistoryShard) { s.Head = "28'5" },
		"receipt at another tail":       func(s *shardHistoryShard) { s.Tail = "28'1" },
		"receipt at another last entry": func(s *shardHistoryShard) { s.LastEntry = "28'3" },
		"receipt without a last entry":  func(s *shardHistoryShard) { s.LastEntry = "" },
		"head past entries claimed":     func(s *shardHistoryShard) { s.HeadPastEntries = true },
	} {
		shard, _, _ := partialWriteShard(true, true)
		edit(&shard)
		if err := checkShardDumps(shard); err == nil {
			t.Error("shard dumps accepted:", name)
		}
	}
	// A head newer than the last entry: accepted only as reported, with
	// this shard's own record from the last entry or earlier to the head.
	if err := checkShardDumps(advancedShard()); err != nil {
		t.Error("advanced nonprimary shard refused:", err)
	}
	earlier := advancedShard()
	earlier.PartialWrites[0].From = "28'2"
	setShardDumps(&earlier, advancedShardLog, strings.Replace(advancedShardInfo, `"from":"28'3"`, `"from":"28'2"`, 1))
	if err := checkShardDumps(earlier); err != nil {
		t.Error("advanced shard with a range from an earlier version refused:", err)
	}
	record := func(from, to string) func(*shardHistoryShard) {
		return func(s *shardHistoryShard) {
			s.PartialWrites[0].From, s.PartialWrites[0].To = from, to
			setShardDumps(s, advancedShardLog, strings.Replace(advancedShardInfo, `"from":"28'3","to":"28'4"`, `"from":"`+from+`","to":"`+to+`"`, 1))
		}
	}
	for name, edit := range map[string]func(*shardHistoryShard){
		"not reported as past the entries": func(s *shardHistoryShard) { s.HeadPastEntries = false },
		"receipt's last entry is the head": func(s *shardHistoryShard) { s.LastEntry = "28'4" },
		"no record": func(s *shardHistoryShard) {
			s.PartialWrites = nil
			setShardDumps(s, advancedShardLog, strings.Replace(advancedShardInfo, `{"id":1,"from":"28'3","to":"28'4"}`, "", 1))
		},
		"record of another shard only": func(s *shardHistoryShard) {
			s.PartialWrites[0].Shard = 2
			setShardDumps(s, advancedShardLog, strings.Replace(advancedShardInfo, `"id":1`, `"id":2`, 1))
		},
		"record ends before the head":           record("28'2", "28'3"),
		"record ends after the head":            record("28'3", "28'5"),
		"record ends in a later epoch":          record("28'3", "29'4"),
		"record starts after the last entry":    record("28'4", "28'4"),
		"record starts in a later epoch":        record("29'1", "28'4"),
		"record start unparsable":               record("28.3", "28'4"),
		"record start without a version":        record("28'", "28'4"),
		"record start with a negative version":  record("28'-1", "28'4"),
		"record start beyond the epoch's range": record("4294967296'1", "28'4"),
		"log without an entry": func(s *shardHistoryShard) {
			s.Entries, s.LogEntries, s.LastEntry = nil, 0, "0'0"
			s.PartialWrites[0].From = "0'0"
			setShardDumps(s, `{"pg_log_t":{"head":"28'4","tail":"0'0","log":[]}}`, strings.Replace(advancedShardInfo, `"from":"28'3"`, `"from":"0'0"`, 1))
		},
	} {
		shard := advancedShard()
		edit(&shard)
		if err := checkShardDumps(shard); err == nil {
			t.Error("advanced shard dumps accepted:", name)
		}
	}
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"28'3", "28'3", true}, {"28'2", "28'3", true}, {"27'9", "28'3", true}, {"0'0", "28'3", true},
		{"28'4", "28'3", false}, {"29'1", "28'3", false}, {"", "28'3", false}, {"28'3", "", false}, {"28", "28'3", false},
		{"x'1", "28'3", false}, {"28'x", "28'3", false}, {"28'3", "28'3'1", false}, {"-1'3", "28'3", false},
	} {
		if got := versionAtOrBefore(c.a, c.b); got != c.want {
			t.Errorf("versionAtOrBefore(%q, %q) = %t", c.a, c.b, got)
		}
	}
}
