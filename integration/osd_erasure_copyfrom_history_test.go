package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Shard-local PG-log control for a successful COPY_FROM2 into an optimized
// erasure-coded pool. The live `pg <pgid> log` command of the pinned source
// (Ceph v20.2.4, src/osd/OSD.cc:2775-2825) is answered by the primary alone.
// The primary of an EC write hands its log entries to the other shards
// inside the sub-write it builds for each of them, and each receiver passes
// them to its own log (src/osd/ECBackend.cc:424-443); the primary may leave a
// shard out before building that sub-write (src/osd/ECCommon.cc:812-889).
// What each acting shard holds is therefore read from that shard's own
// store. These consumer helpers are test-only and nothing is exported.

// shardHistoryShard is one acting shard's part of the fail-closed receipt of
// tools/osd_shard_history_oracle.py: the attribution of its private store
// copy and where the requested identities occur in that copy's own log.
type shardHistoryShard struct {
	Index             int      `json:"index"`
	OSD               int32    `json:"osd"`
	SPG               string   `json:"spg"`
	Whoami            string   `json:"whoami"`
	PausedMillis      int64    `json:"paused_ms"`
	ArchiveBytes      int64    `json:"store_archive_bytes"`
	Complete          bool     `json:"complete"`
	Failures          []string `json:"failures"`
	Bytes             int      `json:"bytes"`
	LimitBytes        int      `json:"limit_bytes"`
	SHA256            string   `json:"sha256"`
	Raw               string   `json:"raw"`
	Head              string   `json:"head"`
	Tail              string   `json:"tail"`
	LastUpdate        string   `json:"last_update"`
	LastComplete      string   `json:"last_complete"`
	LogEntries        int      `json:"log_entries"`
	DupEntries        int      `json:"dup_entries"`
	ExtraReqidEntries int      `json:"extra_reqid_entries"`
	MissingEntries    int      `json:"missing_entries"`
	// LastEntry is the version of the log's last entry. HeadPastEntries says
	// that the head is newer than it: the one shape the oracle admits for a
	// nonprimary shard whose own info records the partial-write range between
	// the two.
	LastEntry       string `json:"last_entry"`
	HeadPastEntries bool   `json:"head_past_entries"`
	// The info dump with the shard ID it names, every ordinary entry's
	// version and named shards, and the info's partial-write records.
	Shard              *int                `json:"shard"`
	InfoBytes          int                 `json:"info_bytes"`
	InfoSHA256         string              `json:"info_sha256"`
	InfoRaw            string              `json:"info_raw"`
	Entries            []shardLogEntry     `json:"entries"`
	PartialWrites      []shardPartialWrite `json:"partial_writes_last_complete"`
	PartialWritesEpoch uint32              `json:"partial_writes_last_complete_epoch"`
	Targets            []struct {
		Reqid     string              `json:"reqid"`
		Locations []pgHistoryLocation `json:"locations"`
	} `json:"targets"`
}

// shardHistorySnapshot is the receipt over every index of the acting vector,
// bracketed by the native placement before the first copy and after the last
// offline dump.
type shardHistorySnapshot struct {
	Complete           bool                `json:"complete"`
	Failures           []string            `json:"failures"`
	Before             osdFixtureOracle    `json:"before"`
	After              osdFixtureOracle    `json:"after"`
	PGID               string              `json:"pgid"`
	Acting             []int32             `json:"acting"`
	AllActingShards    bool                `json:"all_acting_shards"`
	AbsenceEstablished bool                `json:"absence_established"`
	Shards             []shardHistoryShard `json:"shards"`
	// NonprimaryShards are the pool's native nonprimary shard IDs as the
	// oracle read them: the only shards it lets show HeadPastEntries.
	NonprimaryShards []int `json:"nonprimary_shards"`
}

// checkShardHistory requires a complete receipt whose brackets are identical
// and equal the placement the requests used, with one attributed and
// complete shard for every index of that acting vector. It rechecks each
// dump's length and digest and counts every exact quoted identity
// independently of the Python parser. Every log must end at its head, except
// on the shards listed in past: those may show a head newer than their last
// entry, and the oracle must then have admitted that shape for exactly these
// shards as the pool's nonprimary shards.
func checkShardHistory(s shardHistorySnapshot, route osdFixtureOracle, targets []pgHistoryTarget, past []int) error {
	if !s.Complete || len(s.Failures) != 0 {
		return fmt.Errorf("shard-history receipt is incomplete: %q", s.Failures)
	}
	if !s.AllActingShards || s.AbsenceEstablished {
		return errors.New("shard-history receipt scope differs")
	}
	if !reflect.DeepEqual(s.Before, s.After) {
		return fmt.Errorf("placement changed between the brackets: before %+v after %+v", s.Before, s.After)
	}
	if past != nil && !slices.Equal(s.NonprimaryShards, past) {
		return fmt.Errorf("the oracle read nonprimary shards %v; the caller admits a head past the last entry on %v", s.NonprimaryShards, past)
	}
	if b := s.Before; b.ID != route.ID || b.PrimaryID != route.PrimaryID || !slices.Equal(b.Acting, route.Acting) || !slices.Equal(b.Up, route.Up) ||
		b.PGID != route.PGID || b.Shard != route.Shard || b.PGTemp != route.PGTemp || b.Address != route.Address || b.Epoch < route.Epoch {
		return fmt.Errorf("brackets differ from the placement the requests used: %+v vs %+v", b, route)
	}
	if s.PGID != route.PGID || !slices.Equal(s.Acting, route.Acting) || len(s.Shards) != len(route.Acting) || len(route.Acting) == 0 {
		return fmt.Errorf("receipt covers pg %s acting %v with %d shards; the requests used pg %s acting %v", s.PGID, s.Acting, len(s.Shards), route.PGID, route.Acting)
	}
	for i, shard := range s.Shards {
		osd := route.Acting[i]
		if !shard.Complete || len(shard.Failures) != 0 {
			return fmt.Errorf("shard %d is incomplete: %q", i, shard.Failures)
		}
		if shard.Index != i || shard.OSD != osd || shard.SPG != route.PGID+"s"+strconv.Itoa(i) || shard.Whoami != strconv.Itoa(int(osd)) {
			return fmt.Errorf("shard %d is attributed to index %d, osd.%d, %s, whoami %q; want osd.%d", i, shard.Index, shard.OSD, shard.SPG, shard.Whoami, osd)
		}
		if shard.MissingEntries != 0 || shard.LastUpdate != shard.Head || shard.LastComplete != shard.Head || shard.Head == "" {
			return fmt.Errorf("shard %d is not complete at its head: head %s last_update %s last_complete %s, %d missing", i, shard.Head, shard.LastUpdate, shard.LastComplete, shard.MissingEntries)
		}
		if shard.LastEntry == "" || shard.HeadPastEntries != (shard.LastEntry != shard.Head) || shard.HeadPastEntries && !slices.Contains(past, i) {
			return fmt.Errorf("shard %d: log head %s, last entry %q, head past entries %t; admitted on shards %v only", i, shard.Head, shard.LastEntry, shard.HeadPastEntries, past)
		}
		raw, err := base64.StdEncoding.DecodeString(shard.Raw)
		digest := sha256.Sum256(raw)
		if err != nil || len(raw) == 0 || len(raw) != shard.Bytes || shard.Bytes > shard.LimitBytes || hex.EncodeToString(digest[:]) != shard.SHA256 {
			return fmt.Errorf("shard %d raw bytes differ from their receipt: %v, %d of %d bytes", i, err, len(raw), shard.Bytes)
		}
		if len(shard.Targets) != len(targets) {
			return fmt.Errorf("shard %d reports %d identities; %d were requested", i, len(shard.Targets), len(targets))
		}
		for j, target := range targets {
			if shard.Targets[j].Reqid != target.String() {
				return fmt.Errorf("shard %d target order differs: %s vs %s", i, shard.Targets[j].Reqid, target)
			}
			if n := bytes.Count(raw, []byte(`"reqid":"`+target.String()+`"`)); n != len(shard.Targets[j].Locations) {
				return fmt.Errorf("shard %d: parser found %d positions of %s; the raw dump holds %d", i, len(shard.Targets[j].Locations), target, n)
			}
		}
	}
	return nil
}

// copyFromShardHistory is what one acting shard's own log shows for the
// copy. Neither missing state says that anything was evicted or trimmed.
type copyFromShardHistory struct {
	// Parent: the destination entry at the copy's replay version, carrying
	// the copy's own identity.
	Parent bool
	// Extra: the source write's identity with its user version as the first
	// extra request ID of that entry. It cannot be true without Parent.
	Extra bool
}

func (h copyFromShardHistory) String() string {
	switch {
	case !h.Parent:
		return "parent entry missing"
	case !h.Extra:
		return "parent entry present, extra request ID missing"
	}
	return "parent entry present, extra request ID present"
}

// copyFromShardObservation classifies the positions of the copy's identity
// and of the source write's identity in one shard's log. A missing parent and
// a present parent without the source identity are distinct observations.
// Every other shape contradicts the single acknowledged copy and is refused:
// a parent that is not the one successful modify of the destination head at
// the copy's replay version, or a source identity anywhere but on that
// parent as its first extra request ID with the source user version.
func copyFromShardObservation(copyKey, sourceKey []pgHistoryLocation, want copyFromHistoryWant) (copyFromShardHistory, error) {
	if len(copyKey) == 0 {
		if len(sourceKey) != 0 {
			return copyFromShardHistory{}, fmt.Errorf("the source write's identity occurs without the copy's entry: %+v", sourceKey)
		}
		return copyFromShardHistory{}, nil
	}
	if len(copyKey) != 1 {
		return copyFromShardHistory{}, fmt.Errorf("the copy's identity occurs %d times; want one ordinary entry: %+v", len(copyKey), copyKey)
	}
	entry := copyKey[0]
	if entry.Tier != "log" || entry.ExtraIndex != nil || entry.Op != "modify" || entry.ReturnCode == nil || *entry.ReturnCode != 0 ||
		entry.Object != want.object || entry.Version != want.version {
		return copyFromShardHistory{}, fmt.Errorf("the copy's identity is not the successful modify of %s at %s: %+v", want.object, want.version, entry)
	}
	if len(sourceKey) == 0 {
		return copyFromShardHistory{Parent: true}, nil
	}
	if err := copyFromDestinationBinding(copyKey, sourceKey, want); err != nil {
		return copyFromShardHistory{}, err
	}
	return copyFromShardHistory{Parent: true, Extra: true}, nil
}

// shardPGHistory brackets and judges one shard-local capture of the PG that
// holds an object of the owned pool in slot. The oracle stores the first
// placement, the fixture copies and reads every acting shard's store under
// it, and the oracle reads the placement again. It establishes where the
// identities occur in each shard's own log, never absence. past lists the
// shards whose log head may be newer than their last entry; nil admits none.
func shardPGHistory(t *testing.T, ctx context.Context, slot int, object string, route osdFixtureOracle, targets []pgHistoryTarget, past []int) shardHistorySnapshot {
	t.Helper()
	first := nativePool(t, ctx, "shardhistory", map[string]any{"phase": "before", "slot": slot, "object": object}).Placement
	if first.PGID != route.PGID || !slices.Equal(first.Acting, route.Acting) || first.PrimaryID != route.PrimaryID {
		t.Fatalf("placement moved before the shard capture: %+v vs %+v", first, route)
	}
	if err := testcluster.RequestControl(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "capture-osd-shard-history", "osd-shard-history"); err != nil {
		t.Fatal("shard-history capture:", err)
	}
	s := nativePool(t, ctx, "shardhistory", map[string]any{"phase": "after", "slot": slot, "object": object, "targets": targets}).ShardHistory
	for _, shard := range s.Shards {
		raw, _ := base64.StdEncoding.DecodeString(shard.Raw)
		t.Logf("shard copy %s of osd.%d (whoami %q, paused %d ms, %d archive bytes): complete=%t %q, %d bytes sha256 %s, head %s tail %s last entry %s, %d log entries, %d extra reqids, %d dups, %d missing",
			shard.SPG, shard.OSD, shard.Whoami, shard.PausedMillis, shard.ArchiveBytes, shard.Complete, shard.Failures, shard.Bytes, shard.SHA256,
			shard.Head, shard.Tail, shard.LastEntry, shard.LogEntries, shard.ExtraReqidEntries, shard.DupEntries, shard.MissingEntries)
		if len(raw) != 0 && len(raw) <= 16<<10 {
			t.Logf("complete offline log dump of %s: %s", shard.SPG, raw)
		}
	}
	if err := checkShardHistory(s, route, targets, past); err != nil {
		t.Fatal(err)
	}
	t.Logf("shard-history brackets: epoch %d pg %s up %v acting %v primary osd.%d shard %d", s.Before.Epoch, s.PGID, s.Before.Up, s.Acting, s.Before.PrimaryID, s.Before.Shard)
	return s
}

// The Go client writes a fresh source head once in an owned replicated pool
// and copies it once, pinned to that write's user version, to a fresh head of
// an owned optimized isa k=2,m=2 pool. The destination's live primary must
// bind the copy to the source write exactly as in the replicated control.
// Then every acting shard's store is copied while its container is paused
// and read offline, between two identical native placement records. Each
// copy must be attributed to its acting index and complete, and must hold
// the copy's entry with the source write's identity, because this copy
// creates the head on every shard. This is a positive control: it stops
// before any trimming or resubmission and claims nothing about absence,
// eviction or replay.
//
//fixture:isolated pauses each acting OSD for a store copy; no wait for every PG clean without pg_temp afterwards
func TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration(t *testing.T) {
	width := erasureK2M2
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	if os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
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
	pg := 1
	if hash {
		pg = 8
	}
	const (
		sourceSlot, destinationSlot = 1, 2
		source                      = "ec-copy-history-source"
		destination                 = "ec-copy-history-destination"
		writeIncarnation            = 7
		copyIncarnation             = 9
	)
	replicated := nativePool(t, ctx, "create", map[string]any{"slot": sourceSlot, "pg_num": pg, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	if replicated.PoolID <= 0 || replicated.PoolID == base.Pool || replicated.PoolType != 1 {
		t.Fatal("owned source pool is not a separate replicated pool", replicated.PoolID, replicated.PoolType)
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": destinationSlot, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile || created.PoolID == replicated.PoolID {
		t.Fatal("destination pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, true)

	if poolObserve(t, ctx, sourceSlot, source, "", "").Code != -2 || poolObserve(t, ctx, destinationSlot, destination, "", "").Code != -2 {
		t.Fatal("source or destination head already exists")
	}
	sourceRoute, destinationRoute := poolPlace(t, ctx, sourceSlot, source, ""), poolPlace(t, ctx, destinationSlot, destination, "")
	if sourceRoute.Pool != replicated.PoolID || sourceRoute.PoolType != 1 || destinationRoute.Pool != created.PoolID || sourceRoute.PGID == destinationRoute.PGID {
		t.Fatal("source and destination are not in their owned pools", sourceRoute.PGID, destinationRoute.PGID)
	}
	facts.checkPlacement(t, destinationRoute)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	gid := c.Snapshot().GlobalID
	// send issues one Go request exactly once under its own incarnation.
	send := func(label string, route osdFixtureOracle, incarnation uint32, seconds uint32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		message := request(route, ops...)
		message.ClientIncarnation = incarnation
		message.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
		reply, err := connections[route.ID].Request(ctx, message)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatalf("%s: err=%v code=%d flags=%#x; not resent", label, err, reply.Code, reply.Flags)
		}
		if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != incarnation ||
			reply.Submission.Attempt != 0 || reply.RetryAttempt != 0 || reply.Object != route.Name || reply.PG.Pool != uint64(route.Pool) || len(reply.Operations) != len(ops) {
			t.Fatal("reply identity differs from its submission", label, reply.Transaction, reply.Submission)
		}
		return reply
	}

	data := []byte("ec-copy-history\x00\xff source bytes 0123456789")
	attrName, attrValue := "ec-copy-history-attr", []byte{0xff, 0, 'H'}
	written := send("source write", sourceRoute, writeIncarnation, 1700009000,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attrName}, Data: attrValue})
	nativeSource := poolObserve(t, ctx, sourceSlot, source, "", "")
	if nativeSource.Code != 0 || !bytes.Equal(nativeSource.Data, data) || attrNames(nativeSource)[attrName] != string(attrValue) || nativeSource.Version != written.UserVersion || written.UserVersion == 0 {
		t.Fatal("native source differs from the one Go write", nativeSource.Code, nativeSource.Version, written.UserVersion)
	}
	copied := send("version-pinned COPY_FROM2", destinationRoute, copyIncarnation, 1700009001,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: replicated.PoolID, SourceName: source, SourceVersion: written.UserVersion}})
	if got := copied.Operations[0]; got.Result != 0 || got.CopyFromVersion != written.UserVersion || got.PayloadLength != 0 {
		t.Fatal("copy operation result differs", got.Result, got.CopyFromVersion, got.PayloadLength)
	}
	// Independent native readback: the erasure-coded destination holds the
	// source's bytes and xattr at the copy reply's user version; the source
	// did not change.
	nativeDestination := poolObserve(t, ctx, destinationSlot, destination, "", "")
	if nativeDestination.Code != 0 || !bytes.Equal(nativeDestination.Data, data) || !reflect.DeepEqual(attrNames(nativeDestination), attrNames(nativeSource)) || nativeDestination.Version != copied.UserVersion {
		t.Fatalf("native destination differs from the source: code=%d data=%x attrs=%v version=%d reply=%d",
			nativeDestination.Code, nativeDestination.Data, attrNames(nativeDestination), nativeDestination.Version, copied.UserVersion)
	}
	if after := poolObserve(t, ctx, sourceSlot, source, "", ""); after.Version != nativeSource.Version || !bytes.Equal(after.Data, data) {
		t.Fatal("copy altered the source")
	}

	sourceKey := pgHistoryTarget{Type: "client", Num: gid, Inc: writeIncarnation, TID: written.Transaction}
	copyKey := pgHistoryTarget{Type: "client", Num: gid, Inc: copyIncarnation, TID: copied.Transaction}
	if sourceKey == copyKey || written.Transaction == copied.Transaction {
		t.Fatal("write and copy identities are not distinct", sourceKey, copyKey)
	}
	// Near misses: each request's real TID under the other request's
	// incarnation, then one changed field of the source write's key.
	misses := []pgHistoryTarget{
		{Type: "client", Num: gid, Inc: writeIncarnation, TID: copied.Transaction},
		{Type: "client", Num: gid, Inc: copyIncarnation, TID: written.Transaction},
		{Type: "client", Num: gid, Inc: writeIncarnation + 1, TID: written.Transaction},
		{Type: "client", Num: gid + 1, Inc: writeIncarnation, TID: written.Transaction},
		{Type: "osd", Num: gid, Inc: writeIncarnation, TID: written.Transaction},
		{Type: "client", Num: gid, Inc: writeIncarnation, TID: written.Transaction + 1<<20},
	}
	targets := append([]pgHistoryTarget{copyKey, sourceKey}, misses...)
	version := func(reply cephmsgr.OSDReply) string {
		return fmt.Sprintf("%d'%d", reply.ReplayVersion.Epoch, reply.ReplayVersion.Version)
	}
	want := copyFromHistoryWant{object: hobjectHead(destinationRoute), version: version(copied), sourceUserVersion: written.UserVersion}

	// The live primary first: the destination entry binds the source write
	// to the committed copy, as on a replicated destination.
	primary := primaryPGHistory(t, ctx, destinationSlot, destination, targets)
	requirePGHistoryOnRoute(t, primary, destinationRoute)
	if err := copyFromDestinationBinding(primary.Targets[0].Locations, primary.Targets[1].Locations, want); err != nil {
		t.Fatalf("destination primary's PG log does not bind the copy to the source write: %v", err)
	}
	if primary.ExtraReqidEntries != 1 {
		t.Fatalf("destination primary holds %d extra request IDs; the one source write gives one", primary.ExtraReqidEntries)
	}
	for i := range misses {
		if got := primary.Targets[2+i]; len(got.Locations) != 0 {
			t.Fatalf("near-miss identity %s matched on the primary: %+v", got.Reqid, got.Locations)
		}
	}

	// Then every acting shard's own store, with no further subject request.
	shards := shardPGHistory(t, ctx, destinationSlot, destination, destinationRoute, targets, nil)
	if shards.Before.Epoch != primary.After.Epoch {
		t.Fatalf("map epoch moved from %d to %d between the primary snapshot and the shard capture", primary.After.Epoch, shards.Before.Epoch)
	}
	for i, shard := range shards.Shards {
		for j := range misses {
			if got := shard.Targets[2+j]; len(got.Locations) != 0 {
				t.Fatalf("near-miss identity %s matched on shard %s: %+v", got.Reqid, shard.SPG, got.Locations)
			}
		}
		observed, err := copyFromShardObservation(shard.Targets[0].Locations, shard.Targets[1].Locations, want)
		if err != nil {
			t.Fatalf("shard %s (osd.%d) contradicts the one acknowledged copy: %v", shard.SPG, shard.OSD, err)
		}
		t.Logf("shard %s (osd.%d): %s; copy %s at %s, source %s user version %d; %d extra request IDs, %d duplicates",
			shard.SPG, shard.OSD, observed, copyKey, want.version, sourceKey, written.UserVersion, shard.ExtraReqidEntries, shard.DupEntries)
		// A fresh head is created on every shard (pinned
		// src/osd/ECTransaction.cc:390-407 and :597-600 mark all shards
		// written), so no shard's transaction is empty and the primary leaves
		// none out. A partial overwrite could; it is not exercised here.
		if !observed.Parent || !observed.Extra {
			t.Fatalf("shard %s (osd.%d) of the fresh destination head: %s; this copy created the head on every shard", shard.SPG, shard.OSD, observed)
		}
		if shard.ExtraReqidEntries != 1 || shard.DupEntries != 0 {
			t.Fatalf("shard %s holds %d extra request IDs and %d duplicates; the one source write gives one and nothing was trimmed", shard.SPG, shard.ExtraReqidEntries, shard.DupEntries)
		}
		if i == int(destinationRoute.Shard) {
			// The primary's offline copy must repeat its live answer.
			if shard.OSD != primary.OSD || shard.Head != primary.Head || shard.Tail != primary.Tail || shard.LogEntries != primary.LogEntries ||
				shard.ExtraReqidEntries != primary.ExtraReqidEntries || shard.DupEntries != primary.DupEntries || !reflect.DeepEqual(shard.Targets, primary.Targets) {
				t.Fatalf("the primary's store copy %s differs from its live PG log: %+v vs live head %s tail %s", shard.SPG, shard.Targets, primary.Head, primary.Tail)
			}
		}
	}
}

func TestCopyFromShardObservation(t *testing.T) {
	zero, one, seven := 0, 1, uint64(7)
	want := copyFromHistoryWant{object: "4:53c99ff5:::dst:head", version: "28'3", sourceUserVersion: 7}
	entry := pgHistoryLocation{Tier: "log", Index: 2, Version: "28'3", Op: "modify", Object: want.object, ReturnCode: &zero}
	extra := pgHistoryLocation{Tier: "extra_reqids", Index: 2, ExtraIndex: &zero, Version: "28'3", Op: "modify", Object: want.object, UserVersion: &seven}
	list := func(l ...pgHistoryLocation) []pgHistoryLocation { return l }
	// The three observations a shard may show.
	for name, c := range map[string]struct {
		copyKey, sourceKey []pgHistoryLocation
		want               copyFromShardHistory
		text               string
	}{
		"parent and extra": {list(entry), list(extra), copyFromShardHistory{Parent: true, Extra: true}, "parent entry present, extra request ID present"},
		"parent only":      {list(entry), nil, copyFromShardHistory{Parent: true}, "parent entry present, extra request ID missing"},
		"no parent":        {nil, nil, copyFromShardHistory{}, "parent entry missing"},
	} {
		got, err := copyFromShardObservation(c.copyKey, c.sourceKey, want)
		if err != nil || got != c.want || got.String() != c.text {
			t.Errorf("%s: %+v %q %v", name, got, got, err)
		}
	}
	change := func(l pgHistoryLocation, edit func(*pgHistoryLocation)) []pgHistoryLocation {
		edit(&l)
		return []pgHistoryLocation{l}
	}
	six, failed := uint64(6), -5
	// Everything else contradicts one acknowledged copy and is never
	// reported as a missing parent or a missing extra request ID.
	for name, c := range map[string]struct {
		copyKey, sourceKey []pgHistoryLocation
	}{
		"source without the parent":    {nil, list(extra)},
		"source as a lone duplicate":   {nil, change(extra, func(l *pgHistoryLocation) { l.Tier, l.ExtraIndex, l.UserVersion = "dups", nil, nil })},
		"parent twice":                 {list(entry, entry), list(extra)},
		"parent twice, no source":      {list(entry, entry), nil},
		"parent only a duplicate":      {change(entry, func(l *pgHistoryLocation) { l.Tier, l.Op, l.Object, l.ReturnCode = "dups", "", "", nil }), nil},
		"parent is an extra":           {list(extra), nil},
		"parent in the duplicate tier": {change(entry, func(l *pgHistoryLocation) { l.Tier = "dups" }), nil},
		"parent with an extra index":   {change(entry, func(l *pgHistoryLocation) { l.ExtraIndex = &zero }), nil},
		"parent failed":                {change(entry, func(l *pgHistoryLocation) { l.ReturnCode = &failed }), nil},
		"parent without a return code": {change(entry, func(l *pgHistoryLocation) { l.ReturnCode = nil }), nil},
		"parent is a delete":           {change(entry, func(l *pgHistoryLocation) { l.Op = "delete" }), nil},
		"parent on another object":     {change(entry, func(l *pgHistoryLocation) { l.Object = "4:53c99ff5:::other:head" }), nil},
		"parent at another version":    {change(entry, func(l *pgHistoryLocation) { l.Version = "28'4" }), nil},
		"source twice":                 {list(entry), list(extra, extra)},
		"source as its own entry":      {list(entry), change(entry, func(l *pgHistoryLocation) { l.Index, l.Version = 3, "28'4" })},
		"source only a duplicate":      {list(entry), change(extra, func(l *pgHistoryLocation) { l.Tier, l.ExtraIndex, l.UserVersion = "dups", nil, nil })},
		"source on another entry":      {list(entry), change(extra, func(l *pgHistoryLocation) { l.Index, l.Version = 1, "28'2" })},
		"source not the first extra":   {list(entry), change(extra, func(l *pgHistoryLocation) { l.ExtraIndex = &one })},
		"other source user version":    {list(entry), change(extra, func(l *pgHistoryLocation) { l.UserVersion = &six })},
		"source user version missing":  {list(entry), change(extra, func(l *pgHistoryLocation) { l.UserVersion = nil })},
	} {
		if got, err := copyFromShardObservation(c.copyKey, c.sourceKey, want); err == nil || got != (copyFromShardHistory{}) {
			t.Errorf("contradictory shard history classified as %q: %s", got, name)
		}
	}
}

func TestCheckShardHistory(t *testing.T) {
	route := osdFixtureOracle{Up: []int32{1, 3, 2, 0}, Acting: []int32{1, 3, 2, 0}, ID: 1, PrimaryID: 1, Shard: 0, Epoch: 28, PGID: "4.2", Address: "v2:192.0.2.11:6800/1"}
	targets := []pgHistoryTarget{{Type: "client", Num: 4722, Inc: 9, TID: 2}, {Type: "client", Num: 4722, Inc: 7, TID: 1}, {Type: "client", Num: 4722, Inc: 7, TID: 2}}
	// Synthetic dump text; no daemon produced it. Only the quoted identities
	// matter to the independent count.
	raw := []byte(`{"pg_log_t":{"log":[{"reqid":"client.4722.9:2","extra_reqids":[{"reqid":"client.4722.7:1","user_version":"1"}]}],"dups":[]}}`)
	digest := sha256.Sum256(raw)
	zero := 0
	build := func() shardHistorySnapshot {
		s := shardHistorySnapshot{Complete: true, Before: route, After: route, PGID: route.PGID, Acting: slices.Clone(route.Acting), AllActingShards: true}
		s.Before.Up, s.Before.Acting = slices.Clone(route.Up), slices.Clone(route.Acting)
		s.After.Up, s.After.Acting = slices.Clone(route.Up), slices.Clone(route.Acting)
		for i, osd := range route.Acting {
			shard := shardHistoryShard{Index: i, OSD: osd, SPG: "4.2s" + strconv.Itoa(i), Whoami: strconv.Itoa(int(osd)), Complete: true,
				Bytes: len(raw), LimitBytes: 64 << 10, SHA256: hex.EncodeToString(digest[:]), Raw: base64.StdEncoding.EncodeToString(raw),
				Head: "28'3", Tail: "0'0", LastUpdate: "28'3", LastComplete: "28'3", LastEntry: "28'3", LogEntries: 1, ExtraReqidEntries: 1}
			for j, target := range targets {
				var locations []pgHistoryLocation
				if j == 0 {
					locations = []pgHistoryLocation{{Tier: "log", Version: "28'3", Op: "modify", ReturnCode: &zero}}
				}
				if j == 1 {
					locations = []pgHistoryLocation{{Tier: "extra_reqids", ExtraIndex: &zero, Version: "28'3", Op: "modify"}}
				}
				shard.Targets = append(shard.Targets, struct {
					Reqid     string              `json:"reqid"`
					Locations []pgHistoryLocation `json:"locations"`
				}{target.String(), locations})
			}
			s.Shards = append(s.Shards, shard)
		}
		return s
	}
	if err := checkShardHistory(build(), route, targets, nil); err != nil {
		t.Fatal("complete attributed receipt refused:", err)
	}
	newer := build()
	newer.Before.Epoch, newer.After.Epoch = 29, 29
	if err := checkShardHistory(newer, route, targets, nil); err != nil {
		t.Fatal("identical brackets at a newer epoch refused:", err)
	}
	// A head newer than the last entry is accepted only on a shard the
	// caller lists, and only when the oracle admitted exactly those shards.
	advance := func(s *shardHistorySnapshot, shard int) {
		s.Shards[shard].Head, s.Shards[shard].LastUpdate, s.Shards[shard].LastComplete, s.Shards[shard].HeadPastEntries = "28'4", "28'4", "28'4", true
	}
	for name, c := range map[string]struct {
		nonprimary, past []int
		shard            int
		accepted         bool
	}{
		"the admitted nonprimary shard":       {[]int{1}, []int{1}, 1, true},
		"no shard admitted":                   {[]int{1}, nil, 1, false},
		"an empty admission":                  {[]int{1}, []int{}, 1, false},
		"another shard admitted":              {[]int{2}, []int{2}, 1, false},
		"the primary's shard":                 {[]int{1}, []int{1}, 0, false},
		"a parity shard":                      {[]int{1}, []int{1}, 3, false},
		"the oracle admitted no shard":        {nil, []int{1}, 1, false},
		"the oracle admitted another shard":   {[]int{2}, []int{1}, 1, false},
		"the oracle admitted one shard more":  {[]int{1, 2}, []int{1}, 1, false},
		"the caller admitted one shard more":  {[]int{1}, []int{1, 2}, 1, false},
		"the oracle's set in another order":   {[]int{2, 1}, []int{1, 2}, 1, false},
		"both admit two shards, in one order": {[]int{1, 2}, []int{1, 2}, 2, true},
	} {
		s := build()
		s.NonprimaryShards = c.nonprimary
		advance(&s, c.shard)
		if err := checkShardHistory(s, route, targets, c.past); (err == nil) != c.accepted {
			t.Errorf("a head past the last entry on shard %d, %s: %v", c.shard, name, err)
		}
	}
	// With an admission, an ordinary receipt needs the same nonprimary set.
	ordinary := build()
	ordinary.NonprimaryShards = []int{1}
	if err := checkShardHistory(ordinary, route, targets, []int{1}); err != nil {
		t.Error("ordinary receipt with an admitted shard refused:", err)
	}
	if err := checkShardHistory(build(), route, targets, []int{1}); err == nil {
		t.Error("receipt without the oracle's nonprimary shards accepted for an admission")
	}
	for name, edit := range map[string]func(*shardHistorySnapshot){
		"incomplete receipt":             func(s *shardHistorySnapshot) { s.Complete = false },
		"recorded failure":               func(s *shardHistorySnapshot) { s.Failures = []string{"shard 1 (osd.3): offline log exited 1"} },
		"not every acting shard":         func(s *shardHistorySnapshot) { s.AllActingShards = false },
		"absence claimed":                func(s *shardHistorySnapshot) { s.AbsenceEstablished = true },
		"epoch moved during the capture": func(s *shardHistorySnapshot) { s.After.Epoch++ },
		"acting order moved": func(s *shardHistorySnapshot) {
			s.After.Acting[1], s.After.Acting[2] = s.After.Acting[2], s.After.Acting[1]
		},
		"up set moved":                     func(s *shardHistorySnapshot) { s.After.Up[3] = 4 },
		"address changed":                  func(s *shardHistorySnapshot) { s.After.Address = "v2:192.0.2.11:6801/2" },
		"pg_temp appeared":                 func(s *shardHistorySnapshot) { s.After.PGTemp = true },
		"both brackets on another primary": func(s *shardHistorySnapshot) { s.Before.PrimaryID, s.After.PrimaryID = 3, 3 },
		"both brackets on another target":  func(s *shardHistorySnapshot) { s.Before.ID, s.After.ID = 3, 3 },
		"both brackets with another up set": func(s *shardHistorySnapshot) {
			s.Before.Up, s.After.Up = []int32{1, 3, 2, 4}, []int32{1, 3, 2, 4}
		},
		"both brackets under a pg_temp": func(s *shardHistorySnapshot) { s.Before.PGTemp, s.After.PGTemp = true, true },
		"both brackets in another order": func(s *shardHistorySnapshot) {
			s.Before.Acting, s.After.Acting = []int32{3, 1, 2, 0}, []int32{3, 1, 2, 0}
		},
		"both brackets at another address": func(s *shardHistorySnapshot) {
			s.Before.Address, s.After.Address = "v2:192.0.2.9:6800/1", "v2:192.0.2.9:6800/1"
		},
		"both brackets on another shard":  func(s *shardHistorySnapshot) { s.Before.Shard, s.After.Shard = 2, 2 },
		"both brackets in another PG":     func(s *shardHistorySnapshot) { s.Before.PGID, s.After.PGID = "4.3", "4.3" },
		"both brackets at an older epoch": func(s *shardHistorySnapshot) { s.Before.Epoch, s.After.Epoch = 27, 27 },
		"receipt for another PG":          func(s *shardHistorySnapshot) { s.PGID = "4.3" },
		"receipt acting reordered":        func(s *shardHistorySnapshot) { s.Acting[0], s.Acting[3] = s.Acting[3], s.Acting[0] },
		"a shard is missing":              func(s *shardHistorySnapshot) { s.Shards = s.Shards[:3] },
		"a shard is repeated":             func(s *shardHistorySnapshot) { s.Shards = append(s.Shards, s.Shards[3]) },
		"shard incomplete":                func(s *shardHistorySnapshot) { s.Shards[2].Complete = false },
		"shard failure":                   func(s *shardHistorySnapshot) { s.Shards[2].Failures = []string{"the OSD was not resumed"} },
		"shards swapped":                  func(s *shardHistorySnapshot) { s.Shards[1], s.Shards[2] = s.Shards[2], s.Shards[1] },
		"wrong index":                     func(s *shardHistorySnapshot) { s.Shards[1].Index = 2 },
		"wrong OSD":                       func(s *shardHistorySnapshot) { s.Shards[1].OSD = 2 },
		"wrong shard collection":          func(s *shardHistorySnapshot) { s.Shards[1].SPG = "4.2s0" },
		"collection of another PG":        func(s *shardHistorySnapshot) { s.Shards[1].SPG = "4.3s1" },
		"another store's whoami":          func(s *shardHistorySnapshot) { s.Shards[1].Whoami = "1" },
		"no whoami":                       func(s *shardHistorySnapshot) { s.Shards[1].Whoami = "" },
		"missing set not empty":           func(s *shardHistorySnapshot) { s.Shards[3].MissingEntries = 1 },
		"log behind the info":             func(s *shardHistorySnapshot) { s.Shards[3].LastUpdate = "28'4" },
		"last_complete behind":            func(s *shardHistorySnapshot) { s.Shards[3].LastComplete = "28'2" },
		"no last entry":                   func(s *shardHistorySnapshot) { s.Shards[3].LastEntry = "" },
		"last entry behind the head":      func(s *shardHistorySnapshot) { s.Shards[3].LastEntry = "28'2" },
		"head past entries at the head":   func(s *shardHistorySnapshot) { s.Shards[3].HeadPastEntries = true },
		"head past entries, not admitted": func(s *shardHistorySnapshot) { s.Shards[1].LastEntry, s.Shards[1].HeadPastEntries = "28'2", true },
		"no head": func(s *shardHistorySnapshot) {
			s.Shards[3].Head, s.Shards[3].LastUpdate, s.Shards[3].LastComplete = "", "", ""
		},
		// The decoder still returns every byte before the stray character.
		"raw is not base64": func(s *shardHistorySnapshot) { s.Shards[0].Raw += "%" },
		"raw is empty": func(s *shardHistorySnapshot) {
			empty := sha256.Sum256(nil)
			s.Shards[0].Raw, s.Shards[0].Bytes, s.Shards[0].SHA256 = "", 0, hex.EncodeToString(empty[:])
			for j := range s.Shards[0].Targets {
				s.Shards[0].Targets[j].Locations = nil
			}
		},
		"raw length differs":  func(s *shardHistorySnapshot) { s.Shards[0].Bytes++ },
		"raw digest differs":  func(s *shardHistorySnapshot) { s.Shards[0].SHA256 = strings.Repeat("0", 64) },
		"raw over its bound":  func(s *shardHistorySnapshot) { s.Shards[0].LimitBytes = len(raw) - 1 },
		"a target is missing": func(s *shardHistorySnapshot) { s.Shards[0].Targets = s.Shards[0].Targets[:2] },
		"an unrequested target": func(s *shardHistorySnapshot) {
			s.Shards[0].Targets = append(s.Shards[0].Targets, s.Shards[0].Targets[2])
		},
		"targets reordered": func(s *shardHistorySnapshot) {
			s.Shards[0].Targets[0], s.Shards[0].Targets[1] = s.Shards[0].Targets[1], s.Shards[0].Targets[0]
		},
		"parser missed a position":   func(s *shardHistorySnapshot) { s.Shards[0].Targets[1].Locations = nil },
		"parser invented a position": func(s *shardHistorySnapshot) { s.Shards[0].Targets[2].Locations = s.Shards[0].Targets[0].Locations },
		"parser doubled a position": func(s *shardHistorySnapshot) {
			l := s.Shards[0].Targets[0].Locations
			s.Shards[0].Targets[0].Locations = append(l, l...)
		},
		"no shard at all": func(s *shardHistorySnapshot) { s.Shards = nil },
		"raw replaced after the digest": func(s *shardHistorySnapshot) {
			s.Shards[2].Raw = base64.StdEncoding.EncodeToString(append(slices.Clone(raw), ' '))
		},
	} {
		s := build()
		edit(&s)
		if err := checkShardHistory(s, route, targets, nil); err == nil {
			t.Error("shard-history receipt accepted:", name)
		}
	}
}
