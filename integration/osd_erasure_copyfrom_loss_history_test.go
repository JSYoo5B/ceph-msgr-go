package integration_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// copyLossShardReceipt is one acting shard's part of a cell's offline
// capture: the attribution and digest of its log dump and what it holds for
// the copy. It contains no paths or addresses.
type copyLossShardReceipt struct {
	SPG          string `json:"spg"`
	OSD          int32  `json:"osd"`
	SHA256       string `json:"log_sha256"`
	Head         string `json:"head"`
	LastEntry    string `json:"last_entry"`
	LogEntries   int    `json:"log_entries"`
	ExtraReqids  int    `json:"extra_reqid_entries"`
	DupEntries   int    `json:"dup_entries"`
	CopyVersion  string `json:"copy_version"`
	SourceExtra  int    `json:"first_source_extra_index"`
	PausedMillis int64  `json:"paused_ms"`
}

// copyLossShardObservation judges where one complete PG log, a shard's own
// or the live primary's, holds the identities of a copy that was lost with
// the destination primary and then resubmitted once by the caller:
//
//   - the copy's identity exactly once, as the one ordinary successful
//     modify of the destination head at the resubmission's version;
//   - the first source write's identity exactly once, as the second extra
//     request ID of that entry with that write's user version: the final
//     copy-get returns the source object's request IDs newest first
//     (get_object_reqids walks the source log from its end,
//     src/osd/PGLog.h:477-507), and the second source write that the
//     resubmission copied is the newest;
//   - every other identity (the blocker the primary lost with the copy, and
//     near misses of the copy) nowhere.
//
// It returns the first source write's extra index.
func copyLossShardObservation(copyKey, firstWrite []pgHistoryLocation, absent [][]pgHistoryLocation, object, version string, firstUser uint64) (int, error) {
	if err := copyEntryOnly(copyKey, object, version); err != nil {
		return -1, err
	}
	entry := copyKey[0]
	if len(firstWrite) != 1 {
		return -1, fmt.Errorf("the first source write occurs %d times; want one extra request ID: %+v", len(firstWrite), firstWrite)
	}
	extra := firstWrite[0]
	if extra.Tier != "extra_reqids" || extra.ExtraIndex == nil || *extra.ExtraIndex != 1 || extra.Index != entry.Index || extra.Object != entry.Object ||
		extra.Version != entry.Version || extra.Op != entry.Op {
		return -1, fmt.Errorf("the first source write is not the second extra request ID of the copy's entry %d: %+v", entry.Index, extra)
	}
	if extra.UserVersion == nil || *extra.UserVersion != firstUser {
		return -1, fmt.Errorf("the first source write's extra request ID does not carry user version %d: %+v", firstUser, extra)
	}
	for i, locations := range absent {
		if len(locations) != 0 {
			return -1, fmt.Errorf("identity %d that must not occur is at %+v", i, locations)
		}
	}
	return *extra.ExtraIndex, nil
}

// copyLossShardHistory follows a destination-primary loss cell that ended
// with one explicit resubmission and recovery: the original destination
// primary leads again on the original acting set. The live primary's PG log
// is judged first (copyLossShardObservation), then every acting shard's
// store is copied while its container is paused and read offline, between
// two identical placement records, with the interval witness after the
// capture (shardPGHistory). Each shard's own log must hold the copy once at
// the resubmission's version with the first source write as its second
// extra request ID, no duplicate entry, and, after the held final
// transaction, nothing of the blocker write lost with the primary. The
// primary's offline log must repeat its live answer. This establishes what
// every shard persisted for these identities after recovery; it claims no
// absence outside these complete logs, and nothing about the shards' state
// before recovery.
func copyLossShardHistory(t *testing.T, ctx context.Context, r *copyPrimaryChangeRun, h *copyChunkHold, facts erasureFacts, receipt *copyErasurePairLossReceipt) {
	t.Helper()
	if receipt.Unknown == nil || receipt.Reply.Attempt != 1 || receipt.Reply.TID != h.op.TID || receipt.Reply.Replay == "" {
		t.Fatalf("shard history follows a lost copy and one resubmission only: unknown %+v, reply %+v", receipt.Unknown, receipt.Reply)
	}
	route := poolPlace(t, ctx, 2, r.dest, "")
	facts.checkPlacement(t, route)
	if route.ID != r.dstRoute.ID || !slices.Equal(route.Acting, r.dstRoute.Acting) || route.PGID != r.dstRoute.PGID {
		t.Fatalf("the destination PG is not back on its original placement: %+v; was %+v", route, r.dstRoute)
	}
	targets := copyLossTargets(t, r, h, receipt)
	copyKey, writeKey := targets[0], targets[1]
	object, version := hobjectHead(route), receipt.Reply.Replay
	judge := func(where string, got []struct {
		Reqid     string              `json:"reqid"`
		Locations []pgHistoryLocation `json:"locations"`
	}) int {
		var absent [][]pgHistoryLocation
		for _, target := range got[2:] {
			absent = append(absent, target.Locations)
		}
		index, err := copyLossShardObservation(got[0].Locations, got[1].Locations, absent, object, version, h.write.UserVersion)
		if err != nil {
			t.Fatalf("%s: %v", where, err)
		}
		return index
	}
	live := primaryPGHistory(t, ctx, 2, r.dest, targets)
	requirePGHistoryOnRoute(t, live, route)
	judge(fmt.Sprintf("live primary osd.%d", route.ID), live.Targets)

	shards := shardPGHistory(t, ctx, 2, r.dest, route, targets, facts.nonprimary)
	for i, shard := range shards.Shards {
		index := judge(fmt.Sprintf("shard %s (osd.%d)", shard.SPG, shard.OSD), shard.Targets)
		if shard.DupEntries != 0 {
			t.Fatalf("shard %s holds %d duplicate entries; nothing was trimmed", shard.SPG, shard.DupEntries)
		}
		if i == int(route.Shard) && (shard.OSD != live.OSD || shard.Head != live.Head || shard.Tail != live.Tail || shard.LogEntries != live.LogEntries ||
			shard.ExtraReqidEntries != live.ExtraReqidEntries || !reflect.DeepEqual(shard.Targets, live.Targets)) {
			t.Fatalf("the primary's store copy %s differs from its live PG log: head %s tail %s vs live head %s tail %s", shard.SPG, shard.Head, shard.Tail, live.Head, live.Tail)
		}
		receipt.ShardHistory = append(receipt.ShardHistory, copyLossShardReceipt{SPG: shard.SPG, OSD: shard.OSD, SHA256: shard.SHA256, Head: shard.Head, LastEntry: shard.LastEntry,
			LogEntries: shard.LogEntries, ExtraReqids: shard.ExtraReqidEntries, DupEntries: shard.DupEntries, CopyVersion: version, SourceExtra: index, PausedMillis: shard.PausedMillis})
		t.Logf("shard %s (osd.%d): copy %s once at %s with source %s as extra %d; %d entries, %d extra request IDs, %d duplicates",
			shard.SPG, shard.OSD, copyKey, version, writeKey, index, shard.LogEntries, shard.ExtraReqidEntries, shard.DupEntries)
	}
	// Every OSD was paused once for its copy; the next cell starts from a
	// clean map.
	waitOSDFixtureClean(t, ctx, r.recovery.base)
}

// copyLossTargets are the identities a lost-copy history looks up: the
// copy, the first source write, two near misses of the copy and, after a
// held final transaction, the blocker write lost with the primary.
func copyLossTargets(t *testing.T, r *copyPrimaryChangeRun, h *copyChunkHold, receipt *copyErasurePairLossReceipt) []pgHistoryTarget {
	t.Helper()
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: h.write.Transaction}
	targets := []pgHistoryTarget{copyKey, writeKey,
		{Type: "client", Num: copyKey.Num, Inc: copyKey.Inc + 1, TID: copyKey.TID},
		{Type: "client", Num: copyKey.Num, Inc: copyKey.Inc, TID: copyKey.TID + 1<<20}}
	if receipt.Held != nil {
		if receipt.Held.BlockerSubmitTID == 0 {
			t.Fatal("the held cell recorded no blocker submission")
		}
		targets = append(targets, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyHeldBlockerIncarnation, TID: receipt.Held.BlockerSubmitTID})
	}
	return targets
}

// copyLossDegradedReceipt is one offline capture of the degraded
// destination PG: its stage, the placement it was bracketed by, the empty
// position of the lost primary and every present shard's part.
type copyLossDegradedReceipt struct {
	Stage   string                 `json:"stage"`
	Epoch   uint32                 `json:"epoch"`
	Primary int32                  `json:"primary"`
	Acting  []int32                `json:"acting"`
	Absent  []int                  `json:"absent_shards"`
	Shards  []copyLossShardReceipt `json:"shards"`
}

// copyLossLostShardObservation judges one complete PG log of a survivor of
// the lost primary before any resubmission: none of the identities occurs.
// It is an observation of that log only.
func copyLossLostShardObservation(targets [][]pgHistoryLocation) error {
	for i, locations := range targets {
		if len(locations) != 0 {
			return fmt.Errorf("identity %d occurs at %+v before any resubmission", i, locations)
		}
	}
	return nil
}

// copyLossDegradedHistory runs inside settleDestinationLoss while the
// destination PG is degraded by its lost primary: route is the PG under its
// new primary (a pg_temp), and the lost primary's shard is an empty position
// of its acting vector. The live primary's log is judged first, then every
// present shard's store is copied and read offline (shardPGHistory), which
// records the empty position as absent and reads nothing for it: the lost
// OSD is down and no other OSD holds that shard yet.
//
//   - Stage "lost", before the second source write: no log, the live
//     primary's or any survivor's own, holds the copy, the first source
//     write, a near miss or the blocker lost with the primary. Chunk writes
//     to the temporary object carry no log entry (simple_opc_submit,
//     src/osd/PrimaryLogPG.cc:11608-11626, from process_copy_chunk
//     :9784-9790), and the final transaction was never applied on the
//     survivors.
//   - Stage "resubmitted", after the one explicit resubmission and before
//     recovery: every survivor holds the copy once at the resubmission's
//     version with the first source write as its second extra request ID
//     (copyLossShardObservation), from the sub-write the new primary sent it
//     and not by peering with the lost shard.
//
// The new primary's offline log must equal its live answer at both stages.
// No log is evidence of absence elsewhere, and nothing is said about the
// lost OSD's own store.
func copyLossDegradedHistory(t *testing.T, ctx context.Context, r *copyPrimaryChangeRun, h *copyChunkHold, facts erasureFacts, receipt *copyErasurePairLossReceipt, stage string, route osdFixtureOracle, answered *cephmsgr.OSDReply) {
	t.Helper()
	lost := int(h.route.Shard)
	if receipt.Unknown == nil || route.ID == h.route.ID || route.Shard < 0 || lost < 0 || lost >= len(route.Acting) || route.Acting[lost] != crushItemNone || slices.Contains(route.Acting, h.route.ID) {
		t.Fatalf("the destination PG is not degraded by its lost primary osd.%d (shard %d): %+v, unknown %+v", h.route.ID, lost, route, receipt.Unknown)
	}
	if (stage == "resubmitted") != (answered != nil) || stage != "lost" && stage != "resubmitted" {
		t.Fatalf("degraded capture at stage %q with reply %v", stage, answered != nil)
	}
	targets := copyLossTargets(t, r, h, receipt)
	object, version := hobjectHead(route), ""
	if answered != nil {
		version = replayVersion(*answered)
	}
	judge := func(where string, got []struct {
		Reqid     string              `json:"reqid"`
		Locations []pgHistoryLocation `json:"locations"`
	}) int {
		var all [][]pgHistoryLocation
		for _, target := range got {
			all = append(all, target.Locations)
		}
		if answered == nil {
			if err := copyLossLostShardObservation(all); err != nil {
				t.Fatalf("%s before the resubmission: %v", where, err)
			}
			return -1
		}
		index, err := copyLossShardObservation(all[0], all[1], all[2:], object, version, h.write.UserVersion)
		if err != nil {
			t.Fatalf("%s after the resubmission: %v", where, err)
		}
		return index
	}
	live := primaryPGHistory(t, ctx, 2, r.dest, targets)
	requirePGHistoryOnRoute(t, live, route)
	judge(fmt.Sprintf("live primary osd.%d", route.ID), live.Targets)

	shards := shardPGHistory(t, ctx, 2, r.dest, route, targets, facts.nonprimary)
	capture := copyLossDegradedReceipt{Stage: stage, Epoch: shards.Before.Epoch, Primary: route.ID, Acting: slices.Clone(route.Acting), Absent: shards.AbsentShards}
	if !slices.Equal(shards.AbsentShards, []int{lost}) {
		t.Fatalf("the degraded capture reports absent shards %v; the lost primary held shard %d", shards.AbsentShards, lost)
	}
	for i, shard := range shards.Shards {
		if shard.Absent {
			t.Logf("shard %s: absent, the lost osd.%d's position; nothing read", shard.SPG, h.route.ID)
			continue
		}
		index := judge(fmt.Sprintf("shard %s (osd.%d)", shard.SPG, shard.OSD), shard.Targets)
		if shard.DupEntries != 0 {
			t.Fatalf("shard %s holds %d duplicate entries; nothing was trimmed", shard.SPG, shard.DupEntries)
		}
		if i == int(route.Shard) && (shard.OSD != live.OSD || shard.Head != live.Head || shard.Tail != live.Tail || shard.LogEntries != live.LogEntries ||
			shard.ExtraReqidEntries != live.ExtraReqidEntries || !reflect.DeepEqual(shard.Targets, live.Targets)) {
			t.Fatalf("the new primary's store copy %s differs from its live PG log: head %s tail %s vs live head %s tail %s", shard.SPG, shard.Head, shard.Tail, live.Head, live.Tail)
		}
		capture.Shards = append(capture.Shards, copyLossShardReceipt{SPG: shard.SPG, OSD: shard.OSD, SHA256: shard.SHA256, Head: shard.Head, LastEntry: shard.LastEntry,
			LogEntries: shard.LogEntries, ExtraReqids: shard.ExtraReqidEntries, DupEntries: shard.DupEntries, CopyVersion: version, SourceExtra: index, PausedMillis: shard.PausedMillis})
		t.Logf("degraded shard %s (osd.%d) at stage %s: copy at %q, first source write as extra %d; %d entries, head %s", shard.SPG, shard.OSD, stage, version, index, shard.LogEntries, shard.Head)
	}
	receipt.DegradedHistory = append(receipt.DegradedHistory, capture)
}

// For COPY_FROM and COPY_FROM2, the two destination-primary loss cells of
// TestCephOSDErasureOptimizedCopyFromErasureToErasureLossIntegration on the
// owned optimized isa k=2,m=2 source and k=3,m=1 destination, each followed
// by the offline capture of every acting shard's own log
// (copyLossShardHistory): the primary lost between two chunks of a head
// copy, and the primary lost while the copy's final transaction is held
// behind the PG's EC write queue. In both, the original request ended as an
// unknown outcome, the seed was kept, a second source write replaced the
// source and one explicit resubmission executed once on the new primary.
//
// The live `pg <pgid> log` command is answered by the primary alone
// (src/osd/OSD.cc:2775-2825), and an EC primary hands each shard its log
// entries inside that shard's sub-write (src/osd/ECBackend.cc:424-443), so
// what the other shards persisted is read from their own stores. After the
// original primary recovered and leads again, every shard of the four must
// hold the copy once at the resubmission's version, bound to the source
// lineage, and none may hold the lost blocker write. A shard that rejoined
// after the loss received the entries by peering, so the degraded PG is
// captured too, twice, before recovery (copyLossDegradedHistory): once
// after the loss is settled, when no survivor's log holds the copy, the
// source write or the blocker, and once after the resubmission, when every
// survivor holds the copy once with the source lineage. The lost primary's
// shard is then an empty acting position: the capture records it absent and
// reads nothing for it. Absence outside these complete logs is not
// established, and the lost OSD's own store is not read. Not exercised: the
// other width pairs and snapshot sources.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedcopyfromerasuretoerasurelosshistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated freezes and SIGKILLs OSDs, pauses each acting OSD for a store copy, sets pause, lowers a copy chunk at runtime
func TestCephOSDErasureOptimizedCopyFromErasureToErasureLossHistoryIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	var cells []copyErasurePairLossCell
	for _, tc := range copyPrimaryChangeCases {
		cells = append(cells, copyErasurePairLossCell{tc, "destination", false, false}, copyErasurePairLossCell{tc, "destination-before-commit", false, false})
	}
	runCopyErasurePairLoss(t, erasurePairK2M2ToK3M1, "osd-erasure-copyfrom-loss-history-results.json", cells, copyErasurePairLossHooks{after: copyLossShardHistory, degraded: copyLossDegradedHistory})
}

func TestCopyLossShardObservation(t *testing.T) {
	zero, one, two := 0, 1, 2
	user := uint64(41)
	other := uint64(40)
	const object, version = "4:a1b2c3d4:::dst:head", "33'12"
	entry := pgHistoryLocation{Tier: "log", Index: 7, Version: version, Op: "modify", Object: object, ReturnCode: &zero}
	extra := pgHistoryLocation{Tier: "extra_reqids", Index: 7, ExtraIndex: &one, Version: version, Op: "modify", Object: object, UserVersion: &user}
	if index, err := copyLossShardObservation([]pgHistoryLocation{entry}, []pgHistoryLocation{extra}, [][]pgHistoryLocation{nil, nil}, object, version, user); err != nil || index != 1 {
		t.Fatalf("accepted shape refused: %d %v", index, err)
	}
	edit := func(f func(*pgHistoryLocation)) []pgHistoryLocation {
		l := extra
		f(&l)
		return []pgHistoryLocation{l}
	}
	for name, c := range map[string]struct {
		copyKey, write []pgHistoryLocation
		absent         [][]pgHistoryLocation
	}{
		"copy missing":                {nil, []pgHistoryLocation{extra}, nil},
		"copy twice":                  {[]pgHistoryLocation{entry, entry}, []pgHistoryLocation{extra}, nil},
		"copy at another version":     {[]pgHistoryLocation{{Tier: "log", Index: 7, Version: "33'13", Op: "modify", Object: object, ReturnCode: &zero}}, []pgHistoryLocation{extra}, nil},
		"write missing":               {[]pgHistoryLocation{entry}, nil, nil},
		"write twice":                 {[]pgHistoryLocation{entry}, []pgHistoryLocation{extra, extra}, nil},
		"write as an ordinary entry":  {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.Tier, l.ExtraIndex = "log", nil }), nil},
		"write as the newest extra":   {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.ExtraIndex = &zero }), nil},
		"write as the third extra":    {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.ExtraIndex = &two }), nil},
		"write on another entry":      {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.Index = 6 }), nil},
		"write with another version":  {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.UserVersion = &other }), nil},
		"write without user version":  {[]pgHistoryLocation{entry}, edit(func(l *pgHistoryLocation) { l.UserVersion = nil }), nil},
		"lost blocker on a shard":     {[]pgHistoryLocation{entry}, []pgHistoryLocation{extra}, [][]pgHistoryLocation{nil, {entry}}},
		"near miss of the copy found": {[]pgHistoryLocation{entry}, []pgHistoryLocation{extra}, [][]pgHistoryLocation{{entry}}},
	} {
		if _, err := copyLossShardObservation(c.copyKey, c.write, c.absent, object, version, user); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCopyLossLostShardObservation(t *testing.T) {
	zero := 0
	entry := pgHistoryLocation{Tier: "log", Index: 7, Version: "33'12", Op: "modify", Object: "4:a1b2c3d4:::dst:head", ReturnCode: &zero}
	if err := copyLossLostShardObservation([][]pgHistoryLocation{nil, nil, nil, nil, nil}); err != nil {
		t.Fatal("an empty log refused:", err)
	}
	for i := range 5 {
		targets := make([][]pgHistoryLocation, 5)
		targets[i] = []pgHistoryLocation{entry}
		if err := copyLossLostShardObservation(targets); err == nil {
			t.Errorf("identity %d before any resubmission accepted", i)
		}
	}
}
