package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A primary lost between the chunks of a multi-chunk COPY_FROM or
// COPY_FROM2. The pinned source (Ceph v20.2.4) gives the barrier its meaning:
//
//   - src/osd/PrimaryLogPG.h:1398-1407 (get_copy_chunk_size) bounds every
//     copy-get by the destination primary's osd_copyfrom_max_chunk.
//     src/osd/PrimaryLogPG.cc:9776-9793 (process_copy_chunk) writes a chunk
//     that leaves the cursor incomplete to a temporary object, named by
//     generate_temp_object (PrimaryLogPG.cc:8954-8962) as
//     temp_<pgid>_<role>_<gid>_<n>, in an op context of its own that is
//     replicated like any write, and then calls _copy_some for the next
//     chunk at once. src/osd/PrimaryLogPG.cc:9564-9600 (_copy_some) prefixes
//     assert-version once the first chunk taught it the source version, so
//     a copy-get with assert-version is a later chunk of an unversioned copy
//     and one without it is the first.
//   - src/osd/osd_types.cc:7323-7325 and 7388-7393 print a copy-get with
//     " in=<n>b", the length of its encoded cursor and max
//     (src/osdc/Objecter.h:1184-1206, osd_types.cc:5492-5501). That length is
//     29 bytes plus the cursor's OMAP position, so a copy-get of this
//     data-only source prints in=29b in every chunk. The data offset is not
//     printed; the second chunk is told from a restarted first one by its
//     assert-version, its new TID and the unchanged temporary object.
//   - A copy-get is a read: src/osd/PrimaryLogPG.cc:1691-1733 (get_rw_locks)
//     and 2494-2497 make it wait while a write of the source holds the
//     object's write lock, which remove_repop (PrimaryLogPG.cc:11591-11605)
//     releases only after every acting replica committed.
//   - src/osdc/Objecter.cc:3074-3082 (_calc_target) marks an op that was
//     already sent paused when a map with pauserd arrives, without resending
//     it, and Objecter.cc:3553-3620 (handle_osd_op_reply) still accepts its
//     reply; Objecter.cc:2550-2556 (_op_submit) keeps a new read paused and
//     unsent. src/osd/OSD.cc:8215-8217 shares every map with the OSD's
//     Objecter.
//
// The third OSD, a replica of both PGs, is paused; a Go write of the source
// is observed holding the source's write lock ("waiting for sub ops" on the
// source primary); the copy is sent and its first copy-get observed sent;
// `osd set pause` is applied and observed in the destination primary's
// Objecter on that same copy-get; only then is the replica resumed. The first
// chunk can be served only after that, so the copy-get of the second chunk is
// submitted under pauserd and stays unsent. That is the barrier: the
// destination primary's Objecter holds exactly one paused, never sent
// assert-version+copy-get of the source, its op tracker holds the client op
// with no sub-op event (the final transaction was not issued), and a complete
// native listing of the destination PG on each of the three OSDs holds the
// same single temporary object.
//
// src/osd/PrimaryLogPG.cc:13125-13216 (on_change) cancels the copy of a PG
// whose interval changed, requeueing the client op on a primary
// (PrimaryLogPG.cc:13147, 344-368), and on every OSD of the PG removes the
// temporary objects it tracks (src/osd/PGBackend.cc:507-521; a replica tracks
// one from the sub-op, src/osd/ReplicatedBackend.cc:1230-1233). An OSD
// removes every temporary object of its store when it starts
// (src/osd/OSD.cc:3879, 5012).

// copyStagingSnapshot is the receipt of one complete listing of one PG
// collection on one OSD (tools/osd_pool_mode_copystaging_oracle.py): the
// shard's collection of an erasure-coded PG, the PG's of a replicated one
// (shard -1).
type copyStagingSnapshot struct {
	OSD        int32    `json:"osd"`
	PGID       string   `json:"pgid"`
	Shard      int      `json:"shard"`
	Collection string   `json:"collection"`
	Max        int      `json:"max"`
	Objects    int      `json:"objects"`
	Complete   bool     `json:"complete"`
	Temp       []string `json:"temp"`
	SHA256     string   `json:"sha256"`
	Failures   []string `json:"failures"`
}

// copyChunkSetting is the receipt of one copychunk action
// (tools/osd_pool_mode_copychunk_oracle.py).
type copyChunkSetting struct {
	OSD      int32    `json:"osd"`
	Action   string   `json:"action"`
	Limit    uint64   `json:"limit"`
	Value    uint64   `json:"value"`
	Prior    uint64   `json:"prior"`
	Claimed  bool     `json:"claimed"`
	Lowered  bool     `json:"lowered"`
	Restored bool     `json:"restored"`
	Failures []string `json:"failures"`
}

// copyStagingShard is the shard osd holds of route's PG: its index in the
// acting set of an erasure-coded route without pg_temp, -1 for a replicated
// route, and an error when osd holds none.
func copyStagingShard(route osdFixtureOracle, osd int32) (int, error) {
	if route.Shard < 0 {
		return -1, nil
	}
	shard := slices.Index(route.Acting, osd)
	if route.PGTemp || shard < 0 {
		return -1, fmt.Errorf("osd.%d holds no shard of %s by its acting set %v (pg_temp %t)", osd, route.PGID, route.Acting, route.PGTemp)
	}
	return shard, nil
}

// copyStaging lists route's PG collection on one OSD: the collection of the
// shard that OSD holds of an erasure-coded PG.
func copyStaging(t *testing.T, ctx context.Context, osd int32, route osdFixtureOracle) copyStagingSnapshot {
	t.Helper()
	in := map[string]any{"osd": osd, "pgid": route.PGID}
	shard, err := copyStagingShard(route, osd)
	if err != nil {
		t.Fatal(err)
	}
	if shard >= 0 {
		in["shard"] = shard
	}
	var result struct {
		Staging copyStagingSnapshot `json:"copy_staging"`
	}
	nativePoolResult(t, ctx, "copystaging", in, &result)
	return result.Staging
}

// copyChunk runs one copychunk action on one OSD.
func copyChunk(t *testing.T, ctx context.Context, osd int32, action string) copyChunkSetting {
	t.Helper()
	var result struct {
		Setting copyChunkSetting `json:"copy_chunk"`
	}
	nativePoolResult(t, ctx, "copychunk", map[string]any{"osd": osd, "action": action}, &result)
	return result.Setting
}

// copyTempPrefix is how a listing prints the temporary objects of the
// destination pool: pool -2-<pool> (src/common/hobject.h:304-309), and a
// name a primary (role 0) generated for the destination PG.
func copyTempPrefix(pool int64) string { return "#" + strconv.FormatInt(-2-pool, 10) + ":" }

// checkCopyTemp says whether one complete listing of route's PG on osd holds
// exactly the temporary objects in want, which the primary of route's PG
// generated for its pool. On an erasure-coded route the listing is the
// collection of osd's shard, whose entries are printed with that shard in
// hexadecimal, and the primary names its temporary objects after its own
// <pgid>s<shard> (generate_temp_object prints info.pgid, a spg_t). An entry
// with a generation (an erasure-coded rollback copy) is not a staging object.
// The two scrub-error stores a scrub of this collection's own PG shard keeps
// as temporary objects (src/osd/scrubber/ScrubStore.cc:107-118, scrub_<pg>
// and deep_scrub_<pg>) are not counted; every other temporary object is
// refused. It returns the one temporary object without the shard, so the
// listings of every shard of one PG name it alike.
func checkCopyTemp(s copyStagingSnapshot, osd int32, route osdFixtureOracle, want int) (string, error) {
	pgid, prefix, owner, own := route.PGID, copyTempPrefix(route.Pool), route.PGID, route.PGID
	shard, err := copyStagingShard(route, osd)
	if err != nil {
		return "", err
	}
	collection := pgid + "_head"
	if shard >= 0 {
		collection = fmt.Sprintf("%ss%d_head", pgid, shard)
		prefix = strconv.FormatInt(int64(shard), 16) + prefix
		owner = fmt.Sprintf("%ss%d", pgid, route.Shard)
		own = fmt.Sprintf("%ss%d", pgid, shard)
	}
	if !s.Complete || len(s.Failures) != 0 || s.OSD != osd || s.PGID != pgid || s.Shard != shard || s.Collection != collection || s.Objects == 0 || s.Objects >= s.Max {
		return "", fmt.Errorf("the listing of %s on osd.%d is not complete: osd.%d %s shard %d %d/%d objects %q", collection, osd, s.OSD, s.Collection, s.Shard, s.Objects, s.Max, s.Failures)
	}
	var staged []string
	for _, entry := range s.Temp {
		if strings.HasPrefix(entry, prefix) && (strings.HasSuffix(entry, ":::scrub_"+own+":head#") || strings.HasSuffix(entry, ":::deep_scrub_"+own+":head#")) {
			continue
		}
		if !strings.HasPrefix(entry, prefix) || !strings.Contains(entry, ":::temp_"+owner+"_0_") || !strings.HasSuffix(entry, ":head#") {
			return "", fmt.Errorf("osd.%d lists a temporary object %q that is not a copy staging object of %s", osd, entry, owner)
		}
		staged = append(staged, entry)
	}
	if len(staged) != want {
		return "", fmt.Errorf("osd.%d lists %d temporary objects in %s (want %d): %q", osd, len(staged), pgid, want, s.Temp)
	}
	if want == 0 {
		return "", nil
	}
	return strings.TrimPrefix(staged[0], strings.TrimSuffix(prefix, copyTempPrefix(route.Pool))), nil
}

// checkSourceWriteHeld judges one bracketed snapshot of the source primary
// while a destination replica is paused: its op tracker holds exactly one op
// of the writing client, the source write, sent to its replicas and not yet
// answered. That write holds the source's write lock. It returns the op.
func checkSourceWriteHeld(ops opsInFlightSnapshot, route osdFixtureOracle, gid uint64, incarnation int32) (opsInFlightOp, error) {
	if !ops.Complete || len(ops.Failures) != 0 || ops.Code != 0 || ops.AbsenceEstablished {
		return opsInFlightOp{}, fmt.Errorf("incomplete op tracker snapshot of the source primary: %q", ops.Failures)
	}
	if !samePlacement(ops.Before, ops.After) || !samePlacement(ops.Before, route) || ops.OSD != route.ID || ops.PGID != route.PGID {
		return opsInFlightOp{}, fmt.Errorf("the snapshot of osd.%d for %s is not bracketed by the source route of osd.%d for %s", ops.OSD, ops.PGID, route.ID, route.PGID)
	}
	if want := fmt.Sprintf("client.%d.%d", gid, incarnation); ops.Client != want || len(ops.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want the one write of %s)", len(ops.Ops), ops.Client, want)
	}
	op := ops.Ops[0]
	if op.TID == 0 || (op.PGID != route.PGID && op.PGID != route.RawPGID) || op.FlagPoint != "waiting for sub ops" || slices.Contains(op.Events, "commit_sent") {
		return op, fmt.Errorf("the source write %s for %s is at %q with events %q; want it waiting for sub ops", op.Reqid, op.PGID, op.FlagPoint, op.Events)
	}
	return op, nil
}

// copyCursorInput is the input length the Objecter prints for a copy-get
// whose cursor holds an OMAP position of omapOffset bytes: the cursor's
// encoding (ENCODE_START 6 bytes, attr_complete 1, data_offset 8,
// data_complete 1, omap_offset 4+omapOffset, omap_complete 1;
// src/osd/osd_types.cc:5492-5501) and then the u64 max
// (src/osdc/Objecter.h:1184-1206). The print carries no other cursor field,
// so a later chunk's data offset is not observable here; only its OMAP
// position's length is.
func copyCursorInput(omapOffset int) int { return 29 + omapOffset }

// copyFirstChunk is exactly what the Objecter prints for a copy-get of the
// lowered chunk, max bytes (multiChunkLimit, or that limit rounded up to an
// erasure-coded destination's stripe width), without assert-version whose
// cursor's OMAP position is omapOffset bytes, and copyLaterChunk for one
// asserting version (tools/osd_objecter_requests_oracle.py). A first copy-get
// has the initial cursor, omapOffset 0.
func copyFirstChunk(ops []string, max, omapOffset int) bool {
	return slices.Equal(ops, []string{fmt.Sprintf("copy-get max %d in=%db", max, copyCursorInput(omapOffset))})
}

func copyLaterChunk(ops []string, max int, version uint64, omapOffset int) bool {
	return len(ops) == 2 && ops[0] == "assert-version v"+strconv.FormatUint(version, 10) && copyFirstChunk(ops[1:], max, omapOffset)
}

// checkFirstChunkSent judges one snapshot of the destination primary's
// Objecter: exactly one op of the source, the first copy-get, sent once to
// the source primary, and paused or not as want says. A paused op with one
// send attempt was sent before the Objecter consumed the map with pauserd.
// The first copy-get carries the initial cursor.
func checkFirstChunkSent(objecter objecterRequestsSnapshot, route osdFixtureOracle, sourcePrimary int32, paused bool) (objecterRequestsOp, error) {
	if !objecter.Complete || len(objecter.Failures) != 0 || objecter.Code != 0 || objecter.AbsenceEstablished {
		return objecterRequestsOp{}, fmt.Errorf("incomplete objecter snapshot: %q", objecter.Failures)
	}
	if !samePlacement(objecter.Before, objecter.After) || !samePlacement(objecter.Before, route) || objecter.OSD != route.ID {
		return objecterRequestsOp{}, fmt.Errorf("the objecter snapshot of osd.%d is not bracketed by the destination route of osd.%d", objecter.OSD, route.ID)
	}
	if len(objecter.Ops) != 1 {
		return objecterRequestsOp{}, fmt.Errorf("the objecter holds %d ops of the source; want the first copy-get", len(objecter.Ops))
	}
	read := objecter.Ops[0]
	if !copyFirstChunk(read.OSDOps, multiChunkLimit, 0) || read.Attempts != 1 || read.Paused != paused || read.OSD != sourcePrimary {
		return read, fmt.Errorf("the source op is not the first copy-get sent once to osd.%d (paused %t): %+v", sourcePrimary, paused, read)
	}
	return read, nil
}

// checkChunkHeld judges the barrier from two bracketed snapshots of the
// destination primary under pauserd: its op tracker holds the client op
// (checkCopyClientOp) with no sub-op event and no reply, and its Objecter
// holds exactly one op of the source, paused and never sent, that is a
// copy-get of the lowered chunk: a later chunk asserting version when
// version is not zero, otherwise a first chunk, whose cursor's OMAP position
// is exactly omapOffset bytes, of max bytes. The op is not one of earlier. It
// returns both ops.
func checkChunkHeld(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, route osdFixtureOracle, gid uint64, incarnation int32, max int, version uint64, omapOffset int, earlier []uint64) (opsInFlightOp, objecterRequestsOp, error) {
	op, err := checkCopyClientOp(ops, objecter, route, gid, incarnation)
	if err != nil {
		return op, objecterRequestsOp{}, err
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished {
		return op, objecterRequestsOp{}, errors.New("a snapshot claims absence")
	}
	for _, event := range op.Events {
		if strings.HasPrefix(event, "waiting for subops from ") || event == "op_commit" || event == "commit_sent" {
			return op, objecterRequestsOp{}, fmt.Errorf("the client op already issued its transaction: %q", op.Events)
		}
	}
	if op.FlagPoint == "waiting for sub ops" || strings.HasPrefix(op.FlagPoint, "commit sent") {
		return op, objecterRequestsOp{}, fmt.Errorf("the client op is at %q", op.FlagPoint)
	}
	if len(objecter.Ops) != 1 {
		return op, objecterRequestsOp{}, fmt.Errorf("the objecter holds %d ops of the source; want one held copy-get", len(objecter.Ops))
	}
	read := objecter.Ops[0]
	chunk := copyFirstChunk(read.OSDOps, max, omapOffset)
	if version != 0 {
		chunk = copyLaterChunk(read.OSDOps, max, version, omapOffset)
	}
	if !read.Paused || read.Attempts != 0 || !chunk || slices.Contains(earlier, read.TID) {
		return op, read, fmt.Errorf("the source op is not a new paused, never sent copy-get (max %d, assert v%d, in=%db): %+v", max, version, copyCursorInput(omapOffset), read)
	}
	return op, read, nil
}

// checkReplicaLost says whether now is the destination PG under the same
// primary after the source primary lost was marked down: a newer epoch, the
// same PG and primary, and lost in neither the up nor the acting set.
func checkReplicaLost(before, now osdFixtureOracle, lost int32) error {
	if now.Epoch <= before.Epoch || now.PGID != before.PGID || now.Pool != before.Pool || now.Name != before.Name {
		return fmt.Errorf("placement %+v is not a newer epoch of %+v", now, before)
	}
	if now.ID != before.ID || now.PrimaryID != now.ID || now.Address != before.Address || slices.Contains(now.Up, lost) || slices.Contains(now.Acting, lost) {
		return fmt.Errorf("osd.%d (up %v, acting %v) is not the same primary without osd.%d", now.ID, now.Up, now.Acting, lost)
	}
	return nil
}

// copyChunkUnknown is the submission of a copy whose wait ended unknown.
// Cause stores only the error class because the error text can carry
// endpoints.
type copyChunkUnknown struct {
	TID         uint64 `json:"tid"`
	GlobalID    uint64 `json:"global_id"`
	Incarnation uint32 `json:"incarnation"`
	Epoch       uint32 `json:"epoch"`
	Cause       string `json:"cause"`
}

// copyChunkLossCell is one opcode and which primary is lost between chunks.
type copyChunkLossCell struct {
	copyPrimaryChangeCase
	lost string
}

func (c copyChunkLossCell) label() string { return c.name + "/" + c.lost + "-primary" }

// copyChunkLossReceipt is the archived public receipt of every cell:
// identities, versions, digests and native observations, never keys or
// addresses.
type copyChunkLossReceipt struct {
	Cell          string            `json:"cell"`
	SourcePrimary int32             `json:"source_primary"`
	Primary       int32             `json:"destination_primary"`
	Paused        int32             `json:"paused_replica"`
	PausedFor     string            `json:"paused_for"`
	Chunk         uint64            `json:"chunk_limit"`
	SourceBytes   int               `json:"source_bytes"`
	SourceUser    uint64            `json:"source_user_version"`
	WriteReqid    string            `json:"held_write_reqid"`
	FirstChunkTID uint64            `json:"first_copy_get_tid"`
	BarrierEpoch  uint32            `json:"barrier_epoch"`
	BarrierReqid  string            `json:"barrier_reqid"`
	BarrierPoint  string            `json:"barrier_flag_point"`
	BarrierEvents []string          `json:"barrier_events"`
	HeldChunk     []string          `json:"held_copy_get"`
	HeldTID       uint64            `json:"held_copy_get_tid"`
	Temporary     string            `json:"temporary_object"`
	StagingSHA256 []string          `json:"staging_sha256"`
	KillExit      int               `json:"kill_exit_code"`
	Unknown       *copyChunkUnknown `json:"unknown_outcome,omitempty"`
	MovedEpoch    uint32            `json:"moved_epoch"`
	NewPrimary    int32             `json:"new_primary"`
	Restarted     []string          `json:"restarted_copy_get,omitempty"`
	TempRemoved   []int32           `json:"temporary_removed_on"`
	Native        string            `json:"native_after_loss"`
	Reply         struct {
		Code        int32   `json:"code"`
		OpResults   []int32 `json:"op_results"`
		TID         uint64  `json:"tid"`
		Attempt     int32   `json:"attempt"`
		Flags       uint32  `json:"flags"`
		UserVersion uint64  `json:"user_version"`
		Replay      string  `json:"replay_version"`
		Front       int     `json:"raw_front_bytes"`
	} `json:"reply"`
	Final        string  `json:"native_final"`
	TempAfter    []int32 `json:"no_temporary_after_recovery_on"`
	ChunkRestore bool    `json:"chunk_restored"`
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of a 64 KiB source onto
// a natively seeded destination runs in two chunks because the destination
// primary's osd_copyfrom_max_chunk is lowered to 32 KiB, and it is held
// between them by the barrier described above. Then one primary is SIGKILLed.
//
// Destination primary lost: the caller's wait ends as an unknown outcome
// carrying the barrier's transaction. After the dead OSD is marked down and
// pause is cleared, the destination head is still the seed, the new
// primary's PG log names neither the copy nor the source write, and both
// survivors' listings lose the temporary object. A second Go write changes
// the source, and one explicit caller resubmission to the new primary must
// execute the copy once, copying the second source; its raw aggregate and
// per-op results are kept. After recovery the original primary leads again:
// a fresh-session read, a delayed native readback and its PG log show that
// single copy, and the restarted OSD lists no temporary object.
//
// Source primary lost: it is also a replica of the destination PG, so the
// destination primary cancels the copy and requeues the client op. Under
// pause the restarted copy must be held at a first copy-get again, a new op
// without assert-version, with the old temporary object gone from both
// surviving destination replicas. After pause is cleared the same client op
// is answered once on its original connection: code 0, per-op 0, the source
// write's user version, the complete copy natively, the copy once in the PG
// log bound to the source write, and nothing beyond it on a fresh session.
//
// Three-chunk OMAP sources, the third OSD lost between chunks and versioned
// or snapshot sources changed between chunks are in
// osd_copyfrom_multichunk_sources_test.go, and erasure-coded destinations,
// including a loss before the primary's local commit of the final copy
// transaction, in osd_erasure_copyfrom_sources_test.go. Not verified: that
// loss on this replicated destination (no stock control holds a BlueStore
// commit, and the final transaction is queued locally in the call that
// issues it; see #7) and the legacy one-PG pools when their primaries
// coincide.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfrommultichunkloss key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfrommultichunkloss key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDCopyFromMultiChunkPrimaryLossIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var mu sync.Mutex
	var receipts []copyChunkLossReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-multichunk-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("multi-chunk loss receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, lost := range []string{"destination", "source"} {
			cell := copyChunkLossCell{tc, lost}
			t.Run(cell.label(), func(t *testing.T) {
				receipt := copyChunkLossReceipt{Cell: cell.label(), Chunk: multiChunkLimit}
				defer func() {
					if t.Skipped() {
						return
					}
					mu.Lock()
					receipts = append(receipts, receipt)
					mu.Unlock()
				}()
				copyChunkLoss(t, ctx, pools, recovery, cell, &receipt)
			})
		}
	}
}

func copyChunkLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copyChunkLossCell, receipt *copyChunkLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("chunk-%d-%s", cell.code, cell.lost))
	primary := r.dstRoute.ID
	sourcePrimary, paused := r.separatePrimaries()
	receipt.SourcePrimary, receipt.Primary, receipt.Paused = sourcePrimary, primary, paused

	// Cleanups run in reverse: the cluster is restored first, so the chunk
	// restoration reads a running destination primary.
	lowered := false
	t.Cleanup(func() {
		if !lowered {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		restored := copyChunk(t, cleanup, primary, "restore")
		receipt.ChunkRestore = restored.Restored && !restored.Claimed
		if !receipt.ChunkRestore {
			t.Errorf("osd_copyfrom_max_chunk of osd.%d was not restored: %+v", primary, restored)
		}
	})
	t.Cleanup(func() { recovery.restore(t) })
	lowered = true
	setting := copyChunk(t, ctx, primary, "lower")
	if !setting.Lowered || setting.Value != multiChunkLimit || setting.Prior <= 2*multiChunkLimit || len(setting.Failures) != 0 {
		t.Fatalf("osd_copyfrom_max_chunk of osd.%d was not lowered to %d: %+v", primary, multiChunkLimit, setting)
	}
	t.Logf("osd_copyfrom_max_chunk of osd.%d lowered from %d to %d", primary, setting.Prior, setting.Value)

	first, firstSource := r.writeSource("source seed", 1700009000, []byte("multi-chunk seed source"), []byte{8, 0xff})
	poolSeed(t, ctx, 2, r.dest, "", []byte("seeded destination bytes"), []nativeXattrEntry{{Name: []byte("seed-attr"), Value: []byte{7}}})
	seeded := poolObserve(t, ctx, 2, r.dest, "", "")
	if seeded.Code != 0 || seeded.Version == 0 || firstSource.Version != first.UserVersion {
		t.Fatal("native destination seed", seeded.Code)
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("before the copy:", err)
		}
	}
	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*13 + i>>9 + 1)
	}
	route := poolPlace(t, ctx, 2, r.dest, "")
	source := poolPlace(t, ctx, 1, r.source, "")
	if !samePlacement(route, r.dstRoute) || !samePlacement(source, r.srcRoute) {
		t.Fatalf("placements moved before the copy: destination %+v, source %+v", route, source)
	}
	held := r.holdBetweenChunks(cell.code, route, source, paused, data, receipt)
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: held.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: held.write.Transaction}
	if cell.lost == "destination" {
		r.loseDestinationBetweenChunks(held, cell.code, seeded, copyKey, writeKey, data, receipt)
	} else {
		r.loseReplicaBetweenChunks(held, held.source.ID, copyKey, writeKey, data, receipt)
	}
	// The restarted OSD cleared every temporary object when it started; the
	// others removed theirs at the interval change or the final rename.
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
}

// separatePrimaries gives the source a fresh name whose primary is not the
// destination primary (separateSource) and returns that source primary and
// the third OSD, which replicates both PGs.
func (r *copyPrimaryChangeRun) separatePrimaries() (int32, int32) {
	t := r.t
	t.Helper()
	primary, sourcePrimary := r.dstRoute.ID, r.separateSource()
	var paused int32 = -1
	for _, osd := range r.dstRoute.Acting {
		if osd != primary && osd != sourcePrimary {
			paused = osd
		}
	}
	if paused < 0 || len(r.srcRoute.Acting) != 3 || !slices.Contains(r.srcRoute.Acting, paused) || !slices.Contains(r.dstRoute.Acting, sourcePrimary) {
		t.Fatalf("no third OSD replicates both PGs: source %v (primary osd.%d), destination %v (primary osd.%d)", r.srcRoute.Acting, sourcePrimary, r.dstRoute.Acting, primary)
	}
	return sourcePrimary, paused
}

// separateSource gives the source a fresh name whose primary is not the
// destination primary, when the first one shares it, and returns that source
// primary. The one-PG legacy owned pools cannot separate them, so the
// subtest is skipped there.
func (r *copyPrimaryChangeRun) separateSource() int32 {
	t, ctx := r.t, r.ctx
	t.Helper()
	primary := r.dstRoute.ID
	if r.srcRoute.ID == primary {
		// The OSD a barrier pauses or freezes must be neither primary, so
		// they must differ.
		if !isReplicatedPGFixtureProfile() {
			t.Skipf("the one-PG legacy owned pools put the source and the destination on osd.%d", primary)
		}
		found := false
		for i := 0; i < 64 && !found; i++ {
			name := fmt.Sprintf("%s-%d", r.source, i)
			route := poolPlace(t, ctx, 1, name, "")
			if route.ID != primary && route.PrimaryID == route.ID && poolObserve(t, ctx, 1, name, "", "").Code == -2 {
				r.source, r.srcRoute, found = name, route, true
			}
		}
		if !found {
			t.Fatalf("no fresh source name in 64 has a primary other than osd.%d", primary)
		}
	}
	return r.srcRoute.ID
}

// copyChunkHold is the state of a copy held between its chunks.
type copyChunkHold struct {
	o      *cephmsgr.OSDConnection
	done   chan copyChunkOutcome
	route  osdFixtureOracle
	source osdFixtureOracle
	write  cephmsgr.OSDReply
	op     opsInFlightOp
	read   objecterRequestsOp
	first  objecterRequestsOp
	// served are the copy-gets sent before the barrier, oldest first.
	served []uint64
	temp   string
	// max is the copy-get max every chunk prints.
	max int
}

type copyChunkOutcome struct {
	reply cephmsgr.OSDReply
	err   error
}

// pending fails the test if the held copy was already answered.
func (h *copyChunkHold) pending(t *testing.T, stage string) {
	t.Helper()
	select {
	case got := <-h.done:
		t.Fatalf("the held copy was answered %s: code %d err %v", stage, got.reply.Code, got.err)
	default:
	}
}

// holdBetweenChunks builds the barrier: it pauses the third OSD, holds a Go
// write of data on the source primary, sends the copy, sees its first
// copy-get sent, sets pause and sees that copy-get paused after its send,
// resumes the third OSD, and waits for the barrier of the second chunk and
// the temporary object on all three OSDs.
func (r *copyPrimaryChangeRun) holdBetweenChunks(code cephmsgr.OSDOpcode, route, source osdFixtureOracle, paused int32, data []byte, receipt *copyChunkLossReceipt) *copyChunkHold {
	t, ctx := r.t, r.ctx
	t.Helper()
	h := &copyChunkHold{done: make(chan copyChunkOutcome, 1), route: route, source: source, max: multiChunkLimit}
	writer := mutationConnection(t, r.client, ctx, source)
	defer writer.Close()
	h.o = mutationConnection(t, r.client, ctx, route)
	t.Cleanup(func() { h.o.Close() })
	write := r.pools.request(source, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "copy-primary-attr"}, Data: []byte{9, 0xfe}})
	write.ClientIncarnation, write.ModificationTime = copyPrimaryWriteIncarnation, cephmsgr.OSDTime{Seconds: 1700009050}
	written := make(chan copyChunkOutcome, 1)

	frozen := time.Now()
	// The replica must be resumed well inside its 20 s heartbeat grace and
	// the 16 s read lease, as in the destination-loss test.
	thawDeadline := frozen.Add(16 * time.Second)
	workDeadline := thawDeadline.Add(-3 * time.Second)
	// Native observations run under the held phase: one still outstanding
	// at its end is abandoned and fails the test, and the fixture resumes
	// the replica at the lease's deadline all the same (freeze).
	work, stopWork := context.WithDeadline(ctx, workDeadline)
	defer stopWork()
	r.recovery.freeze(t, work, paused, thawDeadline)
	go func() {
		reply, err := writer.Request(ctx, write)
		written <- copyChunkOutcome{reply, err}
	}()
	var writeOp opsInFlightOp
	for {
		ops := copyOpsInFlight(t, work, 1, r.source, nil, r.gid, copyPrimaryWriteIncarnation)
		var err error
		if writeOp, err = checkSourceWriteHeld(ops, source, r.gid, copyPrimaryWriteIncarnation); err == nil && work.Err() == nil {
			break
		} else if work.Err() != nil {
			t.Fatalf("the source write was not held before the pause budget expired: %v (%v)\nop tracker: %s", err, work.Err(), boundedRaw(ops.Raw))
		}
		time.Sleep(100 * time.Millisecond)
	}
	receipt.WriteReqid = writeOp.Reqid

	call, cancel := context.WithTimeout(ctx, 600*time.Second)
	t.Cleanup(cancel)
	go func() {
		reply, err := h.o.Request(call, r.copyRequest(code, route))
		h.done <- copyChunkOutcome{reply, err}
	}()
	for {
		h.pending(t, "before its first copy-get was sent")
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		var err error
		if h.first, err = checkFirstChunkSent(objecter, route, source.ID, false); err == nil && work.Err() == nil {
			break
		} else if work.Err() != nil {
			t.Fatalf("the first copy-get was not sent before the pause budget expired: %v (%v)\nobjecter: %s", err, work.Err(), boundedRaw(objecter.Raw))
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.recovery.pauserd = true
	if err := copyMonCommand(work, r.client, `{"prefix":"osd set","key":"pause"}`); err != nil {
		t.Fatal("osd set pause", err)
	}
	for {
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		got, err := checkFirstChunkSent(objecter, route, source.ID, true)
		if err == nil && got.TID == h.first.TID && work.Err() == nil {
			break
		} else if work.Err() != nil {
			t.Fatalf("the sent first copy-get was not paused before the pause budget expired: %v (%v)\nobjecter: %s", err, work.Err(), boundedRaw(objecter.Raw))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := r.recovery.thaw(paused); err != nil {
		t.Fatalf("resume osd.%d within the pause budget: %v", paused, err)
	}
	receipt.PausedFor = time.Since(frozen).Round(10 * time.Millisecond).String()
	receipt.FirstChunkTID = h.first.TID
	h.served = []uint64{h.first.TID}
	t.Logf("osd.%d paused for %s: source write %s held on osd.%d, first copy-get tid %d sent to osd.%d and paused after its send",
		paused, receipt.PausedFor, writeOp.Reqid, source.ID, h.first.TID, source.ID)
	if flags := nativePool(t, ctx, "osdflags", map[string]any{"action": "show"}); !slices.Contains(flags.OSDMapFlags, "pauserd") || !slices.Contains(flags.OSDMapFlags, "pausewr") {
		t.Fatalf("native OSDMap does not carry pauserd and pausewr: epoch %d flags %v", flags.Epoch, flags.OSDMapFlags)
	}

	var got copyChunkOutcome
	select {
	case got = <-written:
	case <-time.After(60 * time.Second):
		t.Fatal("the held source write was not answered after the replica resumed")
	}
	if got.err != nil || got.reply.Code != 0 || got.reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || got.reply.Transaction != writeOp.TID || got.reply.UserVersion == 0 {
		t.Fatalf("held source write (tracker tid %d): err=%v code=%d flags=%#x tid=%d", writeOp.TID, got.err, got.reply.Code, got.reply.Flags, got.reply.Transaction)
	}
	h.write = got.reply
	receipt.SourceBytes, receipt.SourceUser = len(data), h.write.UserVersion

	for deadline := time.Now().Add(60 * time.Second); ; {
		h.pending(t, "before the barrier")
		ops := copyOpsInFlight(t, ctx, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, ctx, 2, r.dest, nil, r.pools.ids[1], r.source)
		var err error
		if h.op, h.read, err = checkChunkHeld(ops, objecter, route, r.gid, copyPrimaryCopyIncarnation, h.max, h.write.UserVersion, 0, h.served); err == nil {
			receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = ops.Before.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
			receipt.HeldChunk, receipt.HeldTID = h.read.OSDOps, h.read.TID
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("no barrier between chunks: %v\nop tracker: %s\nobjecter: %s", err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
		}
		time.Sleep(250 * time.Millisecond)
	}
	// The chunk's write is replicated asynchronously; the hold keeps the
	// copy from going further, so the listings converge on one object.
	for _, osd := range route.Acting {
		for deadline := time.Now().Add(30 * time.Second); ; {
			s := copyStaging(t, ctx, osd, route)
			temp, err := checkCopyTemp(s, osd, route, 1)
			if err == nil && (h.temp == "" || temp == h.temp) {
				h.temp = temp
				receipt.StagingSHA256 = append(receipt.StagingSHA256, s.SHA256)
				break
			}
			if err == nil {
				err = fmt.Errorf("osd.%d lists %q, osd.%d %q", osd, temp, route.ID, h.temp)
			}
			if time.Now().After(deadline) {
				t.Fatal("the staged chunk is not listed on every OSD:", err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	receipt.Temporary = h.temp
	h.pending(t, "at the barrier")
	t.Logf("barrier on osd.%d epoch %d: op %s flag point %q events %q; objecter tid %d paused %t attempts %d ops %q; temporary %s on osd %v",
		route.ID, receipt.BarrierEpoch, h.op.Reqid, h.op.FlagPoint, h.op.Events, h.read.TID, h.read.Paused, h.read.Attempts, h.read.OSDOps, h.temp, route.Acting)
	return h
}

// recordReply keeps the raw aggregate and per-op results of the reply that
// answered the copy.
func (receipt *copyChunkLossReceipt) recordReply(reply cephmsgr.OSDReply) {
	receipt.Reply.Code, receipt.Reply.TID, receipt.Reply.Attempt = reply.Code, reply.Transaction, reply.RetryAttempt
	receipt.Reply.Flags, receipt.Reply.UserVersion, receipt.Reply.Replay, receipt.Reply.Front = uint32(reply.Flags), reply.UserVersion, replayVersion(reply), len(reply.RawFront)
	receipt.Reply.OpResults = nil
	for _, op := range reply.Operations {
		receipt.Reply.OpResults = append(receipt.Reply.OpResults, op.Result)
	}
}

// waitTempRemoved polls one surviving OSD until its complete listing of the
// destination PG holds no temporary object.
func (r *copyPrimaryChangeRun) waitTempRemoved(osd int32, route osdFixtureOracle) {
	r.t.Helper()
	for deadline := time.Now().Add(60 * time.Second); ; {
		_, err := checkCopyTemp(copyStaging(r.t, r.ctx, osd, route), osd, route, 0)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatal("the temporary object was not removed:", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// observeWritten reads the source once pause is cleared: it must be the held
// write's data, attribute and user version.
func (r *copyPrimaryChangeRun) observeWritten(h *copyChunkHold, data []byte) nativeXattrSnapshot {
	r.t.Helper()
	written := poolObserve(r.t, r.ctx, 1, r.source, "", "")
	if written.Code != 0 || !bytes.Equal(written.Data, data) || written.Version != h.write.UserVersion || attrNames(written)["copy-primary-attr"] != string([]byte{9, 0xfe}) {
		r.t.Fatalf("native source differs from the held write: code %d version %d (reply %d) %d bytes", written.Code, written.Version, h.write.UserVersion, len(written.Data))
	}
	return written
}

// loseDestinationBetweenChunks SIGKILLs the destination primary at the
// barrier and checks the unknown outcome, the kept seed, the temporary
// object's removal on the survivors and one explicit resubmission.
func (r *copyPrimaryChangeRun) loseDestinationBetweenChunks(h *copyChunkHold, code cephmsgr.OSDOpcode, seeded nativeXattrSnapshot, copyKey, writeKey pgHistoryTarget, data []byte, receipt *copyChunkLossReceipt) {
	r.t.Helper()
	receipt.KillExit = r.recovery.kill(r.t, r.ctx, h.route.ID)
	r.settleDestinationLoss(h, code, seeded, copyKey, writeKey, data, receipt)
}

// settleDestinationLoss checks what a destination primary SIGKILLed while
// it held the copy left: the unknown outcome, the kept seed, the temporary
// object's removal on the survivors and one explicit resubmission.
func (r *copyPrimaryChangeRun) settleDestinationLoss(h *copyChunkHold, code cephmsgr.OSDOpcode, seeded nativeXattrSnapshot, copyKey, writeKey pgHistoryTarget, data []byte, receipt *copyChunkLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	primary := h.route.ID
	var lost copyChunkOutcome
	select {
	case lost = <-h.done:
	case <-time.After(60 * time.Second):
		t.Fatal("the copy's wait did not end after its primary was killed")
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != h.op.TID || lost.reply.Submission.GlobalID != r.gid ||
		lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != h.route.Epoch {
		t.Fatalf("lost copy metadata (barrier tid %d): %+v %v", h.op.TID, lost.reply, lost.err)
	}
	receipt.Unknown = &copyChunkUnknown{TID: lost.reply.Submission.Transaction, GlobalID: lost.reply.Submission.GlobalID,
		Incarnation: lost.reply.Submission.ClientIncarnation, Epoch: lost.reply.Submission.MapEpoch, Cause: outcomeCause(lost.err)}
	h.o.Close()
	t.Logf("osd.%d killed (exit %d) between chunks; the copy ended unknown (%s): %v", primary, receipt.KillExit, receipt.Unknown.Cause, lost.err)
	r.recovery.markDown(t, ctx, primary)
	r.restartedPrimary = true
	moved := r.waitMoved()
	receipt.NewPrimary, receipt.MovedEpoch = moved.ID, moved.Epoch
	// An optimized erasure-coded PG leads from another shard under a
	// pg_temp; every survivor keeps the shard it held, so its collection is
	// the one the original route names.
	listing := moved
	if moved.PGTemp && h.route.Shard >= 0 {
		listing = h.route
	}
	for _, osd := range h.route.Acting {
		if osd != primary {
			r.waitTempRemoved(osd, listing)
			receipt.TempRemoved = append(receipt.TempRemoved, osd)
		}
	}
	r.recovery.clearPauseReads(t, ctx)

	// Native evidence of the loss's result, before any resubmission.
	native := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(native)
	if !sameNative(native, seeded) {
		t.Fatalf("the destination is not the seed after the loss: %s; seed %s", nativeDigest(native), nativeDigest(seeded))
	}
	r.observeWritten(h, data)
	if s := r.history(moved, copyKey, writeKey); len(s.Targets[0].Locations) != 0 || len(s.Targets[1].Locations) != 0 {
		t.Fatalf("the new primary's PG log names the lost copy: %+v %+v", s.Targets[0].Locations, s.Targets[1].Locations)
	}
	t.Logf("seed kept after losing osd.%d between chunks: new primary osd.%d at epoch %d; temporary removed on osd %v", primary, moved.ID, moved.Epoch, receipt.TempRemoved)

	second, secondSource := r.writeSource("second source write", 1700009200, []byte("multi-chunk loss second source bytes"), []byte{6, 0xfe})
	again := r.copyRequest(code, moved)
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission to the new primary", moved, again)
	receipt.recordReply(answered)
	t.Logf("resubmitted to osd.%d: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, receipt.Reply.OpResults,
		answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if answered.Transaction != h.op.TID || answered.RetryAttempt != 1 || len(answered.Operations) != 1 || answered.Operations[0].Result != 0 || answered.UserVersion != second.UserVersion {
		t.Fatalf("the resubmission did not execute once: tid %d attempt %d ops %v user %d (second source %d)", answered.Transaction, answered.RetryAttempt, receipt.Reply.OpResults, answered.UserVersion, second.UserVersion)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	if !sameCopy(after, secondSource, second.UserVersion) {
		t.Fatalf("the resubmission did not copy the second source: %s", nativeDigest(after))
	}
	secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
	binding := copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(answered), sourceUserVersion: second.UserVersion}
	// A complete copy carries up to ten request IDs of the source object's
	// log (src/osd/PrimaryLogPG.cc:9439-9446), so the held write is bound
	// too, as an earlier source mutation; the binding names the second.
	s := r.history(moved, copyKey, secondKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("new primary osd.%d after the resubmission: %v", moved.ID, err)
	}
	r.finish(after, answered.Transaction, copyKey, secondKey, binding)
	receipt.Final = nativeDigest(after)
}

// loseReplicaBetweenChunks SIGKILLs lost, a replica of the destination PG
// (the source primary, or the third OSD), at the barrier and checks that the
// copy restarts from its first chunk under the same client op and is
// answered once.
func (r *copyPrimaryChangeRun) loseReplicaBetweenChunks(h *copyChunkHold, lost int32, copyKey, writeKey pgHistoryTarget, data []byte, receipt *copyChunkLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	receipt.KillExit = r.recovery.kill(t, ctx, lost)
	r.recovery.markDown(t, ctx, lost)
	var now, last osdFixtureOracle
	for deadline := time.Now().Add(120 * time.Second); ; {
		now = poolPlace(t, ctx, 2, r.dest, "")
		err := checkReplicaLost(h.route, now, lost)
		if err == nil && samePlacement(now, last) && now.Epoch == last.Epoch {
			break
		}
		last = now
		if time.Now().After(deadline) {
			t.Fatalf("the destination PG did not settle without osd.%d: %v", lost, err)
		}
		time.Sleep(time.Second)
	}
	receipt.NewPrimary, receipt.MovedEpoch = now.ID, now.Epoch
	// The client op keeps the epoch it was sent at.
	at := now
	at.Epoch = h.route.Epoch
	for deadline := time.Now().Add(60 * time.Second); ; {
		h.pending(t, fmt.Sprintf("after osd.%d was lost", lost))
		ops := copyOpsInFlight(t, ctx, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, ctx, 2, r.dest, nil, r.pools.ids[1], r.source)
		op, read, err := checkChunkHeld(ops, objecter, at, r.gid, copyPrimaryCopyIncarnation, h.max, 0, 0, append(slices.Clone(h.served), h.read.TID))
		if err == nil && op.TID != h.op.TID {
			err = fmt.Errorf("the held client op is %s, not the barrier's %s", op.Reqid, h.op.Reqid)
		}
		if err == nil {
			receipt.Restarted = read.OSDOps
			t.Logf("after losing osd.%d at epoch %d: op %s restarted its copy, first copy-get tid %d paused %t attempts %d ops %q", lost, now.Epoch, op.Reqid, read.TID, read.Paused, read.Attempts, read.OSDOps)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy did not restart from its first chunk: %v\nop tracker: %s\nobjecter: %s", err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, osd := range now.Acting {
		// An erasure-coded PG keeps the lost shard's position empty.
		if osd == crushItemNone {
			continue
		}
		r.waitTempRemoved(osd, now)
		receipt.TempRemoved = append(receipt.TempRemoved, osd)
	}
	r.recovery.clearPauseReads(t, ctx)

	var got copyChunkOutcome
	select {
	case got = <-h.done:
	case <-time.After(120 * time.Second):
		t.Fatal("the restarted copy was not answered after pause was cleared")
	}
	receipt.recordReply(got.reply)
	t.Logf("answered on the original connection: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s err=%v", got.reply.Code, receipt.Reply.OpResults,
		got.reply.Transaction, got.reply.RetryAttempt, got.reply.Flags, got.reply.UserVersion, replayVersion(got.reply), got.err)
	if got.err != nil || got.reply.Code != 0 || got.reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || got.reply.Transaction != h.op.TID || got.reply.RetryAttempt != 0 ||
		len(got.reply.Operations) != 1 || got.reply.Operations[0].Result != 0 || got.reply.UserVersion != h.write.UserVersion {
		t.Fatalf("the restarted copy was not answered once for the held write: %+v %v", receipt.Reply, got.err)
	}
	h.o.Close()
	written := r.observeWritten(h, data)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(after)
	if !sameCopy(after, written, h.write.UserVersion) {
		t.Fatalf("the destination is not the complete copy: %s; source %s", nativeDigest(after), nativeDigest(written))
	}
	binding := copyFromHistoryWant{object: hobjectHead(now), version: replayVersion(got.reply), sourceUserVersion: h.write.UserVersion}
	s := r.history(now, copyKey, writeKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("osd.%d after the copy: %v", now.ID, err)
	}
	r.finish(after, got.reply.Transaction, copyKey, writeKey, binding)
	receipt.Final = nativeDigest(after)
}

func TestCopyFromMultiChunkLossJudgements(t *testing.T) {
	const gid, inc, writeInc = 4641, 33, 31
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	source := osdFixtureOracle{Up: []int32{0, 2, 1}, Acting: []int32{0, 2, 1}, Replicas: 3, Pool: 3, Name: "src", ID: 0, PrimaryID: 0, Shard: -1, Epoch: 30, PGID: "3.5", RawPGID: "3.6a0b33c5", Address: "v2:192.0.2.10:6800/1"}
	at := route
	at.Epoch = 31

	temp := "#-6:c7a5d36b:::temp_4.3_0_4123_1:head#"
	listing := copyStagingSnapshot{OSD: 2, PGID: "4.3", Shard: -1, Collection: "4.3_head", Max: 4096, Objects: 3, Complete: true, Temp: []string{temp}}
	if got, err := checkCopyTemp(listing, 2, route, 1); err != nil || got != temp {
		t.Fatal("staged temporary refused:", got, err)
	}
	empty := listing
	empty.Temp, empty.Objects = nil, 2
	if _, err := checkCopyTemp(empty, 2, route, 0); err != nil {
		t.Fatal("empty staging refused:", err)
	}
	// The scrub-error stores of the PG are not staging objects.
	scrubbed := listing
	scrubbed.Temp = []string{"#-6:e0000000:::deep_scrub_4.3:head#", temp, "#-6:e0000000:::scrub_4.3:head#"}
	if got, err := checkCopyTemp(scrubbed, 2, route, 1); err != nil || got != temp {
		t.Fatal("staging beside the scrub stores refused:", got, err)
	}
	scrubbed.Temp = scrubbed.Temp[:1]
	if _, err := checkCopyTemp(scrubbed, 2, route, 0); err != nil {
		t.Fatal("a scrub store counted as staging:", err)
	}
	for name, edit := range map[string]func(*copyStagingSnapshot) (int32, int){
		"missing temporary":     func(s *copyStagingSnapshot) (int32, int) { s.Temp = nil; return 2, 1 },
		"temporary left":        func(*copyStagingSnapshot) (int32, int) { return 2, 0 },
		"two temporaries":       func(s *copyStagingSnapshot) (int32, int) { s.Temp = []string{temp, temp}; return 2, 1 },
		"incomplete":            func(s *copyStagingSnapshot) (int32, int) { s.Complete = false; return 2, 1 },
		"failure":               func(s *copyStagingSnapshot) (int32, int) { s.Failures = []string{"x"}; return 2, 1 },
		"another OSD":           func(*copyStagingSnapshot) (int32, int) { return 1, 1 },
		"another PG":            func(s *copyStagingSnapshot) (int32, int) { s.PGID = "4.4"; return 2, 1 },
		"another collection":    func(s *copyStagingSnapshot) (int32, int) { s.Collection = "4.3s0_head"; return 2, 1 },
		"a shard's listing":     func(s *copyStagingSnapshot) (int32, int) { s.Shard = 0; return 2, 1 },
		"not in the acting set": func(*copyStagingSnapshot) (int32, int) { return 3, 1 },
		"empty listing":         func(s *copyStagingSnapshot) (int32, int) { s.Objects = 0; return 2, 1 },
		"listing at its bound":  func(s *copyStagingSnapshot) (int32, int) { s.Objects = 4096; return 2, 1 },
		"other pool temporary": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-5:c7a5d36b:::temp_4.3_0_4123_1:head#"}
			return 2, 1
		},
		"replica-made temporary": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:c7a5d36b:::temp_4.3_1_4123_1:head#"}
			return 2, 1
		},
		"recovery temporary": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:c7a5d36b:::temp_recovering_4.3_30'2_5_head:head#"}
			return 2, 1
		},
		"other PG's temporary": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:c7a5d36b:::temp_4.30_0_4123_1:head#"}
			return 2, 1
		},
		"only a scrub store": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:e0000000:::deep_scrub_4.3:head#"}
			return 2, 1
		},
		"another PG's scrub store": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:e0000000:::scrub_4.30:head#"}
			return 2, 0
		},
		"another scrub-like name": func(s *copyStagingSnapshot) (int32, int) {
			s.Temp = []string{"#-6:e0000000:::deep_scrub_4.3_x:head#"}
			return 2, 0
		},
	} {
		s := listing
		osd, want := edit(&s)
		if _, err := checkCopyTemp(s, osd, route, want); err == nil {
			t.Error("staging accepted:", name)
		}
	}
	// An erasure-coded PG: osd.0 holds shard 1 of a PG whose primary osd.1
	// holds shard 0 and named the temporary object after 4.3s0.
	ec := route
	ec.PoolType, ec.Shard, ec.Up, ec.Acting = 3, 0, []int32{1, 0, 2, 3}, []int32{1, 0, 2, 3}
	if shard, err := copyStagingShard(ec, 0); err != nil || shard != 1 {
		t.Fatal("EC shard of osd.0 misjudged:", shard, err)
	}
	if shard, err := copyStagingShard(route, 0); err != nil || shard != -1 {
		t.Fatal("replicated shard misjudged:", shard, err)
	}
	ecTemp := "1#-6:c7a5d36b:::temp_4.3s0_0_4123_1:head#"
	shardListing := copyStagingSnapshot{OSD: 0, PGID: "4.3", Shard: 1, Collection: "4.3s1_head", Max: 4096, Objects: 3, Complete: true, Temp: []string{ecTemp}}
	if got, err := checkCopyTemp(shardListing, 0, ec, 1); err != nil || got != ecTemp[1:] {
		t.Fatal("staged EC temporary refused:", got, err)
	}
	// A shard's scrub stores are named after that shard of the PG.
	scrubShard := shardListing
	scrubShard.Temp = []string{"1#-6:e0000000:::deep_scrub_4.3s1:head#"}
	if _, err := checkCopyTemp(scrubShard, 0, ec, 0); err != nil {
		t.Fatal("the shard's own scrub store refused:", err)
	}
	scrubShard.Temp = []string{"1#-6:e0000000:::deep_scrub_4.3s0:head#"}
	if _, err := checkCopyTemp(scrubShard, 0, ec, 0); err == nil {
		t.Error("another shard's scrub store accepted on osd.0")
	}
	for name, edit := range map[string]func(*copyStagingSnapshot, *osdFixtureOracle) int32{
		"the replicated listing": func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 {
			s.Shard, s.Collection = -1, "4.3_head"
			return 0
		},
		"another shard": func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 {
			s.Shard, s.Collection = 2, "4.3s2_head"
			return 0
		},
		"another shard's entry": func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 { s.Temp = []string{"2" + ecTemp[1:]}; return 0 },
		"another primary shard": func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 {
			s.Temp = []string{"1#-6:c7a5d36b:::temp_4.3s1_0_4123_1:head#"}
			return 0
		},
		"an unsharded temporary": func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 {
			s.Temp = []string{"1#-6:c7a5d36b:::temp_4.3_0_4123_1:head#"}
			return 0
		},
		"a rollback generation":    func(s *copyStagingSnapshot, _ *osdFixtureOracle) int32 { s.Temp = []string{ecTemp + "1a"}; return 0 },
		"the shard of another OSD": func(*copyStagingSnapshot, *osdFixtureOracle) int32 { return 2 },
		"a pg_temp":                func(_ *copyStagingSnapshot, r *osdFixtureOracle) int32 { r.PGTemp = true; return 0 },
		"not acting":               func(_ *copyStagingSnapshot, r *osdFixtureOracle) int32 { r.Acting = []int32{1, 4, 2, 3}; return 0 },
	} {
		s, r := shardListing, ec
		r.Acting = slices.Clone(ec.Acting)
		osd := edit(&s, &r)
		if _, err := checkCopyTemp(s, osd, r, 1); err == nil {
			t.Error("EC staging accepted:", name)
		}
	}

	writing := opsInFlightSnapshot{Complete: true, Before: source, After: source, OSD: 0, PGID: "3.5", Client: "client.4641.31",
		Ops: []opsInFlightOp{{Reqid: "client.4641.31:7", TID: 7, PGID: "3.5", Epoch: 30, FlagPoint: "waiting for sub ops", Events: []string{"started", "waiting for subops from 1,2", "op_commit", "sub_op_commit_rec"}}}}
	if op, err := checkSourceWriteHeld(writing, source, gid, writeInc); err != nil || op.TID != 7 {
		t.Fatal("held source write refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot){
		"incomplete":       func(o *opsInFlightSnapshot) { o.Complete = false },
		"brackets differ":  func(o *opsInFlightSnapshot) { o.After.ID = 2 },
		"another OSD":      func(o *opsInFlightSnapshot) { o.OSD = 2 },
		"another PG":       func(o *opsInFlightSnapshot) { o.PGID = "3.6" },
		"another client":   func(o *opsInFlightSnapshot) { o.Client = "client.4641.33" },
		"no write":         func(o *opsInFlightSnapshot) { o.Ops = nil },
		"two writes":       func(o *opsInFlightSnapshot) { o.Ops = append(o.Ops, o.Ops[0]) },
		"not yet issued":   func(o *opsInFlightSnapshot) { o.Ops[0].FlagPoint = "started" },
		"answered":         func(o *opsInFlightSnapshot) { o.Ops[0].Events = append(o.Ops[0].Events, "commit_sent") },
		"write of another": func(o *opsInFlightSnapshot) { o.Ops[0].PGID = "4.3" },
		"absence claimed":  func(o *opsInFlightSnapshot) { o.AbsenceEstablished = true },
	} {
		o := writing
		o.Ops = slices.Clone(writing.Ops)
		o.Ops[0].Events = slices.Clone(writing.Ops[0].Events)
		edit(&o)
		if _, err := checkSourceWriteHeld(o, source, gid, writeInc); err == nil {
			t.Error("held source write accepted:", name)
		}
	}

	// The data-only source's second copy-get continues the data with an
	// empty OMAP position, as the first starts with one: both print in=29b.
	first := []string{"copy-get max 32768 in=29b"}
	later := []string{"assert-version v5", "copy-get max 32768 in=29b"}
	if !copyFirstChunk(first, multiChunkLimit, 0) || copyFirstChunk(later, multiChunkLimit, 0) || !copyLaterChunk(later, multiChunkLimit, 5, 0) || copyLaterChunk(later, multiChunkLimit, 4, 0) || copyLaterChunk(first, multiChunkLimit, 5, 0) ||
		copyFirstChunk([]string{"copy-get max 8388608 in=29b"}, multiChunkLimit, 0) || copyLaterChunk([]string{"assert-version v5", "copy-get max 65536 in=29b"}, multiChunkLimit, 5, 0) {
		t.Fatal("copy-get shapes misjudged")
	}
	// The cursor is judged by its exact printed length, never by a prefix.
	for name, ops := range map[string][]string{
		"an OMAP position":    {"copy-get max 32768 in=40b"},
		"a shorter cursor":    {"copy-get max 32768 in=28b"},
		"no cursor":           {"copy-get max 32768"},
		"an output":           {"copy-get max 32768 in=29b out=12b"},
		"a suffix":            {"copy-get max 32768 in=29bx"},
		"a prefix match only": {"copy-get max 32768 in=290b"},
		"two copy-gets":       {"copy-get max 32768 in=29b", "copy-get max 32768 in=29b"},
		"another op after":    {"copy-get max 32768 in=29b", "stat"},
		"a longer max":        {"copy-get max 327680 in=29b"},
		"a doubled space":     {"copy-get max  32768 in=29b"},
	} {
		if copyFirstChunk(ops, multiChunkLimit, 0) || copyLaterChunk(append([]string{"assert-version v5"}, ops...), multiChunkLimit, 5, 0) {
			t.Error("copy-get with the wrong cursor accepted:", name)
		}
	}
	if !copyFirstChunk([]string{"copy-get max 32768 in=40b"}, multiChunkLimit, 11) || copyFirstChunk([]string{"copy-get max 32768 in=40b"}, multiChunkLimit, 12) {
		t.Fatal("an 11-byte OMAP position misjudged")
	}
	sent := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3",
		Ops: []objecterRequestsOp{{TID: 9, PG: "3.5", OSD: 0, Attempts: 1, OSDOps: first, CopyGetMax: []uint64{32768}}}}
	if op, err := checkFirstChunkSent(sent, route, 0, false); err != nil || op.TID != 9 {
		t.Fatal("sent first chunk refused:", err)
	}
	pausedSent := sent
	pausedSent.Ops = slices.Clone(sent.Ops)
	pausedSent.Ops[0].Paused = true
	if _, err := checkFirstChunkSent(pausedSent, route, 0, true); err != nil {
		t.Fatal("first chunk paused after its send refused:", err)
	}
	if _, err := checkFirstChunkSent(pausedSent, route, 0, false); err == nil {
		t.Error("a paused first chunk accepted as running")
	}
	if _, err := checkFirstChunkSent(sent, route, 0, true); err == nil {
		t.Error("a running first chunk accepted as paused")
	}
	for name, edit := range map[string]func(*objecterRequestsSnapshot){
		"never sent":      func(r *objecterRequestsSnapshot) { r.Ops[0].Attempts = 0 },
		"sent twice":      func(r *objecterRequestsSnapshot) { r.Ops[0].Attempts = 2 },
		"later chunk":     func(r *objecterRequestsSnapshot) { r.Ops[0].OSDOps = later },
		"default chunk":   func(r *objecterRequestsSnapshot) { r.Ops[0].OSDOps = []string{"copy-get max 8388608 in=29b"} },
		"not initial":     func(r *objecterRequestsSnapshot) { r.Ops[0].OSDOps = []string{"copy-get max 32768 in=40b"} },
		"another OSD":     func(r *objecterRequestsSnapshot) { r.Ops[0].OSD = 2 },
		"no op":           func(r *objecterRequestsSnapshot) { r.Ops = nil },
		"two ops":         func(r *objecterRequestsSnapshot) { r.Ops = append(r.Ops, r.Ops[0]) },
		"incomplete":      func(r *objecterRequestsSnapshot) { r.Complete = false },
		"brackets differ": func(r *objecterRequestsSnapshot) { r.After.ID = 0 },
		"asked another":   func(r *objecterRequestsSnapshot) { r.OSD = 0 },
	} {
		r := sent
		r.Ops = slices.Clone(sent.Ops)
		edit(&r)
		if _, err := checkFirstChunkSent(r, route, 0, false); err == nil {
			t.Error("first chunk accepted:", name)
		}
	}

	client := opsInFlightSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.3", Epoch: 30, FlagPoint: "started", Events: []string{"queued_for_pg", "reached_pg", "started"}}}}
	held := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3",
		Ops: []objecterRequestsOp{{TID: 10, PG: "3.5", OSD: 0, Paused: true, OSDOps: later, CopyGetMax: []uint64{32768}}}}
	if op, read, err := checkChunkHeld(client, held, route, gid, inc, multiChunkLimit, 5, 0, []uint64{9}); err != nil || op.TID != 5 || read.TID != 10 {
		t.Fatal("held second chunk refused:", err)
	}
	restarted := held
	restarted.Ops = []objecterRequestsOp{{TID: 11, PG: "3.5", OSD: 2, Paused: true, OSDOps: first, CopyGetMax: []uint64{32768}}}
	if _, _, err := checkChunkHeld(client, restarted, route, gid, inc, multiChunkLimit, 0, 0, []uint64{9, 10}); err != nil {
		t.Fatal("restarted first chunk refused:", err)
	}
	if _, _, err := checkChunkHeld(client, restarted, route, gid, inc, multiChunkLimit, 5, 0, []uint64{9, 10}); err == nil {
		t.Error("a first chunk accepted as the held later chunk")
	}
	if _, _, err := checkChunkHeld(client, held, route, gid, inc, multiChunkLimit, 0, 0, []uint64{9}); err == nil {
		t.Error("a later chunk accepted as a restarted first chunk")
	}
	// A fresh TID does not stand in for the cursor: a held copy-get whose
	// cursor is not the one the claim needs is refused either way.
	if _, _, err := checkChunkHeld(client, held, route, gid, inc, multiChunkLimit, 5, 11, []uint64{9}); err == nil {
		t.Error("a held copy-get without the expected OMAP position accepted")
	}
	omap := held
	omap.Ops = []objecterRequestsOp{{TID: 10, PG: "3.5", OSD: 0, Paused: true, OSDOps: []string{"assert-version v5", "copy-get max 32768 in=40b"}, CopyGetMax: []uint64{32768}}}
	if _, _, err := checkChunkHeld(client, omap, route, gid, inc, multiChunkLimit, 5, 0, []uint64{9}); err == nil {
		t.Error("a held copy-get with an OMAP position accepted as the data-only second chunk")
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot){
		"sent":          func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops[0].Attempts = 1 },
		"not paused":    func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops[0].Paused = false },
		"an earlier op": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops[0].TID = 9 },
		"another version": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops[0].OSDOps = []string{"assert-version v6", later[1]}
		},
		"no source op":    func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops = nil },
		"two source ops":  func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops = append(r.Ops, r.Ops[0]) },
		"absence claimed": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.AbsenceEstablished = true },
		"no client op":    func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops = nil },
		"another epoch":   func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Epoch = 31 },
		"brackets differ": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.After.Epoch = 32 },
		"transaction issued": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(o.Ops[0].Events, "waiting for subops from 0,2")
		},
		"local commit": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(o.Ops[0].Events, "op_commit")
		},
		"answered": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(o.Ops[0].Events, "commit_sent")
		},
		"waiting subops": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].FlagPoint = "waiting for sub ops" },
		"reply sent": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].FlagPoint = "commit sent; apply or cleanup"
		},
	} {
		o, r := client, held
		o.Ops, r.Ops = slices.Clone(client.Ops), slices.Clone(held.Ops)
		o.Ops[0].Events = slices.Clone(client.Ops[0].Events)
		edit(&o, &r)
		if _, _, err := checkChunkHeld(o, r, route, gid, inc, multiChunkLimit, 5, 0, []uint64{9}); err == nil {
			t.Error("held chunk accepted:", name)
		}
	}

	lost := route
	lost.Epoch, lost.Up, lost.Acting = 33, []int32{1, 2}, []int32{1, 2}
	if err := checkReplicaLost(route, lost, 0); err != nil {
		t.Fatal("lost replica refused:", err)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"same epoch":      func(p *osdFixtureOracle) { p.Epoch = 30 },
		"primary moved":   func(p *osdFixtureOracle) { p.ID, p.PrimaryID = 2, 2 },
		"still up":        func(p *osdFixtureOracle) { p.Up = []int32{1, 0, 2} },
		"still acting":    func(p *osdFixtureOracle) { p.Acting = []int32{1, 2, 0} },
		"another PG":      func(p *osdFixtureOracle) { p.PGID = "4.4" },
		"restarted":       func(p *osdFixtureOracle) { p.Address = "v2:192.0.2.11:6800/2" },
		"not the primary": func(p *osdFixtureOracle) { p.PrimaryID = 2 },
	} {
		p := lost
		p.Up, p.Acting = slices.Clone(lost.Up), slices.Clone(lost.Acting)
		edit(&p)
		if checkReplicaLost(route, p, 0) == nil {
			t.Error("lost replica accepted:", name)
		}
	}
}
