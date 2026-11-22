package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Multi-chunk COPY_FROM and COPY_FROM2 sources held at any chunk: a
// three-chunk OMAP source whose destination primary or third OSD is lost
// before its last chunk, and head, versioned and snapshot sources that
// change between two chunks. The barrier of
// osd_copyfrom_multichunk_loss_test.go is generalized: instead of a held
// source write, the source primary itself is frozen while each copy-get is
// in flight, so the barrier can step one chunk at a time and holds a
// snapshot source too. The pinned source (Ceph v20.2.4) gives it its
// meaning:
//
//   - src/osd/PrimaryLogPG.cc:9295-9486 (do_copy_get) fills one chunk of at
//     most the destination primary's chunk limit: the attributes in the
//     first chunk, then data up to the limit, then, only once the data is
//     complete, the OMAP header and entries one by one, each counted as its
//     key and value with their 4-byte lengths, until the budget is spent,
//     the entry that crosses it included. Request IDs are added only to the
//     chunk that completes the cursor. A 32 KiB data source with 48 OMAP
//     entries of 1019 counted bytes each is therefore three chunks under a
//     32 KiB limit: the data, 33 entries with the header, 15 entries.
//   - src/osd/PrimaryLogPG.cc:9776-9793 (process_copy_chunk) writes every
//     chunk that leaves the cursor incomplete into one temporary object and
//     calls _copy_some for the next; _copy_some (9535-9600) prefixes
//     assert-version to every copy-get once the version is known: from the
//     first chunk for a versioned copy, from the second otherwise.
//   - src/osdc/Objecter.cc:3074-3082 marks an op already sent paused when a
//     map with pauserd arrives and still accepts its reply (3553-3620);
//     2550-2556 keeps a new read paused and unsent; a map without pauserd
//     makes _calc_target resend a paused op (attempts counts every send).
//     So, with the source primary frozen, a copy-get is observed sent once,
//     pauserd is set and the op observed paused, and only then the source
//     primary is resumed: it serves that chunk, and the next copy-get is
//     submitted under pauserd and stays unsent. Clearing pauserd with the
//     source primary frozen again sends exactly that op, and the step
//     repeats. Each freeze is a fixture-owned lease well inside the
//     heartbeat grace and the read lease.
//   - src/osd/PrimaryLogPG.cc:6529-6542 (ASSERT_VER) fails a later chunk
//     with -ERANGE once a write raised the source's user version.
//     CopyFromCallback::finish (344-368) answers an error other than
//     -ECANCELED with reply_op_error (src/osd/OSD.cc:1635-1649), which logs
//     nothing, and process_copy_chunk (9852-9880) removes the temporary
//     object afterwards.
//   - A copy at a snap id reads the clone that find_object_context resolves
//     (12092-12264). A head write under a SnapContext whose seq is the
//     source's snapset seq makes no clone (make_writeable, 8640-8760), so the
//     clone and its user version stay. The CLONE log entry has no request ID
//     (8776-8781) and get_object_reqids (src/osd/PGLog.h:476-506) takes only
//     MODIFY, DELETE and ERROR entries with one, so a copy of a clone carries
//     no source request ID.

// copyThreeChunkEntries is the OMAP of the three-chunk source: 48 entries
// of an 11-byte key and a 1000-byte value.
const copyThreeChunkEntries = 48

// copyChunkStep is one copy-get the barrier let through: the source primary
// was frozen while it was sent and paused, then resumed.
type copyChunkStep struct {
	TID           uint64   `json:"tid"`
	Ops           []string `json:"osd_ops"`
	FrozenFor     string   `json:"source_frozen_for"`
	NextTID       uint64   `json:"next_copy_get_tid"`
	NextOps       []string `json:"next_osd_ops"`
	Temporary     string   `json:"temporary_object"`
	StagingSHA256 []string `json:"staging_sha256"`
}

// copyChunkShape says whether ops is a copy-get of the lowered chunk that
// asserts version, or one without assert-version when version is zero.
func copyChunkShape(ops []string, version uint64) bool {
	if version == 0 {
		return copyFirstChunk(ops)
	}
	return copyLaterChunk(ops, version)
}

// checkChunkSent judges one snapshot of the destination primary's Objecter
// while the source primary is frozen: exactly one op of the source, a
// copy-get of the chunk shape version gives, sent once to the source
// primary and paused or not as want says. When tid is not zero it is that
// op, and it is none of served.
func checkChunkSent(objecter objecterRequestsSnapshot, route osdFixtureOracle, sourcePrimary int32, paused bool, version, tid uint64, served []uint64) (objecterRequestsOp, error) {
	if !objecter.Complete || len(objecter.Failures) != 0 || objecter.Code != 0 || objecter.AbsenceEstablished {
		return objecterRequestsOp{}, fmt.Errorf("incomplete objecter snapshot: %q", objecter.Failures)
	}
	if !samePlacement(objecter.Before, objecter.After) || !samePlacement(objecter.Before, route) || objecter.OSD != route.ID {
		return objecterRequestsOp{}, fmt.Errorf("the objecter snapshot of osd.%d is not bracketed by the destination route of osd.%d", objecter.OSD, route.ID)
	}
	if len(objecter.Ops) != 1 {
		return objecterRequestsOp{}, fmt.Errorf("the objecter holds %d ops of the source; want one copy-get", len(objecter.Ops))
	}
	read := objecter.Ops[0]
	if !copyChunkShape(read.OSDOps, version) || read.Attempts != 1 || read.Paused != paused || read.OSD != sourcePrimary {
		return read, fmt.Errorf("the source op is not a copy-get (assert v%d) sent once to osd.%d (paused %t): %+v", version, sourcePrimary, paused, read)
	}
	if (tid != 0 && read.TID != tid) || slices.Contains(served, read.TID) {
		return read, fmt.Errorf("the sent copy-get is tid %d; want %d, none of %v", read.TID, tid, served)
	}
	return read, nil
}

// copyEntryOnly checks one destination snapshot for a copy whose source
// carries no request ID: the copy's identity is exactly one ordinary
// successful modify entry of object at version, and none of the source
// identities occurs.
func copyEntryOnly(copyKey []pgHistoryLocation, object, version string, sources ...[]pgHistoryLocation) error {
	if len(copyKey) != 1 {
		return fmt.Errorf("the copy's identity occurs %d times; want one ordinary entry: %+v", len(copyKey), copyKey)
	}
	entry := copyKey[0]
	if entry.Tier != "log" || entry.ExtraIndex != nil || entry.Op != "modify" || entry.ReturnCode == nil || *entry.ReturnCode != 0 || entry.Object != object || entry.Version != version {
		return fmt.Errorf("the copy's identity is not one successful modify of %s at %s: %+v", object, version, entry)
	}
	for i, source := range sources {
		if len(source) != 0 {
			return fmt.Errorf("source identity %d occurs at the destination: %+v", i, source)
		}
	}
	return nil
}

// poolOmapState reads one owned head's bytes, xattrs, OMAP and header
// natively (tools/osd_pool_mode_omapstate_oracle.py).
func poolOmapState(t *testing.T, ctx context.Context, slot int, object string) omapWriteState {
	t.Helper()
	var result struct {
		State omapWriteState `json:"omap_state"`
	}
	nativePoolResult(t, ctx, "omapstate", map[string]any{"slot": slot, "object": object}, &result)
	return result.State
}

// omapPage is the complete OMAP of one native omapstate observation: its
// one page, which must not report more entries, plus the empty key when the
// explicit by-keys read found it (the page cannot hold it).
func omapPage(t *testing.T, state omapWriteState) map[string][]byte {
	t.Helper()
	if state.Code != 0 || state.Omap == nil || len(state.Omap.Operations) != 2 || state.Header == nil {
		t.Fatal("native OMAP state lacks an existing object inventory/header")
	}
	page, keyed := state.Omap.Operations[0], state.Omap.Operations[1]
	if page.More == nil || *page.More || page.Result != 0 || keyed.Result != 0 {
		t.Fatal("native OMAP page is incomplete")
	}
	result := map[string][]byte{}
	for _, entry := range page.Entries {
		result[string(entry.Key)] = entry.Value
	}
	for _, entry := range keyed.Entries {
		if len(entry.Key) != 0 {
			t.Fatalf("the by-keys read of the empty key returned %q", entry.Key)
		}
		result[""] = entry.Value
	}
	return result
}

// sameOmap says whether two native states hold the same OMAP and header.
func sameOmap(t *testing.T, a, b omapWriteState) bool {
	t.Helper()
	return a.Header != nil && b.Header != nil && bytes.Equal(*a.Header, *b.Header) && equalOmapModel(omapPage(t, a), omapPage(t, b))
}

// lowerCopyChunk lowers the destination primary's osd_copyfrom_max_chunk
// and registers its restoration, which runs after the cluster is restored
// (cleanups run in reverse), so it reads a running destination primary.
func (r *copyPrimaryChangeRun) lowerCopyChunk(restored *bool) {
	t, ctx := r.t, r.ctx
	t.Helper()
	primary := r.dstRoute.ID
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		setting := copyChunk(t, cleanup, primary, "restore")
		*restored = setting.Restored && !setting.Claimed
		if !*restored {
			t.Errorf("osd_copyfrom_max_chunk of osd.%d was not restored: %+v", primary, setting)
		}
	})
	t.Cleanup(func() { r.recovery.restore(t) })
	setting := copyChunk(t, ctx, primary, "lower")
	if !setting.Lowered || setting.Value != multiChunkLimit || setting.Prior <= 3*multiChunkLimit || len(setting.Failures) != 0 {
		t.Fatalf("osd_copyfrom_max_chunk of osd.%d was not lowered to %d: %+v", primary, multiChunkLimit, setting)
	}
	t.Logf("osd_copyfrom_max_chunk of osd.%d lowered from %d to %d", primary, setting.Prior, setting.Value)
}

// seedDestination seeds the destination natively and requires no temporary
// object in its PG on any OSD.
func (r *copyPrimaryChangeRun) seedDestination() nativeXattrSnapshot {
	t, ctx := r.t, r.ctx
	t.Helper()
	poolSeed(t, ctx, 2, r.dest, "", []byte("seeded destination bytes"), []nativeXattrEntry{{Name: []byte("seed-attr"), Value: []byte{7}}})
	seeded := poolObserve(t, ctx, 2, r.dest, "", "")
	if seeded.Code != 0 || seeded.Version == 0 {
		t.Fatal("native destination seed", seeded.Code)
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute.PGID), osd, r.pools.ids[2], r.dstRoute.PGID, 0); err != nil {
			t.Fatal("before the copy:", err)
		}
	}
	return seeded
}

// placeForCopy rechecks both placements just before the copy.
func (r *copyPrimaryChangeRun) placeForCopy() (osdFixtureOracle, osdFixtureOracle) {
	t, ctx := r.t, r.ctx
	t.Helper()
	route, source := poolPlace(t, ctx, 2, r.dest, ""), poolPlace(t, ctx, 1, r.source, "")
	if !samePlacement(route, r.dstRoute) || !samePlacement(source, r.srcRoute) {
		t.Fatalf("placements moved before the copy: destination %+v, source %+v", route, source)
	}
	return route, source
}

// waitChunkSent polls the destination primary's Objecter under work until
// checkChunkSent accepts it.
func (r *copyPrimaryChangeRun) waitChunkSent(work context.Context, h *copyChunkHold, paused bool, version, tid uint64) objecterRequestsOp {
	t := r.t
	t.Helper()
	for {
		h.pending(t, "while a copy-get was in flight")
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		read, err := checkChunkSent(objecter, h.route, h.source.ID, paused, version, tid, h.served)
		if err == nil && work.Err() == nil {
			return read
		}
		if work.Err() != nil {
			t.Fatalf("no copy-get in flight (paused %t) before the freeze budget expired: %v (%v)\nobjecter: %s", paused, err, work.Err(), boundedRaw(objecter.Raw))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// holdAtChunk sends request on a fresh connection to route and holds the
// copy before its chunk held (2 or more). Each earlier chunk is let through
// with the source primary frozen: the first copy-get asserts first (zero for
// an unversioned copy), every later one asserts later. After each chunk the
// next copy-get must be held paused and never sent, with one temporary
// object of the same name listed on every OSD of the destination PG.
func (r *copyPrimaryChangeRun) holdAtChunk(request cephmsgr.OSDRequest, route, source osdFixtureOracle, held int, first, later uint64, steps *[]copyChunkStep) *copyChunkHold {
	t, ctx := r.t, r.ctx
	t.Helper()
	h := &copyChunkHold{done: make(chan copyChunkOutcome, 1), route: route, source: source}
	h.o = mutationConnection(t, r.client, ctx, route)
	t.Cleanup(func() { h.o.Close() })
	call, cancel := context.WithTimeout(ctx, 600*time.Second)
	t.Cleanup(cancel)
	for chunk := 1; chunk < held; chunk++ {
		version, tid := later, uint64(0)
		if chunk == 1 {
			version = first
		} else {
			tid = h.read.TID
		}
		frozen := time.Now()
		// The source primary is resumed well inside its 20 s heartbeat grace
		// and the 16 s read lease, as in the destination-loss test; the
		// fixture resumes it at the lease's deadline all the same.
		thawDeadline := frozen.Add(16 * time.Second)
		work, stopWork := context.WithDeadline(ctx, thawDeadline.Add(-3*time.Second))
		r.recovery.freeze(t, work, source.ID, thawDeadline)
		if chunk == 1 {
			go func() {
				reply, err := h.o.Request(call, request)
				h.done <- copyChunkOutcome{reply, err}
			}()
		} else {
			r.recovery.clearPauseReads(t, work)
		}
		sent := r.waitChunkSent(work, h, false, version, tid)
		r.recovery.setPauseReads(t, work)
		r.waitChunkSent(work, h, true, version, sent.TID)
		if err := r.recovery.thaw(source.ID); err != nil {
			t.Fatalf("resume osd.%d within the freeze budget: %v", source.ID, err)
		}
		stopWork()
		step := copyChunkStep{TID: sent.TID, Ops: sent.OSDOps, FrozenFor: time.Since(frozen).Round(10 * time.Millisecond).String()}
		if chunk == 1 {
			h.first = sent
		}
		h.served = append(h.served, sent.TID)
		t.Logf("chunk %d: copy-get tid %d %q sent to frozen osd.%d and paused after its send; osd.%d frozen for %s", chunk, sent.TID, sent.OSDOps, source.ID, source.ID, step.FrozenFor)

		previous := h.op.TID
		for deadline := time.Now().Add(60 * time.Second); ; {
			h.pending(t, fmt.Sprintf("after chunk %d", chunk))
			ops := copyOpsInFlight(t, ctx, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
			objecter := copyObjecterRequests(t, ctx, 2, r.dest, nil, r.pools.ids[1], r.source)
			op, read, err := checkChunkHeld(ops, objecter, route, r.gid, copyPrimaryCopyIncarnation, later, h.served)
			if err == nil && previous != 0 && op.TID != previous {
				err = fmt.Errorf("the client op is %s, not the earlier chunk's tid %d", op.Reqid, previous)
			}
			if err == nil {
				h.op, h.read = op, read
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("no barrier after chunk %d: %v\nop tracker: %s\nobjecter: %s", chunk, err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
			}
			time.Sleep(250 * time.Millisecond)
		}
		step.NextTID, step.NextOps = h.read.TID, h.read.OSDOps
		// The chunk's write is replicated asynchronously; the hold keeps the
		// copy from going further, so the listings converge on one object,
		// the same one as after the earlier chunks.
		for _, osd := range route.Acting {
			for deadline := time.Now().Add(30 * time.Second); ; {
				s := copyStaging(t, ctx, osd, route.PGID)
				temp, err := checkCopyTemp(s, osd, route.Pool, route.PGID, 1)
				if err == nil && (h.temp == "" || temp == h.temp) {
					h.temp = temp
					step.StagingSHA256 = append(step.StagingSHA256, s.SHA256)
					break
				}
				if err == nil {
					err = fmt.Errorf("osd.%d lists %q, not %q", osd, temp, h.temp)
				}
				if time.Now().After(deadline) {
					t.Fatalf("the chunk %d staging is not listed on every OSD: %v", chunk, err)
				}
				time.Sleep(250 * time.Millisecond)
			}
		}
		step.Temporary = h.temp
		*steps = append(*steps, step)
		t.Logf("barrier after chunk %d on osd.%d: op %s flag point %q; objecter tid %d paused %t attempts %d ops %q; temporary %s on osd %v",
			chunk, route.ID, h.op.Reqid, h.op.FlagPoint, h.read.TID, h.read.Paused, h.read.Attempts, h.read.OSDOps, h.temp, route.Acting)
	}
	h.pending(t, "at the barrier")
	return h
}

// copyThreeChunkReceipt is the archived public receipt of one three-chunk
// loss cell.
type copyThreeChunkReceipt struct {
	copyChunkLossReceipt
	Frozen      int32           `json:"frozen_source_primary"`
	Third       int32           `json:"third_osd"`
	Steps       []copyChunkStep `json:"chunks"`
	OmapEntries int             `json:"source_omap_entries"`
	HeaderBytes int             `json:"source_omap_header_bytes"`
	SourceOmap  string          `json:"source_omap_sha256"`
	FinalOmap   string          `json:"destination_omap_sha256"`
}

// omapDigest is a short fingerprint of a native OMAP state for a receipt.
func omapDigest(t *testing.T, s omapWriteState) string {
	t.Helper()
	raw, err := json.Marshal(omapWriteEntries(omapPage(t, s)))
	if err != nil || s.Header == nil {
		t.Fatal("OMAP digest", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(append(append(raw, 0), *s.Header...)))
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of a source of 32 KiB
// data, one xattr, an OMAP header and 48 OMAP entries onto a natively seeded
// destination runs in three chunks under the destination primary's lowered
// 32 KiB limit, and the barrier described above holds it before the third:
// the temporary object holds the data, the header and the first entries,
// and the third copy-get, asserting the source version, is paused and never
// sent.
//
// Destination primary lost: the caller's wait ends as an unknown outcome
// carrying the barrier's transaction, the seed is kept, the new primary's PG
// log names neither the copy nor the source write and both survivors drop
// the temporary object. A second Go write replaces the source data (the OMAP
// stays), and one explicit caller resubmission to the new primary executes
// the copy once. After recovery the original primary leads again with that
// single copy, the restarted OSD lists no temporary object, and the
// destination holds the source's complete OMAP and header natively.
//
// Third OSD lost: it replicates both PGs and is neither primary. The
// destination primary cancels the copy at the interval change, requeues the
// client op and restarts the copy from a first copy-get, held unsent under
// pauserd, with the temporary object gone from both survivors. After pauserd
// is cleared the copy runs all three chunks and the same client op is
// answered once on its original connection with the source version; the
// destination is the complete copy, OMAP and header included, logged once
// and bound to the source write.
//
// Not verified: EC destinations (no OMAP, and a k=2,m=1 pool on three OSDs
// is inactive with one OSD lost), the legacy one-PG pools when their
// primaries coincide, a loss before the primary's local commit of the final
// copy transaction, and the OMAP of a replica, which is read through the
// primary only.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromthreechunkloss key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromthreechunkloss key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDCopyFromThreeChunkLossIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1800*time.Second)
	var mu sync.Mutex
	var receipts []copyThreeChunkReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-three-chunk-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("three-chunk loss receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, lost := range []string{"destination", "third"} {
			cell := copyChunkLossCell{tc, lost}
			t.Run(cell.name+"/"+lost, func(t *testing.T) {
				receipt := copyThreeChunkReceipt{copyChunkLossReceipt: copyChunkLossReceipt{Cell: cell.name + "/" + lost, Chunk: multiChunkLimit, Paused: -1}}
				defer func() {
					if t.Skipped() {
						return
					}
					mu.Lock()
					receipts = append(receipts, receipt)
					mu.Unlock()
				}()
				copyThreeChunkLoss(t, ctx, pools, recovery, cell, &receipt)
			})
		}
	}
}

// writeOmapSource writes the three-chunk source with one Go request: data,
// the xattr observeWritten expects, the OMAP header and the entries.
func (r *copyPrimaryChangeRun) writeOmapSource(data, header []byte, entries []cephmsgr.OSDOmapEntry) (cephmsgr.OSDReply, omapWriteState) {
	t, ctx := r.t, r.ctx
	t.Helper()
	r.srcRoute = poolPlace(t, ctx, 1, r.source, "")
	request := r.pools.request(r.srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "copy-primary-attr"}, Data: []byte{9, 0xfe}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: entries}})
	request.ClientIncarnation, request.ModificationTime = copyPrimaryWriteIncarnation, cephmsgr.OSDTime{Seconds: 1700009050}
	reply := r.send("three-chunk source write", r.srcRoute, request)
	native := poolOmapState(t, ctx, 1, r.source)
	model := map[string][]byte{}
	for _, entry := range entries {
		model[entry.Key] = entry.Value
	}
	if native.Code != 0 || !bytes.Equal(native.Data, data) || attrNames(native.nativeXattrSnapshot)["copy-primary-attr"] != string([]byte{9, 0xfe}) || native.Version != reply.UserVersion ||
		native.Header == nil || !bytes.Equal(*native.Header, header) || !equalOmapModel(omapPage(t, native), model) {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d) %d bytes", native.Code, native.Version, reply.UserVersion, len(native.Data))
	}
	return reply, native
}

func copyThreeChunkLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copyChunkLossCell, receipt *copyThreeChunkReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("three-%d-%s", cell.code, cell.lost))
	sourcePrimary, third := r.separatePrimaries()
	receipt.SourcePrimary, receipt.Primary, receipt.Frozen, receipt.Third = sourcePrimary, r.dstRoute.ID, sourcePrimary, third
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, multiChunkLimit)
	for i := range data {
		data[i] = byte(i*29 + i>>8 + 3)
	}
	header := []byte("three-chunk header \x00\xff")
	entries := make([]cephmsgr.OSDOmapEntry, copyThreeChunkEntries)
	for i := range entries {
		value := make([]byte, 1000)
		for j := range value {
			value[j] = byte(i*7 + j)
		}
		entries[i] = cephmsgr.OSDOmapEntry{Key: fmt.Sprintf("omap-key-%02d", i), Value: value}
	}
	write, source := r.writeOmapSource(data, header, entries)
	receipt.SourceBytes, receipt.SourceUser, receipt.OmapEntries, receipt.HeaderBytes = len(data), write.UserVersion, len(entries), len(header)
	receipt.SourceOmap = omapDigest(t, source)

	route, src := r.placeForCopy()
	h := r.holdAtChunk(r.copyRequest(cell.code, route), route, src, 3, 0, write.UserVersion, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}
	if cell.lost == "destination" {
		r.loseDestinationBetweenChunks(h, cell.code, seeded, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	} else {
		r.loseReplicaBetweenChunks(h, third, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	}
	// The final copy holds the source's complete OMAP and header, whichever
	// execution made it; the second source write replaced only the data.
	final, now := poolOmapState(t, ctx, 2, r.dest), poolOmapState(t, ctx, 1, r.source)
	if !sameOmap(t, final, now) || !sameOmap(t, now, source) || final.Version != poolObserve(t, ctx, 2, r.dest, "", "").Version {
		t.Fatalf("the destination OMAP is not the source's: header %v / %v", final.Header != nil, now.Header != nil)
	}
	receipt.FinalOmap = omapDigest(t, final)
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute.PGID), osd, pools.ids[2], r.dstRoute.PGID, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
	t.Logf("destination OMAP %s equals the source's (%d entries, %d-byte header)", receipt.FinalOmap, len(entries), len(header))
}

// copySourceChangeCell is one opcode, a head or snapshot source, and an
// unversioned or versioned copy.
type copySourceChangeCell struct {
	copyPrimaryChangeCase
	snapshot, versioned bool
}

func (c copySourceChangeCell) label() string {
	source, version := "head", "unversioned"
	if c.snapshot {
		source = "snapshot"
	}
	if c.versioned {
		version = "versioned"
	}
	return c.name + "/" + source + "-" + version
}

// copySourceChangeReceipt is the archived public receipt of one
// source-change cell.
type copySourceChangeReceipt struct {
	Cell          string          `json:"cell"`
	SourcePrimary int32           `json:"source_primary"`
	Primary       int32           `json:"destination_primary"`
	Chunk         uint64          `json:"chunk_limit"`
	SourceBytes   int             `json:"source_bytes"`
	SnapID        uint64          `json:"source_snap_id,omitempty"`
	CopiedUser    uint64          `json:"copied_user_version"`
	SourceVersion uint64          `json:"request_source_version"`
	Steps         []copyChunkStep `json:"chunks"`
	BarrierReqid  string          `json:"barrier_reqid"`
	HeldChunk     []string        `json:"held_copy_get"`
	Temporary     string          `json:"temporary_object"`
	ChangedUser   uint64          `json:"changed_head_user_version"`
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
	TempRemoved  []int32 `json:"temporary_removed_on"`
	Final        string  `json:"native_final"`
	FreshTID     uint64  `json:"fresh_read_tid"`
	ChunkRestore bool    `json:"chunk_restored"`
}

// judgeSourceChange decides what the held copy's answer must be once the
// source head changed between its chunks: a head source fails the later
// chunk's version assertion, -ERANGE with the copy's per-op result still
// -EINPROGRESS, and the destination keeps its seed; a snapshot source reads
// the unchanged clone, so the copy succeeds with the clone's user version
// and the destination becomes the clone.
func judgeSourceChange(snapshot bool, reply cephmsgr.OSDReply, tid, cloneUser uint64) error {
	if reply.Transaction != tid || reply.RetryAttempt != 0 || len(reply.Operations) != 1 {
		return fmt.Errorf("the answer is tid %d attempt %d with %d ops; want tid %d, attempt 0, one op", reply.Transaction, reply.RetryAttempt, len(reply.Operations), tid)
	}
	if !snapshot {
		if reply.Code != -34 || reply.Operations[0].Result != -115 {
			return fmt.Errorf("the head copy answered %d (per-op %d); want -ERANGE with per-op -EINPROGRESS", reply.Code, reply.Operations[0].Result)
		}
		return nil
	}
	if reply.Code != 0 || reply.Operations[0].Result != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion != cloneUser {
		return fmt.Errorf("the snapshot copy answered %d (per-op %d, flags %#x, user %d); want a committed 0 with the clone's user version %d",
			reply.Code, reply.Operations[0].Result, reply.Flags, reply.UserVersion, cloneUser)
	}
	return nil
}

// For COPY_FROM and COPY_FROM2, a 64 KiB source is copied onto a natively
// seeded destination in two chunks under the destination primary's lowered
// 32 KiB limit, and held before the second by the barrier described above.
// A Go write then replaces the source head, and pauserd is cleared.
//
// Head source, unversioned or pinned to the head's version: the second
// copy-get asserts the version the copy started from, so the copy is
// answered once, on its connection, with -ERANGE and its per-op result
// -EINPROGRESS. The destination keeps its seed natively, every OSD of its PG
// drops the temporary object, its primary's PG log does not name the copy,
// and a fresh session reads the seed.
//
// Snapshot source (a clone of a self-managed snapshot under a rewritten
// head), unversioned or pinned to the clone's version: every chunk reads the
// clone, which the later head write leaves alone, so the copy is answered
// once with 0 and the clone's user version. The destination is the clone
// natively (bytes, xattrs, mtime, user version), the temporary object is
// gone, the PG log holds the copy once without any source request ID, and a
// fresh session reads the clone's bytes.
//
// Not verified: OMAP in a changing source, a source removed or a snapshot
// removed between chunks, EC sources and destinations, and the legacy one-PG
// pools when their primaries coincide.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunksourcechange key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunksourcechange key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates self-managed snapshots in its owned pool
func TestCephOSDCopyFromChunkSourceChangeIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var mu sync.Mutex
	var receipts []copySourceChangeReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-chunk-source-change-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("source-change receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, snapshot := range []bool{false, true} {
			for _, versioned := range []bool{false, true} {
				cell := copySourceChangeCell{tc, snapshot, versioned}
				t.Run(cell.label(), func(t *testing.T) {
					receipt := copySourceChangeReceipt{Cell: cell.label(), Chunk: multiChunkLimit}
					defer func() {
						if t.Skipped() {
							return
						}
						mu.Lock()
						receipts = append(receipts, receipt)
						mu.Unlock()
					}()
					copyChunkSourceChange(t, ctx, pools, recovery, cell, &receipt)
				})
			}
		}
	}
}

// writeSourceHead writes the source head with one Go WRITEFULL and SETXATTR
// under snapc and returns the reply.
func (r *copyPrimaryChangeRun) writeSourceHead(label string, seconds uint32, data, attr []byte, snapc cephmsgr.OSDSnapContext) cephmsgr.OSDReply {
	r.t.Helper()
	r.srcRoute = poolPlace(r.t, r.ctx, 1, r.source, "")
	request := r.pools.request(r.srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "copy-primary-attr"}, Data: attr})
	request.ClientIncarnation, request.ModificationTime, request.SnapContext = copyPrimaryWriteIncarnation, cephmsgr.OSDTime{Seconds: seconds}, snapc
	return r.send(label, r.srcRoute, request)
}

func copyChunkSourceChange(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copySourceChangeCell, receipt *copySourceChangeReceipt) {
	label := fmt.Sprintf("change-%d-%t-%t", cell.code, cell.snapshot, cell.versioned)
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, label)
	sourcePrimary, _ := r.separatePrimaries()
	receipt.SourcePrimary, receipt.Primary = sourcePrimary, r.dstRoute.ID
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*17 + i>>10 + 5)
	}
	receipt.SourceBytes = len(data)
	first := r.writeSourceHead("source write", 1700009300, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != first.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, first.UserVersion)
	}
	var snapc cephmsgr.OSDSnapContext
	var sourceKeys []pgHistoryTarget
	sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction})
	if cell.snapshot {
		snap := nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "create"}).SnapID
		if snap == 0 {
			t.Fatal("native self-managed snapshot allocation")
		}
		receipt.SnapID = snap
		snapc = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
		cloning := r.writeSourceHead("cloning head write", 1700009310, []byte("head after the snapshot"), []byte{5}, snapc)
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction})
		clone := poolObserveSnap(t, ctx, 1, r.source, "", "", snap)
		if !sameNative(clone, copied) {
			t.Fatalf("the clone at snap %d is not the first write: %s; first %s", snap, nativeDigest(clone), nativeDigest(copied))
		}
	}
	receipt.CopiedUser = copied.Version

	route, source := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	firstAssert := uint64(0)
	if cell.snapshot {
		request.Operations[0].CopyFrom.SourceSnapshot = receipt.SnapID
	}
	if cell.versioned {
		request.Operations[0].CopyFrom.SourceVersion, firstAssert = copied.Version, copied.Version
	}
	receipt.SourceVersion = request.Operations[0].CopyFrom.SourceVersion
	h := r.holdAtChunk(request, route, source, 2, firstAssert, copied.Version, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	// The source head changes between the chunks; a snapshot's clone stays.
	changed := r.writeSourceHead("head write between chunks", 1700009320, []byte("source changed between chunks"), []byte{4}, snapc)
	receipt.ChangedUser = changed.UserVersion
	sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: changed.Transaction})
	if changed.UserVersion <= copied.Version {
		t.Fatalf("the write between chunks has user version %d, not above the copied %d", changed.UserVersion, copied.Version)
	}
	// Native reads wait out pauserd in the librados Objecter
	// (Objecter.cc:2550-2556), so the source is observed natively only after
	// the copy was answered; nothing else writes it meanwhile.
	h.pending(t, "after the source changed")
	r.recovery.clearPauseReads(t, ctx)

	var got copyChunkOutcome
	select {
	case got = <-h.done:
	case <-time.After(120 * time.Second):
		t.Fatal("the held copy was not answered after pause was cleared")
	}
	receipt.Reply.Code, receipt.Reply.TID, receipt.Reply.Attempt = got.reply.Code, got.reply.Transaction, got.reply.RetryAttempt
	receipt.Reply.Flags, receipt.Reply.UserVersion, receipt.Reply.Replay, receipt.Reply.Front = uint32(got.reply.Flags), got.reply.UserVersion, replayVersion(got.reply), len(got.reply.RawFront)
	for _, op := range got.reply.Operations {
		receipt.Reply.OpResults = append(receipt.Reply.OpResults, op.Result)
	}
	t.Logf("answered on the original connection: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s err=%v", got.reply.Code, receipt.Reply.OpResults,
		got.reply.Transaction, got.reply.RetryAttempt, got.reply.Flags, got.reply.UserVersion, replayVersion(got.reply), got.err)
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if cell.snapshot && got.err != nil || !cell.snapshot && (!errors.As(got.err, &server) || server.Code != got.reply.Code || errors.As(got.err, &unknown)) {
		t.Fatalf("the held copy ended with %v (code %d)", got.err, got.reply.Code)
	}
	if err := judgeSourceChange(cell.snapshot, got.reply, h.op.TID, copied.Version); err != nil {
		t.Fatal(err)
	}
	h.o.Close()
	if head := poolObserve(t, ctx, 1, r.source, "", ""); head.Code != 0 || head.Version != changed.UserVersion || string(head.Data) != "source changed between chunks" {
		t.Fatalf("native source head is not the write between chunks: code %d version %d (reply %d)", head.Code, head.Version, changed.UserVersion)
	}
	if cell.snapshot {
		if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", receipt.SnapID); !sameNative(clone, copied) {
			t.Fatalf("the head write between chunks changed the clone: %s", nativeDigest(clone))
		}
	}

	want := seeded
	if cell.snapshot {
		want = copied
	}
	for _, osd := range route.Acting {
		r.waitTempRemoved(osd, route)
		receipt.TempRemoved = append(receipt.TempRemoved, osd)
	}
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Final = nativeDigest(after)
	if (!cell.snapshot && !sameNative(after, seeded)) || (cell.snapshot && !sameCopy(after, copied, copied.Version)) {
		t.Fatalf("the destination is %s; want %s", nativeDigest(after), nativeDigest(want))
	}
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	s := r.history(route, copyKey, sourceKeys...)
	var others [][]pgHistoryLocation
	for i := range sourceKeys {
		others = append(others, s.Targets[1+i].Locations)
	}
	if cell.snapshot {
		if err := copyEntryOnly(s.Targets[0].Locations, hobjectHead(route), replayVersion(got.reply), others...); err != nil {
			t.Fatalf("destination primary osd.%d: %v", route.ID, err)
		}
	} else if len(s.Targets[0].Locations) != 0 || slices.ContainsFunc(others, func(l []pgHistoryLocation) bool { return len(l) != 0 }) {
		t.Fatalf("the destination PG log names the failed copy or a source write: %+v %+v", s.Targets[0].Locations, others)
	}

	// A fresh session reads the same destination, and nothing changes later.
	read := r.pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want.Data) + 64)})
	o := mutationConnection(t, r.client, ctx, route)
	reply, err := o.Request(ctx, read)
	o.Close()
	if err != nil || reply.Code != 0 || reply.Transaction <= h.op.TID || !bytes.Equal(reply.Operations[0].Data, want.Data) {
		t.Fatalf("fresh-session read: err=%v code=%d tid=%d %d bytes", err, reply.Code, reply.Transaction, len(reply.Operations[0].Data))
	}
	receipt.FreshTID = reply.Transaction
	time.Sleep(2 * time.Second)
	if later := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(later, after) {
		t.Fatalf("destination changed after the answer: %s -> %s", nativeDigest(after), nativeDigest(later))
	}
	t.Logf("destination %s after the source changed between chunks; temporary removed on osd %v", receipt.Final, receipt.TempRemoved)
}

func TestCopyFromMultiChunkSourceJudgements(t *testing.T) {
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	at := route
	at.Epoch = 32
	first := []string{"copy-get max 32768 in=29b"}
	later := []string{"assert-version v5", "copy-get max 32768 in=41b"}
	if !copyChunkShape(first, 0) || copyChunkShape(first, 5) || !copyChunkShape(later, 5) || copyChunkShape(later, 0) || copyChunkShape(later, 4) {
		t.Fatal("copy-get shapes misjudged")
	}
	sent := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.3",
		Ops: []objecterRequestsOp{{TID: 12, PG: "3.5", OSD: 0, Attempts: 1, OSDOps: later, CopyGetMax: []uint64{32768}}}}
	if op, err := checkChunkSent(sent, route, 0, false, 5, 12, []uint64{9}); err != nil || op.TID != 12 {
		t.Fatal("sent later chunk refused:", err)
	}
	if _, err := checkChunkSent(sent, route, 0, false, 5, 0, []uint64{9}); err != nil {
		t.Fatal("sent later chunk of any tid refused:", err)
	}
	for name, edit := range map[string]func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64){
		"paused wanted":     func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) { return true, 5, 12, nil },
		"first chunk shape": func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) { return false, 0, 12, nil },
		"another version":   func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) { return false, 6, 12, nil },
		"another tid":       func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) { return false, 5, 13, nil },
		"already served":    func(*objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) { return false, 5, 0, []uint64{12} },
		"never sent": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Ops[0].Attempts = 0
			return false, 5, 12, nil
		},
		"sent twice": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Ops[0].Attempts = 2
			return false, 5, 12, nil
		},
		"another OSD": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Ops[0].OSD = 2
			return false, 5, 12, nil
		},
		"default chunk": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Ops[0].OSDOps = []string{"assert-version v5", "copy-get max 8388608 in=41b"}
			return false, 5, 12, nil
		},
		"two ops": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Ops = append(r.Ops, r.Ops[0])
			return false, 5, 12, nil
		},
		"incomplete": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.Complete = false
			return false, 5, 12, nil
		},
		"absence claimed": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.AbsenceEstablished = true
			return false, 5, 12, nil
		},
		"brackets differ": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.After.ID = 0
			return false, 5, 12, nil
		},
		"asked another": func(r *objecterRequestsSnapshot) (bool, uint64, uint64, []uint64) {
			r.OSD = 0
			return false, 5, 12, nil
		},
	} {
		s := sent
		s.Ops = slices.Clone(sent.Ops)
		paused, version, tid, served := edit(&s)
		if _, err := checkChunkSent(s, route, 0, paused, version, tid, served); err == nil {
			t.Error("sent chunk accepted:", name)
		}
	}

	code := 0
	entry := pgHistoryLocation{Tier: "log", Index: 4, Version: "30'7", Op: "modify", Object: "4:c7a5d36b:::dst:head", ReturnCode: &code}
	if err := copyEntryOnly([]pgHistoryLocation{entry}, entry.Object, "30'7", nil, nil); err != nil {
		t.Fatal("copy entry refused:", err)
	}
	extra, failed := 0, -34
	for name, edit := range map[string]func() ([]pgHistoryLocation, [][]pgHistoryLocation){
		"absent": func() ([]pgHistoryLocation, [][]pgHistoryLocation) { return nil, nil },
		"twice":  func() ([]pgHistoryLocation, [][]pgHistoryLocation) { return []pgHistoryLocation{entry, entry}, nil },
		"source present": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			return []pgHistoryLocation{entry}, [][]pgHistoryLocation{nil, {entry}}
		},
		"extra id": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			e := entry
			e.Tier, e.ExtraIndex = "extra_reqids", &extra
			return []pgHistoryLocation{e}, nil
		},
		"dup tier": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			e := entry
			e.Tier = "dups"
			return []pgHistoryLocation{e}, nil
		},
		"error entry": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			e := entry
			e.Op, e.ReturnCode = "error", &failed
			return []pgHistoryLocation{e}, nil
		},
		"another version": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			e := entry
			e.Version = "30'8"
			return []pgHistoryLocation{e}, nil
		},
		"another object": func() ([]pgHistoryLocation, [][]pgHistoryLocation) {
			e := entry
			e.Object = "4:c7a5d36b:::other:head"
			return []pgHistoryLocation{e}, nil
		},
	} {
		locs, sources := edit()
		if copyEntryOnly(locs, entry.Object, "30'7", sources...) == nil {
			t.Error("copy entry accepted:", name)
		}
	}

	reply := cephmsgr.OSDReply{Code: -34, Transaction: 7, Operations: []cephmsgr.OSDOperationResult{{Result: -115}}}
	if err := judgeSourceChange(false, reply, 7, 3); err != nil {
		t.Fatal("head source change refused:", err)
	}
	done := cephmsgr.OSDReply{Code: 0, Transaction: 7, Flags: cephmsgr.OSDReplyOnDisk, UserVersion: 3, Operations: []cephmsgr.OSDOperationResult{{Result: 0}}}
	if err := judgeSourceChange(true, done, 7, 3); err != nil {
		t.Fatal("snapshot copy refused:", err)
	}
	for name, c := range map[string]struct {
		snapshot bool
		edit     func(*cephmsgr.OSDReply)
	}{
		"head copied":           {false, func(r *cephmsgr.OSDReply) { r.Code, r.Operations[0].Result = 0, 0 }},
		"head overflow":         {false, func(r *cephmsgr.OSDReply) { r.Code = -75 }},
		"head per-op":           {false, func(r *cephmsgr.OSDReply) { r.Operations[0].Result = -34 }},
		"head another tid":      {false, func(r *cephmsgr.OSDReply) { r.Transaction = 8 }},
		"head retried":          {false, func(r *cephmsgr.OSDReply) { r.RetryAttempt = 1 }},
		"snapshot failed":       {true, func(r *cephmsgr.OSDReply) { r.Code, r.Operations[0].Result = -34, -115 }},
		"snapshot head version": {true, func(r *cephmsgr.OSDReply) { r.UserVersion = 4 }},
		"snapshot uncommitted":  {true, func(r *cephmsgr.OSDReply) { r.Flags = 0 }},
		"snapshot per-op":       {true, func(r *cephmsgr.OSDReply) { r.Operations[0].Result = -115 }},
		"snapshot two ops":      {true, func(r *cephmsgr.OSDReply) { r.Operations = append(r.Operations, r.Operations[0]) }},
	} {
		r := reply
		if c.snapshot {
			r = done
		}
		r.Operations = slices.Clone(r.Operations)
		c.edit(&r)
		if judgeSourceChange(c.snapshot, r, 7, 3) == nil {
			t.Error("source-change answer accepted:", name)
		}
	}
}
