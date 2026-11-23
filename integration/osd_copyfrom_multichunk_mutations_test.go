package integration_test

import (
	"bytes"
	"context"
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

// Multi-chunk COPY_FROM and COPY_FROM2 sources mutated between two chunks
// other than by a data write: an OMAP edit of the source head, the removal
// of the head and the removal of the snapshot a copy reads. The barrier is
// that of osd_copyfrom_multichunk_sources_test.go (holdAtChunk): the source
// primary is frozen while each copy-get is in flight, pauserd is set before
// it is resumed, and the next copy-get stays paused and unsent with one
// temporary object listed on every OSD of the destination PG. The pinned
// source (Ceph v20.2.4) gives each cell its expected result:
//
//   - Every write raises the head's user version, an OMAP edit included, so
//     the later copy-get's assert-version fails with -ERANGE
//     (src/osd/PrimaryLogPG.cc:6529-6542), as for the data write of the
//     source-change test. A copy at a snap id reads the clone, which keeps
//     the OMAP the head had when it was cloned (make_writeable,
//     PrimaryLogPG.cc:8640-8760, clones the whole object and copy_user_bits
//     keeps the clone's FLAG_OMAP and user version; do_copy_get,
//     PrimaryLogPG.cc:9295-9486, walks the OMAP of the object it resolved),
//     so an edit of the head between chunks does not reach the copy.
//   - A removed head without clones has no object context on disk, and the
//     cached one does not exist: do_op answers a read of it with -ENOENT
//     (PrimaryLogPG.cc:2440-2452 and 2526-2538). Only a copy-get that is the
//     first op gets the copy-get -ENOENT reply with request IDs; a later
//     chunk starts with assert-version, so the source primary answers it
//     with a plain -ENOENT.
//   - A head with clones is removed as a whiteout (_delete_oid,
//     PrimaryLogPG.cc:8273-8350), and the clone stays readable at its snap id
//     through the whiteout's snapset (find_object_context, 12067-12264).
//   - A snap id in the OSDMap removed_snaps_queue is -ENOENT for every read
//     at it (find_object_context, 12232-12237), before any op runs, whether
//     or not the clone was trimmed; nosnaptrim keeps the clone, so the
//     -ENOENT comes from the queue alone.
//   - The destination primary answers a source error other than -ECANCELED
//     with reply_op_error (CopyFromCallback::finish, 344-368), which logs
//     nothing, and process_copy_chunk (9694-9890) removes the temporary
//     object afterwards.

// copyChunkReply is the raw aggregate and per-op result of the reply that
// answered a held copy, for a receipt.
type copyChunkReply struct {
	Code        int32   `json:"code"`
	OpResults   []int32 `json:"op_results"`
	TID         uint64  `json:"tid"`
	Attempt     int32   `json:"attempt"`
	Flags       uint32  `json:"flags"`
	UserVersion uint64  `json:"user_version"`
	Replay      string  `json:"replay_version"`
	Front       int     `json:"raw_front_bytes"`
}

func newCopyChunkReply(reply cephmsgr.OSDReply) copyChunkReply {
	r := copyChunkReply{Code: reply.Code, TID: reply.Transaction, Attempt: reply.RetryAttempt, Flags: uint32(reply.Flags),
		UserVersion: reply.UserVersion, Replay: replayVersion(reply), Front: len(reply.RawFront)}
	for _, op := range reply.Operations {
		r.OpResults = append(r.OpResults, op.Result)
	}
	return r
}

// copyChunkMutationReceipt is the archived public receipt of one cell of
// the tests below: identities, versions, digests and native observations.
type copyChunkMutationReceipt struct {
	Cell          string               `json:"cell"`
	Mutation      string               `json:"mutation"`
	SourcePrimary int32                `json:"source_primary"`
	Primary       int32                `json:"destination_primary"`
	Chunk         uint64               `json:"chunk_limit"`
	SourceBytes   int                  `json:"source_bytes"`
	OmapEntries   int                  `json:"source_omap_entries,omitempty"`
	SnapID        uint64               `json:"source_snap_id,omitempty"`
	CopiedUser    uint64               `json:"copied_user_version"`
	Cursors       []int                `json:"cursor_omap_offset_bytes"`
	Steps         []copyChunkStep      `json:"chunks"`
	BarrierReqid  string               `json:"barrier_reqid"`
	HeldChunk     []string             `json:"held_copy_get"`
	Temporary     string               `json:"temporary_object"`
	MutationUser  uint64               `json:"mutation_user_version,omitempty"`
	Removal       *copyPurgedTrimState `json:"removed_snapshot_state,omitempty"`
	Reply         copyChunkReply       `json:"reply"`
	TempRemoved   []int32              `json:"temporary_removed_on"`
	Final         string               `json:"native_final"`
	SourceOmap    string               `json:"source_omap_sha256,omitempty"`
	HeadOmap      string               `json:"edited_head_omap_sha256,omitempty"`
	FinalOmap     string               `json:"destination_omap_sha256,omitempty"`
	FreshTID      uint64               `json:"fresh_read_tid"`
	ChunkRestore  bool                 `json:"chunk_restored"`
}

// judgeChunkAnswer decides whether the answer to a held copy is the one its
// cell expects: a committed 0 with the user version of the copied object
// when want is 0, otherwise the aggregate want with the copy's per-op result
// still -EINPROGRESS. Either way it is the held client op, its first attempt,
// with one op.
func judgeChunkAnswer(want int32, reply cephmsgr.OSDReply, tid, user uint64) error {
	if reply.Transaction != tid || reply.RetryAttempt != 0 || len(reply.Operations) != 1 {
		return fmt.Errorf("the answer is tid %d attempt %d with %d ops; want tid %d, attempt 0, one op", reply.Transaction, reply.RetryAttempt, len(reply.Operations), tid)
	}
	if want != 0 {
		if reply.Code != want || reply.Operations[0].Result != -115 {
			return fmt.Errorf("the copy answered %d (per-op %d); want %d with per-op -EINPROGRESS", reply.Code, reply.Operations[0].Result, want)
		}
		return nil
	}
	if reply.Code != 0 || reply.Operations[0].Result != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion != user {
		return fmt.Errorf("the copy answered %d (per-op %d, flags %#x, user %d); want a committed 0 with user version %d",
			reply.Code, reply.Operations[0].Result, reply.Flags, reply.UserVersion, user)
	}
	return nil
}

// awaitHeld clears pauserd and waits for the one answer of the held copy on
// its original connection; a failure must be a server error with the reply's
// code, never an unknown outcome.
func (r *copyPrimaryChangeRun) awaitHeld(h *copyChunkHold, receipt *copyChunkMutationReceipt) cephmsgr.OSDReply {
	t := r.t
	t.Helper()
	h.pending(t, "before pause was cleared")
	r.recovery.clearPauseReads(t, r.ctx)
	var got copyChunkOutcome
	select {
	case got = <-h.done:
	case <-time.After(120 * time.Second):
		t.Fatal("the held copy was not answered after pause was cleared")
	}
	receipt.Reply = newCopyChunkReply(got.reply)
	t.Logf("answered on the original connection: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s err=%v", got.reply.Code, receipt.Reply.OpResults,
		got.reply.Transaction, got.reply.RetryAttempt, got.reply.Flags, got.reply.UserVersion, receipt.Reply.Replay, got.err)
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if got.reply.Code == 0 && got.err != nil || got.reply.Code != 0 && (!errors.As(got.err, &server) || server.Code != got.reply.Code || errors.As(got.err, &unknown)) {
		t.Fatalf("the held copy ended with %v (code %d)", got.err, got.reply.Code)
	}
	h.o.Close()
	return got.reply
}

// settleChunkCopy checks what the held copy left once it was answered: the
// temporary object gone from every OSD of the destination PG, the
// destination natively want (the seed after a failure, a complete copy of
// copied at its user version after a success), and a fresh session reading
// want with nothing changing later. The destination primary's PG log must
// name neither the copy nor a source request after a failure. After a
// success it holds the copy once: bound to the first source request, the
// head write copied, when bound is true, and without any source request ID
// otherwise (a clone's copy).
func (r *copyPrimaryChangeRun) settleChunkCopy(h *copyChunkHold, reply cephmsgr.OSDReply, seeded, copied nativeXattrSnapshot, sourceKeys []pgHistoryTarget, bound bool, receipt *copyChunkMutationReceipt) nativeXattrSnapshot {
	t, ctx := r.t, r.ctx
	t.Helper()
	route := h.route
	for _, osd := range route.Acting {
		r.waitTempRemoved(osd, route)
		receipt.TempRemoved = append(receipt.TempRemoved, osd)
	}
	copiedOK := reply.Code == 0
	want := seeded
	if copiedOK {
		want = copied
	}
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Final = nativeDigest(after)
	if (!copiedOK && !sameNative(after, seeded)) || (copiedOK && !sameCopy(after, copied, copied.Version)) {
		t.Fatalf("the destination is %s; want %s", nativeDigest(after), nativeDigest(want))
	}
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	s := r.history(route, copyKey, sourceKeys...)
	var others [][]pgHistoryLocation
	for i := range sourceKeys {
		others = append(others, s.Targets[1+i].Locations)
	}
	switch {
	case copiedOK && bound:
		binding := copyFromHistoryWant{object: hobjectHead(route), version: replayVersion(reply), sourceUserVersion: copied.Version}
		if err := copyFromDestinationBinding(s.Targets[0].Locations, others[0], binding); err != nil {
			t.Fatalf("destination primary osd.%d: %v", route.ID, err)
		}
		if slices.ContainsFunc(others[1:], func(l []pgHistoryLocation) bool { return len(l) != 0 }) {
			t.Fatalf("the destination PG log names a later source request: %+v", others[1:])
		}
	case copiedOK:
		if err := copyEntryOnly(s.Targets[0].Locations, hobjectHead(route), replayVersion(reply), others...); err != nil {
			t.Fatalf("destination primary osd.%d: %v", route.ID, err)
		}
	case len(s.Targets[0].Locations) != 0 || slices.ContainsFunc(others, func(l []pgHistoryLocation) bool { return len(l) != 0 }):
		t.Fatalf("the destination PG log names the failed copy or a source request: %+v %+v", s.Targets[0].Locations, others)
	}

	read := r.pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want.Data) + 64)})
	o := mutationConnection(t, r.client, ctx, route)
	fresh, err := o.Request(ctx, read)
	o.Close()
	if err != nil || fresh.Code != 0 || fresh.Transaction <= h.op.TID || !bytes.Equal(fresh.Operations[0].Data, want.Data) {
		t.Fatalf("fresh-session read: err=%v code=%d tid=%d %d bytes", err, fresh.Code, fresh.Transaction, len(fresh.Operations[0].Data))
	}
	receipt.FreshTID = fresh.Transaction
	time.Sleep(2 * time.Second)
	if later := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(later, after) {
		t.Fatalf("destination changed after the answer: %s -> %s", nativeDigest(after), nativeDigest(later))
	}
	t.Logf("destination %s after the answer; temporary removed on osd %v; fresh read tid %d", receipt.Final, receipt.TempRemoved, fresh.Transaction)
	return after
}

// copyChunkReceipts collects one test's receipts and writes them on cleanup.
func copyChunkReceipts(t *testing.T, recovery *copyPrimaryRecovery, name string) func(copyChunkMutationReceipt) {
	t.Helper()
	var mu sync.Mutex
	var receipts []copyChunkMutationReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, name), append(raw, '\n'))
		}
		if err != nil {
			t.Error(name, err)
		}
	})
	return func(receipt copyChunkMutationReceipt) {
		mu.Lock()
		receipts = append(receipts, receipt)
		mu.Unlock()
	}
}

// copyOmapEdit is the OMAP edit between chunks: the value of the entry the
// second chunk stopped on and of the last entry replaced, the 41st entry
// removed and the header replaced. It returns the edited model and header.
func copyOmapEdit(entries []cephmsgr.OSDOmapEntry, stop int) ([]cephmsgr.OSDOperation, map[string][]byte, []byte) {
	model := map[string][]byte{}
	for _, entry := range entries {
		model[entry.Key] = entry.Value
	}
	set := []cephmsgr.OSDOmapEntry{{Key: entries[stop].Key, Value: []byte("edited between chunks")}, {Key: entries[len(entries)-1].Key, Value: []byte("edited last entry")}}
	removed := entries[40].Key
	header := []byte("edited header \x01")
	for _, entry := range set {
		model[entry.Key] = entry.Value
	}
	delete(model, removed)
	return []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: set}},
		{Code: cephmsgr.OSDRemoveOmapKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{removed}}},
		{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}},
	}, model, header
}

// sendSourceHead sends ops to the source head once under snapc with the
// write incarnation and returns the committed reply.
func (r *copyPrimaryChangeRun) sendSourceHead(label string, seconds uint32, snapc cephmsgr.OSDSnapContext, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
	r.t.Helper()
	r.srcRoute = poolPlace(r.t, r.ctx, 1, r.source, "")
	request := r.pools.request(r.srcRoute, ops...)
	request.ClientIncarnation, request.ModificationTime, request.SnapContext = copyPrimaryWriteIncarnation, cephmsgr.OSDTime{Seconds: seconds}, snapc
	return r.send(label, r.srcRoute, request)
}

// cloneSource allocates a self-managed snapshot of the source pool and makes
// the head's clone of it with one Go SETXATTR under it, which leaves the data
// and OMAP of the head alone; the clone must natively be copied. It returns
// the snap id, the SnapContext and the cloning write's reply.
func (r *copyPrimaryChangeRun) cloneSource(copied nativeXattrSnapshot) (uint64, cephmsgr.OSDSnapContext, cephmsgr.OSDReply) {
	t, ctx := r.t, r.ctx
	t.Helper()
	snap := nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "create"}).SnapID
	if snap == 0 {
		t.Fatal("native self-managed snapshot allocation")
	}
	snapc := cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	cloning := r.sendSourceHead("cloning head write", 1700009410, snapc,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "copy-cloning-attr"}, Data: []byte{3}})
	if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", snap); !sameNative(clone, copied) {
		t.Fatalf("the clone at snap %d is not the copied source: %s; source %s", snap, nativeDigest(clone), nativeDigest(copied))
	}
	return snap, snapc, cloning
}

// copyOmapEditCell is one opcode and a head or snapshot source.
type copyOmapEditCell struct {
	copyPrimaryChangeCase
	snapshot bool
}

func (c copyOmapEditCell) label() string {
	if c.snapshot {
		return c.name + "/snapshot"
	}
	return c.name + "/head"
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of the three-chunk OMAP
// source of osd_copyfrom_multichunk_sources_test.go (32 KiB of data, one
// xattr, a header and 48 entries) onto a natively seeded destination is held
// before its third chunk, in the middle of the OMAP: the held copy-get
// asserts the source version and continues from the 33rd entry's key
// (in=47b). One Go request then edits the source head's OMAP: it replaces the
// value of that 33rd entry and of the last one, removes the 41st and replaces
// the header. Pause is cleared.
//
// Head source: the third copy-get fails its version assertion, so the copy is
// answered once, on its connection, with -ERANGE and per-op -EINPROGRESS. The
// destination keeps its seed natively (bytes, xattrs, user version, mtime,
// and no OMAP or header), every OSD of its PG drops the temporary object, its
// primary's PG log names neither the copy nor a source request, and a fresh
// session reads the seed.
//
// Snapshot source (a clone made by a Go SETXATTR under a self-managed
// snapshot, before the copy): every chunk reads the clone, so the copy is
// answered once with 0 and the clone's user version, and the destination is
// natively the clone with the OMAP and header the source had before the edit,
// while the head natively holds the edited OMAP. The temporary object is
// gone, the PG log holds the copy once without any source request ID, and a
// fresh session reads the clone's bytes.
//
// Not verified: version-pinned copies of an edited source, an edit before the
// OMAP starts (the second chunk), EC sources, and the legacy one-PG pools
// when their primaries coincide.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunkomapedit key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunkomapedit key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates self-managed snapshots in its owned pool
func TestCephOSDCopyFromChunkOmapEditIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	keep := copyChunkReceipts(t, recovery, "osd-copyfrom-chunk-omap-edit-results.json")
	for _, tc := range copyPrimaryChangeCases {
		for _, snapshot := range []bool{false, true} {
			cell := copyOmapEditCell{tc, snapshot}
			t.Run(cell.label(), func(t *testing.T) {
				receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: "omap-edit", Chunk: multiChunkLimit}
				defer func() {
					if !t.Skipped() {
						keep(receipt)
					}
				}()
				copyChunkOmapEdit(t, ctx, pools, recovery, cell, &receipt)
			})
		}
	}
}

func copyChunkOmapEdit(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copyOmapEditCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("omap-%d-%t", cell.code, cell.snapshot))
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()
	seededOmap := poolOmapState(t, ctx, 2, r.dest)
	if seededOmap.Header == nil || len(*seededOmap.Header) != 0 || len(omapPage(t, seededOmap)) != 0 {
		t.Fatal("the native seed has an OMAP or header")
	}

	data := make([]byte, multiChunkLimit)
	for i := range data {
		data[i] = byte(i*31 + i>>7 + 9)
	}
	header := []byte("omap edit header \x00\xfe")
	entries, cursors := copyThreeChunkSource()
	receipt.Cursors, receipt.SourceBytes, receipt.OmapEntries = cursors, len(data), len(entries)
	write, source := r.writeOmapSource(data, header, entries)
	receipt.SourceOmap = omapDigest(t, source)
	copied := source.nativeXattrSnapshot
	receipt.CopiedUser = copied.Version
	sourceKeys := []pgHistoryTarget{{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}}
	var snapc cephmsgr.OSDSnapContext
	if cell.snapshot {
		var cloning cephmsgr.OSDReply
		receipt.SnapID, snapc, cloning = r.cloneSource(copied)
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction})
	}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	if cell.snapshot {
		request.Operations[0].CopyFrom.SourceSnapshot = receipt.SnapID
	}
	h := r.holdAtChunk(request, route, src, 3, 0, copied.Version, cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	edit, model, editedHeader := copyOmapEdit(entries, copyOmapStop(multiChunkLimit, entries))
	edited := r.sendSourceHead("OMAP edit between chunks", 1700009420, snapc, edit...)
	receipt.MutationUser = edited.UserVersion
	sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: edited.Transaction})
	if edited.UserVersion <= copied.Version {
		t.Fatalf("the OMAP edit has user version %d, not above the copied %d", edited.UserVersion, copied.Version)
	}
	reply := r.awaitHeld(h, receipt)
	want, user := int32(-34), uint64(0)
	if cell.snapshot {
		want, user = 0, copied.Version
	}
	if err := judgeChunkAnswer(want, reply, h.op.TID, user); err != nil {
		t.Fatal(err)
	}

	// Native reads wait out pauserd in the librados Objecter, so the edit is
	// observed only now; nothing else writes the source meanwhile.
	head := poolOmapState(t, ctx, 1, r.source)
	if head.Code != 0 || head.Version != edited.UserVersion || head.Header == nil || !bytes.Equal(*head.Header, editedHeader) || !equalOmapModel(omapPage(t, head), model) {
		t.Fatalf("native source head is not the OMAP edit: code %d version %d (reply %d)", head.Code, head.Version, edited.UserVersion)
	}
	receipt.HeadOmap = omapDigest(t, head)
	if cell.snapshot {
		if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", receipt.SnapID); !sameNative(clone, copied) {
			t.Fatalf("the OMAP edit changed the clone: %s", nativeDigest(clone))
		}
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, false, receipt)
	final := poolOmapState(t, ctx, 2, r.dest)
	receipt.FinalOmap = omapDigest(t, final)
	switch {
	case cell.snapshot && (!sameOmap(t, final, source) || sameOmap(t, final, head)):
		t.Fatalf("the destination OMAP %s is not the clone's %s (head %s)", receipt.FinalOmap, receipt.SourceOmap, receipt.HeadOmap)
	case !cell.snapshot && !sameOmap(t, final, seededOmap):
		t.Fatalf("the failed copy left OMAP %s on the seed", receipt.FinalOmap)
	}
	t.Logf("destination OMAP %s; source OMAP before the edit %s, head after it %s", receipt.FinalOmap, receipt.SourceOmap, receipt.HeadOmap)
}

// copyRemovalCell is one opcode and what is removed between the chunks: the
// head of a head source, the head of a snapshot source, or the snapshot a
// snapshot source reads.
type copyRemovalCell struct {
	copyPrimaryChangeCase
	removal string
}

func (c copyRemovalCell) label() string { return c.name + "/" + c.removal }

func (c copyRemovalCell) snapshot() bool { return c.removal != "head-of-head-source" }

// copyRemovalAnswer is the answer each removal must get: -ENOENT once the
// object the copy reads is gone, the clone's copy when only the head went.
func copyRemovalAnswer(removal string, cloneUser uint64) (int32, uint64) {
	if removal == "head-of-snapshot-source" {
		return 0, cloneUser
	}
	return -2, 0
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of a 64 KiB source onto
// a natively seeded destination is held before its second chunk by the
// barrier described above; then the source loses what the copy reads, and
// pause is cleared.
//
// Head of a head source: a Go DELETE removes the head, which has no clones.
// The second copy-get, asserting the source version, is answered -ENOENT, so
// the copy is answered once, on its connection, with -ENOENT and per-op
// -EINPROGRESS. The destination keeps its seed natively, every OSD of its PG
// drops the temporary object, its primary's PG log names neither the copy
// nor a source request, the source head is natively absent, and a fresh
// session reads the seed.
//
// Head of a snapshot source (a clone of a self-managed snapshot under a
// rewritten head): a Go DELETE under the snapshot's context removes the head
// as a whiteout. Every chunk reads the clone, so the copy is answered once
// with 0 and the clone's user version, and the destination is natively the
// clone; the temporary object is gone, the PG log holds the copy once
// without any source request ID, the head is natively absent while the clone
// is still read at its snap id, and a fresh session reads the clone's bytes.
//
// The snapshot of a snapshot source: nosnaptrim is set, the snapshot is
// removed natively, and a native observation shows it in the OSDMap
// removed_snaps_queue and the source PG's snap_trimq. The second copy-get is
// answered -ENOENT, so the copy gets -ENOENT with per-op -EINPROGRESS and the
// seed is kept as for the head source; a native read at the snap id is
// -ENOENT while native listsnaps still lists the clone with that snapshot.
//
// Not verified: version-pinned copies, a removal before the first chunk, a
// trimmed clone (it is -ENOENT by find_object_context as well), EC sources
// and the legacy one-PG pools when their primaries coincide.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunksourceremoval key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromchunksourceremoval key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, removes owned snapshots, sets nosnaptrim
func TestCephOSDCopyFromChunkSourceRemovalIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	keep := copyChunkReceipts(t, recovery, "osd-copyfrom-chunk-source-removal-results.json")
	for _, tc := range copyPrimaryChangeCases {
		for _, removal := range []string{"head-of-head-source", "head-of-snapshot-source", "snapshot-of-snapshot-source"} {
			cell := copyRemovalCell{tc, removal}
			t.Run(cell.label(), func(t *testing.T) {
				receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: removal, Chunk: multiChunkLimit}
				defer func() {
					if !t.Skipped() {
						keep(receipt)
					}
				}()
				copyChunkSourceRemoval(t, ctx, pools, recovery, cell, &receipt)
			})
		}
	}
}

// snapTrimState observes where removed snapshots of the source pool are for
// the source's PG, listing objects natively (no object while pauserd is set:
// listsnaps is a read).
func (r *copyPrimaryChangeRun) snapTrimState(objects []string) copyPurgedTrimState {
	r.t.Helper()
	var result struct {
		SnapTrim copyPurgedTrimState `json:"snap_trim"`
	}
	nativePoolResult(r.t, r.ctx, "snaptrim", map[string]any{"slot": 1, "object": r.source, "objects": objects}, &result)
	s := result.SnapTrim
	if s.PoolID != r.pools.ids[1] || s.PGID != r.srcRoute.PGID || len(s.ListSnaps) != len(objects) {
		r.t.Fatalf("native snaptrim observation is not the source PG's: %+v", s)
	}
	return s
}

// cloneListed says whether a native listsnaps of the source lists a clone
// holding snap.
func cloneListed(listing snapCLIListing, snap uint64) bool {
	for _, clone := range listing.Clones {
		for _, s := range clone.Snapshots {
			if s.ID == snap {
				return true
			}
		}
	}
	return false
}

func copyChunkSourceRemoval(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copyRemovalCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("removal-%d-%s", cell.code, cell.removal))
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()
	if cell.removal == "snapshot-of-snapshot-source" {
		holdSnapTrim(t, ctx, r.client)
	}

	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*23 + i>>9 + 7)
	}
	receipt.SourceBytes, receipt.Cursors = len(data), []int{0, 0}
	first := r.writeSourceHead("source write", 1700009400, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != first.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, first.UserVersion)
	}
	receipt.CopiedUser = copied.Version
	sourceKeys := []pgHistoryTarget{{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction}}
	var snapc cephmsgr.OSDSnapContext
	if cell.snapshot() {
		var cloning cephmsgr.OSDReply
		receipt.SnapID, snapc, cloning = r.cloneSource(copied)
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction})
	}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	if cell.snapshot() {
		request.Operations[0].CopyFrom.SourceSnapshot = receipt.SnapID
	}
	// The source has no OMAP: both copy-gets carry an empty OMAP position.
	h := r.holdAtChunk(request, route, src, 2, 0, copied.Version, receipt.Cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	if cell.removal == "snapshot-of-snapshot-source" {
		nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "remove", "snap_id": receipt.SnapID})
		for deadline := time.Now().Add(60 * time.Second); ; {
			s := r.snapTrimState(nil)
			if phase := copyPurgedPhase(s, receipt.SnapID); phase == "pending" {
				receipt.Removal = &s
				break
			} else if time.Now().After(deadline) {
				t.Fatalf("snap %d is not pending trim in the source PG (phase %q): %+v", receipt.SnapID, phase, s)
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Logf("snap %d removed and pending trim under nosnaptrim: removed_snaps_queue %v, snap_trimq %v at epoch %d",
			receipt.SnapID, receipt.Removal.RemovedSnapQueue, receipt.Removal.SnapTrimQueue, receipt.Removal.OSDMapEpoch)
	} else {
		removed := r.sendSourceHead("head removal between chunks", 1700009430, snapc, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		receipt.MutationUser = removed.UserVersion
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: removed.Transaction})
	}
	reply := r.awaitHeld(h, receipt)
	want, user := copyRemovalAnswer(cell.removal, copied.Version)
	if err := judgeChunkAnswer(want, reply, h.op.TID, user); err != nil {
		t.Fatal(err)
	}

	// The source is observed natively only now that pauserd is clear.
	head := poolObserve(t, ctx, 1, r.source, "", "")
	switch cell.removal {
	case "snapshot-of-snapshot-source":
		if head.Code != 0 {
			t.Fatalf("the source head is natively %d after its snapshot was removed", head.Code)
		}
		if at := poolObserveSnap(t, ctx, 1, r.source, "", "", receipt.SnapID); at.Code != -2 {
			t.Fatalf("a native read at the removed snap %d is %d, not -ENOENT", receipt.SnapID, at.Code)
		}
		s := r.snapTrimState([]string{r.source})
		if copyPurgedPhase(s, receipt.SnapID) != "pending" || !cloneListed(s.ListSnaps[r.source].listing(t, r.source), receipt.SnapID) {
			t.Fatalf("the removed snap %d is not still pending with its clone listed: %+v", receipt.SnapID, s)
		}
	default:
		if head.Code != -2 {
			t.Fatalf("the removed source head is natively %d, not -ENOENT", head.Code)
		}
		if cell.snapshot() {
			if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", receipt.SnapID); !sameNative(clone, copied) {
				t.Fatalf("the head removal changed the clone: %s", nativeDigest(clone))
			}
		}
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, false, receipt)
}

func TestCopyFromChunkMutationJudgements(t *testing.T) {
	entries, _ := copyThreeChunkSource()
	stop := copyOmapStop(multiChunkLimit, entries)
	ops, model, header := copyOmapEdit(entries, stop)
	if len(ops) != 3 || len(model) != len(entries)-1 || string(model[entries[stop].Key]) != "edited between chunks" || string(model[entries[47].Key]) != "edited last entry" ||
		len(header) == 0 || ops[1].Omap.Keys[0] != entries[40].Key {
		t.Fatalf("OMAP edit misjudged: %d ops, %d entries", len(ops), len(model))
	}
	if _, ok := model[entries[40].Key]; ok || stop >= 40 {
		t.Fatal("the removed entry is still in the model, or not after the stop entry")
	}

	if code, user := copyRemovalAnswer("head-of-snapshot-source", 7); code != 0 || user != 7 {
		t.Fatal("head removal of a snapshot source misjudged")
	}
	for _, removal := range []string{"head-of-head-source", "snapshot-of-snapshot-source"} {
		if code, _ := copyRemovalAnswer(removal, 7); code != -2 {
			t.Fatal("removal misjudged:", removal)
		}
		if (copyRemovalCell{removal: removal}).snapshot() != (removal != "head-of-head-source") {
			t.Fatal("removal source misjudged:", removal)
		}
	}

	failed := cephmsgr.OSDReply{Code: -2, Transaction: 7, Operations: []cephmsgr.OSDOperationResult{{Result: -115}}}
	if err := judgeChunkAnswer(-2, failed, 7, 0); err != nil {
		t.Fatal("removed source refused:", err)
	}
	done := cephmsgr.OSDReply{Code: 0, Transaction: 7, Flags: cephmsgr.OSDReplyOnDisk, UserVersion: 3, Operations: []cephmsgr.OSDOperationResult{{Result: 0}}}
	if err := judgeChunkAnswer(0, done, 7, 3); err != nil {
		t.Fatal("copy refused:", err)
	}
	for name, c := range map[string]struct {
		want int32
		edit func(*cephmsgr.OSDReply)
	}{
		"failure copied":       {-2, func(r *cephmsgr.OSDReply) { r.Code, r.Operations[0].Result = 0, 0 }},
		"another failure":      {-2, func(r *cephmsgr.OSDReply) { r.Code = -34 }},
		"failure per-op":       {-2, func(r *cephmsgr.OSDReply) { r.Operations[0].Result = -2 }},
		"failure another tid":  {-2, func(r *cephmsgr.OSDReply) { r.Transaction = 8 }},
		"failure retried":      {-2, func(r *cephmsgr.OSDReply) { r.RetryAttempt = 1 }},
		"failure two ops":      {-2, func(r *cephmsgr.OSDReply) { r.Operations = append(r.Operations, r.Operations[0]) }},
		"copy failed":          {0, func(r *cephmsgr.OSDReply) { r.Code, r.Operations[0].Result = -2, -115 }},
		"copy another version": {0, func(r *cephmsgr.OSDReply) { r.UserVersion = 4 }},
		"copy uncommitted":     {0, func(r *cephmsgr.OSDReply) { r.Flags = 0 }},
		"copy per-op":          {0, func(r *cephmsgr.OSDReply) { r.Operations[0].Result = -115 }},
		"copy another tid":     {0, func(r *cephmsgr.OSDReply) { r.Transaction = 9 }},
	} {
		r := failed
		if c.want == 0 {
			r = done
		}
		r.Operations = slices.Clone(r.Operations)
		c.edit(&r)
		if judgeChunkAnswer(c.want, r, 7, 3) == nil {
			t.Error("chunk answer accepted:", name)
		}
	}

	var listing snapCLIListing
	if err := json.Unmarshal([]byte(`{"name":"src","seq":5,"clones":[{"id":4,"snapshots":[{"id":3,"name":"3"},{"id":4,"name":"4"}],"size":65536,"overlaps":[]},{"id":"head","snapshots":[],"size":0,"overlaps":[]}]}`), &listing); err != nil {
		t.Fatal(err)
	}
	if !cloneListed(listing, 3) || !cloneListed(listing, 4) || cloneListed(listing, 5) || cloneListed(snapCLIListing{}, 3) {
		t.Fatal("listed clone misjudged")
	}
}
