package integration_test

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// rbdJournalFencedHoldClaims checks a fenced-hold record up to one stage.
// It keeps apart what the OSDs listed while the caller was pending (held,
// from "observe-held" on), what the fence put into the OSDMap and what the
// primary then refused or still held (fence), what the caller was told and
// how often it asked (outcome), what the PG's interval history says around
// the request (interval), and what native clients found afterwards, up to
// native librbd's replay (journal).
func rbdJournalFencedHoldClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := rbdJournalHeldText
	h, e := r.Held, r.Entry
	stages, order := rbdJournalFencedPlan(h.Mode)
	reached := slices.Index(stages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if r.PrimaryChange != nil || r.OwnerLoss != nil || r.Trim != nil || r.Inflight != nil || r.ForcedPending != nil || r.Consumer != nil || h.Pending != nil {
		return []string{"record: a fenced-hold record carries another variant's part"}
	}
	f := h.Fenced
	if f == nil || f.LockerAddress == "" {
		return []string{"record: the fenced-hold part or the lock record's address is missing"}
	}
	erasure := rbdJournalErasure(h.Mode)
	followed, dataObject := []string{"rbd_header." + r.ImageID, e.ObjectName, r.JournalHeader}, 1
	pool, poolID := r.Pool, r.PoolID
	if erasure {
		followed, dataObject = []string{e.ObjectName}, 0
		pool, poolID = h.JournalPool, h.JournalPoolID
	}
	if erasure != (h.JournalPool != "") || erasure && (h.JournalPoolID <= 0 || h.JournalPoolID == r.PoolID || h.JournalPool == r.Pool) {
		return []string{"record: the journal pool does not match the mode: " + text(h.JournalPool)}
	}
	inventory := -1
	view := func(name string, at int) *rbdJournalLostAppendView {
		if reached < at {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name {
			fail("%s: native observation missing", name)
			return nil
		}
		if int(e.Object) >= len(v.ObjectSizes) || len(v.ObjectSHA256) != len(v.ObjectSizes) || len(v.ObjectExists) != len(v.ObjectSizes) ||
			inventory >= 0 && len(v.ObjectSizes) != inventory {
			fail("%s: native observation incomplete (%d journal objects, the setup listed %d)", name, len(v.ObjectSizes), inventory)
			return nil
		}
		if inventory < 0 {
			inventory = len(v.ObjectSizes)
		}
		return v
	}
	setup, locked := view("setup", 0), view("observe-locked", 0)
	fenced, replay := view("observe-fenced", 2), view("replay", 3)
	if len(problems) != 0 {
		return problems
	}
	if want := order[:slices.Index(order, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Length <= 0 || len(e.SHA256) != 64 || e.Tag == 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 {
		fail("identity: entry or client identity incomplete")
	}
	client := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	lockRecord := func(name string, v *rbdJournalLostAppendView, watch bool) {
		if v.LockFree || v.LockOwner != client || v.LockCookie != r.LockCookie || v.SubjectWatcher != watch {
			fail("%s: the lock record is not the subject's with its watch %s (free=%v owner=%q cookie=%q watcher=%v)", name, map[bool]string{true: "listed", false: "gone"}[watch],
				v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
		}
	}
	unmoved := func(name string, v *rbdJournalLostAppendView) {
		if v.ImageHeadSHA256 != r.ExpectedHeadBefore || !reflect.DeepEqual(v.ImageCommit, setup.ImageCommit) || v.NextTag != e.Tag+1 || v.LastTag.TID != e.Tag {
			fail("%s: image, image client commit position or tags moved (head=%s commit=%v next tag=%d)", name, v.ImageHeadSHA256, v.ImageCommit, v.NextTag)
		}
	}
	entryAt := func(name string, v *rbdJournalLostAppendView) {
		if v.PrefixSHA256 == nil || *v.PrefixSHA256 != locked.ObjectSHA256[e.Object] || v.EntrySHA256 == nil || *v.EntrySHA256 != e.SHA256 ||
			v.TrailingBytes == nil || *v.TrailingBytes != 0 || v.ObjectSizes[e.Object] != uint64(e.Offset+e.Length) {
			fail("%s: journal object %d is not its earlier bytes followed by exactly the owned entry (size %d, entry at %d+%d)", name, e.Object, v.ObjectSizes[e.Object], e.Offset, e.Length)
		}
		for i := range v.ObjectSizes {
			if uint64(i) != e.Object && (i >= len(locked.ObjectSHA256) || v.ObjectSHA256[i] != locked.ObjectSHA256[i] || v.ObjectSizes[i] != locked.ObjectSizes[i]) {
				fail("%s: journal object %d changed", name, i)
			}
		}
	}
	members := func(osds []int32) []int32 { sorted := slices.Clone(osds); slices.Sort(sorted); return sorted }
	placedOn := func(name string, read *nativeRBDJournalHeldRead, acting []int32, primary int32) {
		if len(read.Objects) != len(followed) || read.PGID == "" {
			fail("%s: the placements of the %d followed objects are missing: %s", name, len(followed), text(read.Objects))
			return
		}
		for i, o := range read.Objects {
			if want := followed[i]; o.Object != want || o.Pool != pool || o.PoolID != poolID || o.PGID != read.PGID ||
				!slices.Equal(o.Acting, acting) || !slices.Equal(o.Up, acting) || o.ActingPrimary != primary || o.Epoch == 0 || o.Epoch > read.Epoch {
				fail("%s: %s is not in the PG on acting set %v led by osd.%d: %s", name, want, acting, primary, text(o))
			}
		}
	}
	// queried checks one read's PG query on acting led by primary: the peers
	// are the two other members, and on the erasure-coded PG every member is
	// named with its acting index.
	queried := func(name string, read *nativeRBDJournalHeldRead, state string, acting []int32, primary int32) *nativeRBDJournalPGQuery {
		q := read.Query
		if q == nil {
			fail("%s: the PG query is missing", name)
			return nil
		}
		if problem := rbdJournalHeldQueryProblem(q, state, acting); problem != "" {
			fail("%s: the PG query %s: %s", name, problem, text(q))
			return nil
		}
		atIndex := func(s *int32, osd int32) bool {
			index := slices.Index(acting, osd)
			return s != nil && index >= 0 && *s == int32(index)
		}
		if erasure && !atIndex(q.Shard, primary) || !erasure && q.Shard != nil {
			fail("%s: the PG query's primary shard is not osd.%d's acting index: %s", name, primary, text(q.Shard))
		}
		peers := []int32{}
		for _, p := range q.Peers {
			peers = append(peers, p.Peer)
			if state == "active+clean" && p.LastUpdate != q.LastUpdate {
				fail("%s: peer osd.%d is at %s, the primary at %s", name, p.Peer, p.LastUpdate, q.LastUpdate)
			}
			if erasure && !atIndex(p.Shard, p.Peer) || !erasure && p.Shard != nil {
				fail("%s: peer osd.%d is named with shard %s, not its acting index", name, p.Peer, text(p.Shard))
			}
		}
		if want := slices.DeleteFunc(members(acting), func(o int32) bool { return o == primary }); !slices.Equal(members(peers), want) {
			fail("%s: the PG query lists peers %v, want %v", name, peers, want)
		}
		return q
	}
	subjectOps := func(ops []nativeRBDJournalHeldOp) []nativeRBDJournalHeldOp {
		var mine []nativeRBDJournalHeldOp
		for _, o := range ops {
			if o.Client == client {
				mine = append(mine, o)
			}
		}
		return mine
	}
	g := r.Gate
	if g == nil {
		fail("transmission: gate state missing")
		return problems
	}

	// Held: before the append, the PG on three members led by P, clean; on
	// the erasure-coded pool P is the header's primary.
	pl := h.Placed
	if pl == nil || len(pl.Objects) != len(followed) {
		fail("observe-placed: the fixture's placement is missing: %s", text(pl))
		return problems
	}
	acting := pl.Objects[0].Acting
	if len(acting) != 3 || len(slices.Compact(members(acting))) != 3 || h.Primary < 0 || !slices.Contains(acting, h.Primary) || !slices.Equal(h.GoActing, acting) ||
		!erasure && h.Primary != acting[0] {
		fail("observe-placed: the PG is not on three distinct members led by osd.%d as the subject placed it (%v): %v", h.Primary, h.GoActing, acting)
		return problems
	}
	if erasure {
		rbdJournalFencedAlignClaims(r, pl, acting, fail)
	} else if f.Align != nil {
		fail("align-primary: the replicated cell aligned a primary: %s", text(f.Align))
	}
	placedOn("observe-placed", pl, acting, h.Primary)
	placedQuery := queried("observe-placed", pl, "active+clean", acting, h.Primary)
	if len(pl.Down) != 0 || !pl.Settled || pl.OSD != -1 || len(pl.Ops) != 0 {
		fail("observe-placed: an OSD is down or operations were read: %s", text(pl))
	}
	lockRecord("observe-locked", locked, true)
	unmoved("observe-locked", locked)
	if locked.EntrySHA256 != nil || locked.ObjectSizes[e.Object] != uint64(e.Offset) || !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) {
		fail("observe-locked: journal objects changed before the append, or the entry offset is not the object size")
	}
	if p := h.PingBefore; p == nil || p.Class != "applied" || p.Code != 0 || p.Invocations != 1 {
		fail("ping: the subject's watch was not renewed before the fault: %s", text(p))
	}
	hd := h.Held
	if hd == nil || len(hd.Objects) != len(followed) {
		fail("observe-held: the fixture's read is missing: %s", text(hd))
		return problems
	}
	if h.SentAtEpoch == 0 || h.SentAtEpoch < pl.Objects[dataObject].Epoch || h.SentAtEpoch > hd.Objects[dataObject].Epoch || h.NamedEpoch != h.SentAtEpoch {
		fail("append: the subject placed the append at epoch %d (named %d), outside the fixture's reads around it", h.SentAtEpoch, h.NamedEpoch)
	}
	paused := acting[2]
	if paused == h.Primary {
		paused = acting[1]
	}
	if h.Replica != paused || h.Lease == nil || h.Lease.Daemon != fmt.Sprintf("osd.%d", h.Replica) || h.Lease.Deadline == 0 {
		fail("pause: the last acting member other than the primary, osd.%d, was not the one paused under a lease: osd.%d, lease %s", paused, h.Replica, text(h.Lease))
	}
	placedOn("observe-held", hd, acting, h.Primary)
	if q := queried("observe-held", hd, "active+clean", acting, h.Primary); q != nil && placedQuery != nil && q.SameIntervalSince != placedQuery.SameIntervalSince {
		fail("observe-held: the PG's interval changed while the append was held (%d, before %d)", q.SameIntervalSince, placedQuery.SameIntervalSince)
	}
	if len(hd.Down) != 0 {
		fail("observe-held: OSDs %v are down", hd.Down)
	}
	held := subjectOps(hd.Ops)
	if hd.OSD != h.Primary || !hd.Settled || len(held) != 1 {
		fail("observe-held: the primary osd.%d does not list exactly one operation of the subject: %s", h.Primary, text(hd))
		return problems
	}
	op := held[0]
	if !strings.Contains(op.Description, ":::"+e.ObjectName+":head [call journal.append") || !strings.HasSuffix(op.Description, " e"+strconv.FormatUint(uint64(h.NamedEpoch), 10)+")") ||
		op.Transaction == 0 {
		fail("observe-held: the subject's operation is not the append to %s at epoch %d: %s", e.ObjectName, h.NamedEpoch, text(op))
	}
	if erasure {
		if op.FlagPoint != "started" || !rbdJournalHeldEvents(op.Events, append(slices.Clone(rbdJournalHeldInPG), "sub_op_started"), "sub_op_committed") {
			fail("observe-held: the append is not waiting for the other shards with only P's shard committed on the erasure-coded primary: %s", text(op))
		}
	} else {
		others := slices.DeleteFunc(members(acting), func(o int32) bool { return o == h.Primary })
		waiting := fmt.Sprintf("waiting for subops from %d,%d", others[0], others[1])
		if op.FlagPoint != rbdJournalHeldSubOpsFlag || !rbdJournalHeldEvents(op.Events, append(slices.Clone(rbdJournalHeldInPG), waiting), "op_commit", "sub_op_commit_rec") {
			fail("observe-held: the append is not waiting for one of its two replica commits on the primary: %s", text(op))
		}
	}
	if !h.PendingAfterHeld {
		fail("observe-held: the caller was not pending after the held observation")
	}
	if g.Bound || g.LargeWrites != 1 || g.Error != "" {
		fail("transmission: not one write of the append size, or the gate withheld a reply: %s", text(g))
	}
	if reached < 1 {
		return problems
	}

	rbdJournalFencedFenceClaims(r, pl.PGID, hd.Epoch, held, fail)
	if reached < 2 {
		return problems
	}

	// Outcome: one known answer 0 on disk to the listed transaction, after
	// the fence.
	o := r.Outcome
	if o == nil {
		fail("outcome: missing")
		return problems
	}
	if o.CallerInvocations != 1 || o.Submission.Transaction != op.Transaction || o.Submission.GlobalID != r.SubjectGlobalID || o.Submission.MapEpoch != h.NamedEpoch || o.RecordedUTC == "" ||
		o.Class != "applied" || o.UnknownOutcome || o.Canceled || o.ReplyCode != 0 || o.ReplyTransaction != o.Submission.Transaction || o.Error != "" ||
		o.OperationCode != nil && *o.OperationCode != 0 {
		fail("outcome: not one known answer 0 to the held transaction %d: %s", op.Transaction, text(o))
	}
	l := h.Lease
	submitted, err := time.Parse(time.RFC3339Nano, h.SubmittedUTC)
	recorded, recordedErr := time.Parse(time.RFC3339Nano, o.RecordedUTC)
	if l == nil || l.Reason != "resumed" || l.LeftMS <= 0 || l.PausedAt == 0 || l.EndedAt <= l.PausedAt || l.EndedAt > l.Deadline || err != nil || recordedErr != nil ||
		l.PausedAt >= submitted.UnixNano() || l.EndedAt >= recorded.UnixNano() {
		fail("replica_resumed: the member was not paused before the append and resumed by the test before its deadline and the answer: %s", text(l))
	}
	st := h.Settled
	if st == nil || len(st.Objects) != len(followed) {
		fail("observe-settled: the fixture's read is missing")
		return problems
	}
	placedOn("observe-settled", st, acting, h.Primary)
	if q := queried("observe-settled", st, "active+clean", acting, h.Primary); q != nil && placedQuery != nil &&
		(q.SameIntervalSince != placedQuery.SameIntervalSince || q.SameUpSince != placedQuery.SameUpSince || q.SamePrimarySince != placedQuery.SamePrimarySince ||
			q.LastUpdate == placedQuery.LastUpdate) {
		fail("observe-settled: the PG changed interval, or its last update did not move: %s, before %s", text(q), text(placedQuery))
	}
	if len(st.Down) != 0 {
		fail("observe-settled: OSDs %v are down", st.Down)
	}
	// Journal: the entry once; the lock record still the subject's, its
	// watch dropped, its address blocklisted.
	entryAt("observe-fenced", fenced)
	lockRecord("observe-fenced", fenced, false)
	unmoved("observe-fenced", fenced)
	if !rbdJournalBlocked(fenced.Blocklist, f.LockerAddress) {
		fail("observe-fenced: the lock record's address is not blocklisted: %s", text(fenced.Blocklist))
	}
	if reached < 3 {
		return problems
	}

	// Transmission: the subject's first connection took the lock and
	// carried the append and the stat. On the erasure-coded pool under an
	// override the subject read the journal object before the alignment
	// from its CRUSH primary, on a second connection that carried no write
	// of the append size.
	want := 1
	if erasure && f.Align != nil && len(f.Align.Commands) != 0 {
		want = 2
	}
	if r.DialsTotal == nil || *r.DialsTotal != want || g.Connections != want || h.AppendConnection != 1 || g.LargeWrites != 1 {
		fail("transmission: not %d subject connections with the append on the lock's and one write of the append size: gate %s, dials %s, append on %d", want, text(g), text(r.DialsTotal),
			h.AppendConnection)
	}
	// Journal: native librbd's replay of the entry.
	event := rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.ImageOffset, Length: e.ImageLength,
		Timestamp: time.Unix(int64(e.Timestamp.Seconds), int64(e.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
	if replay.InspectTotal == nil || replay.InspectErrors == nil || setup.InspectTotal == nil || *replay.InspectTotal != *setup.InspectTotal+1 || *replay.InspectErrors != 0 ||
		len(replay.InspectOwned) != 1 || replay.InspectOwned[0] != event || !slices.Equal(replay.InspectOther, setup.InspectOther) {
		fail("replay: rbd journal inspect does not replay the setup entries and the owned entry once (%v)", replay.InspectOwned)
	}
	entryAt("replay", replay)
	if !replay.LockFree || replay.SubjectWatcher {
		fail("replay: the lock is held or the subject's watch is listed after the replay")
	}
	if replay.ImageHeadSHA256 != r.ExpectedHeadAfter || r.ExpectedHeadAfter == r.ExpectedHeadBefore || len(r.ExpectedCommitAfter) == 0 ||
		r.ExpectedCommitAfter[0] != (rbdJournalPosition{e.Object, e.Tag, e.EntryTID}) || !reflect.DeepEqual(replay.ImageCommit, r.ExpectedCommitAfter) {
		fail("replay: image or image client commit position differs from one native replay of the owned entry (head=%s commit=%v)", replay.ImageHeadSHA256, replay.ImageCommit)
	}
	if replay.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, e.EntryTID}) || replay.NextTag != e.Tag+2 {
		fail("replay: native tag differs (tag=%+v next=%d)", replay.LastTag, replay.NextTag)
	}
	return problems
}

// rbdJournalFencedAlignClaims checks the align-primary receipt of a fenced
// cell on the erasure-coded journal pool: the journal PG on acting, placed
// as pl names it, is led by the header's primary P afterwards with every
// shard in its slot, by one primary override exactly when the CRUSH primary
// was another member, and is clean again.
func rbdJournalFencedAlignClaims(r *rbdJournalLostAppendRecord, pl *nativeRBDJournalHeldRead, acting []int32, fail func(string, ...any)) {
	h, e, text := r.Held, r.Entry, rbdJournalHeldText
	a := h.Fenced.Align
	target := h.Primary
	if a == nil || h.HeaderPrimary != target || a.Header.ActingPrimary != target || a.Header.Object != "rbd_header."+r.ImageID || a.Header.Pool != r.Pool ||
		a.Before.Object != e.ObjectName || a.Before.PGID != pl.PGID || !slices.Equal(a.Before.Acting, acting) || a.Before.ActingPrimary != acting[0] ||
		a.After.PGID != pl.PGID || !slices.Equal(a.After.Acting, acting) || !slices.Equal(a.After.Up, acting) || a.After.ActingPrimary != target || a.After.Epoch < a.Before.Epoch {
		fail("align-primary: the erasure-coded journal PG is not led by the header's primary osd.%d with every shard in its slot: %s", target, text(a))
		return
	}
	want := []string{}
	if a.Before.ActingPrimary != target {
		want = append(want, fmt.Sprintf("osd primary-temp %s %d", pl.PGID, target))
	}
	if !slices.Equal(a.Commands, want) || a.Query == nil || a.Query.State != "active+clean" || !slices.Equal(a.Query.Acting, acting) {
		fail("align-primary: not %v with the PG clean afterwards: %s", want, text(a))
	}
}

// rbdJournalFencedFenceClaims checks the fence of a fenced cell and the stat
// that probed it: the lock record's address goes into the MON's blocklist
// with the one command, P's PG pgid reaches a map with it after the held
// read's epoch, P still lists every held operation of the subject unchanged
// and no other, every caller is still pending, and P refuses the subject's
// next request on the same connection once with -108.
func rbdJournalFencedFenceClaims(r *rbdJournalLostAppendRecord, pgid string, heldEpoch uint32, held []nativeRBDJournalHeldOp, fail func(string, ...any)) {
	h, f, text := r.Held, r.Held.Fenced, rbdJournalHeldText
	client := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	blocked := func(blocks []nativeRBDBlock, address string) bool {
		return slices.ContainsFunc(blocks, func(b nativeRBDBlock) bool { return !b.Range && b.Address == address })
	}
	fc := f.Fence
	if fc == nil {
		fail("fence: the fixture's receipt is missing")
		return
	}
	if fc.Address != f.LockerAddress || fc.OSD != h.Primary || fc.PGID != pgid || !slices.Equal(fc.Commands, []string{"osd blocklist add " + f.LockerAddress}) {
		fail("fence: not one blocklist entry of the lock record's address %s for the PG on osd.%d: %s", f.LockerAddress, h.Primary, text(fc))
	}
	if blocked(fc.BlocklistBefore, f.LockerAddress) || !blocked(fc.Blocklist, f.LockerAddress) {
		fail("fence: the MON did not list the address only after the fence: before %s, after %s", text(fc.BlocklistBefore), text(fc.Blocklist))
	}
	if fc.Epoch <= heldEpoch || fc.PGEpoch < fc.Epoch || fc.PGState != "active+clean" || len(fc.Down) != 0 || fc.Polls < 1 {
		fail("fence: the PG did not reach the OSDMap epoch after the fence, clean with every OSD up (held at %d): %s", heldEpoch, text(fc))
	}
	var still []nativeRBDJournalHeldOp
	for _, o := range fc.Ops {
		if o.Client == client {
			still = append(still, o)
		}
	}
	same := len(still) == len(held) && len(held) != 0
	for _, op := range held {
		same = same && slices.ContainsFunc(still, func(o nativeRBDJournalHeldOp) bool {
			return o.OSD == h.Primary && o.Transaction == op.Transaction && o.Description == op.Description && o.FlagPoint == op.FlagPoint
		})
	}
	if !same {
		fail("fence: the primary no longer lists the %d held appends unchanged once its PG had the fence: %s", len(held), text(still))
	}
	if !f.PendingAfterFence || !f.PendingAfterRefusal {
		fail("fence: an append's caller was answered during the fence or the probe (after fence %v, after probe %v)", f.PendingAfterFence, f.PendingAfterRefusal)
	}
	x := f.Refusal
	if x == nil || x.Label != "stat_fenced" || x.Invocations != 1 || x.Class != "refused" || x.Code != rbdOSDBlocklisted || x.ErrorCode != rbdOSDBlocklisted || x.UnknownOutcome ||
		x.Canceled || x.Submission.GlobalID != r.SubjectGlobalID || x.Submission.Transaction == 0 ||
		slices.ContainsFunc(held, func(o nativeRBDJournalHeldOp) bool { return o.Transaction == x.Submission.Transaction }) ||
		x.ReplyTransaction != x.Submission.Transaction || x.ReplyMapEpoch < fc.Epoch {
		fail("fence: the subject's stat on the appends' connection was not refused once with %d at the fenced epoch %d or later: %s", rbdOSDBlocklisted, fc.Epoch, text(x))
	}
}
