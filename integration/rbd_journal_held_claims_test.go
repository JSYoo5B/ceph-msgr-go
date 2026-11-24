package integration_test

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// rbdJournalHeldKilledExit is the exit status of an OSD the fixture killed
// with SIGKILL.
const rbdJournalHeldKilledExit = 137

// The op tracker's flag points and events of a held append on its primary
// (osd/OpRequest.h and the mark_* calls of osd/OSD.cc, osd/PrimaryLogPG.cc
// and osd/ReplicatedBackend.cc at ceph/ceph/blob/v20.2.4/src/): the steps
// before the op reaches its PG, the PG's steps up to the sub-writes, and the
// flag points of an op that reached none and of one waiting for sub-writes.
var (
	rbdJournalHeldPrePG = []string{"initiated", "header_read", "throttled", "all_read", "dispatched"}
	rbdJournalHeldInPG  = []string{"queued_for_pg", "reached_pg", "started"}
)

const (
	rbdJournalHeldNoFlag     = "no flag points reached"
	rbdJournalHeldSubOpsFlag = "waiting for sub ops"
)

// rbdJournalHeldMapDelays are the marks PrimaryLogPG::do_request records
// when it parks an op that reached its PG before the PG had the op's map
// epoch ("op must wait for map"), or behind another such op of the same
// client ("waiting_for_map not empty"); once the PG has the map the op
// reaches it again (osd/PrimaryLogPG.cc at ceph/ceph/blob/v20.2.4/src/).
// The OSD's map may lead its PG's within one interval, so a held op can
// carry such a pair right after its first reached_pg.
var rbdJournalHeldMapDelays = []string{"op must wait for map", "waiting_for_map not empty"}

// rbdJournalHeldMapDelayLimit bounds the pairs one op may carry.
const rbdJournalHeldMapDelayLimit = 4

// rbdJournalHeldEvents says whether events are exactly the pre-PG steps, then
// the steps of then in order, then each of last once in any order. Each pair
// of a map delay and the following reached_pg directly after a reached_pg
// is the PG catching up with the op's map and is set aside, at most
// rbdJournalHeldMapDelayLimit of them.
func rbdJournalHeldEvents(events, then []string, last ...string) bool {
	var kept []string
	delays := 0
	for i := 0; i < len(events); i++ {
		if i > 0 && i+1 < len(events) && events[i-1] == "reached_pg" && events[i+1] == "reached_pg" && slices.Contains(rbdJournalHeldMapDelays, events[i]) {
			delays++
			i++
			continue
		}
		kept = append(kept, events[i])
	}
	if delays > rbdJournalHeldMapDelayLimit {
		return false
	}
	events = kept
	head := append(slices.Clone(rbdJournalHeldPrePG), then...)
	if len(events) != len(head)+len(last) || !slices.Equal(events[:len(head)], head) {
		return false
	}
	tail := slices.Sorted(slices.Values(events[len(head):]))
	return slices.Equal(tail, slices.Sorted(slices.Values(last)))
}

// rbdJournalHeldQueryProblem names the first field of a PG query that is not
// state on acting set acting with a history consistent with the query's own
// epoch, or returns "".
func rbdJournalHeldQueryProblem(q *nativeRBDJournalPGQuery, state string, acting []int32) string {
	switch {
	case q.State != state:
		return fmt.Sprintf("state is %q, want %q", q.State, state)
	case !slices.Equal(q.Acting, acting):
		return fmt.Sprintf("acting set is %v, want %v", q.Acting, acting)
	case !slices.Equal(q.Up, acting):
		return fmt.Sprintf("up set is %v, want %v", q.Up, acting)
	case q.SameIntervalSince == 0 || q.SameIntervalSince > q.Epoch:
		return fmt.Sprintf("same_interval_since %d is not within its epoch %d", q.SameIntervalSince, q.Epoch)
	case q.SamePrimarySince > q.SameIntervalSince:
		return fmt.Sprintf("same_primary_since %d is after same_interval_since %d", q.SamePrimarySince, q.SameIntervalSince)
	case q.LastUpdate == "":
		return "names no last update"
	}
	return ""
}

// rbdJournalHeldClaims checks a held-request record up to one stage. It
// keeps apart what the OSDs listed while the caller was pending (held, from
// "observe-held" on), what the caller was told and how often it asked
// (outcome), what the PG's interval history says around the request
// (interval), and what native clients found afterwards up to native
// librbd's replay (journal).
func rbdJournalHeldClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := rbdJournalHeldText
	h, e := r.Held, r.Entry
	if rbdJournalHeldPendingMode(h.Mode) {
		return rbdJournalHeldPendingClaims(r, stage)
	}
	if h.Pending != nil {
		return []string{"record: a single-append held record carries pending appends"}
	}
	stages, order := rbdJournalHeldPlan(h.Mode)
	reached := slices.Index(stages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if r.PrimaryChange != nil || r.OwnerLoss != nil || r.Trim != nil || r.Inflight != nil || r.ForcedPending != nil {
		return []string{"record: a held-request record carries another variant's part"}
	}
	replica, change, dead := h.Mode == rbdJournalVariantReplicaHold, h.Mode == rbdJournalVariantHeldPrimaryChange, h.Mode == rbdJournalVariantDeadPrimary
	// inventory is the setup observation's journal object count: every
	// later observation must list exactly as many objects, each with its
	// size, digest and existence, before any two of them are compared.
	inventory := -1
	view := func(name string, at int) *rbdJournalLostAppendView {
		if reached < at {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || int(e.Object) >= len(v.ObjectSizes) || len(v.ObjectSHA256) != len(v.ObjectSizes) || len(v.ObjectExists) != len(v.ObjectSizes) ||
			inventory >= 0 && len(v.ObjectSizes) != inventory {
			fail("%s: native observation missing or incomplete (%d journal objects, the setup listed %d)", name, len(v.ObjectSizes), inventory)
			return nil
		}
		if inventory < 0 {
			inventory = len(v.ObjectSizes)
		}
		return v
	}
	setup, locked := view("setup", 0), view("observe-locked", 0)
	var answered, moved, recorded, replay *rbdJournalLostAppendView
	switch {
	case replica:
		answered, replay = view("observe-answered", 1), view("replay", 2)
	case change:
		moved, recorded, replay = view("change-primary", 1), view("observe-recorded", 2), view("replay", 3)
	case dead:
		recorded, replay = view("observe-recorded", 1), view("replay", 2)
	}
	if len(problems) != 0 {
		return problems
	}
	if want := order[:slices.Index(order, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Length <= 0 || len(e.SHA256) != 64 || e.Tag == 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 {
		fail("identity: entry or client identity incomplete")
	}
	owner := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	client := owner
	lockRecord := func(name string, v *rbdJournalLostAppendView, watch bool) {
		if v.LockFree || v.LockOwner != owner || v.LockCookie != r.LockCookie || watch && !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
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
	absent := func(name string, v *rbdJournalLostAppendView) {
		if !slices.Equal(v.ObjectSHA256, locked.ObjectSHA256) || !slices.Equal(v.ObjectSizes, locked.ObjectSizes) || v.EntrySHA256 != nil {
			fail("%s: the journal objects differ from the observation before the append (sizes %v, want %v)", name, v.ObjectSizes, locked.ObjectSizes)
		}
	}
	members := func(osds []int32) []int32 { sorted := slices.Clone(osds); slices.Sort(sorted); return sorted }
	// placedOn checks one read's placements: the three followed objects in
	// one PG of the owned pool on the acting set acting led by primary.
	placedOn := func(name string, read *nativeRBDJournalHeldRead, acting []int32, primary int32) {
		if len(read.Objects) != 3 || read.PGID == "" {
			fail("%s: the placements of the three followed objects are missing: %s", name, text(read.Objects))
			return
		}
		for i, o := range read.Objects {
			if want := []string{"rbd_header." + r.ImageID, e.ObjectName, r.JournalHeader}[i]; o.Object != want || o.Pool != r.Pool || o.PoolID != r.PoolID || o.PGID != read.PGID ||
				!slices.Equal(o.Acting, acting) || o.ActingPrimary != primary || o.Epoch == 0 || o.Epoch > read.Epoch {
				fail("%s: %s is not in the PG on acting set %v led by osd.%d: %s", name, want, acting, primary, text(o))
			}
		}
	}
	// queried checks one read's PG query: the state, the up and acting sets,
	// a history consistent with the query's own epoch and, when clean, every
	// peer at the primary's last update. The query's epoch is the PG's own
	// map epoch on its primary, which may lag (or lead) the OSDMap epochs of
	// the reads around it, so it is not compared with them; the interval
	// claims compare the history instead.
	queried := func(name string, read *nativeRBDJournalHeldRead, state string, acting []int32) *nativeRBDJournalPGQuery {
		q := read.Query
		if q == nil {
			fail("%s: the PG query is missing", name)
			return nil
		}
		if problem := rbdJournalHeldQueryProblem(q, state, acting); problem != "" {
			fail("%s: the PG query %s: %s", name, problem, text(q))
			return nil
		}
		peers := []int32{}
		for _, p := range q.Peers {
			peers = append(peers, p.Peer)
			if state == "active+clean" && p.LastUpdate != q.LastUpdate {
				fail("%s: peer osd.%d is at %s, the primary at %s", name, p.Peer, p.LastUpdate, q.LastUpdate)
			}
		}
		if want := slices.DeleteFunc(members(acting), func(o int32) bool { return o == acting[0] }); !slices.Equal(members(peers), want) {
			fail("%s: the PG query lists peers %v, want %v", name, peers, want)
		}
		return q
	}
	// subjectOps are the subject's records among one read's operations.
	subjectOps := func(read *nativeRBDJournalHeldRead) []nativeRBDJournalHeldOp {
		var ops []nativeRBDJournalHeldOp
		for _, o := range read.Ops {
			if o.Client == client {
				ops = append(ops, o)
			}
		}
		return ops
	}
	g := r.Gate
	if g == nil {
		fail("transmission: gate state missing")
		return problems
	}

	// Held: before the append, the PG on three members led by P, clean.
	pl := h.Placed
	if pl == nil || len(pl.Objects) != 3 {
		fail("observe-placed: the fixture's placement is missing: %s", text(pl))
		return problems
	}
	acting := pl.Objects[0].Acting
	if len(acting) != 3 || len(slices.Compact(members(acting))) != 3 || h.Primary != acting[0] || h.Primary < 0 || !slices.Equal(h.GoActing, acting) {
		fail("observe-placed: the PG is not on three distinct members led by osd.%d as the subject placed it (%v): %v", h.Primary, h.GoActing, acting)
		return problems
	}
	placedOn("observe-placed", pl, acting, h.Primary)
	placedQuery := queried("observe-placed", pl, "active+clean", acting)
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
	// The OSDMap may gain an epoch between the fixture's read and the
	// subject's own placement; the PG's interval may not (below).
	if hd := h.Held; h.SentAtEpoch == 0 || h.SentAtEpoch < pl.Objects[1].Epoch || hd != nil && h.SentAtEpoch > hd.Objects[1].Epoch {
		fail("append: the subject placed the append at epoch %d, outside the fixture's reads around it", h.SentAtEpoch)
	}
	wantNamed := h.SentAtEpoch
	if change {
		wantNamed += rbdJournalForcedPendingEpochs
	}
	if h.NamedEpoch != wantNamed {
		fail("append: the append names epoch %d, want %d", h.NamedEpoch, wantNamed)
	}
	if replica || dead {
		if h.Replica != acting[2] || h.Lease == nil || h.Lease.Daemon != fmt.Sprintf("osd.%d", h.Replica) || h.Lease.Deadline == 0 {
			fail("pause: the last acting member osd.%d was not the one paused under a lease: replica osd.%d, lease %s", acting[2], h.Replica, text(h.Lease))
		}
	} else if h.Replica != -1 || h.Lease != nil {
		fail("pause: an OSD was paused in the held primary-change mode: %s", text(h.Lease))
	}
	hd := h.Held
	if hd == nil || len(hd.Objects) != 3 {
		fail("observe-held: the fixture's read is missing: %s", text(hd))
		return problems
	}
	// The interval is the one the lock was taken in: a replica paused within
	// the heartbeat grace is still up and the PG still clean.
	placedOn("observe-held", hd, acting, h.Primary)
	if q := queried("observe-held", hd, "active+clean", acting); q != nil && placedQuery != nil && q.SameIntervalSince != placedQuery.SameIntervalSince {
		fail("observe-held: the PG's interval changed while the append was held (%d, before %d)", q.SameIntervalSince, placedQuery.SameIntervalSince)
	}
	if len(hd.Down) != 0 {
		fail("observe-held: OSDs %v are down", hd.Down)
	}
	held := subjectOps(hd)
	if hd.OSD != h.Primary || !hd.Settled || len(held) != 1 {
		fail("observe-held: the primary osd.%d does not list exactly one operation of the subject: %s", h.Primary, text(hd))
		return problems
	}
	op := held[0]
	if !strings.Contains(op.Description, ":::"+e.ObjectName+":head [call journal.append") || !strings.HasSuffix(op.Description, " e"+strconv.FormatUint(uint64(h.NamedEpoch), 10)+")") ||
		op.Transaction == 0 {
		fail("observe-held: the subject's operation is not the append to %s at epoch %d: %s", e.ObjectName, h.NamedEpoch, text(op))
	}
	replicas := slices.DeleteFunc(members(acting), func(o int32) bool { return o == h.Primary })
	waiting := fmt.Sprintf("waiting for subops from %d,%d", replicas[0], replicas[1])
	if replica || dead {
		// Applied on P and sent to both replicas; exactly one replica's
		// commit recorded; never answered: the op tracker's flag point is the
		// replica wait, and its events are the pre-PG steps, the PG's own
		// steps up to the sub-writes, then the local commit and one replica
		// commit in either order, nothing else.
		if op.FlagPoint != rbdJournalHeldSubOpsFlag || !rbdJournalHeldEvents(op.Events, append(slices.Clone(rbdJournalHeldInPG), waiting), "op_commit", "sub_op_commit_rec") {
			fail("observe-held: the append is not waiting for one of its two replica commits on the primary: %s", text(op))
		}
	} else if op.FlagPoint != rbdJournalHeldNoFlag || !rbdJournalHeldEvents(op.Events, nil) {
		// Held for its map: no flag point and no event past dispatch, so the
		// op has not reached its PG.
		fail("observe-held: the append held for its map has progressed into its PG: %s", text(op))
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

	// outcome returns what the caller was told about its one invocation,
	// which must be the transaction the primary listed.
	outcome := func() *rbdJournalLostAppendOutcome {
		o := r.Outcome
		if o == nil {
			fail("outcome: missing")
			return &rbdJournalLostAppendOutcome{}
		}
		if o.CallerInvocations != 1 || o.Submission.Transaction == 0 || o.Submission.GlobalID != r.SubjectGlobalID || o.Submission.Transaction != op.Transaction ||
			o.Submission.MapEpoch != h.NamedEpoch || o.RecordedUTC == "" {
			fail("outcome: not one invocation submitted as the transaction %d the primary listed: %s", op.Transaction, text(o))
		}
		return o
	}
	lease := func(name string) {
		l := h.Lease
		submitted, err := time.Parse(time.RFC3339Nano, h.SubmittedUTC)
		if l == nil || l.Reason != "resumed" || l.LeftMS <= 0 || l.PausedAt == 0 || l.EndedAt <= l.PausedAt || l.EndedAt > l.Deadline || err != nil ||
			l.PausedAt >= submitted.UnixNano() {
			fail("%s: the replica was not paused before the append was sent and resumed by the test before its deadline: %s", name, text(l))
		}
	}

	switch {
	case replica:
		// Outcome: one known answer 0 on disk to the listed transaction.
		o := outcome()
		if o.Class != "applied" || o.UnknownOutcome || o.Canceled || o.ReplyCode != 0 || o.ReplyTransaction != o.Submission.Transaction || o.Error != "" {
			fail("outcome: not one known answer 0 to the held transaction: %s", text(o))
		}
		lease("replica_resumed")
		// Interval: unchanged across the request, every peer at the
		// primary's new last update.
		st := h.Settled
		if st == nil || len(st.Objects) != 3 {
			fail("observe-settled: the fixture's read is missing")
			return problems
		}
		placedOn("observe-settled", st, acting, h.Primary)
		if q := queried("observe-settled", st, "active+clean", acting); q != nil && placedQuery != nil &&
			(q.SameIntervalSince != placedQuery.SameIntervalSince || q.SameUpSince != placedQuery.SameUpSince || q.SamePrimarySince != placedQuery.SamePrimarySince ||
				q.LastUpdate == placedQuery.LastUpdate) {
			fail("observe-settled: the PG changed interval, or its last update did not move: %s, before %s", text(q), text(placedQuery))
		}
		if len(st.Down) != 0 {
			fail("observe-settled: OSDs %v are down", st.Down)
		}
		entryAt("observe-answered", answered)
		lockRecord("observe-answered", answered, true)
		unmoved("observe-answered", answered)
		if reached < 2 {
			return problems
		}
	case dead:
		// Outcome: the call ended with the loss of P's connection, unknown.
		o := outcome()
		if k := h.Kill; k == nil || k.Daemon != fmt.Sprintf("osd.%d", h.Primary) || k.ExitCode != rbdJournalHeldKilledExit || k.OOMKilled {
			fail("kill: the primary osd.%d was not killed with SIGKILL: %s", h.Primary, text(k))
		}
		if o.Class != "uncertain" || !o.UnknownOutcome || o.Canceled || o.ReplyTransaction != 0 || o.ReplyCode != 0 {
			fail("outcome: not one unknown outcome of a call whose connection was lost: %s", text(o))
		}
		lease("replica_resumed")
		if l := h.Lease; l != nil {
			if recordedAt, err := time.Parse(time.RFC3339Nano, o.RecordedUTC); err != nil || l.EndedAt <= recordedAt.UnixNano() {
				fail("replica_resumed: the replica was resumed before the caller's outcome was recorded")
			}
		}
		// Interval: P down, the PG active again in a newer interval led by
		// one of the two others, without P.
		lost := h.Lost
		if lost == nil || len(lost.Objects) != 3 || lost.Query == nil {
			fail("observe-primary-lost: the fixture's read is missing: %s", text(lost))
			return problems
		}
		after := lost.Objects[0].Acting
		if !lost.Settled || !slices.Contains(lost.Down, h.Primary) || !slices.Equal(members(after), replicas) {
			fail("observe-primary-lost: osd.%d is not down with the PG on the two others: %s", h.Primary, text(lost))
		} else {
			placedOn("observe-primary-lost", lost, after, after[0])
			q := lost.Query
			if !strings.HasPrefix(q.State, "active") || !slices.Equal(q.Acting, after) || placedQuery != nil && (q.SameIntervalSince <= placedQuery.SameIntervalSince ||
				q.SamePrimarySince <= placedQuery.SamePrimarySince) || q.SameIntervalSince > q.Epoch {
				fail("observe-primary-lost: the PG is not active in a newer interval with a new primary: %s, before %s", text(q), text(placedQuery))
			}
		}
		// Journal: the entry survived on the member that committed it.
		entryAt("observe-recorded", recorded)
		lockRecord("observe-recorded", recorded, false)
		unmoved("observe-recorded", recorded)
		if reached < 2 {
			return problems
		}
	case change:
		n := h.PrimaryChange
		if n == nil || len(n.Objects) != 3 || n.Out != h.Primary || !slices.Equal(n.Commands, []string{fmt.Sprintf("osd out %d", h.Primary)}) {
			fail("change-primary: not one `osd out` of the primary osd.%d: %s", h.Primary, text(n))
			return problems
		}
		for _, m := range n.Objects {
			a := m.After
			rest := slices.DeleteFunc(members(m.Before.Acting), func(osd int32) bool { return osd == n.Out })
			if !m.Moved || m.Before.ActingPrimary != h.Primary || a.PGID != pl.PGID || len(a.Up) != 2 || !slices.Equal(members(a.Up), rest) ||
				!slices.Equal(a.Acting, append(slices.Clone(a.Up), n.Out)) || a.ActingPrimary != a.Up[0] || a.Epoch >= h.NamedEpoch || m.Before.Epoch < h.SentAtEpoch {
				fail("change-primary: %s did not move to another member before the named epoch %d: %s", m.Object, h.NamedEpoch, text(m))
			}
		}
		if !h.PendingAfterChange {
			fail("change-primary: the caller was answered during the primary change")
		}
		absent("change-primary", moved)
		lockRecord("change-primary", moved, false)
		unmoved("change-primary", moved)
		if reached < 2 {
			return problems
		}
		// Held: P listed the append when the advance began and no longer once
		// it had the named epoch; the pool flag ends clear.
		a := h.Advance
		if a == nil {
			fail("advance-map: the fixture's receipt is missing")
			return problems
		}
		heldBy := func(ops []rbdLockOwnerLossOp) []rbdLockOwnerLossOp {
			var mine []rbdLockOwnerLossOp
			for _, o := range ops {
				if o.Client == client {
					mine = append(mine, o)
				}
			}
			return mine
		}
		before, after := heldBy(a.OpsBefore), heldBy(a.OpsAfter)
		if len(before) != 1 || before[0].OSD != int(h.Primary) || before[0].Transaction != op.Transaction || before[0].Description != op.Description || len(after) != 0 {
			fail("advance-map: the old primary osd.%d did not list the held append once before the advance and none after: before %s, after %s", h.Primary, text(before), text(after))
		}
		if a.Pool != r.Pool || a.EpochBefore <= h.SentAtEpoch || a.EpochBefore >= h.NamedEpoch || a.EpochAfter < h.NamedEpoch || a.EpochAfter < a.EpochTarget || a.FlagFinally ||
			len(a.Commands) == 0 || len(a.Commands) != len(a.Epochs) {
			fail("advance-map: the epoch did not go from after the change past the named epoch %d with the pool flag clear: %s", h.NamedEpoch, text(a))
		}
		for i, command := range a.Commands {
			if want := fmt.Sprintf("osd pool set %s %s %t", r.Pool, a.Flag, i%2 == 0); command != want || a.Flag != "nodeep-scrub" {
				fail("advance-map: command %d is %q, want %q", i, command, want)
			}
		}
		// Interval: the PG is led by the new primary in a newer interval.
		st := h.Settled
		newActing := n.Objects[0].After.Acting
		if st == nil || len(st.Objects) != 3 || st.Query == nil {
			fail("observe-settled: the fixture's read is missing: %s", text(st))
			return problems
		}
		placedOn("observe-settled", st, newActing, newActing[0])
		if q := st.Query; placedQuery != nil && (q.SameIntervalSince <= placedQuery.SameIntervalSince || q.SamePrimarySince <= placedQuery.SamePrimarySince ||
			!slices.Equal(q.Acting, newActing) || !strings.HasPrefix(q.State, "active")) || len(st.Down) != 0 {
			fail("observe-settled: the PG is not active in a newer interval led by the new primary: %s", text(st.Query))
		}
		// Outcome: no answer; the local wait was canceled.
		o := outcome()
		if o.Class != "uncertain" || !o.UnknownOutcome || !o.Canceled || o.ReplyTransaction != 0 || o.ReplyCode != 0 {
			fail("outcome: not one unknown outcome of a canceled wait: %s", text(o))
		}
		absent("observe-recorded", recorded)
		lockRecord("observe-recorded", recorded, false)
		unmoved("observe-recorded", recorded)
		if reached < 3 {
			return problems
		}
	}

	// The handoff after a change or death of the primary.
	if change || dead {
		x := h.Reconnect
		if x == nil || x.Class != "applied" || x.Code != 0 || x.Invocations != 1 || h.HandoffPrimary == h.Primary || h.LockerAddress == "" || h.WatcherAddress == "" {
			fail("handoff: the watch was not reattached on a new primary with the lock record read there: %s (primary osd.%d, lock %q, watch %q)", text(x), h.HandoffPrimary, h.LockerAddress, h.WatcherAddress)
		}
		if r.DialsBeforeClose == nil || *r.DialsBeforeClose != 1 || r.DialsTotal == nil || *r.DialsTotal != 2 || g.Connections != 2 || h.AppendConnection != 1 {
			fail("transmission: not one subject connection to the old primary and one to the new: gate %s, dials before close %s, in total %s", text(g), text(r.DialsBeforeClose), text(r.DialsTotal))
		}
	} else if r.DialsTotal == nil || *r.DialsTotal != 1 || g.Connections != 1 || h.AppendConnection != 1 {
		fail("transmission: not one subject connection: gate %s, dials %s", text(g), text(r.DialsTotal))
	}
	if g.LargeWrites != 1 {
		fail("transmission: %d writes of the append size; the append may have been sent again", g.LargeWrites)
	}

	// Journal: native librbd's replay.
	inspect := func(name string, v *rbdJournalLostAppendView, owned int) {
		want := rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.ImageOffset, Length: e.ImageLength,
			Timestamp: time.Unix(int64(e.Timestamp.Seconds), int64(e.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
		if v.InspectTotal == nil || v.InspectErrors == nil || setup.InspectTotal == nil || *v.InspectTotal != *setup.InspectTotal+owned || *v.InspectErrors != 0 ||
			len(v.InspectOwned) != owned || owned == 1 && v.InspectOwned[0] != want || !slices.Equal(v.InspectOther, setup.InspectOther) {
			fail("%s: rbd journal inspect does not replay the setup entries and %d owned entries (%v)", name, owned, v.InspectOwned)
		}
	}
	if !replay.LockFree || replay.SubjectWatcher {
		fail("replay: the lock is held or the subject's watch is listed after the replay")
	}
	if change {
		absent("replay", replay)
		inspect("replay", replay, 0)
		if replay.ImageHeadSHA256 != r.ExpectedHeadBefore || !reflect.DeepEqual(replay.ImageCommit, setup.ImageCommit) || replay.LastTag.TID != e.Tag+1 || replay.NextTag != e.Tag+2 {
			fail("replay: native librbd's replay moved the image or the image client's position, or allocated no tag (head=%s commit=%v tag=%+v)", replay.ImageHeadSHA256, replay.ImageCommit, replay.LastTag)
		}
		return problems
	}
	entryAt("replay", replay)
	inspect("replay", replay, 1)
	if replay.ImageHeadSHA256 != r.ExpectedHeadAfter || r.ExpectedHeadAfter == r.ExpectedHeadBefore || len(r.ExpectedCommitAfter) == 0 ||
		r.ExpectedCommitAfter[0] != (rbdJournalPosition{e.Object, e.Tag, e.EntryTID}) || !reflect.DeepEqual(replay.ImageCommit, r.ExpectedCommitAfter) {
		fail("replay: image or image client commit position differs from one native replay of the owned entry (head=%s commit=%v)", replay.ImageHeadSHA256, replay.ImageCommit)
	}
	if replay.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, e.EntryTID}) || replay.NextTag != e.Tag+2 {
		fail("replay: native tag differs (tag=%+v next=%d)", replay.LastTag, replay.NextTag)
	}
	return problems
}
