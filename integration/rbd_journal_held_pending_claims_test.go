package integration_test

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// rbdJournalHeldPendingClaims checks a pending-append record up to one
// stage. It keeps apart what the primary listed for each append while every
// caller was pending (held), what each caller was told about its one
// invocation (outcome), what the PG's interval history says around the
// fault and the restart (interval), and what native clients found in each
// append's journal object afterwards, up to native librbd's replay
// (journal).
func rbdJournalHeldPendingClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := rbdJournalHeldText
	h, e := r.Held, r.Entry
	stages, order := rbdJournalHeldPlan(h.Mode)
	reached := slices.Index(stages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if r.PrimaryChange != nil || r.OwnerLoss != nil || r.Trim != nil || r.Inflight != nil || r.ForcedPending != nil || r.Consumer != nil {
		return []string{"record: a held-request record carries another variant's part"}
	}
	restart := h.Mode == rbdJournalVariantPrimaryRestart
	p := h.Pending
	holds := rbdJournalHeldPendingHolds[h.Mode]
	if p == nil || len(p.Calls) != len(holds) {
		return []string{fmt.Sprintf("record: the pending appends are missing or not %d", len(holds))}
	}
	view := func(name string, at int) *rbdJournalLostAppendView {
		if reached < at {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || len(v.ObjectSizes) <= int(e.Object)+len(holds) || len(v.ObjectSizes) != len(v.ObjectSHA256) {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked := view("setup", 0), view("observe-locked", 0)
	var answered, recorded, restarted, replay *rbdJournalLostAppendView
	if restart {
		recorded, restarted, replay = view("observe-recorded", 1), view("observe-restarted", 2), view("replay", 3)
	} else {
		answered, replay = view("observe-answered", 1), view("replay", 2)
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
	client := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	// The entries: entry 0 is the record's, entry i is tag t entry i in
	// journal object i of the object set, at that object's size before the
	// appends, with an image extent of its own.
	prefix := strings.TrimSuffix(e.ObjectName, strconv.FormatUint(e.Object, 10))
	for i, c := range p.Calls {
		x := c.Entry
		if i == 0 && !reflect.DeepEqual(x, e) {
			fail("entries: entry 0 is not the record's entry")
		}
		o := e.Object + uint64(i)
		if x.Tag != e.Tag || x.EntryTID != uint64(i) || x.Object != o || x.ObjectName != prefix+strconv.FormatUint(o, 10) || x.Length <= 0 || len(x.SHA256) != 64 ||
			c.Hold != holds[i] || c.Label != fmt.Sprintf("journal_append_%d", i) || len(c.BeforeSHA256) != 64 || len(c.AppliedSHA256) != 64 || c.BeforeSHA256 == c.AppliedSHA256 {
			fail("entries: entry %d is not tag %d entry %d in journal object %d held as %s: %s", i, e.Tag, i, o, holds[i], text(c))
		}
		if c.BeforeSize != locked.ObjectSizes[o] || c.BeforeSHA256 != locked.ObjectSHA256[o] || uint64(x.Offset) != c.BeforeSize {
			fail("entries: entry %d's journal object before it differs from the native observation of the locked image", i)
		}
		for _, other := range p.Calls[:i] {
			if y := other.Entry; x.ImageOffset < y.ImageOffset+y.ImageLength && y.ImageOffset < x.ImageOffset+x.ImageLength {
				fail("entries: the image extents of entries %d and %d overlap", y.EntryTID, i)
			}
		}
		wantNamed := h.SentAtEpoch
		if c.Hold == rbdJournalHoldMap {
			wantNamed += rbdJournalForcedPendingEpochs
		}
		if c.NamedEpoch != wantNamed {
			fail("entries: entry %d names epoch %d, want %d", i, c.NamedEpoch, wantNamed)
		}
	}
	lockRecord := func(name string, v *rbdJournalLostAppendView, watch bool) {
		if v.LockFree || v.LockOwner != client || v.LockCookie != r.LockCookie || watch && !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
		}
	}
	unmoved := func(name string, v *rbdJournalLostAppendView) {
		if v.ImageHeadSHA256 != r.ExpectedHeadBefore || !reflect.DeepEqual(v.ImageCommit, setup.ImageCommit) || v.NextTag != e.Tag+1 || v.LastTag.TID != e.Tag {
			fail("%s: image, image client commit position or tags moved (head=%s commit=%v next tag=%d)", name, v.ImageHeadSHA256, v.ImageCommit, v.NextTag)
		}
	}
	// journal checks every journal object of one native observation: the
	// object of each applied entry is its earlier bytes followed by exactly
	// that entry, every other object is as the locked image left it.
	journal := func(name string, v *rbdJournalLostAppendView, applied []bool) {
		for i := range v.ObjectSizes {
			size, sum := locked.ObjectSizes[i], locked.ObjectSHA256[i]
			if k := i - int(e.Object); k >= 0 && k < len(p.Calls) && applied[k] {
				size, sum = p.Calls[k].BeforeSize+uint64(p.Calls[k].Entry.Length), p.Calls[k].AppliedSHA256
			}
			if i >= len(locked.ObjectSizes) || v.ObjectSizes[i] != size || v.ObjectSHA256[i] != sum {
				what := "unchanged"
				if k := i - int(e.Object); k >= 0 && k < len(applied) && applied[k] {
					what = fmt.Sprintf("its earlier bytes followed by exactly entry %d", k)
				}
				fail("%s: journal object %d is not %s (size %d, want %d)", name, i, what, v.ObjectSizes[i], size)
			}
		}
	}
	every := func(applied bool) []bool {
		a := make([]bool, len(p.Calls))
		for i := range a {
			a[i] = applied
		}
		return a
	}
	members := func(osds []int32) []int32 { sorted := slices.Clone(osds); slices.Sort(sorted); return sorted }
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
		for _, peer := range q.Peers {
			peers = append(peers, peer.Peer)
			if state == "active+clean" && peer.LastUpdate != q.LastUpdate {
				fail("%s: peer osd.%d is at %s, the primary at %s", name, peer.Peer, peer.LastUpdate, q.LastUpdate)
			}
		}
		if want := slices.DeleteFunc(members(acting), func(o int32) bool { return o == acting[0] }); !slices.Equal(members(peers), want) {
			fail("%s: the PG query lists peers %v, want %v", name, peers, want)
		}
		return q
	}
	g := r.Gate
	if g == nil {
		fail("transmission: gate state missing")
		return problems
	}

	// Held: before the appends, the PG on three members led by P, clean.
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
	if !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) || !slices.Equal(locked.ObjectSizes, setup.ObjectSizes) {
		fail("observe-locked: journal objects changed before the appends")
	}
	if ping := h.PingBefore; ping == nil || ping.Class != "applied" || ping.Code != 0 || ping.Invocations != 1 {
		fail("ping: the subject's watch was not renewed before the fault: %s", text(ping))
	}
	hd := h.Held
	if hd == nil || len(hd.Objects) != 3 {
		fail("observe-held: the fixture's read is missing: %s", text(hd))
		return problems
	}
	if h.SentAtEpoch == 0 || h.SentAtEpoch < pl.Objects[1].Epoch || h.SentAtEpoch > hd.Objects[1].Epoch || h.NamedEpoch != h.SentAtEpoch {
		fail("append: the subject placed the appends at epoch %d, outside the fixture's reads around it", h.SentAtEpoch)
	}
	if h.Replica != acting[2] || h.Lease == nil || h.Lease.Daemon != fmt.Sprintf("osd.%d", h.Replica) || h.Lease.Deadline == 0 {
		fail("pause: the last acting member osd.%d was not the one paused under a lease: replica osd.%d, lease %s", acting[2], h.Replica, text(h.Lease))
	}
	placedOn("observe-held", hd, acting, h.Primary)
	if q := queried("observe-held", hd, "active+clean", acting); q != nil && placedQuery != nil && q.SameIntervalSince != placedQuery.SameIntervalSince {
		fail("observe-held: the PG's interval changed while the appends were held (%d, before %d)", q.SameIntervalSince, placedQuery.SameIntervalSince)
	}
	if len(hd.Down) != 0 {
		fail("observe-held: OSDs %v are down", hd.Down)
	}
	var mine []nativeRBDJournalHeldOp
	for _, o := range hd.Ops {
		if o.Client == client {
			mine = append(mine, o)
		}
	}
	if hd.OSD != h.Primary || !hd.Settled || len(mine) != len(p.Calls) {
		fail("observe-held: the primary osd.%d does not list exactly %d operations of the subject: %s", h.Primary, len(p.Calls), text(hd))
		return problems
	}
	replicas := slices.DeleteFunc(members(acting), func(o int32) bool { return o == h.Primary })
	waiting := fmt.Sprintf("waiting for subops from %d,%d", replicas[0], replicas[1])
	// listed is the transaction the primary listed for each entry.
	listed := make([]uint64, len(p.Calls))
	for i, c := range p.Calls {
		at := slices.IndexFunc(mine, func(o nativeRBDJournalHeldOp) bool {
			return strings.Contains(o.Description, ":::"+c.Entry.ObjectName+":head [call journal.append")
		})
		if at < 0 {
			fail("observe-held: the primary lists no append of the subject to %s", c.Entry.ObjectName)
			continue
		}
		op := mine[at]
		if !strings.HasSuffix(op.Description, " e"+strconv.FormatUint(uint64(c.NamedEpoch), 10)+")") || op.Transaction == 0 || slices.Contains(listed, op.Transaction) {
			fail("observe-held: the subject's operation on %s is not one append at epoch %d: %s", c.Entry.ObjectName, c.NamedEpoch, text(op))
		}
		listed[i] = op.Transaction
		switch c.Hold {
		case rbdJournalHoldSubOps:
			if op.FlagPoint != rbdJournalHeldSubOpsFlag || !rbdJournalHeldEvents(op.Events, append(slices.Clone(rbdJournalHeldInPG), waiting), "op_commit", "sub_op_commit_rec") {
				fail("observe-held: entry %d is not waiting for one of its two replica commits on the primary: %s", i, text(op))
			}
		case rbdJournalHoldMap:
			if op.FlagPoint != rbdJournalHeldNoFlag || !rbdJournalHeldEvents(op.Events, nil) {
				fail("observe-held: entry %d, held for its map, has progressed into its PG: %s", i, text(op))
			}
		}
	}
	if !h.PendingAfterHeld {
		fail("observe-held: a caller was not pending after the held observation")
	}
	if g.Bound || g.LargeWrites != len(p.Calls) || g.Error != "" {
		fail("transmission: not one write of the append size per entry, or the gate withheld a reply: %s", text(g))
	}
	if reached < 1 {
		return problems
	}

	// Outcome: one invocation per entry, submitted as the transaction the
	// primary listed for it, at the epoch it names.
	for i, c := range p.Calls {
		o := c.Outcome
		if o == nil {
			fail("outcome: entry %d has no outcome", i)
			continue
		}
		if o.Label != c.Label || o.CallerInvocations != 1 || o.Submission.Transaction == 0 || o.Submission.GlobalID != r.SubjectGlobalID || o.Submission.Transaction != listed[i] ||
			o.Submission.MapEpoch != c.NamedEpoch || o.RecordedUTC == "" {
			fail("outcome: entry %d is not one invocation submitted as the transaction %d the primary listed: %s", i, listed[i], text(o))
		}
	}
	if r.Outcome == nil || !reflect.DeepEqual(r.Outcome, p.Calls[0].Outcome) {
		fail("outcome: the record's outcome is not entry 0's")
	}
	lease := func() *rbdJournalHeldLease {
		l := h.Lease
		submitted, err := time.Parse(time.RFC3339Nano, h.SubmittedUTC)
		if l == nil || l.Reason != "resumed" || l.LeftMS <= 0 || l.PausedAt == 0 || l.EndedAt <= l.PausedAt || l.EndedAt > l.Deadline || err != nil ||
			l.PausedAt >= submitted.UnixNano() {
			fail("replica_resumed: the replica was not paused before the appends were sent and resumed by the test before its deadline: %s", text(l))
			return nil
		}
		return l
	}
	recordedBefore := func(name, at string, l *rbdJournalHeldLease) {
		if l == nil {
			return
		}
		if t, err := time.Parse(time.RFC3339Nano, at); err != nil || l.EndedAt <= t.UnixNano() {
			fail("replica_resumed: the replica was resumed before %s", name)
		}
	}

	if !restart {
		for i, c := range p.Calls {
			o := c.Outcome
			if o == nil {
				continue
			}
			if i == rbdJournalHeldPendingCanceled {
				if o.Class != "uncertain" || !o.UnknownOutcome || !o.Canceled || o.ReplyTransaction != 0 || o.ReplyCode != 0 {
					fail("outcome: entry %d is not an unknown outcome of a canceled wait: %s", i, text(o))
				}
				recordedBefore("the canceled wait was recorded", o.RecordedUTC, lease())
				if p.CanceledUTC == "" {
					fail("outcome: the canceled wait has no time")
				}
			} else if o.Class != "applied" || o.UnknownOutcome || o.Canceled || o.ReplyCode != 0 || o.ReplyTransaction != o.Submission.Transaction || o.Error != "" {
				fail("outcome: entry %d is not one known answer 0 to its held transaction: %s", i, text(o))
			}
		}
		// Interval: unchanged across the requests, every peer at the
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
		// Journal: every entry once, the canceled one included.
		journal("observe-answered", answered, every(true))
		lockRecord("observe-answered", answered, true)
		unmoved("observe-answered", answered)
	} else {
		if k := h.Kill; k == nil || k.Daemon != fmt.Sprintf("osd.%d", h.Primary) || k.ExitCode != rbdJournalHeldKilledExit || k.OOMKilled {
			fail("kill: the primary osd.%d was not killed with SIGKILL: %s", h.Primary, text(k))
		}
		l := lease()
		for i, c := range p.Calls {
			o := c.Outcome
			if o == nil {
				continue
			}
			if o.Class != "uncertain" || !o.UnknownOutcome || o.Canceled || o.ReplyTransaction != 0 || o.ReplyCode != 0 {
				fail("outcome: entry %d is not an unknown outcome of a call whose connection was lost: %s", i, text(o))
			}
			recordedBefore(fmt.Sprintf("entry %d's outcome was recorded", i), o.RecordedUTC, l)
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
		// Journal: entry 0 survived on the member that committed it; entry
		// 1 never reached the PG.
		applied := []bool{true, false}
		journal("observe-recorded", recorded, applied)
		lockRecord("observe-recorded", recorded, false)
		unmoved("observe-recorded", recorded)
		if reached < 2 {
			return problems
		}
		// Interval: P up again and leading the PG on its original members,
		// clean, in an interval newer than the one without P.
		back := p.Back
		if back == nil || len(back.Objects) != 3 || back.Query == nil || p.StartedUTC == "" {
			fail("observe-primary-back: the fixture's read is missing: %s", text(back))
			return problems
		}
		if !back.Settled || slices.Contains(back.Down, h.Primary) || len(back.Down) != 0 || back.OSD != -1 || len(back.Ops) != 0 {
			fail("observe-primary-back: osd.%d is not up again with every OSD up: %s", h.Primary, text(back))
		}
		placedOn("observe-primary-back", back, acting, h.Primary)
		if q := queried("observe-primary-back", back, "active+clean", acting); q != nil && lost.Query != nil &&
			(q.SameIntervalSince <= lost.Query.SameIntervalSince || q.SamePrimarySince <= lost.Query.SamePrimarySince) {
			fail("observe-primary-back: the PG is not in an interval newer than the one without osd.%d: %s, before %s", h.Primary, text(q), text(lost.Query))
		}
		if p.AddressBefore == "" || p.AddressAfter == "" || p.AddressAfter == p.AddressBefore {
			fail("observe-primary-back: the restarted osd.%d kept its address %q (now %q)", h.Primary, p.AddressBefore, p.AddressAfter)
		}
		// Journal: byte-identical to the observation without P.
		journal("observe-restarted", restarted, applied)
		if !slices.Equal(restarted.ObjectSHA256, recorded.ObjectSHA256) || !slices.Equal(restarted.ObjectSizes, recorded.ObjectSizes) {
			fail("observe-restarted: the journal objects differ from the observation without the primary")
		}
		lockRecord("observe-restarted", restarted, false)
		unmoved("observe-restarted", restarted)
		if reached < 3 {
			return problems
		}
		// The handoff on the restarted primary.
		x := h.Reconnect
		if x == nil || x.Class != "applied" || x.Code != 0 || x.Invocations != 1 || h.HandoffPrimary != h.Primary || h.LockerAddress == "" || h.WatcherAddress == "" {
			fail("handoff: the watch was not reattached on the restarted primary with the lock record read there: %s (primary osd.%d, lock %q, watch %q)", text(x), h.HandoffPrimary, h.LockerAddress, h.WatcherAddress)
		}
	}
	if reached < len(stages)-1 {
		return problems
	}
	if restart {
		if r.DialsBeforeClose == nil || *r.DialsBeforeClose != 1 || r.DialsTotal == nil || *r.DialsTotal != 2 || g.Connections != 2 || h.AppendConnection != 1 {
			fail("transmission: not one subject connection to the primary before its death and one after its restart: gate %s, dials before close %s, in total %s", text(g), text(r.DialsBeforeClose), text(r.DialsTotal))
		}
	} else if r.DialsTotal == nil || *r.DialsTotal != 1 || g.Connections != 1 || h.AppendConnection != 1 {
		fail("transmission: not one subject connection: gate %s, dials %s", text(g), text(r.DialsTotal))
	}
	if g.LargeWrites != len(p.Calls) {
		fail("transmission: %d writes of the append size for %d entries; an append may have been sent again", g.LargeWrites, len(p.Calls))
	}

	// Journal: native librbd's replay of the entries that exist.
	applied := every(true)
	if restart {
		applied = []bool{true, false}
	}
	journal("replay", replay, applied)
	var owned []rbdJournalLostAppendEvent
	last := uint64(0)
	for i, c := range p.Calls {
		if applied[i] {
			x := c.Entry
			owned = append(owned, rbdJournalLostAppendEvent{Type: "AioWrite", Offset: x.ImageOffset, Length: x.ImageLength,
				Timestamp: time.Unix(int64(x.Timestamp.Seconds), int64(x.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)})
			last = x.EntryTID
		}
	}
	if replay.InspectTotal == nil || replay.InspectErrors == nil || setup.InspectTotal == nil || *replay.InspectTotal != *setup.InspectTotal+len(owned) || *replay.InspectErrors != 0 ||
		!reflect.DeepEqual(replay.InspectOwned, owned) || !slices.Equal(replay.InspectOther, setup.InspectOther) {
		fail("replay: rbd journal inspect does not replay the setup entries and the %d existing entries in order (%v)", len(owned), replay.InspectOwned)
	}
	if !replay.LockFree || replay.SubjectWatcher {
		fail("replay: the lock is held or the subject's watch is listed after the replay")
	}
	if replay.ImageHeadSHA256 != r.ExpectedHeadAfter || r.ExpectedHeadAfter == r.ExpectedHeadBefore || len(r.ExpectedCommitAfter) == 0 ||
		r.ExpectedCommitAfter[0] != (rbdJournalPosition{e.Object + last, e.Tag, last}) || !reflect.DeepEqual(replay.ImageCommit, r.ExpectedCommitAfter) {
		fail("replay: image or image client commit position differs from one native replay of the existing entries (head=%s commit=%v)", replay.ImageHeadSHA256, replay.ImageCommit)
	}
	if replay.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, last}) || replay.NextTag != e.Tag+2 {
		fail("replay: native tag differs (tag=%+v next=%d)", replay.LastTag, replay.NextTag)
	}
	return problems
}
