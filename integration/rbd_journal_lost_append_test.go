package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDJournalLostAppendReplyIntegration loses the reply to one Go
// journal append and keeps what the caller then knows apart from what native
// clients see. On a fresh journaling image of the RBD fixture, one Go client
// (the subject) takes the image's exclusive lock in librbd's managed-lock
// format, allocates a journal tag and sends one AioWrite entry with
// `journal.append`, exactly as the journal acceptance does. A test-only gate
// on the subject's OSD connection withholds the reply to that one request.
// While the subject's call is still pending and the lock is still held,
// native librados, `rbd journal status` and `rbd journal inspect` look at the
// journal: the entry is there, the image and the image client's commit
// position are unchanged. The subject then cancels only its local wait,
// records the unknown outcome, and a second native observation shows that
// nothing has moved since. Only after that does it close the silent
// connection, open a new one, reattach its watch, release the lock and
// unwatch. It never sends the append again, never applies the entry and never
// commits a position. Native librbd finally acquires the lock and replays the
// journal. See integration/fixture/RBD_JOURNAL_LOST_APPEND_VALIDATION.md.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.

// rbdJournalLostAppendThreshold is the write size the undecoded gate binds
// to. The append frame is the only write of the subject that reaches it.
const rbdJournalLostAppendThreshold = 4096

// rbdJournalLostAppendWatchTimeout is the subject's image header watch
// timeout in seconds, librbd's default.
const rbdJournalLostAppendWatchTimeout = 30

// rbdJournalLostAppendWithheldWait bounds the wait, after the append is seen
// applied, for the undecoded gate to intercept received bytes on the bound
// connection. It stays well inside the watch gap the test allows.
const rbdJournalLostAppendWithheldWait = 10 * time.Second

// rbdJournalLostAppendOrder is the order in which the steps must complete.
var rbdJournalLostAppendOrder = []string{"setup", "lock", "tag_create", "observe-locked", "append_submitted", "barrier", "checkpoint", "outcome_recorded",
	"observe-recorded", "blocked_connection_closed", "watch_reconnected", "unlock", "unwatch", "replay"}

// The primary-change variant (TestCephRBDJournalPrimaryChangeIntegration)
// runs the same sequence on three replicated BlueStore OSDs, with one PG in
// the owned pool. After the unknown outcome is recorded, the fixture marks
// that PG's primary out, which makes another member the acting primary. The
// subject then reattaches its watch there and resubmits the append once,
// under its original transaction, to the new primary.
var (
	rbdJournalPrimaryChangeOrder = []string{"setup", "lock", "tag_create", "observe-locked", "append_submitted", "barrier", "checkpoint", "outcome_recorded",
		"observe-recorded", "change-primary", "blocked_connection_closed", "watch_reconnected", "resubmitted", "observe-resubmitted", "unlock", "unwatch", "replay"}
	rbdJournalPrimaryChangeStages = []string{"checkpoint", "observe-recorded", "change-primary", "observe-resubmitted", "replay"}
)

// rbdJournalPlacementView is one object's placement as the test resolved it
// through the observer's MON command.
type rbdJournalPlacementView struct {
	Object  string
	Epoch   uint32
	PGID    string
	Primary int32
	Acting  []int32
}

// nativeRBDJournalPlacement and nativeRBDJournalPrimaryChange are the
// fixture's `change-primary` receipt: `ceph osd map` of each object before
// and after its `osd out` command.
type nativeRBDJournalPlacement struct {
	Epoch         uint32  `json:"epoch"`
	Pool          string  `json:"pool"`
	PoolID        int64   `json:"pool_id"`
	Object        string  `json:"objname"`
	RawPGID       string  `json:"raw_pgid"`
	PGID          string  `json:"pgid"`
	Up            []int32 `json:"up"`
	UpPrimary     int32   `json:"up_primary"`
	Acting        []int32 `json:"acting"`
	ActingPrimary int32   `json:"acting_primary"`
}

type nativeRBDJournalPrimaryChange struct {
	Objects []struct {
		Object string                    `json:"object"`
		Moved  bool                      `json:"moved"`
		Before nativeRBDJournalPlacement `json:"before"`
		After  nativeRBDJournalPlacement `json:"after"`
	} `json:"objects"`
	Out       int32    `json:"osd_out"`
	Commands  []string `json:"commands"`
	Polls     int      `json:"polls"`
	ElapsedMS int64    `json:"elapsed_ms"`
}

// rbdJournalResubmission is the raw outcome of the one explicit
// resubmission: the original transaction, sent to the new primary.
type rbdJournalResubmission struct {
	CallerInvocations int
	Class             string
	UnknownOutcome    bool
	Error             string
	Primary           int32
	RetryAttempt      int32
	Submission        cephmsgr.OSDSubmission
	ReplyTransaction  uint64
	ReplyCode         int32
	ReplyRetryAttempt int32
	ReplyMapEpoch     uint32
	OnDisk            bool
	UserVersion       uint64
	RecordedUTC       string
}

// rbdJournalPrimaryChange is the primary-change variant's own record.
type rbdJournalPrimaryChange struct {
	// The objects whose placement is followed: the image header, the journal
	// object of the owned entry and the journal header.
	Objects []string
	// LockPrimary and AppendPrimary are the OSDs the subject sent lock.lock
	// and the append to.
	LockPrimary, AppendPrimary int32
	GoBefore, GoAfter          []rbdJournalPlacementView
	Native                     *nativeRBDJournalPrimaryChange
	// The address in the subject's lock record and the address its watch is
	// listed at once it was reattached on the new primary, both as the new
	// primary returned them to the subject. Both are the address of the
	// connection to the old primary, which registered them.
	LockerAddress, WatcherAddress string
	// The journal object's user version as the subject read it on the new
	// primary before and after the resubmission.
	VersionBefore, VersionAfter uint64
	Resubmission                *rbdJournalResubmission
	// The dial order of the subject connections that carried the append and
	// its resubmission.
	AppendConnection, ResubmitConnection int
}

type rbdJournalLostAppendEntry struct {
	Tag, EntryTID, Object    uint64
	ObjectName               string
	Offset, Length           int
	SHA256                   string
	ImageOffset, ImageLength uint64
	Timestamp                cephmsgr.OSDTime
}

type rbdJournalLostAppendEvent struct {
	Type           string
	Offset, Length uint64
	Timestamp      string
}

type rbdJournalLostAppendTag struct {
	TID                            uint64
	PredecessorValid               bool
	PredecessorTag, PredecessorEnt uint64
}

// rbdJournalLostAppendView is what one native action showed. Every value
// comes from the native driver's output: librados reads and `exec`, librbd's
// locker and watcher lists and the `rbd journal` commands. Pointer fields
// are nil when the action did not observe them.
type rbdJournalLostAppendView struct {
	Action         string
	RequestID      int64
	NativeGlobalID uint64
	ObservedUTC    string
	ObjectSizes    []uint64
	ObjectSHA256   []string
	// The owned entry's place in its journal object: the bytes before it,
	// the entry itself when the object reaches that far, and how many bytes
	// follow it.
	PrefixSHA256    *string
	EntrySHA256     *string
	TrailingBytes   *uint64
	ImageHeadSHA256 string
	ImageCommit     []rbdJournalPosition
	NextTag         uint64
	LastTag         rbdJournalLostAppendTag
	LockFree        bool
	LockOwner       string
	LockCookie      string
	Watchers        int
	SubjectWatcher  bool
	// `rbd journal inspect`: entries replayed, errors, and the entries of the
	// subject's tag.
	InspectTotal  *int
	InspectErrors *int
	InspectOwned  []rbdJournalLostAppendEvent
}

type rbdJournalLostAppendOutcome struct {
	Label             string
	CallerInvocations int
	Class             string
	UnknownOutcome    bool
	Canceled          bool
	Error             string
	Submission        cephmsgr.OSDSubmission
	ReplyTransaction  uint64
	ReplyCode         int32
	RecordedUTC       string
}

type rbdJournalLostAppendRecord struct {
	Token, Pool      string
	PoolID           int64
	GoIdentity       string
	SubjectGlobalID  uint64
	ObserverGlobalID uint64
	ConnectionMode   string
	Image, ImageID   string
	JournalHeader    string
	WatchCookie      uint64
	LockCookie       string
	Entry            rbdJournalLostAppendEntry
	Requests         []rbdWriteRequestEvidence
	Outcome          *rbdJournalLostAppendOutcome
	// Gate is the subject's OSD connections as the test-only gate saw them.
	Gate *testcluster.OSDReplyGateState
	// OSD connections the subject dialed until the silent one was closed, and
	// in total.
	DialsBeforeClose, DialsTotal *int
	// Barrier names what the checkpoint waited for; BarrierPolls counts the
	// observer client's size reads (undecoded mode only).
	Barrier                string
	BarrierPolls           *int
	PendingAfterCheckpoint *bool
	// WithheldBeforeCancel is the undecoded gate's withheld-bytes flag as it
	// was read while the append was still pending, directly before the wait
	// was canceled. It is absent in decoded mode, where the held frame is the
	// evidence. The final gate state cannot stand in for it: bytes that the
	// gate first intercepts during the handoff do not show that anything was
	// withheld from the pending call.
	WithheldBeforeCancel *bool
	Order                []string
	Views                map[string]*rbdJournalLostAppendView
	ExpectedHeadBefore   string
	ExpectedHeadAfter    string
	ExpectedCommitAfter  []rbdJournalPosition
	NativeActions        []rbdNativeActionEvidence
	InspectShutdownLines int
	// ClaimsStage is the last stage at which the claims were judged (empty
	// when the run ended before the checkpoint); Problems are that stage's.
	ClaimsStage string
	Problems    []string
	Timings     map[string]string
	// PrimaryChange is set in the primary-change variant only.
	PrimaryChange *rbdJournalPrimaryChange
}

// rbdJournalLostAppendPlan returns the stages and the step order of the
// variant the record belongs to.
func rbdJournalLostAppendPlan(r *rbdJournalLostAppendRecord) (stages, order []string) {
	if r.PrimaryChange != nil {
		return rbdJournalPrimaryChangeStages, rbdJournalPrimaryChangeOrder
	}
	return rbdJournalLostAppendStages, rbdJournalLostAppendOrder
}

// rbdJournalLostAppendStages are the points at which the record is judged:
// after the checkpoint, after the observation that follows the recorded
// outcome, and after the native replay. Each stage includes the earlier ones.
var rbdJournalLostAppendStages = []string{"checkpoint", "observe-recorded", "replay"}

// rbdJournalLostAppendClaims checks the record up to one stage. It returns
// one line per contradiction and keeps four claims apart: what native clients
// saw while the caller was pending (application, from "checkpoint" on), what
// the caller was told (outcome) and what moved before that was recorded
// (order, both from "observe-recorded" on), and how often the request left
// the client (transmission, with the native replay at "replay").
func rbdJournalLostAppendClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	// Records hold optional fields as pointers; print them as their JSON.
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	e := r.Entry
	stages, order := rbdJournalLostAppendPlan(r)
	if !slices.Contains(stages, stage) {
		return []string{"unknown stage " + stage}
	}
	// reached counts the stages both variants share; the primary-change
	// variant's two stages lie between "observe-recorded" and "replay".
	pc := r.PrimaryChange
	at := slices.Index(stages, stage)
	changed, resubmitted := pc != nil && at >= 2, pc != nil && at >= 3
	reached := slices.Index(rbdJournalLostAppendStages, stage)
	if reached < 0 {
		reached = 1
	}
	view := func(name string, from int) *rbdJournalLostAppendView {
		if reached < from {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || int(e.Object) >= len(v.ObjectSizes) || len(v.ObjectSizes) != len(v.ObjectSHA256) {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked, checkpoint, recorded, replay := view("setup", 0), view("observe-locked", 0), view("checkpoint", 0), view("observe-recorded", 1), view("replay", 2)
	var moved, resolved *rbdJournalLostAppendView
	if changed {
		moved = view("change-primary", 1)
	}
	if resubmitted {
		resolved = view("observe-resubmitted", 1)
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
	held := func(name string, v *rbdJournalLostAppendView) {
		if v.LockFree || v.LockOwner != owner || v.LockCookie != r.LockCookie || !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock with its watch (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
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
			fail("%s: journal object %d is not its earlier bytes followed by exactly the owned entry (size %d, entry at %d+%d, entry sha256 %s, trailing bytes %s)",
				name, e.Object, v.ObjectSizes[e.Object], e.Offset, e.Length, text(v.EntrySHA256), text(v.TrailingBytes))
		}
		for i := range v.ObjectSizes {
			if uint64(i) != e.Object && (i >= len(locked.ObjectSHA256) || v.ObjectSHA256[i] != locked.ObjectSHA256[i] || v.ObjectSizes[i] != locked.ObjectSizes[i]) {
				fail("%s: journal object %d changed", name, i)
			}
		}
	}
	inspected := func(name string, v *rbdJournalLostAppendView) {
		want := rbdJournalLostAppendEvent{Type: "AioWrite", Offset: e.ImageOffset, Length: e.ImageLength,
			Timestamp: time.Unix(int64(e.Timestamp.Seconds), int64(e.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
		if v.InspectTotal == nil || v.InspectErrors == nil || setup.InspectTotal == nil || *v.InspectTotal != *setup.InspectTotal+1 || *v.InspectErrors != 0 ||
			len(v.InspectOwned) != 1 || v.InspectOwned[0] != want {
			fail("%s: rbd journal inspect does not replay the setup entries and the owned entry once (%v)", name, v.InspectOwned)
		}
	}

	// Application: natively observed while the caller was still pending.
	if locked.EntrySHA256 != nil || locked.ObjectSizes[e.Object] != uint64(e.Offset) || !reflect.DeepEqual(locked.ObjectSHA256, setup.ObjectSHA256) {
		fail("observe-locked: journal objects changed before the append, or the entry offset is not the object size")
	}
	held("observe-locked", locked)
	unmoved("observe-locked", locked)
	entryAt("checkpoint", checkpoint)
	inspected("checkpoint", checkpoint)
	held("checkpoint", checkpoint)
	unmoved("checkpoint", checkpoint)
	if r.PendingAfterCheckpoint == nil || !*r.PendingAfterCheckpoint {
		fail("application: the caller was not observed pending after the checkpoint")
	}

	if reached < 1 {
		return problems
	}

	// Outcome: what the caller was told, and that it asked once.
	o := r.Outcome
	if o == nil {
		fail("outcome: missing")
		o = &rbdJournalLostAppendOutcome{}
	} else if o.Class != "uncertain" || !o.UnknownOutcome || !o.Canceled || o.CallerInvocations != 1 || o.Submission.Transaction == 0 ||
		o.Submission.GlobalID != r.SubjectGlobalID || o.ReplyTransaction != 0 || o.ReplyCode != 0 || o.RecordedUTC == "" {
		fail("outcome: not one unknown outcome of a canceled wait with its submission: %s", text(o))
	}

	// Order: nothing moved between the checkpoint and the recorded outcome.
	if !reflect.DeepEqual(recorded.ObjectSHA256, checkpoint.ObjectSHA256) || !reflect.DeepEqual(recorded.ObjectSizes, checkpoint.ObjectSizes) {
		fail("observe-recorded: journal objects changed after the checkpoint")
	}
	entryAt("observe-recorded", recorded)
	held("observe-recorded", recorded)
	unmoved("observe-recorded", recorded)

	if changed {
		rbdJournalPrimaryChangeClaims(r, moved, resolved, checkpoint, fail)
		entryAt("change-primary", moved)
		held("change-primary", moved)
		unmoved("change-primary", moved)
	}
	if resubmitted {
		entryAt("observe-resubmitted", resolved)
		held("observe-resubmitted", resolved)
		unmoved("observe-resubmitted", resolved)
	}

	if reached < 2 {
		return problems
	}

	// Transmission: counted on the subject's connections, not by the daemon.
	g := r.Gate
	if g == nil {
		fail("transmission: gate state missing")
		g = &testcluster.OSDReplyGateState{}
	}
	// The frames that carry the entry: the append alone, or in the
	// primary-change variant the append and its one resubmission, each on a
	// connection of its own.
	bound, large, marked := 1, 1, []int{1}
	if pc == nil {
		if r.DialsBeforeClose == nil || *r.DialsBeforeClose != 1 || r.DialsTotal == nil || *r.DialsTotal != 2 || g.Connections != 2 {
			fail("transmission: not two subject connections, one of them before the silent one was closed: gate %s, dials before close %s, in total %s", text(g), text(r.DialsBeforeClose), text(r.DialsTotal))
		}
	} else {
		bound, large, marked = pc.AppendConnection, 2, []int{pc.AppendConnection, pc.ResubmitConnection}
		if r.DialsBeforeClose == nil || r.DialsTotal == nil || pc.AppendConnection < 1 || pc.AppendConnection > *r.DialsBeforeClose || pc.ResubmitConnection <= *r.DialsBeforeClose ||
			pc.ResubmitConnection > *r.DialsTotal || g.Connections != *r.DialsTotal {
			fail("transmission: the append and its resubmission were not on one connection before and one after the silent connections were closed: gate %s, dials before close %s, in total %s, connections %d and %d",
				text(g), text(r.DialsBeforeClose), text(r.DialsTotal), pc.AppendConnection, pc.ResubmitConnection)
		}
	}
	if !g.Bound || g.BoundConnection != bound || g.LargeWrites != large || g.Error != "" || g.Threshold != rbdJournalLostAppendThreshold || g.BoundWriteBytes < e.Length {
		fail("transmission: not %d write(s) of the append size with the gate bound on subject connection %d: gate %s", large, bound, text(g))
	}
	switch {
	case g.Decoded != (r.ConnectionMode == "crc"):
		fail("transmission: gate mode does not match connection mode %q", r.ConnectionMode)
	case g.Decoded:
		if g.MarkedRequests == nil || *g.MarkedRequests != len(marked) || !slices.Equal(g.MarkedConnections, marked) || g.Transaction == nil || *g.Transaction != o.Submission.Transaction ||
			g.Held == nil || g.Held.Connection != bound || g.Held.Type != msgr.OSDOpReplyMessage || g.Held.Transaction != o.Submission.Transaction || g.Held.Sequence == 0 || g.Held.FrameBytes <= 0 ||
			g.WithheldBytesSeen != nil || r.WithheldBeforeCancel != nil || r.Barrier != "held reply" || r.BarrierPolls != nil {
			fail("transmission: the decoded gate did not see the marked request on connection(s) %v and hold the reply of its transaction %d: gate %s", marked, o.Submission.Transaction, text(g))
		}
	default:
		// An undecoded gate cannot name a frame; a record that does is wrong.
		if g.MarkedRequests != nil || g.MarkedConnections != nil || g.Transaction != nil || g.Sequence != nil || g.Held != nil ||
			r.Barrier != "polled object size" || r.BarrierPolls == nil || *r.BarrierPolls < 1 {
			fail("transmission: the undecoded gate record attributes a frame, or its barrier is missing: gate %s, barrier %q", text(g), r.Barrier)
		}
		// The injected loss itself: received bytes were intercepted on the
		// bound connection while the call was pending. A flag that is
		// present but false means no post-bind read was intercepted.
		if g.WithheldBytesSeen == nil || !*g.WithheldBytesSeen || r.WithheldBeforeCancel == nil || !*r.WithheldBeforeCancel {
			fail("transmission: the undecoded gate did not withhold received bytes before the wait was canceled: at cancellation %s, finally %s", text(r.WithheldBeforeCancel), text(g.WithheldBytesSeen))
		}
	}

	// Replay: native librbd applied the entry once and nothing was appended.
	if !reflect.DeepEqual(replay.ObjectSHA256, checkpoint.ObjectSHA256) || !reflect.DeepEqual(replay.ObjectSizes, checkpoint.ObjectSizes) {
		fail("replay: journal objects changed after the checkpoint")
	}
	entryAt("replay", replay)
	inspected("replay", replay)
	if replay.ImageHeadSHA256 != r.ExpectedHeadAfter || r.ExpectedHeadAfter == r.ExpectedHeadBefore || len(r.ExpectedCommitAfter) == 0 ||
		r.ExpectedCommitAfter[0] != (rbdJournalPosition{e.Object, e.Tag, e.EntryTID}) || !reflect.DeepEqual(replay.ImageCommit, r.ExpectedCommitAfter) {
		fail("replay: image or image client commit position differs from one native replay of the owned entry (head=%s commit=%v)", replay.ImageHeadSHA256, replay.ImageCommit)
	}
	if replay.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, e.EntryTID}) || replay.NextTag != e.Tag+2 || !replay.LockFree || replay.SubjectWatcher {
		fail("replay: native tag, lock or watchers differ (tag=%+v next=%d free=%v subject watcher=%v)", replay.LastTag, replay.NextTag, replay.LockFree, replay.SubjectWatcher)
	}
	return problems
}

// rbdJournalPrimaryChangeClaims checks what the primary-change variant adds:
// that the one primary of the followed objects was marked out and another
// member of their acting set leads them in a newer epoch, as the fixture's
// `ceph osd map` reads and the test's own placements agree;
// that the journal did not change across the move; and, once resolved is
// given, that the one resubmission was a known answer to the original
// transaction from the new primary which left the journal object's version
// and bytes where they were.
func rbdJournalPrimaryChangeClaims(r *rbdJournalLostAppendRecord, moved, resolved, checkpoint *rbdJournalLostAppendView, fail func(string, ...any)) {
	pc, e := r.PrimaryChange, r.Entry
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	n := pc.Native
	if !slices.Equal(pc.Objects, []string{"rbd_header." + r.ImageID, e.ObjectName, r.JournalHeader}) || n == nil || len(n.Objects) != 3 || len(pc.GoBefore) != 3 || len(pc.GoAfter) != 3 {
		fail("primary change: the followed objects, the fixture's receipt or the test's placements are missing: %s", text(pc))
		return
	}
	members := func(osds []int32) []int32 { sorted := slices.Clone(osds); slices.Sort(sorted); return sorted }
	distinct := func(osds []int32, size int) bool {
		return len(osds) == size && len(slices.Compact(members(osds))) == size
	}
	for i, o := range n.Objects {
		b, a := o.Before, o.After
		// Before: three members, up and acting alike, led by the OSD that
		// was then marked out. After, in the same PG and a newer epoch: the
		// two others are the up set, one of them leads, and the OSD that was
		// marked out is the last member of the acting set.
		rest := slices.DeleteFunc(members(b.Acting), func(osd int32) bool { return osd == n.Out })
		before := b.Object == o.Object && b.Pool == r.Pool && b.PoolID == r.PoolID && b.Epoch != 0 && b.PGID != "" && distinct(b.Acting, 3) && slices.Equal(b.Up, b.Acting) &&
			b.ActingPrimary == b.Acting[0] && b.UpPrimary == b.ActingPrimary && b.ActingPrimary == n.Out
		after := a.Object == o.Object && a.Pool == r.Pool && a.PoolID == r.PoolID && a.PGID == b.PGID && a.Epoch > b.Epoch && distinct(a.Up, 2) && slices.Equal(members(a.Up), rest) &&
			slices.Equal(a.Acting, append(slices.Clone(a.Up), n.Out)) && a.UpPrimary == a.Up[0] && a.ActingPrimary == a.Up[0]
		if o.Object != pc.Objects[i] || !o.Moved || !before || !after {
			fail("primary change: the acting primary of %s did not move from osd.%d to another member of its acting set: %s", pc.Objects[i], n.Out, text(o))
		}
		// The test's placements, resolved through the Go client's MON
		// commands before and after the action, name the same PGs and
		// primaries.
		gb, ga := pc.GoBefore[i], pc.GoAfter[i]
		if gb.Object != o.Object || gb.PGID != b.PGID || gb.Primary != b.ActingPrimary || !slices.Equal(gb.Acting, b.Acting) || gb.Epoch == 0 || gb.Epoch > b.Epoch ||
			ga.Object != o.Object || ga.PGID != a.PGID || ga.Primary != a.ActingPrimary || !slices.Equal(ga.Acting, a.Acting) || ga.Epoch < a.Epoch {
			fail("primary change: the test's placement of %s differs from the fixture's: before %s and %s, after %s and %s", o.Object, text(gb), text(b), text(ga), text(a))
		}
	}
	if n.Out < 0 || !slices.Equal(n.Commands, []string{fmt.Sprintf("osd out %d", n.Out)}) {
		fail("primary change: not one `osd out` command for the old primary: osd.%d, %v", n.Out, n.Commands)
	}
	if pc.LockPrimary != n.Out || pc.AppendPrimary != n.Out {
		fail("primary change: the lock (osd.%d) and the append (osd.%d) were not sent to the primary that was then marked out (osd.%d)", pc.LockPrimary, pc.AppendPrimary, n.Out)
	}
	if !reflect.DeepEqual(moved.ObjectSHA256, checkpoint.ObjectSHA256) || !reflect.DeepEqual(moved.ObjectSizes, checkpoint.ObjectSizes) {
		fail("change-primary: journal objects changed across the primary change")
	}
	if resolved == nil {
		if pc.Resubmission != nil {
			fail("resubmission: sent before the primary change was judged")
		}
		return
	}
	x, o := pc.Resubmission, r.Outcome
	if x == nil || o == nil {
		fail("resubmission: missing")
		return
	}
	if x.CallerInvocations != 1 || x.Class != "applied" || x.UnknownOutcome || x.Error != "" || x.ReplyCode != 0 || !x.OnDisk || x.RetryAttempt != 1 || x.ReplyRetryAttempt != 1 ||
		o.Submission.Transaction == 0 || x.ReplyTransaction != o.Submission.Transaction || x.Submission.Transaction != o.Submission.Transaction || x.Submission.GlobalID != r.SubjectGlobalID ||
		x.Primary != n.Objects[1].After.ActingPrimary || x.Primary == pc.AppendPrimary || x.ReplyMapEpoch < n.Objects[1].After.Epoch {
		fail("resubmission: not one known answer 0 to the original transaction %d from the new primary osd.%d: %s", o.Submission.Transaction, n.Objects[1].After.ActingPrimary, text(x))
	}
	// A second execution would have written the object again and raised its
	// version.
	if x.UserVersion == 0 || x.UserVersion != pc.VersionBefore || pc.VersionAfter != pc.VersionBefore {
		fail("resubmission: the journal object's user version is %d before, %d in the answer and %d after; the append may have run again", pc.VersionBefore, x.UserVersion, pc.VersionAfter)
	}
	recordedAt, err1 := time.Parse(time.RFC3339Nano, o.RecordedUTC)
	resubmittedAt, err2 := time.Parse(time.RFC3339Nano, x.RecordedUTC)
	if err1 != nil || err2 != nil || !resubmittedAt.After(recordedAt) {
		fail("resubmission: not recorded after the unknown outcome (%q, %q)", o.RecordedUTC, x.RecordedUTC)
	}
	// The OSD lists a reattached watch under the address it was registered
	// from, which is the address in the lock record: librbd's liveness rule
	// compares exactly these two.
	if pc.LockerAddress == "" || pc.WatcherAddress != pc.LockerAddress {
		fail("resubmission: the new primary does not list the reattached watch at the lock record's address (lock %q, watch %q)", pc.LockerAddress, pc.WatcherAddress)
	}
	if !reflect.DeepEqual(resolved.ObjectSHA256, checkpoint.ObjectSHA256) || !reflect.DeepEqual(resolved.ObjectSizes, checkpoint.ObjectSizes) {
		fail("observe-resubmitted: journal objects changed after the resubmission")
	}
}

// rbdJournalLostAppendViewOf projects one native journal action. entry may be
// nil before the entry is built.
func rbdJournalLostAppendViewOf(dir string, result nativeRBDJournalResult, entry *rbdJournalLostAppendEntry, subject, cookie uint64) (*rbdJournalLostAppendView, error) {
	n := result.Native
	v := &rbdJournalLostAppendView{Action: n.Action, RequestID: n.RequestID, NativeGlobalID: n.NativeGlobalID, ObservedUTC: result.ObservedUTC, Watchers: len(n.Watchers)}
	blob := func(key string) ([]byte, error) {
		b := n.Blobs[key]
		data, err := os.ReadFile(filepath.Join(dir, filepath.Base(b.Name)))
		sum := sha256.Sum256(data)
		if err != nil || uint64(len(data)) != b.Length || hex.EncodeToString(sum[:]) != b.SHA256 {
			return nil, fmt.Errorf("native journal binary view %s differs from its receipt: %v", b.Name, err)
		}
		return data, nil
	}
	head, err := blob("image_head")
	if err != nil {
		return nil, err
	}
	objects, err := blob("journal_objects")
	if err != nil {
		return nil, err
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	v.ImageHeadSHA256 = digest(head)
	for i, o := range n.JournalObjects {
		if o.Number != uint64(i) || o.Offset+o.Size > uint64(len(objects)) {
			return nil, errors.New("native journal object list differs from its binary view")
		}
		data := objects[o.Offset : o.Offset+o.Size]
		v.ObjectSizes, v.ObjectSHA256 = append(v.ObjectSizes, o.Size), append(v.ObjectSHA256, digest(data))
		if entry != nil && uint64(i) == entry.Object && len(data) >= entry.Offset {
			prefix := digest(data[:entry.Offset])
			v.PrefixSHA256 = &prefix
			if len(data) >= entry.Offset+entry.Length {
				owned, trailing := digest(data[entry.Offset:entry.Offset+entry.Length]), uint64(len(data)-entry.Offset-entry.Length)
				v.EntrySHA256, v.TrailingBytes = &owned, &trailing
			}
		}
	}
	var status nativeRBDJournalStatus
	if n.Status == nil || json.Unmarshal(n.Status.Parsed, &status) != nil {
		return nil, errors.New("native rbd journal status missing")
	}
	found := false
	for _, client := range status.Clients {
		if client.ID == "" {
			found = true
			v.ImageCommit = []rbdJournalPosition{}
			for _, p := range client.CommitPosition.Positions {
				v.ImageCommit = append(v.ImageCommit, rbdJournalPosition{p.Object, p.Tag, p.Entry})
			}
		}
	}
	if !found || len(n.ClassCalls) != 11 || n.ClassCalls[9].Method != "get_next_tag_tid" || n.ClassCalls[10].Method != "tag_list" || n.ClassCalls[9].Code != 0 || n.ClassCalls[10].Code != 0 {
		return nil, errors.New("native journal header observation is incomplete")
	}
	next, err1 := hex.DecodeString(n.ClassCalls[9].OutputHex)
	list, err2 := hex.DecodeString(n.ClassCalls[10].OutputHex)
	tags, err3 := decodeRBDJournalTags(list)
	if err1 != nil || err2 != nil || err3 != nil || len(next) != 8 || len(tags) == 0 {
		return nil, errors.New("native journal tag observation does not decode")
	}
	v.NextTag = binary.LittleEndian.Uint64(next)
	last := tags[len(tags)-1]
	td, err := decodeRBDJournalTagData(last.Data)
	if err != nil {
		return nil, err
	}
	v.LastTag = rbdJournalLostAppendTag{last.TID, td.CommitValid, td.PredecessorTag, td.PredecessorEnt}
	v.LockFree = n.Lockers.Empty
	if !n.Lockers.Empty {
		if len(n.Lockers.Lockers) != 1 || !n.Lockers.Exclusive || n.Lockers.Tag != rbdLockTag {
			return nil, errors.New("native locker list is not one exclusive managed lock")
		}
		v.LockOwner, v.LockCookie = n.Lockers.Lockers[0].Client, n.Lockers.Lockers[0].Cookie
	}
	v.SubjectWatcher = slices.ContainsFunc(n.Watchers, func(w nativeRBDWatcher) bool { return w.ID == subject && w.Cookie == cookie && cookie != 0 })
	if n.Inspect != nil {
		var in nativeRBDJournalInspect
		if err := json.Unmarshal(n.Inspect.Parsed, &in); err != nil || in.Summary.Total != len(in.Entries) {
			return nil, errors.New("native rbd journal inspect output is incomplete")
		}
		total, errs := in.Summary.Total, in.Summary.Errors
		v.InspectTotal, v.InspectErrors, v.InspectOwned = &total, &errs, []rbdJournalLostAppendEvent{}
		for i, ne := range in.Entries {
			if ne.CommitTID != uint64(i+1) {
				return nil, errors.New("native rbd journal inspect commit tids are not contiguous")
			}
			if entry == nil || ne.TagID != entry.Tag {
				continue
			}
			ts, err := time.Parse("2006-01-02T15:04:05.000000-0700", ne.Event.Timestamp)
			if err != nil || ne.Event.Offset == nil || ne.Event.Length == nil {
				return nil, fmt.Errorf("native rbd journal inspect entry of tag %d does not parse: %v", ne.TagID, err)
			}
			v.InspectOwned = append(v.InspectOwned, rbdJournalLostAppendEvent{ne.Event.Type, *ne.Event.Offset, *ne.Event.Length, ts.UTC().Format(time.RFC3339Nano)})
		}
	}
	return v, nil
}

func TestCephRBDJournalLostAppendReplyIntegration(t *testing.T) { runRBDJournalLostAppend(t, false) }

// TestCephRBDJournalPrimaryChangeIntegration runs the lost-append sequence on
// three replicated BlueStore OSDs and changes the acting primary while the
// outcome of the append is unknown. The owned pool has one PG there, so the
// image header, the journal header and the journal objects share one acting
// set. Once the unknown outcome is recorded and natively observed, the
// fixture marks that PG's primary out. The OSD keeps running, but another
// member of the acting set is the acting primary from the next epoch on;
// `ceph osd map` before and after is the native record of that. A native
// observation through the new primary shows the entry, the lock and the
// commit position where they were. The subject then closes its connection,
// reattaches its watch on the new primary and sends the append once more as
// an explicit resubmission: the original transaction with retry attempt 1.
// The answer must be a known 0 with the object's
// unchanged version, and a further native observation must find the journal
// byte-identical: the new primary answered from the replicated PG log and did
// not append again. Release and native replay follow as in the single-OSD
// sequence. See integration/fixture/RBD_JOURNAL_PRIMARY_CHANGE_VALIDATION.md.
func TestCephRBDJournalPrimaryChangeIntegration(t *testing.T) { runRBDJournalLostAppend(t, true) }

func runRBDJournalLostAppend(t *testing.T, primaryChange bool) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD fixture (CEPH_MSGR_TEST_RBD=1) under its exact selector")
	}
	if primaryChange && (os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore") {
		t.Skip("requires the RBD fixture on three replicated BlueStore OSDs")
	}
	var oracle nativeRBDOracle
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	token, identity := oracle.Token, os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.Plan.Pool != "ceph-msgr-rbd-"+token || oracle.Pool.Name != oracle.Plan.Pool ||
		oracle.Pool.Flags&1 != 0 || identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" || oracle.Caps["osd"] != "profile rbd pool="+oracle.Plan.Pool {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity)
	}
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_RBD_BASE_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	wantType, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != wantType {
		t.Fatal("RBD credential does not match the selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	options.Identity, options.EnableOSD = identity, true
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	results, outcomeFile := "rbd-journal-lost-append-results.json", "rbd-journal-lost-append-outcome.json"
	if primaryChange {
		results, outcomeFile = "rbd-journal-primary-change-results.json", "rbd-journal-primary-change-outcome.json"
	}

	// The observer is a second Go client with its own connections. It
	// supplies placement and, in undecoded mode, the size reads of the
	// barrier. It is not evidence of the journal's state: the native actions
	// are.
	observer, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate the observer Go client", err)
	}
	defer observer.Close()
	// The subject: every OSD connection it dials goes through the gate.
	mode := "secure"
	if options.ConnectionMode == cephmsgr.CRCMode {
		mode = "crc"
	}
	gate := testcluster.NewOSDReplyGate(mode == "crc", rbdJournalLostAppendThreshold)
	var osdDials atomic.Int32
	subjectOptions := options
	// The native observations run while the subject's connection is silent;
	// a longer silence bound keeps the local outcome a cancellation rather
	// than a keepalive failure.
	subjectOptions.KeepaliveTimeout = 5 * time.Minute
	subjectOptions.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, kind, _ := testcluster.SplitPeer(endpoint)
		conn, err := options.DialContext(ctx, network, endpoint)
		if err != nil || !strings.HasPrefix(kind, "osd.") {
			return conn, err
		}
		osdDials.Add(1)
		return gate.Wrap(conn), nil
	}
	subject, err := cephmsgr.Dial(ctx, subjectOptions)
	if err != nil {
		t.Fatal("authenticate the subject Go client", err)
	}
	defer subject.Close()
	gid := subject.Snapshot().GlobalID
	if subject.Snapshot().OSDTicket.Expires.IsZero() || gid == 0 || gid == observer.Snapshot().GlobalID {
		t.Fatal("subject client identity or OSD ticket missing")
	}

	record := rbdJournalLostAppendRecord{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, SubjectGlobalID: gid, ObserverGlobalID: observer.Snapshot().GlobalID,
		ConnectionMode: mode, Views: map[string]*rbdJournalLostAppendView{}, Timings: map[string]string{}}
	if primaryChange {
		record.PrimaryChange = &rbdJournalPrimaryChange{}
	}
	publish := func() error {
		record.Timings["total"] = time.Since(started).String()
		state := gate.State()
		record.Gate = &state
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return testcluster.WritePublicEvidence(filepath.Join(dir, results), data)
	}
	defer func() {
		// A failed run keeps whatever it observed, with the last judged stage.
		if err := publish(); err != nil {
			t.Error("RBD journal lost-append evidence", err)
		}
	}()
	done := func(step string) { record.Order = append(record.Order, step) }
	// judge stops the run at the first stage whose claims do not hold, so a
	// contradiction is never followed by the next step of the handoff.
	judge := func(t *testing.T, stage string) {
		t.Helper()
		state := gate.State()
		record.Gate = &state
		record.ClaimsStage, record.Problems = stage, rbdJournalLostAppendClaims(&record, stage)
		if len(record.Problems) != 0 {
			t.Fatalf("claims at %s:\n%s", stage, strings.Join(record.Problems, "\n"))
		}
	}

	// One closed native action at a time, each answered with its receipt.
	var plan nativeRBDJournalPlan
	var entry *rbdJournalLostAppendEntry
	var cookie uint64
	native := func(t *testing.T, action string) *nativeRBDJournal {
		t.Helper()
		receipt := filepath.Join(dir, "rbd-journal-"+action+".json")
		if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		begun := time.Now()
		if err := testcluster.RequestControl(ctx, dir, "rbd-journal", "rbd-journal-completed", token, action); err != nil {
			t.Fatal("native RBD journal", action, err)
		}
		record.Timings["native_"+action] = time.Since(begun).String()
		var result nativeRBDJournalResult
		if err := readMDSFreshJSON(receipt, &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-journal-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != strings.HasPrefix(action, "observe-") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != oracle.Pool.ID ||
			(plan.Image != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD journal receipt differs", action, result.Error)
		}
		if len(result.Process.Stderr) != 0 {
			t.Fatalf("native RBD journal %s logged errors: %q", action, result.Process.Stderr)
		}
		for _, cli := range []*nativeRBDJournalCLI{n.Info, n.Status, n.Inspect} {
			if cli == nil {
				continue
			}
			stderr := cli.Stderr
			if cli == n.Inspect {
				// The one tolerated line of the journal acceptance: the
				// inspector's own unregister notification.
				var kept []string
				for _, line := range strings.Split(strings.TrimRight(stderr, "\n"), "\n") {
					if rbdJournalInspectShutdownLine.MatchString(line) {
						record.InspectShutdownLines++
					} else if line != "" {
						kept = append(kept, line)
					}
				}
				stderr = strings.Join(kept, "\n")
			}
			if cli.ReturnCode != 0 || stderr != "" {
				t.Fatalf("native rbd %v failed: %d %q", cli.Argv, cli.ReturnCode, cli.Stderr)
			}
		}
		record.NativeActions = append(record.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		view, err := rbdJournalLostAppendViewOf(dir, result, entry, gid, cookie)
		if err != nil {
			t.Fatal(action, err)
		}
		record.Views[action] = view
		done(action)
		return n
	}

	setup := native(t, "setup")
	var setupResult nativeRBDJournalResult
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-journal-setup.json"), &setupResult); err != nil {
		t.Fatal(err)
	}
	plan = setupResult.Plan
	imageID := setup.ImageID
	journalHeader, prefix, header := "journal."+imageID, fmt.Sprintf("journal_data.%d.%s.", oracle.Pool.ID, imageID), "rbd_header."+imageID
	if plan.Pool != oracle.Plan.Pool || plan.Image != "ceph-msgr-journal-"+token || len(plan.GoWrites) == 0 || plan.Features != rbdJournalImageFeatures ||
		setup.JournalHeader != journalHeader || setup.JournalPrefix != prefix || setup.HeaderObject != header || setup.Features != plan.Features ||
		!setup.Lockers.Empty || len(setup.Watchers) != 0 || len(setup.ClassCalls) != 11 {
		t.Fatal("native RBD journal image setup differs", plan, setup.ImageID, setup.Lockers)
	}
	record.Image, record.ImageID, record.JournalHeader = plan.Image, imageID, journalHeader
	// The expected image: the native setup events, then the one Go extent
	// once native librbd has replayed the entry.
	expected := make([]byte, plan.Size)
	if len(setup.Events) != len(plan.SetupEvents) {
		t.Fatal("native setup events differ from the plan")
	}
	for i, e := range plan.SetupEvents {
		a := setup.Events[i]
		if a.Kind != e.Kind || a.Offset != e.Offset || a.Length != e.Length {
			t.Fatal("native setup event differs from the plan", a)
		}
		if e.Kind == "write" {
			copy(expected[e.Offset:], rbdJournalPattern(token, "setup", e.Offset, e.Length))
		} else {
			clear(expected[e.Offset : e.Offset+e.Length])
		}
	}
	digest := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	record.ExpectedHeadBefore = digest(expected)
	if record.Views["setup"].ImageHeadSHA256 != record.ExpectedHeadBefore {
		t.Fatal("native librbd image read differs from the expected image after setup")
	}
	// Journal metadata as native librados exec returned it.
	var metadata []byte
	for _, call := range setup.ClassCalls[:5] {
		out, err := hex.DecodeString(call.OutputHex)
		if err != nil || call.Code != 0 {
			t.Fatal("native journal metadata exec failed", call)
		}
		metadata = append(metadata, out...)
	}
	md := &rbdDecoder{data: metadata}
	order, splay, _, minimumSet, activeSet := md.u8(), md.u8(), md.u64(), md.u64(), md.u64()
	if err := md.done(); err != nil || order != rbdJournalOrder || splay != rbdJournalSplay || minimumSet != 0 || activeSet != 0 {
		t.Fatal("journal metadata differs", order, splay, minimumSet, activeSet, err)
	}
	imageOutput, err := hex.DecodeString(setup.ClassCalls[6].OutputHex)
	if err != nil || setup.ClassCalls[6].InputHex != hex.EncodeToString(rbdString("")) {
		t.Fatal("native get_client of the image client differs")
	}
	imageClient, err := decodeRBDJournalClient(imageOutput)
	if err != nil || len(imageClient.Positions) == 0 || !reflect.DeepEqual(imageClient.Positions, record.Views["setup"].ImageCommit) {
		t.Fatal("image journal client differs between native exec and rbd journal status", imageClient, err)
	}
	_, class, _, err := decodeRBDJournalImageClientMeta(imageClient.Data)
	if err != nil {
		t.Fatal(err)
	}

	// Subject requests: caller-side placement from the observer's MON
	// commands, every request recorded before it is judged.
	addresses := osdDumpAddresses(t, ctx, observer)
	route := func(t *testing.T, object string) rbdRoute {
		t.Helper()
		p, err := recoveryObjectPlacement(ctx, observer, plan.Pool, object)
		if err != nil {
			t.Fatal(err)
		}
		r := rbdRoute{Object: object, Pool: p.PoolID, Hash: p.Hash, Epoch: p.Epoch, RawPGID: p.RawPGID, PGID: p.PGID, Primary: p.Primary, Address: addresses[p.Primary]}
		if r.Pool != oracle.Pool.ID || r.Primary < 0 || r.Address == "" || p.Acting[0] != p.Primary {
			t.Fatal("caller-side RBD object placement differs", r)
		}
		return r
	}
	request := func(r rbdRoute, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		return cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}, Operations: ops}
	}
	keep := func(label string, r rbdRoute, req cephmsgr.OSDRequest, reply cephmsgr.OSDReply, err error) {
		e := rbdWriteRequestEvidence{Label: label, Route: r, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion, LegacyVersion: reply.LegacyVersion, ReplayVersion: reply.ReplayVersion}
		if req.ModificationTime != (cephmsgr.OSDTime{}) {
			mtime := req.ModificationTime
			e.ModificationTime = &mtime
		}
		for _, op := range req.Operations {
			e.Inputs = append(e.Inputs, rbdOperationInputEvidence{Code: uint16(op.Code), Offset: op.Offset, Length: op.Length, DataLength: len(op.Data), DataSHA256: digest(op.Data),
				Class: op.Class.Class, Method: op.Class.Method, Mode: uint8(op.Class.Mode)})
		}
		for _, op := range reply.Operations {
			e.Operations = append(e.Operations, rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: digest(op.Data)})
		}
		if err != nil {
			e.Error = err.Error()
		}
		record.Requests = append(record.Requests, e)
	}
	now := func() cephmsgr.OSDTime {
		t := time.Now()
		return cephmsgr.OSDTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
	}
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	// The subject keeps one connection per OSD it talks to. On the single-OSD
	// fixture that is one connection until it is closed. dialOf is the
	// position of each connection in the subject's OSD dials, which is how
	// the gate numbers them.
	conns, dialOf := map[int32]*cephmsgr.OSDConnection{}, map[*cephmsgr.OSDConnection]int{}
	connect := func(t *testing.T, r rbdRoute) *cephmsgr.OSDConnection {
		t.Helper()
		if o := conns[r.Primary]; o != nil {
			return o
		}
		dials := osdDials.Load()
		o, err := subject.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.Primary, Address: r.Address})
		if err != nil {
			t.Fatalf("subject explicit connection to osd.%d: %v", r.Primary, err)
		}
		if osdDials.Load() != dials+1 {
			t.Fatal("the subject's OSD transport was not identified", osdDials.Load())
		}
		conns[r.Primary], dialOf[o] = o, int(dials+1)
		return o
	}
	closeAll := func() error {
		var errs []error
		for id, o := range conns {
			errs = append(errs, o.Close())
			delete(conns, id)
		}
		return errors.Join(errs...)
	}
	// send issues one request and requires a successful reply with the
	// subject's identity. Without a connection it uses the subject's
	// connection to the object's primary.
	send := func(t *testing.T, o *cephmsgr.OSDConnection, id uint64, label, object string, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		r := route(t, object)
		if o == nil {
			o = connect(t, r)
		}
		req := request(r, ops...)
		mutation := slices.ContainsFunc(ops, func(op cephmsgr.OSDOperation) bool {
			return op.Code == cephmsgr.OSDCall && op.Class.Mode == cephmsgr.OSDCallWrite
		})
		if mutation {
			req.ModificationTime = now()
		}
		reply, err := o.Request(ctx, req)
		keep(label, r, req, reply, err)
		if err != nil {
			t.Fatalf("Go %s: %v", label, err)
		}
		if reply.Code != 0 || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != id || reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) ||
			reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != len(ops) ||
			mutation && (reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion == 0) {
			t.Fatalf("Go %s: OSD reply identity differs: code=%d tid=%d gid=%d", label, reply.Code, reply.Transaction, reply.GlobalID)
		}
		return reply
	}
	headerRoute := route(t, header)
	first := connect(t, headerRoute)

	// librbd's managed lock: the subject's image header watch handle is the
	// lock cookie.
	watchRequest := request(headerRoute)
	watchRequest.Operations = nil
	w, reply, err := first.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: rbdJournalLostAppendWatchTimeout})
	keep("image_watch_register", headerRoute, watchRequest, reply, err)
	if err != nil {
		t.Fatal("subject image header watch", err)
	}
	cookie = w.Cookie()
	lockCookie := rbdLockCookie(cookie)
	record.WatchCookie, record.LockCookie = cookie, lockCookie
	// The OSD keeps a watch for its timeout after the last ping, and the
	// registration counts as one. This test sends no ping: a ping issued while
	// replies are withheld would be one more request without an answer. The
	// subject must therefore reattach its watch within the timeout, which the
	// test checks before it does so.
	watched := time.Now()
	locked, unlocked := false, false
	defer func() {
		// Compensation after a failure: release the lock on a connection
		// that still delivers replies. The uncertain append is never resent.
		if locked && !unlocked {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_ = closeAll()
			// The header's primary may have changed since the lock was taken.
			r := headerRoute
			if p, err := recoveryObjectPlacement(cleanup, observer, plan.Pool, header); err == nil && addresses[p.Primary] != "" {
				r.Epoch, r.Primary, r.Address = p.Epoch, p.Primary, addresses[p.Primary]
			}
			if o, err := subject.OpenOSD(cleanup, cephmsgr.OSDTarget{ID: r.Primary, Address: r.Address}); err == nil {
				req := request(r, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(lockCookie)))
				req.ModificationTime = now()
				reply, err := o.Request(cleanup, req)
				keep("lock_unlock_compensate", r, req, reply, err)
			}
		}
	}()
	locked = true
	send(t, nil, gid, "lock_lock", header, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(lockCookie)))
	r := send(t, nil, gid, "lock_get_info", header, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
	info, err := decodeRBDLockInfo(r.Operations[0].Data)
	if err != nil || len(info.Lockers) != 1 || info.Lockers[0].NameNumber != int64(gid) || info.Lockers[0].Cookie != lockCookie || info.Tag != rbdLockTag {
		t.Fatal("the subject does not hold the image's exclusive lock", info, err)
	}
	done("lock")
	// Journal open after lock acquisition (librbd/Journal.cc
	// allocate_local_tag), as in the journal acceptance.
	r = send(t, nil, gid, "get_client_image", journalHeader, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString("")))
	if !bytes.Equal(r.Operations[0].Data, imageOutput) {
		t.Fatal("image journal client changed since the native setup observation")
	}
	r = send(t, nil, gid, "get_next_tag_tid", journalHeader, classOp("journal", "get_next_tag_tid", cephmsgr.OSDCallRead, nil))
	tag := binary.LittleEndian.Uint64(r.Operations[0].Data)
	front := imageClient.Positions[0]
	tagData := rbdJournalTagDataEncode(rbdJournalTagData{CommitValid: true, PredecessorTag: front.Tag, PredecessorEnt: front.Entry})
	send(t, nil, gid, "tag_create", journalHeader, classOp("journal", "tag_create", cephmsgr.OSDCallWrite, append(append(rbdLE64(tag), rbdLE64(class)...), rbdJournalBL(tagData)...)))
	done("tag_create")

	// The owned entry: the first Go write of the journal plan as entry 0 of
	// the new tag, at splay offset 0 of the active object set.
	extent := plan.GoWrites[0]
	at := now()
	entryBytes := rbdJournalEntryEncode(tag, 0, rbdJournalWriteEvent(extent.Offset, rbdJournalPattern(token, "go-lost-append", extent.Offset, extent.Length), at))
	object := activeSet * uint64(splay)
	entry = &rbdJournalLostAppendEntry{Tag: tag, EntryTID: 0, Object: object, ObjectName: prefix + strconv.FormatUint(object, 10), Offset: int(setup.JournalObjects[object].Size), Length: len(entryBytes),
		SHA256: digest(entryBytes), ImageOffset: extent.Offset, ImageLength: extent.Length, Timestamp: at}
	record.Entry = *entry
	after := slices.Clone(expected)
	copy(after[extent.Offset:], rbdJournalPattern(token, "go-lost-append", extent.Offset, extent.Length))
	record.ExpectedHeadAfter = digest(after)
	record.ExpectedCommitAfter = rbdJournalCommitPosition(imageClient.Positions, []rbdJournalEntry{{Object: object, Tag: tag, EntryID: 0}}, uint64(splay))

	lockedNative := native(t, "observe-locked")
	objectsBlob := readRBDBlob(t, dir, lockedNative.Blobs["journal_objects"])
	nativeBefore := objectsBlob[lockedNative.JournalObjects[object].Offset : lockedNative.JournalObjects[object].Offset+lockedNative.JournalObjects[object].Size]
	// A read on the subject's own connection passes the armed-to-be gate and
	// equals the native read: the gate withholds nothing yet.
	r = send(t, nil, gid, "append_before", entry.ObjectName, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 256 << 10})
	if !bytes.Equal(r.Operations[1].Data, nativeBefore) || len(nativeBefore) != entry.Offset {
		t.Fatal("journal object before the append differs between Go and native librados", len(r.Operations[1].Data), len(nativeBefore), entry.Offset)
	}
	if s := gate.State(); s.LargeWrites != 0 || s.Bound || s.Connections != len(conns) {
		t.Fatal("the subject already wrote a frame of the append's size, so the gate cannot single out the append", s)
	}
	if err := gate.Arm(entryBytes); err != nil {
		t.Fatal(err)
	}

	// The one append. It is submitted once and never again.
	targetRoute := route(t, entry.ObjectName)
	if !primaryChange && targetRoute.Primary != headerRoute.Primary {
		t.Fatal("journal object and image header are on different OSDs; the single-OSD fixture is required")
	}
	// The control read above opened this connection, so the append is its
	// only write of the gate's size.
	appendConn := conns[targetRoute.Primary]
	if appendConn == nil {
		t.Fatal("the subject has no connection to the journal object's primary")
	}
	if pc := record.PrimaryChange; pc != nil {
		pc.Objects = []string{header, entry.ObjectName, journalHeader}
		pc.LockPrimary, pc.AppendPrimary, pc.AppendConnection = headerRoute.Primary, targetRoute.Primary, dialOf[appendConn]
	}
	appendRequest := request(targetRoute, classOp("journal", "append", cephmsgr.OSDCallWrite, append(rbdLE64(1<<order), rbdJournalBL(entryBytes)...)))
	appendRequest.ModificationTime = now()
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	invocations := 1
	go func() {
		reply, err := appendConn.Request(callCtx, appendRequest)
		finished <- result{reply, err}
	}()
	done("append_submitted")
	select {
	case <-gate.Bound():
	case got := <-finished:
		keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
		t.Fatal("the append ended before the gate bound to it", got.reply.Code, got.err)
	case <-ctx.Done():
		t.Fatal("the gate did not bind to the append", ctx.Err())
	}
	if mode == "crc" {
		// The held frame is the barrier: the OSD replied to this
		// transaction, and no byte of the reply reached the session.
		select {
		case <-gate.Held():
		case got := <-finished:
			keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
			t.Fatal("the append completed although its reply must be withheld", got.reply.Code, got.err)
		case <-ctx.Done():
			t.Fatal("the reply to the append did not arrive", ctx.Err())
		}
		record.Barrier = "held reply"
	} else {
		// No frame can be identified. Wait on the state itself: the observer
		// reads the object's size until it is the earlier size plus the
		// entry. Any other size, or the end of the caller, fails at once.
		o, err := observer.OpenOSD(ctx, cephmsgr.OSDTarget{ID: targetRoute.Primary, Address: targetRoute.Address})
		if err != nil {
			t.Fatal("observer explicit OSD connection", err)
		}
		polls := 0
		record.BarrierPolls = &polls
		for size := uint64(0); size != uint64(entry.Offset+entry.Length); {
			select {
			case got := <-finished:
				keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
				t.Fatal("the append completed although its reply must be withheld", got.reply.Code, got.err)
			case <-ctx.Done():
				t.Fatal("the append was not applied", ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
			stat, err := o.Request(ctx, request(targetRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
			polls++
			if err != nil || len(stat.Operations) != 1 || len(stat.Operations[0].Data) < 8 {
				t.Fatal("observer size read", err)
			}
			if size = binary.LittleEndian.Uint64(stat.Operations[0].Data); size != uint64(entry.Offset) && size != uint64(entry.Offset+entry.Length) {
				t.Fatal("journal object has a size that is neither before nor after the append", size)
			}
		}
		record.Barrier = "polled object size"
		// The size shows the application, not the injection. Wait, while
		// the call is still pending, until the gate has intercepted received
		// bytes on the bound connection. Which frame they belong to stays
		// unknown.
		withheld := time.NewTimer(rbdJournalLostAppendWithheldWait)
		defer withheld.Stop()
		select {
		case <-gate.Held():
		case got := <-finished:
			keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
			t.Fatal("the append completed although its reply must be withheld", got.reply.Code, got.err)
		case <-withheld.C:
			t.Fatal("the append was applied but the gate intercepted no received byte on its connection within ", rbdJournalLostAppendWithheldWait)
		case <-ctx.Done():
			t.Fatal("the gate intercepted no received byte on the append's connection", ctx.Err())
		}
	}
	done("barrier")

	// Application: one native checkpoint while the caller is still pending
	// and the subject still holds the lock.
	native(t, "checkpoint")
	pending := false
	select {
	case got := <-finished:
		keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
		t.Fatal("the withheld append completed before cancellation", got.reply.Code, got.err)
	default:
		pending = true
	}
	record.PendingAfterCheckpoint = &pending
	judge(t, "checkpoint")

	// The undecoded gate's flag is read while the call is still pending: a
	// read intercepted later, during the handoff, would not establish the
	// injection.
	if mode != "crc" {
		atCancel := gate.State().WithheldBytesSeen
		if atCancel == nil || !*atCancel {
			t.Fatal("the undecoded gate withheld no received byte before the wait was canceled")
		}
		record.WithheldBeforeCancel = atCancel
	}

	// Outcome: cancel only the local wait and record what the caller was
	// told, before anything else is done.
	cancelCall()
	got := <-finished
	keep("journal_append", targetRoute, appendRequest, got.reply, got.err)
	var unknown *cephmsgr.OutcomeUnknownError
	outcomeClass, detail := rbdMutationOutcome(got.reply, got.err)
	record.Outcome = &rbdJournalLostAppendOutcome{Label: "journal_append", CallerInvocations: invocations, Class: outcomeClass, UnknownOutcome: errors.As(got.err, &unknown),
		Canceled: errors.Is(got.err, context.Canceled), Error: detail, Submission: got.reply.Submission, ReplyTransaction: got.reply.Transaction, ReplyCode: got.reply.Code,
		RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
	outcome, err := json.Marshal(map[string]any{"token": token, "image_id": imageID, "subject_global_id": gid, "entry": record.Entry, "outcome": record.Outcome})
	if err == nil {
		err = testcluster.WritePublicEvidence(filepath.Join(dir, outcomeFile), outcome)
	}
	if err != nil {
		t.Fatal("record the unknown outcome", err)
	}
	if !record.Outcome.UnknownOutcome || !record.Outcome.Canceled || outcomeClass != "uncertain" || got.reply.Submission.Transaction == 0 {
		t.Fatal("the canceled append did not end with an unknown outcome and its submission", got.reply.Submission, got.err)
	}
	done("outcome_recorded")

	// Order: a second native observation after the record. The lock, the
	// commit position, the tags and every journal byte are where they were.
	native(t, "observe-recorded")
	judge(t, "observe-recorded")

	if pc := record.PrimaryChange; pc != nil {
		// The primary change, while the outcome is unknown and the lock is
		// held. The test resolves the three placements through its own MON
		// commands before and after; the fixture's `ceph osd map` reads
		// around its `osd out` command are the native record.
		placements := func(t *testing.T) []rbdJournalPlacementView {
			t.Helper()
			var views []rbdJournalPlacementView
			for _, object := range pc.Objects {
				p, err := recoveryObjectPlacement(ctx, observer, plan.Pool, object)
				if err != nil {
					t.Fatal(err)
				}
				views = append(views, rbdJournalPlacementView{Object: object, Epoch: p.Epoch, PGID: p.PGID, Primary: p.Primary, Acting: p.Acting})
			}
			return views
		}
		pc.GoBefore = placements(t)
		native(t, "change-primary")
		var receipt struct {
			PrimaryChange *nativeRBDJournalPrimaryChange `json:"primary_change"`
		}
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-journal-change-primary.json"), &receipt); err != nil || receipt.PrimaryChange == nil {
			t.Fatal("fixture primary-change receipt missing", err)
		}
		pc.Native, pc.GoAfter = receipt.PrimaryChange, placements(t)
		judge(t, "change-primary")
	}

	// Only now the handoff. The silent connection is closed; the subject
	// opens a new one, reattaches its watch (the lock cookie names it),
	// confirms its ownership, releases the lock and unwatches. On the
	// single-OSD fixture it sends no append, applies nothing and commits
	// nothing; after a primary change it resubmits the append once.
	before := int(osdDials.Load())
	record.DialsBeforeClose = &before
	if err := closeAll(); err != nil {
		t.Fatal("close the silent OSD connection", err)
	}
	done("blocked_connection_closed")
	if primaryChange {
		// The header has a new primary; requests name the current epoch.
		headerRoute = route(t, header)
		watchRequest = request(headerRoute)
		watchRequest.Operations = nil
	}
	second := connect(t, headerRoute)
	silent := time.Since(watched)
	record.Timings["watch_without_ping"] = silent.String()
	if silent > (rbdJournalLostAppendWatchTimeout-5)*time.Second {
		t.Fatalf("the subject's watch went %v without a ping; its %d s timeout bounds this sequence, so the lock cookie may no longer name a live watch", silent, rbdJournalLostAppendWatchTimeout)
	}
	w2, reply, err := second.ReconnectWatch(ctx, w, watchRequest)
	keep("image_watch_reconnect", headerRoute, watchRequest, reply, err)
	if err != nil || w2.Cookie() != cookie {
		t.Fatal("subject image header watch reconnect", err)
	}
	done("watch_reconnected")
	r = send(t, nil, gid, "lock_get_info_after", header, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
	if info, err = decodeRBDLockInfo(r.Operations[0].Data); err != nil || len(info.Lockers) != 1 || info.Lockers[0].NameNumber != int64(gid) || info.Lockers[0].Cookie != lockCookie {
		t.Fatal("the subject lost the image's exclusive lock", info, err)
	}
	if pc := record.PrimaryChange; pc != nil {
		// The lock record keeps the address of the connection that sent
		// lock.lock, and the watch record that of the connection that
		// registered it: reattaching the watch does not rewrite it.
		pc.LockerAddress = fmt.Sprintf("%s/%d", info.Lockers[0].Address.Endpoint, info.Lockers[0].Address.Nonce)
		listed := send(t, nil, gid, "list_watchers_after", header, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
		for _, watcher := range listed.Operations[0].Watchers {
			if uint64(watcher.NameNumber) == gid && watcher.Cookie == cookie {
				pc.WatcherAddress = fmt.Sprintf("%s/%d", watcher.Address.Endpoint, watcher.Address.Nonce)
			}
		}
		// The explicit resubmission: the same operations and modification
		// time under the original transaction, retry attempt 1, routed to
		// the journal object's new primary. It is sent once. Whatever comes
		// back is recorded before it is judged, and nothing follows an
		// answer that is not a known 0.
		version := func(label string) uint64 {
			return send(t, nil, gid, label, entry.ObjectName, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}).UserVersion
		}
		pc.VersionBefore = version("append_version_before")
		newRoute := route(t, entry.ObjectName)
		retry := request(newRoute, appendRequest.Operations...)
		retry.ModificationTime = appendRequest.ModificationTime
		retry.Resubmit, retry.RetryAttempt = &record.Outcome.Submission, 1
		resubmitConn := connect(t, newRoute)
		pc.ResubmitConnection = dialOf[resubmitConn]
		reply, err := resubmitConn.Request(ctx, retry)
		keep("journal_append_resubmit", newRoute, retry, reply, err)
		class, detail := rbdMutationOutcome(reply, err)
		pc.Resubmission = &rbdJournalResubmission{CallerInvocations: 1, Class: class, UnknownOutcome: errors.As(err, &unknown), Error: detail, Primary: newRoute.Primary, RetryAttempt: retry.RetryAttempt,
			Submission: reply.Submission, ReplyTransaction: reply.Transaction, ReplyCode: reply.Code, ReplyRetryAttempt: reply.RetryAttempt, ReplyMapEpoch: reply.MapEpoch,
			OnDisk: reply.Flags&cephmsgr.OSDReplyOnDisk != 0, UserVersion: reply.UserVersion, RecordedUTC: time.Now().UTC().Format(time.RFC3339Nano)}
		if err != nil || reply.Code != 0 {
			t.Fatal("the resubmitted append was not answered with a known 0; nothing is sent again", reply.Code, err)
		}
		pc.VersionAfter = version("append_version_after")
		done("resubmitted")
		native(t, "observe-resubmitted")
		judge(t, "observe-resubmitted")
	}
	send(t, nil, gid, "lock_unlock", header, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(lockCookie)))
	unlocked = true
	done("unlock")
	reply, err = w2.Unwatch(ctx, headerRoute.Epoch)
	keep("image_watch_unwatch", headerRoute, watchRequest, reply, err)
	if err != nil {
		t.Fatal("subject image header unwatch", err)
	}
	done("unwatch")

	// Native librbd acquires the lock, opens the journal and replays.
	replay := native(t, "replay")
	if replay.Owner == nil || !*replay.Owner {
		t.Fatal("native librbd did not acquire the exclusive lock")
	}
	total := int(osdDials.Load())
	record.DialsTotal = &total
	judge(t, "replay")
	state := gate.State()
	transaction := "not observed"
	if state.Transaction != nil {
		transaction = strconv.FormatUint(*state.Transaction, 10)
	}
	t.Logf("go lost journal append mode=%s tag=%d entry=0 object=%s offset=%d length=%d submission_tid=%d gate_tid=%s large_writes=%d dials=%d+%d watch_without_ping=%s barrier=%q",
		mode, tag, entry.ObjectName, entry.Offset, entry.Length, record.Outcome.Submission.Transaction, transaction, state.LargeWrites, before, total-before,
		silent.Round(time.Millisecond), record.Barrier)
}
