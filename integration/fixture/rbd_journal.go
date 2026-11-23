package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The RBD journaling acceptance reuses the RBD base scope (token, owned pool
// and both identities) and adds one more native librbd image to the owned
// pool, created only on the test's request, with the exclusive-lock and
// journaling features. The Go test reads the image's journal header and
// journal data objects through the explicit OSD API, registers its own
// journal client and commits positions as a mirror-like consumer would,
// watches the journal header while native librbd writes, and appends journal
// entries in librbd's format under the image's exclusive lock. The closed
// control here only lets native librbd create the image (with one native
// journal client registered at an empty commit position, so that `rbd
// journal inspect` replays every entry), write and discard through librbd,
// observe the journal with librados `exec`, librados reads and the `rbd
// journal info`, `status` and `inspect` commands, and finally replay the
// journal by acquiring the exclusive lock, and run one bounded concurrent
// session in which native librbd and the Go test hand the image's exclusive
// lock to each other while both append to the same journal (proceed signals
// rbd-journal-proceed-<n>, published state rbd-journal-state.json). Every
// native action is journaled
// in rbd-base-owned.json before it is issued and is never reissued; the image
// goes with the owned pool at cleanup. The journal encodings, decodings and
// the append protocol stay in the test-only consumer
// (integration/rbd_journal_test.go). Native behavior relied upon:
// cls/journal/cls_journal.cc, cls/journal/cls_journal_client.cc,
// cls/journal/cls_journal_types.cc, journal/Entry.cc,
// journal/JournalMetadata.cc, journal/JournalRecorder.cc,
// journal/ObjectRecorder.cc, journal/Journaler.cc, librbd/Journal.cc,
// librbd/journal/CreateRequest.cc, librbd/journal/Types.cc,
// tools/rbd/action/Journal.cc, pybind/rbd/rbd.pyx and pybind/rados/rados.pyx
// at rbdBaseSource (LGPL-2.1 references; no code copied).

// rbdJournalActions is the closed, ordered action sequence of the journal
// control. Each action runs at most once and only after its predecessors.
var rbdJournalActions = []string{"setup", "observe-registered", "write", "observe-committed", "inspect", "replay", "concurrent"}

// capRBDJournalLostAppend admits the lost-append-reply acceptance. Its test
// runs alone on a fresh RBD fixture, and the journal control replaces the
// ordinary sequence with rbdJournalLostAppendActions.
var capRBDJournalLostAppend = defineExclusiveCapability(fixtureRBD, "rbd-journal-lost-append",
	"run one test alone on a fresh RBD fixture with the lost-append-reply journal sequence",
	"RBD=1", nil)

// rbdJournalLostAppendActions is the closed, ordered action sequence of the
// lost-append-reply acceptance: the setup, an observation once the Go test
// holds the image's exclusive lock and its journal tag, a checkpoint (with
// `rbd journal inspect`) while the reply to the Go append is withheld, an
// observation after the Go test recorded its unknown outcome and before it
// releases the lock, and the native replay. The driver does the same work as
// in the ordinary sequence; only the order and the receipt names differ.
var rbdJournalLostAppendActions = []string{"setup", "observe-locked", "checkpoint", "observe-recorded", "replay"}

// capRBDJournalPrimaryChange admits the lost-append-reply sequence on three
// replicated BlueStore OSDs, with a change of the acting primary before the
// Go test resolves its unknown outcome. Its test runs alone on a fresh RBD
// fixture of that topology (profile.go rbdReplicas), whose owned pool has a
// single PG, so that the image header, the journal header and every journal
// object share one acting set and one primary.
var capRBDJournalPrimaryChange = defineExclusiveCapability(fixtureRBD, "rbd-journal-primary-change",
	"run one test alone on a fresh three-OSD RBD fixture with the primary-change journal sequence",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// rbdJournalPrimaryChangeActions is the lost-append-reply sequence with two
// more actions before the replay. `change-primary` first lets the fixture
// mark the OSD that leads the owned pool's PG out, which removes it from the
// PG's up set and makes one of the two other members the acting primary
// (osd/OSDMap.cc _pg_to_raw_osds and _raw_to_up_osds at rbdBaseSource), and
// then runs the ordinary observation. The OSD keeps running and stays in the
// acting set as its last member. `observe-resubmitted` is the observation after the Go test
// resubmitted its append to the new primary.
//
// The primary is not moved with `osd primary-affinity`: an OSDMap with a
// non-default primary affinity requires CEPH_FEATURE_OSD_PRIMARY_AFFINITY of
// every client (osd/OSDMap.cc get_features), which the legacy-v6 Go client
// does not advertise, so every OSD would refuse its next connection.
var rbdJournalPrimaryChangeActions = []string{"setup", "observe-locked", "checkpoint", "observe-recorded", "change-primary", "observe-resubmitted", "replay"}

// capRBDJournalOwnerLoss admits the lost-append-reply sequence ending in
// the death of the lock owner whose append is uncertain: its test runs alone
// on a fresh RBD fixture, and the journal control replaces the ordinary
// sequence with rbdJournalOwnerLossActions.
var capRBDJournalOwnerLoss = defineExclusiveCapability(fixtureRBD, "rbd-journal-owner-loss",
	"run one test alone on a fresh RBD fixture with the journal owner-loss sequence (native lock break and blocklist)",
	"RBD=1", nil)

// rbdJournalOwnerLossActions is the lost-append-reply sequence up to the
// observation after the recorded unknown outcome, followed by `takeover`
// instead of the replay. The Go owner never releases the lock and its watch
// is no longer renewed. The takeover lists the lock record and the header
// watchers right before its first write (its boundary). Its librbd writes
// then take the lock as the automatic policy does: the run is set up for
// the path where librbd finds the owner's watch listed and requests the
// lock (RequestLock, which the Go test's observer watch receives), then
// blocklists the owner's address and breaks the lock once no watcher
// matches the record (managed_lock/AcquireRequest.cc send_break_lock,
// managed_lock/BreakRequest.cc), acquires it, replays the journal and
// journals and applies its own writes. This driver reports the boundary,
// what is listed after the takeover and the lock state machines' lines of
// librbd's debug log (lock_log); which branch librbd took is the test's to
// judge from them.
// Native behavior relied
// upon in addition to the list above: osd/Watch.cc (a watch is kept for its
// timeout after the last ping) at rbdBaseSource.
var rbdJournalOwnerLossActions = []string{"setup", "observe-locked", "checkpoint", "observe-recorded", "takeover"}

// capRBDJournalForcedBreak admits the forced-break sequence: its test runs
// alone on a fresh RBD fixture, and the journal control replaces the
// ordinary sequence with rbdJournalForcedBreakActions.
var capRBDJournalForcedBreak = defineExclusiveCapability(fixtureRBD, "rbd-journal-forced-break",
	"run one test alone on a fresh RBD fixture with the journal forced-break sequence (native lock break and blocklist)",
	"RBD=1", nil)

// rbdJournalForcedBreakActions is the forced-break sequence. After the setup
// the Go test takes the image's exclusive lock and appends one journal entry
// with a known answer, which it neither applies nor commits.
// `observe-appended` is the read-only observation of that entry while the
// Go owner's watch is live. `force-break` lets a native librbd instance that
// does not own the lock break it with librbd's lock_break, which blocklists
// the owner's address and removes the lock without asking whether the
// owner is alive (internal.cc lock_break, ManagedLock.cc break_lock,
// managed_lock/BreakRequest.cc with force_break_lock). `observe-fenced` is
// the read-only observation after the Go owner's fenced requests, and
// `takeover` the native writes that acquire the free lock, replay the
// journal and journal their own writes.
var rbdJournalForcedBreakActions = []string{"setup", "observe-locked", "observe-appended", "force-break", "observe-fenced", "takeover"}

// capRBDJournalForcedBreakPending admits the forced-break sequence in which
// the Go owner has two journal appends held at the OSD through the break:
// its test runs alone on a fresh RBD fixture, and the journal control
// replaces the ordinary sequence with rbdJournalForcedBreakPendingActions.
var capRBDJournalForcedBreakPending = defineExclusiveCapability(fixtureRBD, "rbd-journal-forced-break-pending",
	"run one test alone on a fresh RBD fixture with the journal forced-break sequence with pending appends (native lock break, blocklist and OSDMap epoch advance)",
	"RBD=1", nil)

// rbdJournalForcedBreakPendingActions is the forced-break sequence with
// appends pending at the OSD. After the Go owner's answered append
// (`observe-appended`) it sends two more appends that name an OSDMap epoch
// the cluster has not reached, so the OSD holds them (osd/OSD.cc
// dispatch_session_waiting at rbdBaseSource); `observe-pending` is the
// read-only observation while both are held. `force-break` breaks the lock
// as in the forced-break sequence. `advance-map` runs no native driver: the
// fixture raises the OSDMap epoch as the lock control's pending sequence
// does (rbdLockAdvanceMap in rbd_lock.go), reading the OSD's operations in
// flight before and after, which lets the OSD answer what it held.
// `observe-fenced` follows the answer, `observe-new-connection` the Go
// owner's requests on a new OSD connection, and `takeover` is the native
// takeover of the forced-break sequence.
var rbdJournalForcedBreakPendingActions = []string{"setup", "observe-locked", "observe-appended", "observe-pending", "force-break", "advance-map", "observe-fenced",
	"observe-new-connection", "takeover"}

// capRBDJournalForcedBreakInflight admits the forced-break sequence in which
// the Go owner's journal append has been applied by the OSD and nothing of
// its outcome has reached the Go caller when the lock is broken (the test's
// reply gate holds the reply frame on CRC connections and intercepts the
// received bytes on secure ones): its test runs alone
// on a fresh RBD fixture, and the journal control replaces the ordinary
// sequence with rbdJournalForcedBreakInflightActions.
var capRBDJournalForcedBreakInflight = defineExclusiveCapability(fixtureRBD, "rbd-journal-forced-break-inflight",
	"run one test alone on a fresh RBD fixture with the journal forced-break sequence with an append whose outcome is in flight (native lock break and blocklist)",
	"RBD=1", nil)

// rbdJournalForcedBreakInflightActions is the lost-append-reply sequence up
// to its checkpoint (taken while the Go owner still holds the lock and the
// outcome of its applied append has not reached it), then `force-break`
// while that is still so, `observe-fenced` after the Go caller recorded its
// unknown outcome, and the `takeover` of the forced-break sequence.
var rbdJournalForcedBreakInflightActions = []string{"setup", "observe-locked", "checkpoint", "force-break", "observe-fenced", "takeover"}

// capRBDJournalTrim admits the journal trimming sequence: its test runs alone
// on a fresh RBD fixture whose journaling image is created with the trim
// plan (rbdJournalTrimOrder, rbdJournalPlanFor), and the journal control
// replaces the ordinary sequence with rbdJournalTrimActions.
var capRBDJournalTrim = defineExclusiveCapability(fixtureRBD, "rbd-journal-trim",
	"run one test alone on a fresh RBD fixture with the journal trimming sequence (small journal objects, native oracle client disconnect, lock break and blocklist, native trim)",
	"RBD=1", nil)

// rbdJournalTrimActions is the trimming sequence. After the setup the Go
// test takes the image's exclusive lock and appends until the first object
// set is full, advances the active set and appends to the next one, applies
// and commits all but its last entry. `observe-committed` is the read-only
// observation of that journal. `disconnect-oracle` flags the native oracle
// client disconnected with `rbd journal client disconnect`, so that only the
// image client's commit position can keep an object set
// (journal/JournalTrimmer.cc handle_metadata_updated skips disconnected
// clients). `force-break` breaks the live Go owner's lock as in the
// forced-break sequence. `takeover` takes the free lock, replays the entry
// Go did not commit, journals its own write and, with the trim plan, keeps
// the image open until its own journal trimmer has raised the journal's
// minimum set or a bound passes (trim_wait).
var rbdJournalTrimActions = []string{"setup", "observe-locked", "observe-committed", "disconnect-oracle", "force-break", "takeover"}

// rbdJournalActionsFor returns the action sequence the profile admits.
func rbdJournalActionsFor(p profile) []string {
	switch {
	case p.admits(capRBDJournalForcedBreakInflight):
		return rbdJournalForcedBreakInflightActions
	case p.admits(capRBDJournalTrim):
		return rbdJournalTrimActions
	case p.admits(capRBDJournalLostAppend):
		return rbdJournalLostAppendActions
	case p.admits(capRBDJournalPrimaryChange):
		return rbdJournalPrimaryChangeActions
	case p.admits(capRBDJournalOwnerLoss):
		return rbdJournalOwnerLossActions
	case p.admits(capRBDJournalForcedBreak):
		return rbdJournalForcedBreakActions
	case p.admits(capRBDJournalForcedBreakPending):
		return rbdJournalForcedBreakPendingActions
	case p.admits(capRBDJournalReplicaHold):
		return rbdJournalReplicaHoldActions
	case p.admits(capRBDJournalHeldPrimaryChange):
		return rbdJournalHeldPrimaryChangeActions
	case p.admits(capRBDJournalDeadPrimary):
		return rbdJournalDeadPrimaryActions
	case p.admits(capRBDJournalConsumer):
		return rbdJournalConsumerActions
	}
	return rbdJournalActions
}

// rbdJournalBlocklisted names the actions that read the OSDMap blocklist:
// the observations and actions of the owner-loss and forced-break sequences
// from the moment a lock break can have happened.
func rbdJournalBlocklisted(action string) bool {
	return action == "observe-appended" || action == "observe-pending" || action == "force-break" || action == "observe-fenced" || action == "observe-new-connection" ||
		action == "takeover"
}

// rbdJournalLockLogged names the actions whose librbd debug log the driver
// filters into its lock_log output: the lock break and the takeover.
func rbdJournalLockLogged(action string) bool { return action == "force-break" || action == "takeover" }

// rbdJournalPlacement is the MON's placement of one object as `ceph osd map`
// prints it.
type rbdJournalPlacement struct {
	Epoch         uint32 `json:"epoch"`
	Pool          string `json:"pool"`
	PoolID        int64  `json:"pool_id"`
	Object        string `json:"objname"`
	RawPGID       string `json:"raw_pgid"`
	PGID          string `json:"pgid"`
	Up            []int  `json:"up"`
	UpPrimary     int    `json:"up_primary"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}

// settled says that p names one object of the pool on three distinct OSDs,
// with equal up and acting sets led by their first member.
func (p rbdJournalPlacement) settled(pool, object string) bool {
	members := slices.Clone(p.Acting)
	slices.Sort(members)
	return p.Epoch != 0 && p.Pool == pool && p.Object == object && p.PGID != "" && len(p.Acting) == 3 && len(slices.Compact(members)) == 3 &&
		slices.Equal(p.Up, p.Acting) && p.ActingPrimary == p.Acting[0] && p.UpPrimary == p.ActingPrimary
}

// movedFrom says that p is the placement the PG of before settles on once
// the OSD out was marked out: a newer epoch, the same PG, the two other
// members as the up set, and an acting set that is the up set followed by
// the OSD that was marked out. That OSD is still running and holds the
// data, so the new primary keeps it in the acting set with a pg_temp
// mapping (osd/PeeringState.cc choose_acting at rbdBaseSource) instead of
// leaving the PG with two copies.
func (p rbdJournalPlacement) movedFrom(before rbdJournalPlacement, out int) bool {
	rest := slices.DeleteFunc(slices.Clone(before.Acting), func(member int) bool { return member == out })
	up := slices.Clone(p.Up)
	slices.Sort(rest)
	slices.Sort(up)
	return p.Epoch > before.Epoch && p.Pool == before.Pool && p.Object == before.Object && p.PGID == before.PGID && len(rest) == 2 && slices.Equal(up, rest) &&
		slices.Equal(p.Acting, append(slices.Clone(p.Up), out)) && p.UpPrimary == p.Up[0] && p.ActingPrimary == p.Up[0]
}

// rbdJournalPrimaryMove is one object's placement before and after the
// primary was marked out.
type rbdJournalPrimaryMove struct {
	Object string              `json:"object"`
	Moved  bool                `json:"moved"`
	Before rbdJournalPlacement `json:"before"`
	After  rbdJournalPlacement `json:"after"`
}

// rbdJournalPrimaryChange is the administrative part of the `change-primary`
// receipt. Out is -1 until the OSD to mark out is known.
type rbdJournalPrimaryChange struct {
	Objects   []rbdJournalPrimaryMove `json:"objects"`
	Out       int                     `json:"osd_out"`
	Commands  []string                `json:"commands"`
	Polls     int                     `json:"polls"`
	ElapsedMS int64                   `json:"elapsed_ms"`
}

// rbdJournalPrimaryOps reads one object's placement and marks one OSD out.
// marking is called before that command with the OSD it is about to change,
// so that the cleanup marks it in again whatever the command's outcome.
type rbdJournalPrimaryOps struct {
	place   func(ctx context.Context, object string) (rbdJournalPlacement, error)
	out     func(ctx context.Context, osd int) error
	marking func(osd int) error
	bound   time.Duration
	pause   time.Duration
}

// rbdJournalChangePrimary moves the acting primary of the objects' PG by
// marking their one common primary out. Every object must start on a
// settled three-member placement led by that OSD. It succeeds once each
// object, in a newer epoch and in the same PG, has the two other members
// as its up set, led by one of them, with the old primary as the last member
// of the acting set.
func rbdJournalChangePrimary(ctx context.Context, pool string, objects []string, ops rbdJournalPrimaryOps) (r rbdJournalPrimaryChange, err error) {
	started := time.Now()
	r = rbdJournalPrimaryChange{Objects: []rbdJournalPrimaryMove{}, Out: -1, Commands: []string{}}
	defer func() { r.ElapsedMS = time.Since(started).Milliseconds() }()
	if len(objects) == 0 {
		return r, errors.New("invalid primary-change scope")
	}
	for _, object := range objects {
		before, err := ops.place(ctx, object)
		if err != nil {
			return r, err
		}
		if !before.settled(pool, object) {
			return r, fmt.Errorf("object %s is not on a settled three-member placement", object)
		}
		if len(r.Objects) != 0 && before.ActingPrimary != r.Objects[0].Before.ActingPrimary {
			return r, errors.New("the objects are led by different OSDs")
		}
		r.Objects = append(r.Objects, rbdJournalPrimaryMove{Object: object, Before: before, After: before})
	}
	osd := r.Objects[0].Before.ActingPrimary
	if err := ops.marking(osd); err != nil {
		return r, err
	}
	r.Out = osd
	r.Commands = append(r.Commands, fmt.Sprintf("osd out %d", osd))
	if err := ops.out(ctx, osd); err != nil {
		return r, err
	}
	for deadline := time.Now().Add(ops.bound); ; {
		r.Polls++
		moved := true
		for i := range r.Objects {
			m := &r.Objects[i]
			after, err := ops.place(ctx, m.Object)
			if err != nil {
				return r, err
			}
			m.After, m.Moved = after, after.movedFrom(m.Before, osd)
			moved = moved && m.Moved
		}
		if moved {
			return r, nil
		}
		if !time.Now().Before(deadline) {
			return r, errors.New("the acting primary did not move")
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(ops.pause):
		}
	}
}

// rbdJournalPrimaryObjects names the objects whose placement the
// `change-primary` action follows: the image header, journal object 0 and
// the journal header.
func rbdJournalPrimaryObjects(poolID int64, imageID string) []string {
	return []string{"rbd_header." + imageID, fmt.Sprintf("journal_data.%d.%s.0", poolID, imageID), "journal." + imageID}
}

func (c *fixtureCluster) rbdJournalPrimaryOps() rbdJournalPrimaryOps {
	x := c.rbdBase
	return rbdJournalPrimaryOps{
		place: func(ctx context.Context, object string) (rbdJournalPlacement, error) {
			var p rbdJournalPlacement
			data, err := c.cluster.Ceph(ctx, "osd", "map", x.plan.Pool, object, "--format", "json")
			if err == nil && (len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &p) != nil) {
				err = errors.New("native object placement unavailable")
			}
			return p, err
		},
		out: func(ctx context.Context, osd int) error {
			_, err := c.cluster.Ceph(ctx, "osd", "out", strconv.Itoa(osd))
			return err
		},
		marking: func(osd int) error {
			if !slices.Contains(x.osdsMarkedOut, osd) {
				x.osdsMarkedOut = append(x.osdsMarkedOut, osd)
			}
			return c.rbdBaseJournal()
		},
		bound: 30 * time.Second, pause: 200 * time.Millisecond,
	}
}

// rbdJournalMarkIn marks every OSD the `change-primary` action marked out in
// again and returns the OSDs the following OSDMap read shows in.
func rbdJournalMarkIn(ctx context.Context, out []int, in func(context.Context, int) error, read func(context.Context) (map[int]bool, error)) ([]int, error) {
	if len(out) == 0 {
		return nil, nil
	}
	var errs []error
	for _, osd := range out {
		errs = append(errs, in(ctx, osd))
	}
	state, err := read(ctx)
	if err != nil {
		return nil, errors.Join(append(errs, err)...)
	}
	var restored []int
	for _, osd := range out {
		if state[osd] {
			restored = append(restored, osd)
		} else {
			errs = append(errs, fmt.Errorf("osd.%d was not marked in again", osd))
		}
	}
	return restored, errors.Join(errs...)
}

func (c *fixtureCluster) rbdJournalMarkInOps() (in func(context.Context, int) error, read func(context.Context) (map[int]bool, error)) {
	in = func(ctx context.Context, osd int) error {
		_, err := c.cluster.Ceph(ctx, "osd", "in", strconv.Itoa(osd))
		return err
	}
	read = func(ctx context.Context) (map[int]bool, error) {
		data, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		var dump struct {
			FSID string `json:"fsid"`
			OSDs []struct {
				ID int `json:"osd"`
				In int `json:"in"`
			} `json:"osds"`
		}
		if err == nil && (len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.FSID != c.profile.fsid) {
			err = errors.New("native OSDMap unavailable")
		}
		if err != nil {
			return nil, err
		}
		state := map[int]bool{}
		for _, o := range dump.OSDs {
			state[o.ID] = o.In == 1
		}
		return state, nil
	}
	return in, read
}

// rbdJournalInspected names the actions that run `rbd journal inspect`.
func rbdJournalInspected(action string) bool {
	return action == "setup" || action == "inspect" || action == "replay" || action == "concurrent" || action == "checkpoint" || action == "takeover" ||
		action == "disconnect-oracle"
}

// rbdJournalPhases is the ordered phase list the concurrent action publishes
// in rbd-journal-state.json and returns in its output.
var rbdJournalPhases = []string{"owner", "issued", "completed", "closed"}

// rbdJournalReadOnly names the observations that change no owned state.
// `rbd journal inspect` registers and unregisters its own journal client, so
// the inspect and checkpoint actions are journaled as mutations.
func rbdJournalReadOnly(action string) bool { return strings.HasPrefix(action, "observe-") }

// rbdJournalBlobViews names the binary views every native journal action
// publishes; the file of view v for action a is rbd-journal-<a>-<v>.bin.
var rbdJournalBlobViews = map[string]string{
	"image_head":      "image-head",
	"journal_objects": "journal-objects",
}

func rbdJournalBlobName(action, key string) string {
	return "rbd-journal-" + action + "-" + rbdJournalBlobViews[key] + ".bin"
}

// rbdJournalObjectCount is the number of journal data objects every
// observation reads: the first two object sets of the default splay width
// (four). The plan never fills object set 0, so set 1 must stay absent.
const rbdJournalObjectCount = 8

// rbdJournalObjectLimit bounds one journal data object read.
const rbdJournalObjectLimit = 256 << 10

// rbdJournalCalls is the fixed native exec list of every observation on
// `journal.<image id>`: the immutable and mutable metadata, the client list,
// the three clients by id, the next tag tid and the image client's tag list.
var rbdJournalCalls = []string{
	"get_order", "get_splay_width", "get_pool_id", "get_minimum_set", "get_active_set", "client_list",
	"get_client", "get_client", "get_client", "get_next_tag_tid", "tag_list",
}

// RBD_FEATURE_LAYERING | EXCLUSIVE_LOCK | JOURNALING.
const rbdJournalFeatures = 1 | 4 | 64

type rbdJournalEvent struct {
	Kind   string `json:"kind"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// rbdJournalPlan is the fixed recipe of the journaling image. SetupEvents
// are librbd writes and discards of the setup action, WriteEvents those of
// the write action, GoWrites the AioWrite events the Go test appends.
// ConcurrentOwnerEvents are the synchronous librbd writes that open the
// concurrent session (native owner), GoConcurrentWrites the AioWrite events
// the Go test appends and applies while it owns the lock, and
// ConcurrentEvents the librbd aio writes issued while Go owns the lock.
// TakeoverEvents are the librbd writes of the takeover action of the
// owner-loss and forced-break sequences.
type rbdJournalPlan struct {
	// JournalOrder is the journal data object order the setup creates the
	// image's journal with; 0 leaves librbd's rbd_journal_order default.
	JournalOrder int               `json:"journal_order"`
	Pool         string            `json:"pool"`
	BaseImage    string            `json:"base_image"`
	Image        string            `json:"image"`
	Order        int               `json:"order"`
	Size         uint64            `json:"size"`
	Features     uint64            `json:"features"`
	OracleClient string            `json:"oracle_client"`
	GoClient     string            `json:"go_client"`
	SetupEvents  []rbdJournalEvent `json:"setup_events"`
	WriteEvents  []rbdJournalEvent `json:"write_events"`
	GoWrites     []rbdBaseExtent   `json:"go_writes"`

	ConcurrentOwnerEvents []rbdJournalEvent `json:"concurrent_owner_events"`
	GoConcurrentWrites    []rbdBaseExtent   `json:"go_concurrent_writes"`
	ConcurrentEvents      []rbdJournalEvent `json:"concurrent_events"`
	TakeoverEvents        []rbdJournalEvent `json:"takeover_events"`

	// The consumer plan's producer batches and laggy-client bound
	// (rbd_journal_consumer.go); empty and zero in every other plan.
	ProduceEvents           []rbdJournalEvent `json:"produce_events,omitempty"`
	LaggyEvents             []rbdJournalEvent `json:"laggy_events,omitempty"`
	MaxConcurrentObjectSets int               `json:"max_concurrent_object_sets,omitempty"`
}

// rbdJournalPlanFor places a 256 KiB, four-object image: setup writes
// objects 0 to 3 and discards object 3 whole, the write action writes
// objects 0 and 1 and discards object 2 whole, and Go appends writes to
// objects 0 and 1 that only a journal replay applies. Every write is at most
// 8 KiB, below librbd's 16 KiB journal payload limit, so each is one
// AioWrite event; every discard covers a whole 64 KiB object, so librbd's
// 64 KiB discard granularity keeps it. The concurrent session writes objects
// 0 and 2 as the native owner, then Go writes objects 0 to 2 (three entries,
// one per splay offset 0 to 2), and the native aio writes issued while Go
// owns the lock cover objects 0 and 3; the first overlaps the second half of
// Go's first extent, so the image shows which owner wrote last. The
// takeover write covers the first half of Go's first appended extent, so
// the image shows that the replay of that entry came before it.
func rbdJournalPlanFor(token string, trim bool) rbdJournalPlan {
	const k = 1 << 10
	if trim {
		return rbdJournalTrimPlanFor(token)
	}
	return rbdJournalPlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, Image: "ceph-msgr-journal-" + token,
		Order: 16, Size: 256 * k, Features: rbdJournalFeatures,
		OracleClient: "ceph-msgr-oracle-" + token[:8], GoClient: "ceph-msgr-mirror-" + token[:8],
		SetupEvents:           []rbdJournalEvent{{"write", 0, 8 * k}, {"write", 72 * k, 8 * k}, {"write", 136 * k, 4 * k}, {"write", 196 * k, 8 * k}, {"discard", 192 * k, 64 * k}},
		WriteEvents:           []rbdJournalEvent{{"write", 16 * k, 8 * k}, {"write", 80 * k, 4 * k}, {"discard", 128 * k, 64 * k}},
		GoWrites:              []rbdBaseExtent{{40 * k, 4 * k}, {100 * k, 4 * k}},
		ConcurrentOwnerEvents: []rbdJournalEvent{{"write", 24 * k, 4 * k}, {"write", 152 * k, 4 * k}},
		GoConcurrentWrites:    []rbdBaseExtent{{48 * k, 8 * k}, {112 * k, 4 * k}, {176 * k, 4 * k}},
		ConcurrentEvents:      []rbdJournalEvent{{"write", 52 * k, 4 * k}, {"write", 200 * k, 4 * k}, {"write", 232 * k, 4 * k}},
		TakeoverEvents:        []rbdJournalEvent{{"write", 40 * k, 2 * k}}}
}

// rbdJournalTrimOrder is the journal object order of the trim plan: 4 KiB
// journal data objects, the smallest order librbd accepts
// (librbd/journal/CreateRequest.cc, rbd_journal_order in
// common/options/rbd.yaml.in at rbdBaseSource). An append to an object that
// already holds at least that many bytes is refused with -EOVERFLOW
// (cls/journal/cls_journal.cc journal_object_append), which is how an
// appender learns that an object set is full.
const rbdJournalTrimOrder = 12

// rbdJournalTrimPlanFor is the plan of the trimming sequence. The setup
// journals one small entry at each splay offset of object set 0 (three
// writes of at most 2 KiB and one whole-object discard), so that no setup
// entry fills an object. The eight Go writes of 3 KiB are entries 0 to 7 of
// the Go tag: with a 4 KiB object soft limit entries 0 to 3 fit in set 0,
// entry 4 finds journal object 0 full, and entries 4 to 7 go to object set
// 1, one per splay offset. The takeover write covers the start of the last
// Go extent, the entry that only the takeover's replay applies. The other
// fields are those of the ordinary plan; the trimming sequence does not use
// them.
func rbdJournalTrimPlanFor(token string) rbdJournalPlan {
	const k = 1 << 10
	p := rbdJournalPlanFor(token, false)
	p.JournalOrder = rbdJournalTrimOrder
	p.SetupEvents = []rbdJournalEvent{{"write", 0, 2 * k}, {"write", 72 * k, 2 * k}, {"write", 136 * k, 1 * k}, {"discard", 192 * k, 64 * k}}
	p.GoWrites = []rbdBaseExtent{{4 * k, 3 * k}, {12 * k, 3 * k}, {20 * k, 3 * k}, {28 * k, 3 * k}, {80 * k, 3 * k}, {88 * k, 3 * k}, {96 * k, 3 * k}, {104 * k, 3 * k}}
	p.TakeoverEvents = []rbdJournalEvent{{"write", 104 * k, 1 * k}}
	return p
}

func (p rbdJournalPlan) owned(token string) bool {
	return len(token) == 32 && (p.JournalOrder == 0 || p.JournalOrder == rbdJournalTrimOrder) && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.Image == "ceph-msgr-journal-"+token &&
		p.Order == 16 && p.Size == 256<<10 && p.Features == rbdJournalFeatures && p.OracleClient == "ceph-msgr-oracle-"+token[:8] &&
		p.GoClient == "ceph-msgr-mirror-"+token[:8] && len(p.SetupEvents) > 0 && len(p.WriteEvents) > 0 && len(p.GoWrites) > 0 &&
		len(p.ConcurrentOwnerEvents) > 0 && len(p.GoConcurrentWrites) > 0 && len(p.ConcurrentEvents) > 0 &&
		!slices.ContainsFunc(p.ConcurrentOwnerEvents, func(e rbdJournalEvent) bool { return e.Kind != "write" }) &&
		!slices.ContainsFunc(p.ConcurrentEvents, func(e rbdJournalEvent) bool { return e.Kind != "write" }) &&
		len(p.TakeoverEvents) > 0 && !slices.ContainsFunc(p.TakeoverEvents, func(e rbdJournalEvent) bool { return e.Kind != "write" }) && p.consumerOwned()
}

// The fixed native journal driver. The fixture passes only the token-bound
// plan and a closed action; the driver re-checks every name. librbd runs
// without its cache and with error-level stderr only. A read-write open
// takes the exclusive lock on the first write (or lock_acquire), which opens
// the journal, replays it and allocates a new local tag; closing flushes the
// commit position. Read-only opens take no lock and open no journal.
// Sources: pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at rbdBaseSource;
// independent driver, no code copied.
const rbdJournalNativePython = `
import hashlib, json, os, re, struct, subprocess, sys, time
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'observe-registered', 'write', 'observe-committed', 'inspect', 'replay', 'concurrent',
           'observe-locked', 'checkpoint', 'observe-recorded', 'change-primary', 'observe-resubmitted',
           'observe-appended', 'force-break', 'observe-fenced', 'takeover', 'observe-pending', 'observe-new-connection', 'disconnect-oracle',
           'observe-answered', 'produce')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('image') != 'ceph-msgr-journal-' + token or plan.get('features') != 69
        or plan.get('oracle_client') != 'ceph-msgr-oracle-' + token[:8] or plan.get('go_client') != 'ceph-msgr-mirror-' + token[:8]
        or plan.get('journal_order') not in (0, 12) or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD journal driver scope is not owned')
OUT = '/out'
CONF = '/tmp/ceph-msgr-test/ceph.conf'
# The consumer plan journals into four object sets; every other plan stays in
# the first two.
CONSUMER = bool(plan.get('produce_events'))
OBJECTS = 16 if CONSUMER else 8
OBJECT_LIMIT = 256 * 1024
pool, name = plan['pool'], plan['image']
PREFIX = 'rbd-journal-%s-' % action
# The trim plan creates the journal with small objects; its takeover waits
# for its own journal trimmer and keeps the trimmer's debug lines.
TRIM = plan['journal_order'] != 0

def lstr(value):
    data = value.encode() if isinstance(value, str) else value
    return struct.pack('<I', len(data)) + data

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-journal:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
        counter += 1
    return bytes(data[:length])

def publish(filename, data):
    temporary = os.path.join(OUT, '.' + filename + '.tmp')
    with open(temporary, 'wb') as f:
        f.write(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, os.path.join(OUT, filename))
    return {'name': filename, 'length': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

def call(ioctx, oid, cls, method, data):
    entry = {'object': oid, 'class': cls, 'method': method, 'input_hex': data.hex()}
    try:
        length, output = ioctx.execute(oid, cls, method, data, 65536)
        output = bytes(output)
        if length != len(output):
            raise ValueError('native class output length differs')
        entry.update(code=0, output_hex=output.hex())
    except rados.Error as error:
        if not isinstance(error.errno, int) or error.errno <= 0:
            raise
        entry.update(code=-error.errno, output_hex='')
    return entry

def journal_calls(ioctx, oid):
    # cls_journal_client.cc inputs: client_list(start_after, max_return),
    # get_client(id), tag_list(start_after_tag_tid, max_return, client_id,
    # optional tag class 0).
    clients = ('', plan['go_client'], plan['oracle_client'])
    calls = [call(ioctx, oid, 'journal', m, b'') for m in ('get_order', 'get_splay_width', 'get_pool_id', 'get_minimum_set', 'get_active_set')]
    calls.append(call(ioctx, oid, 'journal', 'client_list', lstr('') + struct.pack('<Q', 256)))
    calls += [call(ioctx, oid, 'journal', 'get_client', lstr(c)) for c in clients]
    calls.append(call(ioctx, oid, 'journal', 'get_next_tag_tid', b''))
    calls.append(call(ioctx, oid, 'journal', 'tag_list', struct.pack('<QQ', 0, 256) + lstr('') + b'\x01' + struct.pack('<Q', 0)))
    return calls

def cli(args, parse):
    argv = ['rbd', '--conf', CONF, '--id', identity[len('client.'):], '--keyring', native_keyring_file(identity), '--rbd_cache=false', '--log_to_stderr=false', '--err_to_stderr=true'] + args
    started = time.monotonic_ns()
    done = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
    elapsed = time.monotonic_ns() - started
    if len(done.stdout) > 65536 or len(done.stderr) > 4096:
        raise ValueError('rbd journal output exceeds its bound')
    out = {'argv': argv[7:], 'returncode': done.returncode, 'stdout': text(done.stdout), 'stderr': text(done.stderr), 'elapsed_ns': elapsed}
    if done.returncode == 0:
        out['parsed'] = parse(text(done.stdout))
    return out

def parse_inspect(stdout):
    # tools/rbd/action/Journal.cc JournalInspector --verbose: per entry a line
    # "Entry: tag_id=<t>, commit_tid=<c>" and a pretty JSON event_entry
    # object, then "Summary:" and "  <n> entries inspected, <e> errors".
    entries, pending, body, summary = [], None, [], None
    lines = stdout.split('\n')
    for index, line in enumerate(lines):
        if pending is not None:
            body.append(line)
            try:
                event = json.loads('\n'.join(body))
            except ValueError:
                continue
            entries.append(dict(pending, event=event))
            pending, body = None, []
            continue
        if line.startswith('Entry: tag_id='):
            tag, commit = line[len('Entry: tag_id='):].split(', commit_tid=')
            pending = {'tag_id': int(tag), 'commit_tid': int(commit)}
        elif line == 'Summary:':
            words = lines[index + 1].split()
            summary = {'total': int(words[0]), 'errors': int(words[3])}
            break
        elif line.strip():
            raise ValueError('unexpected rbd journal inspect line')
    if pending is not None or summary is None or len(entries) > 64:
        raise ValueError('rbd journal inspect output is incomplete')
    return {'entries': entries, 'summary': summary}

def journal_objects(ioctx, prefix):
    objects, blob = [], bytearray()
    for number in range(OBJECTS):
        oid = prefix + str(number)
        entry = {'number': number, 'object': oid}
        try:
            length, _ = ioctx.stat(oid)
            data = ioctx.read(oid, length, 0) if length else b''
        except rados.ObjectNotFound:
            entry.update(exists=False, size=0, offset=len(blob), sha256='')
            objects.append(entry)
            continue
        if len(data) != length or length > OBJECT_LIMIT:
            raise ValueError('journal object read differs from its stat or exceeds its bound')
        entry.update(exists=True, size=length, offset=len(blob), sha256=hashlib.sha256(data).hexdigest())
        blob += data
        objects.append(entry)
    return objects, bytes(blob)

def lockers(image):
    value = image.list_lockers()
    if not value:
        return {'empty': True, 'tag': '', 'exclusive': False, 'lockers': []}
    return {'empty': False, 'tag': text(value['tag']), 'exclusive': bool(value['exclusive']),
            'lockers': [{'client': text(c), 'cookie': text(k), 'addr': text(a)} for c, k, a in value['lockers']]}

def observe(ioctx, out):
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        image_id = text(image.id())
        out['image_id'] = image_id
        out['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix')}
        out['features'], out['flags'] = image.features(), image.flags()
        out['lockers'] = lockers(image)
        out['watchers'] = [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]
        head = image.read(0, stat['size'])
    header = 'journal.' + image_id
    prefix = 'journal_data.%d.%s.' % (ioctx.get_pool_id(), image_id)
    out['header_object'], out['journal_header'], out['journal_prefix'] = 'rbd_header.' + image_id, header, prefix
    out['class_calls'] = journal_calls(ioctx, header)
    objects, view = journal_objects(ioctx, prefix)
    out['journal_objects'] = objects
    spec = pool + '/' + image_id
    out['info'] = cli(['journal', 'info', '--format', 'json', spec], json.loads)
    out['status'] = cli(['journal', 'status', '--format', 'json', spec], json.loads)
    if action in ('setup', 'inspect', 'replay', 'concurrent', 'checkpoint', 'takeover', 'disconnect-oracle'):
        out['inspect'] = cli(['journal', 'inspect', '--verbose', spec], parse_inspect)
    if action in ('observe-appended', 'observe-pending', 'force-break', 'observe-fenced', 'observe-new-connection', 'takeover'):
        out['blocklist'] = blocklist()
    out['blobs'] = {'image_head': publish(PREFIX + 'image-head.bin', head), 'journal_objects': publish(PREFIX + 'journal-objects.bin', view)}

def apply(image, phase, event):
    started = time.time_ns()
    if event['kind'] == 'write':
        data = pattern(phase, event['offset'], event['length'])
        if image.write(data, event['offset']) != len(data):
            raise ValueError('native RBD write was short')
        digest = hashlib.sha256(data).hexdigest()
    elif event['kind'] == 'discard':
        image.discard(event['offset'], event['length'])
        digest = ''
    else:
        raise ValueError('unplanned native RBD journal event')
    return {'phase': phase, 'kind': event['kind'], 'offset': event['offset'], 'length': event['length'], 'sha256': digest,
            'started_ns': started, 'finished_ns': time.time_ns()}

def watchers(image):
    return [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]

def owners(image):
    return [{'mode': o['mode'], 'owner': text(o['owner'])} for o in image.lock_get_owners()]

def blocklist():
    # The OSDMap blocklist as the MON reports it to this identity. librbd
    # adds a lock owner's address here before it breaks the lock. The MON
    # flushes two JSON arrays one after the other: exact entries, then
    # ranges.
    code, output, status = cluster.mon_command(json.dumps({'prefix': 'osd blocklist ls', 'format': 'json'}), b'', timeout=30)
    if code != 0:
        raise ValueError('native osd blocklist ls failed: %d' % code)
    document, decoder, sections = text(output).strip(), json.JSONDecoder(), []
    while document:
        section, consumed = decoder.raw_decode(document)
        sections.append(section)
        document = document[consumed:].lstrip()
    if len(sections) > 2 or any(not isinstance(s, list) for s in sections) or sum(len(s) for s in sections) > 64:
        raise ValueError('native osd blocklist ls output differs or exceeds its bound')
    entries = [{'addr': ('range:' if index else '') + e['addr'], 'until': e['until']} for index, s in enumerate(sections) for e in s]
    return sorted(entries, key=lambda e: e['addr'])

def wait_proceed(step):
    path, expected = os.path.join(OUT, 'rbd-journal-proceed-%d' % step), '%s %s %d' % (token, request_id, step)
    started, deadline = time.monotonic_ns(), time.monotonic() + 30
    while True:
        try:
            with open(path) as f:
                value = f.read(256).strip()
        except FileNotFoundError:
            value = None
        if value is not None:
            if value != expected:
                raise ValueError('proceed signal belongs to another request')
            os.remove(path)
            return time.monotonic_ns() - started
        if time.monotonic() > deadline:
            raise TimeoutError('proceed signal %d missing' % step)
        time.sleep(0.02)

def trim_wait(ioctx, header):
    # The takeover's journal trimmer removes an object set and then raises
    # the journal header's minimum set (journal/JournalTrimmer.cc
    # handle_set_removed); the image stays open until the minimum set has
    # moved or the bound passes.
    started, deadline, polls = time.monotonic_ns(), time.monotonic() + 60, 0
    while True:
        polls += 1
        length, output = ioctx.execute(header, 'journal', 'get_minimum_set', b'', 64)
        minimum = struct.unpack('<Q', bytes(output)[:8])[0]
        if minimum != 0 or time.monotonic() > deadline:
            return {'polls': polls, 'waited_ns': time.monotonic_ns() - started, 'minimum_set': minimum, 'moved': minimum != 0}
        time.sleep(0.2)

def parse_client(data):
    # cls::journal::Client (cls/journal/cls_journal_types.cc): ENCODE_START(1, 1),
    # id, data, commit_position (ENCODE_START(1, 1), a list of ObjectPosition,
    # each ENCODE_START(1, 1) object_number, tag_tid, entry_tid) and state.
    at = [0]
    def take(n):
        if at[0] + n > len(data):
            raise ValueError('journal client record is short')
        value = data[at[0]:at[0] + n]
        at[0] += n
        return value
    u8 = lambda: take(1)[0]
    u32 = lambda: struct.unpack('<I', take(4))[0]
    u64 = lambda: struct.unpack('<Q', take(8))[0]
    def header():
        u8(), u8(), u32()
    header()
    client_id = text(take(u32()))
    take(u32())
    header()
    count = u32()
    if count > 64:
        raise ValueError('journal commit position too long')
    positions = []
    for _ in range(count):
        header()
        positions.append([u64(), u64(), u64()])
    return {'id': client_id, 'positions': positions, 'state': u8()}

def produce(ioctx, out):
    # One read-write image instance owns the lock and journals the plan's
    # writes in two batches while the Go test consumes the journal as a
    # registered client. Each phase is published with the journal header's
    # exec outputs and the journal objects (rbd-journal-state.json and one
    # binary view per phase); the waits are bounded and reported, not judged.
    state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'phases': []}
    image = rbd.Image(ioctx, name)
    try:
        image_id = text(image.id())
        header, prefix = 'journal.' + image_id, 'journal_data.%d.%s.' % (ioctx.get_pool_id(), image_id)
        splay = call(ioctx, header, 'journal', 'get_splay_width', b'')
        width = bytes.fromhex(splay['output_hex'])[0] if splay['code'] == 0 and len(splay['output_hex']) == 2 else 0
        if width == 0:
            raise ValueError('native journal splay width unavailable')
        def client(client_id):
            entry = call(ioctx, header, 'journal', 'get_client', lstr(client_id))
            if entry['code'] != 0:
                raise ValueError('native journal client %s unavailable' % client_id)
            return parse_client(bytes.fromhex(entry['output_hex']))
        def object_set(method):
            entry = call(ioctx, header, 'journal', method, b'')
            if entry['code'] != 0 or len(entry['output_hex']) != 16:
                raise ValueError('native journal %s unavailable' % method)
            return struct.unpack('<Q', bytes.fromhex(entry['output_hex']))[0]
        minimum = lambda: object_set('get_minimum_set')
        active = lambda: object_set('get_active_set')
        def committed(last):
            # The image client's position names its last entry first and
            # one entry per splay offset, all past object set 0.
            positions = client('')['positions']
            return len(positions) == width and positions[0][2] == last and all(p[0] // width >= 1 for p in positions)
        def until(test, bound):
            started, deadline, polls = time.monotonic_ns(), time.monotonic() + bound, 0
            while True:
                polls += 1
                met = test()
                if met or time.monotonic() > deadline:
                    return {'polls': polls, 'waited_ns': time.monotonic_ns() - started, 'met': bool(met)}
                time.sleep(0.2)
        def phase(label, **extra):
            objects, view = journal_objects(ioctx, prefix)
            entry = dict(phase=label, time_ns=time.time_ns(), is_owner=image.is_exclusive_lock_owner(), class_calls=journal_calls(ioctx, header),
                         journal_objects=objects, blob=publish(PREFIX + label + '-journal-objects.bin', view), **extra)
            state['phases'].append(entry)
            publish('rbd-journal-state.json', json.dumps(state, sort_keys=True).encode())
        events = [apply(image, 'produce', e) for e in plan['produce_events']]
        # The image client's position leaves object set 0 once its commits
        # are flushed; the Go client's position still names set 0.
        waited = until(lambda: committed(len(plan['produce_events']) - 1), 60)
        phase('produced', events=events, wait=waited)
        proceed = wait_proceed(1)
        # The Go client committed a position past object set 0: the
        # producer's trimmer removes the set and raises the minimum set.
        waited = until(lambda: minimum() >= 1, 60)
        phase('trimmed', wait=waited, proceed_ns=proceed)
        proceed = wait_proceed(2)
        laggy = [apply(image, 'laggy', e) for e in plan['laggy_events']]
        # A new list: the published first phase keeps its own.
        events = events + laggy
        # The Go client is now more object sets behind than the plan admits:
        # the producer flags it disconnected and trims up to the active set,
        # which its own image client's position names once all of its entries
        # are committed.
        last = len(plan['produce_events']) + len(plan['laggy_events']) - 1
        waited = until(lambda: client(plan['go_client'])['state'] == 1 and committed(last) and minimum() == active(), 90)
        phase('disconnected', events=laggy, wait=waited, proceed_ns=proceed)
        out['final_wait_ns'] = wait_proceed(3)
        out['events'] = events
    except BaseException:
        image.close()
        raise
    started = time.monotonic_ns()
    image.close()
    out['close_ns'] = time.monotonic_ns() - started
    out['phases_produce'] = state['phases']

def concurrent(ioctx, out):
    # One read-write image instance for the whole session. Its first
    # synchronous write takes the exclusive lock (automatic policy), opens
    # the journal and allocates a local tag; librbd hands the lock to the Go
    # test on its RequestLock. After the first proceed signal the aio writes
    # are issued while Go owns the lock: librbd queues them, asks Go for the
    # lock and, once Go has released it, reacquires it, opens the journal
    # (replaying whatever the image client has not committed), allocates a
    # new local tag and appends and applies the queued writes. No flush is
    # requested: a user flush would journal an AioFlush event.
    state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'phases': []}
    def phase(label, image, **extra):
        entry = dict(phase=label, time_ns=time.time_ns(), **extra)
        if image is not None:
            entry.update(is_owner=image.is_exclusive_lock_owner(), owners=[{'mode': o['mode'], 'owner': text(o['owner'])} for o in image.lock_get_owners()],
                         watchers=watchers(image))
        else:
            entry.update(is_owner=False, owners=[], watchers=[])
        state['phases'].append(entry)
        state['phase'] = label
        publish('rbd-journal-state.json', json.dumps(state, sort_keys=True).encode())
    image = rbd.Image(ioctx, name)
    try:
        owner = [apply(image, 'concurrent-owner', e) for e in plan['concurrent_owner_events']]
        phase('owner', image, events=owner)
        waited = wait_proceed(1)
        issued, returns = [], {}
        def issue(index, event):
            data = pattern('concurrent', event['offset'], event['length'])
            record = {'phase': 'concurrent', 'kind': 'write', 'offset': event['offset'], 'length': event['length'], 'sha256': hashlib.sha256(data).hexdigest(),
                      'started_ns': time.time_ns()}
            def complete(completion):
                returns[index] = (completion.get_return_value(), time.time_ns())
            completion = image.aio_write(data, event['offset'], complete)
            issued.append((record, completion))
        for index, event in enumerate(plan['concurrent_events']):
            issue(index, event)
        phase('issued', image, wait_ns=waited, events=[dict(r) for r, _ in issued])
        deadline = time.monotonic() + 60
        while len(returns) < len(issued):
            if time.monotonic() > deadline:
                raise TimeoutError('native aio writes did not complete')
            time.sleep(0.01)
        for index, (record, completion) in enumerate(issued):
            completion.wait_for_complete_and_cb()
            record['finished_ns'] = returns[index][1]
        out['aio_return_values'] = [returns[i][0] for i in range(len(issued))]
        out['events'] = owner + [r for r, _ in issued]
        phase('completed', image, events=[r for r, _ in issued], aio_return_values=out['aio_return_values'])
        waited = wait_proceed(2)
    except BaseException:
        image.close()
        raise
    started = time.monotonic_ns()
    image.close()
    out['close_ns'] = time.monotonic_ns() - started
    phase('closed', None, wait_ns=waited)
    out['phases'] = state['phases']
    out['final_wait_ns'] = wait_proceed(3)

# The lock break and the takeover keep librbd's debug log at level 10 in a
# private file; after the shutdown the driver keeps only the lines of the
# lock state machines (lock_log). ManagedLock.cc, ExclusiveLock.cc,
# ImageWatcher.cc and managed_lock/AcquireRequest.cc and BreakRequest.cc log
# every step and its result at that level.
LOCK_LOG = '/tmp/ceph-msgr-rbd-journal-%s-%s.log' % (token, request_id)
LOCK_COMPONENTS = ('librbd::ManagedLock', 'librbd::ExclusiveLock', 'librbd::ImageWatcher', 'librbd::managed_lock::AcquireRequest', 'librbd::managed_lock::BreakRequest')
LOCK_LINES, LOCK_TEXT, LOCK_BYTES = 320, 240, 64 << 20
LOG_LINE = re.compile(r'^\S+ +[0-9a-f]+ +(-?[0-9]+) (librbd::[A-Za-z_:]+): (.*)$')
# journal/JournalTrimmer.cc logs every step at level 20 under the prefix
# "JournalTrimmer: <this> " (subsystem journaler).
TRIM_LINE = re.compile(r'^\S+ +[0-9a-f]+ +(-?[0-9]+) JournalTrimmer: 0x[0-9a-f]+ (.*)$')
TRIM_LINES = 256
trim_lines, trim_matched = [], [0]
# journal/JournalMetadata.cc logs the laggy-client disconnect at levels 1 and
# 10 under the prefix "JournalMetadata: <this> " (the second from inside a
# lambda, whose function name is operator()).
LAGGY_LINE = re.compile(r'^\S+ +[0-9a-f]+ +(-?[0-9]+) JournalMetadata: 0x[0-9a-f]+ ((?:schedule_laggy_clients_disconnect|operator\(\)): \S+: (?:scheduling disconnect|flagging disconnected))$')
LAGGY_LINES = 16
laggy_lines, laggy_matched = [], [0]
conf = {'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'}
if action in ('force-break', 'takeover'):
    conf.update(debug_rbd='10/10', log_to_file='true', log_file=LOCK_LOG)
if TRIM and action == 'takeover':
    conf.update(debug_journaler='20/20')
if action == 'produce':
    # The producer's journaler logs its trimmer and its laggy-client
    # disconnect; it disconnects a client the plan's number of object sets
    # behind (librbd/Journal.cc create_journaler) and commits its own
    # position a second after an entry is safe.
    conf.update(debug_journaler='20/20', log_to_file='true', log_file=LOCK_LOG,
                rbd_journal_max_concurrent_object_sets=str(plan['max_concurrent_object_sets']), rbd_journal_commit_age='1')
if TRIM and action == 'setup':
    # librbd/image/CreateRequest.cc reads rbd_journal_order when the
    # journaling feature is enabled at creation.
    conf.update(rbd_journal_order=str(plan['journal_order']))

def lock_log():
    # Pointers become per-run ordinals (@0, @1, ...), so that one request
    # object stays recognizable without publishing an address. A file longer
    # than LOCK_BYTES is read no further: its later lines are lost, so the
    # log says so (over_cap) and counts as truncated whatever the prefix holds.
    try:
        with open(LOCK_LOG, 'rb') as f:
            data = f.read(LOCK_BYTES + 1)
    except FileNotFoundError:
        return {'present': False, 'matched': 0, 'truncated': False, 'over_cap': False, 'lines': []}
    os.remove(LOCK_LOG)
    over_cap = len(data) > LOCK_BYTES
    data = data[:LOCK_BYTES]
    lines, matched, pointers = [], 0, {}
    for raw in data.split(b'\n'):
        line = raw.decode('utf-8', 'replace')
        laggy = LAGGY_LINE.match(line)
        if laggy is not None:
            laggy_matched[0] += 1
            if len(laggy_lines) < LAGGY_LINES:
                laggy_lines.append({'level': int(laggy.group(1)), 'text': laggy.group(2)[:LOCK_TEXT]})
            continue
        trimmed = TRIM_LINE.match(line)
        if trimmed is not None:
            trim_matched[0] += 1
            if len(trim_lines) < TRIM_LINES:
                trim_lines.append({'level': int(trimmed.group(1)), 'text': trimmed.group(2)[:LOCK_TEXT]})
            continue
        found = LOG_LINE.match(line)
        if found is None or found.group(2) not in LOCK_COMPONENTS:
            continue
        if found.group(2) == 'librbd::ImageWatcher' and 'lock' not in found.group(3):
            continue
        matched += 1
        if len(lines) < LOCK_LINES:
            body = re.sub(r'0x[0-9a-f]+', lambda m: '@%d' % pointers.setdefault(m.group(0), len(pointers)), found.group(3))
            lines.append({'level': int(found.group(1)), 'component': found.group(2), 'text': body[:LOCK_TEXT]})
    return {'present': True, 'matched': matched, 'truncated': over_cap or matched > len(lines), 'over_cap': over_cap, 'lines': lines}

cluster = rados.Rados(conffile=CONF, rados_id=identity[len('client.'):], conf=conf)
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'image': name, 'native_global_id': cluster.get_instance_id(),
       'started_ns': time.time_ns()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            # The clone acceptance's parent and clone may precede this image.
            listed = sorted(text(i) for i in rbd.RBD().list(ioctx))
            planned = (plan['base_image'], 'ceph-msgr-parent-' + token, 'ceph-msgr-clone-' + token)
            if plan['base_image'] not in listed or any(i not in planned for i in listed):
                raise ValueError('owned RBD pool holds an unplanned image')
            rbd.RBD().create(ioctx, name, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            with rbd.Image(ioctx, name, read_only=True) as image:
                image_id = text(image.id())
            # A native journal client at an empty commit position, as
            # cls_journal client_register records it, keeps every entry
            # replayable for rbd journal inspect.
            out['oracle_register'] = call(ioctx, 'journal.' + image_id, 'journal', 'client_register', lstr(plan['oracle_client']) + lstr(b''))
            if out['oracle_register']['code'] != 0:
                raise ValueError('native oracle journal client registration failed')
        if action == 'concurrent':
            concurrent(ioctx, out)
        elif action == 'produce':
            produce(ioctx, out)
        elif action in ('setup', 'write'):
            phase = 'setup' if action == 'setup' else 'write'
            image = rbd.Image(ioctx, name)
            try:
                out['events'] = [apply(image, phase, e) for e in plan[phase + '_events']]
            finally:
                started = time.monotonic_ns()
                image.close()
                out['close_ns'] = time.monotonic_ns() - started
        elif action == 'force-break':
            # A read-write instance that does not own the lock breaks it
            # with lock_break, naming the one owner it lists: librbd
            # blocklists that address and removes the lock without checking
            # whether the owner's watch is still listed. The instance takes
            # no lock itself (no write), and its close releases nothing.
            image = rbd.Image(ioctx, name)
            try:
                listed, live, held = owners(image), watchers(image), lockers(image)
                before = blocklist()
                if len(listed) != 1:
                    raise ValueError('the image does not have exactly one lock owner to break')
                started = time.monotonic_ns()
                image.lock_break(rbd.RBD_LOCK_MODE_EXCLUSIVE, listed[0]['owner'])
                out['force_break'] = {'owners': listed, 'watchers_before': live, 'lockers_before': held, 'blocklist_before': before,
                                      'break_ns': time.monotonic_ns() - started, 'owners_after': owners(image), 'is_owner': image.is_exclusive_lock_owner()}
            finally:
                image.close()
        elif action == 'takeover':
            # The takeover's writes take the exclusive lock (automatic
            # policy): librbd asks a listed owner, breaks the lock of an
            # owner whose watch is gone (blocklist, then break_lock) or
            # takes a free lock, opens the journal and replays every entry
            # after the image client's commit position, then journals and
            # applies its own writes. Closing commits their positions. The
            # boundary is the lock record and the header watchers this
            # instance lists after its open and right before its first
            # write, which is when its lock acquisition starts.
            before = blocklist()
            image = rbd.Image(ioctx, name)
            try:
                out['boundary'] = {'lockers': lockers(image), 'watchers': watchers(image)}
                started = time.monotonic_ns()
                out['events'] = [apply(image, 'takeover', e) for e in plan['takeover_events']]
                out['write_ns'] = time.monotonic_ns() - started
                out['owner'] = image.is_exclusive_lock_owner()
                if TRIM:
                    out['trim_wait'] = trim_wait(ioctx, 'journal.' + text(image.id()))
            finally:
                started = time.monotonic_ns()
                image.close()
                out['close_ns'] = time.monotonic_ns() - started
            out['blocklist_before'] = before
        elif action == 'disconnect-oracle':
            # tools/rbd/action/Journal.cc do_disconnect_journal_client: the
            # client's state becomes DISCONNECTED, then a header notify.
            with rbd.Image(ioctx, name, read_only=True) as image:
                image_id = text(image.id())
            out['disconnect'] = cli(['journal', 'client', 'disconnect', '--client-id', plan['oracle_client'], pool + '/' + image_id],
                                    lambda stdout: stdout.rstrip('\n').split('\n'))
        elif action == 'replay':
            # Acquiring the exclusive lock opens the journal, which replays
            # every entry after the image client's commit position.
            image = rbd.Image(ioctx, name)
            try:
                started = time.monotonic_ns()
                image.lock_acquire(rbd.RBD_LOCK_MODE_EXCLUSIVE)
                out['acquire_ns'] = time.monotonic_ns() - started
                out['owner'] = image.is_exclusive_lock_owner()
            finally:
                started = time.monotonic_ns()
                image.close()
                out['close_ns'] = time.monotonic_ns() - started
        observe(ioctx, out)
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
if action in ('force-break', 'takeover'):
    out['lock_log'] = lock_log()
if action == 'produce':
    lock_log()
    out['laggy_log'] = {'matched': laggy_matched[0], 'truncated': laggy_matched[0] > len(laggy_lines), 'lines': laggy_lines}
if TRIM and action in ('takeover', 'produce'):
    out['trim_log'] = {'matched': trim_matched[0], 'truncated': trim_matched[0] > len(trim_lines), 'lines': trim_lines}
out['finished_ns'] = time.time_ns()
print(json.dumps(out, sort_keys=True))
`

type rbdJournalApplied struct {
	Phase      string `json:"phase"`
	Kind       string `json:"kind"`
	Offset     uint64 `json:"offset"`
	Length     uint64 `json:"length"`
	SHA256     string `json:"sha256"`
	StartedNS  int64  `json:"started_ns"`
	FinishedNS int64  `json:"finished_ns"`
}

type rbdJournalObject struct {
	Number uint64 `json:"number"`
	Object string `json:"object"`
	Exists bool   `json:"exists"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

type rbdJournalStat struct {
	Size       uint64 `json:"size"`
	ObjectSize uint64 `json:"obj_size"`
	Objects    uint64 `json:"num_objs"`
	Order      int    `json:"order"`
	Prefix     string `json:"block_name_prefix"`
}

type rbdJournalOwner struct {
	Mode  int    `json:"mode"`
	Owner string `json:"owner"`
}

// rbdJournalPhase is one published phase of the concurrent action: the
// native instance's lock ownership, lock owners and header watchers (empty
// once the image is closed) and the events of that phase.
type rbdJournalPhase struct {
	Phase           string              `json:"phase"`
	TimeNS          int64               `json:"time_ns"`
	IsOwner         bool                `json:"is_owner"`
	Owners          []rbdJournalOwner   `json:"owners"`
	Watchers        []rbdBaseWatcher    `json:"watchers"`
	WaitNS          int64               `json:"wait_ns,omitempty"`
	Events          []rbdJournalApplied `json:"events,omitempty"`
	AioReturnValues []int64             `json:"aio_return_values,omitempty"`
}

// rbdJournalCLI is one `rbd journal` command with its parsed output (the
// JSON of info and status, the entries and summary of inspect).
type rbdJournalCLI struct {
	Argv       []string        `json:"argv"`
	ReturnCode int             `json:"returncode"`
	Stdout     string          `json:"stdout"`
	Stderr     string          `json:"stderr"`
	ElapsedNS  int64           `json:"elapsed_ns"`
	Parsed     json.RawMessage `json:"parsed,omitempty"`
}

// rbdJournalNative is one complete journal-driver output; unknown fields are
// refused.
type rbdJournalNative struct {
	Action         string                 `json:"action"`
	Token          string                 `json:"token"`
	RequestID      int64                  `json:"request_id"`
	Pool           string                 `json:"pool"`
	PoolID         int64                  `json:"pool_id"`
	Image          string                 `json:"image"`
	NativeGlobalID uint64                 `json:"native_global_id"`
	StartedNS      int64                  `json:"started_ns"`
	FinishedNS     int64                  `json:"finished_ns"`
	ImageID        string                 `json:"image_id"`
	HeaderObject   string                 `json:"header_object"`
	JournalHeader  string                 `json:"journal_header"`
	JournalPrefix  string                 `json:"journal_prefix"`
	Stat           *rbdJournalStat        `json:"stat"`
	Features       uint64                 `json:"features"`
	Flags          uint64                 `json:"flags"`
	Lockers        *rbdWriteLockers       `json:"lockers"`
	Watchers       []rbdBaseWatcher       `json:"watchers"`
	ClassCalls     []rbdWriteClassCall    `json:"class_calls"`
	JournalObjects []rbdJournalObject     `json:"journal_objects"`
	Info           *rbdJournalCLI         `json:"info"`
	Status         *rbdJournalCLI         `json:"status"`
	Inspect        *rbdJournalCLI         `json:"inspect,omitempty"`
	Blobs          map[string]rbdBaseBlob `json:"blobs"`
	OracleRegister *rbdWriteClassCall     `json:"oracle_register,omitempty"`
	Events         []rbdJournalApplied    `json:"events,omitempty"`
	CloseNS        int64                  `json:"close_ns,omitempty"`
	AcquireNS      int64                  `json:"acquire_ns,omitempty"`
	Owner          *bool                  `json:"owner,omitempty"`
	Phases         []rbdJournalPhase      `json:"phases,omitempty"`
	AioReturns     []int64                `json:"aio_return_values,omitempty"`
	FinalWaitNS    int64                  `json:"final_wait_ns,omitempty"`
	WriteNS        int64                  `json:"write_ns,omitempty"`
	ForceBreak     *rbdJournalForceBreak  `json:"force_break,omitempty"`
	// The OSDMap blocklist after the action, and for the actions that may
	// break a lock also before it.
	BlocklistBefore *[]rbdLockBlocklistEntry `json:"blocklist_before,omitempty"`
	Blocklist       *[]rbdLockBlocklistEntry `json:"blocklist,omitempty"`
	// Boundary is the takeover's lock record and header watchers, listed
	// right before its first write.
	Boundary *rbdJournalBoundary `json:"boundary,omitempty"`
	// LockLog is the lock state machines' part of librbd's debug log of the
	// force-break and takeover actions.
	LockLog *rbdJournalLockLog `json:"lock_log,omitempty"`
	// Disconnect is the disconnect-oracle action's `rbd journal client
	// disconnect`.
	Disconnect *rbdJournalCLI `json:"disconnect,omitempty"`
	// TrimWait and TrimLog belong to the takeover of the trim plan: how long
	// the open image waited for its journal trimmer to raise the minimum
	// set, and that trimmer's debug lines. The producer of the consumer plan
	// keeps its trimmer's lines too, and its laggy-client lines in LaggyLog.
	TrimWait *rbdJournalTrimWait `json:"trim_wait,omitempty"`
	TrimLog  *rbdJournalTrimLog  `json:"trim_log,omitempty"`
	LaggyLog *rbdJournalTrimLog  `json:"laggy_log,omitempty"`
	// ProducePhases are the producer's published phases.
	ProducePhases []rbdJournalProducePhase `json:"phases_produce,omitempty"`
}

// rbdJournalTrimWait is the takeover's wait for the journal's minimum set to
// move, polled with `get_minimum_set` on the journal header.
type rbdJournalTrimWait struct {
	Polls      int    `json:"polls"`
	WaitedNS   int64  `json:"waited_ns"`
	MinimumSet uint64 `json:"minimum_set"`
	Moved      bool   `json:"moved"`
}

// rbdJournalTrimLines bounds the trimmer lines the driver keeps (TRIM_LINES),
// rbdJournalLaggyLines the laggy-client lines (LAGGY_LINES).
const (
	rbdJournalTrimLines  = 256
	rbdJournalLaggyLines = 16
)

// rbdJournalTrimLog is the journal/JournalTrimmer.cc part of the takeover's
// debug log, pointers removed; Matched counts every such line read.
type rbdJournalTrimLog struct {
	Matched   int  `json:"matched"`
	Truncated bool `json:"truncated"`
	Lines     []struct {
		Level int    `json:"level"`
		Text  string `json:"text"`
	} `json:"lines"`
}

func (w *rbdJournalTrimWait) valid() bool {
	return w != nil && w.Polls > 0 && w.WaitedNS >= 0 && w.Moved == (w.MinimumSet != 0)
}

func (l *rbdJournalTrimLog) valid() bool { return l.validWithin(rbdJournalTrimLines) }

func (l *rbdJournalTrimLog) validWithin(limit int) bool {
	if l == nil || l.Lines == nil || len(l.Lines) > limit || l.Matched < len(l.Lines) || l.Truncated != (l.Matched > len(l.Lines)) {
		return false
	}
	for _, line := range l.Lines {
		if len(line.Text) > rbdJournalLockLogText || line.Level < -1 || line.Level > 30 {
			return false
		}
	}
	return true
}

// rbdJournalLockLogLines, rbdJournalLockLogText and rbdJournalLockLogBytes
// bound the lock log the driver keeps and the part of the file it reads
// (LOCK_LINES, LOCK_TEXT and LOCK_BYTES).
const (
	rbdJournalLockLogLines = 320
	rbdJournalLockLogText  = 240
	rbdJournalLockLogBytes = 64 << 20
)

// rbdJournalLockLog is the driver's filtered librbd debug log: the lines of
// librbd::ManagedLock, ExclusiveLock, managed_lock::AcquireRequest,
// managed_lock::BreakRequest and the ImageWatcher lines that mention a lock,
// in the order written, with pointers replaced by per-run ordinals. Matched
// counts every such line in the part the driver read, OverCap says that the
// file was longer than the driver reads (rbdJournalLockLogBytes), and
// Truncated says that some lines were not kept, either way.
type rbdJournalLockLog struct {
	Present   bool                    `json:"present"`
	Matched   int                     `json:"matched"`
	Truncated bool                    `json:"truncated"`
	OverCap   bool                    `json:"over_cap"`
	Lines     []rbdJournalLockLogLine `json:"lines"`
}

type rbdJournalLockLogLine struct {
	Level     int    `json:"level"`
	Component string `json:"component"`
	Text      string `json:"text"`
}

// valid checks the lock log's shape and bounds.
func (l *rbdJournalLockLog) valid() bool {
	if l == nil || l.Lines == nil || len(l.Lines) > rbdJournalLockLogLines || l.Matched < len(l.Lines) || l.Truncated != (l.OverCap || l.Matched > len(l.Lines)) || !l.Present && (l.Matched != 0 || l.OverCap) {
		return false
	}
	for _, line := range l.Lines {
		if !strings.HasPrefix(line.Component, "librbd::") || len(line.Component) > 64 || len(line.Text) > rbdJournalLockLogText || line.Level < -1 || line.Level > 30 {
			return false
		}
	}
	return true
}

// rbdJournalBoundary is what the takeover action lists after its open and
// before its first write: the lock record and the header watchers, its own
// watch included.
type rbdJournalBoundary struct {
	Lockers  *rbdWriteLockers `json:"lockers"`
	Watchers []rbdBaseWatcher `json:"watchers"`
}

// valid checks the boundary's shape: a lock record (possibly empty) and the
// listed watchers, among them at least the takeover's own.
func (b *rbdJournalBoundary) valid() bool {
	return b != nil && rbdLockLockersValid(b.Lockers) && b.Watchers != nil && len(b.Watchers) > 0 && rbdWriteWatchersValid(b.Watchers)
}

// rbdJournalForceBreak is the force-break action's own output: the lock
// owners, header watchers, lockers and blocklist it saw before lock_break,
// how long lock_break took, and the owners it lists afterwards.
type rbdJournalForceBreak struct {
	Owners          []rbdJournalOwner        `json:"owners"`
	WatchersBefore  []rbdBaseWatcher         `json:"watchers_before"`
	LockersBefore   *rbdWriteLockers         `json:"lockers_before"`
	BlocklistBefore *[]rbdLockBlocklistEntry `json:"blocklist_before"`
	BreakNS         int64                    `json:"break_ns"`
	OwnersAfter     []rbdJournalOwner        `json:"owners_after"`
	IsOwner         bool                     `json:"is_owner"`
}

// valid checks the force-break output: one owner, the lock record that
// names it, and no owner afterwards in the instance that broke it.
func (f *rbdJournalForceBreak) valid() bool {
	return f != nil && len(f.Owners) == 1 && f.Owners[0].Owner != "" && len(f.Owners[0].Owner) <= 128 && rbdWriteWatchersValid(f.WatchersBefore) &&
		rbdLockLockersValid(f.LockersBefore) && !f.LockersBefore.Empty && len(f.LockersBefore.Lockers) == 1 && f.LockersBefore.Lockers[0].Address == f.Owners[0].Owner &&
		rbdLockBlocklistValid(f.BlocklistBefore) && f.BreakNS > 0 && f.OwnersAfter != nil && len(f.OwnersAfter) == 0 && !f.IsOwner
}

// rbdJournalPhasesValid checks the concurrent action's phase list: every
// phase once and in order, the native instance owning the lock with its
// header watch registered after its own writes and after its queued aio
// writes completed, and nothing observed after the close.
func rbdJournalPhasesValid(phases []rbdJournalPhase, plan rbdJournalPlan) bool {
	if len(phases) != len(rbdJournalPhases) {
		return false
	}
	for i, p := range phases {
		ok := p.Phase == rbdJournalPhases[i] && p.TimeNS > 0 && len(p.Owners) <= 4 && rbdWriteWatchersValid(p.Watchers) && (i == 0 || p.TimeNS >= phases[i-1].TimeNS)
		switch p.Phase {
		case "owner":
			ok = ok && p.IsOwner && len(p.Watchers) > 0 && p.WaitNS == 0 && rbdJournalEventsValid(p.Events, "concurrent-owner", plan.ConcurrentOwnerEvents) && p.AioReturnValues == nil
		case "issued":
			ok = ok && p.WaitNS > 0 && len(p.Events) == len(plan.ConcurrentEvents) && p.AioReturnValues == nil
			for j, e := range p.Events {
				want := plan.ConcurrentEvents[min(j, len(plan.ConcurrentEvents)-1)]
				ok = ok && e.Phase == "concurrent" && e.Kind == want.Kind && e.Offset == want.Offset && e.Length == want.Length && len(e.SHA256) == 64 && e.StartedNS > 0 && e.FinishedNS == 0
			}
		case "completed":
			ok = ok && p.IsOwner && p.WaitNS == 0 && rbdJournalEventsValid(p.Events, "concurrent", plan.ConcurrentEvents) && len(p.AioReturnValues) == len(plan.ConcurrentEvents)
		case "closed":
			ok = ok && !p.IsOwner && len(p.Owners) == 0 && len(p.Watchers) == 0 && p.WaitNS > 0 && p.Events == nil && p.AioReturnValues == nil
		}
		if !ok {
			return false
		}
	}
	return true
}

func rbdJournalCLIValid(c *rbdJournalCLI, args ...string) bool {
	if c == nil {
		return false
	}
	want := append([]string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "journal"}, args...)
	return slices.Equal(c.Argv, want) && c.ElapsedNS > 0 && len(c.Stdout) <= 65536 && len(c.Stderr) <= 4096 && (c.ReturnCode == 0) == (len(c.Parsed) != 0)
}

func rbdJournalEventsValid(applied []rbdJournalApplied, phase string, plan []rbdJournalEvent) bool {
	if len(applied) != len(plan) {
		return false
	}
	for i, a := range applied {
		e := plan[i]
		if a.Phase != phase || a.Kind != e.Kind || a.Offset != e.Offset || a.Length != e.Length || (e.Kind == "write") != (len(a.SHA256) == 64) ||
			e.Kind == "discard" && a.SHA256 != "" || a.StartedNS <= 0 || a.FinishedNS < a.StartedNS {
			return false
		}
	}
	return true
}

// decodeRBDJournalNative accepts exactly the shape of one admitted action
// for the owned plan. It checks structure only: which clients, positions,
// tags and entries each observation must show after the Go steps is the
// test's judgment, so a Go failure is reported by the test instead of
// stopping the control loop.
func decodeRBDJournalNative(action, token string, request int64, plan rbdJournalPlan, data []byte) (rbdJournalNative, error) {
	var result rbdJournalNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD journal output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdJournalNative{}, fmt.Errorf("native RBD journal output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Image != plan.Image ||
		result.PoolID < 0 || result.NativeGlobalID == 0 || result.StartedNS <= 0 || result.FinishedNS < result.StartedNS {
		return rbdJournalNative{}, errors.New("native RBD journal output identity differs")
	}
	s := result.Stat
	prefix := fmt.Sprintf("journal_data.%d.%s.", result.PoolID, result.ImageID)
	ok := s != nil && result.ImageID != "" && !strings.ContainsAny(result.ImageID, "./ \x00") && result.HeaderObject == "rbd_header."+result.ImageID &&
		result.JournalHeader == "journal."+result.ImageID && result.JournalPrefix == prefix &&
		s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Prefix == "rbd_data."+result.ImageID && s.Size == plan.Size && s.Objects == plan.Size>>plan.Order &&
		result.Features == plan.Features && rbdLockLockersValid(result.Lockers) && rbdWriteWatchersValid(result.Watchers) &&
		len(result.ClassCalls) == len(rbdJournalCalls) && len(result.JournalObjects) == plan.objects() && len(result.Blobs) == len(rbdJournalBlobViews)
	for i, method := range rbdJournalCalls {
		if i < len(result.ClassCalls) {
			ok = ok && rbdLockCallValid(&result.ClassCalls[i], result.JournalHeader, "journal", method) && len(result.ClassCalls[i].OutputHex) <= 131072
		}
	}
	offset := uint64(0)
	for i, o := range result.JournalObjects {
		ok = ok && o.Number == uint64(i) && o.Object == prefix+strconv.Itoa(i) && o.Offset == offset && o.Size <= rbdJournalObjectLimit &&
			(o.Exists && len(o.SHA256) == 64 || !o.Exists && o.Size == 0 && o.SHA256 == "")
		offset += o.Size
	}
	for key := range rbdJournalBlobViews {
		blob, present := result.Blobs[key]
		ok = ok && present && blob.Name == rbdJournalBlobName(action, key) && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
	}
	spec := plan.Pool + "/" + result.ImageID
	ok = ok && result.Blobs["image_head"].Length == s.Size && result.Blobs["journal_objects"].Length == offset &&
		rbdJournalCLIValid(result.Info, "info", "--format", "json", spec) && rbdJournalCLIValid(result.Status, "status", "--format", "json", spec)
	if rbdJournalInspected(action) {
		ok = ok && rbdJournalCLIValid(result.Inspect, "inspect", "--verbose", spec)
	} else {
		ok = ok && result.Inspect == nil
	}
	ok = ok && (action == "concurrent") == (result.Phases != nil) && (action == "concurrent") == (result.AioReturns != nil) && (action == "concurrent" || action == "produce") == (result.FinalWaitNS > 0)
	ok = ok && (action == "produce") == (result.ProducePhases != nil) && (action == "produce") == (result.LaggyLog != nil) &&
		(action != "produce" || plan.consumer() && rbdJournalProducePhasesValid(result.ProducePhases, plan, result.JournalHeader) && result.LaggyLog.validWithin(rbdJournalLaggyLines))
	ok = ok && (action == "setup") == (result.OracleRegister != nil) && (action == "replay" || action == "takeover") == (result.Owner != nil) && (action == "replay") == (result.AcquireNS > 0)
	ok = ok && (action == "takeover") == (result.WriteNS > 0) && (action == "force-break") == (result.ForceBreak != nil) && (action != "force-break" || result.ForceBreak.valid())
	ok = ok && rbdJournalBlocklisted(action) == (result.Blocklist != nil) && (!rbdJournalBlocklisted(action) || rbdLockBlocklistValid(result.Blocklist))
	ok = ok && (action == "takeover") == (result.BlocklistBefore != nil) && (action != "takeover" || rbdLockBlocklistValid(result.BlocklistBefore))
	ok = ok && (action == "takeover") == (result.Boundary != nil) && (action != "takeover" || result.Boundary.valid())
	ok = ok && rbdJournalLockLogged(action) == (result.LockLog != nil) && (!rbdJournalLockLogged(action) || result.LockLog.valid())
	ok = ok && (action == "disconnect-oracle") == (result.Disconnect != nil) &&
		(action != "disconnect-oracle" || rbdJournalCLIValid(result.Disconnect, "client", "disconnect", "--client-id", plan.OracleClient, spec))
	trimmed, logged := action == "takeover" && plan.JournalOrder != 0, action == "produce" && plan.JournalOrder != 0
	ok = ok && trimmed == (result.TrimWait != nil) && (trimmed || logged) == (result.TrimLog != nil) && (!trimmed || result.TrimWait.valid()) && (!trimmed && !logged || result.TrimLog.valid())
	if result.OracleRegister != nil {
		ok = ok && rbdLockCallValid(result.OracleRegister, result.JournalHeader, "journal", "client_register") && result.OracleRegister.Code == 0
	}
	switch action {
	case "setup":
		ok = ok && rbdJournalEventsValid(result.Events, "setup", plan.SetupEvents) && result.CloseNS > 0
	case "write":
		ok = ok && rbdJournalEventsValid(result.Events, "write", plan.WriteEvents) && result.CloseNS > 0
	case "replay":
		ok = ok && result.Events == nil && result.CloseNS > 0
	case "takeover":
		ok = ok && rbdJournalEventsValid(result.Events, "takeover", plan.TakeoverEvents) && result.CloseNS > 0
	case "produce":
		produced := min(len(plan.ProduceEvents), len(result.Events))
		ok = ok && rbdJournalEventsValid(result.Events[:produced], "produce", plan.ProduceEvents) && rbdJournalEventsValid(result.Events[produced:], "laggy", plan.LaggyEvents) &&
			result.CloseNS > 0
	case "concurrent":
		owner := min(len(plan.ConcurrentOwnerEvents), len(result.Events))
		ok = ok && rbdJournalEventsValid(result.Events[:owner], "concurrent-owner", plan.ConcurrentOwnerEvents) &&
			rbdJournalEventsValid(result.Events[owner:], "concurrent", plan.ConcurrentEvents) && result.CloseNS > 0 &&
			len(result.AioReturns) == len(plan.ConcurrentEvents) && result.FinalWaitNS > 0 && rbdJournalPhasesValid(result.Phases, plan)
	default:
		ok = ok && result.Events == nil && result.CloseNS == 0
	}
	if !ok {
		return rbdJournalNative{}, fmt.Errorf("native RBD journal %s output shape differs", action)
	}
	return result, nil
}

// checkRBDJournalBlobs verifies that each published binary view of one
// action matches its receipt.
func checkRBDJournalBlobs(out, action string, blobs map[string]rbdBaseBlob) error {
	for key := range rbdJournalBlobViews {
		blob := blobs[key]
		name := rbdJournalBlobName(action, key)
		if blob.Name != name {
			return fmt.Errorf("native RBD journal binary view %s is misnamed", name)
		}
		file, err := os.Open(filepath.Join(out, name))
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(file, rbdBaseBlobLimit+1))
		file.Close()
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		if len(data) > rbdBaseBlobLimit || uint64(len(data)) != blob.Length || hex.EncodeToString(sum[:]) != blob.SHA256 {
			return fmt.Errorf("native RBD journal binary view %s differs from its receipt", name)
		}
	}
	return nil
}

// rbdJournalNativeTimeout bounds one native journal action: image creation
// or I/O (the concurrent session waits at most 30 s for each of its three
// proceed signals and 60 s for its aio writes), then the observation with up
// to three `rbd journal` commands of at most 60 s each.
const rbdJournalNativeTimeout = 240 * time.Second

// rbdJournalProduceTimeout bounds the producer, which waits at most 60, 60
// and 90 s for its three states and 30 s for each of its three proceed
// signals, then observes the journal.
const rbdJournalProduceTimeout = 480 * time.Second

// rbdJournalActionTimeout is the bound of one native journal action.
func rbdJournalActionTimeout(action string) time.Duration {
	if action == "produce" {
		return rbdJournalProduceTimeout
	}
	return rbdJournalNativeTimeout
}

func (c *fixtureCluster) rbdJournalRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdJournalNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.journalPlan)
	if err != nil {
		return nil, rbdJournalNative{}, err
	}
	argv := []string{"python3", "-c", rbdJournalNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: rbdJournalActionTimeout(action), MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD journal " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdJournalNative{}, errors.Join(err, fmt.Errorf("native RBD journal %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDJournalNative(action, x.token, request, x.journalPlan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDJournalRequest admits <id> <ownership token> <action> where the
// action is the next one of the selected sequence (next is its index).
func parseRBDJournalRequest(actions, fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD journal request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD journal request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD journal request identity")
	}
	if !slices.Contains(actions, fields[2]) {
		return 0, "", errors.New("RBD journal action is not admitted")
	}
	if next >= len(actions) || fields[2] != actions[next] {
		return 0, "", errors.New("RBD journal action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdJournalTarget names the owned state one journal action changes.
func rbdJournalTarget(plan rbdJournalPlan, action string) string {
	switch action {
	case "setup":
		return plan.Image + ",journal-client:" + plan.OracleClient
	case "write":
		return plan.Image + ":native-write"
	case "inspect", "checkpoint":
		return plan.Image + ":journal-inspect"
	case "replay":
		return plan.Image + ":exclusive-lock,journal-replay"
	case "concurrent":
		return plan.Image + ":exclusive-lock-handoff,native-write,journal-replay"
	case "change-primary":
		return plan.Image + ":osd-out,acting-primary"
	case "force-break":
		return plan.Image + ":exclusive-lock-break,osd-blocklist"
	case "takeover":
		return plan.Image + ":exclusive-lock-break,osd-blocklist,journal-replay,native-write"
	case "advance-map":
		return plan.Pool + ":pool-flag-" + rbdLockAdvanceFlag + ",osdmap-epoch"
	case "disconnect-oracle":
		return plan.Image + ",journal-client:" + plan.OracleClient + ":disconnected"
	case "produce":
		return plan.Image + ":exclusive-lock,native-write,journal-trim,journal-client:" + plan.GoClient + ":disconnected"
	}
	return plan.Image
}

// rbdJournalControl runs one native action of the closed journal sequence.
// The setup, write, inspect, checkpoint and replay actions are journaled as
// mutations, the observations as read-only actions. `advance-map` is the
// fixture's own OSDMap epoch advance and runs no driver.
func (c *fixtureCluster) rbdJournalControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) || !x.journalPlan.owned(x.token) {
		return errors.New("invalid owned RBD journal control scope")
	}
	ctx, stop := context.WithTimeout(ctx, rbdJournalProduceTimeout+20*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD journal control requested after cleanup began")
	}
	id, action, err := parseRBDJournalRequest(rbdJournalActionsFor(c.profile), fields, x.token, x.journalLastRequestID, x.journalNext)
	if err != nil {
		return err
	}
	x.journalLastRequestID = id
	x.journalNext++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.journalPlan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-journal-"+action+".json", result))
	}()
	m, err := c.rbdBaseBeginAction("native-journal-"+action, rbdJournalTarget(x.journalPlan, action), id, rbdJournalReadOnly(action))
	result["mutation"] = m
	if err != nil {
		return err
	}
	if action == "change-primary" {
		// The fixture's own administrative commands, then the observation.
		if x.journalImageID == "" || x.poolID < 0 {
			return c.rbdBaseFinish(m, errors.New("the journaling image is unknown"))
		}
		change, err := rbdJournalChangePrimary(ctx, x.plan.Pool, rbdJournalPrimaryObjects(x.poolID, x.journalImageID), c.rbdJournalPrimaryOps())
		result["primary_change"] = change
		if err != nil {
			return c.rbdBaseFinish(m, err)
		}
	}
	if rbdJournalHeldAdmin(action) {
		// The fixture's own reads of a held-request sequence; no driver.
		read, err := c.rbdJournalHeldControl(ctx, action)
		result["held"] = read
		return c.rbdBaseFinish(m, err)
	}
	if action == "advance-map" {
		// The fixture's own administrative commands only; the Go test's
		// observations follow as their own actions.
		ops := c.rbdLockAdvanceOps(x.plan.Pool)
		if c.profile.admits(capRBDJournalHeldPrimaryChange) {
			// On three OSDs the samples are the old primary's, which holds
			// the append (rbd_journal_held.go).
			if x.journalHeldPrimary < 0 || len(x.osdsMarkedOut) != 1 || x.osdsMarkedOut[0] != x.journalHeldPrimary {
				return c.rbdBaseFinish(m, errors.New("the held append's primary was not the one OSD marked out"))
			}
			held := x.journalHeldPrimary
			ops.osds = func(context.Context) ([]int, error) { return []int{held}, nil }
		}
		advance, err := rbdLockAdvanceMap(ctx, x.plan.Pool, ops)
		result["advance"] = advance
		return c.rbdBaseFinish(m, err)
	}
	process, native, err := c.rbdJournalRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD journal action used another pool")
	}
	if err == nil {
		err = checkRBDJournalBlobs(out, action, native.Blobs)
	}
	for _, p := range native.ProducePhases {
		if err == nil {
			err = checkRBDBlob(out, rbdJournalProduceBlobName(p.Phase), p.Blob)
		}
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	if action == "setup" {
		x.journalImageID = native.ImageID
	}
	// The Go lock owner of the owner-loss and forced-break sequences is
	// blocklisted by whoever breaks its lock; the cleanup removes that
	// entry.
	if action == "observe-locked" && (c.profile.admits(capRBDJournalOwnerLoss) || c.profile.admits(capRBDJournalForcedBreak) || c.profile.admits(capRBDJournalForcedBreakPending) ||
		c.profile.admits(capRBDJournalForcedBreakInflight) || c.profile.admits(capRBDJournalTrim)) &&
		native.Lockers != nil {
		for _, l := range native.Lockers.Lockers {
			x.lockOwnerAddresses = append(x.lockOwnerAddresses, l.Address)
		}
	}
	return c.rbdBaseFinish(m, nil)
}

var _ = defineRequestControl("rbd-journal", frozenControlRank(15), (*fixtureCluster).rbdJournalControl)

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"rbd-journal-advance-map.json",
	"rbd-journal-change-primary.json",
	"rbd-journal-checkpoint.json",
	"rbd-journal-consumer-results.json",
	"rbd-journal-dead-primary-outcome.json",
	"rbd-journal-dead-primary-results.json",
	"rbd-journal-disconnect-oracle.json",
	"rbd-journal-force-break.json",
	"rbd-journal-forced-break-inflight-outcome.json",
	"rbd-journal-forced-break-inflight-results.json",
	"rbd-journal-forced-break-pending-results.json",
	"rbd-journal-forced-break-results.json",
	"rbd-journal-held-primary-change-outcome.json",
	"rbd-journal-held-primary-change-results.json",
	"rbd-journal-inspect.json",
	"rbd-journal-lost-append-outcome.json",
	"rbd-journal-lost-append-results.json",
	"rbd-journal-observe-answered.json",
	"rbd-journal-observe-appended.json",
	"rbd-journal-observe-committed.json",
	"rbd-journal-observe-fenced.json",
	"rbd-journal-observe-held.json",
	"rbd-journal-observe-locked.json",
	"rbd-journal-observe-new-connection.json",
	"rbd-journal-observe-pending.json",
	"rbd-journal-observe-placed.json",
	"rbd-journal-observe-primary-lost.json",
	"rbd-journal-observe-recorded.json",
	"rbd-journal-observe-registered.json",
	"rbd-journal-observe-resubmitted.json",
	"rbd-journal-observe-settled.json",
	"rbd-journal-owner-loss-outcome.json",
	"rbd-journal-owner-loss-results.json",
	"rbd-journal-primary-change-outcome.json",
	"rbd-journal-primary-change-results.json",
	"rbd-journal-produce.json",
	"rbd-journal-replay.json",
	"rbd-journal-replica-hold-outcome.json",
	"rbd-journal-replica-hold-results.json",
	"rbd-journal-results.json",
	"rbd-journal-setup.json",
	"rbd-journal-takeover.json",
	"rbd-journal-trim-results.json",
	"rbd-journal-write.json",
)
