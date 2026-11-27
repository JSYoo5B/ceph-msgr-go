package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The fenced-hold and pre-down restart journal sequences extend the
// held-request sequences of rbd_journal_held.go on the same three-OSD
// topology:
//
//   - fenced hold: the replica hold, and while the Go lock owner's append
//     waits at the primary for the paused replica, the fixture's `fence`
//     action blocklists the lock record's address as librbd's lock break
//     does before it removes the lock (librbd/managed_lock/BreakRequest.cc
//     send_blocklist, then wait_for_osd_map, at rbdBaseSource) and waits
//     until the primary's PG has the map with the entry. The lock itself is
//     left to native librbd's later acquisition, because every write to the
//     image header is held behind the same paused replica.
//   - erasure fenced hold: the fenced hold with the journal's data objects in
//     the erasure-coded journal pool. The append is fenced by the lock
//     record's address only when it travels on the connection that took the
//     lock, so `align-primary` first makes the erasure-coded PG's acting
//     primary the image header's primary with a primary override (shards
//     keep their slots), as the erasure-coded held primary change does.
//   - fenced pending hold: the fenced hold with the pending hold's three
//     appends, to journal objects 0, 1 and 2, all applied at the primary and
//     held there for the paused replica when the fence comes; on the
//     erasure-coded journal pool after `align-primary`, as in the erasure
//     fenced hold.
//   - primary restart before down: the primary-restart sequence with the
//     primary flagged nodown (`keep-up`) before the fault, so that the MON
//     ignores its peers' failure reports (mon/OSDMonitor.cc can_mark_down)
//     and the OSDMap still names the killed instance up at its old address
//     (`observe-primary-dead`) until the new instance boots: prepare_boot
//     then marks the old instance down and the new one up in separate
//     epochs. The erasure-coded variant follows the journal data PG of the
//     erasure-coded journal pool alone, whose primary is the one flagged.
//
// `fence`, `align-primary` and `keep-up` are the fixture's own
// administrative commands; `observe-primary-dead` is a read like the other
// held-request observations. What the receipts mean is the test's to judge.

// capRBDJournalFencedHold admits the fenced-hold sequence.
var capRBDJournalFencedHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-fenced-hold",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose lock owner is blocklisted while its append waits for a paused replica's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalErasureFencedHold admits the erasure fenced-hold sequence.
var capRBDJournalErasureFencedHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-fenced-hold",
	"run one test alone on a fresh three-OSD RBD fixture whose journal data objects are in an erasure-coded pool, with the journal sequence whose lock owner is blocklisted while its append waits for a paused shard's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalPrimaryRestartBeforeDown admits the pre-down restart
// sequence.
var capRBDJournalPrimaryRestartBeforeDown = defineExclusiveCapability(fixtureRBD, "rbd-journal-primary-restart-before-down",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose appends are held at a nodown primary that is killed and started again before the OSDMap marks it down",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalFencedPendingHold and capRBDJournalErasureFencedPendingHold
// admit the fenced pending-hold sequences.
var capRBDJournalFencedPendingHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-fenced-pending-hold",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose lock owner is blocklisted while three of its appends are held for a paused replica's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

var capRBDJournalErasureFencedPendingHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-fenced-pending-hold",
	"run one test alone on a fresh three-OSD RBD fixture whose journal data objects are in an erasure-coded pool, with the journal sequence whose lock owner is blocklisted while three of its appends are held for a paused shard's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalErasurePrimaryRestartBeforeDown admits the pre-down restart
// sequence on the erasure-coded journal pool.
var capRBDJournalErasurePrimaryRestartBeforeDown = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-primary-restart-before-down",
	"run one test alone on a fresh three-OSD RBD fixture whose journal data objects are in an erasure-coded pool, with the journal sequence whose appends are held at a nodown primary that is killed and started again before the OSDMap marks it down",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// The three sequences; the fenced pending holds and the erasure-coded
// pre-down restart run those of their single-append and replicated forms.
// `observe-settled` and `observe-primary-back` are the held-request reads, `observe-fenced` and `observe-restarted` the driver's
// ordinary observation and `replay` the native replay, which acquires the
// lock librbd's way: it breaks the lock of an owner whose watch is gone.
var (
	rbdJournalFencedHoldActions               = []string{"setup", "observe-locked", "observe-placed", "observe-held", "fence", "observe-settled", "observe-fenced", "replay"}
	rbdJournalErasureFencedHoldActions        = []string{"setup", "observe-locked", "align-primary", "observe-placed", "observe-held", "fence", "observe-settled", "observe-fenced", "replay"}
	rbdJournalPrimaryRestartBeforeDownActions = []string{"setup", "observe-locked", "observe-placed", "keep-up", "observe-held", "observe-primary-dead", "observe-primary-back",
		"observe-restarted", "replay"}
)

// rbdJournalFencedSequence says whether the profile selects a fenced-hold
// sequence.
func (p profile) rbdJournalFencedSequence() bool {
	return p.declares(capRBDJournalFencedHold) || p.declares(capRBDJournalErasureFencedHold) || p.declares(capRBDJournalFencedPendingHold) ||
		p.declares(capRBDJournalErasureFencedPendingHold)
}

// rbdJournalAlignedSequence says whether the profile admits a fenced-hold
// sequence on the erasure-coded journal pool, which aligns its primary.
func (p profile) rbdJournalAlignedSequence() bool {
	return p.admits(capRBDJournalErasureFencedHold) || p.admits(capRBDJournalErasureFencedPendingHold)
}

// rbdJournalBeforeDownSequence says whether the profile admits a pre-down
// restart sequence.
func (p profile) rbdJournalBeforeDownSequence() bool {
	return p.admits(capRBDJournalPrimaryRestartBeforeDown) || p.admits(capRBDJournalErasurePrimaryRestartBeforeDown)
}

// rbdJournalOSDInfo is one OSD's entry of `ceph osd dump`: its state, the
// epochs it was last marked up from and down at, and its public v2 address
// as the Go test's routing names it (v2:<ip>:<port>/<nonce>).
type rbdJournalOSDInfo struct {
	Epoch   uint32   `json:"epoch"`
	OSD     int      `json:"osd"`
	Up      bool     `json:"up"`
	State   []string `json:"state"`
	UpFrom  uint32   `json:"up_from"`
	DownAt  uint32   `json:"down_at"`
	Address string   `json:"address"`
	Down    []int    `json:"down"`
}

// decodeRBDJournalOSDInfo projects one `ceph osd dump` output of fsid to
// osd's entry.
func decodeRBDJournalOSDInfo(fsid string, osd int, data []byte) (rbdJournalOSDInfo, error) {
	var dump struct {
		FSID  string `json:"fsid"`
		Epoch uint32 `json:"epoch"`
		OSDs  []struct {
			ID     int      `json:"osd"`
			Up     int      `json:"up"`
			State  []string `json:"state"`
			UpFrom uint32   `json:"up_from"`
			DownAt uint32   `json:"down_at"`
			Public struct {
				Addrvec []struct {
					Type  string `json:"type"`
					Addr  string `json:"addr"`
					Nonce uint32 `json:"nonce"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"osds"`
	}
	if len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.FSID != fsid || dump.Epoch == 0 || len(dump.OSDs) == 0 {
		return rbdJournalOSDInfo{}, errors.New("native OSDMap unavailable")
	}
	info := rbdJournalOSDInfo{Epoch: dump.Epoch, OSD: -1, State: []string{}, Down: []int{}}
	for _, o := range dump.OSDs {
		if o.Up != 1 {
			info.Down = append(info.Down, o.ID)
		}
		if o.ID != osd {
			continue
		}
		if len(o.State) > 16 {
			return rbdJournalOSDInfo{}, errors.New("native OSD state exceeds its bound")
		}
		info.OSD, info.Up, info.State, info.UpFrom, info.DownAt = o.ID, o.Up == 1, append(info.State, o.State...), o.UpFrom, o.DownAt
		for _, a := range o.Public.Addrvec {
			if a.Type == "v2" {
				info.Address = fmt.Sprintf("v2:%s/%d", a.Addr, a.Nonce)
			}
		}
	}
	if info.OSD != osd {
		return rbdJournalOSDInfo{}, fmt.Errorf("native OSDMap does not list osd.%d", osd)
	}
	return info, nil
}

func (c *fixtureCluster) rbdJournalOSDInfo(ctx context.Context, osd int) (rbdJournalOSDInfo, error) {
	data, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return rbdJournalOSDInfo{}, err
	}
	return decodeRBDJournalOSDInfo(c.profile.fsid, osd, data)
}

// rbdJournalFence is the `fence` receipt: the lock record's address, the
// MON's blocklist before and after the one `osd blocklist add`, the
// OSDMap epoch read after it, the epoch of the PG's own map on the primary
// once it reached that epoch, and the primary's operations in flight then.
type rbdJournalFence struct {
	Address         string             `json:"address"`
	OSD             int                `json:"osd"`
	PGID            string             `json:"pgid"`
	Commands        []string           `json:"commands"`
	BlocklistBefore []mdsSessionsBlock `json:"blocklist_before"`
	Blocklist       []mdsSessionsBlock `json:"blocklist"`
	Epoch           uint32             `json:"epoch"`
	PGEpoch         uint32             `json:"pg_epoch"`
	PGState         string             `json:"pg_state"`
	Ops             []rbdJournalHeldOp `json:"ops"`
	Down            []int              `json:"down"`
	Polls           int                `json:"polls"`
	ElapsedMS       int64              `json:"elapsed_ms"`
}

// rbdJournalFenceOps are the fence's native reads and its one command.
type rbdJournalFenceOps struct {
	blocklist func(ctx context.Context) ([]mdsSessionsBlock, error)
	add       func(ctx context.Context, address string) error
	held      rbdJournalHeldOps
	// bound limits the wait for the PG's map, pause is the time between
	// two looks.
	bound, pause time.Duration
}

// rbdJournalFenceAddress blocklists address and waits until the PG pgid on
// osd has a map with the entry.
func rbdJournalFenceAddress(ctx context.Context, address string, osd int, pgid string, ops rbdJournalFenceOps) (r rbdJournalFence, err error) {
	started := time.Now()
	r = rbdJournalFence{Address: address, OSD: osd, PGID: pgid, Commands: []string{}, Ops: []rbdJournalHeldOp{}, Down: []int{}}
	defer func() { r.ElapsedMS = time.Since(started).Milliseconds() }()
	if address == "" || osd < 0 || pgid == "" {
		return r, errors.New("invalid fence scope")
	}
	if r.BlocklistBefore, err = ops.blocklist(ctx); err != nil {
		return r, err
	}
	if mdsSessionsFindBlock(r.BlocklistBefore, address) != nil {
		return r, errors.New("the lock record's address is blocklisted before the fence")
	}
	r.Commands = append(r.Commands, "osd blocklist add "+address)
	if err := ops.add(ctx, address); err != nil {
		return r, err
	}
	if r.Blocklist, err = ops.blocklist(ctx); err != nil {
		return r, err
	}
	if mdsSessionsFindBlock(r.Blocklist, address) == nil {
		return r, errors.New("the MON does not list the lock record's address after the fence")
	}
	if r.Epoch, r.Down, err = ops.held.down(ctx); err != nil {
		return r, err
	}
	// The PG's own map epoch is its query's (rbd_journal_held.go).
	for deadline := time.Now().Add(ops.bound); ; {
		r.Polls++
		q, err := ops.held.query(ctx, pgid)
		if err != nil {
			return r, err
		}
		r.PGEpoch, r.PGState = q.Epoch, q.State
		if q.Epoch >= r.Epoch {
			break
		}
		if !time.Now().Before(deadline) {
			return r, errors.New("the primary's PG did not reach the fenced map")
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(ops.pause):
		}
	}
	if r.Ops, err = ops.held.inFlight(ctx, osd); err != nil {
		return r, err
	}
	return r, nil
}

// rbdJournalAlign is the `align-primary` receipt: the image header's and
// the erasure-coded journal object's placements before, the journal
// object's after, the command (none when the primaries already agree) and
// the erasure-coded PG's query once it is clean under the header's
// primary.
type rbdJournalAlign struct {
	Header   rbdJournalPlacement `json:"header"`
	Before   rbdJournalPlacement `json:"before"`
	After    rbdJournalPlacement `json:"after"`
	Commands []string            `json:"commands"`
	Query    *rbdJournalPGQuery  `json:"pg_query"`
	Polls    int                 `json:"polls"`
	// ElapsedMS is the whole action's time.
	ElapsedMS int64 `json:"elapsed_ms"`
}

// rbdJournalAlignOps are align-primary's reads and its one command.
type rbdJournalAlignOps struct {
	place       func(ctx context.Context, object string) (rbdJournalPlacement, error)
	query       func(ctx context.Context, pgid string) (rbdJournalPGQuery, error)
	primaryTemp func(ctx context.Context, pgid string, osd int) error
	bound       time.Duration
	pause       time.Duration
}

// rbdJournalAlignPrimary makes the erasure-coded journal object's acting
// primary the header's, keeping every shard in its slot.
func rbdJournalAlignPrimary(ctx context.Context, pool, journalPool, header, object string, ops rbdJournalAlignOps) (r rbdJournalAlign, err error) {
	started := time.Now()
	r = rbdJournalAlign{Commands: []string{}}
	defer func() { r.ElapsedMS = time.Since(started).Milliseconds() }()
	if r.Header, err = ops.place(ctx, header); err != nil {
		return r, err
	}
	if r.Before, err = ops.place(ctx, object); err != nil {
		return r, err
	}
	r.After = r.Before
	if !r.Header.settled(pool, header) || !r.Before.settled(journalPool, object) {
		return r, errors.New("the header or the journal object is not on a settled three-member placement")
	}
	target := r.Header.ActingPrimary
	if r.Before.ActingPrimary != target {
		r.Commands = append(r.Commands, fmt.Sprintf("osd primary-temp %s %d", r.Before.PGID, target))
		if err := ops.primaryTemp(ctx, r.Before.PGID, target); err != nil {
			return r, err
		}
	}
	for deadline := time.Now().Add(ops.bound); ; {
		r.Polls++
		if r.After, err = ops.place(ctx, object); err != nil {
			return r, err
		}
		moved := len(r.Commands) == 0 && r.After.samePlacement(r.Before) || len(r.Commands) != 0 && r.After.erasureMovedFrom(r.Before, target)
		if moved {
			q, err := ops.query(ctx, r.After.PGID)
			if err == nil && q.State == "active+clean" && slices.Equal(q.Acting, r.Before.Acting) && slices.Equal(q.Up, r.Before.Up) {
				r.Query = &q
				return r, nil
			}
		}
		if !time.Now().Before(deadline) {
			return r, errors.New("the erasure-coded journal PG is not clean under the header's primary")
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(ops.pause):
		}
	}
}

// samePlacement says that p places the object as before did, in the same
// or a later epoch.
func (p rbdJournalPlacement) samePlacement(before rbdJournalPlacement) bool {
	return p.Epoch >= before.Epoch && p.Pool == before.Pool && p.PoolID == before.PoolID && p.Object == before.Object && p.PGID == before.PGID &&
		slices.Equal(p.Up, before.Up) && slices.Equal(p.Acting, before.Acting) && p.UpPrimary == before.UpPrimary && p.ActingPrimary == before.ActingPrimary
}

// rbdJournalKeepUp is the `keep-up` receipt: the one `osd add-nodown` of
// the PG's primary and that OSD's entry before and after it.
type rbdJournalKeepUp struct {
	OSD       int               `json:"osd"`
	Commands  []string          `json:"commands"`
	Before    rbdJournalOSDInfo `json:"before"`
	After     rbdJournalOSDInfo `json:"after"`
	ElapsedMS int64             `json:"elapsed_ms"`
}

// rbdJournalPrimaryDead is the `observe-primary-dead` read: every sample
// of the PG's primary's OSDMap entry and the followed objects' placements,
// read repeatedly for rbdJournalPrimaryDeadDwell. The fixture keeps each
// sample as it was read and judges none of them; a sample never replaces an
// earlier one, so a state that changes and recovers within the window stays
// visible.
type rbdJournalPrimaryDead struct {
	OSD       int                       `json:"osd"`
	Address   string                    `json:"address"`
	Samples   []rbdJournalPrimarySample `json:"samples"`
	ElapsedMS int64                     `json:"elapsed_ms"`
}

// rbdJournalPrimarySample is one read of the window: when it was taken, the
// primary's OSDMap entry and the followed objects' placements.
type rbdJournalPrimarySample struct {
	UTC     string                `json:"utc"`
	Info    rbdJournalOSDInfo     `json:"info"`
	Objects []rbdJournalPlacement `json:"objects"`
}

// rbdJournalPrimaryDeadDwell is how long observe-primary-dead keeps
// reading: several times the 2.2 s and 2.3 s the cluster took to mark the
// killed primary down from its peers' reports in the primary-restart rows.
// rbdJournalPrimaryDeadSamples bounds the samples one window keeps; a
// window that would need more is an error, not a truncated receipt.
const (
	rbdJournalPrimaryDeadDwell   = 10 * time.Second
	rbdJournalPrimaryDeadSamples = 64
)

// rbdJournalObservePrimaryDead reads osd's entry and the objects'
// placements until the dwell has passed, keeping every sample, and stops
// early only on a read error or at the sample bound. It never reads the PG
// itself: its primary is dead while the OSDMap names it up, so a PG query or
// object read would block on it.
func rbdJournalObservePrimaryDead(ctx context.Context, osd int, address string, objects []string, info func(context.Context, int) (rbdJournalOSDInfo, error),
	place func(context.Context, string) (rbdJournalPlacement, error), dwell, pause time.Duration) (r rbdJournalPrimaryDead, err error) {
	started := time.Now()
	r = rbdJournalPrimaryDead{OSD: osd, Address: address, Samples: []rbdJournalPrimarySample{}}
	defer func() { r.ElapsedMS = time.Since(started).Milliseconds() }()
	if osd < 0 || address == "" || len(objects) == 0 {
		return r, errors.New("invalid primary-dead observation scope")
	}
	for deadline := time.Now().Add(dwell); ; {
		if len(r.Samples) == rbdJournalPrimaryDeadSamples {
			return r, errors.New("the primary-dead window exceeds its sample bound")
		}
		got, err := info(ctx, osd)
		if err != nil {
			return r, err
		}
		sample := rbdJournalPrimarySample{UTC: time.Now().UTC().Format(time.RFC3339Nano), Info: got, Objects: []rbdJournalPlacement{}}
		for _, object := range objects {
			p, err := place(ctx, object)
			if err != nil {
				return r, err
			}
			sample.Objects = append(sample.Objects, p)
		}
		r.Samples = append(r.Samples, sample)
		if !time.Now().Before(deadline) {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(pause):
		}
	}
}

// rbdJournalFencedAdmin names this file's fixture-own actions.
func rbdJournalFencedAdmin(action string) bool {
	return slices.Contains([]string{"fence", "align-primary", "keep-up", "observe-primary-dead"}, action)
}

// rbdJournalFencedControl runs one of this file's fixture-own actions and
// returns its receipt under the receipt's key.
func (c *fixtureCluster) rbdJournalFencedControl(ctx context.Context, action string) (string, any, error) {
	x := c.rbdBase
	if x.journalImageID == "" || x.poolID < 0 {
		return "", nil, errors.New("the journaling image is unknown")
	}
	objects := rbdJournalPrimaryObjects(x.poolID, x.journalImageID)
	held := c.rbdJournalHeldOps()
	switch action {
	case "fence":
		if !c.profile.rbdJournalFencedSequence() || len(x.lockOwnerAddresses) != 1 || x.journalHeldPrimary < 0 {
			return "fence", nil, errors.New("the fence has no single lock record address or primary")
		}
		object := objects[1]
		p, err := held.place(ctx, object)
		if err != nil {
			return "fence", nil, err
		}
		ops := rbdJournalFenceOps{blocklist: c.mdsSessionsBlocklist, held: held, bound: 8 * time.Second, pause: 200 * time.Millisecond,
			add: func(ctx context.Context, address string) error {
				_, err := c.cluster.Ceph(ctx, "osd", "blocklist", "add", address)
				return err
			}}
		fence, err := rbdJournalFenceAddress(ctx, x.lockOwnerAddresses[0], x.journalHeldPrimary, p.PGID, ops)
		return "fence", fence, err
	case "align-primary":
		if !c.profile.rbdJournalAlignedSequence() || x.group.secondPool == nil {
			return "align", nil, errors.New("the primary alignment has no erasure-coded journal pool")
		}
		ops := rbdJournalAlignOps{place: held.place, query: held.query, bound: 30 * time.Second, pause: 200 * time.Millisecond,
			primaryTemp: func(ctx context.Context, pgid string, osd int) error {
				if !strings.HasPrefix(pgid, strconv.FormatInt(x.group.secondPool.ID, 10)+".") {
					return errors.New("primary override does not name the owned journal PG")
				}
				set, err := c.rbdJournalReadPrimaryTemp(ctx, pgid)
				if err != nil {
					return err
				}
				if set {
					return errors.New("owned journal PG already has a primary override")
				}
				// Recorded before the command, so that the cleanup
				// removes it whatever the command's outcome.
				x.journalPrimaryTemp = pgid
				if err := c.rbdBaseJournal(); err != nil {
					return err
				}
				_, err = c.cluster.Ceph(ctx, "osd", "primary-temp", pgid, strconv.Itoa(osd))
				return err
			}}
		align, err := rbdJournalAlignPrimary(ctx, x.plan.Pool, x.journalPlan.JournalPool, objects[0], objects[1], ops)
		return "align", align, err
	case "keep-up":
		if !c.profile.rbdJournalBeforeDownSequence() || x.journalHeldPrimary < 0 {
			return "keep_up", nil, errors.New("keep-up has no primary")
		}
		started := time.Now()
		osd := x.journalHeldPrimary
		r := rbdJournalKeepUp{OSD: osd, Commands: []string{}}
		var err error
		if r.Before, err = c.rbdJournalOSDInfo(ctx, osd); err != nil {
			return "keep_up", r, err
		}
		// Recorded before the command, so that the cleanup removes the flag
		// whatever the command's outcome.
		x.osdsNoDown = append(x.osdsNoDown, osd)
		if err := c.rbdBaseJournal(); err != nil {
			return "keep_up", r, err
		}
		r.Commands = append(r.Commands, "osd add-nodown "+strconv.Itoa(osd))
		if _, err := c.cluster.Ceph(ctx, "osd", "add-nodown", strconv.Itoa(osd)); err != nil {
			return "keep_up", r, err
		}
		r.After, err = c.rbdJournalOSDInfo(ctx, osd)
		r.ElapsedMS = time.Since(started).Milliseconds()
		if err == nil && (!slices.Contains(r.After.State, "nodown") || !r.After.Up || r.After.Address == "") {
			err = errors.New("the OSDMap does not list the primary up and flagged nodown")
		}
		// observe-primary-dead compares the killed instance's address with
		// this one.
		x.journalHeldAddress = r.After.Address
		return "keep_up", r, err
	case "observe-primary-dead":
		if !c.profile.rbdJournalBeforeDownSequence() || x.journalHeldPrimary < 0 || x.journalHeldAddress == "" {
			return "dead", nil, errors.New("the primary-dead observation has no primary")
		}
		followed := objects
		if x.journalPlan.JournalPool != "" {
			// The journal object's erasure-coded PG alone, as the
			// held-request reads follow it: the headers are in another
			// pool's PG.
			followed = objects[1:2]
		}
		dead, err := rbdJournalObservePrimaryDead(ctx, x.journalHeldPrimary, x.journalHeldAddress, followed, c.rbdJournalOSDInfo, held.place, rbdJournalPrimaryDeadDwell,
			500*time.Millisecond)
		return "dead", dead, err
	}
	return "", nil, fmt.Errorf("journal action %s is not the fixture's own", action)
}

// rbdJournalRemoveNoDown clears the nodown flag of every OSD keep-up set
// and returns the OSDs a following OSDMap read shows without it.
func (c *fixtureCluster) rbdJournalRemoveNoDown(ctx context.Context, osds []int) ([]int, error) {
	var errs []error
	for _, osd := range osds {
		_, err := c.cluster.Ceph(ctx, "osd", "rm-nodown", strconv.Itoa(osd))
		errs = append(errs, err)
	}
	var cleared []int
	for _, osd := range osds {
		info, err := c.rbdJournalOSDInfo(ctx, osd)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if slices.Contains(info.State, "nodown") {
			errs = append(errs, fmt.Errorf("osd.%d kept its nodown flag", osd))
			continue
		}
		cleared = append(cleared, osd)
	}
	return cleared, errors.Join(errs...)
}

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's sequences when they exist.
var _ = collectDiagnosticFiles(
	"rbd-journal-align-primary.json",
	"rbd-journal-erasure-fenced-hold-outcome.json",
	"rbd-journal-erasure-fenced-hold-results.json",
	"rbd-journal-erasure-fenced-pending-hold-outcome.json",
	"rbd-journal-erasure-fenced-pending-hold-results.json",
	"rbd-journal-erasure-primary-restart-before-down-outcome.json",
	"rbd-journal-erasure-primary-restart-before-down-results.json",
	"rbd-journal-fence.json",
	"rbd-journal-fenced-hold-outcome.json",
	"rbd-journal-fenced-hold-results.json",
	"rbd-journal-fenced-pending-hold-outcome.json",
	"rbd-journal-fenced-pending-hold-results.json",
	"rbd-journal-keep-up.json",
	"rbd-journal-observe-primary-dead.json",
	"rbd-journal-primary-restart-before-down-outcome.json",
	"rbd-journal-primary-restart-before-down-results.json",
)
