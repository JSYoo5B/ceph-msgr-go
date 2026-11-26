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

// The held-request journal sequences run the lost-append-reply setup on
// three replicated BlueStore OSDs (profile.go rbdReplicas), with one PG in
// the owned pool, and hold the Go lock owner's journal append in the
// cluster instead of withholding its reply in the client:
//
//   - replica hold: the Go test pauses one replica of the PG through the
//     fixture's daemon lease (daemon_lease.go) before it sends the append.
//     The primary executes it, sends one MOSDRepOp to each replica and
//     answers the client only once every acting replica committed
//     (osd/ReplicatedBackend.cc submit_transaction, do_repop_reply;
//     osd/PrimaryLogPG.cc issue_repop and eval_repop at rbdBaseSource). The
//     test then resumes the replica and takes the answer.
//   - held primary change: the append names an OSDMap epoch the cluster has
//     not reached, so the primary holds it (osd/OSD.cc
//     dispatch_session_waiting). The fixture's `change-primary` action marks
//     that primary out, and its `advance-map` action then raises the epoch
//     past the named one as the lock control's pending sequence does
//     (rbdLockAdvanceMap in rbd_lock.go, sampling the old primary's
//     operations in flight before and after), so that the old primary has
//     the named epoch, in which it no longer leads the PG.
//   - dead primary: the replica hold above, then the Go test kills the
//     primary through the daemon control (control_daemon.go) while the
//     append waits there, resumes the replica and lets the cluster mark the
//     primary down.
//   - pending hold: the replica hold with three appends of the Go lock
//     owner, to journal objects 0, 1 and 2, all waiting for the paused
//     replica at once.
//   - erasure hold: the replica hold with the journal's data objects in
//     an erasure-coded pool (k=2, m=1 over the three OSDs, overwrites
//     allowed, one PG) while the image header and the journal header stay
//     in the replicated owned pool. librbd creates the journal with
//     rbd_journal_pool naming that pool (librbd/journal/CreateRequest.cc)
//     and names its data objects after the header's pool
//     (journal/Journaler.cc object_oid_prefix); cls_journal's append only
//     stats and writes the object (cls/journal/cls_journal.cc), which an
//     erasure-coded pool with overwrites executes. The test pauses the last
//     member of the erasure-coded PG's acting set, one shard, before it
//     sends the append to that PG's primary, which then answers only once
//     every shard committed (osd/ECCommon.cc try_finish_rmw). The primary
//     writes its own shard with the client op, which records that
//     sub-write's start and commit (osd/ECBackend.cc handle_sub_write and
//     SubWriteCommitted); the other shards' commits are not recorded on
//     it.
//   - primary restart: the dead primary with two appends, one waiting for
//     the paused replica (journal object 0) and one naming an OSDMap epoch
//     the cluster has not reached (journal object 1); after the primary was
//     marked down the Go test starts it again through the daemon control and
//     `observe-primary-back` waits for the PG to be clean on its original
//     members.
//
// Apart from `change-primary` and `advance-map` the fixture's part is
// observation only: `observe-placed`, `observe-held`, `observe-settled`,
// `observe-primary-lost` and `observe-primary-back` run no native driver.
// Each reads the placement of the image header, journal object 0 and the
// journal header with `ceph osd map`, the PG's peering history, sets and last
// updates with `ceph pg <pgid> query`, and the OSDMap's down OSDs;
// `observe-held` also reads the primary's operations in flight
// (`dump_ops_in_flight`, with their events). `observe-held`,
// `observe-primary-lost` and `observe-primary-back` wait for a state to
// appear, bounded. What the reads mean is the test's to judge.

// capRBDJournalReplicaHold admits the replica-hold sequence: its test runs
// alone on a fresh three-OSD RBD fixture and pauses one replica OSD under a
// lease.
var capRBDJournalReplicaHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-replica-hold",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose append waits for a paused replica's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalHeldPrimaryChange admits the held primary-change sequence:
// its test runs alone on a fresh three-OSD RBD fixture and the fixture marks
// the PG's primary out while the append is held there.
var capRBDJournalHeldPrimaryChange = defineExclusiveCapability(fixtureRBD, "rbd-journal-held-primary-change",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose append is held at a primary that is then marked out",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalDeadPrimary admits the dead-primary sequence: its test runs
// alone on a fresh three-OSD RBD fixture, pauses one replica and kills the
// PG's primary, which is not started again.
var capRBDJournalDeadPrimary = defineExclusiveCapability(fixtureRBD, "rbd-journal-dead-primary",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose append waits at a primary that is then killed",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalPendingHold admits the pending-hold sequence: its test runs
// alone on a fresh three-OSD RBD fixture and pauses one replica OSD under a
// lease while three appends are held for it.
var capRBDJournalPendingHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-pending-hold",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose three appends are held for a paused replica's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalPrimaryRestart admits the primary-restart sequence: its test
// runs alone on a fresh three-OSD RBD fixture, pauses one replica, kills the
// PG's primary while two appends are held there and starts it again.
var capRBDJournalPrimaryRestart = defineExclusiveCapability(fixtureRBD, "rbd-journal-primary-restart",
	"run one test alone on a fresh three-OSD RBD fixture with the journal sequence whose appends are held at a primary that is killed and started again",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

// capRBDJournalErasureHold admits the erasure-hold sequence: its test runs
// alone on a fresh three-OSD RBD fixture with the erasure-coded journal
// pool and pauses one shard OSD of that pool's PG under a lease.
var capRBDJournalErasureHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-hold",
	"run one test alone on a fresh three-OSD RBD fixture whose journal data objects are in an erasure-coded pool, with the journal sequence whose append is held for a paused shard's commit",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

var capRBDJournalErasureDeadPrimary = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-dead-primary",
	"run one isolated erasure-coded journal test that kills the primary with an append still pending",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

var capRBDJournalErasureHeldPrimaryChange = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-held-primary-change",
	"run one isolated erasure-coded journal test that moves the data PG primary while an append waits for an OSDMap",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

var capRBDJournalErasurePendingHold = defineExclusiveCapability(fixtureRBD, "rbd-journal-erasure-pending-hold",
	"run one isolated erasure-coded journal test that holds three appends during a shard pause",
	"RBD=1 OSD_REPLICAS=3 OSD_STORE=bluestore", nil)

func (p profile) rbdJournalErasureFault() bool {
	return p.declares(capRBDJournalErasureDeadPrimary) || p.declares(capRBDJournalErasureHeldPrimaryChange) || p.declares(capRBDJournalErasurePendingHold)
}

func (p profile) rbdJournalErasure() bool {
	return p.declares(capRBDJournalErasureHold) || p.rbdJournalErasureFault()
}

// rbdJournalErasureK and rbdJournalErasureM are the erasure-coded journal
// pool's data and coding shards: all three OSDs hold one shard each.
const (
	rbdJournalErasureK = 2
	rbdJournalErasureM = 1
)

// The six sequences. `observe-answered`, `observe-recorded` and
// `observe-restarted` are the driver's ordinary observation,
// `change-primary` the primary-change sequence's action and `replay` the
// native replay.
var (
	rbdJournalReplicaHoldActions       = []string{"setup", "observe-locked", "observe-placed", "observe-held", "observe-settled", "observe-answered", "replay"}
	rbdJournalHeldPrimaryChangeActions = []string{"setup", "observe-locked", "observe-placed", "observe-held", "change-primary", "advance-map", "observe-settled", "observe-recorded", "replay"}
	rbdJournalDeadPrimaryActions       = []string{"setup", "observe-locked", "observe-placed", "observe-held", "observe-primary-lost", "observe-recorded", "replay"}
	rbdJournalPendingHoldActions       = slices.Clone(rbdJournalReplicaHoldActions)
	rbdJournalErasureHoldActions       = slices.Clone(rbdJournalReplicaHoldActions)
	rbdJournalPrimaryRestartActions    = []string{"setup", "observe-locked", "observe-placed", "observe-held", "observe-primary-lost", "observe-recorded", "observe-primary-back",
		"observe-restarted", "replay"}
)

// rbdJournalThreeOSD says whether the profile selects a journal sequence of
// the three-OSD topology.
func (p profile) rbdJournalThreeOSD() bool {
	return p.declares(capRBDJournalPrimaryChange) || p.declares(capRBDJournalReplicaHold) || p.declares(capRBDJournalHeldPrimaryChange) || p.declares(capRBDJournalDeadPrimary) ||
		p.declares(capRBDJournalPendingHold) || p.declares(capRBDJournalPrimaryRestart) || p.rbdJournalErasure()
}

// rbdJournalHeldAdmin names the fixture's own read-only actions of the
// held-request sequences.
func rbdJournalHeldAdmin(action string) bool {
	return slices.Contains([]string{"observe-placed", "observe-held", "observe-settled", "observe-primary-lost", "observe-primary-back"}, action)
}

// rbdJournalPGPeer is one peer of `ceph pg query` with its last update and,
// in an erasure-coded PG, the shard it holds (absent in a replicated PG).
type rbdJournalPGPeer struct {
	Peer       int    `json:"peer"`
	Shard      *int   `json:"shard,omitempty"`
	LastUpdate string `json:"last_update"`
}

// rbdJournalPGQuery is the part of `ceph pg <pgid> query` the held-request
// sequences keep: the state, the query's epoch, the up and acting sets, the
// primary's last update, the peering history and each peer's last update.
type rbdJournalPGQuery struct {
	State             string `json:"state"`
	Epoch             uint32 `json:"epoch"`
	Up                []int  `json:"up"`
	Acting            []int  `json:"acting"`
	LastUpdate        string `json:"last_update"`
	SameIntervalSince uint32 `json:"same_interval_since"`
	SameUpSince       uint32 `json:"same_up_since"`
	SamePrimarySince  uint32 `json:"same_primary_since"`
	LastEpochStarted  uint32 `json:"last_epoch_started"`
	// Shard is the shard of the answering primary in an erasure-coded PG
	// (absent in a replicated PG).
	Shard *int               `json:"shard,omitempty"`
	Peers []rbdJournalPGPeer `json:"peers"`
}

// decodeRBDJournalPGQuery projects one `ceph pg <pgid> query` output of pgid.
func decodeRBDJournalPGQuery(pgid string, data []byte) (rbdJournalPGQuery, error) {
	var raw struct {
		State  string `json:"state"`
		Epoch  uint32 `json:"epoch"`
		Up     []int  `json:"up"`
		Acting []int  `json:"acting"`
		Info   *struct {
			PGID       string `json:"pgid"`
			LastUpdate string `json:"last_update"`
			History    *struct {
				LastEpochStarted  uint32 `json:"last_epoch_started"`
				SameUpSince       uint32 `json:"same_up_since"`
				SameIntervalSince uint32 `json:"same_interval_since"`
				SamePrimarySince  uint32 `json:"same_primary_since"`
			} `json:"history"`
		} `json:"info"`
		PeerInfo []struct {
			Peer       string `json:"peer"`
			LastUpdate string `json:"last_update"`
		} `json:"peer_info"`
	}
	// An erasure-coded PG's primary names itself with its shard
	// (<pgid>s<shard>) and each peer as <osd>(<shard>) (osd/osd_types.cc
	// spg_t and pg_shard_t at rbdBaseSource). A shard is the member's index
	// in the acting set, which is how the projection binds it.
	shard := func(name, prefix string) (int, bool) {
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok || rest == "" {
			return 0, false
		}
		n, err := strconv.ParseUint(rest, 10, 8)
		return int(n), err == nil && strconv.FormatUint(n, 10) == rest
	}
	if len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &raw) != nil || raw.Info == nil || raw.Info.History == nil || raw.State == "" ||
		raw.Epoch == 0 || raw.Up == nil || raw.Acting == nil || len(raw.PeerInfo) > 8 {
		return rbdJournalPGQuery{}, errors.New("native PG query unavailable")
	}
	var primaryShard *int
	if raw.Info.PGID != pgid {
		s, ok := shard(raw.Info.PGID, pgid+"s")
		if !ok {
			return rbdJournalPGQuery{}, errors.New("native PG query names another PG")
		}
		primaryShard = &s
	}
	h := raw.Info.History
	q := rbdJournalPGQuery{State: raw.State, Epoch: raw.Epoch, Up: raw.Up, Acting: raw.Acting, LastUpdate: raw.Info.LastUpdate, SameIntervalSince: h.SameIntervalSince,
		SameUpSince: h.SameUpSince, SamePrimarySince: h.SamePrimarySince, LastEpochStarted: h.LastEpochStarted, Peers: []rbdJournalPGPeer{}}
	q.Shard = primaryShard
	// In an erasure-coded PG every member is named with its shard: the
	// primary's and each peer's are distinct acting indices, and the acting
	// member at a peer's index is that peer.
	inActing := func(s int) bool { return s < len(raw.Acting) }
	used := map[int]bool{}
	if primaryShard != nil {
		if !inActing(*primaryShard) {
			return rbdJournalPGQuery{}, errors.New("native PG query names a primary shard outside the acting set")
		}
		used[*primaryShard] = true
	}
	for _, p := range raw.PeerInfo {
		peer, peerShard := p.Peer, (*int)(nil)
		if osd, rest, ok := strings.Cut(peer, "("); ok && strings.HasSuffix(rest, ")") {
			s, valid := shard(strings.TrimSuffix(rest, ")"), "")
			if !valid {
				return rbdJournalPGQuery{}, errors.New("native PG query names a peer with a malformed shard")
			}
			peer, peerShard = osd, &s
		}
		id, err := strconv.Atoi(peer)
		if err != nil || id < 0 || strconv.Itoa(id) != peer {
			return rbdJournalPGQuery{}, errors.New("native PG query names a peer that is not an OSD")
		}
		if (primaryShard == nil) != (peerShard == nil) {
			return rbdJournalPGQuery{}, errors.New("native PG query mixes replicated and erasure-coded member names")
		}
		if peerShard != nil {
			if !inActing(*peerShard) || used[*peerShard] || raw.Acting[*peerShard] != id {
				return rbdJournalPGQuery{}, errors.New("native PG query names a peer shard that is not that peer's acting index")
			}
			used[*peerShard] = true
		}
		q.Peers = append(q.Peers, rbdJournalPGPeer{Peer: id, Shard: peerShard, LastUpdate: p.LastUpdate})
	}
	if primaryShard != nil {
		for _, p := range q.Peers {
			if p.Peer == raw.Acting[*primaryShard] {
				return rbdJournalPGQuery{}, errors.New("native PG query names the primary's acting index for a peer")
			}
		}
	}
	return q, nil
}

// rbdJournalHeldOp is one client operation an OSD lists in flight, with the
// events its op tracker recorded, in order.
type rbdJournalHeldOp struct {
	OSD         int      `json:"osd"`
	Description string   `json:"description"`
	Client      string   `json:"client"`
	Transaction uint64   `json:"tid"`
	FlagPoint   string   `json:"flag_point"`
	Events      []string `json:"events"`
}

// rbdJournalHeldOpsLimit and rbdJournalHeldEventsLimit bound what one read
// keeps.
const (
	rbdJournalHeldOpsLimit    = 32
	rbdJournalHeldEventsLimit = 64
)

// decodeRBDJournalHeldOps projects one OSD's `dump_ops_in_flight` output to
// its client operations. The OSD writes the ops array even when it holds
// nothing, so an absent array is refused.
func decodeRBDJournalHeldOps(osd int, data []byte) ([]rbdJournalHeldOp, error) {
	var dump struct {
		Ops *[]struct {
			Description string `json:"description"`
			TypeData    struct {
				FlagPoint  string `json:"flag_point"`
				ClientInfo *struct {
					Client string `json:"client"`
					TID    uint64 `json:"tid"`
				} `json:"client_info"`
				Events []struct {
					Event string `json:"event"`
				} `json:"events"`
			} `json:"type_data"`
		} `json:"ops"`
		Count *int `json:"num_ops"`
	}
	if len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.Ops == nil || dump.Count == nil || *dump.Count != len(*dump.Ops) {
		return nil, errors.New("native OSD operations in flight unavailable")
	}
	ops := []rbdJournalHeldOp{}
	for _, o := range *dump.Ops {
		if o.TypeData.ClientInfo == nil {
			continue
		}
		if len(ops) == rbdJournalHeldOpsLimit || len(o.Description) > 1024 || len(o.TypeData.Events) > rbdJournalHeldEventsLimit {
			return nil, errors.New("native OSD operations in flight exceed their bound")
		}
		op := rbdJournalHeldOp{OSD: osd, Description: o.Description, Client: o.TypeData.ClientInfo.Client, Transaction: o.TypeData.ClientInfo.TID, FlagPoint: o.TypeData.FlagPoint,
			Events: []string{}}
		for _, e := range o.TypeData.Events {
			op.Events = append(op.Events, e.Event)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// rbdJournalHeldRead is the receipt of one fixture observation of the
// held-request sequences. OSD is the OSD whose operations were read (-1 when
// none were), Settled whether the state the action waits for appeared within
// its bound (true for the actions that wait for nothing).
type rbdJournalHeldRead struct {
	Objects   []rbdJournalPlacement `json:"objects"`
	PGID      string                `json:"pgid"`
	Query     *rbdJournalPGQuery    `json:"pg_query"`
	OSD       int                   `json:"osd"`
	Ops       []rbdJournalHeldOp    `json:"ops"`
	Epoch     uint32                `json:"epoch"`
	Down      []int                 `json:"down"`
	Settled   bool                  `json:"settled"`
	Polls     int                   `json:"polls"`
	ElapsedMS int64                 `json:"elapsed_ms"`
}

// rbdJournalHeldOps reads one object's placement, one PG's query, one OSD's
// operations in flight and the OSDMap's epoch with its down OSDs.
type rbdJournalHeldOps struct {
	place    func(ctx context.Context, object string) (rbdJournalPlacement, error)
	query    func(ctx context.Context, pgid string) (rbdJournalPGQuery, error)
	inFlight func(ctx context.Context, osd int) ([]rbdJournalHeldOp, error)
	down     func(ctx context.Context) (uint32, []int, error)
	// held bounds the wait of observe-held, lost that of
	// observe-primary-lost, back that of observe-primary-back, and pause is
	// the time between two looks.
	held, lost, back, pause time.Duration
}

// rbdJournalHeldWait says what one held-request observation waits for.
type rbdJournalHeldWait struct {
	// osd is the OSD whose operations observe-held reads; for
	// observe-primary-lost it is the primary that must go down, and for
	// observe-primary-back the one that must be up and in the PG again.
	osd int
	// ops are the operations observe-held waits for, all at once.
	ops []rbdJournalHeldAwait
	// acting is the acting set observe-primary-back waits for.
	acting []int
}

// rbdJournalHeldAwait is one awaited operation: the object its description
// names and whether a replica's commit must be recorded on it.
type rbdJournalHeldAwait struct {
	object    string
	committed bool
}

// rbdJournalHeldObserve runs one held-request observation of objects, which
// must share one PG.
func rbdJournalHeldObserve(ctx context.Context, action string, objects []string, wait rbdJournalHeldWait, ops rbdJournalHeldOps) (r rbdJournalHeldRead, err error) {
	started := time.Now()
	r = rbdJournalHeldRead{Objects: []rbdJournalPlacement{}, OSD: -1, Ops: []rbdJournalHeldOp{}, Down: []int{}}
	defer func() { r.ElapsedMS = time.Since(started).Milliseconds() }()
	if len(objects) == 0 || !rbdJournalHeldAdmin(action) {
		return r, errors.New("invalid held-request observation scope")
	}
	look := func() error {
		r.Polls++
		r.Objects = r.Objects[:0]
		for _, object := range objects {
			p, err := ops.place(ctx, object)
			if err != nil {
				return err
			}
			if len(r.Objects) != 0 && p.PGID != r.Objects[0].PGID {
				return errors.New("the followed objects are in different PGs")
			}
			r.Objects = append(r.Objects, p)
		}
		r.PGID = r.Objects[0].PGID
		var err error
		if r.Epoch, r.Down, err = ops.down(ctx); err != nil {
			return err
		}
		return nil
	}
	sleep := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ops.pause):
			return nil
		}
	}
	// held says whether the OSD lists every awaited operation.
	held := func() bool {
		for _, w := range wait.ops {
			if !slices.ContainsFunc(r.Ops, func(o rbdJournalHeldOp) bool {
				// A replicated primary records each replica's commit, an
				// erasure-coded one its own shard's.
				return strings.Contains(o.Description, ":::"+w.object+":head ") && (!w.committed || slices.Contains(o.Events, "sub_op_commit_rec") || slices.Contains(o.Events, "sub_op_committed"))
			}) {
				return false
			}
		}
		return true
	}
	readOps := func() error {
		got, err := ops.inFlight(ctx, wait.osd)
		if err != nil {
			return err
		}
		r.OSD, r.Ops = wait.osd, got
		return nil
	}
	switch action {
	case "observe-placed", "observe-settled":
		if err := look(); err != nil {
			return r, err
		}
		r.Settled = true
	case "observe-held":
		if wait.osd < 0 || len(wait.ops) == 0 || slices.ContainsFunc(wait.ops, func(w rbdJournalHeldAwait) bool { return w.object == "" }) {
			return r, errors.New("the held-request observation names no OSD or object")
		}
		// It waits until the OSD lists the operation.
		for deadline := time.Now().Add(ops.held); ; {
			if err := readOps(); err != nil {
				return r, err
			}
			if r.Settled = held(); r.Settled || !time.Now().Before(deadline) {
				break
			}
			if err := sleep(); err != nil {
				return r, err
			}
		}
		if err := look(); err != nil {
			return r, err
		}
	case "observe-primary-lost":
		if wait.osd < 0 {
			return r, errors.New("the held-request observation names no primary")
		}
		// The OSDMap marks the old primary down, then the PG is active again
		// under another primary whose query agrees with the placement.
		for deadline := time.Now().Add(ops.lost); ; {
			if err := look(); err != nil {
				return r, err
			}
			if p := r.Objects[0]; slices.Contains(r.Down, wait.osd) && p.ActingPrimary >= 0 && p.ActingPrimary != wait.osd {
				q, err := ops.query(ctx, r.PGID)
				if err == nil && strings.HasPrefix(q.State, "active") && slices.Equal(q.Acting, p.Acting) && !slices.Contains(q.Acting, wait.osd) {
					r.Query, r.Settled = &q, true
					return r, nil
				}
			}
			if !time.Now().Before(deadline) {
				return r, nil
			}
			if err := sleep(); err != nil {
				return r, err
			}
		}
	case "observe-primary-back":
		if wait.osd < 0 || len(wait.acting) == 0 || !slices.Contains(wait.acting, wait.osd) {
			return r, errors.New("the held-request observation names no restarted primary or acting set")
		}
		// The OSDMap has the restarted OSD up again, and the PG is clean on
		// the awaited acting set as the query agrees.
		for deadline := time.Now().Add(ops.back); ; {
			if err := look(); err != nil {
				return r, err
			}
			if p := r.Objects[0]; !slices.Contains(r.Down, wait.osd) && slices.Equal(p.Acting, wait.acting) && slices.Equal(p.Up, wait.acting) {
				q, err := ops.query(ctx, r.PGID)
				if err == nil && q.State == "active+clean" && slices.Equal(q.Acting, wait.acting) && slices.Equal(q.Up, wait.acting) {
					r.Query, r.Settled = &q, true
					return r, nil
				}
			}
			if !time.Now().Before(deadline) {
				return r, nil
			}
			if err := sleep(); err != nil {
				return r, err
			}
		}
	default:
		return r, fmt.Errorf("held-request action %s is not an observation", action)
	}
	q, err := ops.query(ctx, r.PGID)
	if err != nil {
		return r, err
	}
	r.Query = &q
	return r, nil
}

func (c *fixtureCluster) rbdJournalHeldOps() rbdJournalHeldOps {
	return rbdJournalHeldOps{
		place: c.rbdJournalPrimaryOps().place,
		query: func(ctx context.Context, pgid string) (rbdJournalPGQuery, error) {
			data, err := c.cluster.Ceph(ctx, "pg", pgid, "query", "--format", "json")
			if err != nil {
				return rbdJournalPGQuery{}, err
			}
			return decodeRBDJournalPGQuery(pgid, data)
		},
		inFlight: func(ctx context.Context, osd int) ([]rbdJournalHeldOp, error) {
			data, err := c.cluster.Ceph(ctx, "tell", "osd."+strconv.Itoa(osd), "dump_ops_in_flight")
			if err != nil {
				return nil, err
			}
			return decodeRBDJournalHeldOps(osd, data)
		},
		down: func(ctx context.Context) (uint32, []int, error) {
			data, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			var dump struct {
				FSID  string `json:"fsid"`
				Epoch uint32 `json:"epoch"`
				OSDs  []struct {
					ID int `json:"osd"`
					Up int `json:"up"`
				} `json:"osds"`
			}
			if err == nil && (len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.FSID != c.profile.fsid || dump.Epoch == 0 || len(dump.OSDs) == 0) {
				err = errors.New("native OSDMap unavailable")
			}
			if err != nil {
				return 0, nil, err
			}
			down := []int{}
			for _, o := range dump.OSDs {
				if o.Up != 1 {
					down = append(down, o.ID)
				}
			}
			return dump.Epoch, down, nil
		},
		held: 10 * time.Second, lost: 90 * time.Second, back: 150 * time.Second, pause: 200 * time.Millisecond,
	}
}

// rbdJournalHeldControl runs one fixture observation of a held-request
// sequence and keeps the primary it found before the append, which
// observe-primary-lost waits to see down.
func (c *fixtureCluster) rbdJournalHeldControl(ctx context.Context, action string) (rbdJournalHeldRead, error) {
	x := c.rbdBase
	if x.journalImageID == "" || x.poolID < 0 {
		return rbdJournalHeldRead{}, errors.New("the journaling image is unknown")
	}
	objects := rbdJournalPrimaryObjects(x.poolID, x.journalImageID)
	if x.journalPlan.JournalPool != "" {
		// The journal object follows the erasure-coded journal pool's PG
		// alone: the headers are in another pool's PG.
		objects = objects[1:2]
	}
	wait := rbdJournalHeldWait{osd: -1}
	switch action {
	case "observe-held":
		wait.osd, wait.ops = x.journalHeldPrimary, rbdJournalHeldAwaited(c.profile, x.poolID, x.journalImageID)
	case "observe-primary-lost":
		wait.osd = x.journalHeldPrimary
	case "observe-primary-back":
		wait.osd, wait.acting = x.journalHeldPrimary, x.journalHeldActing
	}
	read, err := rbdJournalHeldObserve(ctx, action, objects, wait, c.rbdJournalHeldOps())
	if err == nil && action == "observe-placed" {
		x.journalHeldPrimary, x.journalHeldActing = read.Objects[0].ActingPrimary, slices.Clone(read.Objects[0].Acting)
	}
	return read, err
}

// rbdJournalHeldAwaited are the operations observe-held waits for in the
// selected sequence: the append to journal object 0, with a replica's commit
// (an erasure-coded primary's own shard commit) recorded unless it is held
// for its map; in the pending-hold sequence the
// appends to journal objects 0, 1 and 2, each with a replica's commit; in
// the primary-restart sequence the append to journal object 0 with a
// replica's commit and the one to journal object 1, which is held for its
// map.
func rbdJournalHeldAwaited(p profile, poolID int64, imageID string) []rbdJournalHeldAwait {
	object := func(n int) string { return fmt.Sprintf("journal_data.%d.%s.%d", poolID, imageID, n) }
	switch {
	case p.admits(capRBDJournalPendingHold) || p.admits(capRBDJournalErasurePendingHold):
		return []rbdJournalHeldAwait{{object(0), true}, {object(1), true}, {object(2), true}}
	case p.admits(capRBDJournalPrimaryRestart):
		return []rbdJournalHeldAwait{{object(0), true}, {object(1), false}}
	}
	return []rbdJournalHeldAwait{{object(0), !(p.admits(capRBDJournalHeldPrimaryChange) || p.admits(capRBDJournalErasureHeldPrimaryChange))}}
}
