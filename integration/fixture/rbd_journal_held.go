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
//
// Apart from `change-primary` and `advance-map` the fixture's part is
// observation only: `observe-placed`, `observe-held`, `observe-settled` and
// `observe-primary-lost` run no native driver. Each reads the placement of
// the image header, journal object 0 and the journal header with `ceph osd
// map`, the PG's peering history, sets and last updates with `ceph pg
// <pgid> query`, and the OSDMap's down OSDs; `observe-held` also reads the
// primary's operations in flight (`dump_ops_in_flight`, with their events),
// and `observe-held` and `observe-primary-lost` wait for a state to appear,
// bounded. What the reads mean is the test's to judge.

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

// The three sequences. `observe-answered` and `observe-recorded` are the
// driver's ordinary observation, `change-primary` the primary-change
// sequence's action and `replay` the native replay.
var (
	rbdJournalReplicaHoldActions       = []string{"setup", "observe-locked", "observe-placed", "observe-held", "observe-settled", "observe-answered", "replay"}
	rbdJournalHeldPrimaryChangeActions = []string{"setup", "observe-locked", "observe-placed", "observe-held", "change-primary", "advance-map", "observe-settled", "observe-recorded", "replay"}
	rbdJournalDeadPrimaryActions       = []string{"setup", "observe-locked", "observe-placed", "observe-held", "observe-primary-lost", "observe-recorded", "replay"}
)

// rbdJournalThreeOSD says whether the profile selects a journal sequence of
// the three-OSD topology.
func (p profile) rbdJournalThreeOSD() bool {
	return p.declares(capRBDJournalPrimaryChange) || p.declares(capRBDJournalReplicaHold) || p.declares(capRBDJournalHeldPrimaryChange) || p.declares(capRBDJournalDeadPrimary)
}

// rbdJournalHeldAdmin names the fixture's own read-only actions of the
// held-request sequences.
func rbdJournalHeldAdmin(action string) bool {
	return slices.Contains([]string{"observe-placed", "observe-held", "observe-settled", "observe-primary-lost"}, action)
}

// rbdJournalPGPeer is one peer of `ceph pg query` with its last update.
type rbdJournalPGPeer struct {
	Peer       int    `json:"peer"`
	LastUpdate string `json:"last_update"`
}

// rbdJournalPGQuery is the part of `ceph pg <pgid> query` the held-request
// sequences keep: the state, the query's epoch, the up and acting sets, the
// primary's last update, the peering history and each peer's last update.
type rbdJournalPGQuery struct {
	State             string             `json:"state"`
	Epoch             uint32             `json:"epoch"`
	Up                []int              `json:"up"`
	Acting            []int              `json:"acting"`
	LastUpdate        string             `json:"last_update"`
	SameIntervalSince uint32             `json:"same_interval_since"`
	SameUpSince       uint32             `json:"same_up_since"`
	SamePrimarySince  uint32             `json:"same_primary_since"`
	LastEpochStarted  uint32             `json:"last_epoch_started"`
	Peers             []rbdJournalPGPeer `json:"peers"`
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
	if len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &raw) != nil || raw.Info == nil || raw.Info.History == nil || raw.Info.PGID != pgid || raw.State == "" ||
		raw.Epoch == 0 || raw.Up == nil || raw.Acting == nil || len(raw.PeerInfo) > 8 {
		return rbdJournalPGQuery{}, errors.New("native PG query unavailable")
	}
	h := raw.Info.History
	q := rbdJournalPGQuery{State: raw.State, Epoch: raw.Epoch, Up: raw.Up, Acting: raw.Acting, LastUpdate: raw.Info.LastUpdate, SameIntervalSince: h.SameIntervalSince,
		SameUpSince: h.SameUpSince, SamePrimarySince: h.SamePrimarySince, LastEpochStarted: h.LastEpochStarted, Peers: []rbdJournalPGPeer{}}
	for _, p := range raw.PeerInfo {
		id, err := strconv.Atoi(p.Peer)
		if err != nil || id < 0 {
			return rbdJournalPGQuery{}, errors.New("native PG query names a peer that is not an OSD")
		}
		q.Peers = append(q.Peers, rbdJournalPGPeer{Peer: id, LastUpdate: p.LastUpdate})
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
	// observe-primary-lost, and pause is the time between two looks.
	held, lost, pause time.Duration
}

// rbdJournalHeldWait says what one held-request observation waits for.
type rbdJournalHeldWait struct {
	// osd is the OSD whose operations observe-held reads; for
	// observe-primary-lost it is the primary that must go down.
	osd int
	// object is the name the awaited operation's description carries, and
	// committed requires that a replica's commit was recorded on it.
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
	// held says whether the OSD lists the awaited operation.
	held := func() bool {
		return slices.ContainsFunc(r.Ops, func(o rbdJournalHeldOp) bool {
			return strings.Contains(o.Description, wait.object) && (!wait.committed || slices.Contains(o.Events, "sub_op_commit_rec"))
		})
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
		if wait.osd < 0 || wait.object == "" {
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
		held: 10 * time.Second, lost: 90 * time.Second, pause: 200 * time.Millisecond,
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
	wait := rbdJournalHeldWait{osd: -1, object: objects[1]}
	switch action {
	case "observe-held":
		wait.osd, wait.committed = x.journalHeldPrimary, !c.profile.admits(capRBDJournalHeldPrimaryChange)
	case "observe-primary-lost":
		wait.osd = x.journalHeldPrimary
	}
	read, err := rbdJournalHeldObserve(ctx, action, objects, wait, c.rbdJournalHeldOps())
	if err == nil && action == "observe-placed" {
		x.journalHeldPrimary = read.Objects[0].ActingPrimary
	}
	return read, err
}
