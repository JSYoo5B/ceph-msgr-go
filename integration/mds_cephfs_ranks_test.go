package integration_test

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSRanksIntegration accepts one Go client's CephFS sessions
// with two active MDS ranks of one filesystem: explicit per-rank targets, a
// request forward answered by resubmitting to the destination rank, the
// capability EXPORT/IMPORT of a subtree migration between the ranks, and a
// standby takeover of rank 0 followed by a CephFS reconnect to the new daemon,
// against live Tentacle daemons with native oracles. See
// integration/fixture/MDS_CEPHFS_RANKS_FIXTURE.md and
// MDS_CEPHFS_RANKS_VALIDATION.md.

// CEPH_STAT_RSTAT (CEPH_CAP_FILE_WREXTEND): a getattr asking for it requires
// the authoritative MDS (Server::handle_client_getattr), so a non-auth rank
// forwards it.
const cephStatRStat = cephCapFileWRExtend

type nativeMDSCephFSRanksPlan struct {
	Directory    string   `json:"directory"`
	Forward      string   `json:"forward"`
	ForwardFile  string   `json:"forward_file"`
	Migrate      string   `json:"migrate"`
	MigrateFiles []string `json:"migrate_files"`
	// The standby-replay extension's directories.
	Chain         string   `json:"chain"`
	ChainFiles    []string `json:"chain_files"`
	Inflight      string   `json:"inflight"`
	InflightFiles []string `json:"inflight_files"`
	Replayed      string   `json:"replayed"`
	ReplayedFiles []string `json:"replayed_files"`
}

type nativeMDSCephFSRanksDaemon struct {
	Name        string `json:"name"`
	GID         uint64 `json:"gid"`
	Rank        int32  `json:"rank"`
	State       string `json:"state"`
	Incarnation int32  `json:"incarnation"`
	Endpoint    string `json:"endpoint"`
	Nonce       uint32 `json:"nonce"`
}

func (d *nativeMDSCephFSRanksDaemon) same(o *nativeMDSCephFSRanksDaemon) bool {
	return d != nil && o != nil && d.Name == o.Name && d.GID == o.GID && d.Rank == o.Rank && d.Incarnation == o.Incarnation && d.Endpoint == o.Endpoint && d.Nonce == o.Nonce
}

func (d *nativeMDSCephFSRanksDaemon) target(fscid, rank int32) cephmsgr.MDSTarget {
	return cephmsgr.MDSTarget{FilesystemID: fscid, Rank: rank, Address: fmt.Sprintf("v2:%s/%d", d.Endpoint, d.Nonce)}
}

type nativeMDSCephFSRanksTopology struct {
	FSMapEpoch    uint32                        `json:"fsmap_epoch"`
	MDSMapEpoch   uint32                        `json:"mdsmap_epoch"`
	MaxMDS        int                           `json:"max_mds"`
	In            []int                         `json:"in"`
	Failed        []int                         `json:"failed"`
	Ranks         []*nativeMDSCephFSRanksDaemon `json:"ranks"`
	Daemons       []nativeMDSCephFSRanksDaemon  `json:"daemons"`
	Standbys      []nativeMDSCephFSRanksDaemon  `json:"owned_standbys"`
	Followers     []nativeMDSCephFSRanksDaemon  `json:"standby_replay"`
	OtherStandbys int                           `json:"other_standbys"`
}

// nativeMDSCephFSRanksShape is the steady topology of one profile: the number
// of active ranks and whether the one spare follows rank 0 (standby replay).
type nativeMDSCephFSRanksShape struct {
	Ranks  int  `json:"ranks"`
	Follow bool `json:"follow"`
}

// spare returns the one spare daemon: the standby or the follower.
func (t nativeMDSCephFSRanksTopology) spare() *nativeMDSCephFSRanksDaemon {
	if len(t.Standbys) == 1 && len(t.Followers) == 0 {
		return &t.Standbys[0]
	}
	if len(t.Followers) == 1 && len(t.Standbys) == 0 {
		return &t.Followers[0]
	}
	return nil
}

// stable reports two active ranks and one standby on three distinct daemons.
func (t nativeMDSCephFSRanksTopology) stable() bool {
	return t.stableAs(nativeMDSCephFSRanksShape{Ranks: 2})
}

// stableAs reports shape.Ranks active ranks and one spare on distinct
// daemons: a standby, or with Follow a standby-replay follower of rank 0.
func (t nativeMDSCephFSRanksTopology) stableAs(shape nativeMDSCephFSRanksShape) bool {
	in := make([]int, shape.Ranks)
	for rank := range in {
		in[rank] = rank
	}
	spare := t.spare()
	if t.MaxMDS != shape.Ranks || !slices.Equal(t.In, in) || len(t.Failed) != 0 || len(t.Ranks) != shape.Ranks || spare == nil || (len(t.Followers) == 1) != shape.Follow || t.OtherStandbys != 0 {
		return false
	}
	names := map[string]bool{}
	for rank, d := range append(slices.Clone(t.Ranks), spare) {
		want, wantRank := "up:active", rank
		if rank == shape.Ranks {
			want, wantRank = "up:standby", -1
			if shape.Follow {
				want, wantRank = "up:standby-replay", 0
			}
		}
		if d == nil || names[d.Name] || d.State != want || d.GID == 0 || d.Nonce == 0 || d.Endpoint == "" || wantRank >= 0 && int(d.Rank) != wantRank {
			return false
		}
		names[d.Name] = true
	}
	return true
}

func (t nativeMDSCephFSRanksTopology) sameAs(o nativeMDSCephFSRanksTopology) bool {
	a, b := t.spare(), o.spare()
	if len(t.Ranks) < 2 || len(t.Ranks) != len(o.Ranks) || a == nil || b == nil {
		return false
	}
	for rank := range t.Ranks {
		if !t.Ranks[rank].same(o.Ranks[rank]) {
			return false
		}
	}
	return a.Name == b.Name && a.GID == b.GID && a.Nonce == b.Nonce && a.State == b.State
}

type nativeMDSCephFSRanksSubtree struct {
	Path            string `json:"path"`
	DirFrag         string `json:"dirfrag"`
	IsAuth          bool   `json:"is_auth"`
	AuthFirst       int    `json:"auth_first"`
	AuthSecond      int    `json:"auth_second"`
	ExportPin       int    `json:"export_pin"`
	ExportPinTarget int    `json:"export_pin_target"`
}

type nativeMDSCephFSRanksCounters struct {
	Forward        uint64 `json:"forward"`
	Exported       uint64 `json:"exported"`
	ExportedInodes uint64 `json:"exported_inodes"`
	Imported       uint64 `json:"imported"`
	ImportedInodes uint64 `json:"imported_inodes"`
}

type nativeMDSCephFSRanksView struct {
	Rank         int32                         `json:"rank"`
	Name         string                        `json:"name"`
	GID          uint64                        `json:"gid"`
	DaemonEpoch  uint32                        `json:"daemon_mdsmap_epoch"`
	Sessions     []nativeMDSSession            `json:"sessions"`
	Subtrees     []nativeMDSCephFSRanksSubtree `json:"subtrees"`
	SubtreeCount int                           `json:"subtree_count"`
	Counters     nativeMDSCephFSRanksCounters  `json:"counters"`
}

func (v nativeMDSCephFSRanksView) session(gid uint64) *nativeMDSSession {
	for i := range v.Sessions {
		if v.Sessions[i].ID == gid {
			return &v.Sessions[i]
		}
	}
	return nil
}

func (v nativeMDSCephFSRanksView) subtree(path string) *nativeMDSCephFSRanksSubtree {
	for i := range v.Subtrees {
		if v.Subtrees[i].Path == path {
			return &v.Subtrees[i]
		}
	}
	return nil
}

type nativeMDSCephFSRanksPin struct {
	Kind           string `json:"kind"`
	RequestID      int64  `json:"request_id"`
	Target         string `json:"target"`
	Path           string `json:"path"`
	Inode          uint64 `json:"inode"`
	Previous       string `json:"previous"`
	Value          string `json:"value"`
	State          string `json:"state"`
	SetterGlobalID uint64 `json:"setter_global_id"`
	Readback       string `json:"readback"`
	ReaderGlobalID uint64 `json:"reader_global_id"`
	Error          string `json:"error"`
}

type nativeMDSCephFSRanksPlacement struct {
	Target       string `json:"target"`
	Path         string `json:"path"`
	Rank         int32  `json:"rank"`
	Converged    bool   `json:"converged"`
	Polls        int    `json:"polls"`
	WaitedMillis int64  `json:"waited_ms"`
}

type nativeMDSCephFSRanksTakeover struct {
	State          string                        `json:"state"`
	Armed          nativeMDSCephFSRanksDaemon    `json:"armed"`
	Standby        nativeMDSCephFSRanksDaemon    `json:"standby"`
	Rank1          nativeMDSCephFSRanksDaemon    `json:"rank1"`
	Others         []nativeMDSCephFSRanksDaemon  `json:"other_ranks"`
	Follow         bool                          `json:"follow"`
	ArmedAddresses []string                      `json:"armed_blocklist_addresses"`
	Output         string                        `json:"native_output"`
	Error          string                        `json:"error"`
	FireStartedUTC string                        `json:"fire_started_utc"`
	FiredUTC       string                        `json:"fired_utc"`
	Adopted        *nativeMDSCephFSRanksTopology `json:"adopted"`
	AdoptedUTC     string                        `json:"adopted_utc"`
	AdoptPolls     int                           `json:"adopt_polls"`
	OldFencing     []nativeMDSSessionsBlock      `json:"old_incarnation_blocklist"`
}

type nativeMDSCephFSRanksStatus struct {
	Topology       nativeMDSCephFSRanksTopology `json:"topology"`
	ArmedPresent   bool                         `json:"armed_gid_present"`
	ArmedBlocklist []nativeMDSSessionsBlock     `json:"armed_blocklist"`
}

type nativeMDSCephFSRanksNative struct {
	Action         string                 `json:"action"`
	Path           string                 `json:"path"`
	NativeGlobalID uint64                 `json:"native_global_id"`
	Stat           *nativeMDSCephFSStat   `json:"stat"`
	Pin            *string                `json:"pin"`
	PinSet         *string                `json:"pin_set"`
	Entries        []nativeMDSCephFSEntry `json:"entries"`
}

type nativeMDSCephFSRanksReplayEnable struct {
	State        string                        `json:"state"`
	Standby      nativeMDSCephFSRanksDaemon    `json:"standby"`
	Output       string                        `json:"native_output"`
	Error        string                        `json:"error"`
	StartedUTC   string                        `json:"started_utc"`
	AppliedUTC   string                        `json:"applied_utc"`
	FollowingUTC string                        `json:"following_utc"`
	Polls        int                           `json:"polls"`
	WaitedMillis int64                         `json:"waited_ms"`
	Follower     *nativeMDSCephFSRanksDaemon   `json:"follower"`
	Topology     *nativeMDSCephFSRanksTopology `json:"topology"`
}

type nativeMDSCephFSRanksRuntime struct {
	Kind            string `json:"kind"`
	Name            string `json:"name"`
	Value           string `json:"value"`
	Baseline        string `json:"baseline"`
	Daemon          string `json:"daemon"`
	GID             uint64 `json:"gid"`
	State           string `json:"state"`
	Readback        string `json:"readback"`
	RestoreReadback string `json:"restore_readback"`
	Error           string `json:"error"`
}

type nativeMDSCephFSRanksHistoryCoverage struct {
	Size, Duration uint64
	TotalOps       int
}

type nativeMDSCephFSRanksReceipt struct {
	Source          string                               `json:"source_commit"`
	Token           string                               `json:"token"`
	RequestID       int64                                `json:"request_id"`
	Action          string                               `json:"action"`
	Target          string                               `json:"target"`
	Value           string                               `json:"value"`
	GlobalID        uint64                               `json:"global_id"`
	FSID            string                               `json:"fsid"`
	FSCID           int32                                `json:"fscid"`
	FSName          string                               `json:"fs_name"`
	Topology        *nativeMDSCephFSRanksTopology        `json:"topology"`
	IdentityChecked bool                                 `json:"identity_checked"`
	Views           []nativeMDSCephFSRanksView           `json:"views"`
	Blocklist       []nativeMDSSessionsBlock             `json:"blocklist"`
	Native          *nativeMDSCephFSRanksNative          `json:"native"`
	Processes       json.RawMessage                      `json:"native_processes,omitempty"`
	Pin             *nativeMDSCephFSRanksPin             `json:"pin"`
	Placement       *nativeMDSCephFSRanksPlacement       `json:"placement"`
	Takeover        *nativeMDSCephFSRanksTakeover        `json:"takeover"`
	Status          *nativeMDSCephFSRanksStatus          `json:"status"`
	ReplayEnable    *nativeMDSCephFSRanksReplayEnable    `json:"replay_enable"`
	Runtime         *nativeMDSCephFSRanksRuntime         `json:"runtime"`
	HistoricOps     []nativeMDSCephFSHistoricOp          `json:"historic_ops"`
	InFlightOps     []nativeMDSCephFSHistoricOp          `json:"ops_in_flight"`
	OpsDaemon       *nativeMDSCephFSRanksDaemon          `json:"ops_daemon"`
	HistoryCoverage *nativeMDSCephFSRanksHistoryCoverage `json:"history_coverage"`
	Complete        bool                                 `json:"complete"`
	Error           string                               `json:"error"`
	ElapsedMillis   int64                                `json:"elapsed_ms"`
	ObservedUTC     string                               `json:"observed_utc"`
}

func (r nativeMDSCephFSRanksReceipt) view(rank int32) *nativeMDSCephFSRanksView {
	for i := range r.Views {
		if r.Views[i].Rank == rank {
			return &r.Views[i]
		}
	}
	return nil
}

type nativeMDSCephFSRanksOracle struct {
	Source            string                       `json:"source_commit"`
	Token             string                       `json:"token"`
	FSID              string                       `json:"fsid"`
	FSCID             int32                        `json:"fscid"`
	FSName            string                       `json:"fs_name"`
	ClientIdentity    string                       `json:"client_identity"`
	NativeIdentity    string                       `json:"native_identity"`
	ClientCaps        map[string]string            `json:"client_caps"`
	NativeCaps        map[string]string            `json:"native_caps"`
	Plan              nativeMDSCephFSRanksPlan     `json:"plan"`
	BaselineBlocklist []nativeMDSSessionsBlock     `json:"baseline_blocklist"`
	Topology          nativeMDSCephFSRanksTopology `json:"topology"`
	Shape             nativeMDSCephFSRanksShape    `json:"shape"`
	Extension         bool                         `json:"standby_replay_extension"`
}

// mdsCephFSRanksControl drives the closed fixture control of this test. One
// request is outstanding at a time; the fixture slot is single-entry.
type mdsCephFSRanksControl struct {
	mu       sync.Mutex
	dir      string
	oracle   nativeMDSCephFSRanksOracle
	topology nativeMDSCephFSRanksTopology
	shape    nativeMDSCephFSRanksShape
	last     int64
	goGID    uint64
	receipts []nativeMDSCephFSRanksReceipt
}

func (x *mdsCephFSRanksControl) request(ctx context.Context, action string, args ...string) (nativeMDSCephFSRanksReceipt, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	var result nativeMDSCephFSRanksReceipt
	path := filepath.Join(x.dir, "mds-cephfs-ranks-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.RequestControl(ctx, x.dir, "mds-cephfs-ranks", "mds-cephfs-ranks-completed", append([]string{x.oracle.Token, action}, args...)...); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	x.receipts = append(x.receipts, result)
	o := x.oracle
	if result.Source != mdsFreshSource || result.Token != o.Token || result.RequestID <= x.last || result.Action != action || result.FSID != o.FSID || result.FSCID != o.FSCID ||
		result.FSName != o.FSName || !result.Complete || result.Error != "" {
		return result, fmt.Errorf("native MDS CephFS ranks receipt scope differs: %s", result.Error)
	}
	x.last = result.RequestID
	switch action {
	case "takeover-fire", "takeover-status":
		if result.Takeover == nil || result.Takeover.Armed.GID != x.topology.Ranks[0].GID || action == "takeover-status" && result.Status == nil {
			return result, errors.New("native MDS CephFS ranks takeover receipt is incomplete")
		}
		return result, nil
	case "takeover-adopt":
		r := result.Takeover
		if r == nil || r.State != "adopted" || r.Adopted == nil || result.Topology == nil || !r.Adopted.sameAs(*result.Topology) || !r.Adopted.stableAs(x.shape) {
			return result, errors.New("native MDS CephFS ranks takeover adoption receipt differs")
		}
		old, adopted := x.topology, *r.Adopted
		oldSpare, newSpare := old.spare(), adopted.spare()
		others := len(adopted.Ranks) == len(old.Ranks)
		for rank := 1; others && rank < len(old.Ranks); rank++ {
			others = adopted.Ranks[rank].same(old.Ranks[rank])
		}
		if adopted.Ranks[0].Name != oldSpare.Name || adopted.Ranks[0].GID != oldSpare.GID || adopted.Ranks[0].Incarnation == old.Ranks[0].Incarnation ||
			!others || newSpare.Name != old.Ranks[0].Name || newSpare.GID == old.Ranks[0].GID {
			return result, errors.New("adopted topology is not the spare's takeover of rank 0")
		}
		x.topology = adopted
	case "replay-enable":
		r := result.ReplayEnable
		follow := nativeMDSCephFSRanksShape{Ranks: x.shape.Ranks, Follow: true}
		old := x.topology.spare()
		if r == nil || r.State != "following" || r.Follower == nil || r.Topology == nil || result.Topology == nil || !r.Topology.stableAs(follow) || !result.Topology.sameAs(*r.Topology) ||
			old == nil || r.Follower.Name != old.Name || r.Follower.GID != old.GID || r.Follower.Rank != 0 || r.Follower.State != "up:standby-replay" {
			return result, errors.New("native MDS CephFS ranks standby-replay receipt differs")
		}
		for rank := range x.topology.Ranks {
			if !r.Topology.Ranks[rank].same(x.topology.Ranks[rank]) {
				return result, errors.New("standby replay changed an active rank")
			}
		}
		x.shape, x.topology = follow, *r.Topology
	}
	if !result.IdentityChecked || result.Topology == nil || !result.Topology.sameAs(x.topology) || !result.Topology.stableAs(x.shape) {
		return result, errors.New("native MDS CephFS ranks receipt lacks its identity proof or names other daemons")
	}
	switch action {
	case "observe":
		if strconv.FormatUint(result.GlobalID, 10) != args[0] || len(result.Views) != x.shape.Ranks {
			return result, errors.New("native MDS CephFS ranks observation scope differs")
		}
		// The fixture proves each daemon's applied epoch after the views; a
		// view may predate a later map epoch.
		for rank, v := range result.Views {
			d := result.Topology.Ranks[rank]
			if v.Rank != int32(rank) || v.Name != d.Name || v.GID != d.GID || v.Sessions == nil || v.DaemonEpoch == 0 {
				return result, errors.New("native MDS CephFS ranks view names another daemon")
			}
		}
	case "stat":
		if result.Native == nil || result.Native.Stat == nil || result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
			return result, errors.New("native CephFS ranks stat missing or bound to the Go client")
		}
	case "pin", "pin-restore":
		p := result.Pin
		if p == nil || p.Target != args[0] || (p.State != "applied" && p.State != "unchanged") || p.Readback != p.Value || action == "pin" && p.Value != args[1] {
			return result, errors.New("native MDS CephFS ranks pin receipt differs")
		}
		if p.State == "applied" && (p.SetterGlobalID == 0 || p.ReaderGlobalID == 0 || p.SetterGlobalID == p.ReaderGlobalID) {
			return result, errors.New("native MDS CephFS ranks pin readback is not independent of the setting mount")
		}
	case "placement":
		if result.Placement == nil || result.Placement.Target != args[0] || strconv.Itoa(int(result.Placement.Rank)) != args[1] || len(result.Views) != x.shape.Ranks {
			return result, errors.New("native MDS CephFS ranks placement receipt differs")
		}
	case "takeover-arm":
		r := result.Takeover
		if r == nil || r.State != "armed" || !r.Armed.same(x.topology.Ranks[0]) || !r.Rank1.same(x.topology.Ranks[1]) || r.Standby.GID != x.topology.spare().GID || r.Follow != x.shape.Follow ||
			len(r.ArmedAddresses) == 0 {
			return result, errors.New("native MDS CephFS ranks takeover arm receipt differs")
		}
	case "list":
		if result.Native == nil || result.Native.Entries == nil || result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
			return result, errors.New("native CephFS ranks listing missing or bound to the Go client")
		}
	case "ops", "ops-in-flight":
		rank, _ := strconv.Atoi(args[0])
		ops := result.HistoricOps
		if action == "ops-in-flight" {
			ops = result.InFlightOps
		}
		if result.OpsDaemon == nil || !result.OpsDaemon.same(x.topology.Ranks[rank]) || strconv.FormatUint(result.GlobalID, 10) != args[1] || ops == nil {
			return result, errors.New("native MDS CephFS ranks op tracker receipt differs")
		}
		if action == "ops" {
			h := result.HistoryCoverage
			if h == nil || h.Size != 256 || h.Duration != 600 || h.TotalOps < len(ops) || h.TotalOps >= int(h.Size) {
				return result, errors.New("native MDS CephFS ranks history coverage differs")
			}
		}
	case "log-pause", "log-resume":
		r, d := result.Runtime, x.topology.Ranks[0]
		want := map[string]string{"log-pause": "applied", "log-resume": "restored"}[action]
		if r == nil || r.Kind != "log-pause" || r.State != want || r.Daemon != d.Name || r.GID != d.GID || action == "log-pause" && r.Readback != "true" ||
			action == "log-resume" && r.RestoreReadback != r.Baseline {
			return result, errors.New("native MDS CephFS ranks journal pause receipt differs")
		}
	}
	return result, nil
}

// ranksRequestEvidence is one Go request and, after a forward, its
// resubmission to the destination rank.
type ranksRequestEvidence struct {
	Name               string
	Rank               int32
	Op                 uint32
	Transaction        uint64
	Forward            uint32
	MDSMapEpoch        uint32
	ForwardedTo        int32  `json:",omitempty"`
	ForwardedCount     int32  `json:",omitempty"`
	MustResend         bool   `json:",omitempty"`
	ForwardedMicros    int64  `json:",omitempty"`
	ResubmittedAs      uint64 `json:",omitempty"`
	FirstSafe          bool
	FirstResult        int32
	FirstArrival       uint64
	FirstMicros        int64
	FlushRequested     bool
	SafeResult         int32
	SafeArrival        uint64
	SafeMicros         int64
	IsDentry, IsTarget bool
	DentryName         string `json:",omitempty"`
	TargetIno          uint64 `json:",omitempty"`
	TargetCapID        uint64 `json:",omitempty"`
	TargetCaps         string `json:",omitempty"`
	PushBefore         uint64
	PushAfter          uint64
	Error              string `json:",omitempty"`
}

// ranksMigrationEvidence is one subtree migration seen by both sides.
type ranksMigrationEvidence struct {
	Target                        string
	FromRank, ToRank              int32
	Pin                           *nativeMDSCephFSRanksPin
	Placement                     *nativeMDSCephFSRanksPlacement
	CountersBefore, CountersAfter [2]nativeMDSCephFSRanksCounters
	NumCapsBefore, NumCapsAfter   [2]int64
	SubtreesAfter                 [2][]nativeMDSCephFSRanksSubtree
	PushBefore, PushAfter         [2]uint64
	Exports, Imports              []cephfsMigrationEvent
	Flushes                       []cephfsFlushEvent
	MigratedInodes                []uint64
	// HandledFirst names, per migrated inode, whether the agents handled the
	// EXPORT or the IMPORT first (they arrive on two connections).
	HandledFirst                    map[uint64]string
	AgentCapsBefore, AgentCapsAfter map[string][]cephfsCap
}

type ranksTakeoverEvidence struct {
	Armed, Standby, Rank1    nativeMDSCephFSRanksDaemon
	FireStarted, FireAcked   time.Time
	LostAt                   time.Time
	LostAfterFireMillis      int64
	ResumeError              recoveryErrorChain
	ResumesBeforeLost        uint64
	LostGeneration           uint64
	LostPushSequence         uint64
	Timeline                 []ranksStatusSample
	ReconnectState           string
	ReconnectTarget          cephmsgr.MDSTarget
	ReconnectCaps            []cephmsgr.MDSCapReconnect
	ReconnectRealms          []cephmsgr.MDSRealmReconnect
	ReconnectStarted         time.Time
	ReconnectAfterFireMillis int64
	ReconnectMicros          int64
	ReconnectError           recoveryErrorChain
	Reopened                 *mdsSessionEvidence
	ReconnectedFrom          uint64
	ActiveAfterFireMillis    int64
	Adopted                  *nativeMDSCephFSRanksTopology
	OldFencing               []nativeMDSSessionsBlock
	Rank0Before, Rank0After  *nativeMDSSession
	Rank1Before, Rank1After  *nativeMDSSession
	Rank1Status              [2]cephmsgr.MDSSessionStatus
	Rank1Resumes             [2]uint64
	Rank1PushDuring          uint64
	PostReconnect            *ranksRequestEvidence
	MDSDials                 []recoveryDial
}

type ranksStatusSample struct {
	AfterFireMillis int64
	FSMapEpoch      uint32
	Rank0, Rank1    *nativeMDSCephFSRanksDaemon
	Standbys        []nativeMDSCephFSRanksDaemon
	Failed          []int
	ArmedPresent    bool
	ArmedBlocklist  []nativeMDSSessionsBlock
}

func TestCephMDSCephFSRanksIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1 for owned multi-rank CephFS acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" ||
		os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("multi-rank CephFS acceptance requires its owned legacy/no-HASHPSPOOL two-rank fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSRanksOracle
	var fresh nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-ranks-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_RANKS_KEY_FILE")
	plan, topo := oracle.Plan, oracle.Topology
	_, tokenErr := hex.DecodeString(oracle.Token)
	fsName := fresh.FSName
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || identity != "client.mds-cephfs-ranks-"+oracle.Token || oracle.ClientIdentity != identity ||
		oracle.NativeIdentity != "client.mds-cephfs-ranks-native-"+oracle.Token || oracle.ClientCaps["mon"] != "allow r" || oracle.ClientCaps["mds"] != "allow rw fsname="+fsName ||
		oracle.ClientCaps["osd"] != "" || oracle.NativeCaps["mds"] != "allow rwp fsname="+fsName || oracle.NativeCaps["osd"] != "" ||
		plan.Directory != "ceph-msgr-ranks-"+oracle.Token || plan.Forward != "fwd" || plan.ForwardFile != "target" || plan.Migrate != "mig" ||
		!slices.Equal(plan.MigrateFiles, []string{"file-0", "file-1", "file-2"}) || !topo.stable() ||
		topo.Ranks[0].Name != fresh.MDS.Name || topo.Ranks[0].GID != fresh.MDS.GID || topo.Ranks[0].Endpoint != fresh.MDS.Endpoint || topo.Ranks[0].Nonce != fresh.MDS.Nonce {
		t.Fatal("owned multi-rank CephFS oracle, plan, identity or topology differ")
	}
	control := &mdsCephFSRanksControl{dir: dir, oracle: oracle, topology: topo, shape: nativeMDSCephFSRanksShape{Ranks: 2}}
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-ranks", "root": "/"}
	options := cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024}
	dirPath := "/" + plan.Directory

	var (
		requests   []*ranksRequestEvidence
		migrations []*ranksMigrationEvidence
		takeover   ranksTakeoverEvidence
		opens      [2]*mdsSessionEvidence
		conns      [2]*cephmsgr.MDSConnection
		sessions   [2]*cephmsgr.MDSSession
		agents     = newCephFSRankedAgents()
		lostAgents []*cephfsCapAgent
		dials      = newRecoveryDials(false, topo.Ranks[0].Endpoint, topo.Ranks[1].Endpoint, topo.Standbys[0].Endpoint)
		facts      = map[string]any{}
		pushes     = map[string][2]uint64{}
		caseTimes  = map[string]int64{}
		realms     = map[uint64]cephmsgr.MDSSnapRealm{}
		started    = time.Now()
	)
	agent := func(rank int32) *cephfsCapAgent {
		agents.mu.Lock()
		defer agents.mu.Unlock()
		return agents.agents[rank]
	}
	snapshots := func() map[string]cephfsAgentSnapshot {
		out := map[string]cephfsAgentSnapshot{}
		for _, rank := range []int32{0, 1} {
			if a := agent(rank); a != nil {
				out[fmt.Sprintf("rank%d", rank)] = a.snapshot()
			}
		}
		for i, a := range lostAgents {
			out[fmt.Sprintf("lost%d", i)] = a.snapshot()
		}
		return out
	}
	t.Cleanup(func() {
		data, err := json.Marshal(struct {
			Source        string                         `json:"source_commit"`
			Oracle        nativeMDSCephFSRanksOracle     `json:"oracle"`
			Opens         [2]*mdsSessionEvidence         `json:"open"`
			Requests      []*ranksRequestEvidence        `json:"requests"`
			Migrations    []*ranksMigrationEvidence      `json:"migrations"`
			Takeover      ranksTakeoverEvidence          `json:"takeover"`
			Agents        map[string]cephfsAgentSnapshot `json:"capability_agents"`
			Facts         map[string]any                 `json:"facts"`
			PushSequence  map[string][2]uint64           `json:"push_sequence_after_leaf"`
			Native        []nativeMDSCephFSRanksReceipt  `json:"native"`
			Dials         []recoveryDial                 `json:"mds_dials"`
			OSDDials      uint32                         `json:"osd_dials"`
			CaseMillis    map[string]int64               `json:"case_ms"`
			ElapsedMillis int64                          `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, opens, requests, migrations, takeover, snapshots(), facts, pushes, control.receipts, dials.attempts(), dials.OSD.Load(), caseTimes,
			time.Since(started).Milliseconds()})
		if err == nil && len(data) > 12<<20 {
			err = errors.New("MDS CephFS ranks evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-ranks-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS ranks public evidence", err)
		}
	})
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		var seq [2]uint64
		for rank, s := range sessions {
			if s != nil {
				seq[rank] = s.Status().PushSequence
			}
		}
		pushes[name] = seq
		for label, s := range snapshots() {
			if len(s.Errors) != 0 {
				t.Errorf("capability agent %s errors after %s: %v", label, name, s.Errors)
				return false
			}
		}
		return ok
	}
	var epoch uint32
	observe := func(t *testing.T) nativeMDSCephFSRanksReceipt {
		t.Helper()
		r, err := control.request(ctx, "observe", strconv.FormatUint(control.goGID, 10))
		if err != nil {
			t.Fatal("native observation", err)
		}
		epoch = r.Topology.MDSMapEpoch
		return r
	}
	native := func(t *testing.T, action string, args ...string) nativeMDSCephFSRanksReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("native", action, args, err)
		}
		if r.Topology != nil {
			epoch = r.Topology.MDSMapEpoch
		}
		return r
	}
	// send submits one request to a rank and applies its first reply to that
	// rank's agent. A forward is answered, when allowed, by a new request to
	// the destination rank with the forward count the MDS reported, as the
	// native client resends (Client::handle_client_request_forward).
	var send func(t *testing.T, rank int32, name string, request cephmsgr.MDSRequest, forwards int) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace, *ranksRequestEvidence)
	send = func(t *testing.T, rank int32, name string, request cephmsgr.MDSRequest, forwards int) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace, *ranksRequestEvidence) {
		t.Helper()
		s := sessions[rank]
		request.MDSMapEpoch = epoch
		e := &ranksRequestEvidence{Name: name, Rank: rank, Op: request.Op, Forward: request.Forward, MDSMapEpoch: epoch, PushBefore: s.Status().PushSequence}
		requests = append(requests, e)
		begin := time.Now()
		call, err := s.Submit(ctx, request)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("submit", name, err)
		}
		e.Transaction = call.Transaction()
		waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		first, err := call.Wait(waitCtx)
		var forwarded *cephmsgr.MDSRequestForwardedError
		if errors.As(err, &forwarded) {
			e.ForwardedTo, e.ForwardedCount, e.MustResend, e.ForwardedMicros = forwarded.DestinationMDS, forwarded.Forward, forwarded.MustResend, time.Since(begin).Microseconds()
			if forwarded.Transaction != call.Transaction() {
				t.Fatal("forward names another transaction", forwarded)
			}
			if forwards == 0 || forwarded.DestinationMDS < 0 || forwarded.DestinationMDS > 1 || forwarded.DestinationMDS == rank || forwarded.Forward <= int32(request.Forward) {
				e.Error = err.Error()
				t.Fatalf("unexpected forward of %s: %+v", name, forwarded)
			}
			request.Forward = uint32(forwarded.Forward)
			f, safe, trace, next := send(t, forwarded.DestinationMDS, name+" resubmitted", request, forwards-1)
			e.ResubmittedAs = next.Transaction
			return f, safe, trace, next
		}
		if err != nil {
			e.Error = err.Error()
			t.Fatal("first reply", name, err)
		}
		e.FirstMicros, e.FirstSafe, e.FirstResult, e.FirstArrival = time.Since(begin).Microseconds(), first.Safe, first.Result, first.Arrival
		e.IsDentry, e.IsTarget = first.IsDentry, first.IsTarget
		trace, _, err := agent(rank).applyReply(name, first)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode reply trace", name, err)
		}
		e.DentryName = trace.DentryName
		if trace.Target != nil {
			e.TargetIno, e.TargetCapID, e.TargetCaps = trace.Target.Ino, trace.Target.Cap.CapID, cephCapString(trace.Target.Cap.Caps)
		}
		snaps, err := first.DecodeSnapTrace()
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode snap trace", name, err)
		}
		if rank == 0 {
			for _, r := range snaps {
				realms[r.Ino] = r
			}
		}
		safe := first
		if !first.Safe {
			if err := s.FlushJournal(ctx); err != nil {
				t.Fatal("flush journal", name, err)
			}
			e.FlushRequested = true
			if safe, err = call.WaitSafe(waitCtx); err != nil {
				e.Error = err.Error()
				t.Fatal("safe reply", name, err)
			}
		}
		e.SafeMicros, e.SafeResult, e.SafeArrival, e.PushAfter = time.Since(begin).Microseconds(), safe.Result, safe.Arrival, s.Status().PushSequence
		if !safe.Safe || safe.Transaction != call.Transaction() || safe.Op != request.Op {
			t.Fatal("safe reply identity differs", name)
		}
		return first, safe, trace, e
	}
	create := func(t *testing.T, rank int32, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, e := send(t, rank, "create "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, 0)
		if first.Result != 0 || safe.Result != 0 || trace.Target == nil || len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != trace.Target.Ino || trace.DentryName != name {
			t.Fatalf("create %s differs: %+v", name, e)
		}
		ino := trace.Target.Ino
		// The created file is open read-write; close it as an application would.
		a := agent(rank)
		a.open(ino, cephFileModeRDWR)
		if _, _, err := a.close(ino, cephFileModeRDWR); err != nil {
			t.Fatal("close created file", name, err)
		}
		return ino
	}
	mkdir := func(t *testing.T, rank int32, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, e := send(t, rank, "mkdir "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755),
			Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, 0)
		if first.Result != 0 || safe.Result != 0 || trace.Target == nil || trace.DentryName != name {
			t.Fatalf("mkdir %s differs: %+v", name, e)
		}
		return trace.Target.Ino
	}
	numCaps := func(r nativeMDSCephFSRanksReceipt) [2]int64 {
		var out [2]int64
		for rank := range out {
			if v := r.view(int32(rank)); v != nil {
				if n := v.session(control.goGID); n != nil {
					out[rank] = n.Caps
				}
			}
		}
		return out
	}
	counters := func(r nativeMDSCephFSRanksReceipt) [2]nativeMDSCephFSRanksCounters {
		var out [2]nativeMDSCephFSRanksCounters
		for rank := range out {
			if v := r.view(int32(rank)); v != nil {
				out[rank] = v.Counters
			}
		}
		return out
	}
	agentCaps := func(inos ...uint64) map[string][]cephfsCap {
		out := map[string][]cephfsCap{}
		for _, rank := range []int32{0, 1} {
			for _, ino := range inos {
				if c, ok := agent(rank).cap(ino); ok {
					out[fmt.Sprintf("rank%d", rank)] = append(out[fmt.Sprintf("rank%d", rank)], c)
				}
			}
		}
		return out
	}
	// migrate pins one owned directory to the other rank and records what both
	// sessions and both daemons report until the agents hold the moved caps.
	migrate := func(t *testing.T, target string, from, to int32, inos []uint64) *ranksMigrationEvidence {
		t.Helper()
		e := &ranksMigrationEvidence{Target: target, FromRank: from, ToRank: to}
		migrations = append(migrations, e)
		before := observe(t)
		e.CountersBefore, e.NumCapsBefore = counters(before), numCaps(before)
		marks := [2]int{}
		flushMark := 0
		for _, rank := range []int32{0, 1} {
			e.PushBefore[rank] = sessions[rank].Status().PushSequence
			marks[rank] = len(agent(rank).snapshot().Migrations)
		}
		flushMark = len(agent(from).snapshot().Flushes)
		e.AgentCapsBefore = agentCaps(inos...)
		pin := native(t, "pin", target, strconv.Itoa(int(to)))
		e.Pin = pin.Pin
		placed := native(t, "placement", target, strconv.Itoa(int(to)))
		e.Placement = placed.Placement
		if !placed.Placement.Converged {
			t.Fatalf("subtree %s did not converge on rank %d: %+v", target, to, placed.Views)
		}
		path := dirPath + "/" + target
		for rank := range e.SubtreesAfter {
			e.SubtreesAfter[rank] = placed.view(int32(rank)).Subtrees
		}
		if s := placed.view(to).subtree(path); s == nil || !s.IsAuth || s.AuthFirst != int(to) || s.ExportPin != int(to) {
			t.Fatalf("rank %d does not report the pinned subtree %s: %+v", to, path, s)
		}
		// The agents run asynchronously; wait until both sides of every
		// migrated cap were handled.
		deadline := time.Now().Add(30 * time.Second)
		for {
			done := true
			for _, ino := range inos {
				c0, _ := agent(from).cap(ino)
				c1, _ := agent(to).cap(ino)
				done = done && c0.CapID == 0 && c0.Exported && c1.CapID != 0 && !c1.Exported && !c1.Released
			}
			s0, s1 := agent(from).snapshot(), agent(to).snapshot()
			done = done && s0.Pending == 0 && s1.Pending == 0
			if done || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		after := observe(t)
		e.CountersAfter, e.NumCapsAfter = counters(after), numCaps(after)
		for _, rank := range []int32{0, 1} {
			e.PushAfter[rank] = sessions[rank].Status().PushSequence
			for _, m := range agent(rank).snapshot().Migrations[marks[rank]:] {
				switch m.Op {
				case cephmsgr.MDSCapOpExport:
					e.Exports = append(e.Exports, m)
				case cephmsgr.MDSCapOpImport:
					e.Imports = append(e.Imports, m)
				}
			}
		}
		e.Flushes = agent(from).snapshot().Flushes[flushMark:]
		e.AgentCapsAfter = agentCaps(inos...)
		e.HandledFirst = map[uint64]string{}
		for _, m := range e.Imports {
			e.MigratedInodes = append(e.MigratedInodes, m.Ino)
		}
		for _, m := range e.Exports {
			if strings.Contains(m.Action, "cap") && strings.Contains(m.Action, "added for rank") {
				e.HandledFirst[m.Ino] = "export"
			} else {
				e.HandledFirst[m.Ino] = "import"
			}
		}
		slices.Sort(e.MigratedInodes)
		return e
	}
	// checkMigration requires one export and one import per moved cap, each
	// naming the other side's cap as its peer, the exporter's FLUSHMSG
	// (Migrator::export_frozen flushes the sessions holding caps in the
	// subtree) answered before any EXPORT and carrying the session's push
	// count, the native counters of one subtree migration, and native num_caps
	// moving between the ranks.
	checkMigration := func(t *testing.T, e *ranksMigrationEvidence, inos []uint64) {
		t.Helper()
		if len(e.Flushes) == 0 {
			t.Fatalf("no FLUSHMSG reached the exporting session during the %s migration", e.Target)
		}
		for _, f := range e.Flushes {
			if f.Error != "" || f.Acked.IsZero() || f.Sequence != f.AgentPushes {
				t.Fatalf("FLUSHMSG was not acknowledged with the counted push sequence: %+v", f)
			}
			for _, exp := range e.Exports {
				if exp.Arrival <= f.Arrival {
					t.Fatalf("EXPORT arrived before the exporter's FLUSHMSG: %+v %+v", exp, f)
				}
			}
		}
		want := slices.Sorted(slices.Values(inos))
		if !slices.Equal(e.MigratedInodes, want) || len(e.Exports) != len(want) || len(e.Imports) != len(want) {
			t.Fatalf("cap migration of %s differs: imports %v exports %d, want %v", e.Target, e.MigratedInodes, len(e.Exports), want)
		}
		for _, imp := range e.Imports {
			if imp.Rank != e.ToRank || imp.Peer.MDS != e.FromRank || imp.Peer.CapID == 0 || imp.CapID == 0 {
				t.Fatalf("IMPORT does not name the exporting rank's cap: %+v", imp)
			}
			i := slices.IndexFunc(e.Exports, func(x cephfsMigrationEvent) bool { return x.Ino == imp.Ino })
			if i < 0 {
				t.Fatalf("no EXPORT for imported inode %d", imp.Ino)
			}
			exp := e.Exports[i]
			if exp.Rank != e.FromRank || exp.Peer.MDS != e.ToRank || exp.Peer.CapID != imp.CapID || exp.CapID != imp.Peer.CapID {
				t.Fatalf("EXPORT and IMPORT peers do not name each other: %+v %+v", exp, imp)
			}
			moved, _ := agent(e.ToRank).cap(imp.Ino)
			if moved.CapID != imp.CapID || moved.Issued == 0 {
				t.Fatalf("agent does not hold the imported cap on rank %d: %+v", e.ToRank, moved)
			}
		}
		b, a := e.CountersBefore, e.CountersAfter
		if a[e.FromRank].Exported != b[e.FromRank].Exported+1 || a[e.ToRank].Imported != b[e.ToRank].Imported+1 ||
			a[e.FromRank].ExportedInodes-b[e.FromRank].ExportedInodes != a[e.ToRank].ImportedInodes-b[e.ToRank].ImportedInodes ||
			a[e.FromRank].ExportedInodes-b[e.FromRank].ExportedInodes < uint64(len(want)) {
			t.Fatalf("native migration counters differ: before %+v after %+v", b, a)
		}
		moved := int64(len(want))
		if e.NumCapsAfter[e.FromRank] != e.NumCapsBefore[e.FromRank]-moved || e.NumCapsAfter[e.ToRank] != e.NumCapsBefore[e.ToRank]+moved {
			t.Fatalf("native num_caps did not move %d caps from rank %d to rank %d: before %v after %v", moved, e.FromRank, e.ToRank, e.NumCapsBefore, e.NumCapsAfter)
		}
	}

	client := recoveryClient(t, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) {
		o.EnableMDS, o.EnableCephFS, o.EnableOSD, o.MaxMDSConnections = true, true, false, 2
	})
	snapshot := client.Snapshot()
	control.goGID = snapshot.GlobalID
	gid, cephfsNonce := snapshot.GlobalID, snapshot.CephFSNonce
	if gid == 0 || cephfsNonce == 0 {
		t.Fatal("CephFS client lacks a GlobalID or shared nonce", snapshot)
	}
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, s := range sessions {
			if s != nil {
				s.Close(closing)
			}
		}
	})
	var dirIno, fwdIno, targetIno, migIno uint64
	var migInos []uint64

	if !leaf("open_sessions_on_two_ranks", func(t *testing.T) {
		for _, rank := range []int32{0, 1} {
			target := topo.Ranks[rank].target(fresh.FSCID, rank)
			var err error
			if conns[rank], err = client.OpenMDS(ctx, target); err != nil {
				t.Fatal("open explicit MDS target of rank", rank, err)
			}
			if sessions[rank], err = conns[rank].OpenSession(ctx, options); err != nil {
				t.Fatal("open CephFS session on rank", rank, err)
			}
			agents.start(ctx, rank, sessions[rank])
			e := describeMDSSession(fmt.Sprintf("rank%d", rank), identity, sessions[rank])
			opens[rank] = &e
			status := sessions[rank].Status()
			if status.State != cephmsgr.MDSSessionOpen || status.GlobalID != gid || status.Target != target || status.PushSequence != 0 {
				t.Fatalf("rank %d session differs: %+v", rank, status)
			}
			for _, bit := range []uint{7, 9, mds.FeatureMultiReconnect, 18, 19, 20} {
				if !cephmsgr.MDSFeatureBitsetHas(status.Open.SupportedFeatures, bit) {
					t.Fatal("MDS OPEN lacks a feature this acceptance relies on", rank, bit)
				}
			}
		}
		if sessions[0].Status().Generation == sessions[1].Status().Generation {
			t.Fatal("the two rank sessions share a generation")
		}
		r := observe(t)
		addresses := map[string]bool{}
		for rank, v := range r.Views {
			n := v.session(gid)
			if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
				t.Fatal("rank", rank, err)
			}
			if n.Caps != 0 || n.Leases != 0 || !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(cephfsNonce), 10)) {
				t.Fatalf("rank %d native session holds caps or lacks the shared nonce: %+v", rank, n)
			}
			addresses[n.Address] = true
		}
		if len(addresses) != 1 {
			t.Fatal("the two ranks see different client address instances", addresses)
		}
		facts["client"] = map[string]any{"global_id": gid, "cephfs_nonce": cephfsNonce, "native_address": slices.Collect(maps.Keys(addresses))[0]}
		facts["startup_topology"] = r.Topology
	}) {
		return
	}

	if !leaf("forward_to_pinned_rank_and_resubmit", func(t *testing.T) {
		dirIno = mkdir(t, 0, 1, plan.Directory)
		fwdIno = mkdir(t, 0, dirIno, plan.Forward)
		targetIno = create(t, 0, fwdIno, plan.ForwardFile)
		e := migrate(t, plan.Forward, 0, 1, []uint64{targetIno})
		checkMigration(t, e, []uint64{targetIno})
		before := observe(t)
		// Rank 0 still holds fwd's inode but not its directory fragment, and a
		// getattr with CEPH_STAT_RSTAT needs the authority of the target.
		first, safe, trace, resubmitted := send(t, 0, "lookup "+plan.ForwardFile+" on rank 0", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsGetattrArgs(cephStatAll | cephStatRStat), Path: cephmsgr.MDSFilePath{Ino: fwdIno, Path: plan.ForwardFile}}, 1)
		original := requests[len(requests)-2]
		if original.Rank != 0 || original.ForwardedTo != 1 || original.ForwardedCount != 1 || !original.MustResend || original.ResubmittedAs != resubmitted.Transaction ||
			resubmitted.Rank != 1 || resubmitted.Forward != 1 || resubmitted.Transaction == original.Transaction {
			t.Fatalf("forward and resubmission differ: %+v %+v", original, resubmitted)
		}
		if first.Result != 0 || !first.Safe || !safe.Safe || trace.Target == nil || trace.Target.Ino != targetIno || trace.DentryName != plan.ForwardFile || trace.DirInode == nil || trace.DirInode.Ino != fwdIno {
			t.Fatalf("resubmitted lookup on rank 1 differs: %+v", resubmitted)
		}
		if sessions[0].Status().State != cephmsgr.MDSSessionOpen {
			t.Fatal("the forward ended the rank-0 session", sessions[0].Err())
		}
		after := observe(t)
		b, a := counters(before), counters(after)
		facts["forward_counters"] = map[string]any{"before": b, "after": a}
		if a[0].Forward != b[0].Forward+1 || a[1].Forward != b[1].Forward {
			t.Fatalf("native forward counters differ: before %+v after %+v", b, a)
		}
		native := native(t, "stat", plan.Forward+"/"+plan.ForwardFile).Native.Stat
		st := trace.Target
		if !native.Exists || native.Ino != st.Ino || native.Mode != st.Mode || native.UID != st.UID || native.GID != st.GID || native.NLink != nativeStatxNLink(st) || native.Size != st.Size {
			t.Fatalf("native stat differs from the rank-1 lookup: native=%+v go=%+v", native, summarizeInode(st))
		}
		facts["forward_lookup_target"] = summarizeInode(st)
	}) {
		return
	}

	if !leaf("cap_export_import_on_migration", func(t *testing.T) {
		migIno = mkdir(t, 0, dirIno, plan.Migrate)
		migInos = nil
		for _, name := range plan.MigrateFiles {
			migInos = append(migInos, create(t, 0, migIno, name))
		}
		// Make rank 0's authority explicit first: pin 0 creates its own
		// subtree there, so the native view proves where the caps start.
		native(t, "pin", plan.Migrate, "0")
		placed := native(t, "placement", plan.Migrate, "0")
		if !placed.Placement.Converged {
			t.Fatalf("subtree %s did not converge on rank 0: %+v", plan.Migrate, placed.Views)
		}
		facts["migrate_rank0_subtree"] = placed.view(0).subtree(dirPath + "/" + plan.Migrate)
		for _, ino := range migInos {
			if c, _ := agent(0).cap(ino); c.CapID == 0 || c.Issued == 0 {
				t.Fatalf("Go holds no rank-0 cap on %d before the migration: %+v", ino, c)
			}
		}
		e := migrate(t, plan.Migrate, 0, 1, migInos)
		checkMigration(t, e, migInos)
		// The rank-1 MDS recognizes Go's imported cap: a getattr there reports
		// the same cap ID.
		first, _, trace, re := send(t, 1, "getattr "+plan.MigrateFiles[0]+" on rank 1", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: migInos[0]}}, 0)
		imported := e.Imports[slices.IndexFunc(e.Imports, func(m cephfsMigrationEvent) bool { return m.Ino == migInos[0] })]
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != migInos[0] || trace.Target.Cap.CapID != imported.CapID {
			t.Fatalf("rank 1 does not report the imported cap: %+v imported %d", re, imported.CapID)
		}
	}) {
		return
	}

	if !leaf("standby_takeover_reconnect", func(t *testing.T) {
		before := observe(t)
		takeover.Rank0Before, takeover.Rank1Before = before.view(0).session(gid), before.view(1).session(gid)
		takeover.Rank1Status[0], takeover.Rank1Resumes[0] = sessions[1].Status(), conns[1].Resumes()
		armed := native(t, "takeover-arm").Takeover
		takeover.Armed, takeover.Standby, takeover.Rank1 = armed.Armed, armed.Standby, armed.Rank1
		lost := sessions[0]
		lostConn := conns[0]
		lostSeen := make(chan time.Time, 1)
		go func() {
			<-lost.Done()
			lostSeen <- time.Now()
		}()
		takeover.FireStarted = time.Now()
		afterFire := func(at time.Time) int64 { return at.Sub(takeover.FireStarted).Milliseconds() }
		if _, err := control.request(ctx, "takeover-fire"); err != nil {
			t.Fatal("native MDS fail", err)
		}
		takeover.FireAcked = time.Now()
		select {
		case takeover.LostAt = <-lostSeen:
		case <-time.After(45 * time.Second):
			t.Fatal("the rank-0 session did not end after its daemon failed")
		}
		status := lost.Status()
		takeover.LostAfterFireMillis, takeover.ResumeError, takeover.ResumesBeforeLost = afterFire(takeover.LostAt), describeRecoveryError(lost.Err()), lostConn.Resumes()
		takeover.LostGeneration, takeover.LostPushSequence = status.Generation, status.PushSequence
		if status.State != cephmsgr.MDSSessionLost {
			t.Fatalf("rank-0 session ended %d, not Lost: %v", status.State, lost.Err())
		}
		previous := agent(0)
		select {
		case <-previous.done:
		case <-time.After(10 * time.Second):
			t.Fatal("rank-0 agent did not observe the lost session")
		}
		lostAgents = append(lostAgents, previous)
		// Native: wait for the standby in rank 0's reconnect phase.
		var newRank0 *nativeMDSCephFSRanksDaemon
		deadline := time.Now().Add(60 * time.Second)
		for newRank0 == nil {
			r, err := control.request(ctx, "takeover-status")
			if err != nil {
				t.Fatal(err)
			}
			s := r.Status
			sample := ranksStatusSample{AfterFireMillis: time.Since(takeover.FireStarted).Milliseconds(), FSMapEpoch: s.Topology.FSMapEpoch, Standbys: s.Topology.Standbys,
				Failed: s.Topology.Failed, ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist}
			if len(s.Topology.Ranks) == 2 {
				sample.Rank0, sample.Rank1 = s.Topology.Ranks[0], s.Topology.Ranks[1]
			}
			takeover.Timeline = append(takeover.Timeline, sample)
			if r0 := sample.Rank0; r0 != nil && r0.GID == armed.Standby.GID && r0.Name == armed.Standby.Name && r0.State == "up:reconnect" {
				newRank0 = r0
				break
			}
			if r0 := sample.Rank0; r0 != nil && r0.GID == armed.Standby.GID && r0.State != "up:replay" && r0.State != "up:resolve" && r0.State != "up:reconnect" {
				t.Fatal("the standby left rank 0's reconnect phase before the Go client reconnected", r0)
			}
			if time.Now().After(deadline) {
				t.Fatal("the standby did not reach rank 0's reconnect phase", s)
			}
			time.Sleep(100 * time.Millisecond)
		}
		takeover.ReconnectState = newRank0.State
		if newRank0.Endpoint != armed.Standby.Endpoint || newRank0.Nonce != armed.Standby.Nonce || newRank0.Incarnation == armed.Armed.Incarnation {
			t.Fatal("rank 0 is not the standby daemon at its registered address in a new incarnation", newRank0, armed.Standby)
		}
		dials.allowMDS(newRank0.Endpoint)
		takeover.ReconnectTarget = newRank0.target(fresh.FSCID, 0)
		if err := lostConn.Close(); err != nil {
			t.Fatal(err)
		}
		previous.mu.Lock()
		maps.Copy(realms, previous.realms)
		previous.mu.Unlock()
		caps, realmState, err := previous.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		takeover.ReconnectCaps, takeover.ReconnectRealms = caps, realmState
		if len(caps) == 0 {
			t.Fatal("the Go client holds no rank-0 caps to reconnect")
		}
		takeover.ReconnectStarted = time.Now()
		takeover.ReconnectAfterFireMillis = afterFire(takeover.ReconnectStarted)
		conn, err := client.OpenMDS(ctx, takeover.ReconnectTarget)
		if err != nil {
			t.Fatal("open the new rank-0 daemon", err)
		}
		conns[0] = conn
		reconnectCtx, stopReconnect := context.WithTimeout(ctx, 30*time.Second)
		session, calls, err := conn.ReconnectSession(reconnectCtx, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState}, options)
		stopReconnect()
		takeover.ReconnectMicros, takeover.ReconnectError = time.Since(takeover.ReconnectStarted).Microseconds(), describeRecoveryError(err)
		if err != nil {
			t.Fatal("CephFS reconnect to the new rank-0 daemon", err)
		}
		sessions[0] = session
		agents.startFrom(ctx, 0, session, previous)
		reopened := describeMDSSession("rank0-reconnected", identity, session)
		takeover.Reopened = &reopened
		st := session.Status()
		takeover.ReconnectedFrom = st.ReconnectedFrom
		if st.State != cephmsgr.MDSSessionOpen || st.ReconnectedFrom != takeover.LostGeneration || st.GlobalID != gid || st.Target != takeover.ReconnectTarget || len(calls) != 0 {
			t.Fatalf("reconnected rank-0 session differs: %+v", st)
		}
		deadline = time.Now().Add(60 * time.Second)
		for {
			r, err := control.request(ctx, "takeover-status")
			if err != nil {
				t.Fatal(err)
			}
			s := r.Status
			sample := ranksStatusSample{AfterFireMillis: time.Since(takeover.FireStarted).Milliseconds(), FSMapEpoch: s.Topology.FSMapEpoch, Standbys: s.Topology.Standbys,
				Failed: s.Topology.Failed, ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist}
			if len(s.Topology.Ranks) == 2 {
				sample.Rank0, sample.Rank1 = s.Topology.Ranks[0], s.Topology.Ranks[1]
			}
			takeover.Timeline = append(takeover.Timeline, sample)
			if r0 := sample.Rank0; r0 != nil && r0.GID == newRank0.GID && r0.State == "up:active" && len(s.Topology.Standbys) == 1 {
				takeover.ActiveAfterFireMillis = afterFire(time.Now())
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the new rank-0 daemon did not become active with a standby", s)
			}
			time.Sleep(250 * time.Millisecond)
		}
		adopted, err := control.request(ctx, "takeover-adopt")
		if err != nil {
			t.Fatal(err)
		}
		takeover.Adopted, takeover.OldFencing = adopted.Takeover.Adopted, adopted.Takeover.OldFencing
		for _, address := range adopted.Takeover.ArmedAddresses {
			if !slices.ContainsFunc(adopted.Takeover.OldFencing, func(b nativeMDSSessionsBlock) bool { return b.Address == address && !b.Range }) {
				t.Fatal("the failed rank-0 incarnation's address is not blocklisted", address, adopted.Takeover.OldFencing)
			}
		}
		after := observe(t)
		takeover.Rank0After, takeover.Rank1After = after.view(0).session(gid), after.view(1).session(gid)
		if err := checkNativeCephFSSession(takeover.Rank0After, identity, metadata); err != nil {
			t.Fatal("the reconnected session is not open on the new rank-0 daemon", err)
		}
		if takeover.Rank0After.Caps != int64(len(caps)) || takeover.Rank0After.Address != takeover.Rank0Before.Address {
			t.Fatalf("new rank-0 daemon holds %d caps at %s, the reconnect sent %d from %s", takeover.Rank0After.Caps, takeover.Rank0After.Address, len(caps), takeover.Rank0Before.Address)
		}
		if err := checkNativeCephFSSession(takeover.Rank1After, identity, metadata); err != nil {
			t.Fatal("the rank-1 session changed natively", err)
		}
		takeover.Rank1Status[1], takeover.Rank1Resumes[1] = sessions[1].Status(), conns[1].Resumes()
		takeover.Rank1PushDuring = takeover.Rank1Status[1].PushSequence - takeover.Rank1Status[0].PushSequence
		if takeover.Rank1After.Caps != takeover.Rank1Before.Caps || takeover.Rank1Status[1].State != cephmsgr.MDSSessionOpen ||
			takeover.Rank1Status[1].Generation != takeover.Rank1Status[0].Generation || takeover.Rank1Resumes[1] != takeover.Rank1Resumes[0] {
			t.Fatalf("the rank-1 session was affected by the rank-0 takeover: %+v %+v", takeover.Rank1Status, takeover.Rank1After)
		}
		// The reconnected session serves requests on the new daemon.
		first, _, trace, re := send(t, 0, "getattr "+plan.Directory+" after takeover", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: dirIno}}, 0)
		takeover.PostReconnect = re
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != dirIno {
			t.Fatalf("getattr on the new rank-0 daemon differs: %+v", re)
		}
		takeover.MDSDials = dials.attempts()
	}) {
		return
	}

	// An MDS command goes to the daemon its explicit target names. The ranks
	// identity's MDS grant is not admin, so a test-only identity with an MDS
	// admin grant (created and removed through the fixture admin's MON auth
	// commands) sends "status" to rank 1 and to the daemon that took over
	// rank 0; each answers as that rank with its own GID.
	if !leaf("commands_follow_rank_targets", func(t *testing.T) {
		monAdmin, err := cephmsgr.Dial(ctx, integrationOptions(t))
		if err != nil {
			t.Fatal(err)
		}
		defer monAdmin.Close()
		const entity = "client.ceph-msgr-ranks-commands"
		mon := func(call context.Context, fields map[string]any) cephmsgr.Result {
			encoded, _ := json.Marshal(fields)
			result, err := monAdmin.MonCommand(call, cephmsgr.Command{JSON: encoded})
			if err != nil {
				t.Fatal(fields["prefix"], err)
			}
			return result
		}
		created := mon(ctx, map[string]any{"prefix": "auth get-or-create", "entity": entity, "format": "json", "caps": []string{"mon", "allow r", "mds", "allow *"}})
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			mon(cleanup, map[string]any{"prefix": "auth rm", "entity": entity})
		}()
		var keys []struct{ Key string }
		if err := json.Unmarshal(created.Data, &keys); err != nil || len(keys) != 1 {
			t.Fatal("auth get-or-create output", err)
		}
		options := integrationOptions(t)
		if options.Key, err = cephmsgr.ParseKey(keys[0].Key); err != nil {
			t.Fatal(err)
		}
		options.Identity = entity
		options.EnableMDS, options.EnableOSD, options.EnableMDSMaps, options.MaxMDSConnections = true, false, false, 2
		admin, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()
		for _, tc := range []struct {
			rank   int32
			target cephmsgr.MDSTarget
			gid    uint64
		}{{0, takeover.ReconnectTarget, takeover.Standby.GID}, {1, topo.Ranks[1].target(fresh.FSCID, 1), topo.Ranks[1].GID}} {
			conn, err := admin.OpenMDS(ctx, tc.target)
			if err != nil {
				t.Fatal("open the rank target for a command", tc.rank, err)
			}
			result, err := conn.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
			var status struct {
				Whoami int32  `json:"whoami"`
				ID     uint64 `json:"id"`
				State  string `json:"state"`
			}
			if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &status) != nil || status.Whoami != tc.rank || status.ID != tc.gid || status.State != "up:active" {
				t.Fatalf("rank %d command answered as %+v (want GID %d): %v", tc.rank, status, tc.gid, err)
			}
		}
	}) {
		return
	}

	t.Run("cleanup_restore_pins_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_restore_pins_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		for _, target := range []string{plan.Forward, plan.Migrate} {
			r := native(t, "pin-restore", target)
			facts["pin_restore_"+target] = r.Pin
			if r.Pin.Value != "-1" {
				t.Fatal("pin baseline of", target, "is not -1:", r.Pin)
			}
		}
		unlink := func(rank int32, parent uint64, name string) {
			first, safe, _, e := send(t, rank, "unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, 2)
			if first.Result != 0 || safe.Result != 0 {
				t.Fatalf("unlink %s differs: %+v", name, e)
			}
		}
		rmdir := func(rank int32, parent uint64, name string) {
			first, safe, _, e := send(t, rank, "rmdir "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, 2)
			if first.Result != 0 || safe.Result != 0 {
				t.Fatalf("rmdir %s differs: %+v", name, e)
			}
		}
		// Files live in fragments rank 1 now holds; the directories' dentries in
		// the owned directory, which rank 0 holds.
		unlink(1, fwdIno, plan.ForwardFile)
		for _, name := range plan.MigrateFiles {
			unlink(1, migIno, name)
		}
		rmdir(0, dirIno, plan.Forward)
		rmdir(0, dirIno, plan.Migrate)
		rmdir(0, 1, plan.Directory)
		st := native(t, "stat", "dir").Native.Stat
		if st.Exists || st.Errno != 2 {
			t.Fatal("native stat still finds the owned directory", st)
		}
		before := numCaps(observe(t))
		for _, rank := range []int32{0, 1} {
			items, barrier := agent(rank).releaseAll()
			releaseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			err := sessions[rank].ReleaseCaps(releaseCtx, items, barrier)
			stop()
			if err != nil {
				t.Fatal("release caps on rank", rank, err)
			}
			facts[fmt.Sprintf("release_items_rank%d", rank)] = items
		}
		samples := [][2]int64{before}
		for range 8 {
			n := numCaps(observe(t))
			samples = append(samples, n)
			if n == [2]int64{} {
				break
			}
		}
		facts["num_caps_before_after_release"] = samples
		if last := samples[len(samples)-1]; last[0] >= before[0] && before[0] != 0 || last[1] >= before[1] && before[1] != 0 {
			t.Fatal("native num_caps did not drop after the release", samples)
		}
		for _, rank := range []int32{0, 1} {
			closing, stop := context.WithTimeout(ctx, 15*time.Second)
			err := sessions[rank].Close(closing)
			stop()
			if err != nil || sessions[rank].Status().State != cephmsgr.MDSSessionClosed || !errors.Is(sessions[rank].Err(), cephmsgr.ErrMDSSessionClosed) {
				t.Fatal("Close was not confirmed by rank", rank, err, sessions[rank].Err())
			}
			select {
			case <-agent(rank).done:
			case <-time.After(10 * time.Second):
				t.Fatal("capability agent did not observe the session end on rank", rank)
			}
			if s := agent(rank).snapshot(); len(s.Errors) != 0 || s.Pending != 0 || !strings.Contains(s.EndError, cephmsgr.ErrMDSSessionClosed.Error()) {
				t.Fatal("capability agent ended with errors or unapplied pushes on rank", rank, s.Errors, s.Pending, s.EndError)
			}
		}
		r := observe(t)
		for rank := range r.Views {
			if r.Views[rank].session(gid) != nil {
				t.Fatal("closed session remains in the native session map of rank", rank)
			}
		}
		if dials.OSD.Load() != 0 {
			t.Fatal("the client dialed an OSD")
		}
		var kinds []string
		for _, s := range snapshots() {
			for _, rec := range s.Records {
				kinds = append(kinds, rec.Kind)
			}
		}
		slices.SortFunc(kinds, cmp.Compare[string])
		facts["agent_record_kinds"] = slices.Compact(kinds)
	})
}
