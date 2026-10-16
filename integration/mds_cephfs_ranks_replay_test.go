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
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSRanksReplayIntegration extends the multi-rank acceptance
// to three active ranks and a standby-replay follower of rank 0: a mutation
// forwarded across ranks and resubmitted with its TID on each destination
// (ResubmitForwarded), Go mutations submitted while their subtree is frozen
// for export (the exporter's FLUSHMSG acknowledgement held), and the
// follower's takeover of rank 0 with ReconnectSession and the resubmission of
// requests whose journal entries never reached RADOS. Native LibCephFS, the
// MDS op trackers and counters are the oracles. See
// integration/fixture/MDS_CEPHFS_RANKS_FIXTURE.md and
// MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md.

// ranksHop is one MClientRequestForward of a request and the resubmission it
// caused.
type ranksHop struct {
	FromRank       int32
	Transaction    uint64
	SentForward    uint32
	ForwardedTo    int32
	ForwardedCount int32
	MustResend     bool
	Arrival        uint64
	Micros         int64
	ResubmittedTID uint64
}

type replayRequestEvidence struct {
	Name           string
	Op             uint32
	Transaction    uint64
	MDSMapEpoch    uint32
	StartRank      int32
	FinalRank      int32
	Hops           []ranksHop `json:",omitempty"`
	FirstOp        uint32
	FirstSafe      bool
	FirstResult    int32
	FirstArrival   uint64
	FirstMicros    int64
	FlushRequested bool
	SafeResult     int32
	SafeArrival    uint64
	SafeMicros     int64
	DentryName     string `json:",omitempty"`
	TargetIno      uint64 `json:",omitempty"`
	TargetCapID    uint64 `json:",omitempty"`
	TargetCaps     string `json:",omitempty"`
	ExtraIno       uint64 `json:",omitempty"`
	ResubmittedBy  string `json:",omitempty"`
	Error          string `json:",omitempty"`
	submitted      time.Time
}

type replayStatusSample struct {
	AfterFireMillis int64
	FSMapEpoch      uint32
	MDSMapEpoch     uint32
	Ranks           []*nativeMDSCephFSRanksDaemon
	Followers       []nativeMDSCephFSRanksDaemon
	Standbys        []nativeMDSCephFSRanksDaemon
	Failed          []int
	ArmedPresent    bool
}

type replayMigrationEvidence struct {
	Target          string
	ToRank          int32
	Pin             *nativeMDSCephFSRanksPin
	Placement       *nativeMDSCephFSRanksPlacement
	SettledMillis   int64
	AgentCapsAfter  map[string][]cephfsCap
	MigrationEvents []cephfsMigrationEvent
}

type replayInflightEvidence struct {
	FlushArrival     uint64
	FlushSequence    uint64
	FlushReceived    time.Time
	HeldFor          time.Duration
	PinStarted       time.Time
	FlushAfterPinMs  int64
	SubmittedAfterMs []int64
	ResolvedInHold   []bool
	ReleasedAt       time.Time
	Outcomes         []string
	ForwardArrivals  []uint64
	ExportArrival    uint64
	Rank0Ops         []nativeMDSCephFSHistoricOp
	Rank1Ops         []nativeMDSCephFSHistoricOp
	Migration        *replayMigrationEvidence
	Listing          []nativeMDSCephFSEntry
}

type replayTakeoverEvidence struct {
	Armed, Follower          nativeMDSCephFSRanksDaemon
	Others                   []nativeMDSCephFSRanksDaemon
	Pause                    *nativeMDSCephFSRanksRuntime
	UnsafeAfterFlushMillis   int64
	UnsafeWindow             *mdsUnsafeWindowEvidence
	StillUnsafe              []bool
	FireStarted, FireAcked   time.Time
	FireAckMillis            int64
	LostAfterFireMillis      int64
	LostError                recoveryErrorChain
	LostGeneration           uint64
	InflightOutcome          []recoveryErrorChain
	Timeline                 []replayStatusSample
	StateFirstSeenMillis     map[string]int64
	ReconnectAfterFireMillis int64
	ReconnectMicros          int64
	ReconnectTarget          cephmsgr.MDSTarget
	ReconnectCaps            []cephmsgr.MDSCapReconnect
	ReconnectRealms          []cephmsgr.MDSRealmReconnect
	Reopened                 *mdsSessionEvidence
	ActiveAfterFireMillis    int64
	Adopted                  *nativeMDSCephFSRanksTopology
	AdoptPolls               int
	OldFencing               []nativeMDSSessionsBlock
	OthersStatus             [2][]cephmsgr.MDSSessionStatus
	OthersResumes            [2][]uint64
	NativeRank0After         *nativeMDSSession
	NativeCapsBefore         []int64
	NativeCapsAfter          []int64
	Rank0Ops                 []nativeMDSCephFSHistoricOp
	Listing                  []nativeMDSCephFSEntry
	MDSDials                 []recoveryDial
}

func TestCephMDSCephFSRanksReplayIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1 and CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY=1 for owned standby-replay CephFS acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" ||
		os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("standby-replay CephFS acceptance requires its owned three-rank fixture")
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
	shape := nativeMDSCephFSRanksShape{Ranks: 3}
	_, tokenErr := hex.DecodeString(oracle.Token)
	fsName := fresh.FSName
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || identity != "client.mds-cephfs-ranks-"+oracle.Token || oracle.ClientIdentity != identity ||
		oracle.ClientCaps["mon"] != "allow r" || oracle.ClientCaps["mds"] != "allow rw fsname="+fsName || oracle.ClientCaps["osd"] != "" || oracle.NativeCaps["mds"] != "allow rwp fsname="+fsName ||
		!oracle.Extension || oracle.Shape != shape || plan.Directory != "ceph-msgr-ranks-"+oracle.Token || plan.Chain != "chain/hop" ||
		!slices.Equal(plan.ChainFiles, []string{"anchor", "forwarded-file", "forwarded-dir"}) || plan.Inflight != "inflight" ||
		!slices.Equal(plan.InflightFiles, []string{"anchor", "held-file", "held-dir"}) || plan.Replayed != "replay" ||
		!slices.Equal(plan.ReplayedFiles, []string{"safe-0", "safe-1", "inflight-file", "inflight-dir"}) || !topo.stableAs(shape) ||
		topo.Ranks[0].Name != fresh.MDS.Name || topo.Ranks[0].GID != fresh.MDS.GID || topo.Ranks[0].Endpoint != fresh.MDS.Endpoint || topo.Ranks[0].Nonce != fresh.MDS.Nonce {
		t.Fatal("owned standby-replay CephFS oracle, plan, identity or topology differ")
	}
	control := &mdsCephFSRanksControl{dir: dir, oracle: oracle, topology: topo, shape: shape}
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-replay", "root": "/"}
	options := cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024}
	dirPath := "/" + plan.Directory
	ranks := []int32{0, 1, 2}

	var (
		requests   []*replayRequestEvidence
		migrations []*replayMigrationEvidence
		inflight   replayInflightEvidence
		takeover   replayTakeoverEvidence
		chainFacts = map[string]any{}
		opens      [3]*mdsSessionEvidence
		conns      [3]*cephmsgr.MDSConnection
		sessions   [3]*cephmsgr.MDSSession
		agents     = newCephFSRankedAgents()
		lostAgents []*cephfsCapAgent
		endpoints  = []string{topo.Ranks[0].Endpoint, topo.Ranks[1].Endpoint, topo.Ranks[2].Endpoint, topo.spare().Endpoint}
		dials      = newRecoveryDials(false, endpoints...)
		facts      = map[string]any{}
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
		for _, rank := range ranks {
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
			Opens         [3]*mdsSessionEvidence         `json:"open"`
			Requests      []*replayRequestEvidence       `json:"requests"`
			Migrations    []*replayMigrationEvidence     `json:"migrations"`
			Chain         map[string]any                 `json:"forward_chain"`
			Inflight      replayInflightEvidence         `json:"inflight_during_export"`
			Takeover      replayTakeoverEvidence         `json:"standby_replay_takeover"`
			Agents        map[string]cephfsAgentSnapshot `json:"capability_agents"`
			Facts         map[string]any                 `json:"facts"`
			Native        []nativeMDSCephFSRanksReceipt  `json:"native"`
			Dials         []recoveryDial                 `json:"mds_dials"`
			OSDDials      uint32                         `json:"osd_dials"`
			CaseMillis    map[string]int64               `json:"case_ms"`
			ElapsedMillis int64                          `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, opens, requests, migrations, chainFacts, inflight, takeover, snapshots(), facts, control.receipts, dials.attempts(), dials.OSD.Load(), caseTimes,
			time.Since(started).Milliseconds()})
		if err == nil && len(data) > 16<<20 {
			err = errors.New("MDS CephFS standby-replay evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-ranks-replay-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS standby-replay public evidence", err)
		}
	})
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		for label, s := range snapshots() {
			if len(s.Errors) != 0 {
				t.Errorf("capability agent %s errors after %s: %v", label, name, s.Errors)
				return false
			}
		}
		return ok
	}
	var epoch uint32
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
	observe := func(t *testing.T) nativeMDSCephFSRanksReceipt {
		t.Helper()
		return native(t, "observe", strconv.FormatUint(control.goGID, 10))
	}
	ops := func(t *testing.T, rank int32) []nativeMDSCephFSHistoricOp {
		t.Helper()
		return native(t, "ops", strconv.Itoa(int(rank)), strconv.FormatUint(control.goGID, 10)).HistoricOps
	}
	listing := func(t *testing.T, target string, want map[string]uint64) []nativeMDSCephFSEntry {
		t.Helper()
		entries := native(t, "list", target).Native.Entries
		got := map[string]uint64{}
		for _, e := range entries {
			got[e.Name] = e.Stat.Ino
		}
		if !maps.Equal(got, want) {
			t.Fatalf("native listing of %s differs: %v, want %v", target, got, want)
		}
		return entries
	}
	numCaps := func(r nativeMDSCephFSRanksReceipt) []int64 {
		out := make([]int64, len(ranks))
		for _, rank := range ranks {
			if v := r.view(rank); v != nil {
				if n := v.session(control.goGID); n != nil {
					out[rank] = n.Caps
				}
			}
		}
		return out
	}
	forwards := func(r nativeMDSCephFSRanksReceipt) []uint64 {
		out := make([]uint64, len(ranks))
		for _, rank := range ranks {
			if v := r.view(rank); v != nil {
				out[rank] = v.Counters.Forward
			}
		}
		return out
	}
	findOp := func(list []nativeMDSCephFSHistoricOp, tid uint64) *nativeMDSCephFSHistoricOp {
		prefix := fmt.Sprintf("client_request(client.%d:%d ", control.goGID, tid)
		for i := range list {
			if strings.HasPrefix(list[i].Description, prefix) {
				return &list[i]
			}
		}
		return nil
	}
	// submit sends one request to a rank's session.
	submit := func(t *testing.T, rank int32, name string, request cephmsgr.MDSRequest) (*cephmsgr.MDSRequestCall, *replayRequestEvidence) {
		t.Helper()
		request.MDSMapEpoch = epoch
		e := &replayRequestEvidence{Name: name, Op: request.Op, MDSMapEpoch: epoch, StartRank: rank, FinalRank: rank, submitted: time.Now()}
		requests = append(requests, e)
		call, err := sessions[rank].Submit(ctx, request)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("submit", name, err)
		}
		e.Transaction = call.Transaction()
		return call, e
	}
	// finish waits for a call's first reply, answering each forward as the
	// native client does (Client::handle_client_request_forward): the same
	// request with the same TID and the reported forward count, sent to the
	// destination rank's session (ResubmitForwarded). The reply is applied to
	// the answering rank's agent; an unsafe reply is followed by a journal
	// flush and the safe reply.
	finish := func(t *testing.T, rank int32, call *cephmsgr.MDSRequestCall, e *replayRequestEvidence, maxForwards int) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		sent := uint32(0)
		var first cephmsgr.MDSReply
		for {
			var err error
			first, err = call.Wait(waitCtx)
			var forwarded *cephmsgr.MDSRequestForwardedError
			if !errors.As(err, &forwarded) {
				if err != nil {
					e.Error = err.Error()
					t.Fatal("first reply", e.Name, err)
				}
				break
			}
			hop := ranksHop{FromRank: rank, Transaction: call.Transaction(), SentForward: sent, ForwardedTo: forwarded.DestinationMDS, ForwardedCount: forwarded.Forward, MustResend: forwarded.MustResend,
				Arrival: forwarded.Arrival, Micros: time.Since(e.submitted).Microseconds()}
			if forwarded.Transaction != call.Transaction() || len(e.Hops) >= maxForwards || forwarded.DestinationMDS < 0 || int(forwarded.DestinationMDS) >= len(ranks) ||
				forwarded.DestinationMDS == rank || forwarded.Forward <= int32(sent) || !forwarded.MustResend {
				e.Hops = append(e.Hops, hop)
				e.Error = err.Error()
				t.Fatalf("unexpected forward of %s: %+v", e.Name, forwarded)
			}
			next, err := sessions[forwarded.DestinationMDS].ResubmitForwarded(ctx, call)
			if err != nil {
				e.Hops = append(e.Hops, hop)
				e.Error = err.Error()
				t.Fatal("resubmit forwarded", e.Name, err)
			}
			hop.ResubmittedTID = next.Transaction()
			e.Hops = append(e.Hops, hop)
			if next.Transaction() != call.Transaction() {
				t.Fatal("ResubmitForwarded changed the TID", call.Transaction(), next.Transaction())
			}
			rank, call, sent, e.FinalRank = forwarded.DestinationMDS, next, uint32(forwarded.Forward), forwarded.DestinationMDS
		}
		e.FirstMicros, e.FirstOp, e.FirstSafe, e.FirstResult, e.FirstArrival = first.Received.Sub(e.submitted).Microseconds(), first.Op, first.Safe, first.Result, first.Arrival
		var trace cephmsgr.MDSTrace
		if first.IsDentry || first.IsTarget {
			var err error
			if trace, _, err = agent(rank).applyReply(e.Name, first); err != nil {
				e.Error = err.Error()
				t.Fatal("decode reply trace", e.Name, err)
			}
		}
		e.DentryName = trace.DentryName
		if trace.Target != nil {
			e.TargetIno, e.TargetCapID, e.TargetCaps = trace.Target.Ino, trace.Target.Cap.CapID, cephCapString(trace.Target.Cap.Caps)
		}
		if len(first.Extra) == 8 {
			e.ExtraIno = binary.LittleEndian.Uint64(first.Extra)
		}
		if snaps, err := first.DecodeSnapTrace(); err == nil && rank == 0 {
			for _, r := range snaps {
				realms[r.Ino] = r
			}
		}
		safe := first
		if !first.Safe {
			if err := sessions[rank].FlushJournal(ctx); err != nil {
				t.Fatal("flush journal", e.Name, err)
			}
			e.FlushRequested = true
			var err error
			if safe, err = call.WaitSafe(waitCtx); err != nil {
				e.Error = err.Error()
				t.Fatal("safe reply", e.Name, err)
			}
		}
		e.SafeMicros, e.SafeResult, e.SafeArrival = safe.Received.Sub(e.submitted).Microseconds(), safe.Result, safe.Arrival
		if !safe.Safe || safe.Transaction != call.Transaction() {
			t.Fatal("safe reply identity differs", e.Name)
		}
		return first, safe, trace
	}
	send := func(t *testing.T, rank int32, name string, request cephmsgr.MDSRequest, maxForwards int) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace, *replayRequestEvidence) {
		t.Helper()
		call, e := submit(t, rank, name, request)
		first, safe, trace := finish(t, rank, call, e, maxForwards)
		return first, safe, trace, e
	}
	createRequest := func(path cephmsgr.MDSFilePath) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: path}
	}
	mkdirRequest := func(path cephmsgr.MDSFilePath) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755), Path: path}
	}
	// closeCreated closes the read-write handle a CREATE opened, as an
	// application would, on the rank that answered it.
	closeCreated := func(t *testing.T, e *replayRequestEvidence) {
		t.Helper()
		a := agent(e.FinalRank)
		a.open(e.TargetIno, cephFileModeRDWR)
		if _, _, err := a.close(e.TargetIno, cephFileModeRDWR); err != nil {
			t.Fatal("close created file", e.Name, err)
		}
	}
	created := func(t *testing.T, first, safe cephmsgr.MDSReply, trace cephmsgr.MDSTrace, e *replayRequestEvidence, name string) uint64 {
		t.Helper()
		if first.Result != 0 || safe.Result != 0 || trace.Target == nil || trace.DentryName != name {
			t.Fatalf("%s differs: %+v", e.Name, e)
		}
		if e.Op == cephmsgr.MDSOpCreate {
			if e.ExtraIno != trace.Target.Ino {
				t.Fatalf("%s extra inode differs: %+v", e.Name, e)
			}
			closeCreated(t, e)
		}
		return trace.Target.Ino
	}
	mkdir := func(t *testing.T, rank int32, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, e := send(t, rank, "mkdir "+name, mkdirRequest(cephmsgr.MDSFilePath{Ino: parent, Path: name}), 0)
		return created(t, first, safe, trace, e, name)
	}
	create := func(t *testing.T, rank int32, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, e := send(t, rank, "create "+name, createRequest(cephmsgr.MDSFilePath{Ino: parent, Path: name}), 0)
		return created(t, first, safe, trace, e, name)
	}
	// settle waits until the agents hold every listed inode's cap only on rank
	// to, with no unapplied pushes.
	settle := func(t *testing.T, to int32, inos []uint64) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			done := true
			for _, ino := range inos {
				for _, rank := range ranks {
					c, _ := agent(rank).cap(ino)
					if rank == to {
						done = done && c.CapID != 0 && !c.Exported && !c.Released
					} else {
						done = done && c.CapID == 0
					}
				}
			}
			for _, rank := range ranks {
				done = done && agent(rank).snapshot().Pending == 0
			}
			if done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("caps of %v did not settle on rank %d: %v", inos, to, snapshots())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	migrationMarks := func() map[int32]int {
		marks := map[int32]int{}
		for _, rank := range ranks {
			marks[rank] = len(agent(rank).snapshot().Migrations)
		}
		return marks
	}
	migrationSince := func(marks map[int32]int) []cephfsMigrationEvent {
		var out []cephfsMigrationEvent
		for _, rank := range ranks {
			out = append(out, agent(rank).snapshot().Migrations[marks[rank]:]...)
		}
		return out
	}
	agentCaps := func(inos ...uint64) map[string][]cephfsCap {
		out := map[string][]cephfsCap{}
		for _, rank := range ranks {
			for _, ino := range inos {
				if c, ok := agent(rank).cap(ino); ok && c.CapID != 0 {
					out[fmt.Sprintf("rank%d", rank)] = append(out[fmt.Sprintf("rank%d", rank)], c)
				}
			}
		}
		return out
	}
	migrate := func(t *testing.T, target string, to int32, inos []uint64) *replayMigrationEvidence {
		t.Helper()
		e := &replayMigrationEvidence{Target: target, ToRank: to}
		migrations = append(migrations, e)
		marks := migrationMarks()
		e.Pin = native(t, "pin", target, strconv.Itoa(int(to))).Pin
		placed := native(t, "placement", target, strconv.Itoa(int(to)))
		e.Placement = placed.Placement
		if !placed.Placement.Converged {
			t.Fatalf("subtree %s did not converge on rank %d: %+v", target, to, placed.Views)
		}
		if s := placed.view(to).subtree(dirPath + "/" + target); s == nil || !s.IsAuth || s.AuthFirst != int(to) || s.ExportPin != int(to) {
			t.Fatalf("rank %d does not report the pinned subtree %s: %+v", to, target, s)
		}
		begin := time.Now()
		settle(t, to, inos)
		e.SettledMillis, e.MigrationEvents, e.AgentCapsAfter = time.Since(begin).Milliseconds(), migrationSince(marks), agentCaps(inos...)
		return e
	}

	client := recoveryClient(t, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) {
		o.EnableMDS, o.EnableCephFS, o.EnableOSD, o.MaxMDSConnections = true, true, false, 3
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
	var dirIno, chainIno, hopIno, chainAnchor, inflightIno, inflightAnchor, replayIno uint64
	inos := map[string]uint64{}

	if !leaf("open_sessions_on_three_ranks", func(t *testing.T) {
		for _, rank := range ranks {
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
		r := observe(t)
		addresses := map[string]bool{}
		for rank, v := range r.Views {
			n := v.session(gid)
			if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
				t.Fatal("rank", rank, err)
			}
			if n.Caps != 0 || !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(cephfsNonce), 10)) {
				t.Fatalf("rank %d native session holds caps or lacks the shared nonce: %+v", rank, n)
			}
			addresses[n.Address] = true
		}
		if len(addresses) != 1 {
			t.Fatal("the ranks see different client address instances", addresses)
		}
		facts["client"] = map[string]any{"global_id": gid, "cephfs_nonce": cephfsNonce, "native_address": slices.Collect(maps.Keys(addresses))[0]}
		facts["startup_topology"] = r.Topology
	}) {
		return
	}

	if !leaf("enable_standby_replay", func(t *testing.T) {
		before := control.topology
		r := native(t, "replay-enable")
		e := r.ReplayEnable
		follower := r.Topology.spare()
		if follower.State != "up:standby-replay" || follower.Rank != 0 || follower.GID != before.spare().GID || follower.Nonce != before.spare().Nonce {
			t.Fatal("the standby does not follow rank 0", follower)
		}
		facts["standby_replay"] = map[string]any{"standby": e.Standby, "follower": e.Follower, "polls": e.Polls, "waited_ms": e.WaitedMillis, "fsmap_epoch_before": before.FSMapEpoch,
			"fsmap_epoch_after": r.Topology.FSMapEpoch, "mdsmap_epoch_after": r.Topology.MDSMapEpoch}
		after := observe(t)
		for rank, v := range after.Views {
			if n := v.session(gid); checkNativeCephFSSession(n, identity, metadata) != nil {
				t.Fatal("a session changed when standby replay was enabled", rank, n)
			}
		}
	}) {
		return
	}

	if !leaf("forward_chain_across_ranks", func(t *testing.T) {
		dirIno = mkdir(t, 0, 1, plan.Directory)
		chainIno = mkdir(t, 0, dirIno, "chain")
		hopIno = mkdir(t, 0, chainIno, "hop")
		chainAnchor = create(t, 0, hopIno, "anchor")
		inos["chain/hop/anchor"] = chainAnchor
		migrate(t, "chain", 1, []uint64{hopIno, chainAnchor})
		migrate(t, "chain/hop", 2, []uint64{chainAnchor})
		before := observe(t)
		// The full path from the root makes rank 0 walk to the chain's
		// dirfrag, whose authority is rank 1; rank 1 walks on to hop's
		// dirfrag, whose authority is rank 2 (MDCache::path_traverse forwards
		// to the dirfrag authority it knows).
		var chained []*replayRequestEvidence
		for _, request := range []struct {
			name string
			req  cephmsgr.MDSRequest
		}{
			{"forwarded-file", createRequest(cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory + "/chain/hop/forwarded-file"})},
			{"forwarded-dir", mkdirRequest(cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory + "/chain/hop/forwarded-dir"})},
		} {
			first, safe, trace, e := send(t, 0, "chained "+request.name, request.req, 3)
			inos["chain/hop/"+request.name] = created(t, first, safe, trace, e, request.name)
			if len(e.Hops) == 0 || e.FinalRank != 2 || e.Hops[len(e.Hops)-1].ForwardedTo != 2 {
				t.Fatalf("%s was not forwarded to rank 2: %+v", request.name, e)
			}
			for i, h := range e.Hops {
				if h.ForwardedCount != int32(i+1) || h.ResubmittedTID != e.Transaction {
					t.Fatalf("forward count or TID of %s hop %d differs: %+v", request.name, i, h)
				}
			}
			chained = append(chained, e)
		}
		after := observe(t)
		b, a := forwards(before), forwards(after)
		hopsFrom := make([]uint64, len(ranks))
		for _, e := range chained {
			for _, h := range e.Hops {
				hopsFrom[h.FromRank]++
			}
		}
		chainFacts["forward_counters"] = map[string]any{"before": b, "after": a, "go_hops_from_rank": hopsFrom}
		for _, rank := range ranks {
			if a[rank]-b[rank] != hopsFrom[rank] {
				t.Fatalf("native forward counters differ from the forwards Go received: before %v after %v hops %v", b, a, hopsFrom)
			}
		}
		perRank := map[string]any{}
		for _, rank := range ranks {
			list := ops(t, rank)
			for _, e := range chained {
				perRank[fmt.Sprintf("rank%d/%s", rank, e.Name)] = findOp(list, e.Transaction)
			}
		}
		chainFacts["historic_ops"] = perRank
		chainFacts["listing"] = listing(t, "chain/hop", map[string]uint64{"anchor": chainAnchor, "forwarded-file": inos["chain/hop/forwarded-file"],
			"forwarded-dir": inos["chain/hop/forwarded-dir"]})
		chainFacts["requests"] = chained
	}) {
		return
	}

	if !leaf("inflight_mutation_during_export", func(t *testing.T) {
		inflightIno = mkdir(t, 0, dirIno, "inflight")
		inflightAnchor = create(t, 0, inflightIno, "anchor")
		inos["inflight/anchor"] = inflightAnchor
		before := observe(t)
		marks := migrationMarks()
		flushMark := len(agent(0).snapshot().Flushes)
		received, release := agent(0).holdNextFlush(20 * time.Second)
		defer release()
		type pinResult struct {
			r   nativeMDSCephFSRanksReceipt
			err error
		}
		pinned := make(chan pinResult, 1)
		inflight.PinStarted = time.Now()
		go func() {
			r, err := control.request(ctx, "pin", "inflight", "1")
			pinned <- pinResult{r, err}
		}()
		select {
		case <-received:
		case <-time.After(45 * time.Second):
			t.Fatal("the exporter's FLUSHMSG did not reach rank 0's session")
		}
		f := agent(0).snapshot().Flushes[flushMark]
		inflight.FlushArrival, inflight.FlushSequence, inflight.FlushReceived = f.Arrival, f.Sequence, f.Received
		inflight.FlushAfterPinMs = f.Received.Sub(inflight.PinStarted).Milliseconds()
		// The subtree is frozen now (Migrator::export_frozen flushes the
		// client sessions after the freeze) and the export waits for this
		// session's acknowledgement.
		type held struct {
			call *cephmsgr.MDSRequestCall
			e    *replayRequestEvidence
			name string
		}
		var calls []held
		for _, h := range []struct {
			name string
			req  cephmsgr.MDSRequest
		}{
			{"held-file", createRequest(cephmsgr.MDSFilePath{Ino: inflightIno, Path: "held-file"})},
			{"held-dir", mkdirRequest(cephmsgr.MDSFilePath{Ino: inflightIno, Path: "held-dir"})},
		} {
			call, e := submit(t, 0, "frozen "+h.name, h.req)
			inflight.SubmittedAfterMs = append(inflight.SubmittedAfterMs, time.Since(f.Received).Milliseconds())
			calls = append(calls, held{call, e, h.name})
		}
		time.Sleep(500 * time.Millisecond)
		probe, stop := context.WithCancel(ctx)
		stop()
		for _, h := range calls {
			_, err := h.call.Wait(probe)
			inflight.ResolvedInHold = append(inflight.ResolvedInHold, !errors.Is(err, context.Canceled))
		}
		inflight.ReleasedAt = time.Now()
		release()
		var pin pinResult
		select {
		case pin = <-pinned:
		case <-time.After(110 * time.Second):
			t.Fatal("native pin did not finish")
		}
		if pin.err != nil || pin.r.Pin == nil || pin.r.Pin.State != "applied" {
			t.Fatal("native pin of the in-flight directory failed", pin.err, pin.r.Pin)
		}
		e := &replayMigrationEvidence{Target: "inflight", ToRank: 1, Pin: pin.r.Pin}
		placed := native(t, "placement", "inflight", "1")
		e.Placement = placed.Placement
		if !placed.Placement.Converged {
			t.Fatal("the in-flight directory did not converge on rank 1", placed.Views)
		}
		for i, h := range calls {
			first, safe, trace := finish(t, 0, h.call, h.e, 2)
			inos["inflight/"+h.name] = created(t, first, safe, trace, h.e, h.name)
			outcome := fmt.Sprintf("answered by rank %d after %d forward(s)", h.e.FinalRank, len(h.e.Hops))
			inflight.Outcomes = append(inflight.Outcomes, outcome)
			if len(h.e.Hops) > 0 {
				inflight.ForwardArrivals = append(inflight.ForwardArrivals, h.e.Hops[0].Arrival)
				if h.e.Hops[0].Arrival <= f.Arrival || h.e.FinalRank != 1 {
					t.Fatalf("frozen request %s was forwarded before the FLUSHMSG or elsewhere: %+v", h.name, h.e)
				}
			}
			if inflight.ResolvedInHold[i] {
				t.Logf("frozen request %s resolved while the export waited: %s", h.name, outcome)
			}
		}
		settle(t, 1, []uint64{inflightAnchor})
		e.MigrationEvents, e.AgentCapsAfter = migrationSince(marks), agentCaps(inflightAnchor)
		for _, m := range e.MigrationEvents {
			if m.Op == cephmsgr.MDSCapOpExport && m.Ino == inflightAnchor && m.Rank == 0 {
				inflight.ExportArrival = m.Arrival
				if m.Arrival <= f.Arrival {
					t.Fatal("EXPORT arrived before the held FLUSHMSG", m, f)
				}
			}
		}
		inflight.Migration = e
		migrations = append(migrations, e)
		after := observe(t)
		facts["inflight_counters"] = map[string]any{"forward_before": forwards(before), "forward_after": forwards(after), "num_caps_before": numCaps(before), "num_caps_after": numCaps(after)}
		f = agent(0).snapshot().Flushes[flushMark]
		inflight.HeldFor = f.HeldFor
		if f.Error != "" || f.Acked.Before(inflight.ReleasedAt) || f.Sequence != f.AgentPushes {
			t.Fatal("held FLUSHMSG acknowledgement differs", f)
		}
		inflight.Rank0Ops, inflight.Rank1Ops = ops(t, 0), ops(t, 1)
		for _, h := range calls {
			facts["inflight_op_rank0_"+h.name], facts["inflight_op_rank1_"+h.name] = findOp(inflight.Rank0Ops, h.e.Transaction), findOp(inflight.Rank1Ops, h.e.Transaction)
		}
		inflight.Listing = listing(t, "inflight", map[string]uint64{"anchor": inflightAnchor, "held-file": inos["inflight/held-file"], "held-dir": inos["inflight/held-dir"]})
	}) {
		return
	}

	if !leaf("standby_replay_takeover_resubmit", func(t *testing.T) {
		replayIno = mkdir(t, 0, dirIno, "replay")
		for _, name := range []string{"safe-0", "safe-1"} {
			inos["replay/"+name] = create(t, 0, replayIno, name)
		}
		pause := native(t, "log-pause")
		takeover.Pause = pause.Runtime
		// Requests answered unsafe while rank 0's journal is paused: their
		// journal entries never reach RADOS, so the follower cannot replay them.
		type pending struct {
			call  *cephmsgr.MDSRequestCall
			e     *replayRequestEvidence
			name  string
			ino   uint64
			first cephmsgr.MDSReply
		}
		var held []*pending
		for _, h := range []struct {
			name string
			req  cephmsgr.MDSRequest
		}{
			{"inflight-file", createRequest(cephmsgr.MDSFilePath{Ino: replayIno, Path: "inflight-file"})},
			{"inflight-dir", mkdirRequest(cephmsgr.MDSFilePath{Ino: replayIno, Path: "inflight-dir"})},
		} {
			call, e := submit(t, 0, "paused "+h.name, h.req)
			waitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			first, err := call.Wait(waitCtx)
			stop()
			if err != nil || first.Safe || first.Result != 0 {
				t.Fatalf("%s was not answered unsafe while the journal was paused: %v %+v", h.name, err, first)
			}
			e.FirstMicros, e.FirstOp, e.FirstSafe, e.FirstResult, e.FirstArrival = first.Received.Sub(e.submitted).Microseconds(), first.Op, first.Safe, first.Result, first.Arrival
			trace, _, err := agent(0).applyReply(e.Name, first)
			if err != nil || trace.Target == nil || trace.DentryName != h.name {
				t.Fatal("unsafe reply trace differs", h.name, err)
			}
			e.TargetIno, e.DentryName = trace.Target.Ino, trace.DentryName
			p := &pending{call, e, h.name, trace.Target.Ino, first}
			if h.name == "inflight-file" {
				if len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != p.ino {
					t.Fatal("unsafe create extra differs", first.Extra)
				}
				e.ExtraIno = p.ino
				closeCreated(t, e)
			}
			held = append(held, p)
		}
		if err := sessions[0].FlushJournal(ctx); err != nil {
			t.Fatal(err)
		}
		flushed := time.Now()
		// The MDS side of the window (ceph-testcontainers-go v0.3.0 unsafe
		// receipt): two dump_ops_in_flight samples of rank 0, at least 3 s
		// apart and from the same daemon GID, hold both held requests after
		// early_replied, at the journal submission and not journal_committed.
		sampleInFlight := func(t *testing.T) mdsUnsafeWindowSample {
			t.Helper()
			sent := time.Now()
			r := native(t, "ops-in-flight", "0", strconv.FormatUint(control.goGID, 10))
			return mdsUnsafeWindowSample{MDSGID: r.OpsDaemon.GID, SentAt: sent, ReturnedAt: time.Now(), Ops: r.InFlightOps}
		}
		firstSample := sampleInFlight(t)
		time.Sleep(time.Until(firstSample.ReturnedAt.Add(mdsUnsafeWindowGap)))
		secondSample := sampleInFlight(t)
		wantHeld := map[uint64]string{}
		for _, p := range held {
			wantHeld[p.e.Transaction] = mdsRequestUnsafe
		}
		window, err := checkMDSUnsafeWindow(firstSample, secondSample, control.goGID, wantHeld)
		takeover.UnsafeWindow = &window
		if err != nil {
			t.Fatal(err)
		}
		if secondSample.MDSGID != control.topology.Ranks[0].GID {
			t.Fatalf("the unsafe window was sampled on MDS %d, not rank 0's %d", secondSample.MDSGID, control.topology.Ranks[0].GID)
		}
		takeover.UnsafeAfterFlushMillis = time.Since(flushed).Milliseconds()
		probe, stop := context.WithCancel(ctx)
		stop()
		for _, p := range held {
			_, err := p.call.WaitSafe(probe)
			still := errors.Is(err, context.Canceled)
			takeover.StillUnsafe = append(takeover.StillUnsafe, still)
			if !still {
				t.Fatal("a request became safe while the journal was paused", p.name, err)
			}
		}
		before := observe(t)
		takeover.NativeCapsBefore = numCaps(before)
		for _, rank := range []int32{1, 2} {
			takeover.OthersStatus[0] = append(takeover.OthersStatus[0], sessions[rank].Status())
			takeover.OthersResumes[0] = append(takeover.OthersResumes[0], conns[rank].Resumes())
		}
		armed := native(t, "takeover-arm").Takeover
		takeover.Armed, takeover.Follower, takeover.Others = armed.Armed, armed.Standby, armed.Others
		if !armed.Follow || armed.Standby.State != "up:standby-replay" {
			t.Fatal("the armed spare is not the standby-replay follower", armed.Standby)
		}
		lost, lostConn := sessions[0], conns[0]
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
		takeover.FireAckMillis = afterFire(takeover.FireAcked)
		var lostAt time.Time
		select {
		case lostAt = <-lostSeen:
		case <-time.After(45 * time.Second):
			t.Fatal("the rank-0 session did not end after its daemon failed")
		}
		status := lost.Status()
		takeover.LostAfterFireMillis, takeover.LostError, takeover.LostGeneration = afterFire(lostAt), describeRecoveryError(lost.Err()), status.Generation
		if status.State != cephmsgr.MDSSessionLost {
			t.Fatalf("rank-0 session ended %d, not Lost: %v", status.State, lost.Err())
		}
		for _, p := range held {
			_, err := p.call.WaitSafe(probe)
			takeover.InflightOutcome = append(takeover.InflightOutcome, describeRecoveryError(err))
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &unknown) {
				t.Fatal("paused request did not end with an unknown outcome", p.name, err)
			}
		}
		previous := agent(0)
		select {
		case <-previous.done:
		case <-time.After(10 * time.Second):
			t.Fatal("rank-0 agent did not observe the lost session")
		}
		lostAgents = append(lostAgents, previous)
		takeover.StateFirstSeenMillis = map[string]int64{}
		sample := func(t *testing.T) (*nativeMDSCephFSRanksDaemon, nativeMDSCephFSRanksStatus) {
			r, err := control.request(ctx, "takeover-status")
			if err != nil {
				t.Fatal(err)
			}
			s := *r.Status
			at := afterFire(time.Now())
			takeover.Timeline = append(takeover.Timeline, replayStatusSample{AfterFireMillis: at, FSMapEpoch: s.Topology.FSMapEpoch, MDSMapEpoch: s.Topology.MDSMapEpoch,
				Ranks: s.Topology.Ranks, Followers: s.Topology.Followers, Standbys: s.Topology.Standbys, Failed: s.Topology.Failed, ArmedPresent: s.ArmedPresent})
			var r0 *nativeMDSCephFSRanksDaemon
			if len(s.Topology.Ranks) > 0 {
				r0 = s.Topology.Ranks[0]
			}
			if r0 != nil && r0.GID == armed.Standby.GID {
				if _, seen := takeover.StateFirstSeenMillis[r0.State]; !seen {
					takeover.StateFirstSeenMillis[r0.State] = at
				}
			}
			for _, f := range s.Topology.Followers {
				if _, seen := takeover.StateFirstSeenMillis["follower:"+f.Name]; !seen {
					takeover.StateFirstSeenMillis["follower:"+f.Name] = at
				}
			}
			return r0, s
		}
		var newRank0 *nativeMDSCephFSRanksDaemon
		deadline := time.Now().Add(60 * time.Second)
		for newRank0 == nil {
			r0, s := sample(t)
			if r0 != nil && r0.GID == armed.Standby.GID && r0.State == "up:reconnect" {
				newRank0 = r0
				break
			}
			if r0 != nil && r0.GID == armed.Standby.GID && !slices.Contains([]string{"up:replay", "up:resolve", "up:reconnect"}, r0.State) {
				t.Fatal("the follower left rank 0's reconnect phase before the Go client reconnected", r0)
			}
			if time.Now().After(deadline) {
				t.Fatal("the follower did not reach rank 0's reconnect phase", s)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if newRank0.Name != armed.Standby.Name || newRank0.Endpoint != armed.Standby.Endpoint || newRank0.Nonce != armed.Standby.Nonce || newRank0.Incarnation == armed.Armed.Incarnation {
			t.Fatal("rank 0 is not the follower at its registered address in a new incarnation", newRank0, armed.Standby)
		}
		takeover.ReconnectTarget = newRank0.target(fresh.FSCID, 0)
		if err := lostConn.Close(); err != nil {
			t.Fatal(err)
		}
		previous.mu.Lock()
		maps.Copy(realms, previous.realms)
		previous.mu.Unlock()
		caps, realmState, err := previous.reconnectState(realms)
		if err != nil || len(caps) == 0 {
			t.Fatal("no rank-0 reconnect state", err)
		}
		takeover.ReconnectCaps, takeover.ReconnectRealms = caps, realmState
		reconnectStarted := time.Now()
		takeover.ReconnectAfterFireMillis = afterFire(reconnectStarted)
		conn, err := client.OpenMDS(ctx, takeover.ReconnectTarget)
		if err != nil {
			t.Fatal("open the new rank-0 daemon", err)
		}
		conns[0] = conn
		resubmit := []*cephmsgr.MDSRequestCall{held[0].call, held[1].call}
		reconnectCtx, stopReconnect := context.WithTimeout(ctx, 30*time.Second)
		session, calls, err := conn.ReconnectSession(reconnectCtx, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState, Resubmit: resubmit}, options)
		stopReconnect()
		takeover.ReconnectMicros = time.Since(reconnectStarted).Microseconds()
		if err != nil {
			t.Fatal("CephFS reconnect to the follower", err)
		}
		sessions[0] = session
		agents.startFrom(ctx, 0, session, previous)
		reopened := describeMDSSession("rank0-reconnected", identity, session)
		takeover.Reopened = &reopened
		st := session.Status()
		if st.State != cephmsgr.MDSSessionOpen || st.ReconnectedFrom != takeover.LostGeneration || st.GlobalID != gid || len(calls) != len(resubmit) {
			t.Fatalf("reconnected rank-0 session differs: %+v", st)
		}
		for i, call := range calls {
			p := held[i]
			e := &replayRequestEvidence{Name: "resubmitted " + p.name, Op: p.e.Op, Transaction: call.Transaction(), StartRank: 0, FinalRank: 0, ResubmittedBy: "ReconnectSession", submitted: reconnectStarted}
			requests = append(requests, e)
			if call.Transaction() != p.call.Transaction() {
				t.Fatal("resubmitted call changed its TID")
			}
			first, safe, trace := finish(t, 0, call, e, 0)
			// A REPLAY request is answered without a trace
			// (Server::reply_client_request); a CREATE's extra still names
			// its inode. The native listing below checks every inode.
			ino := e.ExtraIno
			if trace.Target != nil {
				ino = trace.Target.Ino
			}
			facts["replayed_reply_"+p.name] = map[string]any{"traceless": !first.IsDentry && !first.IsTarget, "extra_ino": e.ExtraIno, "op": first.Op, "safe_first": first.Safe}
			if first.Result != 0 || safe.Result != 0 || ino != 0 && ino != p.ino {
				t.Fatalf("resubmitted %s did not complete with the unsafe reply's inode %d: %+v", p.name, p.ino, e)
			}
			inos["replay/"+p.name] = p.ino
		}
		deadline = time.Now().Add(90 * time.Second)
		for {
			r0, s := sample(t)
			if r0 != nil && r0.GID == newRank0.GID && r0.State == "up:active" && len(s.Topology.Followers) == 1 && s.Topology.Followers[0].Name == armed.Armed.Name {
				takeover.ActiveAfterFireMillis = afterFire(time.Now())
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the follower did not become active with the failed daemon following it", s)
			}
			time.Sleep(250 * time.Millisecond)
		}
		adopted, err := control.request(ctx, "takeover-adopt")
		if err != nil {
			t.Fatal(err)
		}
		takeover.Adopted, takeover.OldFencing, takeover.AdoptPolls = adopted.Takeover.Adopted, adopted.Takeover.OldFencing, adopted.Takeover.AdoptPolls
		for _, address := range adopted.Takeover.ArmedAddresses {
			if !slices.ContainsFunc(adopted.Takeover.OldFencing, func(b nativeMDSSessionsBlock) bool { return b.Address == address && !b.Range }) {
				t.Fatal("the failed rank-0 incarnation's address is not blocklisted", address, adopted.Takeover.OldFencing)
			}
		}
		after := observe(t)
		takeover.NativeCapsAfter = numCaps(after)
		takeover.NativeRank0After = after.view(0).session(gid)
		if err := checkNativeCephFSSession(takeover.NativeRank0After, identity, metadata); err != nil || takeover.NativeRank0After.Address != before.view(0).session(gid).Address {
			t.Fatal("the reconnected session is not open on the new rank-0 daemon at the same address", err, takeover.NativeRank0After)
		}
		for i, rank := range []int32{1, 2} {
			takeover.OthersStatus[1] = append(takeover.OthersStatus[1], sessions[rank].Status())
			takeover.OthersResumes[1] = append(takeover.OthersResumes[1], conns[rank].Resumes())
			b, a := takeover.OthersStatus[0][i], takeover.OthersStatus[1][i]
			if a.State != cephmsgr.MDSSessionOpen || a.Generation != b.Generation || takeover.OthersResumes[1][i] != takeover.OthersResumes[0][i] ||
				takeover.NativeCapsAfter[rank] != takeover.NativeCapsBefore[rank] {
				t.Fatalf("rank %d was affected by the rank-0 takeover: %+v %+v", rank, b, a)
			}
		}
		takeover.Rank0Ops = ops(t, 0)
		for _, p := range held {
			op := findOp(takeover.Rank0Ops, p.call.Transaction())
			facts["replayed_op_"+p.name] = op
			if op == nil || !strings.Contains(op.Description, "REPLAY") {
				t.Fatal("the new rank-0 daemon did not execute the resubmitted request with REPLAY", p.name, op)
			}
		}
		first, _, trace, _ := send(t, 0, "getattr "+plan.Directory+" after takeover", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: dirIno}}, 0)
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != dirIno {
			t.Fatal("getattr on the new rank-0 daemon differs")
		}
		takeover.Listing = listing(t, "replay", map[string]uint64{"safe-0": inos["replay/safe-0"], "safe-1": inos["replay/safe-1"], "inflight-file": inos["replay/inflight-file"],
			"inflight-dir": inos["replay/inflight-dir"]})
		takeover.MDSDials = dials.attempts()
	}) {
		return
	}

	t.Run("cleanup_restore_pins_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_restore_pins_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		for _, target := range []string{"chain/hop", "chain", "inflight"} {
			r := native(t, "pin-restore", target)
			facts["pin_restore_"+target] = r.Pin
			if r.Pin.Value != "-1" {
				t.Fatal("pin baseline of", target, "is not -1:", r.Pin)
			}
		}
		remove := func(rank int32, parent uint64, name string, op uint32) {
			first, safe, _, e := send(t, rank, "remove "+name, cephmsgr.MDSRequest{Op: op, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, 3)
			if first.Result != 0 || safe.Result != 0 {
				t.Fatalf("remove %s differs: %+v", name, e)
			}
		}
		unlink, rmdir := cephmsgr.MDSOpUnlink, cephmsgr.MDSOpRmdir
		remove(2, hopIno, "anchor", unlink)
		remove(2, hopIno, "forwarded-file", unlink)
		remove(2, hopIno, "forwarded-dir", rmdir)
		remove(1, chainIno, "hop", rmdir)
		remove(0, dirIno, "chain", rmdir)
		remove(1, inflightIno, "anchor", unlink)
		remove(1, inflightIno, "held-file", unlink)
		remove(1, inflightIno, "held-dir", rmdir)
		remove(0, dirIno, "inflight", rmdir)
		for _, name := range []string{"safe-0", "safe-1", "inflight-file"} {
			remove(0, replayIno, name, unlink)
		}
		remove(0, replayIno, "inflight-dir", rmdir)
		remove(0, dirIno, "replay", rmdir)
		remove(0, 1, plan.Directory, rmdir)
		st := native(t, "stat", "dir").Native.Stat
		if st.Exists || st.Errno != 2 {
			t.Fatal("native stat still finds the owned directory", st)
		}
		before := numCaps(observe(t))
		for _, rank := range ranks {
			items, barrier := agent(rank).releaseAll()
			releaseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			err := sessions[rank].ReleaseCaps(releaseCtx, items, barrier)
			stop()
			if err != nil {
				t.Fatal("release caps on rank", rank, err)
			}
		}
		samples := [][]int64{before}
		for range 8 {
			n := numCaps(observe(t))
			samples = append(samples, n)
			if slices.Max(n) == 0 {
				break
			}
		}
		facts["num_caps_before_after_release"] = samples
		if last := samples[len(samples)-1]; slices.Max(last) != 0 {
			t.Fatal("native num_caps did not reach zero after the release", samples)
		}
		for _, rank := range ranks {
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
