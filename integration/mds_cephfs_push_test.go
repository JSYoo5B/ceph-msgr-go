package integration_test

import (
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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSPushIntegration accepts MDS pushes that had only
// synthetic coverage against a live Tentacle MDS with native oracles: a
// dentry lease REVOKE answered with RELEASE while a native rename waits for
// it, snapshot-realm and quota pushes provoked by native mksnap and quota
// setxattr, RECALL_STATE answered by releasing capabilities, and FLUSHMSG
// acknowledged after a `cache drop` recall. See integration/fixture/
// MDS_CEPHFS_FIXTURE.md and MDS_CEPHFS_PUSH_VALIDATION.md.

type nativeMDSCephFSPushPlan struct {
	Directory     string   `json:"directory"`
	LeaseDir      string   `json:"lease_dir"`
	Victim        string   `json:"victim"`
	Renamed       string   `json:"renamed"`
	SnapDir       string   `json:"snap_dir"`
	SnapFile      string   `json:"snap_file"`
	Snapshot      string   `json:"snapshot"`
	QuotaDir      string   `json:"quota_dir"`
	QuotaFile     string   `json:"quota_file"`
	QuotaMaxBytes int64    `json:"quota_max_bytes"`
	RecallDir     string   `json:"recall_dir"`
	RecallFiles   []string `json:"recall_files"`
	MinCaps       uint64   `json:"recall_min_caps"`
	MaxCaps       uint64   `json:"recall_max_caps"`
	DropTimeout   int      `json:"cache_drop_timeout_s"`
}

type nativeMDSCephFSPushOracle struct {
	nativeMDSCephFSOracle
	PushIdentity string                  `json:"push_native_identity"`
	PushCaps     map[string]string       `json:"push_native_caps"`
	PushPlan     nativeMDSCephFSPushPlan `json:"push_plan"`
	PushBaseline struct {
		AllowSnaps    bool   `json:"allow_snaps"`
		Section       string `json:"config_section"`
		DaemonMinCaps uint64 `json:"daemon_mds_min_caps_per_client"`
		DaemonMaxCaps uint64 `json:"daemon_mds_max_caps_per_client"`
	} `json:"push_baseline"`
}

type nativeMDSCephFSPushConfig struct {
	Section        string `json:"section"`
	DaemonMinCaps  uint64 `json:"daemon_mds_min_caps_per_client"`
	DaemonMaxCaps  uint64 `json:"daemon_mds_max_caps_per_client"`
	CentralMinCaps string `json:"central_mds_min_caps_per_client"`
	CentralMaxCaps string `json:"central_mds_max_caps_per_client"`
	CentralCount   int    `json:"central_entries"`
}

type nativeMDSCephFSCacheDrop struct {
	RecallReturnCode int64   `json:"client_recall_return_code"`
	RecallMessage    string  `json:"client_recall_message"`
	Recalled         int64   `json:"client_recall_recalled"`
	FlushReturnCode  int64   `json:"flush_journal_return_code"`
	FlushMessage     string  `json:"flush_journal_message"`
	Trimmed          int64   `json:"trim_cache_trimmed"`
	Duration         float64 `json:"duration_s"`
	StartedUTC       string  `json:"started_utc"`
	FinishedUTC      string  `json:"finished_utc"`
}

// mdsCephFSPushControl drives the closed fixture control for this test and
// proves the current owned incarnation in every receipt.
type mdsCephFSPushControl struct {
	dir      string
	token    string
	fresh    nativeMDSFresh
	incarn   int32
	last     int64
	goGIDs   map[uint64]bool
	receipts *[]nativeMDSCephFSReceipt
}

var mdsCephFSPushNativeActions = []string{"push-stat", "push-rename", "push-snapshot-create", "push-snapshot-remove", "push-quota-set", "push-quota-clear"}

func (x *mdsCephFSPushControl) request(ctx context.Context, action, arg string) (nativeMDSCephFSReceipt, error) {
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	var result nativeMDSCephFSReceipt
	path := filepath.Join(x.dir, "mds-cephfs-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	args := []string{x.token, action}
	if arg != "" {
		args = append(args, arg)
	}
	if err := testcluster.RequestControl(ctx, x.dir, "mds-cephfs", "mds-cephfs-completed", args...); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	*x.receipts = append(*x.receipts, result)
	f := x.fresh
	if result.Source != mdsFreshSource || result.Token != x.token || result.RequestID <= x.last || result.Action != action || result.FSID != f.FSID || result.FSCID != f.FSCID ||
		result.FSName != f.FSName || result.MDSGID != f.MDS.GID || result.MDSEndpoint != f.MDS.Endpoint || result.MDSNonce != f.MDS.Nonce || result.MDSIncarnation != x.incarn ||
		result.Rank != 0 || !result.IdentityChecked || !result.Complete || result.Error != "" || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
		return result, fmt.Errorf("native MDS CephFS push receipt scope or unchanged daemon identity differs: %s", result.Error)
	}
	switch {
	case action == "observe":
		if strconv.FormatUint(result.GlobalID, 10) != arg || result.Sessions == nil {
			return result, errors.New("native MDS CephFS push observation scope differs")
		}
	case slices.Contains(mdsCephFSPushNativeActions, action):
		if result.Native == nil || result.Native.NativeGlobalID == 0 || x.goGIDs[result.Native.NativeGlobalID] {
			return result, errors.New("native CephFS push oracle output missing or bound to a Go client")
		}
	case strings.HasPrefix(action, "push-recall-"):
		if result.PushConfig == nil {
			return result, errors.New("native CephFS push recall settings readback missing")
		}
	case action == "push-cache-drop":
		if result.CacheDrop == nil {
			return result, errors.New("native CephFS cache drop result missing")
		}
	}
	if action != "observe" && action != "push-stat" {
		want := "applied"
		if strings.HasSuffix(action, "-off") {
			want = "restored"
		}
		if result.Mutation == nil || result.Mutation.State != want {
			return result, fmt.Errorf("native CephFS push mutation %s did not end %s", action, want)
		}
	}
	x.last = result.RequestID
	return result, nil
}

type pushRequestEvidence struct {
	Name, Session           string
	Op                      uint32
	Transaction             uint64
	MDSMapEpoch             uint32
	FirstSafe, FlushJournal bool
	FirstResult, SafeResult int32
	FirstArrival            uint64
	SafeArrival             uint64
	FirstMicros, SafeMicros int64
	IsDentry, IsTarget      bool
	DentryName              string                  `json:",omitempty"`
	Lease                   *cephmsgr.MDSLeaseStat  `json:",omitempty"`
	Dir, Target             *mdsCephFSInode         `json:",omitempty"`
	Realms                  []cephmsgr.MDSSnapRealm `json:",omitempty"`
	ReaddirEntries          int                     `json:",omitempty"`
	PushBefore, PushAfter   uint64
	Error                   string `json:",omitempty"`
}

// pushArrival is one push or session event an agent recorded, by arrival.
type pushArrival struct {
	Arrival uint64
	Kind    string
	Op      uint32
	Ino     uint64 `json:",omitempty"`
	Seq     uint32 `json:",omitempty"`
	Note    string `json:",omitempty"`
}

func pushArrivals(s cephfsAgentSnapshot, from int) []pushArrival {
	var out []pushArrival
	for _, r := range s.Records[min(from, len(s.Records)):] {
		if r.Arrival != 0 {
			out = append(out, pushArrival{Arrival: r.Arrival, Kind: r.Kind, Op: r.Op, Ino: r.Ino, Seq: r.Seq, Note: strings.TrimSpace(r.Caps + " " + r.Note)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arrival < out[j].Arrival })
	return out
}

func TestCephMDSCephFSPushIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS=1 for owned native CephFS push acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("CephFS push acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSPushOracle
	var fresh nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	startupGID := fresh.MDS.GID
	// Go runs this file before the recovery acceptance, which restarts the
	// owned daemon; the fixture publishes whichever incarnation is current.
	current, err := readMDSCephFSTarget(dir, fresh)
	if err != nil {
		t.Fatal(err)
	}
	fresh.MDS.GID, fresh.MDS.Endpoint, fresh.MDS.Nonce, fresh.MDS.State = current.MDS.GID, current.MDS.Endpoint, current.MDS.Nonce, current.MDS.State
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_KEY_FILE")
	plan := oracle.PushPlan
	_, tokenErr := hex.DecodeString(oracle.Token)
	fsName := fresh.FSName
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || oracle.MDSGID != startupGID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" ||
		identity != "client.mds-cephfs-"+oracle.Token || oracle.ClientIdentity != identity || oracle.ClientCaps["mds"] != "allow rw fsname="+fsName ||
		oracle.PushIdentity != "client.mds-cephfs-push-native-"+oracle.Token || oracle.PushCaps["mon"] != "allow r" || oracle.PushCaps["mds"] != "allow rwps fsname="+fsName || oracle.PushCaps["osd"] != "" ||
		plan.Directory != "ceph-msgr-cephfs-"+oracle.Token+"-push" || plan.LeaseDir != "lease" || plan.Victim != "victim" || plan.Renamed != "renamed" || plan.SnapDir != "snap" ||
		plan.SnapFile != "snap-file" || plan.Snapshot != "ceph-msgr-snap-"+oracle.Token[:8] || plan.QuotaDir != "quota" || plan.QuotaFile != "quota-file" || plan.QuotaMaxBytes < 1<<24 ||
		plan.RecallDir != "recall" || len(plan.RecallFiles) != 24 || plan.MinCaps != 8 || plan.MaxCaps != 12 || plan.DropTimeout != 10 ||
		!oracle.PushBaseline.AllowSnaps || oracle.PushBaseline.Section != "mds."+fresh.MDS.Name || oracle.PushBaseline.DaemonMinCaps <= plan.MaxCaps || oracle.PushBaseline.DaemonMaxCaps <= plan.MaxCaps {
		t.Fatal("owned CephFS push oracle, plan, baseline or identities differ")
	}
	target := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)}
	metadata := func(role string) map[string]string {
		return map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-" + role, "root": "/"}
	}

	var (
		receipts            []nativeMDSCephFSReceipt
		requests            []*pushRequestEvidence
		agentA, agentB      *cephfsCapAgent
		sessionA, sessionB  *cephmsgr.MDSSession
		openA, openB        *mdsSessionEvidence
		facts               = map[string]any{}
		pushes              = map[string][2]uint64{}
		caseTimes           = map[string]int64{}
		started             = time.Now()
		gidA, gidB          uint64
		snapshotCreated     bool
		snapshotRemoved     bool
		quotaSet, quotaGone bool
		recallOn, recallOff bool
	)
	clientA, dialsA := mdsCommandClient(t, ctx, identity, keyFile, fresh.MDS.Endpoint, true, func(o *cephmsgr.Options) { o.EnableCephFS = true })
	clientB, dialsB := mdsCommandClient(t, ctx, identity, keyFile, fresh.MDS.Endpoint, true, func(o *cephmsgr.Options) { o.EnableCephFS = true })
	gidA, gidB = clientA.Snapshot().GlobalID, clientB.Snapshot().GlobalID
	control := &mdsCephFSPushControl{dir: dir, token: oracle.Token, fresh: fresh, incarn: current.MDS.Incarnation, goGIDs: map[uint64]bool{gidA: true, gidB: true}, receipts: &receipts}
	t.Cleanup(func() {
		snapshot := func(a *cephfsCapAgent) *cephfsAgentSnapshot {
			if a == nil {
				return nil
			}
			s := a.snapshot()
			return &s
		}
		data, err := json.Marshal(struct {
			Source       string                    `json:"source_commit"`
			Oracle       nativeMDSCephFSPushOracle `json:"oracle"`
			Target       cephmsgr.MDSTarget        `json:"target"`
			StartTarget  nativeMDSCephFSTarget     `json:"start_target"`
			GlobalIDs    [2]uint64                 `json:"global_ids_go_holder"`
			OpenA        *mdsSessionEvidence       `json:"open_go"`
			OpenB        *mdsSessionEvidence       `json:"open_holder"`
			Requests     []*pushRequestEvidence    `json:"requests"`
			AgentA       *cephfsAgentSnapshot      `json:"agent_go"`
			AgentB       *cephfsAgentSnapshot      `json:"agent_holder"`
			Facts        map[string]any            `json:"facts"`
			PushSequence map[string][2]uint64      `json:"push_sequence_after_leaf_go_holder"`
			Native       []nativeMDSCephFSReceipt  `json:"native"`
			Dials        [2][2]uint32              `json:"mds_osd_dials_go_holder"`
			CaseMillis   map[string]int64          `json:"case_ms"`
			ElapsedMilli int64                     `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, target, current, [2]uint64{gidA, gidB}, openA, openB, requests, snapshot(agentA), snapshot(agentB), facts, pushes, receipts,
			[2][2]uint32{{dialsA.MDS.Load(), dialsA.OSD.Load()}, {dialsB.MDS.Load(), dialsB.OSD.Load()}}, caseTimes, time.Since(started).Milliseconds()})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("MDS CephFS push evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-push-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS push public evidence", err)
		}
	})
	// A failed leaf must not leave owned push state for the later tests in
	// this fixture: restore the recall settings and remove the snapshot and
	// quota (each fixture step is still one-shot and journaled).
	t.Cleanup(func() {
		restore := func(applied, done bool, action string) {
			if !applied || done {
				return
			}
			if _, err := control.request(context.Background(), action, ""); err != nil {
				t.Error("restore owned push state after a failed leaf", action, err)
			}
		}
		restore(recallOn, recallOff, "push-recall-off")
		restore(snapshotCreated, snapshotRemoved, "push-snapshot-remove")
		restore(quotaSet, quotaGone, "push-quota-clear")
	})
	parent := t
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		if sessionA != nil && sessionB != nil {
			pushes[name] = [2]uint64{sessionA.Status().PushSequence, sessionB.Status().PushSequence}
		}
		for _, a := range []*cephfsCapAgent{agentA, agentB} {
			if a != nil {
				if s := a.snapshot(); len(s.Errors) != 0 {
					t.Errorf("capability agent errors after %s: %v", name, s.Errors)
					return false
				}
			}
		}
		return ok
	}
	var epoch uint32
	observe := func(t *testing.T, gid uint64) (nativeMDSCephFSReceipt, *nativeMDSSession) {
		t.Helper()
		r, err := control.request(ctx, "observe", strconv.FormatUint(gid, 10))
		if err != nil {
			t.Fatal("native observation", err)
		}
		epoch = r.Map.Epoch
		return r, r.session(gid)
	}
	native := func(t *testing.T, action, arg string) nativeMDSCephFSReceipt {
		t.Helper()
		r, err := control.request(ctx, action, arg)
		if err != nil {
			t.Fatal("native push action", action, err)
		}
		return r
	}
	// submit sends one request, applies its first reply to the agent and asks
	// for a journal flush after an unsafe reply, as a waiting native client does.
	submit := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, label, name string, request cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace, *cephmsgr.MDSReaddir, *pushRequestEvidence) {
		t.Helper()
		request.MDSMapEpoch = epoch
		e := &pushRequestEvidence{Name: name, Session: label, Op: request.Op, MDSMapEpoch: epoch, PushBefore: s.Status().PushSequence}
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
		if err != nil {
			e.Error = err.Error()
			t.Fatal("first reply", name, err)
		}
		e.FirstMicros, e.FirstSafe, e.FirstResult, e.FirstArrival = first.Received.Sub(begin).Microseconds(), first.Safe, first.Result, first.Arrival
		e.IsDentry, e.IsTarget = first.IsDentry, first.IsTarget
		trace, listing, err := a.applyReply(name, first)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode reply trace", name, err)
		}
		e.Dir, e.Target, e.DentryName, e.Lease = summarizeInode(trace.DirInode), summarizeInode(trace.Target), trace.DentryName, trace.Lease
		if listing != nil {
			e.ReaddirEntries = len(listing.Entries)
		}
		if e.Realms, err = first.DecodeSnapTrace(); err != nil {
			e.Error = err.Error()
			t.Fatal("decode snap trace", name, err)
		}
		safe := first
		if !first.Safe {
			if err := s.FlushJournal(ctx); err != nil {
				t.Fatal("flush journal", name, err)
			}
			e.FlushJournal = true
			if safe, err = call.WaitSafe(waitCtx); err != nil {
				e.Error = err.Error()
				t.Fatal("safe reply", name, err)
			}
		}
		e.SafeMicros, e.SafeResult, e.SafeArrival, e.PushAfter = safe.Received.Sub(begin).Microseconds(), safe.Result, safe.Arrival, s.Status().PushSequence
		if !safe.Safe || safe.Transaction != call.Transaction() || safe.Op != request.Op {
			t.Fatal("safe reply identity differs", name)
		}
		return first, safe, trace, listing, e
	}
	mkdir := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, label string, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, _, e := submit(t, s, a, label, "mkdir "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755),
			Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}})
		if first.Result != 0 || safe.Result != 0 || trace.Target == nil || trace.DentryName != name || trace.DirInode == nil || trace.DirInode.Ino != parent {
			t.Fatalf("mkdir %s differs: %+v", name, e)
		}
		return trace.Target.Ino
	}
	// create makes a file and closes its read-write handle, as an application
	// would; check_caps then drops what a closed file does not retain.
	create := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, label string, parent uint64, name string) uint64 {
		t.Helper()
		first, safe, trace, _, e := submit(t, s, a, label, "create "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}})
		if first.Result != 0 || safe.Result != 0 || trace.Target == nil || trace.DentryName != name || len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != trace.Target.Ino {
			t.Fatalf("create %s differs: %+v", name, e)
		}
		ino := trace.Target.Ino
		a.open(ino, cephFileModeRDWR)
		if _, _, err := a.close(ino, cephFileModeRDWR); err != nil {
			t.Fatal("close created file", name, err)
		}
		return ino
	}
	waitAgent := func(t *testing.T, a *cephfsCapAgent, limit time.Duration, what string, done func(cephfsAgentSnapshot) bool) cephfsAgentSnapshot {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			s := a.snapshot()
			if done(s) {
				return s
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not happen within %v", what, limit)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var pIno, dIno, victimIno, sIno, snapFileIno, qIno, quotaFileIno, rIno, snapID uint64
	recallInos := map[string]uint64{}

	if !leaf("open_push_sessions", func(t *testing.T) {
		for _, side := range []struct {
			client  *cephmsgr.Client
			role    string
			session **cephmsgr.MDSSession
			agent   **cephfsCapAgent
			open    **mdsSessionEvidence
		}{{clientA, "push", &sessionA, &agentA, &openA}, {clientB, "push-holder", &sessionB, &agentB, &openB}} {
			conn, err := side.client.OpenMDS(ctx, target)
			if err != nil {
				t.Fatal("open explicit MDS target", side.role, err)
			}
			if *side.session, err = conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata(side.role), RenewInterval: 2 * time.Second, MaxEvents: 1024}); err != nil {
				t.Fatal("open CephFS session", side.role, err)
			}
			s := *side.session
			parent.Cleanup(func() {
				closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				s.Close(closing)
			})
			*side.agent = startCephFSCapAgent(ctx, s)
			e := describeMDSSession(side.role, identity, s)
			e.Metadata = metadata(side.role)
			*side.open = &e
			status := s.Status()
			if status.State != cephmsgr.MDSSessionOpen || status.PushSequence != 0 || status.ReadOnly || status.Err != nil {
				t.Fatalf("Go CephFS session %s did not open cleanly: %+v", side.role, e)
			}
			for _, bit := range []uint{7, 9, 18, 19, 20} {
				if !cephmsgr.MDSFeatureBitsetHas(status.Open.SupportedFeatures, bit) {
					t.Fatal("MDS OPEN lacks a feature this acceptance relies on", bit)
				}
			}
		}
		for _, side := range []struct {
			gid  uint64
			role string
		}{{gidA, "push"}, {gidB, "push-holder"}} {
			r, n := observe(t, side.gid)
			if err := checkNativeCephFSSession(n, identity, metadata(side.role)); err != nil {
				t.Fatal(err)
			}
			if n.Caps != 0 || n.Leases != 0 || r.blocked(n.Address) {
				t.Fatal("fresh push session already holds caps or leases, or is fenced", side.role, n)
			}
			facts["native_session_address_"+side.role] = n.Address
		}
		pIno = mkdir(t, sessionA, agentA, "go", 1, plan.Directory)
	}) {
		return
	}

	if !leaf("dentry_lease_revoke_released", func(t *testing.T) {
		const hold = 2 * time.Second
		// The holder creates the lease directory and the victim file: as the
		// only client with caps on the directory it becomes its loner and keeps
		// Fsx, so the Go client's later lookup gets no Fs on the directory and
		// the MDS issues a dentry lease instead (Locker::issue_client_lease).
		dIno = mkdir(t, sessionB, agentB, "holder", pIno, plan.LeaseDir)
		victimIno = create(t, sessionB, agentB, "holder", dIno, plan.Victim)
		holderDir, _ := agentB.cap(dIno)
		facts["lease_holder_dir_caps"] = cephCapString(holderDir.Issued)
		first, _, trace, _, e := submit(t, sessionA, agentA, "go", "lookup "+plan.Victim, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: dIno, Path: plan.Victim}})
		goDir, _ := agentA.cap(dIno)
		facts["lease_go_dir_caps"] = cephCapString(goDir.Issued)
		if first.Result != 0 || !first.Safe || !first.IsDentry || trace.Target == nil || trace.Target.Ino != victimIno || trace.DentryName != plan.Victim || trace.Lease == nil {
			t.Fatalf("lookup of the victim differs: %+v", e)
		}
		lease := *trace.Lease
		facts["lookup_lease"] = map[string]any{"mask": lease.Mask, "duration_ms": lease.DurationMS, "seq": lease.Seq, "alternate_name_bytes": len(lease.AlternateName), "reply_arrival": first.Arrival}
		// CEPH_LEASE_VALID is 1|2; a primary dentry also carries PRIMARY_LINK (4).
		if lease.Mask&3 == 0 || lease.Seq == 0 || lease.DurationMS == 0 {
			t.Fatalf("lookup returned no dentry lease (Go dir caps %s, holder dir caps %s): %+v", cephCapString(goDir.Issued), cephCapString(holderDir.Issued), lease)
		}
		before := agentA.snapshot()
		pushBefore := sessionA.Status().PushSequence
		agentA.holdLease(plan.Victim, hold)
		requested := time.Now()
		renamed := native(t, "push-rename", "")
		n := renamed.Native
		if n.Stat == nil || n.SourceStat == nil || !n.Stat.Exists || n.Stat.Ino != victimIno || n.Stat.Mode != 0o100644 || n.SourceStat.Exists || n.SourceStat.Errno != 2 {
			t.Fatalf("native rename result differs: %+v %+v", n.Stat, n.SourceStat)
		}
		after := waitAgent(t, agentA, 10*time.Second, "lease release", func(s cephfsAgentSnapshot) bool {
			return len(s.Leases) > len(before.Leases) && s.Leases[len(s.Leases)-1].Sent
		})
		if len(after.Leases) != len(before.Leases)+1 {
			t.Fatalf("expected exactly one lease revoke, got %d", len(after.Leases)-len(before.Leases))
		}
		ev := after.Leases[len(after.Leases)-1]
		blocked := time.Duration(n.TimingsNS["renamed"])
		facts["lease_revoke"] = map[string]any{"revoke": ev.Revoke, "release": ev.Release, "arrival": ev.Arrival, "pushes_before": ev.PushesBefore, "held_ms": ev.Released.Sub(ev.Received).Milliseconds(),
			"received_after_request_ms": ev.Received.Sub(requested).Milliseconds(), "native_rename_ms": blocked.Milliseconds(), "native_timings_ns": n.TimingsNS,
			"renamed_ino": n.Stat.Ino, "source_errno": n.SourceStat.Errno}
		r := ev.Revoke
		// Locker::revoke_client_leases: mask 1|CEPH_LOCK_DN, the lease's seq,
		// the directory inode and its first snap, CEPH_NOSNAP, the dentry name.
		if r.Action != cephmsgr.MDSLeaseRevoke || r.Mask != 1 || r.Ino != dIno || r.Seq != lease.Seq || r.Last != 0xfffffffffffffffe || r.DentryName != plan.Victim ||
			ev.Release != (cephmsgr.MDSLease{Action: cephmsgr.MDSLeaseRelease, Mask: r.Mask, Ino: r.Ino, First: r.First, Last: r.Last, Seq: r.Seq, DentryName: r.DentryName}) {
			t.Fatalf("lease revoke or its release differs: %+v", ev)
		}
		if ev.Error != "" || ev.Held != hold || ev.Released.Sub(ev.Received) < hold || ev.Arrival <= first.Arrival {
			t.Fatalf("lease release was not held and sent after the lookup: %+v", ev)
		}
		// The native rename waited for the held RELEASE (it started before the
		// revoke reached Go) and finished long before the 30 s lease expiry.
		if blocked < hold-50*time.Millisecond || blocked > hold+10*time.Second {
			t.Fatalf("native rename took %v around a %v held release", blocked, hold)
		}
		if sessionA.Status().PushSequence <= pushBefore {
			t.Fatal("the lease revoke was not counted in the push sequence")
		}
		facts["lease_leaf_arrivals_go"] = pushArrivals(after, len(before.Records))
		facts["lease_leaf_arrivals_holder"] = pushArrivals(agentB.snapshot(), 0)
	}) {
		return
	}

	if !leaf("snapshot_realm_split_push", func(t *testing.T) {
		sIno = mkdir(t, sessionA, agentA, "go", pIno, plan.SnapDir)
		snapFileIno = create(t, sessionA, agentA, "go", sIno, plan.SnapFile)
		beforeA, beforeB := agentA.snapshot(), agentB.snapshot()
		pushA, pushB := sessionA.Status().PushSequence, sessionB.Status().PushSequence
		r := native(t, "push-snapshot-create", "")
		snapshotCreated = true
		if r.Native.Stat == nil || !r.Native.Stat.Exists || r.Native.Stat.Mode&0o170000 != 0o040000 {
			t.Fatalf("native snapshot creation differs: %+v", r.Native.Stat)
		}
		isSplit := func(e cephfsSnapEvent) bool { return e.Op == cephmsgr.MDSSnapOpSplit && e.Split == sIno }
		isGlobal := func(e cephfsSnapEvent) bool { return e.Op == cephmsgr.MDSSnapOpUpdate && e.Split == 3 }
		afterA := waitAgent(t, agentA, 10*time.Second, "snapshot split push", func(s cephfsAgentSnapshot) bool {
			return slices.ContainsFunc(s.Snaps[len(beforeA.Snaps):], isSplit) && slices.ContainsFunc(s.Snaps[len(beforeA.Snaps):], isGlobal)
		})
		afterB := waitAgent(t, agentB, 10*time.Second, "global realm update to the holder", func(s cephfsAgentSnapshot) bool {
			return slices.ContainsFunc(s.Snaps[len(beforeB.Snaps):], isGlobal)
		})
		events := afterA.Snaps[len(beforeA.Snaps):]
		split := events[slices.IndexFunc(events, isSplit)]
		global := events[slices.IndexFunc(events, isGlobal)]
		facts["snapshot_native"] = map[string]any{"stat": r.Native.Stat, "timings_ns": r.Native.TimingsNS}
		facts["snapshot_events_go"], facts["snapshot_events_holder"] = events, afterB.Snaps[len(beforeB.Snaps):]
		facts["snapshot_leaf_arrivals_go"] = pushArrivals(afterA, len(beforeA.Records))
		if len(split.Realms) < 2 || split.Realms[0].Ino != sIno || split.Realms[0].Parent != 1 || len(split.Realms[0].Snaps) != 1 || split.Realms[len(split.Realms)-1].Ino != 1 ||
			!slices.Contains(split.SplitInos, sIno) || !slices.Contains(split.SplitInos, snapFileIno) || len(split.SplitRealms) != 0 || split.Error != "" {
			t.Fatalf("snapshot split push differs: %+v", split)
		}
		if !slices.Contains(split.Moved, sIno) || !slices.Contains(split.Moved, snapFileIno) {
			t.Fatalf("the split did not move the held caps into the new realm: %+v", split)
		}
		for _, ino := range []uint64{sIno, snapFileIno} {
			if c, _ := agentA.cap(ino); c.Realm != sIno {
				t.Fatal("cap realm was not moved to the snapshot realm", ino, c.Realm)
			}
		}
		// MDCache::send_snap_update notifies the global realm before
		// do_realm_invalidate_and_update_notify sends the split.
		if len(global.Realms) != 1 || global.Realms[0].Ino != 3 || global.Arrival >= split.Arrival {
			t.Fatalf("global realm update differs or did not precede the split: %+v / %+v", global, split)
		}
		// The snapshot directory is the snapped directory inode.
		if r.Native.Stat.Ino != sIno {
			t.Fatalf("native snapshot stat names inode %d, not the directory %d", r.Native.Stat.Ino, sIno)
		}
		snapID = split.Realms[0].Snaps[0]
		facts["snapshot_id"], facts["snapshot_realm"] = snapID, split.Realms[0]
		if sessionA.Status().PushSequence < pushA+2 || sessionB.Status().PushSequence < pushB+1 {
			t.Fatal("snapshot pushes were not counted in the push sequences")
		}
	}) {
		return
	}

	if !leaf("quota_push_on_native_setxattr", func(t *testing.T) {
		qIno = mkdir(t, sessionA, agentA, "go", pIno, plan.QuotaDir)
		quotaFileIno = create(t, sessionA, agentA, "go", qIno, plan.QuotaFile)
		before := agentA.snapshot()
		pushA := sessionA.Status().PushSequence
		r := native(t, "push-quota-set", "")
		quotaSet = true
		readback, _ := hex.DecodeString(r.Native.ValueHex)
		if r.Native.Stat == nil || r.Native.Stat.Ino != qIno || string(readback) != strconv.FormatInt(plan.QuotaMaxBytes, 10) || r.Native.QuotaAbsent {
			t.Fatalf("native quota readback differs: %q %+v", readback, r.Native.Stat)
		}
		isSplit := func(e cephfsSnapEvent) bool { return e.Op == cephmsgr.MDSSnapOpSplit && e.Split == qIno }
		after := waitAgent(t, agentA, 10*time.Second, "quota push and quota realm split", func(s cephfsAgentSnapshot) bool {
			return slices.ContainsFunc(s.Quotas[len(before.Quotas):], func(q cephfsQuotaEvent) bool { return q.Quota.Ino == qIno }) &&
				slices.ContainsFunc(s.Snaps[len(before.Snaps):], isSplit)
		})
		quotas := after.Quotas[len(before.Quotas):]
		q := quotas[slices.IndexFunc(quotas, func(q cephfsQuotaEvent) bool { return q.Quota.Ino == qIno })]
		snaps := after.Snaps[len(before.Snaps):]
		split := snaps[slices.IndexFunc(snaps, isSplit)]
		facts["quota_events_go"], facts["quota_split_go"] = quotas, split
		facts["quota_leaf_arrivals_go"] = pushArrivals(after, len(before.Records))
		facts["quota_native"] = map[string]any{"readback": string(readback), "stat": r.Native.Stat}
		if q.Quota.MaxBytes != plan.QuotaMaxBytes || q.Quota.MaxFiles != 0 || !q.Known || q.Quota.RBytes < 0 || q.Quota.RFiles < 0 || q.Quota.RSubdirs < 1 {
			t.Fatalf("quota push differs: %+v", q)
		}
		if c, _ := agentA.cap(qIno); c.Quota == nil || c.Quota.MaxBytes != plan.QuotaMaxBytes {
			t.Fatal("the quota push was not applied to the cached directory")
		}
		// Setting a quota on a directory without a realm creates one: the MDS
		// broadcasts the quota while projecting the change and sends the realm
		// split after journaling it.
		if len(split.Realms) < 2 || split.Realms[0].Ino != qIno || split.Realms[0].Parent != 1 || len(split.Realms[0].Snaps) != 0 || !slices.Contains(split.SplitInos, qIno) ||
			!slices.Contains(split.SplitInos, quotaFileIno) || split.Error != "" || q.Arrival >= split.Arrival {
			t.Fatalf("quota realm split differs or did not follow the quota push: %+v / %+v", split, q)
		}
		if sessionA.Status().PushSequence < pushA+2 {
			t.Fatal("quota pushes were not counted in the push sequence")
		}
	}) {
		return
	}

	if !leaf("recall_state_released_to_target", func(t *testing.T) {
		rIno = mkdir(t, sessionA, agentA, "go", pIno, plan.RecallDir)
		for _, name := range plan.RecallFiles {
			recallInos[name] = create(t, sessionA, agentA, "go", rIno, name)
		}
		_, n := observe(t, gidA)
		if n == nil {
			t.Fatal("Go session vanished")
		}
		liveBefore, nativeBefore := agentA.liveCaps(), n.Caps
		facts["recall_caps_before_native_agent"] = []int64{nativeBefore, int64(liveBefore)}
		if nativeBefore <= int64(plan.MaxCaps) {
			t.Fatalf("Go holds %d caps, not more than the recall maximum %d", nativeBefore, plan.MaxCaps)
		}
		_, nb := observe(t, gidB)
		if nb == nil || nb.Caps > int64(plan.MinCaps) {
			t.Fatal("the holder session would also be recalled", nb)
		}
		before := agentA.snapshot()
		requested := time.Now()
		on := native(t, "push-recall-on", "")
		recallOn = true
		configured := time.Now()
		c := on.PushConfig
		facts["recall_config_on"] = c
		if c.DaemonMinCaps != plan.MinCaps || c.DaemonMaxCaps != plan.MaxCaps || c.CentralMinCaps != strconv.FormatUint(plan.MinCaps, 10) || c.CentralMaxCaps != strconv.FormatUint(plan.MaxCaps, 10) ||
			c.Section != oracle.PushBaseline.Section || c.CentralCount != 2 {
			t.Fatalf("owned recall settings differ: %+v", c)
		}
		after := waitAgent(t, agentA, 30*time.Second, "RECALL_STATE", func(s cephfsAgentSnapshot) bool {
			return len(s.Recalls) > len(before.Recalls) && !s.Recalls[len(before.Recalls)].Sent.IsZero()
		})
		recall := after.Recalls[len(before.Recalls)]
		samples := []int64{nativeBefore}
		for range 15 {
			_, n = observe(t, gidA)
			if n == nil {
				t.Fatal("Go session vanished after the recall")
			}
			samples = append(samples, n.Caps)
			if n.Caps <= int64(plan.MaxCaps) {
				break
			}
		}
		final := agentA.snapshot()
		facts["recall_event"], facts["recall_native_num_caps"], facts["recall_agent_live_after"] = recall, samples, agentA.liveCaps()
		facts["recall_events_in_leaf"] = len(final.Recalls) - len(before.Recalls)
		// The fixture returns only after it has read both values back from the
		// daemon, so the upkeep thread's recall can precede that return.
		facts["recall_received_after_request_ms"], facts["recall_received_before_config_return_ms"] = recall.Received.Sub(requested).Milliseconds(), configured.Sub(recall.Received).Milliseconds()
		facts["recall_release_after_receipt_us"] = recall.Sent.Sub(recall.Received).Microseconds()
		facts["recall_leaf_arrivals_go"] = pushArrivals(final, len(before.Records))
		// Server::recall_client_state asks for mds_min_caps_per_client while
		// the session holds fewer than mds_recall_max_caps.
		if recall.MaxCaps != uint32(plan.MinCaps) || recall.Error != "" || recall.Before != liveBefore || recall.After > int(recall.MaxCaps) || len(recall.Released) != recall.Before-recall.After {
			t.Fatalf("RECALL_STATE or its trim differs: %+v", recall)
		}
		if last := samples[len(samples)-1]; last >= nativeBefore || last > int64(plan.MaxCaps) {
			t.Fatalf("native num_caps did not drop to the recall maximum: %v", samples)
		}
		off := native(t, "push-recall-max-off", "")
		facts["recall_config_max_off"] = off.PushConfig
		if off.PushConfig.DaemonMaxCaps != oracle.PushBaseline.DaemonMaxCaps || off.PushConfig.DaemonMinCaps != plan.MinCaps || off.PushConfig.CentralMaxCaps != "" {
			t.Fatalf("mds_max_caps_per_client was not restored: %+v", off.PushConfig)
		}
	}) {
		return
	}

	if !leaf("flushmsg_acknowledged_after_cache_drop", func(t *testing.T) {
		first, _, trace, listing, e := submit(t, sessionA, agentA, "go", "readdir "+plan.RecallDir, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpReaddir,
			Args: cephfsReaddirArgs(0, 1024, 0, cephReaddirReplyBitflags, 0), Path: cephmsgr.MDSFilePath{Ino: rIno}})
		if first.Result != 0 || listing == nil || len(listing.Entries) != len(plan.RecallFiles) || trace.Target == nil || trace.Target.Ino != rIno {
			t.Fatalf("readdir of the recall directory differs: %+v", e)
		}
		_, n := observe(t, gidA)
		if n == nil || n.Caps <= int64(plan.MinCaps) {
			t.Fatal("Go does not hold more caps than the recall target before the cache drop", n)
		}
		nativeBefore, liveBefore := n.Caps, agentA.liveCaps()
		before := agentA.snapshot()
		drop := native(t, "push-cache-drop", "").CacheDrop
		facts["cache_drop"] = drop
		after := waitAgent(t, agentA, 20*time.Second, "RECALL_STATE and FLUSHMSG", func(s cephfsAgentSnapshot) bool {
			return len(s.Recalls) > len(before.Recalls) && len(s.Flushes) > len(before.Flushes) && !s.Flushes[len(before.Flushes)].Acked.IsZero()
		})
		recall, flush := after.Recalls[len(before.Recalls)], after.Flushes[len(before.Flushes)]
		_, n = observe(t, gidA)
		if n == nil {
			t.Fatal("Go session vanished after the cache drop")
		}
		facts["flush_recall_event"], facts["flush_event"] = recall, flush
		facts["flush_native_num_caps"], facts["flush_agent_live"] = []int64{nativeBefore, n.Caps}, []int{liveBefore, agentA.liveCaps()}
		facts["flush_leaf_arrivals_go"] = pushArrivals(after, len(before.Records))
		// Server::flush_session sends FLUSHMSG with the session's push
		// sequence; the agent counted every push that arrived before it.
		if flush.Sequence != flush.AgentPushes || flush.Error != "" || flush.Arrival <= recall.Arrival || flush.Acked.Before(recall.Sent) || flush.Sequence == 0 {
			t.Fatalf("FLUSHMSG sequence, order or acknowledgement differs: %+v after %+v", flush, recall)
		}
		if recall.MaxCaps != uint32(plan.MinCaps) || recall.Error != "" || recall.After > int(recall.MaxCaps) || len(recall.Released) == 0 {
			t.Fatalf("cache drop RECALL_STATE or its trim differs: %+v", recall)
		}
		// C_Drop_Cache waits for the FLUSHMSG gather with a 1 s timer before it
		// recalls again; only finish_flush_session (our FLUSHMSG_ACK, a stale
		// session or a kill) completes the gather earlier. A run shorter than
		// that timer shows the MDS accepted the acknowledgement.
		if drop.RecallReturnCode != 0 || drop.FlushReturnCode != 0 || drop.Recalled <= 0 || drop.Duration >= 1 {
			t.Fatalf("native cache drop did not complete its recall through the acknowledged flush: %+v", drop)
		}
		if n.Caps > int64(plan.MinCaps) || n.Caps >= nativeBefore {
			t.Fatalf("native num_caps did not drop to the recall target: %d -> %d", nativeBefore, n.Caps)
		}
		if sessionA.Status().PushSequence < flush.Sequence {
			t.Fatal("the library push sequence is behind the MDS's")
		}
		recallOff = true
		off := native(t, "push-recall-off", "")
		facts["recall_config_off"] = off.PushConfig
		if off.PushConfig.DaemonMinCaps != oracle.PushBaseline.DaemonMinCaps || off.PushConfig.DaemonMaxCaps != oracle.PushBaseline.DaemonMaxCaps || off.PushConfig.CentralCount != 0 {
			t.Fatalf("recall settings were not restored: %+v", off.PushConfig)
		}
	}) {
		return
	}

	t.Run("cleanup_snapshot_quota_namespace_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_snapshot_quota_namespace_release_close"] = time.Since(begin).Milliseconds() }()
		before := agentA.snapshot()
		snapshotRemoved = true
		removed := native(t, "push-snapshot-remove", "")
		if removed.Native.Stat == nil || removed.Native.Stat.Exists || removed.Native.Stat.Errno != 2 {
			t.Fatalf("native snapshot removal differs: %+v", removed.Native.Stat)
		}
		isDestroy := func(e cephfsSnapEvent) bool { return e.Op == cephmsgr.MDSSnapOpDestroy && e.Split == 3 }
		isSplit := func(e cephfsSnapEvent) bool { return e.Op == cephmsgr.MDSSnapOpSplit && e.Split == sIno }
		after := waitAgent(t, agentA, 10*time.Second, "snapshot removal pushes", func(s cephfsAgentSnapshot) bool {
			return slices.ContainsFunc(s.Snaps[len(before.Snaps):], isDestroy) && slices.ContainsFunc(s.Snaps[len(before.Snaps):], isSplit)
		})
		events := after.Snaps[len(before.Snaps):]
		destroy, split := events[slices.IndexFunc(events, isDestroy)], events[slices.IndexFunc(events, isSplit)]
		facts["snapshot_remove_events_go"] = events
		if len(split.Realms) == 0 || split.Realms[0].Ino != sIno || len(split.Realms[0].Snaps) != 0 || len(split.SplitInos) != 0 || len(destroy.Realms) != 1 || destroy.Realms[0].Ino != 3 {
			t.Fatalf("snapshot removal pushes differ: %+v / %+v", destroy, split)
		}
		facts["snapshot_removed_id_in_destroy_trace"] = slices.Contains(destroy.Realms[0].Snaps, snapID)
		before = agentA.snapshot()
		quotaGone = true
		cleared := native(t, "push-quota-clear", "")
		if cleared.Native.Stat == nil || cleared.Native.Stat.Ino != qIno {
			t.Fatalf("native quota clear differs: %+v", cleared.Native)
		}
		after = waitAgent(t, agentA, 10*time.Second, "quota clear push", func(s cephfsAgentSnapshot) bool {
			return slices.ContainsFunc(s.Quotas[len(before.Quotas):], func(q cephfsQuotaEvent) bool { return q.Quota.Ino == qIno && q.Quota.MaxBytes == 0 })
		})
		facts["quota_clear_events_go"], facts["quota_clear_native_absent"] = after.Quotas[len(before.Quotas):], cleared.Native.QuotaAbsent
		unlink := func(parent uint64, name string) {
			first, safe, _, _, e := submit(t, sessionA, agentA, "go", "unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}})
			if first.Result != 0 || safe.Result != 0 {
				t.Fatalf("unlink %s differs: %+v", name, e)
			}
		}
		rmdir := func(parent uint64, name string) {
			first, safe, _, _, e := submit(t, sessionA, agentA, "go", "rmdir "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}})
			if first.Result != 0 || safe.Result != 0 {
				t.Fatalf("rmdir %s differs: %+v", name, e)
			}
		}
		unlink(dIno, plan.Renamed)
		unlink(sIno, plan.SnapFile)
		unlink(qIno, plan.QuotaFile)
		for _, name := range slices.Sorted(maps.Keys(recallInos)) {
			unlink(rIno, name)
		}
		for _, name := range []string{plan.LeaseDir, plan.SnapDir, plan.QuotaDir, plan.RecallDir} {
			rmdir(pIno, name)
		}
		rmdir(1, plan.Directory)
		gone := native(t, "push-stat", "dir")
		if gone.Native.Stat == nil || gone.Native.Stat.Exists || gone.Native.Stat.Errno != 2 {
			t.Fatal("native stat still finds the push directory", gone.Native.Stat)
		}
		for _, side := range []struct {
			gid     uint64
			role    string
			session *cephmsgr.MDSSession
			agent   *cephfsCapAgent
		}{{gidA, "push", sessionA, agentA}, {gidB, "push-holder", sessionB, agentB}} {
			_, n := observe(t, side.gid)
			if err := checkNativeCephFSSession(n, identity, metadata(side.role)); err != nil {
				t.Fatal(err)
			}
			beforeCaps := n.Caps
			items, barrier := side.agent.releaseAll()
			releaseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			err := side.session.ReleaseCaps(releaseCtx, items, barrier)
			stop()
			if err != nil {
				t.Fatal("release caps", side.role, err)
			}
			samples := []int64{beforeCaps}
			for range 8 {
				if _, n = observe(t, side.gid); n == nil {
					t.Fatal("session vanished before close", side.role)
				}
				samples = append(samples, n.Caps)
				if n.Caps == 0 {
					break
				}
			}
			facts["release_"+side.role] = map[string]any{"items": len(items), "barrier": barrier, "num_caps": samples}
			if samples[len(samples)-1] >= beforeCaps && beforeCaps != 0 || len(items) == 0 {
				t.Fatal("native num_caps did not drop after the release", side.role, samples, len(items))
			}
			closing, stopClose := context.WithTimeout(ctx, 15*time.Second)
			closeErr := side.session.Close(closing)
			stopClose()
			if closeErr != nil || side.session.Status().State != cephmsgr.MDSSessionClosed || !errors.Is(side.session.Err(), cephmsgr.ErrMDSSessionClosed) {
				t.Fatal("Close was not confirmed by the MDS", side.role, closeErr, side.session.Err())
			}
			select {
			case <-side.agent.done:
			case <-time.After(10 * time.Second):
				t.Fatal("capability agent did not observe the session end", side.role)
			}
			r, err := control.request(ctx, "observe", strconv.FormatUint(side.gid, 10))
			if err != nil {
				t.Fatal(err)
			}
			if r.session(side.gid) != nil {
				t.Fatal("closed session remains in the native session map", side.role)
			}
			s := side.agent.snapshot()
			if len(s.Errors) != 0 || s.Pending != 0 || !strings.Contains(s.EndError, cephmsgr.ErrMDSSessionClosed.Error()) {
				t.Fatal("capability agent ended with errors or unapplied pushes", side.role, s.Errors, s.Pending, s.EndError)
			}
			var kinds []string
			for _, r := range s.Records {
				kinds = append(kinds, r.Kind)
			}
			sort.Strings(kinds)
			facts["agent_record_kinds_"+side.role], facts["agent_pushes_"+side.role] = slices.Compact(kinds), s.Pushes
			if status := side.session.Status(); status.PushSequence != s.Pushes {
				t.Fatal("the agent's counted pushes differ from the session push sequence", side.role, s.Pushes, status.PushSequence)
			}
		}
		if dialsA.OSD.Load() != 0 || dialsB.OSD.Load() != 0 || dialsA.MDS.Load() != 1 || dialsB.MDS.Load() != 1 {
			t.Fatal("a push client dialed an OSD or redialed the MDS", dialsA.MDS.Load(), dialsA.OSD.Load(), dialsB.MDS.Load(), dialsB.OSD.Load())
		}
	})
}
