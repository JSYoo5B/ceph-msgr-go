package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// ranksHeldFlockEvidence is the retained record of the held-lock standby
// takeover.
type ranksHeldFlockEvidence struct {
	GlobalID         uint64
	Directory, File  string
	Ino              uint64
	Lock             cephfsFilelock
	ReconnectBlobHex string
	// Calls are the SETFILELOCK calls, recorded once Submit accepted them.
	Calls []heldFlockCall
	// Requests is every request the test submitted, in order.
	Requests            []ranksHeldFlockRequest
	ProbeBefore         []nativeMDSCephFSRanksReceipt
	ProbeAfter          []nativeMDSCephFSRanksReceipt
	GetBefore           *cephfsFilelock
	GetAfter            *cephfsFilelock
	Armed, Standby      *nativeMDSCephFSRanksDaemon
	NewRank0            *nativeMDSCephFSRanksDaemon
	Adopted             *nativeMDSCephFSRanksTopology
	Rank0Lost           cephmsgr.MDSSessionStatus
	Reconnected         cephmsgr.MDSSessionStatus
	ReconnectCaps       []cephmsgr.MDSCapReconnect
	Timeline            []ranksStatusSample
	NamespaceRemoved    bool
	SessionClosedNative bool
}

// ranksHeldFlockRequest is one submitted request.
type ranksHeldFlockRequest struct {
	Name   string
	Op     uint32
	TID    uint64
	Result int32
	Safe   bool
	Error  string `json:",omitempty"`
}

// TestCephMDSCephFSRanksHeldFlockTakeoverIntegration holds one exclusive
// whole-file FLOCK lock across a standby takeover of rank 0: the owned
// two-active-rank fixture fails exactly the proven rank-0 daemon and its
// standby, another daemon process, takes the rank over. The Go client's rank-0
// session becomes Lost and reconnects to the standby in up:reconnect with the
// lock blob on the lock file's capability, without a second SETFILELOCK.
// After adoption a native nonblocking exclusive probe (a mount with sessions
// on both ranks) is refused and GETFILELOCK names the original owner; after
// one explicit unlock the probe acquires and releases. The lock file is in
// rank 0's subtree; rank 1 holds no session of this client. Not covered: a
// lock in rank 1's subtree, a standby-replay follower's takeover and several
// clients.
//
//scenario:row mds-cephfs-ranks-heldflock.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsranksheldflocktakeover key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_ranks=1 capabilities=mds-ranks-held-flock
//scenario:row mds-cephfs-ranks-heldflock.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfsranksheldflocktakeover key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_ranks=1 capabilities=mds-ranks-held-flock
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated fails the rank-0 MDS with mds fail for a standby takeover; rank layout not restored
func TestCephMDSCephFSRanksHeldFlockTakeoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1 for owned held-lock standby takeover acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("held-lock standby takeover requires its owned legacy/no-HASHPSPOOL two-rank fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	if oracle.Source != mdsFreshSource || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || identity != "client.mds-cephfs-ranks-"+oracle.Token || oracle.ClientIdentity != identity ||
		plan.Directory != "ceph-msgr-ranks-"+oracle.Token || plan.Migrate != "mig" || !slices.Equal(plan.MigrateFiles, []string{"file-0", "file-1", "file-2"}) || !topo.stable() ||
		topo.Ranks[0].GID != fresh.MDS.GID || len(topo.Standbys) != 1 {
		t.Fatal("owned two-rank CephFS oracle, plan, identity or topology differ")
	}
	control := &mdsCephFSRanksControl{dir: dir, oracle: oracle, topology: topo, shape: nativeMDSCephFSRanksShape{Ranks: 2}}
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-heldflock", "root": "/"}
	options := cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024}
	dials := newRecoveryDials(false, topo.Ranks[0].Endpoint, topo.Standbys[0].Endpoint)
	client := recoveryClient(t, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS = true, true })
	gid := client.Snapshot().GlobalID
	control.goGID = gid
	target := plan.Migrate + "/" + plan.MigrateFiles[0]
	e := &ranksHeldFlockEvidence{GlobalID: gid, Directory: plan.Directory, File: target}
	caseTimes := map[string]int64{}
	var (
		conn    *cephmsgr.MDSConnection
		session *cephmsgr.MDSSession
		agent   *cephfsCapAgent
		epoch   uint32
		dirIno  uint64
		migIno  uint64
		realms  = map[uint64]cephmsgr.MDSSnapRealm{}
		started = time.Now()
	)
	t.Cleanup(func() {
		var snapshot *cephfsAgentSnapshot
		if agent != nil {
			s := agent.snapshot()
			snapshot = &s
		}
		data, err := json.Marshal(map[string]any{"source_commit": mdsFreshSource, "held_flock": e, "agent": snapshot, "native": control.receipts,
			"mds_dials": dials.attempts(), "osd_dials": dials.OSD.Load(), "case_ms": caseTimes, "elapsed_ms": time.Since(started).Milliseconds()})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("held-lock standby takeover evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-ranks-heldflock-results.json"), data)
		}
		if err != nil {
			t.Error("preserve held-lock standby takeover evidence", err)
		}
	})
	t.Cleanup(func() {
		if session != nil {
			closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			session.Close(closing)
		}
	})
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
	// submit sends one request on the rank-0 session, applies its first
	// reply and waits for the safe one. accepted is called once Submit
	// accepted the call.
	submit := func(t *testing.T, name string, r cephmsgr.MDSRequest, accepted func(uint64)) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		r.MDSMapEpoch = epoch
		rec := ranksHeldFlockRequest{Name: name, Op: r.Op}
		call, err := session.Submit(ctx, r)
		if err != nil {
			rec.Error = err.Error()
			e.Requests = append(e.Requests, rec)
			t.Fatal("submit", name, err)
		}
		rec.TID = call.Transaction()
		if accepted != nil {
			accepted(call.Transaction())
		}
		wait, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		first, err := call.Wait(wait)
		if err == nil && !first.Safe {
			if err = session.FlushJournal(wait); err == nil {
				var safe cephmsgr.MDSReply
				safe, err = call.WaitSafe(wait)
				if err == nil && safe.Result != first.Result {
					err = fmt.Errorf("safe result %d differs from %d", safe.Result, first.Result)
				}
			}
		}
		rec.Result, rec.Safe = first.Result, first.Safe
		if err != nil {
			rec.Error = err.Error()
		}
		e.Requests = append(e.Requests, rec)
		if err != nil || first.Result != 0 {
			t.Fatal("reply", name, first.Result, err)
		}
		trace, _, err := agent.applyReply(name, first)
		if err != nil {
			t.Fatal(err)
		}
		rs, err := first.DecodeSnapTrace()
		if err != nil {
			t.Fatal(err)
		}
		for _, realm := range rs {
			realms[realm.Ino] = realm
		}
		return first, trace
	}
	setLock := func(t *testing.T, name string, kind byte) {
		t.Helper()
		submit(t, name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Path: cephmsgr.MDSFilePath{Ino: e.Ino}, Args: cephfsFlockArgs(kind, cephFlockGoOwner)}, func(tid uint64) {
			e.Calls = append(e.Calls, heldFlockCall{Name: name, Rule: cephLockRuleFlock, Client: gid, Owner: cephFlockGoOwner, Type: kind, TID: tid, Outcome: "reply result 0"})
		})
	}
	getLock := func(t *testing.T, name string) *cephfsFilelock {
		t.Helper()
		first, _ := submit(t, name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock, Path: cephmsgr.MDSFilePath{Ino: e.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner+1)}, nil)
		got, err := cephfsDecodeFilelock(first.Extra)
		if err != nil || got != e.Lock {
			t.Fatalf("GETFILELOCK (%s) does not identify the owned Go lock %+v: %+v %v", name, e.Lock, got, err)
		}
		return &got
	}
	var baseline *nativeMDSCephFSFlockState
	probe := func(t *testing.T, wantErrno int, into *[]nativeMDSCephFSRanksReceipt) {
		t.Helper()
		r := native(t, "flock-ex-nb", target)
		*into = append(*into, r)
		if err := checkRanksHeldFlockProbe(r, gid, e.Ino, wantErrno, &baseline); err != nil {
			t.Fatal(err)
		}
	}
	leaf := func(name string, run func(*testing.T)) bool {
		at := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(at).Milliseconds()
		if agent != nil {
			if s := agent.snapshot(); len(s.Errors) != 0 {
				t.Error("held-lock takeover cap agent", s.Errors)
				ok = false
			}
		}
		return ok
	}

	if !leaf("hold_flock_on_rank0", func(t *testing.T) {
		var err error
		if conn, err = client.OpenMDS(ctx, topo.Ranks[0].target(fresh.FSCID, 0)); err != nil {
			t.Fatal(err)
		}
		if session, err = conn.OpenSession(ctx, options); err != nil {
			t.Fatal(err)
		}
		agent = startCephFSCapAgent(ctx, session)
		observed := native(t, "observe", strconv.FormatUint(gid, 10))
		if err := checkNativeCephFSSession(observed.view(0).session(gid), identity, metadata); err != nil {
			t.Fatal(err)
		}
		_, tr := submit(t, "mkdir dir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}}, nil)
		if tr.Target == nil {
			t.Fatal("mkdir trace missing")
		}
		dirIno = tr.Target.Ino
		_, tr = submit(t, "mkdir mig", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.Migrate}}, nil)
		if tr.Target == nil {
			t.Fatal("mkdir trace missing")
		}
		migIno = tr.Target.Ino
		first, tr := submit(t, "create lock file", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: migIno, Path: plan.MigrateFiles[0]}}, nil)
		if tr.Target == nil || !cephfsCreated(first.Extra, tr.Target.Ino) {
			t.Fatal("lock file create differs")
		}
		e.Ino = tr.Target.Ino
		agent.open(e.Ino, cephFileModeRDWR)
		e.Lock = cephfsFilelock{Client: gid, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
		setLock(t, "flock-exclusive-acquire", cephFlockExclusive)
		probe(t, 11, &e.ProbeBefore)
		e.GetBefore = getLock(t, "flock get before takeover")
	}) {
		return
	}

	if !leaf("standby_takeover_restores_flock", func(t *testing.T) {
		armed := native(t, "takeover-arm").Takeover
		e.Armed, e.Standby = &armed.Armed, &armed.Standby
		lost := session
		if _, err := control.request(ctx, "takeover-fire"); err != nil {
			t.Fatal("native MDS fail", err)
		}
		fired := time.Now()
		select {
		case <-lost.Done():
		case <-time.After(45 * time.Second):
			t.Fatal("the rank-0 session did not end after its daemon failed")
		}
		e.Rank0Lost = lost.Status()
		if e.Rank0Lost.State != cephmsgr.MDSSessionLost {
			t.Fatalf("rank-0 session ended %d, not Lost: %v", e.Rank0Lost.State, lost.Err())
		}
		previous := agent
		select {
		case <-previous.done:
		case <-time.After(10 * time.Second):
			t.Fatal("the rank-0 agent did not observe the lost session")
		}
		deadline := time.Now().Add(60 * time.Second)
		for e.NewRank0 == nil {
			r, err := control.request(ctx, "takeover-status")
			if err != nil {
				t.Fatal(err)
			}
			s := r.Status
			sample := ranksStatusSample{AfterFireMillis: time.Since(fired).Milliseconds(), FSMapEpoch: s.Topology.FSMapEpoch, Standbys: s.Topology.Standbys,
				Failed: s.Topology.Failed, ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist}
			if len(s.Topology.Ranks) == 2 {
				sample.Rank0, sample.Rank1 = s.Topology.Ranks[0], s.Topology.Ranks[1]
			}
			e.Timeline = append(e.Timeline, sample)
			if r0 := sample.Rank0; r0 != nil && r0.GID == armed.Standby.GID && r0.Name == armed.Standby.Name && r0.State == "up:reconnect" {
				e.NewRank0 = r0
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
		if e.NewRank0.Name == armed.Armed.Name || e.NewRank0.Endpoint != armed.Standby.Endpoint || e.NewRank0.Incarnation == armed.Armed.Incarnation {
			t.Fatal("rank 0 is not the standby daemon in a new incarnation", e.NewRank0, armed.Standby)
		}
		dials.allowMDS(e.NewRank0.Endpoint)
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		caps, realmState, err := previous.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		// The held lock travels only in this caller-built record; no
		// SETFILELOCK is sent again.
		at := slices.IndexFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == e.Ino })
		if at < 0 || len(caps[at].Flock) != 0 {
			t.Fatal("the lock file has no capability to reconnect, or it already carries a lock blob")
		}
		blob := cephfsFilelockBlob(nil, []cephfsFilelock{e.Lock})
		caps[at].Flock, e.ReconnectBlobHex, e.ReconnectCaps = blob, hex.EncodeToString(blob), caps
		if conn, err = client.OpenMDS(ctx, e.NewRank0.target(fresh.FSCID, 0)); err != nil {
			t.Fatal("open the new rank-0 daemon", err)
		}
		reconnecting, stop := context.WithTimeout(ctx, 30*time.Second)
		reopened, calls, err := conn.ReconnectSession(reconnecting, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState}, options)
		stop()
		if err != nil || len(calls) != 0 {
			t.Fatal("CephFS reconnect to the standby that took rank 0", err)
		}
		session = reopened
		agent = startCephFSCapAgentFrom(ctx, reopened, previous)
		e.Reconnected = reopened.Status()
		if e.Reconnected.State != cephmsgr.MDSSessionOpen || e.Reconnected.ReconnectedFrom != e.Rank0Lost.Generation || e.Reconnected.GlobalID != gid {
			t.Fatalf("reconnected rank-0 session differs: %+v", e.Reconnected)
		}
		deadline = time.Now().Add(60 * time.Second)
		for {
			r, err := control.request(ctx, "takeover-status")
			if err != nil {
				t.Fatal(err)
			}
			s := r.Status
			if len(s.Topology.Ranks) == 2 && s.Topology.Ranks[0] != nil && s.Topology.Ranks[0].GID == e.NewRank0.GID && s.Topology.Ranks[0].State == "up:active" && len(s.Topology.Standbys) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the new rank-0 daemon did not become active with a standby", s)
			}
			time.Sleep(250 * time.Millisecond)
		}
		adopted := native(t, "takeover-adopt")
		e.Adopted = adopted.Takeover.Adopted
		after := native(t, "observe", strconv.FormatUint(gid, 10))
		if err := checkNativeCephFSSession(after.view(0).session(gid), identity, metadata); err != nil {
			t.Fatal("the reconnected session is not open on the new rank-0 daemon", err)
		}
		probe(t, 11, &e.ProbeAfter)
		e.GetAfter = getLock(t, "flock get after takeover")
		setLock(t, "flock-exclusive-unlock", cephFlockUnlock)
		probe(t, 0, &e.ProbeAfter)
		// Exactly one acquire before and one unlock after the takeover.
		if err := heldFlockCallsMatch(e.Calls, []heldFlockCall{heldFlockWantCall(heldLock{Rule: cephLockRuleFlock, cephfsFilelock: e.Lock}, cephLockRuleFlock, cephFlockExclusive, 0),
			heldFlockWantCall(heldLock{Rule: cephLockRuleFlock, cephfsFilelock: e.Lock}, cephLockRuleFlock, cephFlockUnlock, 0)}); err != nil {
			t.Fatal(err)
		}
		var locks []uint64
		for _, r := range e.Requests {
			if r.Op == cephmsgr.MDSOpSetFileLock {
				locks = append(locks, r.TID)
			}
		}
		if !slices.Equal(locks, []uint64{e.Calls[0].TID, e.Calls[1].TID}) {
			t.Fatal("a SETFILELOCK was submitted outside the two lock calls", locks)
		}
	}) {
		return
	}

	leaf("cleanup_unlink_release_close", func(t *testing.T) {
		if _, _, err := agent.close(e.Ino, cephFileModeRDWR); err != nil {
			t.Fatal(err)
		}
		submit(t, "unlink lock file", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: migIno, Path: plan.MigrateFiles[0]}}, nil)
		submit(t, "rmdir mig", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.Migrate}}, nil)
		submit(t, "rmdir dir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}}, nil)
		st := native(t, "stat", "dir").Native
		if e.NamespaceRemoved = st != nil && st.Stat != nil && !st.Stat.Exists && st.Stat.Errno == 2; !e.NamespaceRemoved {
			t.Fatal("native stat still finds the owned directory", st)
		}
		items, barrier := agent.releaseAll()
		if len(items) != 0 {
			if err := session.ReleaseCaps(ctx, items, barrier); err != nil {
				t.Fatal(err)
			}
		}
		closing, stop := context.WithTimeout(ctx, 15*time.Second)
		err := session.Close(closing)
		stop()
		if err != nil || session.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("Close was not confirmed", err)
		}
		after := native(t, "observe", strconv.FormatUint(gid, 10))
		if e.SessionClosedNative = after.view(0).session(gid) == nil && after.view(1).session(gid) == nil; !e.SessionClosedNative {
			t.Fatal("the closed session remains natively")
		}
		if dials.OSD.Load() != 0 {
			t.Fatal("the held-lock takeover client dialed an OSD")
		}
	})
}

// checkRanksHeldFlockProbe checks one lock probe receipt of the two-rank
// fixture: the closed action and its mutation record, a native mount that is
// not the Go client, the wanted errno, and the unchanged empty lock file.
func checkRanksHeldFlockProbe(r nativeMDSCephFSRanksReceipt, gid, ino uint64, wantErrno int, baseline **nativeMDSCephFSFlockState) error {
	n := r.Native
	if n == nil || n.Flock == nil || r.Action != "flock-ex-nb" || n.Action != "flock-ex-nb" || n.NativeGlobalID == 0 || n.NativeGlobalID == gid ||
		n.Stat != nil || n.Entries != nil || n.Pin != nil || n.PinSet != nil {
		return fmt.Errorf("native ranks lock receipt is incomplete, of another action or from the Go client: %+v", n)
	}
	p := *n.Flock
	if r.Mutation == nil || r.Mutation.Kind != "native-flock-ex-nb" || r.Mutation.State != "applied" || r.Mutation.RequestID != r.RequestID ||
		p.Owner != cephFlockNativeOwner || p.Errno != wantErrno || p.Acquired != (wantErrno == 0) || p.Unlocked != p.Acquired || p.ElapsedNS <= 0 ||
		p.Before != p.After || p.Before.Ino != ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return fmt.Errorf("native ranks lock probe differs: want errno %d on inode %d, probe %+v mutation %+v", wantErrno, ino, p, r.Mutation)
	}
	if *baseline == nil {
		first := p.Before
		*baseline = &first
	} else if **baseline != p.Before {
		return fmt.Errorf("ranks lock probes changed the native file: %+v then %+v", **baseline, p.Before)
	}
	return nil
}

// TestCephFSRanksHeldFlockProbeChecks is a cluster-free check of the probe
// receipt refusals.
func TestCephFSRanksHeldFlockProbeChecks(t *testing.T) {
	const gid, ino = 42, 0x10000000003
	state := nativeMDSCephFSFlockState{Ino: ino, MTime: 1}
	receipt := func(errno int, edit func(*nativeMDSCephFSRanksReceipt)) nativeMDSCephFSRanksReceipt {
		r := nativeMDSCephFSRanksReceipt{RequestID: 7, Action: "flock-ex-nb", Mutation: &nativeMDSCephFSMutation{Kind: "native-flock-ex-nb", RequestID: 7, State: "applied"},
			Native: &nativeMDSCephFSRanksNative{Action: "flock-ex-nb", NativeGlobalID: 77,
				Flock: &nativeMDSCephFSFlockProbe{Owner: cephFlockNativeOwner, Errno: errno, Acquired: errno == 0, Unlocked: errno == 0, ElapsedNS: 1, Before: state, After: state}}}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	var baseline *nativeMDSCephFSFlockState
	for _, errno := range []int{11, 0} {
		if err := checkRanksHeldFlockProbe(receipt(errno, nil), gid, ino, errno, &baseline); err != nil {
			t.Fatal("correct ranks probe refused", errno, err)
		}
	}
	for name, tc := range map[string]struct {
		got, want int
		edit      func(*nativeMDSCephFSRanksReceipt)
	}{
		"acquired while held":   {0, 11, nil},
		"refused after unlock":  {11, 0, nil},
		"the Go client":         {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.NativeGlobalID = gid }},
		"another inode":         {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock.Before.Ino++; r.Native.Flock.After.Ino++ }},
		"file changed":          {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock.After.MTime++ }},
		"file changed between":  {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock.Before.Version++; r.Native.Flock.After.Version++ }},
		"uncertain process":     {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Mutation.State = "uncertain" }},
		"no mutation":           {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Mutation = nil }},
		"another request":       {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Mutation.RequestID++ }},
		"stat beside the probe": {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Stat = &nativeMDSCephFSStat{} }},
		"another owner":         {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock.Owner = cephFlockGoOwner }},
		"not released":          {0, 0, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock.Unlocked = false }},
		"no probe":              {11, 11, func(r *nativeMDSCephFSRanksReceipt) { r.Native.Flock = nil }},
	} {
		if checkRanksHeldFlockProbe(receipt(tc.got, tc.edit), gid, ino, tc.want, &baseline) == nil {
			t.Fatal("wrong ranks lock probe accepted:", name)
		}
	}
}
