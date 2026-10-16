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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSCompletionIntegration accepts the request-completion and
// reconnect edge cases of a restarted rank-0 MDS against live Tentacle
// daemons: a lost reply of a delivered request and a REPLAY answered from the
// completed-request record (one restart, ms_blackhole_client on the old
// incarnation only), a denied reconnect, a missed reconnect window with OSD
// fencing, a forced read-only filesystem, a dentry lease RENEW and a cap snap
// flush. See integration/fixture/MDS_CEPHFS_COMPLETION_FIXTURE.md and
// MDS_CEPHFS_COMPLETION_VALIDATION.md.

type nativeMDSCephFSCompletionPlan struct {
	Directory        string   `json:"directory"`
	Children         []string `json:"children"`
	Grandchildren    []string `json:"grandchildren"`
	Snapshot         string   `json:"snapshot"`
	FencedPayloadHex string   `json:"fenced_payload_hex"`
	ReconnectTimeout string   `json:"reconnect_timeout"`
}

type nativeMDSCephFSCompletionOracle struct {
	Source            string                        `json:"source_commit"`
	Token             string                        `json:"token"`
	FSID              string                        `json:"fsid"`
	FSCID             int32                         `json:"fscid"`
	FSName            string                        `json:"fs_name"`
	MDSGID            uint64                        `json:"mds_gid"`
	ClientIdentity    string                        `json:"client_identity"`
	NativeIdentity    string                        `json:"native_identity"`
	ClientCaps        map[string]string             `json:"client_caps"`
	NativeCaps        map[string]string             `json:"native_caps"`
	Plan              nativeMDSCephFSCompletionPlan `json:"plan"`
	BaselineBlocklist []nativeMDSSessionsBlock      `json:"baseline_blocklist"`
	BaselineConfig    map[string]string             `json:"baseline_config"`
	DataPool          int64                         `json:"data_pool"`
	DataPoolName      string                        `json:"data_pool_name"`
}

type nativeMDSCephFSMutation struct {
	Kind           string `json:"kind"`
	RequestID      int64  `json:"request_id"`
	Target         string `json:"target"`
	State          string `json:"state"`
	NativeGlobalID uint64 `json:"native_global_id"`
	GlobalID       uint64 `json:"global_id"`
	Address        string `json:"address"`
	Until          string `json:"blocklist_until"`
	Output         string `json:"native_output"`
	Error          string `json:"error"`
}

type nativeMDSCephFSCompletionRuntime struct {
	Kind             string            `json:"kind"`
	Settings         map[string]string `json:"settings"`
	Baseline         map[string]string `json:"baseline"`
	IncarnationGID   uint64            `json:"incarnation_gid"`
	RequestID        int64             `json:"request_id"`
	State            string            `json:"state"`
	Readback         map[string]string `json:"readback"`
	RestoreRequestID int64             `json:"restore_request_id"`
	RestoreReadback  map[string]string `json:"restore_readback"`
	Error            string            `json:"error"`
}

type nativeMDSCephFSCompletionCentral struct {
	Kind             string `json:"kind"`
	Section          string `json:"section"`
	Name             string `json:"name"`
	Value            string `json:"value"`
	RequestID        int64  `json:"request_id"`
	State            string `json:"state"`
	DaemonValue      string `json:"daemon_value"`
	RestoreRequestID int64  `json:"restore_request_id"`
	RestoredDaemon   string `json:"restored_daemon_value"`
	Error            string `json:"error"`
}

type nativeMDSCephFSCompletionDaemon struct {
	GID         uint64 `json:"gid"`
	Rank        int32  `json:"whoami"`
	State       string `json:"state"`
	MDSMapEpoch uint32 `json:"mdsmap_epoch"`
	FSName      string `json:"fs_name"`
}

type nativeMDSCephFSCompletionRecord struct {
	GlobalID         uint64 `json:"global_id"`
	State            string `json:"state"`
	Reconnecting     bool   `json:"reconnecting"`
	RequestsInFlight uint64 `json:"requests_in_flight"`
	NumCompleted     uint64 `json:"num_completed_requests"`
	Requests         []struct {
		TID        uint64 `json:"tid"`
		CreatedIno string `json:"created_ino"`
	} `json:"completed_requests"`
}

// created returns the created inode the record holds for one TID.
func (r *nativeMDSCephFSCompletionRecord) created(tid uint64) (uint64, bool) {
	if r == nil {
		return 0, false
	}
	for _, req := range r.Requests {
		if req.TID == tid {
			ino, err := strconv.ParseUint(strings.TrimPrefix(req.CreatedIno, "0x"), 16, 64)
			return ino, err == nil
		}
	}
	return 0, false
}

type nativeMDSCephFSCompletionLog struct {
	Mark     int      `json:"mark"`
	Total    int      `json:"total_lines"`
	Lines    []string `json:"lines"`
	Dropped  int      `json:"dropped"`
	GlobalID uint64   `json:"global_id"`
	Nonce    uint32   `json:"nonce"`
}

func (l *nativeMDSCephFSCompletionLog) find(parts ...string) []string {
	var out []string
	if l == nil {
		return nil
	}
	for _, line := range l.Lines {
		if !slices.ContainsFunc(parts, func(p string) bool { return !strings.Contains(line, p) }) {
			out = append(out, line)
		}
	}
	return out
}

type nativeMDSCephFSCompletionReceipt struct {
	Source          string                            `json:"source_commit"`
	Token           string                            `json:"token"`
	RequestID       int64                             `json:"request_id"`
	Action          string                            `json:"action"`
	Target          string                            `json:"target"`
	GlobalID        uint64                            `json:"global_id"`
	FSID            string                            `json:"fsid"`
	FSCID           int32                             `json:"fscid"`
	FSName          string                            `json:"fs_name"`
	MDSGID          uint64                            `json:"mds_gid"`
	MDSEndpoint     string                            `json:"mds_endpoint"`
	MDSNonce        uint32                            `json:"mds_nonce"`
	MDSIncarnation  int32                             `json:"mds_incarnation"`
	Rank            int32                             `json:"rank"`
	IdentityChecked bool                              `json:"identity_checked"`
	Map             *nativeMDSSessionsMap             `json:"map"`
	Daemon          *nativeMDSCephFSCompletionDaemon  `json:"daemon"`
	Sessions        []nativeMDSSession                `json:"sessions"`
	Completed       []nativeMDSCephFSCompletionRecord `json:"completed"`
	Blocklist       []nativeMDSSessionsBlock          `json:"blocklist"`
	Native          *nativeMDSCephFSOutput            `json:"native"`
	Process         json.RawMessage                   `json:"native_process,omitempty"`
	Mutation        *nativeMDSCephFSMutation          `json:"mutation"`
	Runtime         *nativeMDSCephFSCompletionRuntime `json:"runtime"`
	Central         *nativeMDSCephFSCompletionCentral `json:"central"`
	Config          map[string]string                 `json:"config"`
	Restart         *nativeMDSCephFSRestart           `json:"restart"`
	RankStatus      *nativeMDSCephFSRankStatus        `json:"rank_status"`
	Ops             []nativeMDSCephFSHistoricOp       `json:"ops"`
	FlushJournal    *struct {
		ReturnCode int64  `json:"return_code"`
		Message    string `json:"message"`
	} `json:"flush_journal"`
	Log           *nativeMDSCephFSCompletionLog `json:"log"`
	Complete      bool                          `json:"complete"`
	Error         string                        `json:"error"`
	ElapsedMillis int64                         `json:"elapsed_ms"`
	ObservedUTC   string                        `json:"observed_utc"`
}

func (r nativeMDSCephFSCompletionReceipt) session(gid uint64) *nativeMDSSession {
	for i := range r.Sessions {
		if r.Sessions[i].ID == gid {
			return &r.Sessions[i]
		}
	}
	return nil
}

func (r nativeMDSCephFSCompletionReceipt) record(gid uint64) *nativeMDSCephFSCompletionRecord {
	for i := range r.Completed {
		if r.Completed[i].GlobalID == gid {
			return &r.Completed[i]
		}
	}
	return nil
}

func (r nativeMDSCephFSCompletionReceipt) blocked(address string) *nativeMDSSessionsBlock {
	for i := range r.Blocklist {
		if !r.Blocklist[i].Range && r.Blocklist[i].Address == address {
			return &r.Blocklist[i]
		}
	}
	return nil
}

// mdsCephFSCompletionControl drives the closed fixture control. One request
// is outstanding at a time.
type mdsCephFSCompletionControl struct {
	mu       sync.Mutex
	dir      string
	token    string
	fresh    nativeMDSFresh
	mds      nativeMDSCephFSDaemon
	pending  bool
	last     int64
	goGIDs   map[uint64]bool
	receipts []nativeMDSCephFSCompletionReceipt
}

func (x *mdsCephFSCompletionControl) request(ctx context.Context, action string, args ...string) (nativeMDSCephFSCompletionReceipt, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	ctx, stop := context.WithTimeout(ctx, 130*time.Second)
	defer stop()
	var result nativeMDSCephFSCompletionReceipt
	path := filepath.Join(x.dir, "mds-cephfs-completion-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.RequestControl(ctx, x.dir, "mds-cephfs-completion", "mds-cephfs-completion-completed", append([]string{x.token, action}, args...)...); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	x.receipts = append(x.receipts, result)
	f := x.fresh
	if result.Source != mdsFreshSource || result.Token != x.token || result.RequestID <= x.last || result.Action != action || result.FSID != f.FSID || result.FSCID != f.FSCID ||
		result.FSName != f.FSName || result.Rank != 0 || !result.Complete || result.Error != "" {
		return result, fmt.Errorf("native MDS CephFS completion receipt scope differs: %s", result.Error)
	}
	x.last = result.RequestID
	expected := x.mds
	switch {
	case action == "restart-fire" || action == "restart-status":
		if result.Restart == nil || result.Restart.Armed.GID != x.mds.GID || action == "restart-status" && result.RankStatus == nil {
			return result, errors.New("native MDS completion restart receipt is incomplete")
		}
		if action == "restart-fire" {
			x.pending = true
		}
		return result, nil
	case x.pending && action != "restart-adopt":
		// Daemon-local diagnostics of the respawned daemon before adoption.
		d, s := result.Daemon, result.RankStatus
		if d == nil || s == nil || s.Rank0 == nil || s.Rank0.GID != d.GID || d.GID == x.mds.GID || s.Rank0.Name != x.mds.Name || d.Rank != 0 {
			return result, errors.New("native MDS completion diagnostic does not name the respawned owned daemon")
		}
		return result, nil
	case action == "restart-adopt":
		r := result.Restart
		if r == nil || r.State != "adopted" || r.Adopted == nil || r.Armed.GID != x.mds.GID {
			return result, errors.New("native MDS completion restart adoption receipt differs")
		}
		a := *r.Adopted
		if a.Name != x.mds.Name || a.GID == x.mds.GID || a.Nonce == x.mds.Nonce || a.Nonce == 0 || a.Rank != 0 || a.State != "up:active" || a.Incarnation == x.mds.Incarnation {
			return result, errors.New("adopted MDS incarnation is not a new incarnation of the owned daemon")
		}
		expected = a
	}
	if result.MDSGID != expected.GID || result.MDSEndpoint != expected.Endpoint || result.MDSNonce != expected.Nonce || result.MDSIncarnation != expected.Incarnation {
		return result, errors.New("native MDS completion receipt names a different MDS incarnation")
	}
	d := result.Daemon
	if !result.IdentityChecked || result.Map == nil || d == nil || d.GID != expected.GID || d.State != "up:active" || d.Rank != 0 || d.MDSMapEpoch < result.Map.Epoch {
		return result, errors.New("native MDS completion receipt lacks its identity proof")
	}
	switch action {
	case "observe":
		if result.GlobalID == 0 || result.Sessions == nil || result.Completed == nil {
			return result, errors.New("native MDS completion observation scope differs")
		}
	case "stat", "list", "write-fenced", "snapshot-create", "snapshot-remove":
		if result.Native == nil || result.Native.NativeGlobalID == 0 || x.goGIDs[result.Native.NativeGlobalID] {
			return result, errors.New("native CephFS completion oracle output missing or bound to a Go client")
		}
	}
	x.mds, x.pending = expected, false
	return result, nil
}

func (x *mdsCephFSCompletionControl) stat(ctx context.Context, target string) (nativeMDSCephFSStat, error) {
	r, err := x.request(ctx, "stat", target)
	if err != nil {
		return nativeMDSCephFSStat{}, err
	}
	if r.Native.Stat == nil {
		return nativeMDSCephFSStat{}, errors.New("native completion stat missing")
	}
	return *r.Native.Stat, nil
}

// completionReply is one reply as the Go client received it.
type completionReply struct {
	Name         string
	Transaction  uint64
	Op           uint32
	Result       int32
	Safe         bool
	IsDentry     bool
	IsTarget     bool
	MDSMapEpoch  uint32
	TraceBytes   int
	SnapBytes    int
	ExtraHex     string `json:",omitempty"`
	ExtraIno     uint64 `json:",omitempty"`
	TargetIno    uint64 `json:",omitempty"`
	DentryName   string `json:",omitempty"`
	TargetMode   uint32 `json:",omitempty"`
	TargetCaps   string `json:",omitempty"`
	Arrival      uint64
	AfterMicros  int64
	Error        string             `json:",omitempty"`
	ErrorChain   recoveryErrorChain `json:",omitempty"`
	ExpectedNote string             `json:",omitempty"`
}

func describeCompletionReply(name string, call *cephmsgr.MDSRequestCall, r cephmsgr.MDSReply, err error, since time.Time) completionReply {
	e := completionReply{Name: name, Transaction: call.Transaction()}
	if err != nil {
		e.Error, e.ErrorChain = err.Error(), describeRecoveryError(err)
		return e
	}
	e.Op, e.Result, e.Safe, e.IsDentry, e.IsTarget, e.MDSMapEpoch, e.TraceBytes, e.SnapBytes = r.Op, r.Result, r.Safe, r.IsDentry, r.IsTarget, r.MDSMapEpoch, len(r.Trace), len(r.SnapTrace)
	e.ExtraHex, e.Arrival, e.AfterMicros = hex.EncodeToString(r.Extra), r.Arrival, r.Received.Sub(since).Microseconds()
	if len(r.Extra) == 8 {
		e.ExtraIno = binary.LittleEndian.Uint64(r.Extra)
	}
	if trace, err := r.DecodeTrace(); err == nil && trace.Target != nil {
		e.TargetIno, e.DentryName, e.TargetMode, e.TargetCaps = trace.Target.Ino, trace.DentryName, trace.Target.Mode, cephCapString(trace.Target.Cap.Caps)
	}
	return e
}

type completionLostEvidence struct {
	ArmedGID, AdoptedGID                 uint64
	ArmedIncarnation, AdoptedIncarnation int32
	ReplayedTID, LostFileTID, LostDirTID uint64
	ReplayedUnsafe                       completionReply
	ReplayedIno                          uint64
	NoReplyWithin                        string
	UnsafeWindow                         *mdsUnsafeWindowEvidence
	InFlight                             []nativeMDSCephFSHistoricOp
	FlushJournal                         any
	CompletedBefore                      *nativeMDSCephFSCompletionRecord
	HistoricBefore                       []nativeMDSCephFSHistoricOp
	BlackholeLines                       []string
	ProbeBeforeRestart                   map[string]recoveryErrorChain
	LostAfterFireMicros                  int64
	ResumeError                          recoveryErrorChain
	NewIncarnationConfig                 map[string]string
	ReconnectError                       recoveryErrorChain
	ReconnectMicros                      int64
	Replies                              []completionReply
	SessionAfterReplies                  string
	SessionError                         recoveryErrorChain
	MDSLogLines                          []string
	ActiveAfterFireMicros                int64
	OldIncarnationFencing                []nativeMDSSessionsBlock
	NativeListing                        map[string]uint64
	NativeLostDirMode                    uint32
	NativeSessionAfter                   *nativeMDSSession
	OrphanEvicted                        *nativeMDSCephFSMutation
	ProductFinding                       string `json:",omitempty"`
}

type completionDenyEvidence struct {
	ArmedGID, AdoptedGID     uint64
	Central                  *nativeMDSCephFSCompletionCentral
	NewIncarnationConfig     map[string]string
	StateAtReconnect         string
	ReconnectAfterFireMicros int64
	ReconnectError           recoveryErrorChain
	ActiveAfterFireMicros    int64
	MDSLogLines              []string
	SessionAddress           string
	Blocklisted              *nativeMDSSessionsBlock
	SessionAfter             *nativeMDSSession
	SameClientError          recoveryErrorChain
	SameClientReject         *cephmsgr.MDSSessionRejectedError
	NewClientGlobalID        uint64
	NewClientOpen            *mdsSessionEvidence
	NewClientClosed          bool
	Restored                 *nativeMDSCephFSCompletionCentral
}

type completionWindowEvidence struct {
	ArmedGID, AdoptedGID  uint64
	Central               *nativeMDSCephFSCompletionCentral
	FencedIno             uint64
	Object                string
	Placement             recoveryPlacement
	PlacementAfter        recoveryPlacement
	OSDAddress            string
	ReadBefore            recoveryOSDResult
	NewIncarnationConfig  map[string]string
	StatusTimeline        []recoveryStatusSample
	ActiveAfterFireMicros int64
	MDSLogLines           []string
	SessionAddress        string
	Blocklisted           *nativeMDSSessionsBlock
	ReconnectAfterFire    int64
	ReconnectError        recoveryErrorChain
	ReconnectLogLines     []string
	ReadAfter             recoveryOSDResult
	ReaderRead            recoveryOSDResult
	Restored              *nativeMDSCephFSCompletionCentral
}

type completionReadOnlyEvidence struct {
	FileIno                           uint64
	FileCapsBefore                    string
	ForceRO                           *nativeMDSCephFSMutation
	ForceROArrival                    uint64
	ForceROAfterMicros                int64
	PushBefore, PushAfter             uint64
	StatusReadOnly                    bool
	RevokesAfter                      []cephfsCapRecord
	MkdirReply, LookupReply, GetReply completionReply
	MDSLogLines                       []string
	ArmedGID, AdoptedGID              uint64
	ReconnectError                    recoveryErrorChain
	ReadOnlyAfterReconnect            bool
	MkdirAfterReply                   completionReply
}

type completionLeaseEvidence struct {
	HolderGlobalID   uint64
	LeaseDirIno      uint64
	LeasedIno        uint64
	GoDirCaps        string
	Lookup           completionReply
	LeaseStat        cephmsgr.MDSLeaseStat
	Renew            cephmsgr.MDSLease
	Answer           *cephfsLeaseRenewal
	AnswerAfterMicro int64
	PushBefore       uint64
	PushAfter        uint64
	MDSLogLines      []string
}

type completionSnapEvidence struct {
	SnapDirIno, FileIno uint64
	CapBefore           cephfsCap
	FollowsRealm        cephmsgr.MDSSnapRealm
	Snapshot            *nativeMDSCephFSOutput
	SnapEvents          []cephfsSnapEvent
	CapAfterSplit       cephfsCap
	FlushSnap           cephmsgr.MDSCaps
	FlushTID            uint64
	Ack                 *cephfsFlushSnapAck
	AckAfterMicros      int64
	MDSLogLines         []string
	SnapshotRemoved     *nativeMDSCephFSOutput
}

func TestCephMDSCephFSCompletionIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned native CephFS completion acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS") == "1" ||
		os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("CephFS completion acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSCompletionOracle
	var fresh nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-completion-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	var current nativeMDSCephFSTarget
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-completion-target.json"), &current); err != nil {
		t.Fatal(err)
	}
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE")
	plan := oracle.Plan
	payload, payloadErr := hex.DecodeString(plan.FencedPayloadHex)
	fsName := fresh.FSName
	if oracle.Source != mdsFreshSource || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || oracle.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" ||
		identity != "client.mds-cephfs-completion-"+oracle.Token || oracle.ClientIdentity != identity || oracle.NativeIdentity != "client.mds-cephfs-completion-native-"+oracle.Token ||
		oracle.ClientCaps["mon"] != "allow r" || oracle.ClientCaps["mds"] != "allow rw fsname="+fsName || oracle.ClientCaps["osd"] != "allow r tag cephfs data="+fsName ||
		oracle.NativeCaps["mds"] != "allow rws fsname="+fsName || plan.Directory != "ceph-msgr-completion-"+oracle.Token ||
		!slices.Equal(plan.Children, []string{"lease", "replayed", "lost-file", "lost-dir", "fenced", "ro-file", "ro-dir", "snap"}) ||
		!slices.Equal(plan.Grandchildren, []string{"lease/leased", "snap/snap-file"}) || plan.Snapshot != "completion-"+oracle.Token[:8] || plan.ReconnectTimeout != "5" ||
		payloadErr != nil || len(payload) != 5000 || oracle.DataPoolName != fsName+"-data" || oracle.DataPool < 0 ||
		current.Source != mdsFreshSource || current.Token != oracle.Token || current.Adopted != 0 || current.MDS.GID != fresh.MDS.GID || current.MDS.Nonce != fresh.MDS.Nonce ||
		oracle.BaselineConfig["ms_blackhole_client"] != "false" || oracle.BaselineConfig["mds_early_reply"] != "true" || oracle.BaselineConfig["mds_log_pause"] != "false" {
		t.Fatal("owned CephFS completion oracle, plan, target or identity differ")
	}
	control := &mdsCephFSCompletionControl{dir: dir, token: oracle.Token, fresh: fresh, mds: current.MDS, goGIDs: map[uint64]bool{}}
	target := func(m nativeMDSCephFSDaemon) cephmsgr.MDSTarget {
		return cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", m.Endpoint, m.Nonce)}
	}
	metadata := func(role string) map[string]string {
		return map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-" + role, "root": "/"}
	}
	sessionOptions := func(role string) cephmsgr.MDSSessionOptions {
		return cephmsgr.MDSSessionOptions{Metadata: metadata(role), RenewInterval: 2 * time.Second, MaxEvents: 1024}
	}

	var (
		leaseEv    completionLeaseEvidence
		lostEv     completionLostEvidence
		denyEv     completionDenyEvidence
		windowEv   completionWindowEvidence
		roEv       completionReadOnlyEvidence
		snapEv     completionSnapEvidence
		agents     = map[string]*cephfsCapAgent{}
		facts      = map[string]any{}
		caseTimes  = map[string]int64{}
		realms     = map[uint64]cephmsgr.MDSSnapRealm{}
		requests   []completionReply
		findings   []string
		started    = time.Now()
		dirIno     uint64
		inos       = map[string]uint64{}
		consistent = true
	)
	t.Cleanup(func() {
		snapshots := map[string]cephfsAgentSnapshot{}
		for name, a := range agents {
			snapshots[name] = a.snapshot()
		}
		data, err := json.Marshal(struct {
			Source       string                             `json:"source_commit"`
			Oracle       any                                `json:"oracle"`
			StartTarget  nativeMDSCephFSTarget              `json:"start_target"`
			Requests     []completionReply                  `json:"requests"`
			Lease        completionLeaseEvidence            `json:"lease_renew"`
			Lost         completionLostEvidence             `json:"lost_reply_and_completed_replay"`
			Deny         completionDenyEvidence             `json:"reconnect_denied"`
			Window       completionWindowEvidence           `json:"reconnect_window_missed"`
			ReadOnly     completionReadOnlyEvidence         `json:"force_readonly"`
			Snap         completionSnapEvidence             `json:"cap_snap_flush"`
			Agents       map[string]cephfsAgentSnapshot     `json:"agents"`
			Facts        map[string]any                     `json:"facts"`
			Findings     []string                           `json:"product_findings"`
			Native       []nativeMDSCephFSCompletionReceipt `json:"native"`
			CaseMillis   map[string]int64                   `json:"case_ms"`
			ElapsedMilli int64                              `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, current, requests, leaseEv, lostEv, denyEv, windowEv, roEv, snapEv, snapshots, facts, findings, control.receipts, caseTimes,
			time.Since(started).Milliseconds()})
		if err == nil && len(data) > 16<<20 {
			err = errors.New("MDS CephFS completion evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-completion-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS completion public evidence", err)
		}
	})
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		for role, a := range agents {
			if s := a.snapshot(); len(s.Errors) != 0 {
				t.Errorf("capability agent %s errors after %s: %v", role, name, s.Errors)
				ok = false
			}
		}
		if control.pending {
			consistent = false
		}
		return ok
	}
	req := func(t *testing.T, action string, args ...string) nativeMDSCephFSCompletionReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("native control", action, err)
		}
		return r
	}
	observe := func(t *testing.T, gid uint64) (nativeMDSCephFSCompletionReceipt, *nativeMDSSession) {
		t.Helper()
		r := req(t, "observe", strconv.FormatUint(gid, 10))
		return r, r.session(gid)
	}
	var epoch uint32
	// client dials one Go client of the owned identity through counted relays.
	client := func(t *testing.T, role string, osd bool, endpoints ...string) (*cephmsgr.Client, *recoveryDials) {
		t.Helper()
		dials := newRecoveryDials(osd, endpoints...)
		c := recoveryClient(t, ctx, identity, keyFile, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = true, true, osd })
		snap := c.Snapshot()
		if snap.GlobalID == 0 || snap.CephFSNonce == 0 {
			t.Fatal("CephFS client lacks a GlobalID or shared nonce", role, snap)
		}
		control.goGIDs[snap.GlobalID] = true
		return c, dials
	}
	open := func(t *testing.T, c *cephmsgr.Client, role string) (*cephmsgr.MDSConnection, *cephmsgr.MDSSession, *cephfsCapAgent, string) {
		t.Helper()
		conn, err := c.OpenMDS(ctx, target(control.mds))
		if err != nil {
			t.Fatal("open explicit MDS target", role, err)
		}
		s, err := conn.OpenSession(ctx, sessionOptions(role))
		if err != nil {
			t.Fatal("open CephFS session", role, err)
		}
		a := startCephFSCapAgent(ctx, s)
		agents[role] = a
		r, n := observe(t, c.Snapshot().GlobalID)
		if err := checkNativeCephFSSession(n, identity, metadata(role)); err != nil {
			t.Fatal(err)
		}
		epoch = r.Map.Epoch
		if !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(c.Snapshot().CephFSNonce), 10)) {
			t.Fatalf("native session address %s does not carry the shared nonce", n.Address)
		}
		return conn, s, a, n.Address
	}
	// submit sends one request and waits for its first reply (and the safe
	// reply when flush asks for it).
	submit := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, name string, request cephmsgr.MDSRequest, safe bool) (*cephmsgr.MDSRequestCall, cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		request.MDSMapEpoch = epoch
		begin := time.Now()
		call, err := s.Submit(ctx, request)
		if err != nil {
			t.Fatal("submit", name, err)
		}
		wait, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		first, err := call.Wait(wait)
		e := describeCompletionReply(name, call, first, err, begin)
		requests = append(requests, e)
		if err != nil {
			t.Fatal("first reply", name, err)
		}
		trace, _, err := a.applyReply(name, first)
		if err != nil {
			t.Fatal("decode reply trace", name, err)
		}
		snaps, err := first.DecodeSnapTrace()
		if err != nil {
			t.Fatal("decode snap trace", name, err)
		}
		for _, r := range snaps {
			realms[r.Ino] = r
		}
		if safe && !first.Safe {
			if err := s.FlushJournal(ctx); err != nil {
				t.Fatal("flush journal", name, err)
			}
			last, err := call.WaitSafe(wait)
			e := describeCompletionReply(name+" safe", call, last, err, begin)
			requests = append(requests, e)
			if err != nil || last.Result != first.Result {
				t.Fatal("safe reply", name, err, last.Result)
			}
		}
		return call, first, trace
	}
	create := func(parent uint64, name string) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0),
			Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}
	}
	mkdir := func(parent uint64, name string) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}
	}
	lookup := func(parent uint64, name string) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}
	}
	// waitRank polls the transitional rank-0 status until done.
	waitRank := func(t *testing.T, limit time.Duration, timeline *[]recoveryStatusSample, fired time.Time, done func(*nativeMDSCephFSDaemon) bool) *nativeMDSCephFSDaemon {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			r := req(t, "restart-status")
			s := r.RankStatus
			if timeline != nil {
				*timeline = append(*timeline, recoveryStatusSample{AfterFireMillis: time.Since(fired).Milliseconds(), FSMapEpoch: s.FSMapEpoch, Rank0: s.Rank0, Failed: s.Failed,
					ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist})
			}
			if s.Rank0 != nil && s.Rank0.GID != control.mds.GID && s.Rank0.Name == control.mds.Name && s.Rank0.Nonce != 0 && done(s.Rank0) {
				return s.Rank0
			}
			if time.Now().After(deadline) {
				t.Fatal("the restarted MDS did not reach the awaited state", s)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	// fire fails the armed daemon and waits until session s is Lost.
	fire := func(t *testing.T, s *cephmsgr.MDSSession) (time.Time, time.Duration) {
		t.Helper()
		lost := make(chan time.Time, 1)
		go func() {
			<-s.Done()
			lost <- time.Now()
		}()
		fired := time.Now()
		req(t, "restart-fire")
		select {
		case at := <-lost:
			if st := s.Status().State; st != cephmsgr.MDSSessionLost {
				t.Fatalf("session ended %d, not Lost: %v", st, s.Err())
			}
			return fired, at.Sub(fired)
		case <-time.After(45 * time.Second):
			t.Fatal("the session did not end after the MDS restart")
		}
		return fired, 0
	}
	logLines := func(t *testing.T, gid uint64, nonce uint32) []string {
		t.Helper()
		r := req(t, "mds-log", strconv.FormatUint(gid, 10), strconv.FormatUint(uint64(nonce), 10))
		if r.Log == nil {
			t.Fatal("daemon log receipt missing")
		}
		return r.Log.Lines
	}
	closeSession := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, gid uint64) {
		t.Helper()
		items, barrier := a.releaseAll()
		if len(items) != 0 {
			release, stop := context.WithTimeout(ctx, 10*time.Second)
			err := s.ReleaseCaps(release, items, barrier)
			stop()
			if err != nil {
				t.Fatal("release caps", err)
			}
		}
		closing, stop := context.WithTimeout(ctx, 15*time.Second)
		err := s.Close(closing)
		stop()
		if err != nil || s.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("Close was not confirmed by the MDS", err, s.Err())
		}
		if r, _ := observe(t, gid); r.session(gid) != nil {
			t.Fatal("closed session remains in the native session map")
		}
	}

	clientA, dialsA := client(t, "main", false, current.MDS.Endpoint)
	gidA, nonceA := clientA.Snapshot().GlobalID, clientA.Snapshot().CephFSNonce
	var connA *cephmsgr.MDSConnection
	var sessionA *cephmsgr.MDSSession
	var agentA *cephfsCapAgent
	activeA := func() *cephmsgr.MDSSession { return sessionA }
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if s := activeA(); s != nil {
			s.Close(closing)
		}
	})

	if !leaf("open_completion_session", func(t *testing.T) {
		var address string
		connA, sessionA, agentA, address = open(t, clientA, "main")
		status := sessionA.Status()
		for _, bit := range []uint{7, 9, mds.FeatureMultiReconnect, 18, 19, 20} {
			if !cephmsgr.MDSFeatureBitsetHas(status.Open.SupportedFeatures, bit) {
				t.Fatal("MDS OPEN lacks a feature this acceptance relies on", bit)
			}
		}
		_, first, trace := submit(t, sessionA, agentA, "mkdir dir", mkdir(1, plan.Directory), true)
		if first.Result != 0 || trace.Target == nil || trace.DentryName != plan.Directory {
			t.Fatal("mkdir of the owned directory differs", first.Result)
		}
		dirIno = trace.Target.Ino
		facts["main_client"] = map[string]any{"global_id": gidA, "cephfs_nonce": nonceA, "native_address": address, "dir_ino": dirIno}
	}) {
		return
	}

	leaf("lease_renew_answered", func(t *testing.T) {
		// A second client creates lease/leased: as the directory's only
		// client with caps it keeps Fsx, so the main client's lookup gets a
		// dentry lease (Locker::issue_client_lease) instead of Fs.
		holder, _ := client(t, "holder", false, control.mds.Endpoint)
		leaseEv.HolderGlobalID = holder.Snapshot().GlobalID
		_, sessionH, agentH, _ := open(t, holder, "holder")
		_, _, hdir := submit(t, sessionH, agentH, "holder mkdir lease", mkdir(dirIno, "lease"), true)
		leaseEv.LeaseDirIno = hdir.Target.Ino
		_, _, hfile := submit(t, sessionH, agentH, "holder create leased", create(leaseEv.LeaseDirIno, "leased"), true)
		leaseEv.LeasedIno = hfile.Target.Ino
		agentH.open(leaseEv.LeasedIno, cephFileModeRDWR)
		if _, _, err := agentH.close(leaseEv.LeasedIno, cephFileModeRDWR); err != nil {
			t.Fatal(err)
		}
		inos["lease"], inos["lease/leased"] = leaseEv.LeaseDirIno, leaseEv.LeasedIno
		call, first, trace := submit(t, sessionA, agentA, "lookup leased", lookup(leaseEv.LeaseDirIno, "leased"), false)
		leaseEv.Lookup = describeCompletionReply("lookup leased", call, first, nil, first.Received)
		goDir, _ := agentA.cap(leaseEv.LeaseDirIno)
		leaseEv.GoDirCaps = cephCapString(goDir.Issued)
		if first.Result != 0 || trace.Lease == nil || trace.Target == nil || trace.Target.Ino != leaseEv.LeasedIno || trace.Lease.Mask&3 == 0 || trace.Lease.Seq == 0 {
			t.Fatalf("lookup returned no dentry lease (dir caps %s): %+v", leaseEv.GoDirCaps, trace.Lease)
		}
		leaseEv.LeaseStat = *trace.Lease
		req(t, "mds-log-mark")
		req(t, "debug-on")
		// Locker::handle_client_lease reads the directory inode (head, last
		// CEPH_NOSNAP), the dentry name and the action; it echoes the rest.
		leaseEv.Renew = cephmsgr.MDSLease{Action: cephmsgr.MDSLeaseRenew, Mask: 1, Ino: leaseEv.LeaseDirIno, First: 0xfffffffffffffffe, Last: 0xfffffffffffffffe,
			Seq: trace.Lease.Seq, DentryName: "leased"}
		before := agentA.snapshot()
		leaseEv.PushBefore = sessionA.Status().PushSequence
		sent := time.Now()
		if err := sessionA.SendLease(ctx, leaseEv.Renew); err != nil {
			t.Fatal("send lease renew", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			s := agentA.snapshot()
			if len(s.Renewals) > len(before.Renewals) {
				answer := s.Renewals[len(s.Renewals)-1]
				leaseEv.Answer, leaseEv.AnswerAfterMicro = &answer, answer.Received.Sub(sent).Microseconds()
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the MDS sent no lease RENEW answer")
			}
			time.Sleep(20 * time.Millisecond)
		}
		leaseEv.PushAfter = sessionA.Status().PushSequence
		leaseEv.MDSLogLines = logLines(t, gidA, nonceA)
		req(t, "debug-off")
		l := leaseEv.Answer.Lease
		// The answer is the request copied with a new sequence and the
		// pool-1 duration (MDCache client_lease_durations[1] = 30 s).
		if l.Action != cephmsgr.MDSLeaseRenew || l.Mask != leaseEv.Renew.Mask || l.Ino != leaseEv.LeaseDirIno || l.First != leaseEv.Renew.First || l.Last != leaseEv.Renew.Last ||
			l.Seq != trace.Lease.Seq+1 || l.DurationMS != 30000 || l.DentryName != "leased" || leaseEv.PushAfter < leaseEv.PushBefore+1 || leaseEv.Answer.Arrival <= first.Arrival {
			t.Fatalf("lease RENEW answer differs: %+v", leaseEv)
		}
		if len((&nativeMDSCephFSCompletionLog{Lines: leaseEv.MDSLogLines}).find("handle_client_lease", "client."+strconv.FormatUint(gidA, 10), "renew on")) == 0 {
			t.Fatal("the daemon log lacks the renew line", leaseEv.MDSLogLines)
		}
		closeSession(t, sessionH, agentH, leaseEv.HolderGlobalID)
		delete(agents, "holder")
	})

	if !consistent || !leaf("lost_reply_and_completed_replay", func(t *testing.T) {
		e := &lostEv
		armed := req(t, "restart-arm")
		e.ArmedGID, e.ArmedIncarnation = armed.Restart.Armed.GID, armed.Restart.Armed.Incarnation
		req(t, "log-pause")
		// R: its unsafe reply arrives; the paused journal keeps it unsafe.
		callR, firstR, traceR := submit(t, sessionA, agentA, "create replayed", create(dirIno, "replayed"), false)
		e.ReplayedUnsafe = describeCompletionReply("create replayed", callR, firstR, nil, firstR.Received)
		if firstR.Result != 0 || firstR.Safe || traceR.Target == nil || len(firstR.Extra) != 8 || binary.LittleEndian.Uint64(firstR.Extra) != traceR.Target.Ino {
			t.Fatalf("the replayed create was not answered unsafe: %+v", e.ReplayedUnsafe)
		}
		e.ReplayedIno, e.ReplayedTID = traceR.Target.Ino, callR.Transaction()
		agentA.open(e.ReplayedIno, cephFileModeRDWR)
		// L and M: executed in memory, waiting for the journal; without
		// early replies the MDS sends nothing yet.
		req(t, "early-reply-off")
		requestL, requestM := create(dirIno, "lost-file"), mkdir(dirIno, "lost-dir")
		requestL.MDSMapEpoch, requestM.MDSMapEpoch = epoch, epoch
		callL, err := sessionA.Submit(ctx, requestL)
		if err != nil {
			t.Fatal(err)
		}
		callM, err := sessionA.Submit(ctx, requestM)
		if err != nil {
			t.Fatal(err)
		}
		e.LostFileTID, e.LostDirTID = callL.Transaction(), callM.Transaction()
		submittedLM := time.Now()
		// The MDS side of the window (ceph-testcontainers-go v0.3.0 unsafe
		// receipt): two dump_ops_in_flight samples at least 3 s apart, from
		// the same MDS GID, hold R after early_replied and L and M without a
		// reply, all at the journal submission and none journal_committed.
		sampleInFlight := func(t *testing.T) mdsUnsafeWindowSample {
			t.Helper()
			sent := time.Now()
			r := req(t, "ops-in-flight", strconv.FormatUint(gidA, 10))
			if !r.IdentityChecked || r.MDSGID == 0 {
				t.Fatalf("in-flight sample lacks its MDS identity proof: %+v", r)
			}
			return mdsUnsafeWindowSample{MDSGID: r.MDSGID, SentAt: sent, ReturnedAt: time.Now(), Ops: r.Ops}
		}
		window, err := awaitMDSUnsafeWindow(t, sampleInFlight, gidA, map[uint64]string{
			e.ReplayedTID: mdsRequestUnsafe, e.LostFileTID: mdsRequestNoEarlyReply, e.LostDirTID: mdsRequestNoEarlyReply})
		e.UnsafeWindow = &window
		if err != nil {
			t.Fatal(err)
		}
		if window.Second.MDSGID != e.ArmedGID {
			t.Fatalf("the unsafe window was sampled on MDS %d, not the armed daemon %d", window.Second.MDSGID, e.ArmedGID)
		}
		// probe reports, without waiting, whether any reply arrived: a
		// canceled local wait returns context.Canceled only without one.
		probe := func() (map[string]recoveryErrorChain, bool) {
			ctx, stop := context.WithCancel(ctx)
			stop()
			_, errL := callL.Wait(ctx)
			_, errM := callM.Wait(ctx)
			_, errR := callR.WaitSafe(ctx)
			none := errors.Is(errL, context.Canceled) && errors.Is(errM, context.Canceled) && errors.Is(errR, context.Canceled)
			return map[string]recoveryErrorChain{"lost-file": describeRecoveryError(errL), "lost-dir": describeRecoveryError(errM), "replayed-safe": describeRecoveryError(errR)}, none
		}
		if p, none := probe(); !none {
			t.Fatalf("a reply arrived while the journal was paused: %+v", p)
		}
		e.NoReplyWithin = time.Since(submittedLM).Round(time.Millisecond).String()
		observe(t, gidA)
		e.InFlight = window.Second.Ops
		// Drop every later message to clients, then let the journal commit.
		req(t, "mds-log-mark")
		req(t, "blackhole-on")
		req(t, "log-resume")
		flushed := req(t, "flush-journal")
		e.FlushJournal = flushed.FlushJournal
		r, n := observe(t, gidA)
		e.CompletedBefore = r.record(gidA)
		if n == nil || e.CompletedBefore == nil || e.CompletedBefore.RequestsInFlight != 0 {
			t.Fatalf("completed-request record missing or requests still in flight: %+v", e.CompletedBefore)
		}
		for name, tid := range map[string]uint64{"replayed": e.ReplayedTID, "lost-file": e.LostFileTID, "lost-dir": e.LostDirTID} {
			ino, ok := e.CompletedBefore.created(tid)
			if !ok || ino == 0 || name == "replayed" && ino != e.ReplayedIno {
				t.Fatalf("completed-request record lacks %s (tid %d): %+v", name, tid, e.CompletedBefore)
			}
			inos[name] = ino
		}
		e.HistoricBefore = req(t, "historic-ops", strconv.FormatUint(gidA, 10)).Ops
		for _, tid := range []uint64{e.ReplayedTID, e.LostFileTID, e.LostDirTID} {
			prefix := fmt.Sprintf("client_request(client.%d:%d ", gidA, tid)
			if !slices.ContainsFunc(e.HistoricBefore, func(op nativeMDSCephFSHistoricOp) bool {
				return strings.HasPrefix(op.Description, prefix) && op.FlagPoint == "done" && slices.Contains(op.Events, "journal_committed: ")
			}) {
				t.Fatalf("request %d did not complete natively: %+v", tid, e.HistoricBefore)
			}
		}
		lines := logLines(t, gidA, nonceA)
		for _, tid := range []uint64{e.ReplayedTID, e.LostFileTID, e.LostDirTID} {
			found := (&nativeMDSCephFSCompletionLog{Lines: lines}).find(" blackhole ", fmt.Sprintf("client_reply(???:%d = 0 (0) Success safe)", tid))
			if len(found) != 1 {
				t.Fatalf("the daemon log lacks exactly one blackholed safe reply for tid %d: %v", tid, lines)
			}
			e.BlackholeLines = append(e.BlackholeLines, found...)
		}
		var none bool
		if e.ProbeBeforeRestart, none = probe(); !none {
			t.Fatalf("a dropped reply reached the client: %+v", e.ProbeBeforeRestart)
		}
		fired, lostAfter := fire(t, sessionA)
		e.LostAfterFireMicros, e.ResumeError = lostAfter.Microseconds(), describeRecoveryError(sessionA.Err())
		lost := sessionA
		newRank := waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State != "up:replay" && d.State != "up:boot" })
		if newRank.State != "up:reconnect" {
			t.Fatal("the restarted MDS is not waiting in up:reconnect", newRank)
		}
		cfg := req(t, "config-read")
		e.NewIncarnationConfig = cfg.Config
		if cfg.Config["ms_blackhole_client"] != "false" || cfg.Config["mds_early_reply"] != "true" || cfg.Config["mds_log_pause"] != "false" {
			t.Fatalf("the new incarnation inherited a runtime change: %v", cfg.Config)
		}
		req(t, "debug-on")
		req(t, "mds-log-mark")
		dialsA.allowMDS(newRank.Endpoint)
		if err := connA.Close(); err != nil {
			t.Fatal(err)
		}
		caps, realmState, err := agentA.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		resubmit := []*cephmsgr.MDSRequestCall{callR, callL, callM}
		reconnectStarted := time.Now()
		connA, err = clientA.OpenMDS(ctx, target(*newRank))
		if err != nil {
			t.Fatal("open the new MDS incarnation", err)
		}
		reconnectCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		s2, calls, err := connA.ReconnectSession(reconnectCtx, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState, Resubmit: resubmit}, sessionOptions("main"))
		stop()
		e.ReconnectMicros, e.ReconnectError = time.Since(reconnectStarted).Microseconds(), describeRecoveryError(err)
		if err != nil || len(calls) != 3 {
			t.Fatal("CephFS reconnect to the new incarnation", err)
		}
		sessionA = s2
		agentA = startCephFSCapAgentFrom(ctx, s2, agentA)
		agents["main"] = agentA
		for i, name := range []string{"replayed", "lost-file", "lost-dir"} {
			if calls[i].Transaction() != resubmit[i].Transaction() {
				t.Fatal("resubmitted call changed its TID", name)
			}
			wait, stop := context.WithTimeout(ctx, 30*time.Second)
			reply, err := calls[i].Wait(wait)
			stop()
			e.Replies = append(e.Replies, describeCompletionReply(name+" resubmitted", calls[i], reply, err, reconnectStarted))
			if err == nil && (reply.IsDentry || reply.IsTarget) {
				if _, _, err := agentA.applyReply(name, reply); err != nil {
					t.Error("apply resubmitted reply", name, err)
				}
			}
		}
		requests = append(requests, e.Replies...)
		e.SessionAfterReplies, e.SessionError = fmt.Sprint(sessionA.Status().State), describeRecoveryError(sessionA.Err())
		active := waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State == "up:active" })
		e.ActiveAfterFireMicros = time.Since(fired).Microseconds()
		e.MDSLogLines = logLines(t, gidA, nonceA)
		req(t, "debug-off")
		adopted := req(t, "restart-adopt")
		e.AdoptedGID, e.AdoptedIncarnation, e.OldIncarnationFencing = active.GID, adopted.Restart.Adopted.Incarnation, adopted.Restart.OldFencing
		rM := e.Replies[2]
		if rM.Error != "" || sessionA.Status().State != cephmsgr.MDSSessionOpen {
			// Containment only, not reached when the converted reply is
			// accepted: a client that lost its transport leaves the MDS
			// holding the session and its caps, so a native mount would wait
			// on revokes and later restarts on its reconnect.
			ev := req(t, "evict", strconv.FormatUint(gidA, 10))
			e.OrphanEvicted = ev.Mutation
			sessionA = nil
		} else {
			_, n := observe(t, gidA)
			e.NativeSessionAfter = n
			if n == nil || n.State != "open" {
				t.Errorf("the reconnected session is not open natively after the converted reply: %+v", n)
			}
		}
		listing := req(t, "list")
		e.NativeListing = map[string]uint64{}
		for _, entry := range listing.Native.Entries {
			if _, dup := e.NativeListing[entry.Name]; dup {
				t.Fatal("native listing shows an entry twice", entry.Name)
			}
			e.NativeListing[entry.Name] = entry.Stat.Ino
			if entry.Name == "lost-dir" {
				e.NativeLostDirMode = entry.Stat.Mode
			}
		}
		check := func(cond bool, format string, args ...any) {
			if !cond {
				t.Errorf(format, args...)
			}
		}
		log := &nativeMDSCephFSCompletionLog{Lines: e.MDSLogLines}
		rR, rL := e.Replies[0], e.Replies[1]
		// REPLAY of a journaled create: "already completed", a traceless safe
		// reply whose extra is the created inode (Server::handle_client_request).
		check(rR.Error == "" && rR.Op == cephmsgr.MDSOpCreate && rR.Result == 0 && rR.Safe && !rR.IsDentry && !rR.IsTarget && rR.TraceBytes == 0 && rR.ExtraIno == e.ReplayedIno,
			"REPLAY answer differs: %+v", rR)
		check(len(log.find("already completed client."+strconv.FormatUint(gidA, 10)+":"+strconv.FormatUint(e.ReplayedTID, 10))) == 1, "log lacks the already-completed line")
		check(len(log.find("queuing replayed op")) >= 1 && len(log.find("queuing completed op")) >= 2, "log lacks the replay queue lines")
		// Retried create without REPLAY during clientreplay: opened, not
		// recreated; the reply carries the trace and the created inode.
		check(rL.Error == "" && rL.Op == cephmsgr.MDSOpCreate && rL.Result == 0 && rL.Safe && rL.IsDentry && rL.IsTarget && rL.TargetIno == inos["lost-file"] && rL.ExtraIno == inos["lost-file"] &&
			rL.DentryName == "lost-file", "retried create answer differs: %+v", rL)
		check(e.NativeListing["replayed"] == e.ReplayedIno && e.NativeListing["lost-file"] == inos["lost-file"] && e.NativeListing["lost-dir"] == inos["lost-dir"] &&
			e.NativeLostDirMode == 0o40755, "native listing differs: %+v", e.NativeListing)
		// Retried mkdir: converted to LOOKUP (dentry wanted) because the
		// completed request created an inode; the reply's op is LOOKUP.
		converted := log.find("completed request which created new inode 0x" + strconv.FormatUint(inos["lost-dir"], 16) + ", convert it to lookup request")
		check(len(converted) == 1, "log lacks the conversion of the retried mkdir: %v", e.MDSLogLines)
		if rM.Error != "" {
			e.ProductFinding = "the MDS answered the retried MKDIR of a completed request with a LOOKUP reply (Server::handle_client_request converts it); the Go client failed the transport: " + rM.Error
			findings = append(findings, e.ProductFinding)
			t.Errorf("product rejected the converted reply of the retried mkdir: %s", rM.Error)
		} else {
			// The session keeps running: the converted reply is accepted, it
			// carries the MDS's op (LOOKUP, dentry wanted), the created
			// directory as dentry and target, and arrives after the create's.
			check(rM.Op == cephmsgr.MDSOpLookup && rM.Result == 0 && rM.Safe && rM.IsDentry && rM.IsTarget && rM.TargetIno == inos["lost-dir"] && rM.DentryName == "lost-dir" &&
				rM.TargetMode == 0o40755 && rM.Arrival > rL.Arrival && rL.Arrival > rR.Arrival, "retried mkdir answer differs: %+v", rM)
			check(e.SessionError.Error == "" && e.SessionAfterReplies == fmt.Sprint(cephmsgr.MDSSessionOpen), "the session ended after the converted reply: %s %+v", e.SessionAfterReplies, e.SessionError)
		}
		if sessionA != nil {
			// Close the main session so later restarts wait for no client
			// that will not reconnect.
			if _, _, err := agentA.close(e.ReplayedIno, cephFileModeRDWR); err != nil {
				t.Error(err)
			}
			closeSession(t, sessionA, agentA, gidA)
			sessionA = nil
		}
	}) {
		if !consistent {
			return
		}
	}

	if consistent {
		leaf("reconnect_denied", func(t *testing.T) {
			e := &denyEv
			clientB, dialsB := client(t, "deny", false, control.mds.Endpoint)
			gidB, nonceB := clientB.Snapshot().GlobalID, clientB.Snapshot().CephFSNonce
			connB, sessionB, agentB, address := open(t, clientB, "deny")
			e.SessionAddress = address
			submit(t, sessionB, agentB, "lookup dir", lookup(1, plan.Directory), false)
			on := req(t, "deny-on")
			e.Central = on.Central
			if on.Central == nil || on.Central.State != "applied" || on.Central.DaemonValue != "true" {
				t.Fatal("deny-all reconnect setting was not applied", on.Central)
			}
			armed := req(t, "restart-arm")
			e.ArmedGID = armed.Restart.Armed.GID
			fired, _ := fire(t, sessionB)
			newRank := waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State != "up:replay" && d.State != "up:boot" })
			e.StateAtReconnect = newRank.State
			e.NewIncarnationConfig = req(t, "config-read").Config
			if e.NewIncarnationConfig["mds_deny_all_reconnect"] != "true" {
				t.Fatal("the new incarnation does not deny reconnects", e.NewIncarnationConfig)
			}
			req(t, "mds-log-mark")
			dialsB.allowMDS(newRank.Endpoint)
			if err := connB.Close(); err != nil {
				t.Fatal(err)
			}
			caps, realmState, err := agentB.reconnectState(realms)
			if err != nil {
				t.Fatal(err)
			}
			conn2, err := clientB.OpenMDS(ctx, target(*newRank))
			if err != nil {
				t.Fatal(err)
			}
			e.ReconnectAfterFireMicros = time.Since(fired).Microseconds()
			reconnectCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			s2, _, err := conn2.ReconnectSession(reconnectCtx, sessionB, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState}, sessionOptions("deny"))
			stop()
			e.ReconnectError = describeRecoveryError(err)
			if s2 != nil || !errors.Is(err, cephmsgr.ErrMDSSessionReconnectDenied) || !errors.Is(err, cephmsgr.ErrMDSSessionClosed) {
				t.Fatal("reconnect was not denied with CLOSE", err)
			}
			delete(agents, "deny")
			active := waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State == "up:active" })
			e.ActiveAfterFireMicros = time.Since(fired).Microseconds()
			e.MDSLogLines = logLines(t, gidB, nonceB)
			adopted := req(t, "restart-adopt")
			e.AdoptedGID = adopted.Restart.Adopted.GID
			if e.AdoptedGID != active.GID {
				t.Fatal("adopted incarnation differs")
			}
			r, n := observe(t, gidB)
			e.SessionAfter, e.Blocklisted = n, r.blocked(address)
			log := &nativeMDSCephFSCompletionLog{Lines: e.MDSLogLines}
			if n != nil || len(log.find("mds_deny_all_reconnect was set to speed up reboot phase, ignoring reconnect, sending close")) != 1 {
				t.Fatalf("denied session remains or the daemon log lacks the denial: %+v %v", n, e.MDSLogLines)
			}
			if e.Blocklisted == nil {
				t.Errorf("the denied client's address instance %s was not blocklisted", address)
			}
			// The same Client, a fresh session on a new connection.
			if err := conn2.Close(); err != nil {
				t.Fatal(err)
			}
			conn3, err := clientB.OpenMDS(ctx, target(control.mds))
			if err != nil {
				t.Fatal(err)
			}
			s3, err := conn3.OpenSession(ctx, sessionOptions("deny"))
			e.SameClientError = describeRecoveryError(err)
			var reject *cephmsgr.MDSSessionRejectedError
			if errors.As(err, &reject) {
				e.SameClientReject = reject
			}
			if err == nil {
				s3.Close(ctx)
				if e.Blocklisted != nil {
					t.Error("a blocklisted address instance opened a fresh session")
				}
			} else if e.Blocklisted == nil || reject == nil || !reject.Blocklisted && !strings.Contains(reject.Reason, "blocklisted") {
				t.Errorf("fresh session on the same client failed unexpectedly: %v", err)
			}
			conn3.Close()
			// A new Client (new address nonce) opens normally.
			clientB2, _ := client(t, "deny-new", false, control.mds.Endpoint)
			e.NewClientGlobalID = clientB2.Snapshot().GlobalID
			_, sessionB2, agentB2, _ := open(t, clientB2, "deny-new")
			ev := describeMDSSession("deny-new", identity, sessionB2)
			e.NewClientOpen = &ev
			closeSession(t, sessionB2, agentB2, e.NewClientGlobalID)
			e.NewClientClosed = true
			delete(agents, "deny-new")
			off := req(t, "deny-off")
			e.Restored = off.Central
			if off.Central == nil || off.Central.State != "restored" || off.Central.RestoredDaemon != "false" {
				t.Fatal("deny-all reconnect setting was not restored", off.Central)
			}
		})
	}

	if consistent {
		leaf("reconnect_window_missed", func(t *testing.T) {
			e := &windowEv
			write := req(t, "write-fenced")
			st := write.Native.Stat
			if write.Mutation == nil || write.Mutation.State != "applied" || st == nil || !st.Exists || st.Size != uint64(len(payload)) {
				t.Fatal("native fenced-file write did not complete", write.Mutation)
			}
			e.FencedIno, inos["fenced"] = st.Ino, st.Ino
			e.Object = fmt.Sprintf("%x.%08x", st.Ino, 0)
			clientC, dialsC := client(t, "window", true, control.mds.Endpoint)
			gidC, nonceC := clientC.Snapshot().GlobalID, clientC.Snapshot().CephFSNonce
			connC, sessionC, agentC, address := open(t, clientC, "window")
			e.SessionAddress = address
			submit(t, sessionC, agentC, "lookup dir", lookup(1, plan.Directory), false)
			var err error
			if e.Placement, err = recoveryObjectPlacement(ctx, clientC, oracle.DataPoolName, e.Object); err != nil {
				t.Fatal(err)
			}
			e.OSDAddress = osdDumpAddresses(t, ctx, clientC)[e.Placement.Primary]
			osd, err := clientC.OpenOSD(ctx, cephmsgr.OSDTarget{ID: e.Placement.Primary, Address: e.OSDAddress})
			if err != nil {
				t.Fatal("open primary OSD", err)
			}
			request := func(p recoveryPlacement) cephmsgr.OSDRequest {
				return cephmsgr.OSDRequest{MapEpoch: p.Epoch, Object: cephmsgr.OSDObject{Pool: oracle.DataPool, Name: e.Object, Hash: p.Hash},
					Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDStat}, {Code: cephmsgr.OSDRead, Length: uint64(len(payload))}}}
			}
			e.ReadBefore = recoveryOSDRead(ctx, osd, request(e.Placement), payload)
			if r := e.ReadBefore; r.Error != "" || r.Code != 0 || !r.DataEqual || r.GlobalID != gidC {
				t.Fatalf("data object read before the restart differs: %+v", r)
			}
			on := req(t, "window-on")
			e.Central = on.Central
			if on.Central == nil || on.Central.State != "applied" {
				t.Fatal("reconnect timeout setting was not applied", on.Central)
			}
			armed := req(t, "restart-arm")
			e.ArmedGID = armed.Restart.Armed.GID
			fired, _ := fire(t, sessionC)
			waitRank(t, 60*time.Second, &e.StatusTimeline, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State != "up:boot" })
			e.NewIncarnationConfig = req(t, "config-read").Config
			window, _ := strconv.ParseFloat(plan.ReconnectTimeout, 64)
			if got, err := strconv.ParseFloat(e.NewIncarnationConfig["mds_reconnect_timeout"], 64); err != nil || got != window || window <= 0 {
				t.Fatal("the new incarnation does not use the short reconnect window", e.NewIncarnationConfig)
			}
			req(t, "mds-log-mark")
			active := waitRank(t, 90*time.Second, &e.StatusTimeline, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State == "up:active" })
			e.ActiveAfterFireMicros = time.Since(fired).Microseconds()
			e.MDSLogLines = logLines(t, gidC, nonceC)
			adopted := req(t, "restart-adopt")
			e.AdoptedGID = adopted.Restart.Adopted.GID
			if e.AdoptedGID != active.GID {
				t.Fatal("adopted incarnation differs")
			}
			r, n := observe(t, gidC)
			e.Blocklisted = r.blocked(address)
			log := &nativeMDSCephFSCompletionLog{Lines: e.MDSLogLines}
			if n != nil || e.Blocklisted == nil || len(log.find("reconnect gives up on client."+strconv.FormatUint(gidC, 10))) != 1 {
				t.Fatalf("the late session was not evicted and blocklisted at the window's end: %+v %+v %v", n, e.Blocklisted, e.MDSLogLines)
			}
			req(t, "mds-log-mark")
			dialsC.allowMDS(control.mds.Endpoint)
			if err := connC.Close(); err != nil {
				t.Fatal(err)
			}
			caps, realmState, err := agentC.reconnectState(realms)
			if err != nil {
				t.Fatal(err)
			}
			conn2, err := clientC.OpenMDS(ctx, target(control.mds))
			if err != nil {
				t.Fatal(err)
			}
			e.ReconnectAfterFire = time.Since(fired).Microseconds()
			reconnectCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			s2, _, err := conn2.ReconnectSession(reconnectCtx, sessionC, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState}, sessionOptions("window"))
			stop()
			e.ReconnectError = describeRecoveryError(err)
			e.ReconnectLogLines = logLines(t, gidC, nonceC)
			if s2 != nil || !errors.Is(err, cephmsgr.ErrMDSSessionReconnectDenied) {
				t.Fatal("a reconnect after the window was not denied with CLOSE", err)
			}
			delete(agents, "window")
			conn2.Close()
			if e.PlacementAfter, err = recoveryObjectPlacement(ctx, clientC, oracle.DataPoolName, e.Object); err != nil {
				t.Fatal(err)
			}
			e.ReadAfter = recoveryOSDRead(ctx, osd, request(e.PlacementAfter), payload)
			if r := e.ReadAfter; r.Error == "" || r.DataBytes != 0 || r.OSDErrCode != -108 {
				t.Errorf("the fenced address instance's OSD read did not fail with -108: %+v", r)
			}
			reader := recoveryClient(t, ctx, identity, keyFile, newRecoveryDials(true), func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = false, false, true })
			control.goGIDs[reader.Snapshot().GlobalID] = true
			readerOSD, err := reader.OpenOSD(ctx, cephmsgr.OSDTarget{ID: e.PlacementAfter.Primary, Address: e.OSDAddress})
			if err != nil {
				t.Fatal(err)
			}
			e.ReaderRead = recoveryOSDRead(ctx, readerOSD, request(e.PlacementAfter), payload)
			if r := e.ReaderRead; r.Error != "" || r.Code != 0 || !r.DataEqual {
				t.Fatalf("a separate client could not read the object: %+v", r)
			}
			off := req(t, "window-off")
			e.Restored = off.Central
			if off.Central == nil || off.Central.State != "restored" {
				t.Fatal("reconnect timeout setting was not restored", off.Central)
			}
		})
	}

	clientE, dialsE := client(t, "readonly", false, control.mds.Endpoint)
	gidE, nonceE := clientE.Snapshot().GlobalID, clientE.Snapshot().CephFSNonce
	var connE *cephmsgr.MDSConnection
	var sessionE *cephmsgr.MDSSession
	var agentE *cephfsCapAgent
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if sessionE != nil {
			sessionE.Close(closing)
		}
	})
	if consistent {
		leaf("force_readonly", func(t *testing.T) {
			e := &roEv
			connE, sessionE, agentE, _ = open(t, clientE, "readonly")
			_, _, trace := submit(t, sessionE, agentE, "create ro-file", create(dirIno, "ro-file"), true)
			e.FileIno, inos["ro-file"] = trace.Target.Ino, trace.Target.Ino
			agentE.open(e.FileIno, cephFileModeRDWR)
			c, _ := agentE.cap(e.FileIno)
			e.FileCapsBefore = cephCapString(c.Issued)
			before := agentE.snapshot()
			e.PushBefore = sessionE.Status().PushSequence
			req(t, "mds-log-mark")
			sent := time.Now()
			ro := req(t, "force-readonly")
			e.ForceRO = ro.Mutation
			deadline := time.Now().Add(10 * time.Second)
			for {
				s := agentE.snapshot()
				if len(s.ReadOnly) > len(before.ReadOnly) {
					e.ForceROAfterMicros = s.ReadOnly[len(s.ReadOnly)-1].Sub(sent).Microseconds()
					for _, rec := range s.Records[len(before.Records):] {
						if rec.Kind == "session" && rec.Op == cephmsgr.MDSSessionOpForceReadOnly {
							e.ForceROArrival = rec.Arrival
						}
						if rec.Kind == "caps" || rec.Kind == "update" {
							e.RevokesAfter = append(e.RevokesAfter, rec)
						}
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no FORCE_RO session push arrived")
				}
				time.Sleep(20 * time.Millisecond)
			}
			e.StatusReadOnly, e.PushAfter = sessionE.Status().ReadOnly, sessionE.Status().PushSequence
			if !e.StatusReadOnly || e.ForceROArrival == 0 {
				t.Fatal("the session did not report read-only", e)
			}
			req(t, "debug-on")
			call, first, _ := submit(t, sessionE, agentE, "mkdir ro-dir (read-only)", mkdir(dirIno, "ro-dir"), false)
			e.MkdirReply = describeCompletionReply("mkdir ro-dir (read-only)", call, first, nil, first.Received)
			call, first, _ = submit(t, sessionE, agentE, "lookup ro-file (read-only)", lookup(dirIno, "ro-file"), false)
			e.LookupReply = describeCompletionReply("lookup ro-file (read-only)", call, first, nil, first.Received)
			call, first, _ = submit(t, sessionE, agentE, "getattr ro-file (read-only)", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll),
				Path: cephmsgr.MDSFilePath{Ino: e.FileIno}}, false)
			e.GetReply = describeCompletionReply("getattr ro-file (read-only)", call, first, nil, first.Received)
			e.MDSLogLines = logLines(t, gidE, nonceE)
			req(t, "debug-off")
			if m := e.MkdirReply; m.Result != -30 || !m.Safe || m.IsTarget || m.Op != cephmsgr.MDSOpMkdir {
				t.Fatalf("mutation on a read-only MDS did not fail with EROFS: %+v", m)
			}
			if e.LookupReply.Result != 0 || e.LookupReply.TargetIno != e.FileIno || e.GetReply.Result != 0 || e.GetReply.TargetIno != e.FileIno {
				t.Fatalf("reads on a read-only MDS failed: %+v %+v", e.LookupReply, e.GetReply)
			}
			if len((&nativeMDSCephFSCompletionLog{Lines: e.MDSLogLines}).find("force file system read-only")) == 0 {
				t.Error("the daemon log lacks the read-only line")
			}
			// Restore with a restart: read-only is in-memory MDCache state.
			armed := req(t, "restart-arm")
			e.ArmedGID = armed.Restart.Armed.GID
			fired, _ := fire(t, sessionE)
			newRank := waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State != "up:replay" && d.State != "up:boot" })
			if newRank.State != "up:reconnect" {
				t.Fatal("the restarted MDS is not waiting in up:reconnect", newRank)
			}
			dialsE.allowMDS(newRank.Endpoint)
			if err := connE.Close(); err != nil {
				t.Fatal(err)
			}
			caps, realmState, err := agentE.reconnectState(realms)
			if err != nil {
				t.Fatal(err)
			}
			connE, err = clientE.OpenMDS(ctx, target(*newRank))
			if err != nil {
				t.Fatal(err)
			}
			reconnectCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			s2, _, err := connE.ReconnectSession(reconnectCtx, sessionE, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState}, sessionOptions("readonly"))
			stop()
			e.ReconnectError = describeRecoveryError(err)
			if err != nil {
				t.Fatal("reconnect after the read-only restart", err)
			}
			sessionE, agentE = s2, startCephFSCapAgentFrom(ctx, s2, agentE)
			agents["readonly"] = agentE
			e.ReadOnlyAfterReconnect = sessionE.Status().ReadOnly
			waitRank(t, 60*time.Second, nil, fired, func(d *nativeMDSCephFSDaemon) bool { return d.State == "up:active" })
			adopted := req(t, "restart-adopt")
			e.AdoptedGID = adopted.Restart.Adopted.GID
			call, first, trace = submit(t, sessionE, agentE, "mkdir ro-dir", mkdir(dirIno, "ro-dir"), true)
			e.MkdirAfterReply = describeCompletionReply("mkdir ro-dir", call, first, nil, first.Received)
			if e.ReadOnlyAfterReconnect || first.Result != 0 || trace.Target == nil {
				t.Fatalf("the restarted MDS is still read-only: %+v", e.MkdirAfterReply)
			}
			inos["ro-dir"] = trace.Target.Ino
			if _, _, err := agentE.close(e.FileIno, cephFileModeRDWR); err != nil {
				t.Fatal(err)
			}
		})
	}

	if consistent && sessionE != nil && sessionE.Status().State == cephmsgr.MDSSessionOpen {
		leaf("cap_snap_flush", func(t *testing.T) {
			e := &snapEv
			_, _, dtrace := submit(t, sessionE, agentE, "mkdir snap", mkdir(dirIno, "snap"), true)
			e.SnapDirIno, inos["snap"] = dtrace.Target.Ino, dtrace.Target.Ino
			_, _, ftrace := submit(t, sessionE, agentE, "create snap-file", create(e.SnapDirIno, "snap-file"), true)
			e.FileIno, inos["snap/snap-file"] = ftrace.Target.Ino, ftrace.Target.Ino
			agentE.open(e.FileIno, cephFileModeRDWR)
			e.CapBefore, _ = agentE.cap(e.FileIno)
			if e.CapBefore.Issued&cephCapFileWR == 0 {
				t.Fatal("the open file does not hold Fw", cephCapString(e.CapBefore.Issued))
			}
			realm, ok := realms[e.CapBefore.Realm]
			if !ok {
				t.Fatal("no snap trace describes the file's realm", e.CapBefore.Realm)
			}
			e.FollowsRealm = realm
			before := agentE.snapshot()
			req(t, "mds-log-mark")
			req(t, "debug-on")
			created := req(t, "snapshot-create")
			e.Snapshot = created.Native
			if created.Native.Stat == nil || !created.Native.Stat.Exists {
				t.Fatal("native snapshot creation differs", created.Native)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				s := agentE.snapshot()
				e.SnapEvents = s.Snaps[len(before.Snaps):]
				if slices.ContainsFunc(e.SnapEvents, func(ev cephfsSnapEvent) bool {
					return ev.Op == cephmsgr.MDSSnapOpSplit && ev.Split == e.SnapDirIno && slices.Contains(ev.Moved, e.FileIno)
				}) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("no snap SPLIT moved the file into the new realm: %+v", e.SnapEvents)
				}
				time.Sleep(20 * time.Millisecond)
			}
			e.CapAfterSplit, _ = agentE.cap(e.FileIno)
			c := e.CapAfterSplit
			// Client::flush_snaps: the cap snap follows the old realm sequence
			// and carries the dirty Fw state with this client's flush TID.
			e.FlushTID = 1
			e.FlushSnap = cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpFlushSnap, Ino: e.FileIno, Realm: c.Realm, CapID: c.CapID, MigrateSeq: c.MSeq, Caps: c.Issued, Dirty: cephCapFileWR,
				SnapFollows: e.FollowsRealm.Seq, UID: c.UID, GID: c.GID, Mode: c.Mode, Size: c.Size, MTime: c.MTime, ATime: c.ATime, CTime: c.CTime, TimeWarpSeq: c.TimeWarpSeq,
				OldestFlushTID: e.FlushTID, BTime: c.BTime, ChangeAttr: c.ChangeAttr, Files: -1, Subdirs: -1}
			if err := agentE.expectFlushSnap(e.FileIno, e.FollowsRealm.Seq, e.FlushTID, cephCapFileWR); err != nil {
				t.Fatal(err)
			}
			sent := time.Now()
			if err := sessionE.SendCaps(ctx, e.FlushSnap, e.FlushTID); err != nil {
				t.Fatal("send FLUSHSNAP", err)
			}
			deadline = time.Now().Add(10 * time.Second)
			for {
				s := agentE.snapshot()
				if len(s.SnapAcks) > len(before.SnapAcks) {
					ack := s.SnapAcks[len(s.SnapAcks)-1]
					e.Ack, e.AckAfterMicros = &ack, ack.Received.Sub(sent).Microseconds()
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no FLUSHSNAP_ACK arrived")
				}
				time.Sleep(20 * time.Millisecond)
			}
			e.MDSLogLines = logLines(t, gidE, nonceE)
			req(t, "debug-off")
			a := e.Ack.Caps
			if e.Ack.Transaction != e.FlushTID || a.Op != cephmsgr.MDSCapOpFlushSnapAck || a.Ino != e.FileIno || a.SnapFollows != e.FollowsRealm.Seq || a.Dirty != cephCapFileWR {
				t.Fatalf("FLUSHSNAP_ACK differs: %+v", e.Ack)
			}
			removed := req(t, "snapshot-remove")
			e.SnapshotRemoved = removed.Native
			if _, _, err := agentE.close(e.FileIno, cephFileModeRDWR); err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Run("cleanup_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		if !consistent {
			t.Skip("an owned restart is pending; cleanup is left to the fixture")
		}
		s, a, gid := sessionE, agentE, gidE
		if s == nil || s.Status().State != cephmsgr.MDSSessionOpen {
			if connE != nil {
				connE.Close()
			}
			connE, s, a, _ = open(t, clientE, "cleanup")
			sessionE, agentE = s, a
		}
		remove := func(parent uint64, name string, dir bool) {
			op := cephmsgr.MDSOpUnlink
			if dir {
				op = cephmsgr.MDSOpRmdir
			}
			_, first, _ := submit(t, s, a, "remove "+name, cephmsgr.MDSRequest{Op: op, Path: cephmsgr.MDSFilePath{Ino: parent, Path: name}}, true)
			if first.Result != 0 && first.Result != -2 {
				t.Fatalf("remove %s: %d", name, first.Result)
			}
		}
		if ino := inos["lease"]; ino != 0 {
			remove(ino, "leased", false)
			remove(dirIno, "lease", true)
		}
		if ino := inos["snap"]; ino != 0 {
			remove(ino, "snap-file", false)
			remove(dirIno, "snap", true)
		}
		for _, name := range []string{"replayed", "lost-file", "fenced", "ro-file"} {
			if inos[name] != 0 {
				remove(dirIno, name, false)
			}
		}
		for _, name := range []string{"lost-dir", "ro-dir"} {
			if inos[name] != 0 {
				remove(dirIno, name, true)
			}
		}
		remove(1, plan.Directory, true)
		st, err := control.stat(ctx, "dir")
		if err != nil {
			t.Fatal(err)
		}
		if st.Exists || st.Errno != 2 {
			t.Fatal("native stat still finds the completion directory", st)
		}
		closeSession(t, s, a, gid)
		sessionE = nil
		facts["final_inos"] = maps.Clone(inos)
	})
}
