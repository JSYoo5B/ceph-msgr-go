package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSRecoveryIntegration accepts journal flush, fencing through
// the shared CephFS address nonce, and CephFS reconnect with request
// resubmission after a restart of the owned rank-0 MDS daemon, against live
// Tentacle daemons with native oracles. See integration/fixture/
// MDS_CEPHFS_FIXTURE.md and MDS_CEPHFS_RECOVERY_VALIDATION.md.

type nativeMDSCephFSRecoveryPlan struct {
	Directory        string   `json:"directory"`
	FlushPlain       []string `json:"flush_plain"`
	FlushRequested   []string `json:"flush_requested"`
	Fenced           string   `json:"fenced"`
	Held             string   `json:"held"`
	Replayed         string   `json:"replayed"`
	Unanswered       string   `json:"unanswered"`
	FencedPayloadHex string   `json:"fenced_payload_hex"`
}

type nativeMDSCephFSRecoveryOracle struct {
	nativeMDSCephFSOracle
	RecoveryIdentity string                      `json:"recovery_identity"`
	RecoveryCaps     map[string]string           `json:"recovery_caps"`
	RecoveryPlan     nativeMDSCephFSRecoveryPlan `json:"recovery_plan"`
	DataPool         int64                       `json:"data_pool"`
	DataPoolName     string                      `json:"data_pool_name"`
}

// nativeMDSCephFSDaemon is one MDS daemon incarnation as the fixture reports it.
type nativeMDSCephFSDaemon struct {
	Name        string `json:"name"`
	GID         uint64 `json:"gid"`
	Rank        int32  `json:"rank"`
	State       string `json:"state"`
	Incarnation int32  `json:"incarnation"`
	Endpoint    string `json:"endpoint"`
	Nonce       uint32 `json:"nonce"`
}

type nativeMDSCephFSTarget struct {
	Source  string                `json:"source_commit"`
	Token   string                `json:"token"`
	FSID    string                `json:"fsid"`
	FSCID   int32                 `json:"fscid"`
	FSName  string                `json:"fs_name"`
	MDS     nativeMDSCephFSDaemon `json:"mds"`
	Adopted int                   `json:"adopted_restarts"`
}

// readMDSCephFSTarget reads the owned rank-0 incarnation the fixture currently
// proves: the startup daemon, or the incarnation it adopted after the owned
// restart (same daemon name, new GID and nonce).
func readMDSCephFSTarget(dir string, fresh nativeMDSFresh) (nativeMDSCephFSTarget, error) {
	var target nativeMDSCephFSTarget
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-target.json"), &target); err != nil {
		return target, err
	}
	m := target.MDS
	if target.Source != mdsFreshSource || target.Token != fresh.Token || target.FSID != fresh.FSID || target.FSCID != fresh.FSCID || target.FSName != fresh.FSName ||
		m.Name != fresh.MDS.Name || m.Rank != 0 || m.State != "up:active" || m.GID == 0 || m.Nonce == 0 || m.Endpoint == "" || target.Adopted < 0 || target.Adopted > 1 ||
		target.Adopted == 0 && (m.GID != fresh.MDS.GID || m.Nonce != fresh.MDS.Nonce || m.Endpoint != fresh.MDS.Endpoint) ||
		target.Adopted == 1 && (m.GID == fresh.MDS.GID || m.Nonce == fresh.MDS.Nonce) {
		return target, errors.New("owned current MDS target differs from the fresh daemon")
	}
	return target, nil
}

type nativeMDSCephFSRestart struct {
	State          string                   `json:"state"`
	ArmRequestID   int64                    `json:"arm_request_id"`
	FireRequestID  int64                    `json:"fire_request_id"`
	AdoptRequestID int64                    `json:"adopt_request_id"`
	Armed          nativeMDSCephFSDaemon    `json:"armed"`
	ArmedAddresses []string                 `json:"armed_blocklist_addresses"`
	Output         string                   `json:"native_output"`
	Error          string                   `json:"error"`
	FireStartedUTC string                   `json:"fire_started_utc"`
	FiredUTC       string                   `json:"fired_utc"`
	Adopted        *nativeMDSCephFSDaemon   `json:"adopted"`
	AdoptedUTC     string                   `json:"adopted_utc"`
	OldFencing     []nativeMDSSessionsBlock `json:"old_incarnation_blocklist"`
}

type nativeMDSCephFSHistoricOp struct {
	Description string   `json:"description"`
	InitiatedAt string   `json:"initiated_at"`
	Duration    float64  `json:"duration"`
	FlagPoint   string   `json:"flag_point"`
	Result      *int64   `json:"result"`
	Events      []string `json:"events"`
}

type nativeMDSCephFSRankStatus struct {
	FSMapEpoch     uint32                   `json:"fsmap_epoch"`
	MDSMapEpoch    uint32                   `json:"mdsmap_epoch"`
	Failed         []int                    `json:"failed"`
	Rank0          *nativeMDSCephFSDaemon   `json:"rank0"`
	Daemons        []nativeMDSCephFSDaemon  `json:"daemons"`
	Standbys       []nativeMDSCephFSDaemon  `json:"owned_standbys"`
	ArmedPresent   bool                     `json:"armed_gid_present"`
	ArmedBlocklist []nativeMDSSessionsBlock `json:"armed_blocklist"`
}

// mdsCephFSRecoveryControl drives the closed fixture control for this test.
// One request is outstanding at a time; the fixture slot is single-entry.
type mdsCephFSRecoveryControl struct {
	mu       sync.Mutex
	dir      string
	token    string
	fresh    nativeMDSFresh
	mds      nativeMDSCephFSDaemon
	last     int64
	goGIDs   map[uint64]bool
	receipts []nativeMDSCephFSReceipt
}

func (x *mdsCephFSRecoveryControl) request(ctx context.Context, action, arg string) (nativeMDSCephFSReceipt, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
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
	x.receipts = append(x.receipts, result)
	f := x.fresh
	if result.Source != mdsFreshSource || result.Token != x.token || result.RequestID <= x.last || result.Action != action || result.FSID != f.FSID || result.FSCID != f.FSCID ||
		result.FSName != f.FSName || result.Rank != 0 || !result.Complete || result.Error != "" {
		return result, fmt.Errorf("native MDS CephFS recovery receipt scope differs: %s", result.Error)
	}
	x.last = result.RequestID
	expected := x.mds
	if action == "restart-adopt" {
		r := result.Restart
		if r == nil || r.State != "adopted" || r.Adopted == nil || r.Armed.GID != x.mds.GID {
			return result, errors.New("native MDS restart adoption receipt differs")
		}
		a := *r.Adopted
		if a.Name != x.mds.Name || a.GID == x.mds.GID || a.Nonce == x.mds.Nonce || a.Nonce == 0 || a.Rank != 0 || a.State != "up:active" || a.Incarnation == x.mds.Incarnation {
			return result, errors.New("adopted MDS incarnation is not a new incarnation of the owned daemon")
		}
		expected = a
	}
	if result.MDSGID != expected.GID || result.MDSEndpoint != expected.Endpoint || result.MDSNonce != expected.Nonce || result.MDSIncarnation != expected.Incarnation {
		return result, errors.New("native MDS CephFS recovery receipt names a different MDS incarnation")
	}
	switch action {
	case "restart-fire", "restart-status":
		if result.Restart == nil || result.Restart.Armed.GID != x.mds.GID || action == "restart-status" && result.RankStatus == nil {
			return result, errors.New("native MDS restart receipt is incomplete")
		}
	default:
		if !result.IdentityChecked || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
			return result, errors.New("native MDS CephFS recovery receipt lacks its identity proof")
		}
	}
	switch action {
	case "observe":
		if strconv.FormatUint(result.GlobalID, 10) != arg || result.Sessions == nil {
			return result, errors.New("native MDS CephFS observation scope differs")
		}
	case "evict":
		m := result.Mutation
		if m == nil || m.Kind != "eviction" || m.State != "evicted" || m.Until == "" || m.Address == "" || strconv.FormatUint(m.GlobalID, 10) != arg {
			return result, errors.New("native MDS CephFS eviction receipt differs")
		}
	case "restart-arm":
		if result.Restart == nil || result.Restart.State != "armed" || result.Restart.Armed.GID != x.mds.GID || result.Restart.Armed.Nonce != x.mds.Nonce || len(result.Restart.ArmedAddresses) == 0 {
			return result, errors.New("native MDS restart arm receipt differs")
		}
	case "recovery-ops":
		if strconv.FormatUint(result.GlobalID, 10) != arg {
			return result, errors.New("native MDS historic ops scope differs")
		}
	case "recovery-stat", "recovery-list", "recovery-write", "flock-ex-nb":
		if result.Native == nil || result.Native.NativeGlobalID == 0 || x.goGIDs[result.Native.NativeGlobalID] {
			return result, errors.New("native CephFS recovery oracle output missing or bound to a Go client")
		}
	}
	x.mds = expected
	return result, nil
}

func (x *mdsCephFSRecoveryControl) stat(ctx context.Context, target string) (nativeMDSCephFSStat, error) {
	r, err := x.request(ctx, "recovery-stat", target)
	if err != nil {
		return nativeMDSCephFSStat{}, err
	}
	if r.Native.Stat == nil {
		return nativeMDSCephFSStat{}, errors.New("native recovery stat missing")
	}
	return *r.Native.Stat, nil
}

// recoveryDial is one dial of an MDS endpoint by a recovery client.
type recoveryDial struct {
	At      time.Time
	Address string
	HeldFor time.Duration `json:",omitempty"`
	Err     string        `json:",omitempty"`
}

// recoveryDials relays and records MDS dials of one client, optionally
// holding the next one (a test-only control of the client's own dial path),
// and admits or refuses OSD dials.
type recoveryDials struct {
	osd      bool
	MDS, OSD atomic.Uint32
	mu       sync.Mutex
	mds      map[string]bool
	relays   []*testcluster.StreamRelay
	log      []recoveryDial
	hold     chan struct{}
	holding  chan struct{}
}

func newRecoveryDials(osd bool, endpoints ...string) *recoveryDials {
	d := &recoveryDials{osd: osd, mds: map[string]bool{}}
	for _, e := range endpoints {
		d.mds[e] = true
	}
	return d
}

func (d *recoveryDials) allowMDS(endpoint string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mds[endpoint] = true
}

// armHold makes the next MDS dial wait until release is called (bounded to
// 10 s). holding closes when that dial starts waiting.
func (d *recoveryDials) armHold() (holding <-chan struct{}, release func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	hold, signal := make(chan struct{}), make(chan struct{})
	d.hold, d.holding = hold, signal
	var once sync.Once
	return signal, func() { once.Do(func() { close(hold) }) }
}

func (d *recoveryDials) attempts() []recoveryDial {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.log)
}

// recoveryClient dials one client of the owned recovery identity. MDS
// endpoints pass through counted, closable relays; OSD dials are admitted only
// when the client is meant to reach an OSD.
func recoveryClient(t *testing.T, ctx context.Context, identity, keyPath string, dials *recoveryDials, configure func(*cephmsgr.Options)) *cephmsgr.Client {
	t.Helper()
	options := integrationOptions(t)
	encoded, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal("read recovery credential", err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	want, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != want {
		t.Fatal("recovery credential does not match selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	options.Identity, options.MaxMDSConnections = identity, 1
	options.EnableMDSMaps = false
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Hour, 2*time.Hour
	options.ConnectTimeout = 5 * time.Second
	options.MaxFrameSize = 1 << 20
	configure(&options)
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, address string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(address)
		if strings.HasPrefix(role, "osd.") {
			dials.OSD.Add(1)
			if !dials.osd {
				return nil, errors.New("this recovery client must not dial an OSD")
			}
			return dial(call, network, address)
		}
		dials.mu.Lock()
		isMDS := dials.mds[address]
		hold, holding := dials.hold, dials.holding
		if isMDS {
			dials.hold, dials.holding = nil, nil
		}
		dials.mu.Unlock()
		if !isMDS {
			return dial(call, network, address)
		}
		dials.MDS.Add(1)
		record := recoveryDial{At: time.Now(), Address: address}
		if hold != nil {
			close(holding)
			select {
			case <-hold:
			case <-call.Done():
			case <-time.After(10 * time.Second):
			}
			record.HeldFor = time.Since(record.At)
		}
		remote, err := dial(call, network, address)
		dials.mu.Lock()
		defer dials.mu.Unlock()
		if err != nil {
			record.Err = err.Error()
			dials.log = append(dials.log, record)
			return nil, err
		}
		relay := testcluster.NewStreamRelay(remote)
		dials.relays = append(dials.relays, relay)
		dials.log = append(dials.log, record)
		return relay, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate recovery client", err)
	}
	t.Cleanup(func() {
		c.Close()
		dials.mu.Lock()
		defer dials.mu.Unlock()
		for _, relay := range dials.relays {
			relay.Close()
			relay.Wait()
		}
	})
	return c
}

// recoveryPlacement is the caller-side placement of one data object, read
// from the MON's `osd map` (test-only interpretation of native JSON).
type recoveryPlacement struct {
	Epoch   uint32  `json:"epoch"`
	PoolID  int64   `json:"pool_id"`
	Object  string  `json:"objname"`
	RawPGID string  `json:"raw_pgid"`
	PGID    string  `json:"pgid"`
	Up      []int32 `json:"up"`
	Acting  []int32 `json:"acting"`
	Primary int32   `json:"acting_primary"`
	Hash    uint32  `json:"-"`
}

func recoveryObjectPlacement(ctx context.Context, c *cephmsgr.Client, pool, object string) (recoveryPlacement, error) {
	var p recoveryPlacement
	command, _ := json.Marshal(map[string]string{"prefix": "osd map", "pool": pool, "object": object, "format": "json"})
	result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: command})
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(result.Data, &p); err != nil || p.Epoch == 0 || p.Object != object || len(p.Acting) == 0 {
		return p, fmt.Errorf("native osd map output differs: %s", result.Data)
	}
	_, seed, ok := strings.Cut(p.RawPGID, ".")
	hash, err := strconv.ParseUint(seed, 16, 32)
	if !ok || err != nil || p.RawPGID != fmt.Sprintf("%d.%s", p.PoolID, seed) {
		return p, fmt.Errorf("native raw PG differs: %s", p.RawPGID)
	}
	p.Hash = uint32(hash)
	return p, nil
}

type recoveryRequestEvidence struct {
	Name                 string
	Session              string
	Op                   uint32
	Transaction          uint64
	MDSMapEpoch          uint32
	FirstSafe            bool
	FirstResult          int32
	FirstArrival         uint64
	FirstMicros          int64
	FlushRequested       bool
	FlushAfterMicros     int64 `json:",omitempty"`
	SafeResult           int32
	SafeArrival          uint64
	SafeMicros           int64
	UnsafeToSafeMicros   int64
	IsDentry, IsTarget   bool
	ExtraHex             string `json:",omitempty"`
	TargetIno            uint64 `json:",omitempty"`
	TargetCaps           string `json:",omitempty"`
	SafeTraceBytes       int
	SafeExtraHex         string `json:",omitempty"`
	ResubmittedFrom      uint64 `json:",omitempty"`
	Error                string `json:",omitempty"`
	submitted, firstSeen time.Time
}

type recoveryErrorChain struct {
	Error                     string
	SessionReset              bool
	EOF                       bool
	ConnectionRefused         bool
	ResumeDeadline            bool
	OutcomeUnknown            bool
	MDSSessionClosed          bool
	MDSSessionReconnectDenied bool
}

func describeRecoveryError(err error) recoveryErrorChain {
	if err == nil {
		return recoveryErrorChain{}
	}
	var unknown *cephmsgr.OutcomeUnknownError
	return recoveryErrorChain{Error: err.Error(), SessionReset: errors.Is(err, session.ErrSessionReset), EOF: errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF),
		ConnectionRefused: errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(err.Error(), "connection refused"), ResumeDeadline: errors.Is(err, context.DeadlineExceeded),
		OutcomeUnknown: errors.As(err, &unknown), MDSSessionClosed: errors.Is(err, cephmsgr.ErrMDSSessionClosed), MDSSessionReconnectDenied: errors.Is(err, cephmsgr.ErrMDSSessionReconnectDenied)}
}

type recoveryRestartEvidence struct {
	ArmedGID, AdoptedGID                 uint64
	ArmedNonce, AdoptedNonce             uint32
	ArmedIncarnation, AdoptedIncarnation int32
	ArmedEndpoint, AdoptedEndpoint       string
	TickSyncedByUnsafeSafe               bool
	HeldCreateUnsafeToSafeMicros         int64
	ReplayedUnsafeAfterTickMicros        int64
	FireStarted, FireAcked               time.Time
	FireAckMicros                        int64
	RedialHeldAt                         time.Time
	RedialAfterFireMicros                int64
	UnansweredSubmitError                string `json:",omitempty"`
	UnansweredSubmittedAfterFireMicros   int64
	LostAt                               time.Time
	LostAfterFireMicros                  int64
	LostAfterRedialMicros                int64
	ResumeError                          recoveryErrorChain
	ResumesBeforeLost                    uint64
	ConnectionDone                       bool
	MDSDials                             []recoveryDial
	ReplayedBeforeLost                   recoveryErrorChain
	ReplayedFirstStillUnsafe             bool
	UnansweredBeforeLost                 recoveryErrorChain
	StatusTimeline                       []recoveryStatusSample
	ReconnectStateSeen                   string
	ReconnectStarted                     time.Time
	ReconnectAfterFireMicros             int64
	ReconnectAfterLostMicros             int64
	ReconnectMicros                      int64
	ReconnectError                       recoveryErrorChain
	ReconnectCaps                        []cephmsgr.MDSCapReconnect
	ReconnectRealms                      []cephmsgr.MDSRealmReconnect
	Reopened                             *mdsSessionEvidence
	ReconnectedFrom                      uint64
	LostGeneration                       uint64
	ActiveAfterFireMicros                int64
	OldIncarnationFencing                []nativeMDSSessionsBlock
	ReplayedHistoricOp                   *nativeMDSCephFSHistoricOp
	UnansweredHistoricOp                 *nativeMDSCephFSHistoricOp
	HistoricOps                          []nativeMDSCephFSHistoricOp
	ReplayExecutedByNewIncarnation       bool
	NativeSessionAfter                   *nativeMDSSession
	ReplayedIno, UnansweredIno           uint64
	ReplayedNativeIno, UnansweredNative  uint64
}

type recoveryStatusSample struct {
	AfterFireMillis int64
	FSMapEpoch      uint32
	Rank0           *nativeMDSCephFSDaemon
	Failed          []int
	ArmedPresent    bool
	ArmedBlocklist  []nativeMDSSessionsBlock
}

type recoveryFencingEvidence struct {
	ClientGlobalID, ReaderGlobalID uint64
	CephFSNonce, ReaderNonce       uint32
	NativeSessionAddress           string
	ClientIP                       string
	FencedIno                      uint64
	Object                         string
	Placement, PlacementAfter      recoveryPlacement
	OSDAddress                     string
	ReadBefore                     recoveryOSDResult
	EvictionMutation               any
	NewBlocklist                   []nativeMDSSessionsBlock
	SessionState                   cephmsgr.MDSSessionState
	SessionError                   recoveryErrorChain
	LostAfterEvictMillis           int64
	EvictAckMillis                 int64
	MDSResumes                     uint64
	MDSDials                       []recoveryDial
	ReadAfter                      recoveryOSDResult
	ReaderRead                     recoveryOSDResult
	OSDDials                       [2]uint32
}

type recoveryOSDResult struct {
	Code        int32
	Error       string `json:",omitempty"`
	OSDErrCode  int32  `json:",omitempty"`
	Transaction uint64
	GlobalID    uint64
	MapEpoch    uint32
	PGPool      uint64
	PGSeed      uint32
	Operations  []int32
	DataBytes   int
	DataEqual   bool
	StatSize    uint64
	Micros      int64
}

func recoveryOSDRead(ctx context.Context, o *cephmsgr.OSDConnection, request cephmsgr.OSDRequest, payload []byte) recoveryOSDResult {
	begin := time.Now()
	ctx, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	reply, err := o.Request(ctx, request)
	r := recoveryOSDResult{Code: reply.Code, Transaction: reply.Transaction, GlobalID: reply.GlobalID, MapEpoch: reply.MapEpoch, PGPool: reply.PG.Pool, PGSeed: reply.PG.Seed, Micros: time.Since(begin).Microseconds()}
	if err != nil {
		r.Error = err.Error()
		var osdErr *cephmsgr.OSDError
		if errors.As(err, &osdErr) {
			r.OSDErrCode = osdErr.Code
		}
	}
	for _, op := range reply.Operations {
		r.Operations = append(r.Operations, op.Result)
	}
	if err == nil && len(reply.Operations) == 2 {
		if len(reply.Operations[0].Data) == 16 {
			r.StatSize = binary.LittleEndian.Uint64(reply.Operations[0].Data[:8])
		}
		r.DataBytes, r.DataEqual = len(reply.Operations[1].Data), bytes.Equal(reply.Operations[1].Data, payload)
	}
	return r
}

func TestCephMDSCephFSRecoveryIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS=1 for owned native CephFS recovery acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("CephFS recovery acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSRecoveryOracle
	var fresh nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	current, err := readMDSCephFSTarget(dir, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if current.Adopted != 0 {
		t.Fatal("the owned MDS was already restarted; this acceptance performs the only restart")
	}
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_RECOVERY_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_RECOVERY_KEY_FILE")
	plan := oracle.RecoveryPlan
	payload, payloadErr := hex.DecodeString(plan.FencedPayloadHex)
	_, tokenErr := hex.DecodeString(oracle.Token)
	fsName := fresh.FSName
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(oracle.Token) != 32 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fsName || oracle.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" ||
		identity != "client.mds-cephfs-recovery-"+oracle.Token || oracle.RecoveryIdentity != identity ||
		oracle.RecoveryCaps["mon"] != "allow r" || oracle.RecoveryCaps["mds"] != "allow rw fsname="+fsName || oracle.RecoveryCaps["osd"] != "allow r tag cephfs data="+fsName ||
		plan.Directory != "ceph-msgr-cephfs-"+oracle.Token+"-recovery" || !slices.Equal(plan.FlushPlain, []string{"flush-plain-0", "flush-plain-1", "flush-plain-2"}) ||
		!slices.Equal(plan.FlushRequested, []string{"flush-requested-0", "flush-requested-1", "flush-requested-2"}) || plan.Fenced != "fenced" || plan.Held != "held" ||
		plan.Replayed != "replayed" || plan.Unanswered != "unanswered" || payloadErr != nil || len(payload) != 5000 || oracle.DataPoolName != fsName+"-data" || oracle.DataPool < 0 {
		t.Fatal("owned CephFS recovery oracle, plan or identity differ")
	}
	mdsTarget := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", current.MDS.Endpoint, current.MDS.Nonce)}
	control := &mdsCephFSRecoveryControl{dir: dir, token: oracle.Token, fresh: fresh, mds: current.MDS, goGIDs: map[uint64]bool{}}
	metadata := func(role string) map[string]string {
		return map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-" + role, "root": "/"}
	}

	var (
		requests  []*recoveryRequestEvidence
		flushes   []*recoveryRequestEvidence
		fencing   recoveryFencingEvidence
		restart   recoveryRestartEvidence
		agentR    *cephfsCapAgent
		agentR2   *cephfsCapAgent
		agentF    *cephfsCapAgent
		dialsR    *recoveryDials
		sessionR  *cephmsgr.MDSSession
		active    *cephmsgr.MDSSession
		connR     *cephmsgr.MDSConnection
		openR     *mdsSessionEvidence
		facts     = map[string]any{}
		caseTimes = map[string]int64{}
		realms    = map[uint64]cephmsgr.MDSSnapRealm{}
		started   = time.Now()
	)
	t.Cleanup(func() {
		snapshot := func(a *cephfsCapAgent) *cephfsAgentSnapshot {
			if a == nil {
				return nil
			}
			s := a.snapshot()
			return &s
		}
		var dialLog []recoveryDial
		if dialsR != nil {
			dialLog = dialsR.attempts()
		}
		data, err := json.Marshal(struct {
			Source       string                     `json:"source_commit"`
			Oracle       any                        `json:"oracle"`
			StartTarget  nativeMDSCephFSTarget      `json:"start_target"`
			Requests     []*recoveryRequestEvidence `json:"requests"`
			Flushes      []*recoveryRequestEvidence `json:"journal_flush"`
			Fencing      recoveryFencingEvidence    `json:"fencing"`
			Restart      recoveryRestartEvidence    `json:"restart"`
			Open         *mdsSessionEvidence        `json:"open"`
			AgentBefore  *cephfsAgentSnapshot       `json:"agent_before_restart"`
			AgentAfter   *cephfsAgentSnapshot       `json:"agent_after_reconnect"`
			AgentFenced  *cephfsAgentSnapshot       `json:"agent_fenced"`
			Dials        []recoveryDial             `json:"restart_client_mds_dials"`
			Facts        map[string]any             `json:"facts"`
			Native       []nativeMDSCephFSReceipt   `json:"native"`
			CaseMillis   map[string]int64           `json:"case_ms"`
			ElapsedMilli int64                      `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, current, requests, flushes, fencing, restart, openR, snapshot(agentR), snapshot(agentR2), snapshot(agentF), dialLog, facts, control.receipts, caseTimes,
			time.Since(started).Milliseconds()})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("MDS CephFS recovery evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-recovery-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS recovery public evidence", err)
		}
	})
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		for _, a := range []*cephfsCapAgent{agentR, agentR2, agentF} {
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
	// submitFirst sends one request and applies its first reply to the agent.
	submitFirst := func(t *testing.T, s *cephmsgr.MDSSession, a *cephfsCapAgent, label, name string, request cephmsgr.MDSRequest) (*cephmsgr.MDSRequestCall, cephmsgr.MDSReply, cephmsgr.MDSTrace, *recoveryRequestEvidence) {
		t.Helper()
		request.MDSMapEpoch = epoch
		e := &recoveryRequestEvidence{Name: name, Session: label, Op: request.Op, MDSMapEpoch: epoch, submitted: time.Now()}
		requests = append(requests, e)
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
		e.firstSeen = first.Received
		e.FirstMicros, e.FirstSafe, e.FirstResult, e.FirstArrival = first.Received.Sub(e.submitted).Microseconds(), first.Safe, first.Result, first.Arrival
		e.IsDentry, e.IsTarget, e.ExtraHex = first.IsDentry, first.IsTarget, hex.EncodeToString(first.Extra)
		trace, _, err := a.applyReply(name, first)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode reply trace", name, err)
		}
		if trace.Target != nil {
			e.TargetIno, e.TargetCaps = trace.Target.Ino, cephCapString(trace.Target.Cap.Caps)
		}
		snaps, err := first.DecodeSnapTrace()
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode snap trace", name, err)
		}
		for _, r := range snaps {
			realms[r.Ino] = r
		}
		return call, first, trace, e
	}
	// waitSafe waits for the safe reply; flush sends REQUEST_FLUSH_MDLOG right
	// after an unsafe first reply.
	waitSafe := func(t *testing.T, s *cephmsgr.MDSSession, call *cephmsgr.MDSRequestCall, first cephmsgr.MDSReply, e *recoveryRequestEvidence, flush bool) cephmsgr.MDSReply {
		t.Helper()
		safe := first
		if !first.Safe {
			if flush {
				if err := s.FlushJournal(ctx); err != nil {
					t.Fatal("flush journal", e.Name, err)
				}
				e.FlushRequested, e.FlushAfterMicros = true, time.Since(first.Received).Microseconds()
			}
			waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
			defer stop()
			var err error
			if safe, err = call.WaitSafe(waitCtx); err != nil {
				e.Error = err.Error()
				t.Fatal("safe reply", e.Name, err)
			}
			e.UnsafeToSafeMicros = safe.Received.Sub(first.Received).Microseconds()
		}
		e.SafeMicros, e.SafeResult, e.SafeArrival, e.SafeTraceBytes, e.SafeExtraHex = safe.Received.Sub(e.submitted).Microseconds(), safe.Result, safe.Arrival, len(safe.Trace), hex.EncodeToString(safe.Extra)
		if !safe.Safe || safe.Transaction != call.Transaction() || safe.Op != first.Op {
			t.Fatal("safe reply identity differs", e.Name)
		}
		return safe
	}
	create := func(dirIno uint64, name string) cephmsgr.MDSRequest {
		return cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0),
			Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}}
	}
	createdIno := func(t *testing.T, name string, first cephmsgr.MDSReply, trace cephmsgr.MDSTrace) uint64 {
		t.Helper()
		if first.Result != 0 || trace.Target == nil || len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != trace.Target.Ino || trace.DentryName != name {
			t.Fatalf("create %s reply differs: result %d extra %x", name, first.Result, first.Extra)
		}
		return trace.Target.Ino
	}
	var clientR *cephmsgr.Client
	var gidR uint64
	var nonceR uint32
	var addressR string
	var dirIno uint64
	inos := map[string]uint64{}
	// The held-FLOCK composition uses the plan directory and go-file of the
	// ordinary metadata acceptance: the closed native probe admits only that
	// path. They are removed again before the cleanup leaf ends, so the
	// ordinary acceptance that follows in this fixture still creates them.
	var hf heldFlockEvidence
	var heldNativeBaseline *nativeMDSCephFSFlockState
	var heldDirIno uint64
	facts["held_flock"] = &hf

	// The restart client outlives every leaf: its cleanup belongs to the test.
	dialsR = newRecoveryDials(false, current.MDS.Endpoint)
	clientR = recoveryClient(t, ctx, identity, keyFile, dialsR, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = true, true, false })

	if !leaf("open_recovery_session", func(t *testing.T) {
		snapshot := clientR.Snapshot()
		gidR, nonceR = snapshot.GlobalID, snapshot.CephFSNonce
		control.goGIDs[gidR] = true
		if gidR == 0 || nonceR == 0 {
			t.Fatal("CephFS client lacks a GlobalID or shared nonce", snapshot)
		}
		var err error
		if connR, err = clientR.OpenMDS(ctx, mdsTarget); err != nil {
			t.Fatal("open explicit MDS target", err)
		}
		if sessionR, err = connR.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata("recovery"), RenewInterval: 2 * time.Second, MaxEvents: 1024}); err != nil {
			t.Fatal("open CephFS session", err)
		}
		active = sessionR
		agentR = startCephFSCapAgent(ctx, sessionR)
		e := describeMDSSession("recovery", identity, sessionR)
		openR = &e
		status := sessionR.Status()
		for _, bit := range []uint{7, 9, mds.FeatureMultiReconnect, 18, 19, 20} {
			if !cephmsgr.MDSFeatureBitsetHas(status.Open.SupportedFeatures, bit) {
				t.Fatal("MDS OPEN lacks a feature this acceptance relies on", bit)
			}
		}
		_, n := observe(t, gidR)
		if err := checkNativeCephFSSession(n, identity, metadata("recovery")); err != nil {
			t.Fatal(err)
		}
		addressR = n.Address
		if !strings.HasSuffix(n.Address, ":0/"+strconv.FormatUint(uint64(nonceR), 10)) || n.Caps != 0 {
			t.Fatalf("native session address %s does not carry the shared nonce %d", n.Address, nonceR)
		}
		facts["restart_client"] = map[string]any{"global_id": gidR, "cephfs_nonce": nonceR, "native_address": n.Address}
	}) {
		return
	}
	t.Cleanup(func() {
		// The active session after a reconnect is the one to close.
		closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if active != nil {
			active.Close(closing)
		}
	})

	if !leaf("journal_flush_unsafe_to_safe", func(t *testing.T) {
		call, first, trace, e := submitFirst(t, sessionR, agentR, "restart", "mkdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}})
		waitSafe(t, sessionR, call, first, e, true)
		if first.Result != 0 || trace.Target == nil || trace.DentryName != plan.Directory {
			t.Fatalf("mkdir reply differs: %+v", e)
		}
		dirIno = trace.Target.Ino
		for i := range plan.FlushPlain {
			for _, flush := range []bool{false, true} {
				name := plan.FlushPlain[i]
				if flush {
					name = plan.FlushRequested[i]
				}
				call, first, trace, e := submitFirst(t, sessionR, agentR, "restart", name, create(dirIno, name))
				ino := createdIno(t, name, first, trace)
				safe := waitSafe(t, sessionR, call, first, e, flush)
				if safe.Result != 0 {
					t.Fatalf("create %s safe result %d", name, safe.Result)
				}
				inos[name] = ino
				flushes = append(flushes, e)
				// The create opened the file read-write; close it after the
				// measurement, as an application would.
				agentR.open(ino, cephFileModeRDWR)
				if _, _, err := agentR.close(ino, cephFileModeRDWR); err != nil {
					t.Fatal("close created file", err)
				}
			}
		}
		summary := map[string][]int64{}
		for _, e := range flushes {
			key := "plain_unsafe_to_safe_us"
			if e.FlushRequested {
				key = "flush_unsafe_to_safe_us"
			}
			if e.FirstSafe {
				key = strings.Replace(key, "unsafe_to_safe", "safe_first", 1)
			}
			summary[key] = append(summary[key], e.UnsafeToSafeMicros)
		}
		facts["journal_flush"] = summary
		listing, err := control.request(ctx, "recovery-list", "")
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]uint64{}
		for _, entry := range listing.Native.Entries {
			got[entry.Name] = entry.Stat.Ino
		}
		if len(got) != len(inos) {
			t.Fatalf("native listing differs: %v vs %v", got, inos)
		}
		for name, ino := range inos {
			if got[name] != ino {
				t.Fatalf("native listing ino of %s differs: %d vs %d", name, got[name], ino)
			}
		}
	}) {
		return
	}

	if !leaf("eviction_fences_shared_nonce", func(t *testing.T) {
		write, err := control.request(ctx, "recovery-write", "")
		if err != nil {
			t.Fatal(err)
		}
		st := write.Native.Stat
		if write.Mutation == nil || write.Mutation.State != "applied" || st == nil || !st.Exists || st.Size != uint64(len(payload)) || write.Native.Written != len(payload) {
			t.Fatal("native fenced-file write did not complete", write.Mutation, write.Native)
		}
		fencing.FencedIno = st.Ino
		inos[plan.Fenced] = st.Ino
		fencing.Object = fmt.Sprintf("%x.%08x", st.Ino, 0)
		dialsF := newRecoveryDials(true, current.MDS.Endpoint)
		clientF := recoveryClient(t, ctx, identity, keyFile, dialsF, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = true, true, true })
		snapshot := clientF.Snapshot()
		fencing.ClientGlobalID, fencing.CephFSNonce = snapshot.GlobalID, snapshot.CephFSNonce
		control.goGIDs[snapshot.GlobalID] = true
		if snapshot.CephFSNonce == 0 || snapshot.CephFSNonce == nonceR || snapshot.OSDTicket.Expires.IsZero() {
			t.Fatal("fenced client lacks its own shared nonce or OSD ticket", snapshot)
		}
		connF, err := clientF.OpenMDS(ctx, mdsTarget)
		if err != nil {
			t.Fatal("open fenced client's MDS", err)
		}
		sessionF, err := connF.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata("fenced"), RenewInterval: 2 * time.Second, MaxEvents: 1024})
		if err != nil {
			t.Fatal("open fenced client's CephFS session", err)
		}
		agentF = startCephFSCapAgent(ctx, sessionF)
		_, n := observe(t, fencing.ClientGlobalID)
		if err := checkNativeCephFSSession(n, identity, metadata("fenced")); err != nil {
			t.Fatal(err)
		}
		suffix := ":0/" + strconv.FormatUint(uint64(fencing.CephFSNonce), 10)
		fencing.NativeSessionAddress = n.Address
		if !strings.HasSuffix(n.Address, suffix) {
			t.Fatalf("fenced session address %s lacks the shared nonce", n.Address)
		}
		fencing.ClientIP = strings.TrimSuffix(n.Address, suffix)
		if fencing.Placement, err = recoveryObjectPlacement(ctx, clientF, oracle.DataPoolName, fencing.Object); err != nil {
			t.Fatal(err)
		}
		if fencing.Placement.PoolID != oracle.DataPool {
			t.Fatal("data object placement names another pool", fencing.Placement)
		}
		fencing.OSDAddress = osdDumpAddresses(t, ctx, clientF)[fencing.Placement.Primary]
		osd, err := clientF.OpenOSD(ctx, cephmsgr.OSDTarget{ID: fencing.Placement.Primary, Address: fencing.OSDAddress})
		if err != nil {
			t.Fatal("open primary OSD from the CephFS client", err)
		}
		request := func(p recoveryPlacement) cephmsgr.OSDRequest {
			return cephmsgr.OSDRequest{MapEpoch: p.Epoch, Object: cephmsgr.OSDObject{Pool: oracle.DataPool, Name: fencing.Object, Hash: p.Hash},
				Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDStat}, {Code: cephmsgr.OSDRead, Length: uint64(len(payload))}}}
		}
		fencing.ReadBefore = recoveryOSDRead(ctx, osd, request(fencing.Placement), payload)
		if r := fencing.ReadBefore; r.Error != "" || r.Code != 0 || !r.DataEqual || r.StatSize != uint64(len(payload)) || r.GlobalID != fencing.ClientGlobalID {
			t.Fatalf("data object read before eviction differs from the native write: %+v", r)
		}
		lostSeen := make(chan time.Time, 1)
		go func() {
			<-sessionF.Done()
			lostSeen <- time.Now()
		}()
		evictedAt := time.Now()
		evicted, err := control.request(ctx, "evict", strconv.FormatUint(fencing.ClientGlobalID, 10))
		if err != nil {
			t.Fatal(err)
		}
		acked := time.Now()
		fencing.EvictionMutation = evicted.Mutation
		if evicted.Mutation.Address != n.Address || evicted.session(fencing.ClientGlobalID) != nil {
			t.Fatal("eviction receipt names another address or the session remains", evicted.Mutation)
		}
		for _, entry := range evicted.Blocklist {
			if !slices.ContainsFunc(oracle.BaselineBlocklist, func(b nativeMDSSessionsBlock) bool { return b.Address == entry.Address && b.Range == entry.Range }) {
				fencing.NewBlocklist = append(fencing.NewBlocklist, entry)
			}
		}
		if len(fencing.NewBlocklist) != 1 || fencing.NewBlocklist[0].Range || fencing.NewBlocklist[0].Address != fencing.ClientIP+suffix || fencing.NewBlocklist[0].Until != evicted.Mutation.Until {
			t.Fatalf("native blocklist does not contain exactly the evicted address instance: %+v", fencing.NewBlocklist)
		}
		var lostAt time.Time
		select {
		case lostAt = <-lostSeen:
		case <-time.After(40 * time.Second):
			t.Fatal("evicted CephFS session did not end")
		}
		status := sessionF.Status()
		fencing.SessionState, fencing.SessionError, fencing.MDSResumes = status.State, describeRecoveryError(sessionF.Err()), connF.Resumes()
		fencing.LostAfterEvictMillis, fencing.EvictAckMillis = lostAt.Sub(evictedAt).Milliseconds(), acked.Sub(evictedAt).Milliseconds()
		fencing.MDSDials = dialsF.attempts()
		if status.State != cephmsgr.MDSSessionLost {
			t.Fatalf("evicted session ended %d, not Lost: %v", status.State, sessionF.Err())
		}
		if fencing.PlacementAfter, err = recoveryObjectPlacement(ctx, clientF, oracle.DataPoolName, fencing.Object); err != nil {
			t.Fatal(err)
		}
		// The request names an OSDMap epoch that includes the blocklist entry,
		// so the OSD evaluates it with that map.
		fencing.ReadAfter = recoveryOSDRead(ctx, osd, request(fencing.PlacementAfter), payload)
		if r := fencing.ReadAfter; r.Error == "" || r.DataBytes != 0 || r.OSDErrCode != 0 && r.OSDErrCode != -108 {
			t.Fatalf("fenced address instance still read object data or failed differently: %+v", r)
		}
		dialsP := newRecoveryDials(true)
		reader := recoveryClient(t, ctx, identity, keyFile, dialsP, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableOSD = false, false, true })
		fencing.ReaderGlobalID, fencing.ReaderNonce = reader.Snapshot().GlobalID, reader.Snapshot().CephFSNonce
		control.goGIDs[fencing.ReaderGlobalID] = true
		readerOSD, err := reader.OpenOSD(ctx, cephmsgr.OSDTarget{ID: fencing.PlacementAfter.Primary, Address: fencing.OSDAddress})
		if err != nil {
			t.Fatal("open primary OSD from the separate client", err)
		}
		fencing.ReaderRead = recoveryOSDRead(ctx, readerOSD, request(fencing.PlacementAfter), payload)
		if r := fencing.ReaderRead; r.Error != "" || r.Code != 0 || !r.DataEqual || r.GlobalID != fencing.ReaderGlobalID {
			t.Fatalf("a separate client could not read the object: %+v", r)
		}
		fencing.OSDDials = [2]uint32{dialsF.OSD.Load(), dialsP.OSD.Load()}
		if err := errors.Join(reader.Close(), clientF.Close()); err != nil {
			t.Fatal(err)
		}
		_, n = observe(t, gidR)
		if err := checkNativeCephFSSession(n, identity, metadata("recovery")); err != nil || n.Address != addressR {
			t.Fatal("restart client session changed during the eviction", err)
		}
	}) {
		return
	}

	if !leaf("held_flock_acquired_before_restart", func(t *testing.T) {
		hf.Directory, hf.File, hf.GlobalID, hf.CephFSNonce, hf.NativeAddress = oracle.Plan.Directory, oracle.Plan.GoFile, gidR, nonceR, addressR
		hf.MDSBefore = control.mds
		call, first, trace, e := submitFirst(t, sessionR, agentR, "restart", "mkdir flock directory", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: oracle.Plan.Directory}})
		waitSafe(t, sessionR, call, first, e, true)
		if first.Result != 0 || trace.Target == nil || trace.DentryName != oracle.Plan.Directory {
			t.Fatalf("flock directory mkdir reply differs: %+v", e)
		}
		heldDirIno = trace.Target.Ino
		call, first, trace, e = submitFirst(t, sessionR, agentR, "restart", "create flock file", create(heldDirIno, oracle.Plan.GoFile))
		hf.Ino = createdIno(t, oracle.Plan.GoFile, first, trace)
		if safe := waitSafe(t, sessionR, call, first, e, true); safe.Result != 0 {
			t.Fatalf("flock file create safe result %d", safe.Result)
		}
		// As in the ordinary metadata acceptance, close the create's handle and
		// give the file a nonzero change_attr with one xattr (the closed native
		// probe requires a nonzero version), observe it natively, then reopen
		// it. The new handle stays open for the whole lock lifetime and is
		// closed only after the explicit unlock.
		agentR.open(hf.Ino, cephFileModeRDWR)
		if _, _, err := agentR.close(hf.Ino, cephFileModeRDWR); err != nil {
			t.Fatal("close the created flock file", err)
		}
		xattrValue, err := hex.DecodeString(oracle.Plan.XattrValueHex)
		if err != nil || len(xattrValue) == 0 {
			t.Fatal("plan xattr value is not usable", err)
		}
		call, first, _, e = submitFirst(t, sessionR, agentR, "restart", "flock-file-setxattr", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetxattr, Args: cephfsSetxattrArgs(0, 0),
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}, Path2: cephmsgr.MDSFilePath{Path: oracle.Plan.XattrName}, Data: xattrValue})
		if safe := waitSafe(t, sessionR, call, first, e, true); first.Result != 0 || safe.Result != 0 {
			t.Fatalf("flock file setxattr failed: %d %d", first.Result, safe.Result)
		}
		nativeXattr, err := control.request(ctx, "getxattr", "go-file")
		if err != nil || nativeXattr.Native == nil || nativeXattr.Native.ValueHex != oracle.Plan.XattrValueHex || control.goGIDs[nativeXattr.Native.NativeGlobalID] {
			t.Fatal("native getxattr does not return the xattr just set by Go", err)
		}
		agentR.open(hf.Ino, cephFileModeRDWR)
		_, opened, _, oe := submitFirst(t, sessionR, agentR, "restart", "flock-open", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(cephOpenReadWrite, 0, 0),
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}})
		if opened.Result != 0 || !opened.Safe {
			t.Fatalf("flock file open differs: %+v", oe)
		}
		want := cephfsFilelock{Client: gidR, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
		hf.Lock = want
		call, first, _, e = submitFirst(t, sessionR, agentR, "restart", "flock-exclusive-acquire", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock,
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner)})
		hf.AcquireTID, hf.SetFileLockSubmissions = call.Transaction(), 1
		safe := waitSafe(t, sessionR, call, first, e, false)
		if first.Result != 0 || safe.Result != 0 {
			t.Fatalf("Go exclusive flock acquire failed: %d %d", first.Result, safe.Result)
		}
		p, err := heldFlockProbeNative(ctx, control, hf.Ino, 11, &heldNativeBaseline)
		hf.ProbeBefore = append(hf.ProbeBefore, p)
		hf.NativeBefore = heldNativeBaseline
		if err != nil || control.goGIDs[p.NativeGlobalID] {
			t.Fatal("native nonblocking probe does not see the Go lock", err)
		}
		getCall, getFirst, _, _ := submitFirst(t, sessionR, agentR, "restart", "flock-get-before-restart", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock,
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner+1)})
		lock, err := cephfsDecodeFilelock(getFirst.Extra)
		if getFirst.Result != 0 || err != nil || lock != want {
			t.Fatalf("GETFILELOCK before restart does not identify the owned Go lock: %+v %v (tid %d)", lock, err, getCall.Transaction())
		}
		hf.GetBefore = &lock
	}) {
		return
	}

	if !leaf("mds_restart_reconnect_replay", func(t *testing.T) {
		armed, err := control.request(ctx, "restart-arm", "")
		if err != nil {
			t.Fatal(err)
		}
		a := armed.Restart.Armed
		restart.ArmedGID, restart.ArmedNonce, restart.ArmedIncarnation, restart.ArmedEndpoint = a.GID, a.Nonce, a.Incarnation, a.Endpoint
		// The held file: create it and wait for the safe reply without asking
		// for a journal flush, so the safe reply marks a journal tick.
		call, first, trace, e := submitFirst(t, sessionR, agentR, "restart", plan.Held, create(dirIno, plan.Held))
		heldIno := createdIno(t, plan.Held, first, trace)
		safe := waitSafe(t, sessionR, call, first, e, false)
		inos[plan.Held] = heldIno
		restart.TickSyncedByUnsafeSafe, restart.HeldCreateUnsafeToSafeMicros = !first.Safe, e.UnsafeToSafeMicros
		tick := safe.Received
		agentR.open(heldIno, cephFileModeRDWR)
		// Hold both handles across the restart. A caller-owned pending size
		// flush will extend this empty file sparsely, without object writes.
		agentR.open(heldIno, cephFileModeRD)
		_, opened, _, oe := submitFirst(t, sessionR, agentR, "restart", "open "+plan.Held, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(cephOpenReadOnly, 0, 0),
			Path: cephmsgr.MDSFilePath{Ino: heldIno}})
		if opened.Result != 0 || !opened.Safe {
			t.Fatalf("open for read differs: %+v", oe)
		}
		if c, _ := agentR.cap(heldIno); c.Issued&cephCapFileRD == 0 {
			t.Fatal("held file was not issued Fr", cephCapString(c.Issued))
		}
		// The mutation to replay: its unsafe reply arrives right after a tick.
		callA, firstA, traceA, eA := submitFirst(t, sessionR, agentR, "restart", plan.Replayed, create(dirIno, plan.Replayed))
		restart.ReplayedIno = createdIno(t, plan.Replayed, firstA, traceA)
		restart.ReplayedUnsafeAfterTickMicros = firstA.Received.Sub(tick).Microseconds()
		if firstA.Safe {
			t.Fatalf("the replayed create was answered safe at once: %+v", eA)
		}
		holding, release := dialsR.armHold()
		defer release()
		lostSeen := make(chan time.Time, 1)
		go func(s *cephmsgr.MDSSession) {
			<-s.Done()
			lostSeen <- time.Now()
		}(sessionR)
		fireDone := make(chan struct{})
		var fireErr error
		restart.FireStarted = time.Now()
		go func() {
			defer close(fireDone)
			_, fireErr = control.request(ctx, "restart-fire", "")
			restart.FireAcked = time.Now()
		}()
		afterFire := func(at time.Time) int64 { return at.Sub(restart.FireStarted).Microseconds() }
		// The first redial after the transport fault is held while a request
		// that never reaches any MDS is submitted.
		var callB *cephmsgr.MDSRequestCall
		select {
		case <-holding:
			restart.RedialHeldAt = time.Now()
			restart.RedialAfterFireMicros = afterFire(restart.RedialHeldAt)
			probe, stop := context.WithCancel(ctx)
			stop()
			if _, err := callA.WaitSafe(probe); err == nil {
				t.Fatal("the replayed create became safe before the MDS failed; the journal tick won the race")
			}
			eB := &recoveryRequestEvidence{Name: plan.Unanswered, Session: "restart", Op: cephmsgr.MDSOpCreate, MDSMapEpoch: epoch, submitted: time.Now()}
			requests = append(requests, eB)
			requestB := create(dirIno, plan.Unanswered)
			requestB.MDSMapEpoch = epoch
			callB, err = sessionR.Submit(ctx, requestB)
			restart.UnansweredSubmittedAfterFireMicros = afterFire(time.Now())
			if err != nil {
				restart.UnansweredSubmitError, eB.Error = err.Error(), err.Error()
			} else {
				eB.Transaction = callB.Transaction()
			}
			release()
		case <-sessionR.Done():
			t.Fatal("session ended before any redial", sessionR.Err())
		case <-time.After(30 * time.Second):
			t.Fatal("the transport did not fault after the MDS fail")
		}
		<-fireDone
		restart.FireAckMicros = afterFire(restart.FireAcked)
		if fireErr != nil {
			t.Fatal("native MDS fail", fireErr)
		}
		select {
		case restart.LostAt = <-lostSeen:
		case <-time.After(45 * time.Second):
			t.Fatal("the session did not end after the MDS restart")
		}
		lost := sessionR
		lostStatus := lost.Status()
		restart.LostAfterFireMicros, restart.LostAfterRedialMicros = afterFire(restart.LostAt), restart.LostAt.Sub(restart.RedialHeldAt).Microseconds()
		restart.ResumeError, restart.ResumesBeforeLost, restart.LostGeneration = describeRecoveryError(lost.Err()), connR.Resumes(), lostStatus.Generation
		select {
		case <-connR.Done():
			restart.ConnectionDone = true
		default:
		}
		restart.MDSDials = dialsR.attempts()
		probe, stop := context.WithCancel(ctx)
		stop()
		_, errA := callA.WaitSafe(probe)
		restart.ReplayedBeforeLost = describeRecoveryError(errA)
		if first, err := callA.Wait(probe); err == nil && !first.Safe {
			restart.ReplayedFirstStillUnsafe = true
		}
		if callB != nil {
			_, errB := callB.Wait(probe)
			restart.UnansweredBeforeLost = describeRecoveryError(errB)
		}
		if lostStatus.State != cephmsgr.MDSSessionLost || !restart.ReplayedBeforeLost.OutcomeUnknown || !restart.ReplayedFirstStillUnsafe {
			t.Fatalf("session or replay candidate did not end with an unknown outcome: %+v", restart)
		}
		select {
		case <-agentR.done:
		case <-time.After(10 * time.Second):
			t.Fatal("capability agent did not observe the lost session")
		}
		// Native: wait for the new incarnation in up:reconnect.
		var newRank *nativeMDSCephFSDaemon
		deadline := time.Now().Add(60 * time.Second)
		for newRank == nil {
			r, err := control.request(ctx, "restart-status", "")
			if err != nil {
				t.Fatal(err)
			}
			s := r.RankStatus
			restart.StatusTimeline = append(restart.StatusTimeline, recoveryStatusSample{AfterFireMillis: time.Since(restart.FireStarted).Milliseconds(), FSMapEpoch: s.FSMapEpoch, Rank0: s.Rank0,
				Failed: s.Failed, ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist})
			if s.Rank0 != nil && s.Rank0.GID != restart.ArmedGID && s.Rank0.Name == a.Name && s.Rank0.Nonce != 0 && s.Rank0.State != "up:replay" {
				newRank = s.Rank0
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the restarted MDS did not reach its reconnect phase", s)
			}
			time.Sleep(100 * time.Millisecond)
		}
		restart.ReconnectStateSeen = newRank.State
		if newRank.Nonce == restart.ArmedNonce || newRank.State != "up:reconnect" {
			t.Fatal("the restarted MDS is not a new incarnation waiting in up:reconnect", newRank)
		}
		dialsR.allowMDS(newRank.Endpoint)
		newTarget := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", newRank.Endpoint, newRank.Nonce)}
		if err := connR.Close(); err != nil {
			t.Fatal(err)
		}
		caps, realmState, err := agentR.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		// The held lock is carried only by this explicit, caller-selected
		// reconnect record. SETFILELOCK is not submitted again.
		heldBlob := cephfsFilelockBlob(nil, []cephfsFilelock{hf.Lock})
		heldCap := slices.IndexFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == hf.Ino })
		if heldCap < 0 || len(caps[heldCap].Flock) != 0 {
			t.Fatal("the held-lock inode has no capability to reconnect, or already carries a lock blob")
		}
		caps[heldCap].Flock, hf.ReconnectBlobHex = heldBlob, hex.EncodeToString(heldBlob)
		// Stage an explicit pending UPDATE while the old session is Lost.
		// Its first send precedes MClientReconnect; it is not a retry.
		agentR.mu.Lock()
		held := agentR.caps[heldIno]
		if held == nil || held.Issued&cephCapFileWR == 0 || held.MaxSize < 4096 {
			agentR.mu.Unlock()
			t.Fatal("held file lacks the write capability for the pending flush")
		}
		heldRealm, ok := realms[held.Realm]
		if !ok {
			agentR.mu.Unlock()
			t.Fatal("held file lacks its snap realm context")
		}
		held.Size = 4096
		agentR.flushTID++
		flushTID := agentR.flushTID
		agentR.flushing[flushTID] = heldIno
		pendingFlush := cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpUpdate, Ino: hf.Ino, CapID: held.CapID,
			Caps: held.Implemented, Wanted: held.fileWanted() | held.Wanted, Dirty: cephCapFileWR,
			UID: held.UID, GID: held.GID, Mode: held.Mode, NLink: held.NLink,
			Size: held.Size, MaxSize: held.MaxSize, TruncateSize: held.TruncateSize, TruncateSeq: held.TruncateSeq,
			MTime: held.MTime, ATime: held.ATime, CTime: held.CTime, TimeWarpSeq: held.TimeWarpSeq,
			InlineVersion: held.InlineVersion, BTime: held.BTime, ChangeAttr: held.ChangeAttr,
			OldestFlushTID: agentR.oldestFlushTIDLocked(), SnapFollows: heldRealm.Seq, OSDEpochBarrier: agentR.barrier,
			CallerUID: ^uint32(0), CallerGID: ^uint32(0), Files: -1, Subdirs: -1}
		agentR.mu.Unlock()
		flushEvidence := map[string]any{"message": pendingFlush, "flush_tid": flushTID, "sparse_metadata_only": true}
		facts["pre_reconnect_cap_flush"] = flushEvidence
		restart.ReconnectCaps, restart.ReconnectRealms = caps, realmState
		restart.ReconnectStarted = time.Now()
		restart.ReconnectAfterFireMicros, restart.ReconnectAfterLostMicros = afterFire(restart.ReconnectStarted), restart.ReconnectStarted.Sub(restart.LostAt).Microseconds()
		connR2, err := clientR.OpenMDS(ctx, newTarget)
		if err != nil {
			t.Fatal("open the new MDS incarnation", err)
		}
		resubmit := []*cephmsgr.MDSRequestCall{callA}
		if callB != nil {
			resubmit = append(resubmit, callB)
		}
		reconnectCtx, stopReconnect := context.WithTimeout(ctx, 30*time.Second)
		sessionR2, calls, err := connR2.ReconnectSession(reconnectCtx, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState, Resubmit: resubmit,
			PendingCapFlushes: []cephmsgr.MDSReconnectCapFlush{{Caps: pendingFlush, FlushTID: flushTID}}},
			cephmsgr.MDSSessionOptions{Metadata: metadata("recovery"), RenewInterval: 2 * time.Second, MaxEvents: 1024})
		stopReconnect()
		restart.ReconnectMicros = time.Since(restart.ReconnectStarted).Microseconds()
		restart.ReconnectError = describeRecoveryError(err)
		if err != nil {
			t.Fatal("CephFS reconnect to the new incarnation", err)
		}
		active = sessionR2
		agentR2 = startCephFSCapAgentFrom(ctx, sessionR2, agentR)
		reopened := describeMDSSession("reconnected", identity, sessionR2)
		restart.Reopened = &reopened
		status := sessionR2.Status()
		restart.ReconnectedFrom = status.ReconnectedFrom
		if status.State != cephmsgr.MDSSessionOpen || status.ReconnectedFrom != restart.LostGeneration || status.GlobalID != gidR || len(calls) != len(resubmit) {
			t.Fatalf("reconnected session differs: %+v", status)
		}
		// The resubmitted calls keep their TIDs.
		for i, call := range calls {
			name := plan.Replayed
			if i == 1 {
				name = plan.Unanswered
			}
			e := &recoveryRequestEvidence{Name: name + " resubmitted", Session: "reconnected", Op: cephmsgr.MDSOpCreate, Transaction: call.Transaction(), ResubmittedFrom: resubmit[i].Transaction(), submitted: restart.ReconnectStarted}
			requests = append(requests, e)
			if call.Transaction() != resubmit[i].Transaction() {
				t.Fatal("resubmitted call changed its TID", call.Transaction(), resubmit[i].Transaction())
			}
			waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
			first, err := call.Wait(waitCtx)
			stop()
			if err != nil {
				e.Error = err.Error()
				t.Fatal("first reply of a resubmitted call", name, err)
			}
			e.FirstMicros, e.FirstSafe, e.FirstResult, e.FirstArrival = first.Received.Sub(e.submitted).Microseconds(), first.Safe, first.Result, first.Arrival
			e.IsDentry, e.IsTarget, e.ExtraHex = first.IsDentry, first.IsTarget, hex.EncodeToString(first.Extra)
			if first.IsDentry || first.IsTarget {
				trace, _, err := agentR2.applyReply(name, first)
				if err != nil {
					t.Fatal(err)
				}
				if trace.Target != nil {
					e.TargetIno, e.TargetCaps = trace.Target.Ino, cephCapString(trace.Target.Cap.Caps)
				}
			}
			safe := waitSafe(t, sessionR2, call, first, e, true)
			if first.Result != 0 || safe.Result != 0 || len(first.Extra) != 8 {
				t.Fatalf("resubmitted %s did not complete: %+v", name, e)
			}
			ino := binary.LittleEndian.Uint64(first.Extra)
			if i == 0 {
				if ino != restart.ReplayedIno {
					t.Fatalf("replayed create reported ino %d, unsafe reply reported %d", ino, restart.ReplayedIno)
				}
			} else {
				restart.UnansweredIno = ino
			}
			inos[name] = ino
		}
		facts["resubmitted_replies"] = requests[len(requests)-len(calls):]
		// The new incarnation becomes active once reconnect, rejoin and client
		// replay finish; then the fixture adopts it.
		deadline = time.Now().Add(60 * time.Second)
		for {
			r, err := control.request(ctx, "restart-status", "")
			if err != nil {
				t.Fatal(err)
			}
			s := r.RankStatus
			restart.StatusTimeline = append(restart.StatusTimeline, recoveryStatusSample{AfterFireMillis: time.Since(restart.FireStarted).Milliseconds(), FSMapEpoch: s.FSMapEpoch, Rank0: s.Rank0,
				Failed: s.Failed, ArmedPresent: s.ArmedPresent, ArmedBlocklist: s.ArmedBlocklist})
			if s.Rank0 != nil && s.Rank0.GID == newRank.GID && s.Rank0.State == "up:active" {
				restart.ActiveAfterFireMicros = afterFire(time.Now())
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the restarted MDS did not become active", s)
			}
			time.Sleep(250 * time.Millisecond)
		}
		adopted, err := control.request(ctx, "restart-adopt", "")
		if err != nil {
			t.Fatal(err)
		}
		ad := adopted.Restart.Adopted
		restart.AdoptedGID, restart.AdoptedNonce, restart.AdoptedIncarnation, restart.AdoptedEndpoint = ad.GID, ad.Nonce, ad.Incarnation, ad.Endpoint
		restart.OldIncarnationFencing = adopted.Restart.OldFencing
		if ad.GID != newRank.GID || ad.Nonce != newRank.Nonce || ad.Endpoint != newRank.Endpoint {
			t.Fatal("adopted incarnation differs from the one reconnected", ad, newRank)
		}
		ackDeadline := time.Now().Add(30 * time.Second)
		for {
			snapshot := agentR2.snapshot()
			if ack := slices.IndexFunc(snapshot.FlushAcks, func(a cephfsFlushAck) bool { return a.Transaction == flushTID }); ack >= 0 {
				a := snapshot.FlushAcks[ack]
				if a.Caps.Ino != heldIno || a.Caps.Dirty != pendingFlush.Dirty {
					t.Fatal("pre-reconnect flush ACK changed inode or dirty mask", a)
				}
				flushEvidence["ack"] = a
				break
			}
			if len(snapshot.Errors) != 0 || time.Now().After(ackDeadline) {
				t.Fatal("pre-reconnect flush did not receive its original TID ACK", snapshot.Errors)
			}
			time.Sleep(20 * time.Millisecond)
		}
		heldStat, err := control.stat(ctx, plan.Held)
		if err != nil || !heldStat.Exists || heldStat.Ino != heldIno || heldStat.Size != pendingFlush.Size {
			t.Fatal("native stat did not observe the pre-reconnect size flush", heldStat, err)
		}
		flushEvidence["native_stat"] = heldStat
		for _, address := range adopted.Restart.ArmedAddresses {
			if !slices.ContainsFunc(adopted.Restart.OldFencing, func(b nativeMDSSessionsBlock) bool { return b.Address == address && !b.Range }) {
				t.Fatal("the failed incarnation's address is not blocklisted", address, adopted.Restart.OldFencing)
			}
		}
		// The new incarnation's op tracker shows whether it executed the
		// replayed create (an MDRequest with REPLAY) or answered it from the
		// session's completed-request record (no MDRequest at all).
		ops, err := control.request(ctx, "recovery-ops", strconv.FormatUint(gidR, 10))
		if err != nil {
			t.Fatal(err)
		}
		restart.HistoricOps = ops.HistoricOps
		find := func(tid uint64) *nativeMDSCephFSHistoricOp {
			prefix := fmt.Sprintf("client_request(client.%d:%d ", gidR, tid)
			for i := range ops.HistoricOps {
				if strings.HasPrefix(ops.HistoricOps[i].Description, prefix) {
					return &ops.HistoricOps[i]
				}
			}
			return nil
		}
		hf.MDSAfter = *ad
		replayedLock := find(hf.AcquireTID) != nil
		hf.AcquireReplayedByMDS = &replayedLock
		if replayedLock {
			t.Fatal("the new incarnation tracked a client request for the original SETFILELOCK TID", hf.AcquireTID)
		}
		restart.ReplayedHistoricOp = find(callA.Transaction())
		restart.ReplayExecutedByNewIncarnation = restart.ReplayedHistoricOp != nil
		if callB != nil {
			restart.UnansweredHistoricOp = find(callB.Transaction())
			if restart.UnansweredHistoricOp == nil {
				t.Fatal("the new incarnation's op tracker lacks the resubmitted unanswered create", ops.HistoricOps)
			}
		}
		if op := restart.ReplayedHistoricOp; op != nil && !strings.Contains(op.Description, " REPLAY") {
			t.Fatal("the tracked replayed create lacks the REPLAY flag", op.Description)
		}
		_, n := observe(t, gidR)
		if err := checkNativeCephFSSession(n, identity, metadata("recovery")); err != nil {
			t.Fatal("reconnected session is not natively open", err)
		}
		restart.NativeSessionAfter = n
		if n.Address != addressR || n.Caps <= 0 {
			t.Fatalf("reconnected session changed address or holds no caps: %+v", n)
		}
		replayed, err := control.stat(ctx, plan.Replayed)
		if err != nil {
			t.Fatal(err)
		}
		restart.ReplayedNativeIno = replayed.Ino
		if !replayed.Exists || replayed.Ino != restart.ReplayedIno || replayed.Mode != 0o100644 || replayed.NLink != 1 {
			t.Fatalf("replayed file differs natively: %+v (ino %d)", replayed, restart.ReplayedIno)
		}
		if callB != nil {
			unanswered, err := control.stat(ctx, plan.Unanswered)
			if err != nil {
				t.Fatal(err)
			}
			restart.UnansweredNative = unanswered.Ino
			if !unanswered.Exists || unanswered.Ino != restart.UnansweredIno {
				t.Fatalf("resubmitted unanswered create differs natively: %+v", unanswered)
			}
		}
		listing, err := control.request(ctx, "recovery-list", "")
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]uint64{}
		for _, entry := range listing.Native.Entries {
			names[entry.Name] = entry.Stat.Ino
		}
		if len(names) != len(inos) {
			t.Fatalf("native listing after replay differs: %v vs %v", names, inos)
		}
		for name, ino := range inos {
			if names[name] != ino {
				t.Fatalf("native listing entry %s differs: %d vs %d", name, names[name], ino)
			}
		}
		// The restored lock: a native nonblocking probe conflicts with it and
		// the Go GETFILELOCK names the original owner and GlobalID, with the
		// same client identity and a different MDS incarnation.
		if hf.MDSAfter.GID == hf.MDSBefore.GID || hf.MDSAfter.Nonce == hf.MDSBefore.Nonce || hf.MDSAfter.Rank != hf.MDSBefore.Rank || hf.MDSAfter.Rank != 0 {
			t.Fatal("held-lock recovery did not cross to a new rank-0 MDS incarnation", hf.MDSBefore, hf.MDSAfter)
		}
		if p, err := heldFlockProbeNative(ctx, control, hf.Ino, 11, &heldNativeBaseline); err != nil || control.goGIDs[p.NativeGlobalID] {
			hf.ProbeAfter = append(hf.ProbeAfter, p)
			t.Fatal("native nonblocking probe does not see the restored lock", err)
		} else {
			hf.ProbeAfter = append(hf.ProbeAfter, p)
		}
		_, getAfter, _, _ := submitFirst(t, sessionR2, agentR2, "reconnected", "flock-get-after-restart", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock,
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner+1)})
		restored, err := cephfsDecodeFilelock(getAfter.Extra)
		if getAfter.Result != 0 || err != nil || restored != hf.Lock {
			t.Fatalf("GETFILELOCK after restart does not identify the original Go lock: %+v %v", restored, err)
		}
		hf.GetAfter = &restored
		unlockCall, unlockFirst, _, unlockE := submitFirst(t, sessionR2, agentR2, "reconnected", "flock-unlock-after-restart", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock,
			Path: cephmsgr.MDSFilePath{Ino: hf.Ino}, Args: cephfsFlockArgs(cephFlockUnlock, cephFlockGoOwner)})
		hf.UnlockTID, hf.SetFileLockSubmissions = unlockCall.Transaction(), hf.SetFileLockSubmissions+1
		if unlockSafe := waitSafe(t, sessionR2, unlockCall, unlockFirst, unlockE, false); unlockFirst.Result != 0 || unlockSafe.Result != 0 {
			t.Fatalf("explicit unlock after restart failed: %d %d", unlockFirst.Result, unlockSafe.Result)
		}
		if p, err := heldFlockProbeNative(ctx, control, hf.Ino, 0, &heldNativeBaseline); err != nil {
			hf.ProbeAfter = append(hf.ProbeAfter, p)
			t.Fatal("native probe could not acquire the file after the explicit unlock", err)
		} else {
			hf.ProbeAfter = append(hf.ProbeAfter, p)
		}
		if _, _, err := agentR2.close(hf.Ino, cephFileModeRDWR); err != nil {
			t.Fatal("close held-lock handle", err)
		}
		if _, _, err := agentR2.close(heldIno, cephFileModeRD); err != nil {
			t.Fatal("close held handle", err)
		}
		if _, _, err := agentR2.close(heldIno, cephFileModeRDWR); err != nil {
			t.Fatal("close held write handle", err)
		}
	}) {
		return
	}

	t.Run("cleanup_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		call, first, _, e := submitFirst(t, active, agentR2, "reconnected", "unlink "+hf.File, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: heldDirIno, Path: hf.File}})
		if safe := waitSafe(t, active, call, first, e, true); safe.Result != 0 || first.Result != 0 {
			t.Fatalf("flock file unlink differs: %+v", e)
		}
		call, first, _, e = submitFirst(t, active, agentR2, "reconnected", "rmdir "+hf.Directory, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: hf.Directory}})
		if safe := waitSafe(t, active, call, first, e, true); safe.Result != 0 || first.Result != 0 {
			t.Fatalf("flock directory rmdir differs: %+v", e)
		}
		for _, name := range slices.Sorted(maps.Keys(inos)) {
			call, first, _, e := submitFirst(t, active, agentR2, "reconnected", "unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
			if safe := waitSafe(t, active, call, first, e, true); safe.Result != 0 || first.Result != 0 {
				t.Fatalf("unlink differs: %+v", e)
			}
		}
		call, first, _, e = submitFirst(t, active, agentR2, "reconnected", "rmdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}})
		if safe := waitSafe(t, active, call, first, e, true); safe.Result != 0 || first.Result != 0 {
			t.Fatalf("rmdir differs: %+v", e)
		}
		st, err := control.stat(ctx, "dir")
		if err != nil {
			t.Fatal(err)
		}
		if st.Exists || st.Errno != 2 {
			t.Fatal("native stat still finds the recovery directory", st)
		}
		_, n := observe(t, gidR)
		if n == nil {
			t.Fatal("reconnected session vanished before close")
		}
		before := n.Caps
		items, barrier := agentR2.releaseAll()
		releaseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		err = active.ReleaseCaps(releaseCtx, items, barrier)
		stop()
		if err != nil {
			t.Fatal("release caps", err)
		}
		samples := []int64{before}
		for range 8 {
			if _, n = observe(t, gidR); n == nil {
				t.Fatal("reconnected session vanished before close")
			}
			samples = append(samples, n.Caps)
			if n.Caps == 0 {
				break
			}
		}
		facts["release_items"], facts["num_caps_before_after_release"] = items, samples
		if samples[len(samples)-1] >= before || len(items) == 0 {
			t.Fatal("native num_caps did not drop after the release", samples)
		}
		closing, stopClose := context.WithTimeout(ctx, 15*time.Second)
		closeErr := active.Close(closing)
		stopClose()
		if closeErr != nil || active.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("Close was not confirmed by the MDS", closeErr, active.Err())
		}
		select {
		case <-agentR2.done:
		case <-time.After(10 * time.Second):
			t.Fatal("capability agent did not observe the session end")
		}
		r, err := control.request(ctx, "observe", strconv.FormatUint(gidR, 10))
		if err != nil {
			t.Fatal(err)
		}
		if r.session(gidR) != nil {
			t.Fatal("closed session remains in the native session map")
		}
		var kinds []string
		for _, rec := range agentR2.snapshot().Records {
			kinds = append(kinds, rec.Kind)
		}
		sort.Strings(kinds)
		facts["reconnected_agent_record_kinds"] = slices.Compact(kinds)
		facts["restart_client_dials"] = [2]uint32{dialsR.MDS.Load(), dialsR.OSD.Load()}
		if dialsR.OSD.Load() != 0 {
			t.Fatal("restart client dialed an OSD")
		}
	})
}
