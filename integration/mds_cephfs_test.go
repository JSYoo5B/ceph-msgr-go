package integration_test

import (
	"bytes"
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

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSIntegration accepts the opt-in CephFS metadata API against
// a live Tentacle MDS with a native LibCephFS oracle. See
// integration/fixture/MDS_CEPHFS_FIXTURE.md and MDS_CEPHFS_VALIDATION.md.

type nativeMDSCephFSPlan struct {
	Directory     string   `json:"directory"`
	GoFile        string   `json:"go_file"`
	NativeFiles   []string `json:"native_files"`
	Missing       string   `json:"missing"`
	XattrName     string   `json:"xattr_name"`
	XattrValueHex string   `json:"xattr_value_hex"`
	PayloadHex    string   `json:"payload_hex"`
}

type nativeMDSCephFSOracle struct {
	Source            string                   `json:"source_commit"`
	Token             string                   `json:"token"`
	FSID              string                   `json:"fsid"`
	FSCID             int32                    `json:"fscid"`
	FSName            string                   `json:"fs_name"`
	MDSGID            uint64                   `json:"mds_gid"`
	ClientIdentity    string                   `json:"client_identity"`
	NativeIdentity    string                   `json:"native_identity"`
	ClientCaps        map[string]string        `json:"client_caps"`
	NativeCaps        map[string]string        `json:"native_caps"`
	Plan              nativeMDSCephFSPlan      `json:"plan"`
	BaselineBlocklist []nativeMDSSessionsBlock `json:"baseline_blocklist"`
}

type nativeMDSCephFSStat struct {
	Exists bool   `json:"exists"`
	Errno  int    `json:"errno"`
	Ino    uint64 `json:"ino"`
	Mode   uint32 `json:"mode"`
	UID    uint32 `json:"uid"`
	GID    uint32 `json:"gid"`
	NLink  uint32 `json:"nlink"`
	Size   uint64 `json:"size"`
}

type nativeMDSCephFSEntry struct {
	Name string              `json:"name"`
	DIno uint64              `json:"d_ino"`
	Stat nativeMDSCephFSStat `json:"stat"`
}

type nativeMDSCephFSOutput struct {
	Action         string                 `json:"action"`
	Path           string                 `json:"path"`
	NativeGlobalID uint64                 `json:"native_global_id"`
	Stat           *nativeMDSCephFSStat   `json:"stat"`
	SourceStat     *nativeMDSCephFSStat   `json:"source_stat"`
	QuotaAbsent    bool                   `json:"quota_absent"`
	ValueHex       string                 `json:"value_hex"`
	Created        []nativeMDSCephFSEntry `json:"created"`
	Entries        []nativeMDSCephFSEntry `json:"entries"`
	Written        int                    `json:"written"`
	TimingsNS      map[string]int64       `json:"timings_ns"`
	DataHex        string                 `json:"data_hex"`
}

type nativeMDSCephFSReceipt struct {
	Source          string                   `json:"source_commit"`
	Token           string                   `json:"token"`
	RequestID       int64                    `json:"request_id"`
	Action          string                   `json:"action"`
	Target          string                   `json:"target"`
	GlobalID        uint64                   `json:"global_id"`
	FSID            string                   `json:"fsid"`
	FSCID           int32                    `json:"fscid"`
	FSName          string                   `json:"fs_name"`
	MDSGID          uint64                   `json:"mds_gid"`
	MDSEndpoint     string                   `json:"mds_endpoint"`
	MDSNonce        uint32                   `json:"mds_nonce"`
	MDSIncarnation  int32                    `json:"mds_incarnation"`
	Rank            int32                    `json:"rank"`
	IdentityChecked bool                     `json:"identity_checked"`
	Map             *nativeMDSSessionsMap    `json:"map"`
	DaemonEpoch     uint32                   `json:"daemon_mdsmap_epoch"`
	Sessions        []nativeMDSSession       `json:"sessions"`
	Blocklist       []nativeMDSSessionsBlock `json:"blocklist"`
	Native          *nativeMDSCephFSOutput   `json:"native"`
	Process         json.RawMessage          `json:"native_process,omitempty"`
	Mutation        *struct {
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
	} `json:"mutation"`
	Restart       *nativeMDSCephFSRestart     `json:"restart"`
	RankStatus    *nativeMDSCephFSRankStatus  `json:"rank_status"`
	HistoricOps   []nativeMDSCephFSHistoricOp `json:"historic_ops"`
	PushConfig    *nativeMDSCephFSPushConfig  `json:"push_config"`
	CacheDrop     *nativeMDSCephFSCacheDrop   `json:"cache_drop"`
	Complete      bool                        `json:"complete"`
	Error         string                      `json:"error"`
	ElapsedMillis int64                       `json:"elapsed_ms"`
	ObservedUTC   string                      `json:"observed_utc"`
}

func (r nativeMDSCephFSReceipt) session(gid uint64) *nativeMDSSession {
	for i := range r.Sessions {
		if r.Sessions[i].ID == gid {
			return &r.Sessions[i]
		}
	}
	return nil
}

func (r nativeMDSCephFSReceipt) blocked(address string) bool {
	return slices.ContainsFunc(r.Blocklist, func(b nativeMDSSessionsBlock) bool { return !b.Range && b.Address == address })
}

type mdsCephFSControl struct {
	dir      string
	oracle   nativeMDSCephFSOracle
	fresh    nativeMDSFresh
	incarn   int32
	last     int64
	goGID    uint64
	receipts *[]nativeMDSCephFSReceipt
}

// request runs one closed native action and checks the unchanged owned
// target: FSID, FSCID, name, daemon GID, endpoint, nonce, incarnation, rank 0.
func (x *mdsCephFSControl) request(ctx context.Context, action, arg string) (nativeMDSCephFSReceipt, error) {
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	var result nativeMDSCephFSReceipt
	path := filepath.Join(x.dir, "mds-cephfs-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	args := []string{x.oracle.Token, action}
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
	if result.Source != mdsFreshSource || result.Token != x.oracle.Token || result.RequestID <= x.last || result.Action != action ||
		result.FSID != f.FSID || result.FSCID != f.FSCID || result.FSName != f.FSName || result.MDSGID != f.MDS.GID || result.MDSEndpoint != f.MDS.Endpoint || result.MDSNonce != f.MDS.Nonce ||
		result.MDSIncarnation != x.incarn || result.Rank != 0 || !result.IdentityChecked || !result.Complete || result.Error != "" || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
		return result, fmt.Errorf("native MDS CephFS receipt scope or unchanged daemon identity differs: %s", result.Error)
	}
	if action == "observe" {
		if result.GlobalID != x.goGID || result.Sessions == nil {
			return result, errors.New("native MDS CephFS observation scope differs")
		}
	} else if result.Native == nil || result.Native.NativeGlobalID == 0 || result.Native.NativeGlobalID == x.goGID {
		return result, errors.New("native MDS CephFS oracle output missing or bound to the Go client")
	}
	x.last = result.RequestID
	return result, nil
}

func (x *mdsCephFSControl) stat(ctx context.Context, target string) (nativeMDSCephFSStat, error) {
	r, err := x.request(ctx, "stat", target)
	if err != nil {
		return nativeMDSCephFSStat{}, err
	}
	if r.Native.Stat == nil {
		return nativeMDSCephFSStat{}, errors.New("native stat missing")
	}
	return *r.Native.Stat, nil
}

func checkNativeCephFSSession(n *nativeMDSSession, identity string, metadata map[string]string) error {
	declared := nativeFeatureBits(mds.CephFSClientFeatures)
	if n == nil || n.State != "open" || n.AuthType != "client" || n.AuthID != strings.TrimPrefix(identity, "client.") || n.EntityType != "client" || n.Address == "" ||
		n.ClientFeatures != declared || n.ClientFeatures != "0x00000000001c83ff" || n.MetricFlags != "0x0" || !maps.Equal(n.Metadata, metadata) {
		return fmt.Errorf("native CephFS session differs from the declared client: %+v", n)
	}
	return nil
}

// nativeStatxNLink is what the pinned Client::fill_statx reports for an
// InodeStat: a linked directory (wire nlink 1) is presented as
// 2 + nsubdirs, any other inode with its wire nlink.
func nativeStatxNLink(s *cephmsgr.MDSInodeStat) uint32 {
	if s.Mode&0o170000 == 0o040000 && s.NLink == 1 {
		return 2 + uint32(s.Subdirs)
	}
	return s.NLink
}

type mdsCephFSInode struct {
	Ino, Version, XattrVersion  uint64
	Mode, UID, GID, NLink       uint32
	Size, MaxSize               uint64
	Cap                         cephmsgr.MDSReplyCap
	CapString, CapWantedString  string
	Layout                      cephmsgr.MDSFileLayout
	Files, Subdirs, RBytes      int64
	XattrBytes, OptionalBytes   int
	InlineVersion, ChangeAttr   uint64
	FragTree                    []cephmsgr.MDSFragSplit `json:",omitempty"`
	QuotaMaxBytes, QuotaMaxFile int64
}

func summarizeInode(s *cephmsgr.MDSInodeStat) *mdsCephFSInode {
	if s == nil {
		return nil
	}
	return &mdsCephFSInode{Ino: s.Ino, Version: s.Version, XattrVersion: s.XattrVersion, Mode: s.Mode, UID: s.UID, GID: s.GID, NLink: s.NLink, Size: s.Size, MaxSize: s.MaxSize,
		Cap: s.Cap, CapString: cephCapString(s.Cap.Caps), CapWantedString: cephCapString(s.Cap.Wanted), Layout: s.Layout, Files: s.Files, Subdirs: s.Subdirs, RBytes: s.RBytes,
		XattrBytes: len(s.Xattrs), OptionalBytes: len(s.OptionalMetadata), InlineVersion: s.InlineVersion, ChangeAttr: s.ChangeAttr, FragTree: s.FragTree,
		QuotaMaxBytes: s.QuotaMaxBytes, QuotaMaxFile: s.QuotaMaxFiles}
}

type mdsCephFSRequestEvidence struct {
	Name                      string
	Op                        uint32
	Transaction               uint64
	MDSMapEpoch               uint32
	FirstSafe, UnsafeSeen     bool
	FirstResult, SafeResult   int32
	ReplyMDSMapEpoch          uint32
	FirstMillis, SafeMillis   int64
	IsDentry, IsTarget        bool
	TraceBytes, ExtraBytes    int
	SnapBytes                 int
	SafeTraceBytes            int
	SafeExtraBytes            int
	ExtraHex                  string `json:",omitempty"`
	DentryName                string `json:",omitempty"`
	Lease                     *cephmsgr.MDSLeaseStat
	Dir                       *mdsCephFSInode
	Target                    *mdsCephFSInode
	DirStat                   *cephmsgr.MDSDirStat
	Realms                    []cephmsgr.MDSSnapRealm
	PushBefore, PushAfter     uint64
	ReaddirFlags              uint16
	ReaddirEntries            []string `json:",omitempty"`
	Error                     string   `json:",omitempty"`
	ConnectionClosedAfterSend bool     `json:",omitempty"`
}

type mdsCephFSResumeEvidence struct {
	Case                   string
	ClosedAt, ResumedAt    time.Time
	ResumeMillis           int64
	ResumesBefore          uint64
	ResumesAfter           uint64
	MDSDialsBefore         uint32
	MDSDialsAfter          uint32
	StatesObserved         []string
	SessionErr             string `json:",omitempty"`
	ConnectionErr          string `json:",omitempty"`
	RequestCompletedMillis int64  `json:",omitempty"`
	RequestCompletedMicros int64  `json:",omitempty"`
	// HeldIncomingAtClose reports that MDS bytes sent after Submit were
	// already held in the relay (and discarded) when it was closed.
	HeldIncomingAtClose bool `json:",omitempty"`
}

func TestCephMDSCephFSIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS=1 for owned native CephFS metadata acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("MDS-only CephFS acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSCephFSOracle
	var fresh nativeMDSFresh
	var incarnation struct {
		MDS struct {
			Incarnation *int32 `json:"incarnation"`
		} `json:"mds"`
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-cephfs-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &incarnation); err != nil || incarnation.MDS.Incarnation == nil {
		t.Fatal("owned MDS incarnation missing", err)
	}
	startupGID := fresh.MDS.GID
	// The recovery acceptance in the same fixture restarts the owned rank-0
	// daemon; the fixture publishes the incarnation it adopted. Every native
	// receipt below proves that current incarnation instead of the startup one.
	current, err := readMDSCephFSTarget(dir, fresh)
	if err != nil {
		t.Fatal(err)
	}
	fresh.MDS.GID, fresh.MDS.Endpoint, fresh.MDS.Nonce, fresh.MDS.State = current.MDS.GID, current.MDS.Endpoint, current.MDS.Nonce, current.MDS.State
	incarnation.MDS.Incarnation = &current.MDS.Incarnation
	identity, keyFile := os.Getenv("CEPH_MSGR_MDS_CEPHFS_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_KEY_FILE")
	plan := oracle.Plan
	token, tokenErr := hex.DecodeString(oracle.Token)
	xattrValue, xattrErr := hex.DecodeString(plan.XattrValueHex)
	payload, payloadErr := hex.DecodeString(plan.PayloadHex)
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(token) != 16 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID || oracle.FSCID != fresh.FSCID ||
		oracle.FSName != fresh.FSName || oracle.MDSGID != startupGID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" || fresh.MDS.Nonce == 0 ||
		identity != "client.mds-cephfs-"+oracle.Token || oracle.ClientIdentity != identity || oracle.NativeIdentity != "client.mds-cephfs-native-"+oracle.Token ||
		oracle.ClientCaps["mds"] != "allow rw fsname="+fresh.FSName || oracle.ClientCaps["osd"] != "" || oracle.NativeCaps["osd"] != "allow rw tag cephfs data="+fresh.FSName ||
		plan.Directory != "ceph-msgr-cephfs-"+oracle.Token || plan.GoFile != "go-file" || !slices.Equal(plan.NativeFiles, []string{"native-0", "native-1", "native-2"}) || plan.Missing != "missing" ||
		plan.XattrName != "user.ceph-msgr" || xattrErr != nil || len(xattrValue) < 16 || !bytes.Contains(xattrValue, []byte{0}) || payloadErr != nil || len(payload) != 64 {
		t.Fatal("owned MDS CephFS oracle, plan or identities differ")
	}
	target := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)}
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-cephfs", "root": "/"}

	var receipts []nativeMDSCephFSReceipt
	var requests []*mdsCephFSRequestEvidence
	var resumes []mdsCephFSResumeEvidence
	var agent *cephfsCapAgent
	var session *cephmsgr.MDSSession
	var conn *cephmsgr.MDSConnection
	var openEvidence *mdsSessionEvidence
	facts := map[string]any{}
	pushes := map[string]uint64{}
	caseTimes := map[string]int64{}
	started := time.Now()
	client, dials := mdsCommandClient(t, ctx, identity, keyFile, fresh.MDS.Endpoint, true, func(o *cephmsgr.Options) { o.EnableCephFS = true })
	gid := client.Snapshot().GlobalID
	control := &mdsCephFSControl{dir: dir, oracle: oracle, fresh: fresh, incarn: *incarnation.MDS.Incarnation, goGID: gid, receipts: &receipts}
	t.Cleanup(func() {
		var snapshot *cephfsAgentSnapshot
		if agent != nil {
			s := agent.snapshot()
			snapshot = &s
		}
		data, err := json.Marshal(struct {
			Source          string                      `json:"source_commit"`
			Oracle          nativeMDSCephFSOracle       `json:"oracle"`
			Target          cephmsgr.MDSTarget          `json:"target"`
			GlobalID        uint64                      `json:"global_id"`
			DeclaredCephFS  string                      `json:"declared_cephfs_client_features_hex"`
			DeclaredSession string                      `json:"mds_client_features_api_hex"`
			Open            *mdsSessionEvidence         `json:"open"`
			Requests        []*mdsCephFSRequestEvidence `json:"requests"`
			Agent           *cephfsAgentSnapshot        `json:"capability_agent"`
			Resumes         []mdsCephFSResumeEvidence   `json:"resumes"`
			Facts           map[string]any              `json:"facts"`
			PushSequence    map[string]uint64           `json:"push_sequence_after_leaf"`
			Native          []nativeMDSCephFSReceipt    `json:"native"`
			Dials           [2]uint32                   `json:"mds_osd_dials"`
			CaseMillis      map[string]int64            `json:"case_ms"`
			ElapsedMilli    int64                       `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, target, gid, hex.EncodeToString(mds.CephFSClientFeatures), hex.EncodeToString(cephmsgr.MDSCephFSClientFeatures()), openEvidence, requests, snapshot, resumes, facts, pushes,
			receipts, [2]uint32{dials.MDS.Load(), dials.OSD.Load()}, caseTimes, time.Since(started).Milliseconds()})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("MDS CephFS evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS CephFS public evidence", err)
		}
	})
	parent := t
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		if session != nil {
			pushes[name] = session.Status().PushSequence
		}
		if agent != nil {
			if s := agent.snapshot(); len(s.Errors) != 0 {
				t.Errorf("capability agent errors after %s: %v", name, s.Errors)
				return false
			}
		}
		return ok
	}
	var epoch uint32
	// submit sends one request and applies its first reply to the agent, as
	// the native client inserts only the first reply's trace.
	submit := func(t *testing.T, name string, request cephmsgr.MDSRequest, afterSubmit func()) (cephmsgr.MDSReply, cephmsgr.MDSReply, cephmsgr.MDSTrace, *cephmsgr.MDSReaddir, *mdsCephFSRequestEvidence) {
		t.Helper()
		request.MDSMapEpoch = epoch
		e := &mdsCephFSRequestEvidence{Name: name, Op: request.Op, MDSMapEpoch: epoch, PushBefore: session.Status().PushSequence}
		requests = append(requests, e)
		begin := time.Now()
		call, err := session.Submit(ctx, request)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("submit", name, err)
		}
		e.Transaction = call.Transaction()
		if afterSubmit != nil {
			afterSubmit()
			e.ConnectionClosedAfterSend = true
		}
		waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		first, err := call.Wait(waitCtx)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("first reply", name, err)
		}
		e.FirstMillis, e.FirstSafe, e.FirstResult, e.ReplyMDSMapEpoch = time.Since(begin).Milliseconds(), first.Safe, first.Result, first.MDSMapEpoch
		e.IsDentry, e.IsTarget, e.TraceBytes, e.ExtraBytes, e.SnapBytes = first.IsDentry, first.IsTarget, len(first.Trace), len(first.Extra), len(first.SnapTrace)
		if len(first.Extra) <= 64 {
			e.ExtraHex = hex.EncodeToString(first.Extra)
		}
		trace, listing, err := agent.applyReply(name, first)
		if err != nil {
			e.Error = err.Error()
			t.Fatal("decode reply trace", name, err)
		}
		e.Dir, e.Target, e.DentryName, e.Lease, e.DirStat = summarizeInode(trace.DirInode), summarizeInode(trace.Target), trace.DentryName, trace.Lease, trace.Dir
		if listing != nil {
			e.ReaddirFlags = listing.Flags
			for _, entry := range listing.Entries {
				e.ReaddirEntries = append(e.ReaddirEntries, entry.Name)
			}
		}
		if e.Realms, err = first.DecodeSnapTrace(); err != nil {
			e.Error = err.Error()
			t.Fatal("decode snap trace", name, err)
		}
		safe := first
		if !first.Safe {
			e.UnsafeSeen = true
			if safe, err = call.WaitSafe(waitCtx); err != nil {
				e.Error = err.Error()
				t.Fatal("safe reply", name, err)
			}
			e.SafeTraceBytes, e.SafeExtraBytes = len(safe.Trace), len(safe.Extra)
		}
		e.SafeMillis, e.SafeResult, e.PushAfter = time.Since(begin).Milliseconds(), safe.Result, session.Status().PushSequence
		if !safe.Safe || safe.Transaction != call.Transaction() || safe.Op != request.Op {
			t.Fatal("safe reply identity differs", name)
		}
		return first, safe, trace, listing, e
	}
	observe := func(t *testing.T) (nativeMDSCephFSReceipt, *nativeMDSSession) {
		t.Helper()
		r, err := control.request(ctx, "observe", strconv.FormatUint(gid, 10))
		if err != nil {
			t.Fatal("native observation", err)
		}
		epoch = r.Map.Epoch
		return r, r.session(gid)
	}
	var nativeAddress string
	var dirIno, fileIno uint64
	nativeInos := map[string]uint64{}

	if !leaf("open_cephfs_session", func(t *testing.T) {
		var err error
		conn, err = client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal("open explicit MDS target", err)
		}
		session, err = conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
		if err != nil {
			t.Fatal("open CephFS session", err)
		}
		parent.Cleanup(func() {
			closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			session.Close(closing)
		})
		agent = startCephFSCapAgent(ctx, session)
		status := session.Status()
		e := describeMDSSession("cephfs", identity, session)
		e.Metadata = metadata
		openEvidence = &e
		supported, _ := hex.DecodeString(mdsSessionNativeSupportedHex)
		if status.State != cephmsgr.MDSSessionOpen || status.GlobalID != gid || status.Target != target || status.Err != nil || status.ReadOnly || status.PushSequence != 0 ||
			!bytes.Equal(status.Open.SupportedFeatures, supported) || len(status.Open.CapAuths) != 1 || status.Open.CapAuths[0].UID != -1 || len(status.Open.CapAuths[0].GIDs) != 0 ||
			status.Open.CapAuths[0].Path != "" || status.Open.CapAuths[0].FilesystemName != fresh.FSName || status.Open.CapAuths[0].RootSquash || !status.Open.CapAuths[0].Readable || !status.Open.CapAuths[0].Writeable {
			t.Fatalf("Go CephFS session OPEN state differs from the read-write fsname grant: %+v", e)
		}
		for _, bit := range []uint{9, 18, 19, 20} {
			if !cephmsgr.MDSFeatureBitsetHas(status.Open.SupportedFeatures, bit) {
				t.Fatal("MDS OPEN lacks a CephFS request feature", bit)
			}
		}
		facts["mds_client_features_api"] = nativeFeatureBits(cephmsgr.MDSCephFSClientFeatures())
		r, n := observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
			t.Fatal(err)
		}
		if n.Caps != 0 || n.Leases != 0 || r.blocked(n.Address) {
			t.Fatal("fresh CephFS session already holds caps or leases, or is fenced", n.Caps, n.Leases)
		}
		nativeAddress = n.Address
		facts["native_session_address"] = n.Address
	}) {
		return
	}

	if !leaf("mkdir_matches_native_stat", func(t *testing.T) {
		first, safe, trace, _, e := submit(t, "mkdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755),
			Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}}, nil)
		if first.Result != 0 || safe.Result != 0 || !first.IsDentry || !first.IsTarget || trace.DirInode == nil || trace.DirInode.Ino != 1 || trace.DentryName != plan.Directory || trace.Target == nil {
			t.Fatalf("mkdir reply differs: %+v", e)
		}
		dirIno = trace.Target.Ino
		facts["mkdir_unsafe_reply_first"] = e.UnsafeSeen
		facts["mkdir_wire_nlink"], facts["mkdir_wire_subdirs"] = trace.Target.NLink, trace.Target.Subdirs
		if len(e.Realms) == 0 || e.Realms[0].Ino != 1 {
			t.Fatal("mkdir snap trace does not start with the root realm", e.Realms)
		}
		native, err := control.stat(ctx, "dir")
		if err != nil {
			t.Fatal(err)
		}
		st := trace.Target
		if !native.Exists || native.Ino != st.Ino || native.Mode != st.Mode || st.Mode != 0o40755 || native.UID != st.UID || native.GID != st.GID || st.UID != 0 || st.GID != 0 || st.NLink != 1 || native.NLink != nativeStatxNLink(st) {
			t.Fatalf("native stat of the Go directory differs: native=%+v go=%+v", native, summarizeInode(st))
		}
	}) {
		return
	}

	if !leaf("create_reports_created_inode", func(t *testing.T) {
		first, safe, trace, _, e := submit(t, "create", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.GoFile}}, nil)
		if first.Result != 0 || safe.Result != 0 || !first.IsTarget || trace.Target == nil || trace.DirInode == nil || trace.DirInode.Ino != dirIno || trace.DentryName != plan.GoFile {
			t.Fatalf("create reply differs: %+v", e)
		}
		st := trace.Target
		fileIno = st.Ino
		// REPLY_CREATE_INODE without DELEG_INO: the extra blob is the created
		// inodeno_t, a little-endian u64, in the first reply.
		if len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != st.Ino {
			t.Fatalf("created-inode extra differs: %x target %d", first.Extra, st.Ino)
		}
		facts["create_unsafe_reply_first"], facts["create_extra_hex"] = e.UnsafeSeen, hex.EncodeToString(first.Extra)
		if st.Mode != 0o100644 || st.UID != 0 || st.GID != 0 || st.NLink != 1 || st.Size != 0 {
			t.Fatalf("created inode attributes differ: %+v", summarizeInode(st))
		}
		facts["create_reply_caps"] = cephCapString(st.Cap.Caps)
		// The created file is open read-write; close it as an application
		// would. check_caps then drops what a closed file does not retain.
		agent.open(fileIno, cephFileModeRDWR)
		update, sent, err := agent.close(fileIno, cephFileModeRDWR)
		if err != nil {
			t.Fatal("close created file", err)
		}
		facts["create_close_update_sent"], facts["create_close_update_caps"], facts["create_close_update_wanted"] = sent, cephCapString(update.Caps), cephCapString(update.Wanted)
		native, err := control.stat(ctx, "go-file")
		if err != nil {
			t.Fatal(err)
		}
		if !native.Exists || native.Ino != st.Ino || native.Mode != st.Mode || native.UID != st.UID || native.GID != st.GID || native.NLink != nativeStatxNLink(st) || native.Size != st.Size {
			t.Fatalf("native stat of the Go file differs: native=%+v go=%+v", native, summarizeInode(st))
		}
	}) {
		return
	}

	if !leaf("setxattr_matches_native_getxattr", func(t *testing.T) {
		_, safe, _, _, e := submit(t, "setxattr", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetxattr, Args: cephfsSetxattrArgs(0, 0), Path: cephmsgr.MDSFilePath{Ino: fileIno},
			Path2: cephmsgr.MDSFilePath{Path: plan.XattrName}, Data: xattrValue}, nil)
		if safe.Result != 0 || e.FirstResult != 0 {
			t.Fatalf("setxattr reply differs: %+v", e)
		}
		facts["setxattr_unsafe_reply_first"] = e.UnsafeSeen
		native, err := control.request(ctx, "getxattr", "go-file")
		if err != nil {
			t.Fatal(err)
		}
		if native.Native.ValueHex != plan.XattrValueHex {
			t.Fatalf("native getxattr differs: %s", native.Native.ValueHex)
		}
		first, _, trace, _, e := submit(t, "getattr-xattrs", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: fileIno}}, nil)
		if first.Result != 0 || !first.Safe || trace.Target == nil || trace.Target.Ino != fileIno {
			t.Fatalf("getattr reply differs: %+v", e)
		}
		xattrs, err := cephmsgr.DecodeMDSXattrs(trace.Target.Xattrs)
		if err != nil || !bytes.Equal(xattrs[plan.XattrName], xattrValue) || trace.Target.XattrVersion == 0 {
			t.Fatalf("getattr xattrs differ: %v %x version %d", err, xattrs[plan.XattrName], trace.Target.XattrVersion)
		}
		names := slices.Sorted(maps.Keys(xattrs))
		facts["getattr_xattr_names"], facts["getattr_xattr_version"] = names, trace.Target.XattrVersion
	}) {
		return
	}

	if !leaf("readdir_matches_native_listing", func(t *testing.T) {
		created, err := control.request(ctx, "create-native", "")
		if err != nil {
			t.Fatal(err)
		}
		if created.Mutation == nil || created.Mutation.State != "applied" || len(created.Native.Created) != 3 {
			t.Fatal("native creation was not applied", created.Mutation)
		}
		for _, entry := range created.Native.Created {
			nativeInos[entry.Name] = entry.Stat.Ino
		}
		first, _, trace, listing, e := submit(t, "readdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpReaddir, Args: cephfsReaddirArgs(0, 1024, 0, cephReaddirReplyBitflags, 0),
			Path: cephmsgr.MDSFilePath{Ino: dirIno}}, nil)
		if first.Result != 0 || !first.Safe || listing == nil || trace.Target == nil || trace.Target.Ino != dirIno {
			t.Fatalf("readdir reply differs: %+v", e)
		}
		facts["readdir_flags"], facts["readdir_dirstat"] = listing.Flags, listing.Dir
		if listing.Flags&mds.ReaddirFragEnd == 0 {
			t.Fatal("readdir did not end the only fragment", listing.Flags)
		}
		native, err := control.request(ctx, "list", "dir")
		if err != nil {
			t.Fatal(err)
		}
		goEntries := map[string]cephmsgr.MDSInodeStat{}
		for _, entry := range listing.Entries {
			goEntries[entry.Name] = entry.Inode
		}
		want := map[string]uint64{plan.GoFile: fileIno}
		maps.Copy(want, nativeInos)
		if len(goEntries) != 4 || len(native.Native.Entries) != 4 {
			t.Fatalf("listing sizes differ: go=%d native=%d", len(goEntries), len(native.Native.Entries))
		}
		for _, entry := range native.Native.Entries {
			st, ok := goEntries[entry.Name]
			if !ok || want[entry.Name] == 0 || st.Ino != want[entry.Name] || entry.DIno != st.Ino || entry.Stat.Ino != st.Ino || entry.Stat.Mode != st.Mode || entry.Stat.UID != st.UID || entry.Stat.GID != st.GID || entry.Stat.NLink != nativeStatxNLink(&st) {
				t.Fatalf("readdir entry differs from native listing: %s go=%+v native=%+v", entry.Name, summarizeInode(&st), entry)
			}
		}
	}) {
		return
	}

	if !leaf("lookup_existing_and_missing", func(t *testing.T) {
		first, _, trace, _, e := submit(t, "lookup", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: "native-0"}}, nil)
		if first.Result != 0 || !first.Safe || trace.Target == nil || trace.Target.Ino != nativeInos["native-0"] || trace.DentryName != "native-0" {
			t.Fatalf("lookup reply differs: %+v", e)
		}
		native, err := control.stat(ctx, "native-0")
		if err != nil {
			t.Fatal(err)
		}
		if !native.Exists || native.Ino != trace.Target.Ino || native.Mode != trace.Target.Mode || native.UID != trace.Target.UID || native.GID != trace.Target.GID || native.NLink != nativeStatxNLink(trace.Target) {
			t.Fatalf("lookup target differs from native stat: %+v %+v", native, summarizeInode(trace.Target))
		}
		missing, _, trace, _, e := submit(t, "lookup-missing", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.Missing}}, nil)
		if missing.Result != -2 || !missing.Safe || missing.IsTarget || trace.Target != nil {
			t.Fatalf("missing lookup differs: %+v", e)
		}
		facts["lookup_missing_dentry_trace"] = missing.IsDentry
		absent, err := control.stat(ctx, "missing")
		if err != nil {
			t.Fatal(err)
		}
		if absent.Exists || absent.Errno != 2 {
			t.Fatal("native stat of the missing name differs", absent)
		}
	}) {
		return
	}

	if !leaf("revoke_acknowledged_before_native_write", func(t *testing.T) {
		const holdFor = 2 * time.Second
		pushBefore := session.Status().PushSequence
		agent.open(fileIno, cephFileModeRD)
		first, _, trace, _, e := submit(t, "open-read", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(cephOpenReadOnly, 0, 0), Path: cephmsgr.MDSFilePath{Ino: fileIno}}, nil)
		if first.Result != 0 || !first.Safe || trace.Target == nil || trace.Target.Ino != fileIno {
			t.Fatalf("open reply differs: %+v", e)
		}
		opened, _ := agent.cap(fileIno)
		facts["open_reply_cap"] = map[string]any{"caps": cephCapString(trace.Target.Cap.Caps), "wanted": cephCapString(trace.Target.Cap.Wanted), "cap_id": trace.Target.Cap.CapID,
			"seq": trace.Target.Cap.Seq, "mseq": trace.Target.Cap.MSeq, "realm": trace.Target.Cap.Realm, "flags": trace.Target.Cap.Flags, "agent_issued": cephCapString(opened.Issued)}
		if opened.Issued&cephCapFileRD == 0 {
			t.Fatal("open for read was not issued Fr", cephCapString(opened.Issued))
		}
		agent.holdRevoke(fileIno, holdFor)
		writer, err := control.request(ctx, "write", "go-file")
		if err != nil {
			t.Fatal("native writer", err)
		}
		hold := agent.hold(fileIno)
		facts["revoke"] = map[string]any{"received": hold.Received, "acked": hold.Acked, "held_ms": hold.Acked.Sub(hold.Received).Milliseconds(), "seq": hold.RevokeSeq,
			"before": cephCapString(hold.Before), "revoke_caps": cephCapString(hold.Revoke), "ack_caps": cephCapString(hold.AckCaps), "ack_wanted": cephCapString(hold.AckWanted), "sent": hold.Sent, "error": hold.Error}
		facts["writer_timings_ns"] = writer.Native.TimingsNS
		if hold.Received.IsZero() {
			t.Fatal("the MDS sent no revoke for the Go file while the native writer ran")
		}
		if !hold.Sent || hold.Error != "" || hold.Acked.Sub(hold.Received) < holdFor || hold.Before&^hold.Revoke == 0 {
			t.Fatalf("held revoke was not acknowledged with the remaining caps: %+v", hold)
		}
		if writer.Mutation == nil || writer.Mutation.State != "applied" || writer.Native.Written != len(payload) || writer.Native.Stat == nil || writer.Native.Stat.Size != uint64(len(payload)) {
			t.Fatal("native write did not complete", writer.Mutation, writer.Native.Written)
		}
		// The writer started before the revoke reached Go and could finish its
		// write only after the acknowledgement, so its own monotonic clock
		// must cover at least the hold.
		if blocked := time.Duration(writer.Native.TimingsNS["written"]); blocked < holdFor-50*time.Millisecond {
			t.Fatalf("native write completed %v after start, before the %v held acknowledgement", blocked, holdFor)
		}
		read, err := control.request(ctx, "read", "go-file")
		if err != nil {
			t.Fatal(err)
		}
		if read.Native.DataHex != plan.PayloadHex || read.Native.Stat.Size != uint64(len(payload)) {
			t.Fatal("independent native read differs", read.Native.DataHex)
		}
		update, sent, err := agent.close(fileIno, cephFileModeRD)
		if err != nil {
			t.Fatal("close read handle", err)
		}
		facts["read_close_update_sent"], facts["read_close_update_caps"] = sent, cephCapString(update.Caps)
		facts["revoke_push_sequence"] = []uint64{pushBefore, session.Status().PushSequence}
		if session.Status().PushSequence <= pushBefore {
			t.Fatal("the revoke was not counted in the push sequence")
		}
		_, n := observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
			t.Fatal(err)
		}
		facts["num_caps_after_revoke"] = n.Caps
	}) {
		return
	}

	if !leaf("messenger_resume_keeps_session", func(t *testing.T) {
		// awaitResume samples the session state from the transport close until
		// the resume counter advances; the session must never leave Open.
		awaitResume := func(t *testing.T, e mdsCephFSResumeEvidence) mdsCephFSResumeEvidence {
			t.Helper()
			deadline := time.Now().Add(30 * time.Second)
			for {
				status := session.Status()
				if state := strconv.Itoa(int(status.State)); len(e.StatesObserved) == 0 || e.StatesObserved[len(e.StatesObserved)-1] != state {
					e.StatesObserved = append(e.StatesObserved, state)
				}
				if conn.Resumes() > e.ResumesBefore {
					e.ResumedAt = time.Now()
					break
				}
				if status.State != cephmsgr.MDSSessionOpen || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			e.ResumesAfter, e.MDSDialsAfter = conn.Resumes(), dials.MDS.Load()
			if !e.ResumedAt.IsZero() {
				e.ResumeMillis = e.ResumedAt.Sub(e.ClosedAt).Milliseconds()
			}
			if err := session.Err(); err != nil {
				e.SessionErr = err.Error()
			}
			select {
			case <-conn.Done():
				e.ConnectionErr = "MDS connection ended"
			default:
			}
			resumes = append(resumes, e)
			if e.ResumedAt.IsZero() || e.ResumesAfter != e.ResumesBefore+1 || e.MDSDialsAfter != e.MDSDialsBefore+1 || session.Status().State != cephmsgr.MDSSessionOpen || e.ConnectionErr != "" {
				t.Fatalf("Messenger session did not resume: %+v", e)
			}
			return e
		}
		idle := mdsCephFSResumeEvidence{Case: "idle", ResumesBefore: conn.Resumes(), MDSDialsBefore: dials.MDS.Load()}
		relay := dials.latest(t)
		idle.ClosedAt = time.Now()
		relay.Close()
		awaitResume(t, idle)
		first, _, trace, _, e := submit(t, "lookup-after-resume", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll),
			Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: "native-1"}}, nil)
		if first.Result != 0 || !first.Safe || trace.Target == nil || trace.Target.Ino != nativeInos["native-1"] {
			t.Fatalf("lookup after resume differs: %+v", e)
		}
		// Close the transport right after Submit queued the request. The relay
		// holds every MDS byte that arrives after Submit, so no reply can be
		// delivered on the old connection: the request or its reply must be
		// carried across the resume.
		inflight := mdsCephFSResumeEvidence{Case: "in-flight request", ResumesBefore: conn.Resumes(), MDSDialsBefore: dials.MDS.Load()}
		relay = dials.latest(t)
		relay.ArmReadPause()
		first, _, trace, _, e = submit(t, "getattr-across-resume", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: dirIno}}, func() {
			inflight.ClosedAt = time.Now()
			select {
			case <-relay.ReadBlocked:
				inflight.HeldIncomingAtClose = true
			default:
			}
			relay.Close()
		})
		inflight.RequestCompletedMillis = time.Since(inflight.ClosedAt).Milliseconds()
		inflight.RequestCompletedMicros = time.Since(inflight.ClosedAt).Microseconds()
		if first.Result != 0 || !first.Safe || trace.Target == nil || trace.Target.Ino != dirIno {
			t.Fatalf("getattr across resume differs: %+v", e)
		}
		awaitResume(t, inflight)
		r, n := observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
			t.Fatal("Go session is not natively open after resume", err)
		}
		facts["native_session_address_after_resume"] = n.Address
		if n.Address != nativeAddress || r.blocked(n.Address) {
			t.Fatalf("resumed session address changed or was fenced: %s -> %s", nativeAddress, n.Address)
		}
		if conn.Resumes() != 2 || dials.OSD.Load() != 0 {
			t.Fatal("unexpected resume or OSD dial count", conn.Resumes(), dials.OSD.Load())
		}
	}) {
		return
	}

	t.Run("cleanup_unlink_release_close", func(t *testing.T) {
		begin := time.Now()
		defer func() { caseTimes["cleanup_unlink_release_close"] = time.Since(begin).Milliseconds() }()
		for _, name := range append([]string{plan.GoFile}, plan.NativeFiles...) {
			_, safe, _, _, e := submit(t, "unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}}, nil)
			if safe.Result != 0 || e.FirstResult != 0 {
				t.Fatalf("unlink differs: %+v", e)
			}
		}
		_, safe, _, _, e := submit(t, "rmdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: plan.Directory}}, nil)
		if safe.Result != 0 || e.FirstResult != 0 {
			t.Fatalf("rmdir differs: %+v", e)
		}
		for _, name := range append([]string{"dir", plan.GoFile}, plan.NativeFiles...) {
			st, err := control.stat(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if st.Exists || st.Errno != 2 {
				t.Fatal("native stat still finds a removed entry", name, st)
			}
		}
		_, n := observe(t)
		if err := checkNativeCephFSSession(n, identity, metadata); err != nil {
			t.Fatal(err)
		}
		before := n.Caps
		items, barrier := agent.releaseAll()
		releaseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		err := session.ReleaseCaps(releaseCtx, items, barrier)
		stop()
		if err != nil {
			t.Fatal("release caps", err)
		}
		samples := []int64{before}
		for range 8 {
			_, n = observe(t)
			if n == nil {
				t.Fatal("Go session vanished before close")
			}
			samples = append(samples, n.Caps)
			if n.Caps == 0 {
				break
			}
		}
		facts["release_items"], facts["release_barrier"], facts["num_caps_before_after_release"] = items, barrier, samples
		if samples[len(samples)-1] >= before || len(items) == 0 {
			t.Fatal("native num_caps did not drop after the release", samples, len(items))
		}
		closing, stopClose := context.WithTimeout(ctx, 15*time.Second)
		closeErr := session.Close(closing)
		stopClose()
		if closeErr != nil || session.Status().State != cephmsgr.MDSSessionClosed || !errors.Is(session.Err(), cephmsgr.ErrMDSSessionClosed) {
			t.Fatal("Close was not confirmed by the MDS", closeErr, session.Err())
		}
		select {
		case <-agent.done:
		case <-time.After(10 * time.Second):
			t.Fatal("capability agent did not observe the session end")
		}
		r, err := control.request(ctx, "observe", strconv.FormatUint(gid, 10))
		if err != nil {
			t.Fatal(err)
		}
		if r.session(gid) != nil {
			t.Fatal("closed session remains in the native session map")
		}
		snapshot := agent.snapshot()
		if len(snapshot.Errors) != 0 || snapshot.Pending != 0 || !strings.Contains(snapshot.EndError, cephmsgr.ErrMDSSessionClosed.Error()) {
			t.Fatal("capability agent ended with errors or unapplied pushes", snapshot.Errors, snapshot.Pending, snapshot.EndError)
		}
		if dials.OSD.Load() != 0 || dials.MDS.Load() != uint32(1+conn.Resumes()) {
			t.Fatal("client dialed an OSD or redialed the MDS outside resumes", dials.MDS.Load(), dials.OSD.Load(), conn.Resumes())
		}
		var kinds []string
		for _, r := range snapshot.Records {
			kinds = append(kinds, r.Kind)
		}
		sort.Strings(kinds)
		facts["agent_record_kinds"] = slices.Compact(kinds)
	})
}
