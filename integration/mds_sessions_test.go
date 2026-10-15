package integration_test

import (
	"bytes"
	"context"
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
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// Raw OPEN images of the fresh native vector mds-session-native-open
// (internal/mds/testdata), encoded by the pinned MDS with
// CEPHFS_FEATURES_MDS_SUPPORTED (bits 0-23) and CEPHFS_METRIC_FEATURES_ALL
// (bits 0-15). TestMDSSessionNativeOpenImages re-derives them from the vector.
const mdsSessionNativeSupportedHex = "ffffff0000000000"
const mdsSessionNativeMetricHex = "ffff000000000000"

type nativeMDSSessionsMap struct {
	Epoch     uint32            `json:"mdsmap_epoch"`
	Refuse    bool              `json:"refuse_client_session"`
	Required  map[string]string `json:"required_client_features"`
	Timeout   int64             `json:"session_timeout"`
	Autoclose int64             `json:"session_autoclose"`
}

type nativeMDSSessionsBlock struct {
	Address string `json:"address"`
	Range   bool   `json:"range"`
	Until   string `json:"until"`
}

type nativeMDSSessionsOracle struct {
	Source          string `json:"source_commit"`
	Token           string `json:"token"`
	FSID            string `json:"fsid"`
	FSCID           int32  `json:"fscid"`
	FSName          string `json:"fs_name"`
	MDSGID          uint64 `json:"mds_gid"`
	ClientIdentity  string `json:"client_identity"`
	ForeignIdentity string `json:"foreign_identity"`
	ForeignFSName   string `json:"foreign_fs_name"`
	Baseline        struct {
		Map                nativeMDSSessionsMap     `json:"map"`
		DeferStale         string                   `json:"daemon_mds_defer_session_stale"`
		BlocklistOnEvict   string                   `json:"daemon_mds_session_blocklist_on_evict"`
		BlocklistOnTimeout string                   `json:"daemon_mds_session_blocklist_on_timeout"`
		Blocklist          []nativeMDSSessionsBlock `json:"blocklist"`
	} `json:"baseline"`
	StaleWindow struct {
		Timeout   int64 `json:"session_timeout"`
		Autoclose int64 `json:"session_autoclose"`
	} `json:"stale_window"`
}

type nativeMDSSession struct {
	ID             uint64            `json:"id"`
	State          string            `json:"state"`
	AuthType       string            `json:"auth_type"`
	AuthID         string            `json:"auth_id"`
	EntityType     string            `json:"entity_type"`
	AddressType    string            `json:"address_type"`
	Address        string            `json:"address"`
	Caps           int64             `json:"num_caps"`
	Leases         int64             `json:"num_leases"`
	ClientFeatures string            `json:"client_features"`
	MetricFlags    string            `json:"metric_flags"`
	Metadata       map[string]string `json:"metadata"`
}

type nativeMDSSessionsReceipt struct {
	Source          string                   `json:"source_commit"`
	Token           string                   `json:"token"`
	RequestID       int64                    `json:"request_id"`
	Action          string                   `json:"action"`
	GlobalIDs       []uint64                 `json:"global_ids,omitempty"`
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
	DeferStale      string                   `json:"daemon_mds_defer_session_stale"`
	Sessions        []nativeMDSSession       `json:"sessions"`
	RawSessions     json.RawMessage          `json:"raw_sessions,omitempty"`
	Blocklist       []nativeMDSSessionsBlock `json:"blocklist"`
	Mutation        *struct {
		Kind      string `json:"kind"`
		RequestID int64  `json:"request_id"`
		Target    string `json:"target"`
		State     string `json:"state"`
		GlobalID  uint64 `json:"global_id"`
		Address   string `json:"address"`
		Until     string `json:"blocklist_until"`
		Output    string `json:"native_output"`
		Error     string `json:"error"`
	} `json:"mutation"`
	Complete          bool                     `json:"complete"`
	Error             string                   `json:"error"`
	ElapsedMillis     int64                    `json:"elapsed_ms"`
	ObservedUTC       string                   `json:"observed_utc"`
	BaselineBlocklist []nativeMDSSessionsBlock `json:"baseline_blocklist"`
}

func (r nativeMDSSessionsReceipt) session(gid uint64) *nativeMDSSession {
	for i := range r.Sessions {
		if r.Sessions[i].ID == gid {
			return &r.Sessions[i]
		}
	}
	return nil
}

func (r nativeMDSSessionsReceipt) blocked(address string) *nativeMDSSessionsBlock {
	for i := range r.Blocklist {
		if !r.Blocklist[i].Range && r.Blocklist[i].Address == address {
			return &r.Blocklist[i]
		}
	}
	return nil
}

// nativeFeatureBits renders a raw feature_bitset_t image the way the pinned
// feature_bitset_t::print does: "0x0" when empty, else every 64-bit block as
// 16 hex digits, highest block first.
func nativeFeatureBits(raw []byte) string {
	if len(raw) == 0 {
		return "0x0"
	}
	blocks := (len(raw) + 7) / 8
	var b strings.Builder
	b.WriteString("0x")
	for i := blocks - 1; i >= 0; i-- {
		var block [8]byte
		copy(block[:], raw[i*8:min(len(raw), i*8+8)])
		for j := 7; j >= 0; j-- {
			fmt.Fprintf(&b, "%02x", block[j])
		}
	}
	return b.String()
}

type mdsSessionsControl struct {
	dir      string
	oracle   nativeMDSSessionsOracle
	fresh    nativeMDSFresh
	incarn   int32
	last     int64
	receipts *[]nativeMDSSessionsReceipt
}

func (x *mdsSessionsControl) request(ctx context.Context, action string, gids ...uint64) (nativeMDSSessionsReceipt, error) {
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	var result nativeMDSSessionsReceipt
	path := filepath.Join(x.dir, "mds-sessions-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	args := []string{x.oracle.Token, action}
	for _, gid := range gids {
		args = append(args, strconv.FormatUint(gid, 10))
	}
	if err := testcluster.RequestControl(ctx, x.dir, "mds-sessions", "mds-sessions-completed", args...); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	*x.receipts = append(*x.receipts, result)
	f := x.fresh
	if result.Source != mdsFreshSource || result.Token != x.oracle.Token || result.RequestID <= x.last || result.Action != action || !slices.Equal(result.GlobalIDs, gids) ||
		result.FSID != f.FSID || result.FSCID != f.FSCID || result.FSName != f.FSName || result.MDSGID != f.MDS.GID || result.MDSEndpoint != f.MDS.Endpoint || result.MDSNonce != f.MDS.Nonce ||
		result.MDSIncarnation != x.incarn || result.Rank != 0 || !result.IdentityChecked || !result.Complete || result.Error != "" || result.Map == nil || result.DaemonEpoch < result.Map.Epoch {
		return result, fmt.Errorf("native MDS sessions receipt scope or unchanged daemon identity differs: %s", result.Error)
	}
	x.last = result.RequestID
	return result, nil
}

func checkNativeOpenSession(n *nativeMDSSession, identity string, metadata map[string]string) error {
	declared := nativeFeatureBits(cephmsgr.MDSClientFeatures())
	if n == nil || n.State != "open" || n.AuthType != "client" || n.AuthID != strings.TrimPrefix(identity, "client.") || n.EntityType != "client" || n.Address == "" || n.Caps != 0 || n.Leases != 0 ||
		n.ClientFeatures != declared || n.ClientFeatures != "0x00000000000001ff" || n.MetricFlags != "0x0" || !maps.Equal(n.Metadata, metadata) {
		return fmt.Errorf("native open session differs from the declared client: %+v", n)
	}
	return nil
}

type mdsSessionEvidence struct {
	Name, Identity               string
	GlobalID                     uint64
	State                        cephmsgr.MDSSessionState
	Generation                   uint64
	ConnectionGeneration         uint64
	RenewSequence                uint64
	LastRenewal                  cephmsgr.MDSSessionRenewal
	SupportedFeaturesHex         string
	MetricFlagsHex               string
	OpenFlags                    uint32
	OpenMetadata                 map[string]string
	CapAuths                     []cephmsgr.MDSCapAuth
	Err                          string
	RejectReason                 string
	RejectFlags                  uint32
	RejectBlocklisted            bool
	RejectMetadata               map[string]string
	StaleAfterConfirmedMillis    int64 `json:",omitempty"`
	ResumedAfterStaleMillis      int64 `json:",omitempty"`
	EvictionToLostMillis         int64 `json:",omitempty"`
	CloseError, ReopenMDSError   string
	MDSDials, OSDDials           uint32
	Metadata                     map[string]string
	ObservedNonOpenDuringStale   []string `json:",omitempty"`
	EventsAfterStaleWindowSample string   `json:",omitempty"`
}

func describeMDSSession(name, identity string, s *cephmsgr.MDSSession) mdsSessionEvidence {
	status := s.Status()
	e := mdsSessionEvidence{Name: name, Identity: identity, GlobalID: status.GlobalID, State: status.State, Generation: status.Generation, ConnectionGeneration: status.ConnectionGeneration,
		RenewSequence: status.RenewSequence, LastRenewal: status.LastRenewal, SupportedFeaturesHex: hex.EncodeToString(status.Open.SupportedFeatures), MetricFlagsHex: hex.EncodeToString(status.Open.MetricFlags),
		OpenFlags: status.Open.Flags, OpenMetadata: status.Open.Metadata, CapAuths: status.Open.CapAuths}
	if status.Err != nil {
		e.Err = status.Err.Error()
	}
	return e
}

func waitMDSSession(ctx context.Context, s *cephmsgr.MDSSession, limit time.Duration, done func(cephmsgr.MDSSessionStatus) bool) (cephmsgr.MDSSessionStatus, error) {
	ctx, stop := context.WithTimeout(ctx, limit)
	defer stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		status := s.Status()
		if done(status) {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return status, fmt.Errorf("MDS session status wait: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func TestCephMDSSessionsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_SESSIONS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_SESSIONS=1 for owned native CephFS session acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") == "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("MDS-only session acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	var oracle nativeMDSSessionsOracle
	var fresh nativeMDSFresh
	var incarnation struct {
		MDS struct {
			Incarnation *int32 `json:"incarnation"`
		} `json:"mds"`
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-sessions-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &incarnation); err != nil || incarnation.MDS.Incarnation == nil {
		t.Fatal("owned MDS incarnation missing", err)
	}
	identity, foreign := os.Getenv("CEPH_MSGR_MDS_FRESH_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY")
	token, tokenErr := hex.DecodeString(oracle.Token)
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(token) != 16 || oracle.Token != fresh.Token || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSID != fresh.FSID || oracle.FSCID != fresh.FSCID || oracle.FSName != fresh.FSName ||
		oracle.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" || fresh.MDS.Nonce == 0 || identity != fresh.ClientIdentity || oracle.ClientIdentity != identity ||
		foreign != "client.mds-sessions-foreign-"+oracle.Token || oracle.ForeignIdentity != foreign || oracle.ForeignFSName == "" || oracle.ForeignFSName == oracle.FSName {
		t.Fatal("owned MDS sessions and active-daemon oracle identities differ")
	}
	base := oracle.Baseline
	if base.Map.Refuse || len(base.Map.Required) != 0 || base.DeferStale != "true" || base.BlocklistOnEvict != "true" || oracle.StaleWindow.Timeout != 30 || oracle.StaleWindow.Autoclose <= 60 || base.Map.Timeout == oracle.StaleWindow.Timeout {
		t.Fatal("native session baseline or stale window differs", base, oracle.StaleWindow)
	}
	target := cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)}
	freshKey, foreignKey := os.Getenv("CEPH_MSGR_MDS_FRESH_KEY_FILE"), os.Getenv("CEPH_MSGR_MDS_SESSIONS_FOREIGN_KEY_FILE")
	prefix := oracle.Token[:8]
	metadata := func(suffix string) map[string]string {
		return map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + prefix + suffix, "root": "/"}
	}
	var receipts []nativeMDSSessionsReceipt
	var sessions []mdsSessionEvidence
	type dialRecord struct {
		name  string
		dials *mdsCommandDials
	}
	var dialRecords []dialRecord
	control := &mdsSessionsControl{dir: dir, oracle: oracle, fresh: fresh, incarn: *incarnation.MDS.Incarnation, receipts: &receipts}
	started := time.Now()
	caseTimes := map[string]int64{}
	t.Cleanup(func() {
		dials := map[string][2]uint32{}
		for _, r := range dialRecords {
			dials[r.name] = [2]uint32{r.dials.MDS.Load(), r.dials.OSD.Load()}
		}
		data, err := json.Marshal(struct {
			Source       string                     `json:"source_commit"`
			Oracle       nativeMDSSessionsOracle    `json:"oracle"`
			Target       cephmsgr.MDSTarget         `json:"target"`
			Declared     string                     `json:"declared_client_features_hex"`
			Sessions     []mdsSessionEvidence       `json:"sessions"`
			Native       []nativeMDSSessionsReceipt `json:"native"`
			Dials        map[string][2]uint32       `json:"mds_osd_dials"`
			CaseMillis   map[string]int64           `json:"case_ms"`
			ElapsedMilli int64                      `json:"elapsed_ms"`
		}{mdsFreshSource, oracle, target, hex.EncodeToString(cephmsgr.MDSClientFeatures()), sessions, receipts, dials, caseTimes, time.Since(started).Milliseconds()})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("MDS sessions evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-sessions-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS sessions public evidence", err)
		}
	})
	newClient := func(t *testing.T, name, id, key string) (*cephmsgr.Client, *cephmsgr.MDSConnection, *mdsCommandDials) {
		t.Helper()
		client, dials := mdsCommandClient(t, ctx, id, key, fresh.MDS.Endpoint, true)
		dialRecords = append(dialRecords, dialRecord{name, dials})
		conn, err := client.OpenMDS(ctx, target)
		if err != nil {
			t.Fatal("open explicit MDS target", name, err)
		}
		return client, conn, dials
	}
	// Close any session a failed test leaves open before its client closes,
	// so an abandoned session is not later evicted and blocklisted. Sessions
	// spanning leaves register on the parent test, others on their leaf.
	closeLater := func(t *testing.T, s *cephmsgr.MDSSession) {
		t.Cleanup(func() {
			closing, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			s.Close(closing)
		})
	}
	leaf := func(name string, run func(t *testing.T)) bool {
		begin := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(begin).Milliseconds()
		return ok
	}

	parent := t
	clientA, connA, _ := newClient(t, "A", identity, freshKey)
	clientB, connB, _ := newClient(t, "B", identity, freshKey)
	gidA, gidB := clientA.Snapshot().GlobalID, clientB.Snapshot().GlobalID
	if gidA == 0 || gidB == 0 || gidA == gidB {
		t.Fatal("independent session clients did not retain distinct GlobalIDs")
	}
	var sA, sB *cephmsgr.MDSSession
	metaA, metaB := metadata(""), metadata("-b")
	supported, _ := hex.DecodeString(mdsSessionNativeSupportedHex)
	metric, _ := hex.DecodeString(mdsSessionNativeMetricHex)

	if !leaf("open_native_session", func(t *testing.T) {
		var err error
		sA, err = connA.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metaA, RenewInterval: 2 * time.Second})
		if err != nil {
			t.Fatal("open CephFS session", err)
		}
		closeLater(parent, sA)
		status := sA.Status()
		e := describeMDSSession("A", identity, sA)
		e.Metadata = metaA
		sessions = append(sessions, e)
		// The identity's MDS cap "allow r fsname=<fs>" is one grant: any UID,
		// no GIDs, no path, this filesystem name, read without write.
		if status.State != cephmsgr.MDSSessionOpen || status.Generation != 1 || status.ConnectionGeneration != 1 || status.GlobalID != gidA || status.Target != target || status.Err != nil || status.ReadOnly || status.PushSequence != 0 ||
			status.Open.Sequence != 0 || status.Open.Flags != 0 || len(status.Open.Metadata) != 0 || len(status.Open.CapAuths) != 1 ||
			status.Open.CapAuths[0].UID != -1 || len(status.Open.CapAuths[0].GIDs) != 0 || status.Open.CapAuths[0].Path != "" || status.Open.CapAuths[0].FilesystemName != fresh.FSName ||
			status.Open.CapAuths[0].RootSquash || !status.Open.CapAuths[0].Readable || status.Open.CapAuths[0].Writeable {
			t.Fatalf("Go session OPEN state differs from the read-only fsname grant: %+v", e)
		}
		if !bytes.Equal(status.Open.SupportedFeatures, supported) || !bytes.Equal(status.Open.MetricFlags, metric) {
			t.Fatalf("live OPEN feature images differ from the native vector: supported=%x metric=%x", status.Open.SupportedFeatures, status.Open.MetricFlags)
		}
		native, err := control.request(ctx, "observe", gidA)
		if err != nil {
			t.Fatal("native observation", err)
		}
		if err := checkNativeOpenSession(native.session(gidA), identity, metaA); err != nil {
			t.Fatal(err)
		}
	}) {
		return
	}

	if !leaf("periodic_renewal", func(t *testing.T) {
		first, err := waitMDSSession(ctx, sA, 5*time.Second, func(s cephmsgr.MDSSessionStatus) bool { return s.LastRenewal.Sequence >= 1 })
		if err != nil {
			t.Fatal("immediate renewal after OPEN was not acknowledged", err)
		}
		previous := first.LastRenewal
		samples := []cephmsgr.MDSSessionRenewal{previous}
		last, err := waitMDSSession(ctx, sA, 10*time.Second, func(s cephmsgr.MDSSessionStatus) bool {
			r := s.LastRenewal
			if r.Sequence != previous.Sequence {
				samples = append(samples, r)
				previous = r
			}
			return r.Sequence >= first.LastRenewal.Sequence+3
		})
		if err != nil || last.State != cephmsgr.MDSSessionOpen {
			t.Fatal("periodic renewals did not advance", err, samples)
		}
		for i, r := range samples {
			if r.Requested.IsZero() || r.Confirmed.Before(r.Requested) || i > 0 && (r.Sequence <= samples[i-1].Sequence || r.Confirmed.Before(samples[i-1].Confirmed) || r.Requested.Sub(samples[i-1].Requested) < time.Second) {
				t.Fatal("renewal sequence or local timing is not monotonic", samples)
			}
		}
		if last.RenewSequence < last.LastRenewal.Sequence || last.RenewSequence-last.LastRenewal.Sequence > 1 {
			t.Fatal("renewals are outstanding beyond one period", last.RenewSequence, last.LastRenewal)
		}
		e := describeMDSSession("A-renewal", identity, sA)
		sessions = append(sessions, e)
		native, err := control.request(ctx, "observe", gidA)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkNativeOpenSession(native.session(gidA), identity, metaA); err != nil {
			t.Fatal(err)
		}
	}) {
		return
	}

	if !leaf("stale_and_automatic_resume", func(t *testing.T) {
		on, err := control.request(ctx, "stale-on")
		if err != nil {
			t.Fatal("apply owned stale window", err)
		}
		if on.Map.Timeout != oracle.StaleWindow.Timeout || on.Map.Autoclose != oracle.StaleWindow.Autoclose || on.DeferStale != "false" || on.Mutation == nil || on.Mutation.State != "applied" {
			t.Fatal("native stale window was not applied", on.Map, on.DeferStale)
		}
		sB, err = connB.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metaB, RenewInterval: time.Hour})
		if err != nil {
			t.Fatal("open second CephFS session", err)
		}
		closeLater(parent, sB)
		opened, err := waitMDSSession(ctx, sB, 5*time.Second, func(s cephmsgr.MDSSessionStatus) bool { return s.LastRenewal.Sequence == 1 })
		if err != nil || opened.State != cephmsgr.MDSSessionOpen || opened.Generation != 1 || opened.GlobalID != gidB {
			t.Fatal("second session did not open with its immediate renewal", err, opened.State)
		}
		staleCtx, stop := context.WithTimeout(ctx, 80*time.Second)
		defer stop()
		type next struct {
			event cephmsgr.MDSSessionEvent
			err   error
		}
		events := make(chan next, 1)
		go func() {
			e, err := sB.Next(staleCtx)
			events <- next{e, err}
		}()
		var anomalies []string
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		var got next
	wait:
		for {
			select {
			case got = <-events:
				break wait
			case <-tick.C:
				if s := sA.Status(); s.State != cephmsgr.MDSSessionOpen {
					anomalies = append(anomalies, fmt.Sprintf("%s state=%d", time.Now().UTC().Format(time.RFC3339Nano), s.State))
				}
			}
		}
		e := describeMDSSession("B-stale", identity, sB)
		e.Metadata, e.ObservedNonOpenDuringStale = metaB, anomalies
		if got.err != nil || got.event.Op != cephmsgr.MDSSessionOpStale || got.event.Received.Before(opened.LastRenewal.Confirmed) {
			sessions = append(sessions, e)
			t.Fatal("no STALE event within the bounded window", got.err, got.event.Op)
		}
		e.StaleAfterConfirmedMillis = got.event.Received.Sub(opened.LastRenewal.Confirmed).Milliseconds()
		resumed, err := waitMDSSession(ctx, sB, 10*time.Second, func(s cephmsgr.MDSSessionStatus) bool {
			return s.State == cephmsgr.MDSSessionOpen && s.LastRenewal.Sequence == s.RenewSequence && s.LastRenewal.Sequence >= 2 && !s.LastRenewal.Requested.Before(got.event.Received)
		})
		e.ResumedAfterStaleMillis = time.Since(got.event.Received).Milliseconds()
		if err != nil {
			sessions = append(sessions, e)
			t.Fatal("automatic renewal did not resume the stale session", err, resumed.State)
		}
		e.State, e.RenewSequence, e.LastRenewal = resumed.State, resumed.RenewSequence, resumed.LastRenewal
		probe, stopProbe := context.WithTimeout(ctx, 200*time.Millisecond)
		extra, probeErr := sA.Next(probe)
		stopProbe()
		if !errors.Is(probeErr, context.DeadlineExceeded) {
			e.EventsAfterStaleWindowSample = fmt.Sprintf("op=%d err=%v", extra.Op, probeErr)
		}
		sessions = append(sessions, e)
		if e.StaleAfterConfirmedMillis < 25_000 || len(anomalies) != 0 || e.EventsAfterStaleWindowSample != "" || sA.Status().State != cephmsgr.MDSSessionOpen {
			t.Fatal("stale timing or the renewing session's state differs", e.StaleAfterConfirmedMillis, anomalies, e.EventsAfterStaleWindowSample)
		}
		native, err := control.request(ctx, "observe", gidA, gidB)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkNativeOpenSession(native.session(gidA), identity, metaA); err != nil {
			t.Fatal(err)
		}
		if err := checkNativeOpenSession(native.session(gidB), identity, metaB); err != nil {
			t.Fatal("resumed session is not natively open", err)
		}
		off, err := control.request(ctx, "stale-off")
		if err != nil {
			t.Fatal("restore owned stale window", err)
		}
		if off.Map.Timeout != base.Map.Timeout || off.Map.Autoclose != base.Map.Autoclose || off.DeferStale != base.DeferStale || off.Mutation == nil || off.Mutation.State != "restored" {
			t.Fatal("native stale window was not restored", off.Map, off.DeferStale)
		}
	}) {
		return
	}

	if !leaf("confirmed_close", func(t *testing.T) {
		for _, c := range []struct {
			name string
			s    *cephmsgr.MDSSession
		}{{"A-closed", sA}, {"B-closed", sB}} {
			closing, stop := context.WithTimeout(ctx, 15*time.Second)
			err := c.s.Close(closing)
			stop()
			e := describeMDSSession(c.name, identity, c.s)
			if err != nil {
				e.CloseError = err.Error()
			}
			sessions = append(sessions, e)
			select {
			case <-c.s.Done():
			default:
				t.Fatal("closed session Done remained open", c.name)
			}
			if err != nil || c.s.Status().State != cephmsgr.MDSSessionClosed || !errors.Is(c.s.Err(), cephmsgr.ErrMDSSessionClosed) {
				t.Fatal("Close was not confirmed by the MDS", c.name, err, c.s.Err())
			}
		}
		native, err := control.request(ctx, "observe", gidA, gidB)
		if err != nil {
			t.Fatal(err)
		}
		if native.session(gidA) != nil || native.session(gidB) != nil {
			t.Fatal("closed sessions remain in the native session map")
		}
	}) {
		return
	}

	reject := func(t *testing.T, name, id string, conn *cephmsgr.MDSConnection, gid uint64, meta map[string]string) *cephmsgr.MDSSessionRejectedError {
		t.Helper()
		s, err := conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: meta, RenewInterval: 2 * time.Second})
		e := mdsSessionEvidence{Name: name, Identity: id, GlobalID: gid, Metadata: meta}
		var rejected *cephmsgr.MDSSessionRejectedError
		if err != nil {
			e.Err = err.Error()
		}
		if errors.As(err, &rejected) {
			e.State, e.RejectReason, e.RejectFlags, e.RejectBlocklisted, e.RejectMetadata = cephmsgr.MDSSessionRejected, rejected.Reason, rejected.Flags, rejected.Blocklisted, rejected.Metadata
		}
		sessions = append(sessions, e)
		if s != nil {
			closeLater(t, s)
		}
		if s != nil || rejected == nil || rejected.Blocklisted || rejected.Flags != 0 || rejected.Metadata["error_string"] != rejected.Reason || errors.Is(err, cephmsgr.ErrMDSSessionActive) {
			t.Fatal("explicit native REJECT was not returned unchanged", name, err)
		}
		native, err := control.request(ctx, "observe", gid)
		if err != nil {
			t.Fatal(err)
		}
		if native.session(gid) != nil {
			t.Fatal("rejected session is present in the native session map", name)
		}
		return rejected
	}

	if !leaf("reject_refused_sessions", func(t *testing.T) {
		on, err := control.request(ctx, "refuse-on")
		if err != nil || !on.Map.Refuse || on.Mutation == nil || on.Mutation.State != "applied" {
			t.Fatal("refuse_client_session was not applied", err)
		}
		client, conn, _ := newClient(t, "C", identity, freshKey)
		gid := client.Snapshot().GlobalID
		rejected := reject(t, "C-refused", identity, conn, gid, metadata("-c"))
		if !strings.Contains(rejected.Reason, "new sessions are not permitted") {
			t.Fatalf("refused-session REJECT reason differs: %q", rejected.Reason)
		}
		off, err := control.request(ctx, "refuse-off")
		if err != nil || off.Map.Refuse != base.Map.Refuse || off.Mutation == nil || off.Mutation.State != "restored" {
			t.Fatal("refuse_client_session was not restored", err)
		}
		// The REJECT left no local session: the same handle may open again.
		s, err := conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata("-c"), RenewInterval: 2 * time.Second})
		if err != nil {
			t.Fatal("open after restored refusal", err)
		}
		closeLater(t, s)
		closing, stop := context.WithTimeout(ctx, 15*time.Second)
		closeErr := s.Close(closing)
		stop()
		e := describeMDSSession("C-reopened", identity, s)
		sessions = append(sessions, e)
		if s.Status().Generation != 2 || closeErr != nil || !errors.Is(s.Err(), cephmsgr.ErrMDSSessionClosed) {
			t.Fatal("reopened session did not close confirmed", closeErr, e)
		}
		native, err := control.request(ctx, "observe", gid)
		if err != nil || native.session(gid) != nil {
			t.Fatal("reopened session remained after close", err)
		}
	}) {
		return
	}

	if !leaf("reject_missing_required_feature", func(t *testing.T) {
		on, err := control.request(ctx, "require-on")
		if err != nil || on.Map.Required["feature_9"] != "reply_encoding" || len(on.Map.Required) != 1 || on.Mutation == nil || on.Mutation.State != "applied" {
			t.Fatal("reply_encoding requirement was not applied", err)
		}
		client, conn, _ := newClient(t, "D", identity, freshKey)
		rejected := reject(t, "D-required", identity, conn, client.Snapshot().GlobalID, metadata("-d"))
		if !strings.Contains(rejected.Reason, "missing required features") {
			t.Fatalf("required-feature REJECT reason differs: %q", rejected.Reason)
		}
		off, err := control.request(ctx, "require-off")
		if err != nil || !maps.Equal(off.Map.Required, base.Map.Required) || off.Mutation == nil || off.Mutation.State != "restored" {
			t.Fatal("required client features were not restored", err)
		}
	}) {
		return
	}

	if !leaf("reject_foreign_filesystem_caps", func(t *testing.T) {
		client, conn, _ := newClient(t, "E", foreign, foreignKey)
		meta := map[string]string{"entity_id": strings.TrimPrefix(foreign, "client."), "hostname": "ceph-msgr-go-" + prefix + "-e", "root": "/"}
		rejected := reject(t, "E-foreign", foreign, conn, client.Snapshot().GlobalID, meta)
		if rejected.Reason != fmt.Sprintf("client doesn't have caps for FS %q", fresh.FSName) {
			t.Fatalf("filesystem-capability REJECT reason differs: %q", rejected.Reason)
		}
	}) {
		return
	}

	if !leaf("native_eviction_is_lost", func(t *testing.T) {
		client, conn, dials := newClient(t, "F", identity, freshKey)
		gid := client.Snapshot().GlobalID
		meta := metadata("-f")
		s, err := conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: meta, RenewInterval: 2 * time.Second})
		if err != nil {
			t.Fatal("open session to evict", err)
		}
		closeLater(t, s)
		if _, err := waitMDSSession(ctx, s, 5*time.Second, func(s cephmsgr.MDSSessionStatus) bool { return s.LastRenewal.Sequence >= 1 }); err != nil {
			t.Fatal(err)
		}
		before, err := control.request(ctx, "observe", gid)
		if err != nil {
			t.Fatal(err)
		}
		victim := before.session(gid)
		if err := checkNativeOpenSession(victim, identity, meta); err != nil {
			t.Fatal(err)
		}
		if before.blocked(victim.Address) != nil {
			t.Fatal("eviction target address is already blocklisted")
		}
		requested := time.Now()
		evicted, err := control.request(ctx, "evict", gid)
		if err != nil {
			t.Fatal("native eviction", err)
		}
		if evicted.Mutation == nil || evicted.Mutation.State != "evicted" || evicted.Mutation.GlobalID != gid || evicted.Mutation.Address != victim.Address || evicted.Mutation.Until == "" || evicted.session(gid) != nil || evicted.blocked(victim.Address) == nil {
			t.Fatal("native eviction did not remove the session and blocklist its address", evicted.Mutation)
		}
		select {
		case <-s.Done():
		case <-time.After(30 * time.Second):
			sessions = append(sessions, describeMDSSession("F-not-lost", identity, s))
			t.Fatal("evicted session did not end locally")
		}
		e := describeMDSSession("F-evicted", identity, s)
		e.Metadata, e.EvictionToLostMillis = meta, time.Since(requested).Milliseconds()
		closing, stop := context.WithTimeout(ctx, 5*time.Second)
		closeErr := s.Close(closing)
		stop()
		if closeErr != nil {
			e.CloseError = closeErr.Error()
		}
		select {
		case <-conn.Done():
		case <-time.After(10 * time.Second):
			sessions = append(sessions, e)
			t.Fatal("evicted MDS transport did not end")
		}
		_, reopenErr := client.OpenMDS(ctx, target)
		if reopenErr != nil {
			e.ReopenMDSError = reopenErr.Error()
		}
		e.MDSDials, e.OSDDials = dials.MDS.Load(), dials.OSD.Load()
		sessions = append(sessions, e)
		if s.Status().State != cephmsgr.MDSSessionLost || s.Err() == nil || errors.Is(s.Err(), cephmsgr.ErrMDSSessionClosed) || !errors.Is(closeErr, cephmsgr.ErrMDSSessionActive) || !errors.Is(reopenErr, cephmsgr.ErrMDSSessionActive) || dials.MDS.Load() != 1 {
			t.Fatal("evicted session was not Lost with an unresolved address guard", s.Status().State, s.Err(), closeErr, reopenErr, dials.MDS.Load())
		}
		after, err := control.request(ctx, "observe", gid)
		if err != nil {
			t.Fatal(err)
		}
		if after.session(gid) != nil || after.blocked(victim.Address) == nil {
			t.Fatal("native eviction result changed")
		}
	}) {
		return
	}

	t.Run("mds_only_dials", func(t *testing.T) {
		for _, r := range dialRecords {
			if r.dials.OSD.Load() != 0 || r.dials.MDS.Load() != 1 {
				t.Fatal("session client dialed an OSD or redialed the MDS", r.name, r.dials.MDS.Load(), r.dials.OSD.Load())
			}
		}
		if len(dialRecords) != 6 {
			t.Fatal("unexpected number of session clients", len(dialRecords))
		}
	})
}

func TestMDSSessionNativeOpenImages(t *testing.T) {
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join("..", "internal", "mds", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	front, header := read("mds-session-native-open.front.hex"), read("mds-session-native-open.header.hex")
	// ceph_msg_header2: type at 16, version at 20, compat_version at 37.
	if len(header) != 41 || header[16] != 22 || header[20] != 7 || header[37] != 1 {
		t.Fatal("native OPEN vector header differs", hex.EncodeToString(header))
	}
	open, err := mds.DecodeClientSession(msgr.MessageData{Type: 22, Priority: 127, Version: 7, CompatVersion: 1, Front: front}, 0)
	if err != nil || open.Op != mds.SessionOpen || hex.EncodeToString(open.SupportedFeatures) != mdsSessionNativeSupportedHex || hex.EncodeToString(open.MetricFlags) != mdsSessionNativeMetricHex {
		t.Fatal("native OPEN vector feature images differ from the acceptance constants", open, err)
	}
	if got := nativeFeatureBits(cephmsgr.MDSClientFeatures()); got != "0x00000000000001ff" {
		t.Fatal("declared client features do not print as bits 0-8", got)
	}
	for raw, want := range map[string]string{"": "0x0", "ff01": "0x00000000000001ff", "000000000000000001": "0x00000000000000010000000000000000"} {
		data, _ := hex.DecodeString(raw)
		if got := nativeFeatureBits(data); got != want {
			t.Fatal("feature_bitset_t rendering differs", raw, got, want)
		}
	}
}
