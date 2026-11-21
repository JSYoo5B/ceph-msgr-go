package integration_test

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// nativeReportDigest is one natively encoded PGMapDigest with the MON
// statfs projection its native encoder recorded (PGMapDigest::get_statfs
// without a pool). The native test binary runs away from the source tree,
// so it embeds testdata/mgr-report-native-digests.json, a copy of digests a
// and b of internal/msgr/testdata/mgr-report-native-provenance.json that
// TestMgrReportDigestsMatchNativeProvenance keeps equal to it.
type nativeReportDigest struct {
	Bytes  []byte
	Statfs struct {
		KB, KBUsed, KBAvail, Objects uint64
	}
}

//go:embed testdata/mgr-report-native-digests.json
var mgrReportNativeDigests []byte

type nativeDigestRecord struct {
	Hex    string `json:"bytes_hex"`
	Statfs struct {
		KB      uint64 `json:"kb"`
		KBUsed  uint64 `json:"kb_used"`
		KBAvail uint64 `json:"kb_avail"`
		Objects uint64 `json:"num_objects"`
	} `json:"statfs"`
}

func loadNativeDigests(t *testing.T, names ...string) map[string]nativeReportDigest {
	t.Helper()
	var copied struct {
		Commit  string                        `json:"ceph_commit"`
		Digests map[string]nativeDigestRecord `json:"digests"`
	}
	if err := json.Unmarshal(mgrReportNativeDigests, &copied); err != nil || copied.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" {
		t.Fatal("native MGR report digests", err, copied.Commit)
	}
	out := map[string]nativeReportDigest{}
	for _, name := range names {
		f, ok := copied.Digests[name]
		b, err := hex.DecodeString(f.Hex)
		if !ok || err != nil || len(b) == 0 {
			t.Fatal("native digest", name, ok, err)
		}
		d := nativeReportDigest{Bytes: b}
		d.Statfs.KB, d.Statfs.KBUsed, d.Statfs.KBAvail, d.Statfs.Objects = f.Statfs.KB, f.Statfs.KBUsed, f.Statfs.KBAvail, f.Statfs.Objects
		out[name] = d
	}
	return out
}

// TestMgrReportDigestsMatchNativeProvenance keeps the embedded digests equal
// to the native provenance they were copied from.
func TestMgrReportDigestsMatchNativeProvenance(t *testing.T) {
	raw, err := os.ReadFile("../internal/msgr/testdata/mgr-report-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Commit   string `json:"ceph_commit"`
		Fixtures map[string]struct {
			Hex     string             `json:"bytes_hex"`
			Decoded nativeDigestRecord `json:"decoded"`
		} `json:"fixtures"`
	}
	var copied struct {
		Commit  string                        `json:"ceph_commit"`
		Digests map[string]nativeDigestRecord `json:"digests"`
	}
	if json.Unmarshal(raw, &proof) != nil || json.Unmarshal(mgrReportNativeDigests, &copied) != nil || proof.Commit != copied.Commit || len(copied.Digests) != 2 {
		t.Fatal("native digest sources")
	}
	for name, c := range copied.Digests {
		p, ok := proof.Fixtures[name]
		if !ok || p.Hex != c.Hex || p.Decoded.Statfs != c.Statfs {
			t.Fatal("embedded digest differs from the native provenance", name)
		}
	}
}

func (d nativeReportDigest) matches(s cephmsgr.StatfsResult) bool {
	return s.KB == d.Statfs.KB && s.KBUsed == d.Statfs.KBUsed && s.KBAvail == d.Statfs.KBAvail && s.Objects == d.Statfs.Objects
}

// mgrReportCheck is one health check as the MON renders it.
type mgrReportCheck struct {
	Severity string   `json:"severity"`
	Summary  string   `json:"summary"`
	Count    int64    `json:"count"`
	Detail   []string `json:"detail"`
}

// mgrReportReceipt is the owned-pool oracle's mgrreport result
// (tools/osd_pool_mode_mgrreport_oracle.py): a native client's own MON
// session's view of the MgrMap roles, cluster_stat and health checks.
type mgrReportReceipt struct {
	Before, After struct {
		Epoch       uint32   `json:"epoch"`
		ActiveGID   uint64   `json:"active_gid"`
		ActiveName  string   `json:"active_name"`
		StandbyGIDs []uint64 `json:"standby_gids"`
	}
	Health      map[string]mgrReportCheck `json:"health"`
	ClusterStat struct {
		KB      uint64 `json:"kb"`
		KBUsed  uint64 `json:"kb_used"`
		KBAvail uint64 `json:"kb_avail"`
		Objects uint64 `json:"num_objects"`
	} `json:"cluster_stat"`
}

func (r mgrReportReceipt) stable(active, standby uint64) bool {
	return r.Before.ActiveGID == active && r.After.ActiveGID == active && r.Before.Epoch == r.After.Epoch &&
		slices.Contains(r.Before.StandbyGIDs, standby) && slices.Contains(r.After.StandbyGIDs, standby)
}

func (r mgrReportReceipt) statfs(d nativeReportDigest) bool {
	return r.ClusterStat.KB == d.Statfs.KB && r.ClusterStat.KBUsed == d.Statfs.KBUsed && r.ClusterStat.KBAvail == d.Statfs.KBAvail && r.ClusterStat.Objects == d.Statfs.Objects
}

func nativeMgrReport(t *testing.T, ctx context.Context, prefix string) mgrReportReceipt {
	t.Helper()
	var r struct {
		Report mgrReportReceipt `json:"mgr_report"`
	}
	nativePoolResult(t, ctx, "mgrreport", map[string]any{"prefix": prefix}, &r)
	return r.Report
}

// reportChecks returns the health checks with prefix that `health detail`
// lists, read through the Go admin client's own MON session.
func reportChecks(t *testing.T, ctx context.Context, admin *cephmsgr.Client, prefix string) map[string]mgrReportCheck {
	t.Helper()
	var detail struct {
		Checks map[string]struct {
			Severity string `json:"severity"`
			Summary  struct {
				Message string `json:"message"`
				Count   int64  `json:"count"`
			} `json:"summary"`
			Detail []struct {
				Message string `json:"message"`
			} `json:"detail"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "health", "detail": "detail", "format": "json"}), &detail); err != nil {
		t.Fatal("health detail", err)
	}
	out := map[string]mgrReportCheck{}
	for code, c := range detail.Checks {
		if strings.HasPrefix(code, prefix) {
			check := mgrReportCheck{Severity: c.Severity, Summary: c.Summary.Message, Count: c.Summary.Count}
			for _, d := range c.Detail {
				check.Detail = append(check.Detail, d.Message)
			}
			out[code] = check
		}
	}
	return out
}

// monNameOf maps a MON session's peer endpoint to its MON name.
func monNameOf(t *testing.T, ctx context.Context, admin *cephmsgr.Client, endpoint string) string {
	t.Helper()
	var mons struct {
		Mons []struct {
			Name  string `json:"name"`
			Addrs struct {
				Addrvec []nativeMgrAddress `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"mons"`
	}
	if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "mon dump", "format": "json"}), &mons); err != nil {
		t.Fatal(err)
	}
	for _, mon := range mons.Mons {
		for _, a := range mon.Addrs.Addrvec {
			if a.Type == "v2" && a.Addr == endpoint {
				return mon.Name
			}
		}
	}
	t.Fatal("MON endpoint is not in the MonMap", endpoint)
	return ""
}

// monSessionsOf returns the MON sessions whose global ID is one of gids,
// as each MON's own `sessions` tell lists them: MON name to global IDs.
func monSessionsOf(t *testing.T, ctx context.Context, admin *cephmsgr.Client, gids ...uint64) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	for _, member := range admin.Snapshot().Monitor.Members {
		result, err := admin.MonTellTo(ctx, member.Name, cephmsgr.Command{JSON: []byte(`{"prefix":"sessions","format":"json"}`)})
		var sessions []struct {
			GlobalID uint64 `json:"global_id"`
			Open     bool   `json:"open"`
		}
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &sessions) != nil {
			t.Fatalf("MON %s sessions: code %d %q %v", member.Name, result.Code, result.Message, err)
		}
		for _, s := range sessions {
			if slices.Contains(gids, s.GlobalID) {
				out[member.Name] = append(out[member.Name], s.GlobalID)
			}
		}
	}
	return out
}

// monDebug sets one MON's own debug_mon through a daemon-local tell and
// returns the previous value.
func monDebug(t *testing.T, ctx context.Context, admin *cephmsgr.Client, mon, value string) string {
	t.Helper()
	result, err := admin.MonTellTo(ctx, mon, cephmsgr.Command{JSON: []byte(`{"prefix":"config get","var":"debug_mon","format":"json"}`)})
	var current struct {
		Value string `json:"debug_mon"`
	}
	if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &current) != nil || current.Value == "" {
		t.Fatalf("MON %s debug_mon: code %d %q %v", mon, result.Code, result.Message, err)
	}
	set, _ := json.Marshal(map[string]any{"prefix": "config set", "var": "debug_mon", "val": []string{value}})
	if result, err := admin.MonTellTo(ctx, mon, cephmsgr.Command{JSON: set}); err != nil || result.Code != 0 {
		t.Fatalf("MON %s set debug_mon %s: code %d %q %v", mon, value, result.Code, result.Message, err)
	}
	return current.Value
}

// ignoredReports counts a MON's "ignoring report from non-active mgr <gid>"
// log lines (fixture control mgr-report-mon-log).
func ignoredReports(t *testing.T, ctx context.Context, control, label, mon string, gid uint64) int {
	t.Helper()
	if err := testcluster.RequestControl(ctx, control, "mgr-report-mon-log", "mgr-report-mon-log-completed", label, mon, fmt.Sprint(gid)); err != nil {
		t.Fatal("MON log", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "mgr-report-mon-log.json"))
	var receipt struct {
		Label    string `json:"label"`
		Admitted bool   `json:"admitted"`
		Monitor  string `json:"monitor"`
		GID      uint64 `json:"gid"`
		Ignored  int    `json:"ignored"`
	}
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Label != label || !receipt.Admitted || receipt.Monitor != mon || receipt.GID != gid {
		t.Fatal("MON log receipt", label, err, string(data))
	}
	return receipt.Ignored
}

// TestCephMgrDaemonReportIntegration sends caller-supplied MMonMgrReports
// from Go MGR daemons (MgrMonitorSession.SendReport) to stock Tentacle MONs
// on a disposable fixture and reads what the MONs committed through
// independent clients, with the stock MGRs stopped so that no other report
// reaches the MONs.
//
// Two scoped MGR principals are admitted: A, which MON selects as active
// once the stock MGRs are stopped and failed, and B, a standby with its own
// nonzero GID, both kept beaconing. Native MgrMap reads bracket every
// observation window and must show A active and B standby throughout.
// Reports carry natively encoded PGMapDigests
// (internal/msgr/testdata/mgr-report-native-provenance.json, digests a and
// b with distinct values) and a fixture-specific health check per report.
//
//  1. Accepted report: A sends digest a with marker check A. The Go admin
//     client's Statfs (its own MON session) must return the four values the
//     native encoder projected from digest a (PGMapDigest::get_statfs) at a
//     newer MgrStat version, `health detail` must list marker A with its
//     severity, summary, count and detail exactly, and the native oracle
//     (librados cluster_stat and health detail on its own session) must
//     agree.
//  2. Stale nonzero GID: with debug_mon 10 on the MON holding B's session,
//     B sends digest b with marker B. For a bounded window the MgrStat
//     version and the Statfs values must stay those of digest a, marker A
//     must stay and marker B never appear, in Go and native reads; that MON's
//     log must show "ignoring report from non-active mgr <B>"
//     (MgrStatMonitor::preprocess_report), so the report reached the
//     handler.
//  3. Positive control: A sends the same content. Digest b and marker B
//     must appear and marker A disappear, at a newer version, within less
//     than the stale window, or the negative result above is inconclusive
//     and the test fails.
//  4. Connection replacement: stopping the MON holding A's session makes A
//     reconnect to another MON. The old handle's Done closes, and a report
//     through it is refused as ErrMgrMonitorExpired wrapping ErrNotSent
//     while Statfs keeps digest b. A new handle on the new session sends
//     digest a with no health check: Statfs returns digest a and both
//     markers are gone, which also restores the health check set.
//  5. Cleanup: after both daemons close, their handles refuse reports with
//     the closed cause, and no MON's own `sessions` list holds a session of
//     either GID within a bounded wait. The stock MGRs restart and one is
//     active again.
//
// Not verified here: progress events and a ServiceMap on a live MON (native
// codec vectors and synthetic tests only; the ServiceMap needs an epoch
// positive control of its own, see #137), every PGMapDigest field beyond
// the statfs projection, a report written but uncertain (synthetic), a
// stale report from a GID that MON never assigned, and other key, mode or
// address profiles.
//
// Pinned sources: ceph/ceph/blob/v20.2.4/src/mon/MgrStatMonitor.cc
// (preprocess_report, prepare_report, preprocess_statfs),
// ceph/ceph/blob/v20.2.4/src/mon/PGMap.cc (get_statfs),
// ceph/ceph/blob/v20.2.4/src/mon/HealthMonitor.cc,
// ceph/ceph/blob/v20.2.4/src/mon/Session.h (the `sessions` dump).
//
//scenario:row mgr-report.aes256k.ip4.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mgrdaemonreport key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 capabilities=mgr-daemon-control
//scenario:covers Daemon-to-daemon
//fixture:isolated stops both stock MGRs and one MON, reports test digests and health checks, raises debug_mon; restores all
func TestCephMgrDaemonReportIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" {
		t.Skip("requires the disposable one-OSD two-MGR fixture")
	}
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	if _, admitted := mgrDaemonDump(t, ctx, control, "report-admission"); !admitted {
		t.Skip("stopping the stock MGRs requires this test alone with fixture capability mgr-daemon-control")
	}
	digests := loadNativeDigests(t, "digest-a", "digest-b")
	digestA, digestB := digests["digest-a"], digests["digest-b"]
	options := integrationOptions(t)
	admin, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ttl := runningServiceTicketTTL(t, ctx, admin)

	var nonce [3]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "GOTEST_MGR_REPORT_"
	markerA, markerB := fmt.Sprintf("%sA_%X", prefix, nonce), fmt.Sprintf("%sB_%X", prefix, nonce)
	checkA := cephmsgr.MgrHealthCheck{Code: markerA, Severity: cephmsgr.MgrHealthWarn, Summary: "gotest report A " + markerA, Detail: []string{"accepted from the active GID"}, Count: 7}
	checkB := cephmsgr.MgrHealthCheck{Code: markerB, Severity: cephmsgr.MgrHealthError, Summary: "gotest report B " + markerB, Detail: []string{"first", "second"}, Count: 11}
	reportA := cephmsgr.MgrMonReport{HealthChecks: []cephmsgr.MgrHealthCheck{checkA}, Digest: digestA.Bytes}
	reportB := cephmsgr.MgrMonReport{HealthChecks: []cephmsgr.MgrHealthCheck{checkB}, Digest: digestB.Bytes}
	rendered := func(c cephmsgr.MgrHealthCheck) mgrReportCheck {
		return mgrReportCheck{Severity: map[cephmsgr.MgrHealthSeverity]string{cephmsgr.MgrHealthWarn: "HEALTH_WARN", cephmsgr.MgrHealthError: "HEALTH_ERR"}[c.Severity], Summary: c.Summary, Count: c.Count, Detail: c.Detail}
	}
	only := func(got map[string]mgrReportCheck, c cephmsgr.MgrHealthCheck) bool {
		want, ok := got[c.Code]
		return ok && len(got) == 1 && want.Severity == rendered(c).Severity && want.Summary == c.Summary && want.Count == c.Count && slices.Equal(want.Detail, c.Detail)
	}

	// Two scoped MGR principals, removed before the admin client closes.
	var entities []string
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, entity := range entities {
			adminCommand(t, cleanup, admin, map[string]any{"prefix": "auth rm", "entity": entity})
		}
	}()
	daemonOptions := func(entity string) cephmsgr.MgrDaemonOptions {
		var created []struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "auth get-or-create", "entity": entity, "caps": []string{"mon", "allow profile mgr", "osd", "allow *", "mds", "allow *"}, "format": "json"}), &created); err != nil || len(created) != 1 {
			t.Fatal("scoped MGR credential", entity, err)
		}
		entities = append(entities, entity)
		key, err := cephmsgr.ParseKey(created[0].Key)
		if err != nil {
			t.Fatal(err)
		}
		return cephmsgr.MgrDaemonOptions{Monitors: options.Monitors, Identity: entity, Key: key, ConnectionMode: options.ConnectionMode,
			ExpectedFSID: options.ExpectedFSID, ServiceTicketTTL: ttl, DialContext: options.DialContext}
	}
	base := time.Now().UnixNano() % 1_000_000
	nameA, nameB := fmt.Sprintf("gorpta%d", base), fmt.Sprintf("gorptb%d", base)
	reporter, err := cephmsgr.DialMgrDaemon(ctx, daemonOptions("mgr."+nameA))
	if err != nil {
		t.Fatal(err)
	}
	defer reporter.Close()
	gidA := reporter.Identity().GlobalID
	beaconsA := startBeaconLoop(reporter, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "reporter"}})
	defer beaconsA.close()
	standby := nextMgrEvent(t, ctx, reporter, "reporter standby", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleStandby })
	stock := waitMgrDump(t, ctx, control, "report-standby", func(d nativeMgrDump) bool {
		return d.Epoch >= standby.Map.Epoch && d.standby(gidA, nameA)
	})
	stockNames := []string{stock.ActiveName}
	for _, s := range stock.Standbys {
		if s.GID != gidA {
			stockNames = append(stockNames, s.Name)
		}
	}
	restored := false
	defer func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		for _, n := range stockNames {
			if err := testcluster.ControlDaemon(cleanup, control, "start", "mgr", n); err != nil {
				t.Error("restart stock MGR", n, err)
			}
		}
	}()
	for _, n := range stockNames {
		if err := testcluster.ControlDaemon(ctx, control, "stop", "mgr", n); err != nil {
			t.Fatal("stop stock MGR", n, err)
		}
	}
	for _, n := range append(stockNames[1:], stockNames[0]) {
		adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": n})
	}
	nextMgrEvent(t, ctx, reporter, "reporter selection", func(e cephmsgr.MgrMapEvent) bool { return e.Role == cephmsgr.MgrRoleActive })
	stale, err := cephmsgr.DialMgrDaemon(ctx, daemonOptions("mgr."+nameB))
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	gidB := stale.Identity().GlobalID
	beaconsB := startBeaconLoop(stale, cephmsgr.MgrBeacon{Metadata: map[string]string{"gotest": "stale"}})
	defer beaconsB.close()
	roles := waitMgrDump(t, ctx, control, "report-roles", func(d nativeMgrDump) bool {
		return d.ActiveGID == gidA && d.ActiveName == nameA && d.standby(gidB, nameB)
	})
	t.Logf("native mgr dump epoch %d: %s/%d active, %s/%d standby; stock %v stopped", roles.Epoch, nameA, gidA, nameB, gidB, stockNames)
	handleA, err := reporter.Monitor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	handleB, err := stale.Monitor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	statfs := func() cephmsgr.StatfsResult {
		t.Helper()
		call, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		s, err := admin.Statfs(call, nil)
		if err != nil {
			t.Fatal("statfs", err)
		}
		return s
	}
	// awaitCommit waits for a Statfs at a version above after with digest d
	// and the health checks ok accepts.
	awaitCommit := func(label string, after uint64, d nativeReportDigest, ok func(map[string]mgrReportCheck) bool) (cephmsgr.StatfsResult, time.Duration) {
		t.Helper()
		started := time.Now()
		for deadline := started.Add(time.Minute); ; {
			s := statfs()
			if s.Version > after && d.matches(s) && ok(reportChecks(t, ctx, admin, prefix)) {
				return s, time.Since(started)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: MON did not commit the report: statfs %+v, checks %v", label, s, reportChecks(t, ctx, admin, prefix))
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	// 1. An accepted report from the active GID.
	baseline := statfs()
	if err := handleA.SendReport(ctx, reportA); err != nil {
		t.Fatal("report A", err)
	}
	committedA, latency := awaitCommit("report A", baseline.Version, digestA, func(c map[string]mgrReportCheck) bool { return only(c, checkA) })
	native := nativeMgrReport(t, ctx, prefix)
	if !native.stable(gidA, gidB) || !native.statfs(digestA) || !only(native.Health, checkA) {
		t.Fatalf("native view after report A: %+v", native)
	}
	t.Logf("report A from active %d on MON session %d (%s): MgrStat version %d -> %d after %v; statfs %d/%d/%d KiB, %d objects as natively projected from digest a; health detail lists %s exactly; the native client agrees",
		gidA, handleA.Generation(), handleA.Endpoint(), baseline.Version, committedA.Version, latency.Round(time.Millisecond), committedA.KB, committedA.KBUsed, committedA.KBAvail, committedA.Objects, markerA)

	// 2. The stale nonzero GID's report never replaces it.
	staleMON := monNameOf(t, ctx, admin, handleB.Endpoint())
	previousDebug := monDebug(t, ctx, admin, staleMON, "10/10")
	debugRestored := false
	defer func() {
		if !debugRestored {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			monDebug(t, cleanup, admin, staleMON, previousDebug)
		}
	}()
	ignoredBefore := ignoredReports(t, ctx, control, "report-log-before", staleMON, gidB)
	opened := nativeMgrReport(t, ctx, prefix)
	if err := handleB.SendReport(ctx, reportB); err != nil {
		t.Fatal("stale report B", err)
	}
	const window = 15 * time.Second
	polls := 0
	for end := time.Now().Add(window); time.Now().Before(end); polls++ {
		s := statfs()
		checks := reportChecks(t, ctx, admin, prefix)
		if s.Version != committedA.Version || !digestA.matches(s) || !only(checks, checkA) {
			t.Fatalf("the stale GID's report changed MON state after %d polls: statfs %+v (accepted version %d), checks %v", polls, s, committedA.Version, checks)
		}
		time.Sleep(500 * time.Millisecond)
	}
	closed := nativeMgrReport(t, ctx, prefix)
	ignored := ignoredReports(t, ctx, control, "report-log-after", staleMON, gidB) - ignoredBefore
	if !opened.stable(gidA, gidB) || !closed.stable(gidA, gidB) {
		t.Fatalf("MgrMap roles moved across the stale window: before %+v after %+v", opened, closed)
	}
	if !closed.statfs(digestA) || !only(closed.Health, checkA) {
		t.Fatalf("native view after the stale window: %+v", closed)
	}
	if ignored < 1 {
		t.Fatalf("mon.%s logged no ignored report from %d: the stale report's MON receipt is unobserved and the negative result inconclusive", staleMON, gidB)
	}
	monDebug(t, ctx, admin, staleMON, previousDebug)
	debugRestored = true
	t.Logf("stale report B from standby %d via mon.%s: %d polls over %v kept MgrStat version %d, digest a's statfs and only %s; MgrMap epochs %d and %d both with %d active and %d standby; mon.%s logged %d ignored report(s) from %d",
		gidB, staleMON, polls, window, committedA.Version, markerA, opened.After.Epoch, closed.Before.Epoch, gidA, gidB, staleMON, ignored, gidB)

	// 3. Positive control: the same content from the active GID.
	if err := handleA.SendReport(ctx, reportB); err != nil {
		t.Fatal("report B from the active GID", err)
	}
	committedB, latency := awaitCommit("positive control", committedA.Version, digestB, func(c map[string]mgrReportCheck) bool { return only(c, checkB) })
	if latency >= window {
		t.Fatalf("the accepted report took %v, not less than the %v stale window: the stale result is inconclusive", latency, window)
	}
	native = nativeMgrReport(t, ctx, prefix)
	if !native.stable(gidA, gidB) || !native.statfs(digestB) || !only(native.Health, checkB) {
		t.Fatalf("native view after the positive control: %+v", native)
	}
	t.Logf("positive control: the same content from active %d committed at MgrStat version %d after %v (< %v window); digest b's statfs and only %s, native client agrees",
		gidA, committedB.Version, latency.Round(time.Millisecond), window, markerB)

	// 4. Connection replacement and the stale handle.
	monA := monNameOf(t, ctx, admin, handleA.Endpoint())
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", monA); err != nil {
		t.Fatal("stop MON", monA, err)
	}
	monStopped := true
	defer func() {
		if monStopped {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := testcluster.ControlDaemon(cleanup, control, "start", "mon", monA); err != nil {
				t.Error("restart MON", monA, err)
			}
		}
	}()
	select {
	case <-handleA.Done():
	case <-ctx.Done():
		t.Fatal("the handle of the stopped MON's session did not end")
	}
	replaced := waitDaemonState(t, ctx, reporter, func(s cephmsgr.MgrDaemonState) bool {
		return s.Monitor.Ready && s.Monitor.Generation > handleA.Generation()
	})
	if err := handleA.SendReport(ctx, reportA); !errors.Is(err, cephmsgr.ErrMgrMonitorExpired) || !errors.Is(err, cephmsgr.ErrNotSent) {
		t.Fatal("a report through the replaced session's handle", err)
	}
	if s := statfs(); s.Version != committedB.Version || !digestB.matches(s) {
		t.Fatalf("MON state changed after the refused report: %+v", s)
	}
	handleA2, err := reporter.Monitor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if handleA2.Generation() < replaced.Monitor.Generation || handleA2.Endpoint() == handleA.Endpoint() {
		t.Fatalf("new handle generation %d at %s; old %d at %s", handleA2.Generation(), handleA2.Endpoint(), handleA.Generation(), handleA.Endpoint())
	}
	if err := handleA2.SendReport(ctx, cephmsgr.MgrMonReport{Digest: digestA.Bytes}); err != nil {
		t.Fatal("report on the new session", err)
	}
	restoredA, _ := awaitCommit("report on the new session", committedB.Version, digestA, func(c map[string]mgrReportCheck) bool { return len(c) == 0 })
	t.Logf("mon.%s stopped: MON session %d (%s) ended, its handle refused a report as %v; session %d (%s) committed digest a with no health check at MgrStat version %d",
		monA, handleA.Generation(), handleA.Endpoint(), cephmsgr.ErrMgrMonitorExpired, handleA2.Generation(), handleA2.Endpoint(), restoredA.Version)
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", monA); err != nil {
		t.Fatal("restart MON", monA, err)
	}
	monStopped = false
	waitFor(t, ctx, "MON quorum restored", func() bool {
		var status struct {
			Quorum []string `json:"quorum_names"`
		}
		return json.Unmarshal(adminCommand(t, ctx, admin, map[string]any{"prefix": "quorum_status", "format": "json"}), &status) == nil &&
			slices.Contains(status.Quorum, monA)
	})

	// 5. Close both daemons; no MON keeps their sessions.
	if err := beaconsA.close(); err != nil {
		t.Fatal("reporter beacons", err)
	}
	if err := beaconsB.close(); err != nil {
		t.Fatal("stale beacons", err)
	}
	reporter.Close()
	stale.Close()
	for _, h := range []*cephmsgr.MgrMonitorSession{handleA2, handleB} {
		if err := h.SendReport(ctx, reportA); !errors.Is(err, cephmsgr.ErrMgrMonitorExpired) || !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal("a report after Close", err)
		}
	}
	var left map[string][]uint64
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		if left = monSessionsOf(t, ctx, admin, gidA, gidB); len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("MON sessions of the closed daemons remain", left)
		}
	}
	t.Logf("after Close: both handles refuse reports; no MON lists a session of %d or %d", gidA, gidB)

	// Restore a stock active MGR.
	for _, n := range stockNames {
		if err := testcluster.ControlDaemon(ctx, control, "start", "mgr", n); err != nil {
			t.Fatal("restart stock MGR", n, err)
		}
	}
	restored = true
	adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": nameA})
	adminCommand(t, ctx, admin, map[string]any{"prefix": "mgr fail", "who": nameB})
	back := waitMgrDump(t, ctx, control, "report-restored", func(d nativeMgrDump) bool {
		return d.Available && d.ActiveGID != gidA && d.ActiveGID != gidB && slices.Contains(stockNames, d.ActiveName)
	})
	t.Logf("restored: stock %s/%d active at native epoch %d", back.ActiveName, back.ActiveGID, back.Epoch)
}
