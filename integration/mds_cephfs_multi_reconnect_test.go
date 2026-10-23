package integration_test

import (
	"context"
	"encoding/binary"
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
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephMDSCephFSMultiReconnectIntegration restores only real retained
// capabilities, with no request replay or pending metadata flush. The small
// frame bound makes this a split-wire acceptance rather than a scale test.
func TestCephMDSCephFSMultiReconnectIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned split reconnect acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("split reconnect requires the owned completion fixture")
	}
	ctx, stop := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(stop)
	var oracle nativeMDSCephFSCompletionOracle
	var fresh nativeMDSFresh
	var current nativeMDSCephFSTarget
	for _, read := range []struct {
		name   string
		target any
	}{{"mds-cephfs-completion-oracle.json", &oracle}, {"mds-fresh-oracle.json", &fresh}, {"mds-cephfs-completion-target.json", &current}} {
		if err := readMDSFreshJSON(filepath.Join(dir, read.name), read.target); err != nil {
			t.Fatal(err)
		}
	}
	identity, key := os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE")
	names := make([]string, 32)
	for i := range names {
		names[i] = fmt.Sprintf("split-%03d-%s", i, strings.Repeat("s", 215))
	}
	if oracle.Source != mdsFreshSource || oracle.Token != fresh.Token || len(oracle.Token) != 32 || oracle.FSID != fresh.FSID || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fresh.FSName || identity != "client.mds-cephfs-completion-"+oracle.Token || oracle.ClientIdentity != identity ||
		oracle.ClientCaps["mds"] != "allow rw fsname="+fresh.FSName || oracle.Plan.Directory != "ceph-msgr-completion-"+oracle.Token || !slices.Equal(oracle.Plan.SplitFiles, names) ||
		current.Source != mdsFreshSource || current.Token != oracle.Token || current.Adopted != 0 || current.MDS.GID != fresh.MDS.GID || current.MDS.Nonce != fresh.MDS.Nonce {
		t.Fatal("split reconnect oracle or closed file plan differs")
	}
	const frameLimit = 8 << 10
	control := &mdsCephFSCompletionControl{dir: dir, token: oracle.Token, fresh: fresh, mds: current.MDS, goGIDs: map[uint64]bool{}}
	target := func(d nativeMDSCephFSDaemon) cephmsgr.MDSTarget {
		return cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", d.Endpoint, d.Nonce)}
	}
	dials := newRecoveryDials(false, current.MDS.Endpoint)
	client := recoveryClient(t, ctx, identity, key, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS = true, true })
	snap := client.Snapshot()
	control.goGIDs[snap.GlobalID] = true
	metadata := map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-split", "root": "/"}
	options := cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024}
	var connection *cephmsgr.MDSConnection
	var session *cephmsgr.MDSSession
	var agent *cephfsCapAgent
	var epoch uint32
	var dirIno uint64
	inodes := map[string]uint64{}
	realms := map[uint64]cephmsgr.MDSSnapRealm{}
	facts := map[string]any{"reconnect_message_limit": frameLimit, "frame_limit": 1 << 20, "files": names, "global_id": snap.GlobalID, "shared_nonce": snap.CephFSNonce, "request_resubmissions": 0, "pending_cap_flushes": 0}
	var requests []completionReply
	var nativeLines []string
	caseTimes := map[string]int64{}
	t.Cleanup(func() {
		facts["mds_dials"], facts["osd_dials"] = dials.attempts(), dials.OSD.Load()
		if agent != nil {
			facts["agent"] = agent.snapshot()
		}
		data, err := json.Marshal(map[string]any{"source_commit": mdsFreshSource, "facts": facts, "requests": requests, "native": control.receipts, "native_reconnect_lines": nativeLines, "case_ms": caseTimes})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("split reconnect evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-multi-reconnect-results.json"), data)
		}
		if err != nil {
			t.Error("preserve split reconnect evidence", err)
		}
	})
	req := func(t *testing.T, action string, args ...string) nativeMDSCephFSCompletionReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("split reconnect control", action, err)
		}
		return r
	}
	observe := func(t *testing.T) *nativeMDSSession {
		t.Helper()
		r := req(t, "observe", strconv.FormatUint(snap.GlobalID, 10))
		epoch = r.Map.Epoch
		return r.session(snap.GlobalID)
	}
	submit := func(t *testing.T, name string, r cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		r.MDSMapEpoch = epoch
		at := time.Now()
		call, err := session.Submit(ctx, r)
		if err != nil {
			t.Fatal("submit", name, err)
		}
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		first, err := call.Wait(wait)
		requests = append(requests, describeCompletionReply(name, call, first, err, at))
		if err != nil || first.Result != 0 {
			t.Fatal("first reply", name, first.Result, err)
		}
		trace, _, err := agent.applyReply(name, first)
		if err != nil {
			t.Fatal(err)
		}
		rs, err := first.DecodeSnapTrace()
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rs {
			realms[r.Ino] = r
		}
		if !first.Safe {
			if err := session.FlushJournal(wait); err != nil {
				t.Fatal(err)
			}
			safe, err := call.WaitSafe(wait)
			requests = append(requests, describeCompletionReply(name+" safe", call, safe, err, at))
			if err != nil || safe.Result != 0 || !safe.Safe {
				t.Fatal("safe reply", name, safe.Result, err)
			}
		}
		return first, trace
	}
	leaf := func(name string, run func(*testing.T)) bool {
		at := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(at).Milliseconds()
		if agent != nil {
			if s := agent.snapshot(); len(s.Errors) != 0 {
				t.Error("split cap agent", s.Errors)
				ok = false
			}
		}
		return ok
	}
	if !leaf("retain_real_capabilities_beyond_one_frame", func(t *testing.T) {
		var err error
		connection, err = client.OpenMDS(ctx, target(current.MDS))
		if err != nil {
			t.Fatal(err)
		}
		session, err = connection.OpenSession(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		agent = startCephFSCapAgent(ctx, session)
		if err := checkNativeCephFSSession(observe(t), identity, metadata); err != nil {
			t.Fatal(err)
		}
		_, tr := submit(t, "split mkdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: oracle.Plan.Directory}})
		if tr.Target == nil {
			t.Fatal("mkdir trace missing")
		}
		dirIno = tr.Target.Ino
		for _, name := range names {
			first, tr := submit(t, "split create "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
			if tr.Target == nil || len(first.Extra) != 8 || binary.LittleEndian.Uint64(first.Extra) != tr.Target.Ino {
				t.Fatal("real create inode differs", name)
			}
			inodes[name] = tr.Target.Ino
			agent.open(tr.Target.Ino, cephFileModeRDWR)
		}
		listing := req(t, "list").Native
		if listing == nil || len(listing.Entries) != len(names) {
			t.Fatal("native initial listing differs", listing)
		}
		for _, e := range listing.Entries {
			if inodes[e.Name] != e.Stat.Ino || e.DIno != e.Stat.Ino || e.Stat.Size != 0 {
				t.Fatal("native initial inode differs", e)
			}
		}
		facts["native_before"], facts["inodes"] = listing, inodes
	}) {
		return
	}
	if !leaf("split_reconnect_to_new_incarnation", func(t *testing.T) {
		armed := req(t, "restart-arm").Restart
		facts["armed"] = armed
		req(t, "restart-fire")
		select {
		case <-session.Done():
		case <-time.After(45 * time.Second):
			t.Fatal("split session did not end after restart")
		}
		lost := session
		if lost.Status().State != cephmsgr.MDSSessionLost || !describeRecoveryError(lost.Err()).SessionReset {
			t.Fatal("split session did not become Lost with RESET", lost.Status())
		}
		select {
		case <-agent.done:
		case <-time.After(10 * time.Second):
			t.Fatal("old split cap agent did not stop")
		}
		caps, rs, err := agent.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		for name, ino := range inodes {
			if !slices.ContainsFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == ino && c.PathBase == dirIno && c.Path == name }) {
				t.Fatal("reconnect omitted a real held cap", name, ino)
			}
		}
		full, err := mds.EncodeReconnect(caps, rs, false)
		if err != nil {
			t.Fatal(err)
		}
		fullBytes := 41 + len(full.Front) + len(full.Middle) + len(full.Data)
		if fullBytes <= frameLimit {
			t.Fatal("real reconnect state fits in one frame", fullBytes)
		}
		facts["reconnect_caps"], facts["reconnect_realms"], facts["unsplit_bytes"], facts["lost"] = caps, rs, fullBytes, lost.Status()
		var next *nativeMDSCephFSDaemon
		until := time.Now().Add(60 * time.Second)
		for next == nil {
			status := req(t, "restart-status").RankStatus
			if status.Rank0 != nil && status.Rank0.GID != armed.Armed.GID && status.Rank0.Name == armed.Armed.Name && status.Rank0.State != "up:replay" {
				next = status.Rank0
				break
			}
			if time.Now().After(until) {
				t.Fatal("split replacement never reached reconnect", status)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if next.State != "up:reconnect" || next.Nonce == armed.Armed.Nonce || next.Incarnation == armed.Armed.Incarnation {
			t.Fatal("split replacement incarnation differs", next)
		}
		facts["replacement"] = next
		req(t, "debug-on")
		req(t, "mds-log-mark")
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		dials.allowMDS(next.Endpoint)
		connection, err = client.OpenMDS(ctx, target(*next))
		if err != nil {
			t.Fatal(err)
		}
		opening, cancel := context.WithTimeout(ctx, 30*time.Second)
		reopened, calls, err := connection.ReconnectSession(opening, lost, cephmsgr.MDSReconnectState{MaxMessageSize: frameLimit, Caps: caps, Realms: rs}, options)
		cancel()
		if err != nil || len(calls) != 0 || reopened == nil || reopened.Status().State != cephmsgr.MDSSessionOpen || reopened.Status().ReconnectedFrom != lost.Status().Generation || reopened.Status().GlobalID != snap.GlobalID {
			t.Fatal("split reconnect did not OPEN without replay", err)
		}
		session = reopened
		agent = startCephFSCapAgentFrom(ctx, reopened, agent)
		facts["reconnected"] = session.Status()
		until = time.Now().Add(60 * time.Second)
		for {
			status := req(t, "restart-status").RankStatus
			if status.Rank0 != nil && status.Rank0.GID == next.GID && status.Rank0.State == "up:active" {
				break
			}
			if time.Now().After(until) {
				t.Fatal("split replacement never became active", status)
			}
			time.Sleep(100 * time.Millisecond)
		}
		req(t, "restart-adopt")
		log := req(t, "reconnect-log", strconv.FormatUint(snap.GlobalID, 10), strconv.FormatUint(uint64(snap.CephFSNonce), 10)).Log
		if log == nil || log.Dropped != 0 {
			t.Fatal("native split reconnect log missing or truncated", log)
		}
		prefix := fmt.Sprintf("handle_client_reconnect client.%d", snap.GlobalID)
		for _, line := range log.Lines {
			if strings.Contains(line, prefix) {
				nativeLines = append(nativeLines, line)
			}
		}
		if len(nativeLines) < 2 {
			t.Fatal("native MDS handled fewer than two reconnect fragments", nativeLines)
		}
		for i, line := range nativeLines {
			if strings.HasSuffix(strings.TrimSpace(line), " (more)") != (i < len(nativeLines)-1) {
				t.Fatal("native nonfinal/final reconnect ordering differs", nativeLines)
			}
		}
		facts["fragment_count"] = len(nativeLines)
		req(t, "debug-off")
		native := observe(t)
		if err := checkNativeCephFSSession(native, identity, metadata); err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(native.Address, ":0/"+strconv.FormatUint(uint64(snap.CephFSNonce), 10)) || native.Caps < int64(len(names)) {
			t.Fatal("split reconnect changed native identity or lost held caps", native)
		}
	}) {
		return
	}
	if !leaf("native_namespace_and_go_lookups_after_split", func(t *testing.T) {
		listing := req(t, "list").Native
		if listing == nil || len(listing.Entries) != len(names) {
			t.Fatal("native post-reconnect listing differs", listing)
		}
		for _, e := range listing.Entries {
			if inodes[e.Name] != e.Stat.Ino || e.DIno != e.Stat.Ino || e.Stat.Size != 0 {
				t.Fatal("native post-reconnect inode differs", e)
			}
		}
		facts["native_after"] = listing
		for _, name := range names {
			_, tr := submit(t, "split lookup "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
			if tr.Target == nil || tr.Target.Ino != inodes[name] {
				t.Fatal("Go lookup changed native inode after split", name)
			}
		}
		if dials.OSD.Load() != 0 || client.Snapshot().GlobalID != snap.GlobalID || client.Snapshot().CephFSNonce != snap.CephFSNonce {
			t.Fatal("split reconnect issued object I/O or changed client identity")
		}
	}) {
		return
	}
	leaf("cleanup_unlink_release_close", func(t *testing.T) {
		for _, name := range names {
			if _, _, err := agent.close(inodes[name], cephFileModeRDWR); err != nil {
				t.Fatal(err)
			}
			submit(t, "split unlink "+name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: name}})
		}
		submit(t, "split rmdir", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpRmdir, Path: cephmsgr.MDSFilePath{Ino: 1, Path: oracle.Plan.Directory}})
		native, err := control.stat(ctx, "dir")
		if err != nil || native.Exists || native.Errno != 2 {
			t.Fatal("native split directory remained", native, err)
		}
		items, barrier := agent.releaseAll()
		if len(items) != 0 {
			if err := session.ReleaseCaps(ctx, items, barrier); err != nil {
				t.Fatal(err)
			}
		}
		closing, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = session.Close(closing)
		cancel()
		if err != nil || session.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("split close unconfirmed", err)
		}
		if observe(t) != nil {
			t.Fatal("split session remains in native map")
		}
	})
}
