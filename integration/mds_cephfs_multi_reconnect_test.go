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
	runCephFSSplitReconnect(t, nil)
}

// runCephFSSplitReconnect is the split reconnect recipe. With held set it
// also holds one exclusive whole-file FLOCK on each of the plan's
// predetermined split files that held names across the restart
// (mds_cephfs_heldflock_split_test.go).
func runCephFSSplitReconnect(t *testing.T, held *splitHeldFlock) {
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
	results, debugOn, debugOff := "mds-cephfs-multi-reconnect-results.json", "debug-on", "debug-off"
	if held != nil {
		// Level 15 adds the native per-capability reconnect lines.
		results, debugOn, debugOff = held.Results, "debug-caps-on", "debug-caps-off"
	}
	facts := map[string]any{"reconnect_message_limit": frameLimit, "frame_limit": 1 << 20, "files": names, "global_id": snap.GlobalID, "shared_nonce": snap.CephFSNonce, "request_resubmissions": 0, "pending_cap_flushes": 0}
	var requests []completionReply
	// submitted is the common ledger of every request Submit accepted.
	var submitted []splitSubmission
	var nativeLines []string
	caseTimes := map[string]int64{}
	t.Cleanup(func() {
		facts["mds_dials"], facts["osd_dials"] = dials.attempts(), dials.OSD.Load()
		if agent != nil {
			facts["agent"] = agent.snapshot()
		}
		data, err := json.Marshal(map[string]any{"source_commit": mdsFreshSource, "facts": facts, "requests": requests, "submitted": submitted, "native": control.receipts, "native_reconnect_lines": nativeLines, "case_ms": caseTimes})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("split reconnect evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, results), data)
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
	// submitWith reports an accepted call to accepted before it awaits the
	// first reply, and the outcome of that wait to settled before any check
	// on it can fail.
	submitWith := func(t *testing.T, name string, r cephmsgr.MDSRequest, accepted func(uint64), settled func(cephmsgr.MDSReply, error)) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		r.MDSMapEpoch = epoch
		at := time.Now()
		call, err := session.Submit(ctx, r)
		if err != nil {
			t.Fatal("submit", name, err)
		}
		submitted = append(submitted, splitSubmission{Name: name, Op: r.Op, TID: call.Transaction()})
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		first, err := awaitAcceptedMDSCall(wait, call, accepted, settled)
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
	submit := func(t *testing.T, name string, r cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		return submitWith(t, name, r, nil, nil)
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
	// getFlock asks with an exclusive test lock of another owner which lock
	// is in the way on the lock's inode.
	getFlock := func(t *testing.T, name string, l *splitHeldLock) cephfsFilelock {
		t.Helper()
		first, _ := submit(t, name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock, Path: cephmsgr.MDSFilePath{Ino: l.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner+1)})
		got, err := cephfsDecodeFilelock(first.Extra)
		if err != nil || got != l.Lock {
			t.Fatalf("GETFILELOCK (%s) does not identify the owned Go lock %+v: %+v %v", name, l.Lock, got, err)
		}
		return got
	}
	if held != nil {
		facts["held_flock"] = held
		if !leaf("hold_flock_on_split_member", func(t *testing.T) {
			// The create handles of the lock files are still open read-write.
			held.Directory, held.MDSBefore = oracle.Plan.Directory, control.mds
			for _, l := range held.Locks {
				l.File = oracle.Plan.SplitFiles[0]
				if l.Final {
					l.File = oracle.Plan.SplitFiles[len(oracle.Plan.SplitFiles)-1]
				}
				l.Ino, l.Lock = inodes[l.File], cephfsFilelock{Client: snap.GlobalID, Owner: l.Owner, Type: cephFlockExclusive}
			}
			for _, l := range held.Locks {
				submitWith(t, "split flock acquire "+l.Name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Path: cephmsgr.MDSFilePath{Ino: l.Ino}, Args: cephfsFlockArgs(cephFlockExclusive, l.Owner)},
					func(tid uint64) { held.acceptFlock(l, false, tid) }, func(r cephmsgr.MDSReply, err error) { l.settleFlock(false, r, err) })
			}
			errnos, err := splitFlockProbeAll(ctx, control, held, false, func(*splitHeldLock) int { return 11 })
			if err != nil {
				t.Fatal("native nonblocking probes do not see every Go lock on its split file: errnos ", errnos, ": ", err)
			}
			for _, l := range held.Locks {
				lock := getFlock(t, "split flock get "+l.Name+" before restart", l)
				l.GetBefore = &lock
				t.Logf("held-lock split: %s lock, inode %#x locked by Go owner %#x TID %d; native probe errno %d, native version %d",
					l.Name, l.Ino, l.Owner, l.AcquireTID, l.ProbeBefore[0].Probe.Errno, l.ProbeBefore[0].Probe.Before.Version)
			}
		}) {
			return
		}
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
		if held != nil {
			// Each caller-built native-layout blob goes on exactly its locked
			// inode's capability; SETFILELOCK is not submitted again.
			if held.SentInodes, err = splitHeldFlockAttach(caps, held); err != nil {
				t.Fatal(err)
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
		req(t, debugOn)
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
		if held != nil {
			// Which fragment carried the locked inode's capability, from the
			// replacement MDS's own per-capability lines.
			capLog := req(t, "reconnect-cap-log", strconv.FormatUint(snap.GlobalID, 10), strconv.FormatUint(uint64(snap.CephFSNonce), 10)).Log
			if capLog == nil || capLog.Dropped != 0 {
				t.Fatal("native per-capability reconnect log missing or truncated", capLog)
			}
			held.CapLines = capLog.Lines
			if held.Fragments, err = splitReconnectCapFragments(capLog.Lines, snap.GlobalID); err != nil {
				t.Fatal(err)
			}
			if err = splitHeldFlockFragments(held, len(nativeLines)); err != nil {
				t.Fatal(err)
			}
			var perFragment []int
			for _, f := range held.Fragments {
				perFragment = append(perFragment, len(f.Inodes))
			}
			t.Logf("held-lock split: %d caps and %d unsplit bytes in %d fragments; native capability lines per fragment %v", len(caps), fullBytes, len(held.Fragments), perFragment)
			for _, l := range held.Locks {
				t.Logf("held-lock split: %s lock, inode %#x with a %d-byte blob in fragment %d of %d (more=%v)", l.Name, l.Ino, len(l.ReconnectBlobHex)/2, l.Fragment, len(held.Fragments), held.Fragments[l.Fragment].More)
			}
		}
		req(t, debugOff)
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
	if held != nil && !leaf("held_flock_restored_then_released", func(t *testing.T) {
		held.MDSAfter = control.mds
		if held.MDSAfter.GID == held.MDSBefore.GID || held.MDSAfter.Nonce == held.MDSBefore.Nonce || held.MDSAfter.Rank != 0 || held.MDSBefore.Rank != 0 {
			t.Fatal("held-lock split recovery did not cross to a new rank-0 MDS incarnation", held.MDSBefore, held.MDSAfter)
		}
		// The one bounded historic-ops snapshot read after adoption holds no
		// client request for an original lock TID. An aged-out entry would
		// look the same, so this is not proof that the new incarnation never
		// received it.
		ops := req(t, "historic-ops", strconv.FormatUint(snap.GlobalID, 10)).Ops
		for _, l := range held.Locks {
			prefix := fmt.Sprintf("client_request(client.%d:%d ", snap.GlobalID, l.AcquireTID)
			found := slices.ContainsFunc(ops, func(op nativeMDSCephFSHistoricOp) bool { return strings.HasPrefix(op.Description, prefix) })
			l.AcquireTIDInHistoricOps = &found
			if found {
				t.Fatal("the new incarnation tracked a client request for the original SETFILELOCK TID", l.AcquireTID)
			}
		}
		// Every lock file is probed before any check fails, so a lock that
		// was not restored is reported beside the state of the others.
		errnos, err := splitFlockProbeAll(ctx, control, held, true, func(*splitHeldLock) int { return 11 })
		if err != nil {
			t.Fatal("native nonblocking probes do not see every restored lock on its split file: errnos ", errnos, ": ", err)
		}
		for _, l := range held.Locks {
			restored := getFlock(t, "split flock get "+l.Name+" after restart", l)
			l.GetAfter = &restored
		}
		t.Logf("held-lock split: MDS GID %d to %d; native probe errnos after restart %v", held.MDSBefore.GID, held.MDSAfter.GID, errnos)
		// One explicit unlock at a time: the native probe then acquires that
		// file and is still refused on every file whose lock is still held.
		for i, l := range held.Locks {
			submitWith(t, "split flock unlock "+l.Name+" after restart", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Path: cephmsgr.MDSFilePath{Ino: l.Ino}, Args: cephfsFlockArgs(cephFlockUnlock, l.Owner)},
				func(tid uint64) { held.acceptFlock(l, true, tid) }, func(r cephmsgr.MDSReply, err error) { l.settleFlock(true, r, err) })
			released := held.Locks[:i+1]
			errnos, err := splitFlockProbeAll(ctx, control, held, true, func(p *splitHeldLock) int {
				if slices.Contains(released, p) {
					return 0
				}
				return 11
			})
			if err != nil {
				t.Fatal("native probes after the explicit unlock of the ", l.Name, " lock differ: errnos ", errnos, ": ", err)
			}
			for _, other := range held.Locks[i+1:] {
				getFlock(t, "split flock get "+other.Name+" after unlock of "+l.Name, other)
			}
			t.Logf("held-lock split: %s lock restored for owner %#x client %d, unlock TID %d; native probe errnos afterwards %v", l.Name, l.GetAfter.Owner, l.GetAfter.Client, l.UnlockTID, errnos)
		}
		// Exactly one acquire and one unlock per lock were submitted, by any
		// call site of the recipe.
		if err := splitHeldFlockLedgerMatches(submitted, held); err != nil {
			t.Fatal(err)
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
