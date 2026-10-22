package integration_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// capSendUpdateBranch classifies the owned MDS's processing of one UPDATE in
// a filtered cap log: the fixture keeps a handler line only after its exact
// incoming source, and appends at most one same-thread branch right after
// it. "applied" is the dirty-flush branch for this client and inode,
// "completed" the completed-flush (deduplicated) branch, "handled" a handler
// without a recorded branch, and "" no handler.
func capSendUpdateBranch(log *nativeMDSCephFSCompletionLog, gid, ino, tid, follows uint64) (string, error) {
	if log == nil {
		return "", nil
	}
	handler := fmt.Sprintf("handle_client_caps on 0x%x tid %d follows %x op update", ino, tid, follows)
	at := -1
	for i, line := range log.Lines {
		if strings.Contains(line, handler) {
			if at >= 0 {
				return "", fmt.Errorf("UPDATE tid %d was handled more than once", tid)
			}
			at = i
		}
	}
	if at < 0 {
		return "", nil
	}
	if at+1 < len(log.Lines) {
		next := log.Lines[at+1]
		if strings.Contains(next, fmt.Sprintf("handle_client_caps already flushed tid %d for client.%d", tid, gid)) {
			return "completed", nil
		}
		if strings.Contains(next, fmt.Sprintf(" flush client.%d dirty Fw ", gid)) && strings.Contains(next, fmt.Sprintf("[inode 0x%x ", ino)) {
			return "applied", nil
		}
	}
	return "handled", nil
}

// capSendIncomingDrop finds the attributed ProtocolV2 drop of one UPDATE.
func capSendIncomingDrop(log *nativeMDSCephFSCompletionLog, ino, tid, follows uint64) []string {
	return log.find(".handle_message blackhole ", fmt.Sprintf("client_caps(update ino 0x%x ", ino), fmt.Sprintf(" tid %d ", tid), fmt.Sprintf(" follows %x ", follows))
}

// recoverUncertainCapSends is a test consumer decision across one owned MDS
// restart. Its data writes have completed before either UPDATE is prepared.
//
//  1. With the owned daemon dropping incoming client messages after
//     Messenger receipt, the first UPDATE is admitted by SendCaps. The
//     boundary is the native drop line for its exact tuple from the
//     registered peer nonce, never elapsed time. Its remote outcome is
//     uncertain to the caller.
//  2. The second UPDATE is prepared but first attempted only after the
//     session is Lost; SendCaps returns the terminal cause before admission,
//     a known-unsent outcome.
//  3. After reconnect and adoption, the caller explicitly resubmits the
//     first with its original TID and first-sends the second. The new
//     incarnation must apply both as new flushes (neither deduplicated), and
//     both FLUSH_ACKs must correlate. No object request is repeated.
func recoverUncertainCapSends(t *testing.T, ctx context.Context, control *mdsCephFSDataControl,
	client *cephmsgr.Client, dials *recoveryDials, oldConnection *cephmsgr.MDSConnection,
	oldSession *cephmsgr.MDSSession, oldAgent *cephfsCapAgent, realms map[uint64]cephmsgr.MDSSnapRealm,
	metadata map[string]string, io *cephfsDataIO, uncertain *cephfsHeadFlush, prepareRefused func() *cephfsHeadFlush, facts map[string]any,
) (*cephmsgr.MDSConnection, *cephmsgr.MDSSession, *cephfsCapAgent, cephmsgr.MDSTarget, *cephfsHeadFlush) {
	t.Helper()
	request := func(action string, args ...string) nativeMDSCephFSDataReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("cap-send recovery control", action, err)
		}
		return r
	}
	countIO := func() int {
		io.mu.Lock()
		defer io.mu.Unlock()
		return len(io.log)
	}
	identity := client.Snapshot()
	gid, nonce := strconv.FormatUint(identity.GlobalID, 10), strconv.FormatUint(uint64(identity.CephFSNonce), 10)
	ino, follows := uncertain.Ino, uncertain.Caps.SnapFollows
	snapshotFlush := func(f *cephfsHeadFlush) cephfsHeadFlush {
		oldAgent.mu.Lock()
		defer oldAgent.mu.Unlock()
		return cloneCephFSHeadFlush(f)
	}
	facts["prepared_uncertain"] = snapshotFlush(uncertain)
	facts["debug"] = request("debug-on")
	facts["cap_log_mark"] = request("cap-log-mark")
	facts["client_drop"] = request("client-drop-on")
	if err := oldAgent.firstSendHeadFlush(uncertain, oldSession.Status(), oldSession.SendCaps); err != nil {
		t.Fatal("admit the first UPDATE on the old session", err)
	}
	if oldAgent.headFlushOutcome(uncertain) != "uncertain" {
		t.Fatal("admitted UPDATE is not an uncertain outcome", uncertain.Attempts)
	}
	var dropLines []string
	deadline := time.Now().Add(15 * time.Second)
	for {
		observed := request("cap-log", gid, nonce)
		facts["old_caps_log"] = observed.CapLog
		dropLines = capSendIncomingDrop(observed.CapLog, ino, uncertain.Transaction, follows)
		if len(dropLines) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old MDS did not record the attributed drop of the admitted UPDATE", observed.CapLog)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(dropLines) != 1 {
		t.Fatal("the admitted UPDATE was received more than once", dropLines)
	}
	facts["old_incoming_drop"] = dropLines
	refused := prepareRefused()
	if refused == nil || refused.Ino != ino || refused.Transaction <= uncertain.Transaction || refused.Caps.OldestFlushTID != uncertain.Transaction {
		t.Fatal("second UPDATE identity differs", refused)
	}
	facts["prepared_refused"] = snapshotFlush(refused)
	beforeIO := countIO()
	// Final old-incarnation view before the restart: the dropped UPDATE never
	// reached its handler.
	observed := request("cap-log", gid, nonce)
	facts["old_caps_log"] = observed.CapLog
	if branch, err := capSendUpdateBranch(observed.CapLog, identity.GlobalID, ino, uncertain.Transaction, follows); err != nil || branch != "" ||
		len(capSendIncomingDrop(observed.CapLog, ino, uncertain.Transaction, follows)) != 1 ||
		len(observed.CapLog.find(fmt.Sprintf(" tid %d ", refused.Transaction))) != 0 {
		t.Fatal("old MDS handled the dropped UPDATE or saw the unsent one", branch, err, observed.CapLog)
	}
	armed := request("restart-arm").Restart
	facts["armed"], facts["object_requests_before_restart"] = armed, beforeIO
	facts["fire_started"] = time.Now()
	request("restart-fire")
	select {
	case <-oldSession.Done():
	case <-time.After(45 * time.Second):
		t.Fatal("cap-send session did not end after MDS restart")
	}
	lost := oldSession.Status()
	facts["lost"], facts["resume_error"] = lost, describeRecoveryError(oldSession.Err())
	if lost.State != cephmsgr.MDSSessionLost || !describeRecoveryError(oldSession.Err()).SessionReset {
		t.Fatal("cap-send session did not end with the restarted MDS's RESET", lost)
	}
	// Known unsent: the Lost session refuses the second UPDATE before admission.
	err := oldAgent.firstSendHeadFlush(refused, lost, oldSession.SendCaps)
	if err == nil || oldSession.Err() == nil || !errors.Is(err, oldSession.Err()) {
		t.Fatal("cap send on the Lost session was not refused with its terminal cause", err)
	}
	if oldAgent.headFlushOutcome(refused) != "unsent" || oldAgent.headFlushOutcome(uncertain) != "uncertain" {
		t.Fatal("cap-send outcome classes differ after Lost", uncertain.Attempts, refused.Attempts)
	}
	facts["refused_send_error"] = err.Error()
	select {
	case <-oldAgent.done:
	case <-time.After(10 * time.Second):
		t.Fatal("old cap-send agent did not stop")
	}
	oldSnapshot := oldAgent.snapshot()
	facts["agent_before_reconnect"] = oldSnapshot
	if len(oldSnapshot.Errors) != 0 || oldSnapshot.Pending != 0 ||
		slices.ContainsFunc(oldSnapshot.FlushAcks, func(a cephfsFlushAck) bool {
			return a.Transaction == uncertain.Transaction || a.Transaction == refused.Transaction
		}) {
		t.Fatal("old cap-send agent ended with errors, unapplied pushes or an ACK", oldSnapshot.Errors, oldSnapshot.Pending)
	}
	var next *nativeMDSCephFSDaemon
	var timeline []*nativeMDSCephFSRankStatus
	deadline = time.Now().Add(60 * time.Second)
	for next == nil {
		s := request("restart-status").RankStatus
		timeline = append(timeline, s)
		if s.Rank0 != nil && s.Rank0.GID != armed.Armed.GID && s.Rank0.Name == armed.Armed.Name && s.Rank0.State != "up:replay" {
			next = s.Rank0
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned MDS did not reach cap-send reconnect", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["status_timeline"] = timeline
	if next.State != "up:reconnect" || next.Nonce == 0 || next.Nonce == armed.Armed.Nonce || next.Incarnation == armed.Armed.Incarnation {
		t.Fatal("cap-send reconnect target is not a new owned incarnation", next)
	}
	caps, realmState, err := oldAgent.reconnectState(realms)
	if err != nil || !slices.ContainsFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == ino }) {
		t.Fatal("cap-send reconnect state", caps, err)
	}
	facts["reconnect_caps"], facts["reconnect_realms"] = caps, realmState
	if err := oldConnection.Close(); err != nil {
		t.Fatal(err)
	}
	dials.allowMDS(next.Endpoint)
	target := cephmsgr.MDSTarget{FilesystemID: control.fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", next.Endpoint, next.Nonce)}
	connection, err := client.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal("open cap-send replacement MDS", err)
	}
	opening, stop := context.WithTimeout(ctx, 30*time.Second)
	s, calls, err := connection.ReconnectSession(opening, oldSession, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState},
		cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
	stop()
	if err != nil || len(calls) != 0 || s.Status().ReconnectedFrom != lost.Generation || s.Status().GlobalID != identity.GlobalID {
		t.Fatal("reconnect after cap-send failure", err)
	}
	agent := startCephFSCapAgentFrom(ctx, s, oldAgent)
	t.Cleanup(func() { facts["agent_after_reconnect"] = agent.snapshot() })
	facts["reconnected"] = s.Status()
	deadline = time.Now().Add(60 * time.Second)
	for {
		state := request("restart-status").RankStatus
		if state.Rank0 != nil && state.Rank0.GID == next.GID && state.Rank0.State == "up:active" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cap-send replacement MDS did not become active", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["adopted"] = request("restart-adopt").Restart
	var recorded nativeMDSCephFSTarget
	if err := readMDSFreshJSON(filepath.Join(control.dir, "mds-cephfs-data-target.json"), &recorded); err != nil ||
		recorded.Source != mdsFreshSource || recorded.Token != control.oracle.Token || recorded.FSID != control.fresh.FSID || recorded.FSCID != control.fresh.FSCID ||
		recorded.FSName != control.fresh.FSName || recorded.Adopted != 1 || recorded.MDS.GID != next.GID || recorded.MDS.Nonce != next.Nonce || recorded.MDS.State != "up:active" {
		t.Fatal("recorded data target differs from adopted cap-send incarnation", recorded, err)
	}
	if countIO() != beforeIO || client.Snapshot().GlobalID != identity.GlobalID || client.Snapshot().CephFSNonce != identity.CephFSNonce {
		t.Fatal("cap-send reconnect repeated object I/O or changed client identity")
	}
	facts["new_observed"] = request("observe", gid)
	facts["new_debug"] = request("debug-on")
	facts["new_cap_log_mark"] = request("cap-log-mark")
	if err := agent.recoverHeadFlushes([]*cephfsHeadFlush{uncertain, refused}); err != nil {
		t.Fatal("explicit recovery of the uncertain and refused UPDATEs", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		observed := request("cap-log", gid, nonce)
		facts["new_caps_log"] = observed.CapLog
		first, e1 := capSendUpdateBranch(observed.CapLog, identity.GlobalID, ino, uncertain.Transaction, follows)
		second, e2 := capSendUpdateBranch(observed.CapLog, identity.GlobalID, ino, refused.Transaction, follows)
		if e1 != nil || e2 != nil || first == "completed" || second == "completed" {
			t.Fatal("new MDS deduplicated or repeated a flush the old incarnation never applied", first, second, e1, e2, observed.CapLog)
		}
		if first == "applied" && second == "applied" {
			facts["new_branches"] = map[string]string{"uncertain": first, "refused": second}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new MDS did not apply both recovered UPDATEs", first, second, observed.CapLog)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["new_debug_restored"] = request("debug-off")
	if countIO() != beforeIO {
		t.Fatal("metadata-only cap-send recovery repeated object writeback")
	}
	facts["object_requests_after_recovery"] = countIO()
	return connection, s, agent, target, refused
}

func TestCephFSCapSendUpdateBranchClassification(t *testing.T) {
	const gid, ino = 123, 0x10000000017
	source := "2026-10-18T01:02:03.000+0000 7fffff012345 1 --  <== client.123 v2:172.19.0.9:0/77 9 ==== client_caps(update ino 0x10000000017 1 seq 0 issue_seq 0 tid 7 caps=pAsxLsXsxFsxcrwb dirty=Fw wanted=pAsxXsxFxwb follows 1 size 200000/4194304) ==== 300+0+0 (crc 0 0 0)"
	handler := "2026-10-18T01:02:03.001+0000 7fffff012345 7 mds.0.locker handle_client_caps on 0x10000000017 tid 7 follows 1 op update flags 0x1"
	flush := "2026-10-18T01:02:03.002+0000 7fffff012345 7 mds.0.locker  flush client.123 dirty Fw seq 0 on [inode 0x10000000017 [2,head] /d/f auth v12]"
	dedupe := "2026-10-18T01:02:03.002+0000 7fffff012345 7 mds.0.locker handle_client_caps already flushed tid 7 for client.123"
	for _, tc := range []struct {
		lines []string
		want  string
		err   bool
	}{
		{nil, "", false},
		{[]string{source, handler, flush}, "applied", false},
		{[]string{source, handler, dedupe}, "completed", false},
		{[]string{source, handler}, "handled", false},
		{[]string{source, handler, strings.ReplaceAll(flush, "client.123", "client.124")}, "handled", false},
		{[]string{source, handler, strings.ReplaceAll(flush, "0x10000000017", "0x10000000018")}, "handled", false},
		{[]string{source, handler, flush, source, handler, flush}, "", true},
		{[]string{source, strings.ReplaceAll(handler, "tid 7", "tid 8"), flush}, "", false},
	} {
		got, err := capSendUpdateBranch(&nativeMDSCephFSCompletionLog{Lines: tc.lines}, gid, ino, 7, 1)
		if got != tc.want || (err != nil) != tc.err {
			t.Fatal(tc.lines, got, err)
		}
	}
	drop := "2026-10-18T01:02:03.020+0000 7fffff012350 10 --2- [v2:172.19.0.10:6800/66] >> v2:172.19.0.9:0/77 conn(0x1 0x2 secure :-1 s=READY).handle_message blackhole client_caps(update ino 0x10000000017 1 seq 4 issue_seq 1 tid 7 caps=p dirty=Fw wanted=p follows 1 size 200000/0)"
	log := &nativeMDSCephFSCompletionLog{Lines: []string{drop}}
	if len(capSendIncomingDrop(log, ino, 7, 1)) != 1 || len(capSendIncomingDrop(log, ino, 8, 1)) != 0 || len(capSendIncomingDrop(log, ino, 7, 2)) != 0 {
		t.Fatal("incoming drop tuple matching differs")
	}
}
