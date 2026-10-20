package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// reconnectPreparedCapSnaps is a test consumer decision: data writeback has
// completed. One FLUSHSNAP is journaled by the old MDS while its ACK is
// deliberately dropped; the other remains unsent. After Lost the caller
// explicitly resubmits the first, proves completed-flush deduplication, and
// first-sends the second, without repeating either data writeback.
func reconnectPreparedCapSnaps(t *testing.T, ctx context.Context, control *mdsCephFSDataControl,
	client *cephmsgr.Client, dials *recoveryDials, oldConnection *cephmsgr.MDSConnection,
	oldSession *cephmsgr.MDSSession, oldAgent *cephfsCapAgent, realms map[uint64]cephmsgr.MDSSnapRealm,
	metadata map[string]string, io *cephfsDataIO, ino, replaySnapshot uint64, facts map[string]any,
) (*cephmsgr.MDSConnection, *cephmsgr.MDSSession, *cephfsCapAgent, cephmsgr.MDSTarget) {
	t.Helper()
	request := func(action string, args ...string) nativeMDSCephFSDataReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("cap-snap restart control", action, err)
		}
		return r
	}
	countIO := func() int {
		io.mu.Lock()
		defer io.mu.Unlock()
		return len(io.log)
	}
	before := oldAgent.snapshot()
	beforeIO := countIO()
	identity := client.Snapshot()
	if len(before.CapSnaps) != 2 || before.CapSnaps[0].Ino != ino || before.CapSnaps[1].Ino != ino {
		t.Fatal("mixed cap-snap recovery requires two prepared generations", before.CapSnaps)
	}
	first := before.CapSnaps[0]
	key := cephfsFlushSnapKey{ino, first.Context.Sequence}
	facts["debug"] = request("debug-on")
	facts["cap_log_mark"] = request("cap-log-mark")
	facts["journal_pause"] = request("log-pause")
	if err := oldAgent.sendPreparedCapSnap(key, oldSession.SendCaps); err != nil {
		t.Fatal("admit first FLUSHSNAP on old session", err)
	}
	facts["old_admitted"] = oldAgent.snapshot().CapSnaps
	handler := fmt.Sprintf("handle_client_caps on 0x%x tid %d follows %x op flushsnap", ino, first.Transaction, first.Context.Sequence)
	journal := fmt.Sprintf("flushsnap snap %x client.%d", replaySnapshot, identity.GlobalID)
	deadline := time.Now().Add(15 * time.Second)
	for {
		observed := request("cap-log", strconv.FormatUint(identity.GlobalID, 10), strconv.FormatUint(uint64(identity.CephFSNonce), 10))
		facts["old_caps_log"] = observed.CapLog
		if len(observed.CapLog.find(handler)) != 0 && len(observed.CapLog.find(journal, fmt.Sprintf("on [inode 0x%x", ino))) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old MDS did not prove the selected FLUSHSNAP's owned journal path", handler, journal, observed.CapLog)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Close the ACK path before releasing the journal. The barrier confirms
	// durability; the attributed native log proves this exact ACK was dropped.
	facts["ack_blackhole"] = request("blackhole-on")
	facts["journal_resume"] = request("log-resume")
	facts["journal_barrier"] = request("flush-journal")
	deadline = time.Now().Add(15 * time.Second)
	for {
		observed := request("cap-log", strconv.FormatUint(identity.GlobalID, 10), strconv.FormatUint(uint64(identity.CephFSNonce), 10))
		facts["old_caps_log"] = observed.CapLog
		if len(observed.CapLog.find("client blackhole", "client_caps(flushsnap_ack", fmt.Sprintf("ino 0x%x ", ino),
			fmt.Sprintf("tid %d ", first.Transaction), fmt.Sprintf("follows %x ", first.Context.Sequence))) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old MDS did not prove the selected durable FLUSHSNAP ACK was dropped", observed.CapLog)
		}
		time.Sleep(100 * time.Millisecond)
	}
	armed := request("restart-arm").Restart
	facts["prepared"], facts["armed"], facts["object_requests_before_restart"] = before.CapSnaps, armed, beforeIO
	fire := time.Now()
	facts["fire_started"] = fire
	request("restart-fire")
	select {
	case <-oldSession.Done():
	case <-time.After(45 * time.Second):
		t.Fatal("cap-snap session did not end after MDS restart")
	}
	lost := oldSession.Status()
	facts["lost"] = lost
	facts["resume_error"] = describeRecoveryError(oldSession.Err())
	if lost.State != cephmsgr.MDSSessionLost || !describeRecoveryError(oldSession.Err()).SessionReset {
		t.Fatal("cap-snap session did not end with the restarted MDS's RESET", lost)
	}
	select {
	case <-oldAgent.done:
	case <-time.After(10 * time.Second):
		t.Fatal("old cap-snap agent did not stop")
	}
	oldSnapshot := oldAgent.snapshot()
	facts["agent_before_reconnect"] = oldSnapshot
	if len(oldSnapshot.Errors) != 0 || oldSnapshot.Pending != 0 {
		t.Fatal("old cap-snap agent ended with errors or unapplied pushes", oldSnapshot.Errors, oldSnapshot.Pending)
	}
	oldAgent.mu.Lock()
	pending, pendingOK := oldAgent.flushSnaps[key]
	cap := oldAgent.caps[ino]
	oldAgent.mu.Unlock()
	facts["old_pending_ack"] = pending
	if !pendingOK || pending.TID != first.Transaction || pending.Dirty != first.Caps.Dirty || cap == nil || cap.Released ||
		oldSnapshot.CapSnaps[0].Sent.IsZero() || oldSnapshot.CapSnaps[0].SentGeneration != lost.Generation || !oldSnapshot.CapSnaps[0].Acked.IsZero() ||
		!oldSnapshot.CapSnaps[1].Sent.IsZero() || !oldSnapshot.CapSnaps[1].Acked.IsZero() ||
		slices.ContainsFunc(oldSnapshot.SnapAcks, func(a cephfsFlushSnapAck) bool { return a.Transaction == first.Transaction }) {
		t.Fatal("old session accepted an ACK or lost the exact pending identity", oldSnapshot, pending)
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
			t.Fatal("owned MDS did not reach cap-snap reconnect", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["status_timeline"] = timeline
	if next.State != "up:reconnect" || next.Nonce == 0 || next.Nonce == armed.Armed.Nonce || next.Incarnation == armed.Armed.Incarnation {
		t.Fatal("cap-snap reconnect target is not a new owned incarnation", next)
	}
	caps, realmState, err := oldAgent.reconnectState(realms)
	if err != nil {
		t.Fatal("cap-snap reconnect state", err)
	}
	i := slices.IndexFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == ino })
	if i < 0 || len(before.CapSnaps) != 2 || caps[i].SnapFollows != before.CapSnaps[0].Context.Sequence {
		t.Fatal("reconnect omitted the first pending cap-snap follows", caps, before.CapSnaps)
	}
	facts["reconnect_caps"], facts["reconnect_realms"] = caps, realmState
	if err := oldConnection.Close(); err != nil {
		t.Fatal(err)
	}
	dials.allowMDS(next.Endpoint)
	target := cephmsgr.MDSTarget{FilesystemID: control.fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", next.Endpoint, next.Nonce)}
	connection, err := client.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal("open cap-snap replacement MDS", err)
	}
	opening, stop := context.WithTimeout(ctx, 30*time.Second)
	s, calls, err := connection.ReconnectSession(opening, oldSession, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState},
		cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
	stop()
	if err != nil || len(calls) != 0 || s.Status().ReconnectedFrom != lost.Generation || s.Status().GlobalID != identity.GlobalID {
		t.Fatal("reconnect prepared cap-snaps", err)
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
			t.Fatal("cap-snap replacement MDS did not become active", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	adopted := request("restart-adopt").Restart
	facts["adopted"] = adopted
	var recorded nativeMDSCephFSTarget
	if err := readMDSFreshJSON(filepath.Join(control.dir, "mds-cephfs-data-target.json"), &recorded); err != nil ||
		recorded.Source != mdsFreshSource || recorded.Token != control.oracle.Token || recorded.FSID != control.fresh.FSID || recorded.FSCID != control.fresh.FSCID ||
		recorded.FSName != control.fresh.FSName || recorded.Adopted != 1 || recorded.MDS.GID != next.GID || recorded.MDS.Nonce != next.Nonce || recorded.MDS.State != "up:active" {
		t.Fatal("recorded data target differs from adopted cap-snap incarnation", recorded, err)
	}
	if countIO() != beforeIO || client.Snapshot().GlobalID != identity.GlobalID || client.Snapshot().CephFSNonce != identity.CephFSNonce {
		t.Fatal("cap-snap reconnect repeated object I/O or changed client identity")
	}
	facts["new_observed"] = request("observe", strconv.FormatUint(identity.GlobalID, 10))
	facts["new_debug"] = request("debug-on")
	facts["new_cap_log_mark"] = request("cap-log-mark")
	if err := agent.resubmitCapSnaps([]uint64{first.Transaction}); err != nil {
		t.Fatal("explicit same-TID FLUSHSNAP resubmission after reconnect", err)
	}
	facts["explicit_resubmitted_tids"] = []uint64{first.Transaction}
	deadline = time.Now().Add(15 * time.Second)
	for {
		observed := request("cap-log", strconv.FormatUint(identity.GlobalID, 10), strconv.FormatUint(uint64(identity.CephFSNonce), 10))
		facts["new_caps_log"] = observed.CapLog
		if len(observed.CapLog.find(handler)) != 0 &&
			len(observed.CapLog.find(fmt.Sprintf("handle_client_caps already flushed tid %d for client.%d", first.Transaction, identity.GlobalID))) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new MDS did not prove the original TID's completed-flush branch", observed.CapLog)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["new_debug_restored"] = request("debug-off")
	if err := agent.sendPreparedCapSnaps(ino); err != nil {
		t.Fatal("explicit first send of the remaining FLUSHSNAP after reconnect", err)
	}
	facts["object_requests_after_reconnect_and_flush"] = countIO()
	if countIO() != beforeIO {
		t.Fatal("metadata-only cap-snap flush repeated object writeback")
	}
	return connection, s, agent, target
}
