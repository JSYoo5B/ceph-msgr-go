package integration_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Test-only consumer bookkeeping for interrupted OSD writeback (issue #10
// checkpoint 2). The product only transports object requests and cap
// messages; which writes count as completed, which metadata may be flushed and
// what recovery may resend are this consumer's decisions.

// Object write outcomes, from the product result alone.
const (
	// cephfsWriteCompleted: a reply with result 0 and the ONDISK flag.
	cephfsWriteCompleted = "completed"
	// cephfsWriteUncertain: admitted, but the bounded local wait ended
	// without a reply (OutcomeUnknownError). It may or may not have applied.
	cephfsWriteUncertain = "uncertain"
	// cephfsWriteFenced: the OSD answered -EBLOCKLISTED; known not applied.
	cephfsWriteFenced = "fenced"
	// cephfsWriteRefused: any other server result or a local refusal before
	// transmission; known not applied.
	cephfsWriteRefused = "refused"
)

const cephEBLOCKLISTED = -108

// cephfsWriteOutcome classifies one object write.
func cephfsWriteOutcome(e cephfsDataObjectIO, err error) string {
	var unknown *cephmsgr.OutcomeUnknownError
	switch {
	case err == nil && e.Code == 0 && e.ReplyFlags&cephmsgr.OSDReplyOnDisk != 0:
		return cephfsWriteCompleted
	case errors.As(err, &unknown):
		return cephfsWriteUncertain
	case e.OSDError == cephEBLOCKLISTED:
		return cephfsWriteFenced
	}
	return cephfsWriteRefused
}

// cephfsWritebackWrite is one object write of the consumer's writeback and
// the file ranges (offset, length) its bytes belong to.
type cephfsWritebackWrite struct {
	Phase   string
	Spans   [][2]uint64
	Outcome string
	IO      cephfsDataObjectIO
}

// cephfsWritebackLedger records every object write of one file in order.
type cephfsWritebackLedger struct {
	Writes []cephfsWritebackWrite
}

// committedEnd is the largest size a metadata flush may report for the file:
// the end of the file range from base that completed writes cover without a
// gap. A metadata flush never covers an uncertain, fenced or refused extent,
// so it never invents completed file data.
func (l *cephfsWritebackLedger) committedEnd(base uint64) uint64 {
	type span struct{ start, end uint64 }
	var done []span
	for _, w := range l.Writes {
		if w.Outcome != cephfsWriteCompleted {
			continue
		}
		for _, r := range w.Spans {
			if r[1] != 0 {
				done = append(done, span{r[0], r[0] + r[1]})
			}
		}
	}
	slices.SortFunc(done, func(a, b span) int { return compareUint64(a.start, b.start) })
	end := base
	for _, s := range done {
		if s.start > end {
			break
		}
		end = max(end, s.end)
	}
	return end
}

// count returns the writes of one outcome, optionally restricted to a phase.
func (l *cephfsWritebackLedger) count(outcome, phase string) int {
	n := 0
	for _, w := range l.Writes {
		if w.Outcome == outcome && (phase == "" || w.Phase == phase) {
			n++
		}
	}
	return n
}

// mayResend is the consumer's recovery rule for one earlier object write: a
// completed write is never repeated, and an uncertain one is never resent
// automatically. Only a write known not applied may be issued again, and
// only by an explicit decision.
func (l *cephfsWritebackLedger) mayResend(i int) error {
	if i < 0 || i >= len(l.Writes) {
		return errors.New("no such object write")
	}
	switch l.Writes[i].Outcome {
	case cephfsWriteCompleted:
		return errors.New("the object write completed; recovery must not repeat it")
	case cephfsWriteUncertain:
		return errors.New("the object write has an unknown outcome; it is never resent automatically")
	}
	return nil
}

// writeback issues the object extents of one file range with the given local
// wait and records each outcome. It stops at the first write that did not
// complete, as the consumer then knows its writeback is interrupted.
func (l *cephfsWritebackLedger) writeback(d *cephfsDataIO, phase string, ino uint64, layout cephfsStriping, offset uint64, data []byte,
	snapc cephmsgr.OSDSnapContext, mtime cephmsgr.MDSTime, trunc cephfsTrunc, wait time.Duration) []string {
	d.wait = wait
	defer func() { d.wait = 0 }()
	var outcomes []string
	for _, ext := range layout.fileToExtents(offset, uint64(len(data))) {
		buf := make([]byte, 0, ext.Length)
		var spans [][2]uint64
		for _, b := range ext.Buffers {
			buf = append(buf, data[b.Offset:b.Offset+b.Length]...)
			spans = append(spans, [2]uint64{offset + b.Offset, b.Length})
		}
		size, seq := trunc.wire(layout, ext.ObjectNo)
		reply, e, err := d.request(cephfsObjectName(ino, ext.ObjectNo), ext.ObjectNo, "write", cephmsgr.OSDRequest{
			ModificationTime: cephmsgr.OSDTime{Seconds: mtime.Seconds, Nanoseconds: mtime.Nanoseconds}, SnapContext: snapc,
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWrite, Offset: ext.Offset, Length: ext.Length, Data: buf, TruncateSize: size, TruncateSequence: seq}}})
		outcome := cephfsWriteOutcome(e, err)
		if outcome == cephfsWriteCompleted && (len(reply.Operations) != 1 || reply.Operations[0].Length != ext.Length) {
			// A trimmed extent is not the data the consumer wrote.
			outcome = cephfsWriteRefused
		}
		l.Writes = append(l.Writes, cephfsWritebackWrite{Phase: phase, Spans: spans, Outcome: outcome, IO: e})
		outcomes = append(outcomes, outcome)
		if outcome != cephfsWriteCompleted {
			break
		}
	}
	return outcomes
}

// restartAcrossInterruptedWriteback fails the owned rank-0 MDS while the
// consumer still holds an uncertain object write, then reconnects the same
// client to the new incarnation. Nothing is resent: the completed writes are
// not repeated, the uncertain one is not retried, and no flush is chosen,
// because the consumer's only dirty data has no completed writeback.
func restartAcrossInterruptedWriteback(t *testing.T, ctx context.Context, control *mdsCephFSDataControl, client *cephmsgr.Client, dials *recoveryDials,
	oldConnection *cephmsgr.MDSConnection, oldSession *cephmsgr.MDSSession, oldAgent *cephfsCapAgent, realms map[uint64]cephmsgr.MDSSnapRealm,
	metadata map[string]string, io *cephfsDataIO, ino uint64, facts map[string]any,
) (*cephmsgr.MDSConnection, *cephmsgr.MDSSession, *cephfsCapAgent, cephmsgr.MDSTarget) {
	t.Helper()
	request := func(action string, args ...string) nativeMDSCephFSDataReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("writeback restart control", action, err)
		}
		return r
	}
	countIO := func() int {
		io.mu.Lock()
		defer io.mu.Unlock()
		return len(io.log)
	}
	identity := client.Snapshot()
	armed := request("restart-arm").Restart
	beforeIO := countIO()
	facts["armed"], facts["object_requests_before_restart"] = armed, beforeIO
	request("restart-fire")
	select {
	case <-oldSession.Done():
	case <-time.After(45 * time.Second):
		t.Fatal("writeback session did not end after MDS restart")
	}
	lost := oldSession.Status()
	facts["lost"], facts["resume_error"] = lost, describeRecoveryError(oldSession.Err())
	if lost.State != cephmsgr.MDSSessionLost || !describeRecoveryError(oldSession.Err()).SessionReset {
		t.Fatal("writeback session did not end with the restarted MDS's RESET", lost)
	}
	select {
	case <-oldAgent.done:
	case <-time.After(10 * time.Second):
		t.Fatal("old writeback agent did not stop")
	}
	oldSnapshot := oldAgent.snapshot()
	facts["agent_before_reconnect"] = oldSnapshot
	if len(oldSnapshot.Errors) != 0 || oldSnapshot.Pending != 0 {
		t.Fatal("old writeback agent ended with errors or unapplied pushes", oldSnapshot.Errors, oldSnapshot.Pending)
	}
	if d, ok := oldAgent.dirtyState(ino); !ok || d.Dirty != 0 || d.Flushing != 0 || d.Buffered {
		t.Fatal("the consumer carries dirty or flushing metadata into the restart", d)
	}
	var next *nativeMDSCephFSDaemon
	var timeline []*nativeMDSCephFSRankStatus
	deadline := time.Now().Add(60 * time.Second)
	for next == nil {
		s := request("restart-status").RankStatus
		timeline = append(timeline, s)
		if s.Rank0 != nil && s.Rank0.GID != armed.Armed.GID && s.Rank0.Name == armed.Armed.Name && s.Rank0.State != "up:replay" {
			next = s.Rank0
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned MDS did not reach writeback reconnect", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["status_timeline"] = timeline
	if next.State != "up:reconnect" || next.Nonce == 0 || next.Nonce == armed.Armed.Nonce || next.Incarnation == armed.Armed.Incarnation {
		t.Fatal("writeback reconnect target is not a new owned incarnation", next)
	}
	reconnecting := time.Now()
	caps, realmState, err := oldAgent.reconnectState(realms)
	if err != nil || !slices.ContainsFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == ino }) {
		t.Fatal("writeback reconnect state", caps, err)
	}
	facts["reconnect_caps"], facts["reconnect_realms"] = caps, realmState
	if err := oldConnection.Close(); err != nil {
		t.Fatal(err)
	}
	dials.allowMDS(next.Endpoint)
	target := cephmsgr.MDSTarget{FilesystemID: control.fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", next.Endpoint, next.Nonce)}
	connection, err := client.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal("open writeback replacement MDS", err)
	}
	opening, stop := context.WithTimeout(ctx, 30*time.Second)
	s, calls, err := connection.ReconnectSession(opening, oldSession, cephmsgr.MDSReconnectState{Caps: caps, Realms: realmState},
		cephmsgr.MDSSessionOptions{Metadata: metadata, RenewInterval: 2 * time.Second, MaxEvents: 1024})
	stop()
	if err != nil || len(calls) != 0 || s.Status().ReconnectedFrom != lost.Generation || s.Status().GlobalID != identity.GlobalID {
		t.Fatal("reconnect after interrupted writeback", err)
	}
	agent := startCephFSCapAgentFrom(ctx, s, oldAgent)
	facts["reconnected"] = s.Status()
	deadline = time.Now().Add(60 * time.Second)
	for {
		state := request("restart-status").RankStatus
		if state.Rank0 != nil && state.Rank0.GID == next.GID && state.Rank0.State == "up:active" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writeback replacement MDS did not become active", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	facts["adopted"] = request("restart-adopt").Restart
	var recorded nativeMDSCephFSTarget
	if err := readMDSFreshJSON(filepath.Join(control.dir, "mds-cephfs-data-target.json"), &recorded); err != nil ||
		recorded.Source != mdsFreshSource || recorded.Token != control.oracle.Token || recorded.FSID != control.fresh.FSID || recorded.FSCID != control.fresh.FSCID ||
		recorded.FSName != control.fresh.FSName || recorded.Adopted != 1 || recorded.MDS.GID != next.GID || recorded.MDS.Nonce != next.Nonce || recorded.MDS.State != "up:active" {
		t.Fatal("recorded data target differs from adopted writeback incarnation", recorded, err)
	}
	if countIO() != beforeIO || client.Snapshot().GlobalID != identity.GlobalID || client.Snapshot().CephFSNonce != identity.CephFSNonce {
		t.Fatal("writeback reconnect repeated object I/O or changed client identity")
	}
	if tids := agent.dirtyFlushesSince(ino, reconnecting); len(tids) != 0 {
		t.Fatal("reconnect chose a metadata flush without completed writeback", tids)
	}
	facts["object_requests_after_reconnect"] = countIO()
	return connection, s, agent, target
}

// dirtyFlushesSince returns the flush TIDs of cap updates that flushed dirty
// metadata of one inode after since.
func (a *cephfsCapAgent) dirtyFlushesSince(ino uint64, since time.Time) []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var tids []uint64
	if d := a.dirty[ino]; d != nil {
		for _, ev := range d.Events {
			if ev.Kind == "cap-update" && ev.TID != 0 && !ev.At.Before(since) {
				tids = append(tids, ev.TID)
			}
		}
	}
	return tids
}

func TestCephFSWritebackLedgerClassifiesAndBoundsMetadata(t *testing.T) {
	ondisk := cephfsDataObjectIO{ReplyFlags: cephmsgr.OSDReplyOnDisk}
	for _, tc := range []struct {
		e    cephfsDataObjectIO
		err  error
		want string
	}{
		{ondisk, nil, cephfsWriteCompleted},
		// A success without ONDISK is not a completed mutation.
		{cephfsDataObjectIO{}, nil, cephfsWriteRefused},
		{cephfsDataObjectIO{}, fmt.Errorf("write: %w", &cephmsgr.OutcomeUnknownError{Cause: context.DeadlineExceeded}), cephfsWriteUncertain},
		{cephfsDataObjectIO{OSDError: cephEBLOCKLISTED}, &cephmsgr.OSDError{Code: cephEBLOCKLISTED}, cephfsWriteFenced},
		{cephfsDataObjectIO{OSDError: -28}, &cephmsgr.OSDError{Code: -28}, cephfsWriteRefused},
		{cephfsDataObjectIO{}, errors.New("placement failed before transmission"), cephfsWriteRefused},
	} {
		if got := cephfsWriteOutcome(tc.e, tc.err); got != tc.want {
			t.Fatal(tc, got)
		}
	}
	l := &cephfsWritebackLedger{Writes: []cephfsWritebackWrite{
		// Object 1 then object 0 of the acceptance layout: interleaved spans.
		{Phase: "a", Spans: [][2]uint64{{65536, 65536}, {196608, 3392}}, Outcome: cephfsWriteCompleted},
		{Phase: "a", Spans: [][2]uint64{{0, 65536}, {131072, 65536}}, Outcome: cephfsWriteCompleted},
		{Phase: "b", Spans: [][2]uint64{{200000, 20000}}, Outcome: cephfsWriteUncertain},
		{Phase: "c", Spans: [][2]uint64{{220000, 20000}}, Outcome: cephfsWriteFenced},
	}}
	if end := (&cephfsWritebackLedger{Writes: l.Writes[:1]}).committedEnd(0); end != 0 {
		t.Fatal("object 1 alone does not cover the file from 0", end)
	}
	if end := l.committedEnd(0); end != 200000 {
		t.Fatal("metadata may cover only contiguous completed data", end)
	}
	// A completed write after a gap does not extend the committed end.
	l.Writes = append(l.Writes, cephfsWritebackWrite{Phase: "d", Spans: [][2]uint64{{240000, 10}}, Outcome: cephfsWriteCompleted})
	if end := l.committedEnd(0); end != 200000 {
		t.Fatal("a completed write beyond an uncertain extent extended the size", end)
	}
	if l.committedEnd(300000) != 300000 {
		t.Fatal("the committed end fell below the base size")
	}
	if l.count(cephfsWriteCompleted, "a") != 2 || l.count(cephfsWriteUncertain, "") != 1 || l.count(cephfsWriteFenced, "c") != 1 {
		t.Fatal("ledger counts differ")
	}
	for i, want := range []bool{false, false, false, true, false} {
		if err := l.mayResend(i); (err == nil) != want {
			t.Fatal("resend rule differs", i, err)
		}
	}
	if l.mayResend(5) == nil {
		t.Fatal("resend of an unknown write admitted")
	}
}
