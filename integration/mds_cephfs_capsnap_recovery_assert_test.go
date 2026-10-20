package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

func validateMixedCapSnapRecovery(flushed []cephfsCapSnap, status cephmsgr.MDSSessionStatus) error {
	if len(flushed) != 2 || status.Generation == 0 || status.ReconnectedFrom == 0 || status.Generation <= status.ReconnectedFrom {
		return errors.New("mixed cap-snap recovery requires two generations and a reconnected session")
	}
	first, second := flushed[0], flushed[1]
	if len(first.Resubmissions) != 1 {
		return errors.New("first cap-snap requires exactly one explicit resubmission")
	}
	attempt := first.Resubmissions[0]
	// Sent records SendCaps returning after queue admission, not a peer
	// receipt. A fast ACK can arrive while the sender is still returning.
	// Started precedes the send call and is the causal lower bound instead.
	if attempt.Generation != status.Generation || attempt.ReconnectedFrom != status.ReconnectedFrom || attempt.ReconnectedFrom != first.SentGeneration ||
		attempt.Started.IsZero() || attempt.Sent.IsZero() || attempt.Sent.Before(attempt.Started) || attempt.Error != "" ||
		first.Acked.IsZero() || first.Acked.Before(attempt.Started) {
		return errors.New("original-TID cap-snap resubmission identity or ACK evidence differs")
	}
	if len(second.Resubmissions) != 0 || second.SentGeneration != status.Generation || second.Sent.IsZero() || second.Acked.IsZero() {
		return errors.New("second cap-snap must be first-sent and acknowledged in the new generation")
	}
	return nil
}

func fastACKMixedCapSnapRecovery(t *testing.T) ([]cephfsCapSnap, cephmsgr.MDSSessionStatus) {
	t.Helper()
	a, writes := preparedCapSnapAgent(t)
	if err := a.prepareCapSnaps(17); err != nil {
		t.Fatal(err)
	}
	firstKey := cephfsFlushSnapKey{17, 2}
	if err := a.sendPreparedCapSnap(firstKey, func(context.Context, cephmsgr.MDSCaps, uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	a.capSnaps[firstKey].SentGeneration = 11
	originalTID := a.capSnaps[firstKey].Transaction
	b := newCephFSCapAgentFrom(context.Background(), nil, a)
	status := capSnapReplayStatus()
	var ack cephmsgr.MDSCaps
	var ackTID uint64
	var received time.Time
	// The transport can publish the ACK event before SendCaps returns. The
	// agent mutex delays consumption, but does not change event.Received.
	sender := func(ctx context.Context, caps cephmsgr.MDSCaps, tid uint64) error {
		ack = cephmsgr.MDSCaps{Ino: caps.Ino, SnapFollows: caps.SnapFollows, Dirty: caps.Dirty}
		ackTID, received = tid, time.Now()
		// Windows may sample both events within one clock tick. Observe a
		// strictly later tick before returning rather than assuming clock
		// resolution or sleeping for an arbitrary fixed duration.
		waiting, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for !time.Now().After(received) {
			select {
			case <-waiting.Done():
				return errors.Join(errors.New("clock did not advance after synthetic ACK before sender return"), waiting.Err())
			case <-tick.C:
			}
		}
		return nil
	}
	if err := b.resubmitCapSnapsWith([]uint64{originalTID}, status, sender); err != nil {
		t.Fatal(err)
	}
	if ackTID != originalTID || !b.consumeFlushSnapLocked(&ack, ackTID, received) {
		t.Fatal("fast ACK lost the original pending identity")
	}
	if err := b.sendPreparedCapSnapsWith(17, sender); err != nil {
		t.Fatal(err)
	}
	second := b.capSnaps[cephfsFlushSnapKey{17, 3}]
	second.SentGeneration = status.Generation // Synthetic new-session seam.
	if ackTID != second.Transaction || !b.consumeFlushSnapLocked(&ack, ackTID, received) || len(*writes) != 2 {
		t.Fatal("first send of B lost its identity or repeated data writeback")
	}
	flushed := b.snapshot().CapSnaps
	attempt := flushed[0].Resubmissions[0]
	if flushed[0].Acked.Before(attempt.Started) || !flushed[0].Acked.Before(attempt.Sent) {
		t.Fatal("sender did not reproduce Started <= ACKReceived < admission return", attempt, flushed[0].Acked)
	}
	return flushed, status
}

func TestCephFSCapSnapRecoveryAcceptsACKBeforeAdmissionReturn(t *testing.T) {
	flushed, status := fastACKMixedCapSnapRecovery(t)
	if err := validateMixedCapSnapRecovery(flushed, status); err != nil {
		t.Fatal("valid fast ACK was rejected", err)
	}
	// Equality at the causal lower bound is allowed for coarser clocks.
	flushed[0].Acked = flushed[0].Resubmissions[0].Started
	if err := validateMixedCapSnapRecovery(flushed, status); err != nil {
		t.Fatal("ACK at send start was rejected", err)
	}
}

func TestCephFSCapSnapRecoveryRejectsInvalidACKAndIdentity(t *testing.T) {
	flushed, status := fastACKMixedCapSnapRecovery(t)
	for name, edit := range map[string]func([]cephfsCapSnap, *cephmsgr.MDSSessionStatus){
		"ACK before send": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) {
			s[0].Acked = s[0].Resubmissions[0].Started.Add(-time.Nanosecond)
		},
		"missing ACK":        func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[0].Acked = time.Time{} },
		"missing send start": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[0].Resubmissions[0].Started = time.Time{} },
		"missing admission":  func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[0].Resubmissions[0].Sent = time.Time{} },
		"admission before start": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) {
			s[0].Resubmissions[0].Sent = s[0].Resubmissions[0].Started.Add(-time.Nanosecond)
		},
		"wrong replay generation": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[0].Resubmissions[0].Generation++ },
		"wrong lost generation":   func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[0].SentGeneration++ },
		"wrong predecessor":       func(_ []cephfsCapSnap, s *cephmsgr.MDSSessionStatus) { s.ReconnectedFrom-- },
		"resubmission failed": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) {
			s[0].Resubmissions[0].Error = "uncertain admission"
		},
		"resubmitted twice": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) {
			s[0].Resubmissions = append(s[0].Resubmissions, s[0].Resubmissions[0])
		},
		"B replayed": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) {
			s[1].Resubmissions = append(s[1].Resubmissions, s[0].Resubmissions[0])
		},
		"B sent on old generation": func(s []cephfsCapSnap, _ *cephmsgr.MDSSessionStatus) { s[1].SentGeneration = s[0].SentGeneration },
	} {
		t.Run(name, func(t *testing.T) {
			bad := []cephfsCapSnap{cloneCephFSCapSnap(&flushed[0]), cloneCephFSCapSnap(&flushed[1])}
			badStatus := status
			edit(bad, &badStatus)
			if err := validateMixedCapSnapRecovery(bad, badStatus); err == nil {
				t.Fatal("invalid ACK or mixed recovery identity accepted")
			}
		})
	}
	for _, short := range [][]cephfsCapSnap{nil, flushed[:1]} {
		if err := validateMixedCapSnapRecovery(short, status); err == nil {
			t.Fatal("incomplete mixed recovery accepted")
		}
	}
}
