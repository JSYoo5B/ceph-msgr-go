package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func electionTestExpected() electionExpectation {
	return electionExpectation{FSID: "02000000-0000-0000-0000-000000000000", Epoch: 4,
		Members: []electionMember{{"a", 0}, {"b", 1}, {"c", 2}, {"map-wait-spare", 3}},
		Quorum:  []string{"a", "b", "c"}, Label: "added"}
}
func electionTestJSON(epoch uint64) map[string]any {
	want := electionTestExpected()
	return map[string]any{"election_epoch": epoch, "quorum": []uint32{0, 1, 2},
		"quorum_names": []string{"a", "b", "c"}, "quorum_leader_name": "a",
		"monmap": map[string]any{"fsid": want.FSID, "epoch": want.Epoch, "mons": want.Members}}
}
func electionEncode(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type electionFrame struct {
	data []byte
	err  error
	at   time.Duration
}
type fakeElection struct {
	t      *testing.T
	frames []electionFrame
	index  int
	seen   []*electionObservation
}

func (f *fakeElection) backend() electionBackend {
	return electionBackend{
		query: func(ctx context.Context) ([]byte, error) {
			deadline, present := ctx.Deadline()
			if !present || time.Until(deadline) > 5*time.Second || time.Until(deadline) <= 0 {
				f.t.Fatal("native query lacks a positive per-call bound", deadline)
			}
			return f.frames[f.index].data, f.frames[f.index].err
		},
		now: func() time.Time { return time.Unix(1700000000, 0).Add(f.frames[f.index].at) },
		observe: func(observed *electionObservation, err error) error {
			f.seen = append(f.seen, observed)
			return nil
		},
		wait: func(ctx context.Context) error {
			f.index++
			if f.index >= len(f.frames) {
				return errors.New("observations exhausted without stability")
			}
			return ctx.Err()
		},
	}
}

func TestSettledElectionAsyncConvergenceAndTwoRoundsResetWindow(t *testing.T) {
	old := electionTestJSON(10)
	old["monmap"].(map[string]any)["epoch"] = uint32(3)
	old["monmap"].(map[string]any)["mons"] = electionTestExpected().Members[:3]
	f := fakeElection{t: t, frames: []electionFrame{
		{data: electionEncode(t, old)},
		{data: electionEncode(t, electionTestJSON(12)), at: time.Second},
		{data: electionEncode(t, electionTestJSON(12)), at: 2 * time.Second},
		{data: electionEncode(t, electionTestJSON(14)), at: 2900 * time.Millisecond},
		{data: electionEncode(t, electionTestJSON(14)), at: 4 * time.Second},
		{data: electionEncode(t, electionTestJSON(14)), at: 5 * time.Second},
	}}
	receipt, err := waitSettledElection(context.Background(), electionTestExpected(), f.backend())
	if err != nil || f.index != 5 || receipt.First.ElectionEpoch != 14 || receipt.Second.ElectionEpoch != 14 || receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt) != 2100*time.Millisecond {
		t.Fatal("second election round did not reset stability", receipt, f.index, err)
	}
	if len(f.seen) != 6 || !reflect.DeepEqual(receipt.First.Members, electionTestExpected().Members) {
		t.Fatal("convergence history or membership lost", receipt, f.seen)
	}
}

func TestSettledElectionTwoEqualAnswersBeforeTwoSecondsCannotPass(t *testing.T) {
	f := fakeElection{t: t, frames: []electionFrame{
		{data: electionEncode(t, electionTestJSON(12))},
		{data: electionEncode(t, electionTestJSON(12)), at: time.Second},
	}}
	if _, err := waitSettledElection(context.Background(), electionTestExpected(), f.backend()); err == nil {
		t.Fatal("two quick identical reads incorrectly established stability")
	}
}

func TestSettledElectionTimedOutReadClearsPriorWindow(t *testing.T) {
	f := fakeElection{t: t, frames: []electionFrame{
		{data: electionEncode(t, electionTestJSON(12))},
		{err: errElectionObservationPending, at: time.Second},
		{data: electionEncode(t, electionTestJSON(12)), at: 3 * time.Second},
		{data: electionEncode(t, electionTestJSON(12)), at: 5 * time.Second},
	}}
	receipt, err := waitSettledElection(context.Background(), electionTestExpected(), f.backend())
	if err != nil || f.index != 3 || !receipt.First.ObservedAt.Equal(time.Unix(1700000003, 0)) {
		t.Fatal("timeout incorrectly retained old stability window", receipt, err)
	}
}

func TestSettledElectionIncompleteSchemaAndWrongIdentityFailImmediately(t *testing.T) {
	for _, fault := range []string{"missing election", "missing quorum", "missing rank", "wrong fsid", "newer map", "wrong member", "duplicate rank", "duplicate quorum", "wrong rank", "outside leader"} {
		value := electionTestJSON(12)
		monmap := value["monmap"].(map[string]any)
		switch fault {
		case "missing election":
			delete(value, "election_epoch")
		case "missing quorum":
			delete(value, "quorum")
		case "missing rank":
			monmap["mons"] = []map[string]any{{"name": "a"}}
		case "wrong fsid":
			monmap["fsid"] = "another-cluster"
		case "newer map":
			monmap["epoch"] = uint32(5)
		case "wrong member":
			members := electionTestExpected().Members
			members[3].Name = "other"
			monmap["mons"] = members
		case "duplicate rank":
			members := electionTestExpected().Members
			members[3].Rank = 2
			monmap["mons"] = members
		case "duplicate quorum":
			value["quorum_names"] = []string{"a", "a", "c"}
		case "wrong rank":
			value["quorum"] = []uint32{1, 0, 2}
		case "outside leader":
			value["quorum_leader_name"] = "map-wait-spare"
		}
		f := fakeElection{t: t, frames: []electionFrame{{data: electionEncode(t, value)}}}
		if _, err := waitSettledElection(context.Background(), electionTestExpected(), f.backend()); err == nil || f.index != 0 {
			t.Fatal("malformed/identity observation retried or passed", fault, err, f.index)
		}
	}
}

func TestSettledElectionEmptyOrPartialQuorumCannotEstablishReadiness(t *testing.T) {
	for _, empty := range []bool{true, false} {
		pending := electionTestJSON(12)
		pending["quorum"], pending["quorum_names"], pending["quorum_leader_name"] = []uint32{}, []string{}, ""
		if !empty {
			pending["quorum"], pending["quorum_names"], pending["quorum_leader_name"] = []uint32{0, 1}, []string{"a", "b"}, "a"
		}
		f := fakeElection{t: t, frames: []electionFrame{
			{data: electionEncode(t, pending)},
			{data: electionEncode(t, electionTestJSON(12)), at: 3 * time.Second},
			{data: electionEncode(t, electionTestJSON(12)), at: 5 * time.Second},
		}}
		receipt, err := waitSettledElection(context.Background(), electionTestExpected(), f.backend())
		if err != nil || f.index != 2 || !receipt.First.ObservedAt.Equal(time.Unix(1700000003, 0)) {
			t.Fatal("partial quorum counted as stable expected quorum", receipt, err)
		}
	}
}

func TestSettledElectionCancellationAndPermanentQueryFailureDoNotPublish(t *testing.T) {
	for _, fault := range []string{"cancelled before query", "cancelled during query", "permanent native error", "record failure"} {
		ctx, cancel := context.WithCancel(context.Background())
		f := fakeElection{t: t, frames: []electionFrame{{data: electionEncode(t, electionTestJSON(12))}}}
		backend := f.backend()
		switch fault {
		case "cancelled before query":
			cancel()
		case "cancelled during query":
			backend.query = func(context.Context) ([]byte, error) { cancel(); return f.frames[0].data, nil }
		case "permanent native error":
			f.frames[0].err = errors.New("native authentication rejected")
		case "record failure":
			backend.observe = func(*electionObservation, error) error { return errors.New("receipt write failed") }
		}
		receipt, err := waitSettledElection(ctx, electionTestExpected(), backend)
		cancel()
		if err == nil || receipt.SchemaVersion != 0 || f.index != 0 {
			t.Fatal("failed observation published readiness or retried", fault, receipt, err)
		}
	}
}

func TestSettledElectionParentDeadlineAndObservationCountAreBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	backend := electionBackend{query: func(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }}
	if _, err := waitSettledElection(ctx, electionTestExpected(), backend); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("caller deadline lost", err)
	}
	calls := 0
	backend = electionBackend{
		query: func(context.Context) ([]byte, error) { calls++; return nil, errElectionObservationPending },
		wait:  func(context.Context) error { return nil },
	}
	if _, err := waitSettledElection(context.Background(), electionTestExpected(), backend); err == nil || calls != 128 {
		t.Fatal("observation count unbounded", calls, err)
	}
}

func TestSettledElectionHistoryBoundPreservesEarlierObservations(t *testing.T) {
	out := t.TempDir()
	first := bytes.Repeat([]byte("x"), (512<<10)-1)
	if err := appendElectionHistory(out, first); err != nil {
		t.Fatal(err)
	}
	if err := appendElectionHistory(out, []byte("later")); err == nil {
		t.Fatal("history exceeded 512 KiB")
	}
	got, err := os.ReadFile(filepath.Join(out, "monitor-election-history.jsonl"))
	if err != nil || !bytes.Equal(got, append(first, '\n')) {
		t.Fatal("bound failure modified original evidence", err)
	}
}

func TestSettledElectionQueryTimeoutCannotHideUnstoppedOrTruncatedFailure(t *testing.T) {
	for _, fault := range []string{"confirmed timeout", "unstopped timeout", "truncated timeout", "auth failure", "complete"} {
		result := ceph.NativeCommandResult{Stdout: []byte("metadata")}
		var runErr error
		switch fault {
		case "confirmed timeout":
			result.TimedOut = true
			runErr = context.DeadlineExceeded
		case "unstopped timeout":
			runErr = errors.Join(context.DeadlineExceeded, &ceph.NativeCommandUnstoppedError{})
		case "truncated timeout":
			result.TimedOut, result.StdoutTruncated = true, true
		case "auth failure":
			result.ExitCode = 13
		}
		data, err := electionQueryResult(result, runErr)
		if errors.Is(err, errElectionObservationPending) != (fault == "confirmed timeout") {
			t.Fatal("permanent/unstopped failure was treated as a retryable read", fault, err)
		}
		if fault == "complete" && (err != nil || string(data) != "metadata") || fault != "complete" && (err == nil || data != nil) {
			t.Fatal("incomplete native data was exposed as a complete observation", fault, data, err)
		}
	}
}
