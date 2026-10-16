package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// The MDS side of an unsafe-to-safe window, following the
// ceph-testcontainers-go v0.3.0 receipt (TestCephFSUnsafeRequestWindow,
// docs/guides/messenger-client-fixtures.md "MDS sessions"): while the owned
// MDS's journal is paused, two dump_ops_in_flight samples at least
// mdsUnsafeWindowGap apart must both hold each Go request at the journal
// submission flag point and without journal_committed; a request answered
// early must show early_replied, and one held without a reply must not. The
// MDS GID must be the same nonzero GID in both samples. A fixed sleep proves
// none of that; the samples replace it.
const mdsUnsafeWindowGap = 3 * time.Second

const mdsJournalAndReply = "submit entry: journal_and_reply"

// Request classes, from the MDS events (upstream requestSafety).
const (
	mdsRequestUnsafe       = "unsafe"
	mdsRequestNoEarlyReply = "no-early-reply"
	mdsRequestSafe         = "safe"
	mdsRequestInconsistent = "inconsistent"
)

// mdsUnsafeWindowSample is one dump_ops_in_flight observation of one MDS.
type mdsUnsafeWindowSample struct {
	MDSGID     uint64
	ReturnedAt time.Time `json:"returned_at"`
	SentAt     time.Time `json:"sent_at"`
	Ops        []nativeMDSCephFSHistoricOp
}

// mdsUnsafeWindowEvidence records both samples, their gap on the test
// clock (second request sent minus first reply received) and each request's
// classification in both samples.
type mdsUnsafeWindowEvidence struct {
	First, Second mdsUnsafeWindowSample
	GapMillis     int64             `json:"gap_ms"`
	Classes       map[string]string `json:"classes"`
}

func mdsRequestSafety(op nativeMDSCephFSHistoricOp) string {
	early, committed, replied := -1, -1, -1
	for i, event := range op.Events {
		switch {
		case event == "early_replied":
			early = i
		case strings.HasPrefix(event, "journal_committed"):
			committed = i
		case event == "replying":
			replied = i
		}
	}
	switch {
	case early < 0:
		return mdsRequestNoEarlyReply
	case committed < 0 && replied < 0:
		return mdsRequestUnsafe
	case committed > early && replied > committed:
		return mdsRequestSafe
	}
	return mdsRequestInconsistent
}

func findMDSClientOp(ops []nativeMDSCephFSHistoricOp, client, tid uint64) (nativeMDSCephFSHistoricOp, bool) {
	prefix := fmt.Sprintf("client_request(client.%d:%d ", client, tid)
	i := slices.IndexFunc(ops, func(op nativeMDSCephFSHistoricOp) bool { return strings.HasPrefix(op.Description, prefix) })
	if i < 0 {
		return nativeMDSCephFSHistoricOp{}, false
	}
	return ops[i], true
}

// checkMDSUnsafeWindow checks two samples of one MDS for the client's
// requests (TID -> wanted class: mdsRequestUnsafe or mdsRequestNoEarlyReply)
// and returns the evidence, even on failure.
func checkMDSUnsafeWindow(first, second mdsUnsafeWindowSample, client uint64, want map[uint64]string) (mdsUnsafeWindowEvidence, error) {
	e := mdsUnsafeWindowEvidence{First: first, Second: second, GapMillis: second.SentAt.Sub(first.ReturnedAt).Milliseconds(), Classes: map[string]string{}}
	if first.ReturnedAt.IsZero() || second.SentAt.Before(first.ReturnedAt.Add(mdsUnsafeWindowGap)) {
		return e, fmt.Errorf("MDS in-flight samples are %d ms apart, want at least %s", e.GapMillis, mdsUnsafeWindowGap)
	}
	if first.MDSGID == 0 || first.MDSGID != second.MDSGID {
		return e, fmt.Errorf("MDS GID changed across the unsafe window: %d then %d", first.MDSGID, second.MDSGID)
	}
	if len(want) == 0 {
		return e, fmt.Errorf("no request named for the unsafe window")
	}
	for tid, class := range want {
		if class != mdsRequestUnsafe && class != mdsRequestNoEarlyReply {
			return e, fmt.Errorf("request %d: unsupported wanted class %q", tid, class)
		}
		for i, sample := range []mdsUnsafeWindowSample{first, second} {
			op, ok := findMDSClientOp(sample.Ops, client, tid)
			if !ok {
				return e, fmt.Errorf("sample %d: request client.%d:%d is not in flight", i+1, client, tid)
			}
			got := mdsRequestSafety(op)
			e.Classes[fmt.Sprintf("%d/%d", tid, i+1)] = got
			if op.FlagPoint != mdsJournalAndReply || slices.ContainsFunc(op.Events, func(ev string) bool { return strings.HasPrefix(ev, "journal_committed") }) || got != class {
				return e, fmt.Errorf("sample %d: request %d is %s at %q, want %s waiting for the journal: %v", i+1, tid, got, op.FlagPoint, class, op.Events)
			}
		}
	}
	return e, nil
}

func TestMDSUnsafeWindowClassification(t *testing.T) {
	base := []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "acquired locks"}
	op := func(tid uint64, flag string, events ...string) nativeMDSCephFSHistoricOp {
		return nativeMDSCephFSHistoricOp{Description: fmt.Sprintf("client_request(client.4239:%d create #0x1/x 2026-10-15T15:51:25.344400+0000 caller_uid=0, caller_gid=0{})", tid), FlagPoint: flag, Events: append(slices.Clone(base), events...)}
	}
	unsafe := op(4, mdsJournalAndReply, "early_replied", mdsJournalAndReply)
	held := op(5, mdsJournalAndReply, mdsJournalAndReply)
	committed := op(4, "done", "early_replied", mdsJournalAndReply, "journal_committed: ", "replying", "done")
	if mdsRequestSafety(unsafe) != mdsRequestUnsafe || mdsRequestSafety(held) != mdsRequestNoEarlyReply || mdsRequestSafety(committed) != mdsRequestSafe ||
		mdsRequestSafety(op(4, "done", "early_replied", "replying", "journal_committed: ")) != mdsRequestInconsistent {
		t.Fatal("request classification differs from the upstream receipt")
	}
	at := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	sample := func(gid uint64, sent time.Time, ops ...nativeMDSCephFSHistoricOp) mdsUnsafeWindowSample {
		return mdsUnsafeWindowSample{MDSGID: gid, SentAt: sent, ReturnedAt: sent.Add(100 * time.Millisecond), Ops: ops}
	}
	want := map[uint64]string{4: mdsRequestUnsafe, 5: mdsRequestNoEarlyReply}
	first := sample(77, at, unsafe, held)
	if e, err := checkMDSUnsafeWindow(first, sample(77, at.Add(3100*time.Millisecond), held, unsafe), 4239, want); err != nil || e.GapMillis != 3000 || e.Classes["4/2"] != mdsRequestUnsafe || e.Classes["5/1"] != mdsRequestNoEarlyReply {
		t.Fatal(e, err)
	}
	for name, second := range map[string]mdsUnsafeWindowSample{
		"too close":         sample(77, at.Add(3*time.Second), unsafe, held),
		"gid changed":       sample(78, at.Add(4*time.Second), unsafe, held),
		"request gone":      sample(77, at.Add(4*time.Second), unsafe),
		"committed":         sample(77, at.Add(4*time.Second), committed, held),
		"other client":      sample(77, at.Add(4*time.Second), nativeMDSCephFSHistoricOp{Description: strings.Replace(unsafe.Description, "4239", "4240", 1), FlagPoint: unsafe.FlagPoint, Events: unsafe.Events}, held),
		"held got a reply":  sample(77, at.Add(4*time.Second), unsafe, op(5, mdsJournalAndReply, "early_replied", mdsJournalAndReply)),
		"other flag point":  sample(77, at.Add(4*time.Second), op(4, "failed to xlock, waiting", "early_replied"), held),
		"commit flag point": sample(77, at.Add(4*time.Second), op(4, mdsJournalAndReply, "early_replied", "journal_committed: "), held),
	} {
		if _, err := checkMDSUnsafeWindow(first, second, 4239, want); err == nil {
			t.Fatal(name, "accepted")
		}
	}
	if _, err := checkMDSUnsafeWindow(sample(0, at), sample(0, at.Add(4*time.Second)), 4239, want); err == nil {
		t.Fatal("zero MDS GID accepted")
	}
	if _, err := checkMDSUnsafeWindow(first, sample(77, at.Add(4*time.Second), unsafe, held), 4239, map[uint64]string{4: mdsRequestSafe}); err == nil {
		t.Fatal("a safe class accepted as an unsafe window")
	}
}
