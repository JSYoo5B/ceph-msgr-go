package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// An explicit resubmission repeats a request under its original transaction ID
// and incarnation with FLAG_RETRY, so the OSD answers from its PG log instead of
// appending again. A new request with a new ID executes again (the control).
func TestCephOSDResubmitIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	_, _, base, _ := fixture(t)
	// The fixture's own context is a short bootstrap deadline.
	ctx, cancelAll := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancelAll()
	baseline := []byte{0, 0xff, 'r', 'e', 's', 'u', 'b', 10, 1, 2, 3}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned objects were not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 5})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 6 {
		t.Fatal("native seed differs", owned.SeedCode, owned.Objects)
	}
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name}).State
	}
	route := func(name string) osdFixtureOracle {
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if r.Name != name || r.Pool != base.Pool || r.ID < 0 {
			t.Fatal("native placement differs", r)
		}
		return r
	}
	const incarnation = 7
	appendOp := func(tail string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: []byte(tail)}
	}
	build := func(target osdFixtureOracle, tail string) cephmsgr.OSDRequest {
		r := request(target, appendOp(tail))
		r.ClientIncarnation = incarnation
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001000}
		return r
	}

	t.Run("completed request answered from the PG log", func(t *testing.T) {
		client, _, _, _ := fixture(t)
		target := route(owned.Objects[0])
		o := mutationConnection(t, client, ctx, target)
		defer o.Close()
		request := build(target, "once")
		first, err := o.Request(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		afterFirst := observe(owned.Objects[0])
		want := append(bytes.Clone(baseline), "once"...)
		if !bytes.Equal(afterFirst.Data, want) || first.UserVersion != afterFirst.Version {
			t.Fatal("first append differs from native", afterFirst.Version, first.UserVersion)
		}
		again := request
		again.Resubmit, again.RetryAttempt = &first.Submission, 1
		second, err := o.Request(ctx, again)
		if err != nil {
			t.Fatal("resubmission", err)
		}
		t.Logf("resubmitted reply: flags=%#x user=%d replay=%+v attempt=%d ops=%+v", second.Flags, second.UserVersion, second.ReplayVersion, second.RetryAttempt, second.Operations)
		if second.Transaction != first.Transaction || second.RetryAttempt != 1 || second.UserVersion != first.UserVersion || second.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("resubmitted receipt differs from the original", second.Transaction, second.UserVersion)
		}
		afterSecond := observe(owned.Objects[0])
		if !bytes.Equal(afterSecond.Data, want) || afterSecond.Version != afterFirst.Version {
			t.Fatalf("resubmission executed again: %x version %d", afterSecond.Data, afterSecond.Version)
		}
		// Control: a new request with a new identity does execute again.
		fresh := build(target, "once")
		third, err := o.Request(ctx, fresh)
		if err != nil || third.Transaction == first.Transaction {
			t.Fatal("fresh request", err)
		}
		if got := observe(owned.Objects[0]); !bytes.Equal(got.Data, append(bytes.Clone(want), "once"...)) {
			t.Fatalf("a fresh identity did not execute a second append: %x", got.Data)
		}
	})

	t.Run("unknown outcome resolved by resubmission", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		target := route(owned.Objects[1])
		var armed atomic.Bool
		blocked := make(chan struct{}, 1)
		dial := options.DialContext
		options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			conn, err := dial(ctx, network, endpoint)
			if err != nil {
				return nil, err
			}
			if _, kind, _ := testcluster.SplitPeer(endpoint); kind != "osd."+strconv.Itoa(int(target.ID)) {
				return conn, nil
			}
			return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
				select {
				case blocked <- struct{}{}:
				default:
				}
			}}, nil
		}
		c, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		o := mutationConnection(t, c, ctx, target)
		armed.Store(true)
		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		request := build(target, "lost")
		type result struct {
			reply cephmsgr.OSDReply
			err   error
		}
		finished := make(chan result, 1)
		go func() {
			r, e := o.Request(callCtx, request)
			finished <- result{r, e}
		}()
		select {
		case <-blocked:
		case <-ctx.Done():
			t.Fatal("receive fault not activated", ctx.Err())
		}
		// The reserved name starts absent, so the append creates it.
		want := []byte("lost")
		var executed omapWriteState
		for {
			if executed = observe(owned.Objects[1]); executed.Code == 0 && bytes.Equal(executed.Data, want) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("append not visible", ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		cancel()
		lost := <-finished
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(lost.err, &unknown) || lost.reply.Submission.Transaction == 0 {
			t.Fatal("expected an unknown outcome with its identity", lost.err)
		}
		if err := o.Close(); err != nil {
			t.Fatal(err)
		}
		armed.Store(false)
		o = mutationConnection(t, c, ctx, target)
		defer o.Close()
		again := request
		again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
		resolved, err := o.Request(ctx, again)
		if err != nil {
			t.Fatal("resubmission of the unknown outcome", err)
		}
		if resolved.Transaction != lost.reply.Submission.Transaction || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatalf("resolved receipt differs: tid=%d user=%d flags=%#x native=%d", resolved.Transaction, resolved.UserVersion, resolved.Flags, executed.Version)
		}
		time.Sleep(300 * time.Millisecond)
		if after := observe(owned.Objects[1]); !bytes.Equal(after.Data, want) || after.Version != executed.Version {
			t.Fatalf("resubmission appended again: %x version %d", after.Data, after.Version)
		}
	})

	// Re-executing these vectors would give a different reply or state: CREATE
	// with EXCL would answer EEXIST, DELETE would answer ENOENT and a second
	// APPEND would double the bytes. The logged answer must come back instead.
	t.Run("non-idempotent vectors answered from the PG log", func(t *testing.T) {
		client, _, _, _ := fixture(t)
		for _, tc := range []struct {
			name, object string
			prepare      []cephmsgr.OSDOperation
			ops          []cephmsgr.OSDOperation
			check        func(t *testing.T, state omapWriteState)
		}{
			{name: "exclusive create", object: owned.Objects[2], ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}},
				check: func(t *testing.T, s omapWriteState) {
					if s.Code != 0 || len(s.Data) != 0 {
						t.Fatal("created object differs", s.Code, len(s.Data))
					}
				}},
			{name: "delete", object: owned.Objects[3], prepare: []cephmsgr.OSDOperation{appendOp("doomed")}, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDDelete}},
				check: func(t *testing.T, s omapWriteState) {
					if s.Code != -2 {
						t.Fatal("object survived the delete", s.Code)
					}
				}},
			{name: "append with xattr", object: owned.Objects[4], ops: []cephmsgr.OSDOperation{appendOp("ab"), {Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "k"}, Data: []byte("v")}},
				check: func(t *testing.T, s omapWriteState) {
					if s.Code != 0 || !bytes.Equal(s.Data, []byte("ab")) || len(s.Attributes) != 1 {
						t.Fatalf("compound state differs: %q %v", s.Data, s.Attributes)
					}
				}},
		} {
			target := route(tc.object)
			o := mutationConnection(t, client, ctx, target)
			if len(tc.prepare) > 0 {
				prepared := request(target, tc.prepare...)
				prepared.ClientIncarnation, prepared.ModificationTime = incarnation, cephmsgr.OSDTime{Seconds: 1700001001}
				if _, err := o.Request(ctx, prepared); err != nil {
					t.Fatal(tc.name, "prepare", err)
				}
			}
			original := request(target, tc.ops...)
			original.ClientIncarnation, original.ModificationTime = incarnation, cephmsgr.OSDTime{Seconds: 1700001002}
			first, err := o.Request(ctx, original)
			if err != nil {
				t.Fatal(tc.name, "first", err)
			}
			before := observe(tc.object)
			again := original
			again.Resubmit, again.RetryAttempt = &first.Submission, 1
			second, err := o.Request(ctx, again)
			if err != nil {
				t.Fatalf("%s: resubmission was re-executed or refused: %v", tc.name, err)
			}
			if second.Code != first.Code || second.UserVersion != first.UserVersion || len(second.Operations) != len(first.Operations) || second.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatalf("%s: resubmitted receipt differs: first=%+v second=%+v", tc.name, first, second)
			}
			for i := range first.Operations {
				if second.Operations[i].Result != first.Operations[i].Result {
					t.Fatalf("%s: per-op result %d differs", tc.name, i)
				}
			}
			after := observe(tc.object)
			if after.Version != before.Version || !bytes.Equal(after.Data, before.Data) {
				t.Fatalf("%s: native state changed under the resubmission", tc.name)
			}
			tc.check(t, after)
			o.Close()
		}
	})
}
