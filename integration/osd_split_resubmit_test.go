package integration_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// An append executes but its reply is withheld, so it ends as an unknown
// outcome. The PG then splits and the object moves to the child PG. The same
// client resubmits the append under its original identity to the child,
// routed by the new placement: the OSD answers from the child's PG log (the
// split carried the entry with its request ID) with the original user version,
// and nothing executes again. The same holds in reverse: an APPEND lost in the
// child is answered from the parent's log after the PGs merge back.
func TestCephOSDSplitResubmitIntegration(t *testing.T) {
	if !isReplicatedPGFixtureProfile() || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("requires an owned single-OSD replicated-PG PG split fixture")
	}
	_, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	var name string
	prepared, started, restored := false, false, false
	t.Cleanup(func() {
		// Restore the PG count before deleting the owned objects.
		if started && !restored {
			cleanup, stop := context.WithTimeout(context.Background(), 165*time.Second)
			finish, err := splitControl(cleanup, "finish", name)
			stop()
			if err != nil || !finish.Done || !finish.Restored {
				t.Error("native PG split restoration unconfirmed; owned objects retained", err)
				return
			}
		}
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			if !replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned objects were not removed")
			}
		}
	})
	var route osdFixtureOracle
	for batch := 0; batch < 4 && name == ""; batch++ {
		owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("split resubmit")}, Reserve: 8})
		prepared = true
		for _, candidate := range owned.Objects {
			if native := replicatedPGNative(t, ctx, candidate); native.Placement.Hash&8 != 0 {
				name, route = candidate, native.Placement
				break
			}
		}
		if name == "" {
			if !replicatedPGOwned(t, ctx, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Fatal("unselected candidate cleanup incomplete")
			}
			prepared = false
		}
	}
	if name == "" {
		t.Fatal("bounded candidates did not yield a native split child")
	}

	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	options := integrationOptions(t)
	options.EnableOSD = true
	options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(call, network, endpoint)
		if err != nil {
			return nil, err
		}
		if _, daemon, _ := testcluster.SplitPeer(endpoint); daemon != "osd.0" {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			// Only the armed connection withholds replies; later ones read freely.
			armed.Store(false)
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
	before := replicatedPGNative(t, ctx, name).Actual
	tail := []byte("\x00appended-once-across-split\xff")
	appendOp := cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail}
	request := replicatedPGRequest(route, appendOp)
	request.ClientIncarnation = 101
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001200}
	o := replicatedPGConnection(t, c, ctx, route)
	call, stopCall := context.WithCancel(ctx)
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan completion, 1)
	armed.Store(true)
	go func() { r, e := o.Request(call, request); done <- completion{r, e} }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("reply withholding was not activated")
	}
	want := append(bytes.Clone(before.Data), tail...)
	var executed mutationSnapshot
	for {
		executed = replicatedPGNative(t, ctx, name).Actual
		if executed.Code == 0 && bytes.Equal(executed.Data, want) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the append was not observed natively")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stopCall()
	lost := <-done
	checkSplitUnknown(t, lost.reply, lost.err, request, context.Canceled)
	o.Close()

	started = true
	start, err := splitControl(ctx, "start", name)
	if err != nil || !start.Ready {
		t.Fatal("native actual PG split", start, err)
	}
	child := replicatedPGNative(t, ctx, name)
	if child.Placement.PG != route.PG+8 || child.Placement.Epoch <= route.Epoch {
		t.Fatal("the object did not move to the split child", route.PGID, child.Placement.PGID)
	}
	fresh := replicatedPGConnection(t, c, ctx, child.Placement)
	defer fresh.Close()
	again := replicatedPGRequest(child.Placement, appendOp)
	again.ClientIncarnation, again.ModificationTime = request.ClientIncarnation, request.ModificationTime
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	resolved, err := fresh.Request(ctx, again)
	if err != nil {
		t.Fatal("resubmission to the split child", err)
	}
	t.Logf("resubmitted to %s (parent %s): code=%d flags=%#x user=%d epoch %d -> %d", child.Placement.PGID, route.PGID, resolved.Code, resolved.Flags, resolved.UserVersion, route.Epoch, child.Placement.Epoch)
	if resolved.Transaction != lost.reply.Submission.Transaction || resolved.Code != 0 || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resubmitted receipt differs: tid=%d user=%d native=%d", resolved.Transaction, resolved.UserVersion, executed.Version)
	}
	time.Sleep(300 * time.Millisecond)
	if after := replicatedPGNative(t, ctx, name).Actual; after.Version != executed.Version || !bytes.Equal(after.Data, want) {
		t.Fatalf("the resubmission executed again: version %d -> %d", executed.Version, after.Version)
	}

	// The reverse direction: an APPEND in the child whose reply is withheld,
	// then the merge back into the parent, then a resubmission to the parent.
	tail2 := []byte("\x00appended-once-across-merge\xfe")
	appendOp2 := cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail2)), Data: tail2}
	request2 := replicatedPGRequest(child.Placement, appendOp2)
	request2.ClientIncarnation = request.ClientIncarnation
	request2.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001201}
	call2, stopCall2 := context.WithCancel(ctx)
	defer stopCall2()
	armed.Store(true)
	go func() { r, e := fresh.Request(call2, request2); done <- completion{r, e} }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("reply withholding was not activated in the child")
	}
	want2 := append(bytes.Clone(want), tail2...)
	var executed2 mutationSnapshot
	for {
		executed2 = replicatedPGNative(t, ctx, name).Actual
		if executed2.Code == 0 && bytes.Equal(executed2.Data, want2) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the child append was not observed natively")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stopCall2()
	lost2 := <-done
	checkSplitUnknown(t, lost2.reply, lost2.err, request2, context.Canceled)
	fresh.Close()

	finish, err := splitControl(ctx, "finish", name)
	if err != nil || !finish.Done || !finish.Restored {
		t.Fatal("native PG merge restoration", finish, err)
	}
	restored = true
	parent := replicatedPGNative(t, ctx, name)
	if parent.Placement.PG != route.PG || parent.Placement.Epoch <= child.Placement.Epoch {
		t.Fatal("the object did not return to the merged parent", child.Placement.PGID, parent.Placement.PGID)
	}
	merged := replicatedPGConnection(t, c, ctx, parent.Placement)
	defer merged.Close()
	again2 := replicatedPGRequest(parent.Placement, appendOp2)
	again2.ClientIncarnation, again2.ModificationTime = request2.ClientIncarnation, request2.ModificationTime
	again2.Resubmit, again2.RetryAttempt = &lost2.reply.Submission, 1
	resolved2, err := merged.Request(ctx, again2)
	if err != nil {
		t.Fatal("resubmission to the merged parent", err)
	}
	t.Logf("resubmitted to %s (merged from %s): code=%d flags=%#x user=%d epoch %d -> %d", parent.Placement.PGID, child.Placement.PGID, resolved2.Code, resolved2.Flags, resolved2.UserVersion, child.Placement.Epoch, parent.Placement.Epoch)
	if resolved2.Transaction != lost2.reply.Submission.Transaction || resolved2.Code != 0 || resolved2.UserVersion != executed2.Version || resolved2.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resubmitted receipt after the merge differs: tid=%d user=%d native=%d", resolved2.Transaction, resolved2.UserVersion, executed2.Version)
	}
	time.Sleep(300 * time.Millisecond)
	if after := replicatedPGNative(t, ctx, name).Actual; after.Version != executed2.Version || !bytes.Equal(after.Data, want2) {
		t.Fatalf("the resubmission executed again after the merge: version %d -> %d", executed2.Version, after.Version)
	}
}
