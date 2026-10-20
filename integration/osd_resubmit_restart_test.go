package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// An APPEND executes but its reply is withheld, so it ends as an unknown
// outcome. Its OSD is then SIGKILLed and restarted. The same client
// resubmits the APPEND under its original identity to the restarted daemon
// (new address nonce): the OSD answers from the PG log it reloaded from
// BlueStore with the original user version, and nothing executes again.
func TestCephOSDResubmitRestartIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		// The shared restoration helper is legacy-v6 only.
		t.Skip("daemon restart needs the legacy-v6 BlueStore fixture")
	}
	request := osdObjectRequest
	admin, _, base, original := osdFixture(t)
	ctx, cancelAll := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancelAll()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
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
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("restart")}, Reserve: 1})
	prepared = true
	name := owned.Objects[1]
	route := func() osdFixtureOracle {
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if r.Name != name || r.ID < 0 {
			t.Fatal("native placement differs", r)
		}
		return r
	}
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name}).State
	}
	target := route()

	options := integrationOptions(t)
	options.EnableOSD = true
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
	tail := []byte("\x00once-across-restart\xff")
	build := func(r osdFixtureOracle) cephmsgr.OSDRequest {
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
		req.ClientIncarnation = 13
		req.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001500}
		return req
	}
	o := mutationConnection(t, c, ctx, target)
	armed.Store(true)
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	first := build(target)
	go func() {
		r, e := o.Request(call, first)
		finished <- result{r, e}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault not activated", ctx.Err())
	}
	// The reserved name starts absent, so the append creates it.
	var executed omapWriteState
	for {
		if executed = observe(); executed.Code == 0 && bytes.Equal(executed.Data, tail) {
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
	o.Close()
	armed.Store(false)

	victim := strconv.Itoa(int(target.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim); err != nil {
		t.Fatal("kill", err)
	}
	recoverMutationFixture(t, admin, base, original, victim, true)
	fresh := route()
	if fresh.Address == target.Address {
		t.Fatal("the restarted OSD kept its address nonce")
	}
	n := mutationConnection(t, c, ctx, fresh)
	defer n.Close()
	again := build(fresh)
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	resolved, err := n.Request(ctx, again)
	if err != nil {
		t.Fatal("resubmission to the restarted OSD", err)
	}
	t.Logf("resubmitted to osd.%d after its restart (primary before osd.%d): code=%d flags=%#x user=%d epoch %d -> %d", fresh.ID, target.ID, resolved.Code, resolved.Flags, resolved.UserVersion, target.Epoch, fresh.Epoch)
	if resolved.Transaction != lost.reply.Submission.Transaction || resolved.Code != 0 || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resolved receipt differs: tid=%d user=%d flags=%#x native=%d", resolved.Transaction, resolved.UserVersion, resolved.Flags, executed.Version)
	}
	time.Sleep(300 * time.Millisecond)
	if after := observe(); !bytes.Equal(after.Data, tail) || after.Version != executed.Version {
		t.Fatalf("resubmission appended again: %x version %d", after.Data, after.Version)
	}
}
