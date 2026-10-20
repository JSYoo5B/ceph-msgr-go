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

// The PG log, and so its request index, is replicated with each entry. After
// SIGKILL of the primary, an explicit resubmission to the replacement primary
// under the original transaction ID and incarnation is answered from that log:
// a completed exclusive CREATE does not answer EEXIST and an append whose reply
// was lost is not executed a second time.
func TestCephOSDResubmitFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three sparse-file BlueStore replicas")
	}
	// The shared restoration helper is legacy-v6 only, so no PG profile runs here.
	request := osdObjectRequest
	c, _, base, original := osdFixture(t)
	ctx, cancelAll := context.WithTimeout(context.Background(), 300*time.Second)
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
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("seed")}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("native seed differs", owned.SeedCode, owned.Objects)
	}
	// The reserved name starts absent.
	subject := owned.Objects[1]
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: subject}).State
	}
	route := func() osdFixtureOracle {
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": subject, "namespace": ""}).Placement
		if r.Name != subject || r.Pool != base.Pool || r.ID < 0 || r.Address == "" {
			t.Fatal("native placement differs", r)
		}
		return r
	}
	target := route()
	if len(target.Acting) != 3 {
		t.Fatalf("expected a healthy three-replica placement: %+v", target)
	}
	primary := strconv.Itoa(int(target.ID))
	stopped := false
	defer func() { recoverMutationFixture(t, c, base, original, primary, stopped) }()

	// One client issues every request: resubmission is bound to its GlobalID.
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
		if _, kind, _ := testcluster.SplitPeer(endpoint); kind != "osd."+primary {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	lc, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	const incarnation = 11
	build := func(target osdFixtureOracle, seconds uint32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		r := request(target, ops...)
		r.ClientIncarnation = incarnation
		r.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
		return r
	}
	o := mutationConnection(t, lc, ctx, target)
	createRequest := build(target, 1700002000, cephmsgr.OSDOperation{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate})
	created, err := o.Request(ctx, createRequest)
	if err != nil || created.Code != 0 || created.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("exclusive create before failover", err, created.Code)
	}

	tail := []byte("lost-across-failover")
	appendRequest := build(target, 1700002001, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	armed.Store(true)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		r, e := o.Request(callCtx, appendRequest)
		finished <- result{r, e}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault not activated", ctx.Err())
	}
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
	if err = testcluster.ControlDaemon(ctx, dir, "kill", "osd", primary); err != nil {
		t.Fatal("force primary termination", err)
	}
	stopped = true
	cancel()
	lost := <-finished
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Submission.Transaction == 0 {
		t.Fatal("expected an unknown outcome with its identity", lost.err)
	}
	o.Close()
	armed.Store(false)

	fresh := route()
	if fresh.Epoch <= target.Epoch || fresh.ID == target.ID || len(fresh.Acting) != 2 {
		t.Fatalf("expected a refreshed two-replica placement without the killed primary: %+v", fresh)
	}
	if after := observe(); after.Version != executed.Version || !bytes.Equal(after.Data, tail) {
		t.Fatalf("surviving replicas differ from the executed append: %x version %d", after.Data, after.Version)
	}
	o = mutationConnection(t, lc, ctx, fresh)
	defer o.Close()

	// The routing fields come from the new placement; the identity from the
	// original submission.
	again := build(fresh, 1700002000, createRequest.Operations...)
	again.Resubmit, again.RetryAttempt = &created.Submission, 1
	recreated, err := o.Request(ctx, again)
	if err != nil {
		t.Fatal("resubmitted exclusive create was re-executed or refused", err)
	}
	t.Logf("resubmitted create: code=%d flags=%#x user=%d ops=%+v", recreated.Code, recreated.Flags, recreated.UserVersion, recreated.Operations)
	if recreated.Transaction != created.Transaction || recreated.Code != 0 || recreated.UserVersion != created.UserVersion || recreated.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resubmitted create receipt differs: tid=%d code=%d user=%d want user %d", recreated.Transaction, recreated.Code, recreated.UserVersion, created.UserVersion)
	}

	retry := build(fresh, 1700002001, appendRequest.Operations...)
	retry.Resubmit, retry.RetryAttempt = &lost.reply.Submission, 1
	resolved, err := o.Request(ctx, retry)
	if err != nil {
		t.Fatal("resubmission of the unknown outcome", err)
	}
	t.Logf("resubmitted append: code=%d flags=%#x user=%d", resolved.Code, resolved.Flags, resolved.UserVersion)
	if resolved.Transaction != lost.reply.Submission.Transaction || resolved.Code != 0 || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resolved receipt differs: tid=%d user=%d flags=%#x native=%d", resolved.Transaction, resolved.UserVersion, resolved.Flags, executed.Version)
	}
	time.Sleep(300 * time.Millisecond)
	if after := observe(); !bytes.Equal(after.Data, tail) || after.Version != executed.Version {
		t.Fatalf("resubmission to the replacement primary appended again: %x version %d", after.Data, after.Version)
	}
	t.Log("the replacement primary answered both resubmissions from the replicated PG log without re-execution")
}
