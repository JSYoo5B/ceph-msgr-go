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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A WRITEFULL carrying a self-managed SnapContext executes (the OSD clones the
// previous head under the id) but its reply is withheld, so it ends as an
// unknown outcome. Resubmitting it under the original identity is answered from
// the PG log with the original user version, and native observation shows one
// clone and one head: the snapshotted write did not run again.
func TestCephOSDResubmitSnapshotIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "resubmit-snap-object"
	original := bytes.Repeat([]byte("snap-original\x00"), 200)
	poolSeed(t, ctx, 1, object, "", original, nil)
	allocated, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal("allocate self-managed snapshot", err)
	}
	snap, err := allocated.SnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	route := poolPlace(t, ctx, 1, object, "")

	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	options := integrationOptions(t)
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(call, network, endpoint)
		if err != nil {
			return nil, err
		}
		if _, kind, _ := testcluster.SplitPeer(endpoint); kind != "osd."+strconv.Itoa(int(route.ID)) {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			armed.Store(false)
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	o := mutationConnection(t, client, ctx, route)

	updated := bytes.Repeat([]byte("snap-updated\xff"), 180)
	snapWrite := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(updated)), Data: updated})
	snapWrite.ClientIncarnation = 23
	snapWrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009100}
	snapWrite.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	call, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	armed.Store(true)
	go func() {
		r, e := o.Request(call, snapWrite)
		finished <- result{r, e}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault not activated", ctx.Err())
	}
	for {
		if head := poolObserve(t, ctx, 1, object, "", ""); bytes.Equal(head.Data, updated) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the snapshotted write was not observed")
		case <-time.After(100 * time.Millisecond):
		}
	}
	executed := poolObserve(t, ctx, 1, object, "", "")
	cloneBefore := poolObserveSnap(t, ctx, 1, object, "", "", snap)
	if !bytes.Equal(cloneBefore.Data, original) {
		t.Fatal("the snapshot clone does not hold the original bytes", len(cloneBefore.Data))
	}
	cancelCall()
	lost := <-finished
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Submission.Transaction == 0 {
		t.Fatal("the withheld snapshotted write must end as an unknown outcome", lost.err)
	}
	o.Close()

	n := mutationConnection(t, client, ctx, route)
	defer n.Close()
	again := snapWrite
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	resolved, err := n.Request(ctx, again)
	if err != nil {
		t.Fatal("resubmission of the snapshotted write", err)
	}
	t.Logf("resubmitted snapshotted write: code=%d flags=%#x user=%d (native head version %d)", resolved.Code, resolved.Flags, resolved.UserVersion, executed.Version)
	if resolved.Transaction != lost.reply.Submission.Transaction || resolved.Code != 0 || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resolved receipt differs: tid=%d user=%d native=%d", resolved.Transaction, resolved.UserVersion, executed.Version)
	}
	time.Sleep(300 * time.Millisecond)
	head := poolObserve(t, ctx, 1, object, "", "")
	clone := poolObserveSnap(t, ctx, 1, object, "", "", snap)
	if head.Version != executed.Version || !bytes.Equal(head.Data, updated) || !bytes.Equal(clone.Data, original) {
		t.Fatalf("the resubmission ran again: head version %d -> %d", executed.Version, head.Version)
	}
}
