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

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A write under a self-managed SnapContext creates a clone; after ONDISK the
// clone and the new head survive SIGKILL and restart of the object's primary,
// and LIST_SNAPS still names the snapshot.
func TestCephOSDSnapshotDurabilityIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon termination persistence needs the BlueStore fixture")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "snapshot-durability"
	original := bytes.Repeat([]byte("original\x00"), 100)
	poolSeed(t, ctx, 1, object, "", original, nil)
	allocated, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := allocated.SnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	route := poolPlace(t, ctx, 1, object, "")
	o := mutationConnection(t, c, ctx, route)
	updated := []byte("updated head")
	w := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(updated)), Data: updated})
	w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006000}
	w.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	reply, err := o.Request(ctx, w)
	o.Close()
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK snapshot write", err)
	}
	name := strconv.FormatInt(int64(route.ID), 10)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		if !restarted {
			recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
			defer stop()
			testcluster.ControlDaemon(recover, dir, "start", "osd", name)
			waitOSDFixtureClean(t, recover, base)
		}
	}()
	if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted = true
	waitOSDFixtureClean(t, ctx, base)
	fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
	publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	if head := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(head.Data, updated) || head.Version != reply.UserVersion {
		t.Fatal("native head after restart differs", head.Version, reply.UserVersion)
	}
	if clone := poolObserveSnap(t, ctx, 1, object, "", "", snap); !bytes.Equal(clone.Data, original) {
		t.Fatal("native clone after restart differs")
	}
	route = poolPlace(t, ctx, 1, object, "")
	o = mutationConnection(t, c, ctx, route)
	defer o.Close()
	list, err := o.Request(ctx, pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}))
	if err != nil {
		t.Fatal("list snapshots", err)
	}
	found := false
	for _, clone := range list.Operations[0].Snapshots.Clones {
		for _, id := range clone.Snapshots {
			found = found || id == snap
		}
	}
	if !found {
		t.Fatalf("snapshot %d missing after restart: %+v", snap, list.Operations[0].Snapshots)
	}
}

// A snapshot write whose reply is lost resolves through an explicit
// resubmission answered from the PG log; the clone exists once, holding the
// original bytes, and the head holds the new ones.
func TestCephOSDSnapshotLostReplyIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "snapshot-lost-reply"
	original := []byte("snapshot original bytes")
	poolSeed(t, ctx, 1, object, "", original, nil)
	allocated, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := allocated.SnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	route := poolPlace(t, ctx, 1, object, "")
	options := integrationOptions(t)
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		if _, kind, _ := testcluster.SplitPeer(endpoint); kind != "osd."+strconv.Itoa(int(route.ID)) {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
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
	armed.Store(true)
	updated := []byte("snapshot updated head")
	w := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(updated)), Data: updated})
	w.ClientIncarnation, w.ModificationTime = 4, cephmsgr.OSDTime{Seconds: 1700007000}
	w.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	callCtx, stop := context.WithCancel(ctx)
	defer stop()
	type result struct {
		r cephmsgr.OSDReply
		e error
	}
	lost := make(chan result, 1)
	go func() { r, e := o.Request(callCtx, w); lost <- result{r, e} }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("reply fault not activated")
	}
	var head nativeXattrSnapshot
	for {
		if head = poolObserve(t, ctx, 1, object, "", ""); bytes.Equal(head.Data, updated) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the snapshot write never executed")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stop()
	got := <-lost
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(got.e, &unknown) || got.r.Submission.Transaction == 0 {
		t.Fatal("expected an unknown outcome with identity", got.e)
	}
	o.Close()
	armed.Store(false)
	o = mutationConnection(t, client, ctx, route)
	defer o.Close()
	again := w
	again.Resubmit, again.RetryAttempt = &got.r.Submission, 1
	resolved, err := o.Request(ctx, again)
	if err != nil || resolved.UserVersion != head.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatalf("resubmission was not answered from the log: %v (user %d native %d)", err, resolved.UserVersion, head.Version)
	}
	if after := poolObserve(t, ctx, 1, object, "", ""); after.Version != head.Version || !bytes.Equal(after.Data, updated) {
		t.Fatal("the head changed under the resubmission")
	}
	if clone := poolObserveSnap(t, ctx, 1, object, "", "", snap); !bytes.Equal(clone.Data, original) {
		t.Fatal("the clone does not hold the original bytes")
	}
	list, err := o.Request(ctx, pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}))
	if err != nil {
		t.Fatal(err)
	}
	clones := 0
	for _, clone := range list.Operations[0].Snapshots.Clones {
		if clone.ID != cephmsgr.OSDHeadSnapshot {
			clones++
		}
	}
	if clones != 1 {
		t.Fatalf("expected exactly one clone, got %+v", list.Operations[0].Snapshots)
	}
}
