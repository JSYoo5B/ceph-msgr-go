package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Balanced reads at a replica while the PG's primary is frozen (docker pause).
// Each read ends either with the native bytes or with a known -EAGAIN, never
// with other bytes or an invented result, and the client resends nothing.
// One read is sent at once (the replica's read lease from the frozen primary is
// still valid) and one after the lease has expired but before the cluster
// marks the primary down; the test then waits for the new primary.
func TestCephOSDReplicaReadPrimaryFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "replica-failover-object"
	data := []byte("served across a primary failover")
	poolSeed(t, ctx, 1, object, "", data, nil)
	route := poolPlace(t, ctx, 1, object, "")
	if len(route.Acting) != 3 || route.ID != route.PrimaryID {
		t.Fatalf("expected a healthy three-replica placement: %+v", route)
	}
	// A Go write to another object of the PG makes the seeded write stable on
	// every replica (OSD_REPLICA_READ_VALIDATION.md, unstable write).
	for i := 0; ; i++ {
		if i == 400 {
			t.Fatal("no further object name maps to the subject's PG")
		}
		name := fmt.Sprintf("replica-failover-filler-%d", i)
		if r := poolPlace(t, ctx, 1, name, ""); r.PGID == route.PGID {
			o := mutationConnection(t, c, ctx, r)
			w := pools.request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 6, Data: []byte("filler")})
			w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001400}
			_, err := o.Request(ctx, w)
			o.Close()
			if err != nil {
				t.Fatal("filler write", err)
			}
			break
		}
	}
	var replica int32 = -1
	for _, id := range route.Acting {
		if id != route.ID {
			replica = id
			break
		}
	}
	// The replica connection and placement are prepared before the freeze.
	at := poolPlaceOn(t, ctx, 1, object, "", &replica)
	o := mutationConnection(t, c, ctx, at)
	defer o.Close()
	read := func(timeout time.Duration) (cephmsgr.OSDReply, time.Duration, error) {
		req := pools.request(at, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
		if req.Placement != nil {
			req.Placement.Primary = at.PrimaryID
		}
		req.BalanceReads = true
		call, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		started := time.Now()
		reply, err := o.Request(call, req)
		return reply, time.Since(started), err
	}
	check := func(label string, reply cephmsgr.OSDReply, err error, elapsed time.Duration) {
		t.Helper()
		var server *cephmsgr.OSDError
		switch {
		case err == nil:
			if len(reply.Operations) != 1 || !bytes.Equal(reply.Operations[0].Data, data) {
				t.Fatalf("%s: osd.%d served other bytes", label, replica)
			}
			t.Logf("%s: osd.%d served the native bytes in %s (retry attempt %d)", label, replica, elapsed.Round(time.Millisecond), reply.RetryAttempt)
		case errors.As(err, &server) && server.Code == -11 && reply.Code == -11:
			t.Logf("%s: osd.%d answered -EAGAIN in %s (retry attempt %d)", label, replica, elapsed.Round(time.Millisecond), reply.RetryAttempt)
		default:
			t.Fatalf("%s: expected the native bytes or a known -EAGAIN after %s: code %d: %v", label, elapsed, reply.Code, err)
		}
		if reply.RetryAttempt != 0 {
			t.Fatalf("%s: the client resent the read (attempt %d)", label, reply.RetryAttempt)
		}
	}
	reply, _, err := read(20 * time.Second)
	if err != nil || !bytes.Equal(reply.Operations[0].Data, data) {
		t.Fatal("balanced read before the freeze", err)
	}

	frozen := strconv.Itoa(int(route.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "pause", "osd", frozen); err != nil {
		t.Fatal("pause primary", err)
	}
	paused := true
	defer func() {
		// Resume, SIGKILL and start the frozen daemon (a thawed daemon past its
		// heartbeat grace did not return to clean PGs in the fixture's wait).
		restore, stop := context.WithTimeout(context.Background(), 240*time.Second)
		defer stop()
		if paused {
			if err := testcluster.ControlDaemon(restore, dir, "resume", "osd", frozen); err != nil {
				t.Error("resume", err)
				return
			}
		}
		for _, action := range []string{"kill", "start"} {
			if err := testcluster.ControlDaemon(restore, dir, action, "osd", frozen); err != nil {
				t.Error(action, err)
				return
			}
		}
		waitOSDFixtureClean(t, restore, base)
		fresh := nativeMutation(t, restore, map[string]any{"compare": false, "placement": true}).Placement
		publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	}()
	frozenAt := time.Now()
	reply, elapsed, err := read(60 * time.Second)
	check("lease valid", reply, err, elapsed)

	// Past the read lease (osd_pool_default_read_lease_ratio 0.8 of the 20 s
	// heartbeat grace) but before the primary is marked down.
	time.Sleep(time.Until(frozenAt.Add(18 * time.Second)))
	reply, elapsed, err = read(90 * time.Second)
	check("lease expired", reply, err, elapsed)
	moved := poolPlace(t, ctx, 1, object, "")
	for deadline := time.Now().Add(90 * time.Second); moved.PrimaryID == route.ID; moved = poolPlace(t, ctx, 1, object, "") {
		if time.Now().After(deadline) {
			t.Fatal("the frozen primary was not replaced")
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("primary osd.%d -> osd.%d (epoch %d -> %d), %s after the freeze", route.ID, moved.PrimaryID, route.Epoch, moved.Epoch, time.Since(frozenAt).Round(time.Second))
	if n := poolObserve(t, ctx, 1, object, "", ""); n.Code != 0 || !bytes.Equal(n.Data, data) {
		t.Fatal("native bytes changed", n.Code)
	}
}
