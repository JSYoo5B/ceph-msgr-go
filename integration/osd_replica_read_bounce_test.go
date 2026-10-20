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

// A replica answers a balanced read with -EAGAIN instead of serving bytes it
// cannot vouch for: an object whose last write is not yet known committed on
// every replica, and an object the replica is still missing after it rejoined
// with recovery held. Both are known refusals with no effect; the client
// resends nothing, and the caller's later read (after the next write in the
// PG, or after recovery) is served with the native bytes.
func TestCephOSDReplicaReadBounceIntegration(t *testing.T) {
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
	const object = "replica-bounce-object"
	poolSeed(t, ctx, 1, object, "", []byte("seed"), nil)
	route := poolPlace(t, ctx, 1, object, "")
	if len(route.Acting) != 3 || route.ID != route.PrimaryID {
		t.Fatalf("expected a healthy three-replica placement: %+v", route)
	}
	// fillPG writes another object of the same PG through Go, which advances
	// the PG's committed point past every earlier write.
	fill := 0
	fillPG := func(t *testing.T) {
		t.Helper()
		for ; ; fill++ {
			if fill == 400 {
				t.Fatal("no further object name maps to the subject's PG")
			}
			name := fmt.Sprintf("replica-bounce-filler-%d", fill)
			if r := poolPlace(t, ctx, 1, name, ""); r.PGID == route.PGID {
				o := mutationConnection(t, c, ctx, r)
				w := pools.request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 6, Data: []byte("filler")})
				w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001300}
				_, err := o.Request(ctx, w)
				o.Close()
				if err != nil {
					t.Fatal("filler write", err)
				}
				fill++
				return
			}
		}
	}
	write := func(t *testing.T, data []byte) cephmsgr.OSDReply {
		t.Helper()
		r := poolPlace(t, ctx, 1, object, "")
		o := mutationConnection(t, c, ctx, r)
		defer o.Close()
		w := pools.request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001301}
		reply, err := o.Request(ctx, w)
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("subject write", err)
		}
		return reply
	}
	// balancedRead sends one BALANCE_READS read to osd id.
	balancedRead := func(t *testing.T, id int32) (cephmsgr.OSDReply, error) {
		t.Helper()
		r := poolPlaceOn(t, ctx, 1, object, "", &id)
		o := mutationConnection(t, c, ctx, r)
		defer o.Close()
		req := pools.request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
		if req.Placement != nil {
			req.Placement.Primary = r.PrimaryID
		}
		req.BalanceReads = true
		callCtx, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		return o.Request(callCtx, req)
	}
	expectBounce := func(t *testing.T, id int32, why string) {
		t.Helper()
		reply, err := balancedRead(t, id)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -11 || reply.Code != -11 || reply.RetryAttempt != 0 {
			t.Fatalf("osd.%d: %s: expected a known -EAGAIN without a local resend, got code %d: %v", id, why, reply.Code, err)
		}
	}
	expectServed := func(t *testing.T, id int32, want []byte) {
		t.Helper()
		reply, err := balancedRead(t, id)
		if err != nil || !bytes.Equal(reply.Operations[0].Data, want) {
			t.Fatalf("osd.%d did not serve the native bytes: %v", id, err)
		}
	}
	var replicas []int32
	for _, id := range route.Acting {
		if id != route.ID {
			replicas = append(replicas, id)
		}
	}

	t.Run("unstable write", func(t *testing.T) {
		fillPG(t)
		data := []byte("unstable until the next write")
		write(t, data)
		// The subject's write is the PG's last one: no replica may serve it yet.
		expectBounce(t, replicas[0], "last write not known committed")
		if n := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(n.Data, data) {
			t.Fatal("native bytes differ from the write")
		}
		fillPG(t)
		time.Sleep(time.Second)
		expectServed(t, replicas[0], data)
	})

	t.Run("object missing on a rejoined replica", func(t *testing.T) {
		victim := replicas[1]
		name := strconv.Itoa(int(victim))
		nativePool(t, ctx, "osdflags", map[string]any{"action": "set"})
		flagsSet, started := true, true
		defer func() {
			recover, stop := context.WithTimeout(context.Background(), 200*time.Second)
			defer stop()
			if !started {
				testcluster.ControlDaemon(recover, dir, "start", "osd", name)
			}
			if flagsSet {
				nativePool(t, recover, "osdflags", map[string]any{"action": "unset"})
			}
			waitOSDFixtureClean(t, recover, base)
		}()
		if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
			t.Fatal(err)
		}
		started = false
		deadline := time.Now().Add(120 * time.Second)
		for slicesContains(poolPlace(t, ctx, 1, object, "").Acting, victim) {
			if time.Now().After(deadline) {
				t.Fatal("victim never left the acting set")
			}
			time.Sleep(2 * time.Second)
		}
		data := []byte("written while a replica was down")
		write(t, data)
		fillPG(t)
		if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
			t.Fatal(err)
		}
		started = true
		for !slicesContains(poolPlace(t, ctx, 1, object, "").Acting, victim) {
			if time.Now().After(deadline.Add(120 * time.Second)) {
				t.Fatal("victim never rejoined the acting set")
			}
			time.Sleep(2 * time.Second)
		}
		// Peering can lag the map; poll until the rejoined replica answers.
		var reply cephmsgr.OSDReply
		var err error
		for i := 0; i < 15; i++ {
			if reply, err = balancedRead(t, victim); err != nil && reply.Code == -11 {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if reply.Code != -11 {
			t.Fatalf("rejoined replica osd.%d without the object did not bounce: code %d %v", victim, reply.Code, err)
		}
		// Another write in the PG, now replicated to the rejoined OSD too, rules
		// out the unstable-write cause: only the missing object remains.
		fillPG(t)
		time.Sleep(time.Second)
		expectBounce(t, victim, "object missing while recovery is held")
		expectServed(t, route.ID, data)
		nativePool(t, ctx, "osdflags", map[string]any{"action": "unset"})
		flagsSet = false
		waitOSDFixtureClean(t, ctx, base)
		// Recovery re-establishes the PG; the recovered replica may still bounce
		// until a write in the new interval. Reads are idempotent, so the
		// caller simply asks again after one.
		fillPG(t)
		for i := 0; ; i++ {
			reply, err := balancedRead(t, victim)
			if err == nil {
				if !bytes.Equal(reply.Operations[0].Data, data) {
					t.Fatal("recovered replica served other bytes")
				}
				break
			}
			if reply.Code != -11 || i == 10 {
				t.Fatalf("recovered replica osd.%d: code %d %v", victim, reply.Code, err)
			}
			t.Logf("recovered replica osd.%d bounced again (attempt %d)", victim, i)
			time.Sleep(2 * time.Second)
		}
		t.Logf("osd.%d bounced while missing the object and served it after recovery", victim)
	})
}
