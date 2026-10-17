package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// BALANCE_READS and LOCALIZE_READS let any acting-set member serve a read-only
// vector. Without them a replica treats the request as misdirected and never
// answers. Native observation supplies the expected bytes and attributes.
func TestCephOSDReplicaReadIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" {
		t.Skip("requires three replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "replica-read-object"
	data := []byte{0, 0xff, 'r', 'e', 'p', 'l', 'i', 'c', 'a', 10, 1, 2, 3, 4, 5, 6, 7, 8}
	attrs := []nativeXattrEntry{{Name: []byte("replica-attr"), Value: []byte{0xff, 0, 'R'}}}
	seeded := poolSeed(t, ctx, 1, object, "", data, attrs)
	primaryRoute := poolPlace(t, ctx, 1, object, "")
	if len(primaryRoute.Acting) != 3 || primaryRoute.ID != primaryRoute.PrimaryID {
		t.Fatalf("expected a healthy three-replica placement: %+v", primaryRoute)
	}
	// A replica serves an object only once its pg_committed_to has passed the
	// object's last write, and the primary advances that only with the next
	// write in the PG. Write a second object in the same PG so the seed is
	// known committed on every replica; without it replicas answer -EAGAIN.
	for i := 0; ; i++ {
		if i == 200 {
			t.Fatal("no second object name maps to the seed's PG")
		}
		filler := fmt.Sprintf("replica-read-filler-%d", i)
		if poolPlace(t, ctx, 1, filler, "").PGID == primaryRoute.PGID {
			poolSeed(t, ctx, 1, filler, "", []byte("filler"), nil)
			break
		}
	}
	time.Sleep(time.Second)
	for _, id := range primaryRoute.Acting {
		id := id
		route := poolPlaceOn(t, ctx, 1, object, "", &id)
		if route.ID != id || route.PrimaryID != primaryRoute.ID || route.Address == "" {
			t.Fatal("native replica routing differs", route)
		}
		replica := id != primaryRoute.ID
		connection := mutationConnection(t, c, ctx, route)
		send := func(timeout time.Duration, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
			r := pools.request(route, ops...)
			if r.Placement != nil {
				r.Placement.Primary = route.PrimaryID
			}
			configure(&r)
			callCtx, stop := context.WithTimeout(ctx, timeout)
			defer stop()
			return connection.Request(callCtx, r)
		}
		read := cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(data) + 8)}
		stat := cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}
		attr := cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "replica-attr", MaxOutputBytes: 64}}
		for _, flag := range []struct {
			name      string
			configure func(*cephmsgr.OSDRequest)
		}{
			{"balance", func(r *cephmsgr.OSDRequest) { r.BalanceReads = true }},
			{"localize", func(r *cephmsgr.OSDRequest) { r.LocalizeReads = true }},
		} {
			reply, err := send(20*time.Second, flag.configure, read, stat, attr)
			if err != nil {
				t.Fatalf("osd.%d (replica=%v) %s read: %v", id, replica, flag.name, err)
			}
			if len(reply.Operations) != 3 || !bytes.Equal(reply.Operations[0].Data, data) || !bytes.Equal(reply.Operations[2].Data, attrs[0].Value) || reply.UserVersion != seeded.Version {
				t.Fatalf("osd.%d %s read differs from native: %+v", id, flag.name, reply.Operations)
			}
		}
		if replica {
			_, err := send(4*time.Second, func(r *cephmsgr.OSDRequest) {}, read)
			if err == nil {
				t.Fatalf("osd.%d answered a read without a replica-read flag", id)
			}
		}
		t.Logf("osd.%d replica=%v served both flagged reads", id, replica)
		connection.Close()
	}
	t.Run("misuse is refused before submission", func(t *testing.T) {
		o := mutationConnection(t, c, ctx, primaryRoute)
		defer o.Close()
		w := pools.request(primaryRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 1, Data: []byte{1}})
		w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000900}
		w.BalanceReads = true
		if reply, err := o.Request(ctx, w); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
			t.Fatal("a write carried a replica-read flag", err)
		}
	})
}
