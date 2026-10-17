package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// SCRUBLS reads the scrub errors a PG primary retained. Before any scrub the
// primary has none to report (-ENOENT, as native list-inconsistent-obj); a
// data error injected on one replica and found by a deep scrub is listed as
// one raw inconsistent-object value for that object, under the interval the
// native CLI reports; a stale interval is -EAGAIN.
func TestCephOSDScrubListIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("a replica data error needs three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	createOwnedPools(t, ctx, base, 1)
	const object = "scrub-subject"
	poolSeed(t, ctx, 1, object, "", bytes.Repeat([]byte("scrub"), 1000), nil)
	route := poolPlace(t, ctx, 1, object, "")
	if len(route.Acting) != 3 {
		t.Fatalf("expected three replicas: %+v", route)
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	// do_scrub_ls compares the request's PG with the PG's own id unfolded, so
	// the seed is the actual PG in every profile (PGNLS folds a raw hash).
	target := cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.PG, Primary: route.ID}
	list := func(interval uint32, snapsets bool) (cephmsgr.OSDInconsistentReply, error) {
		return o.ListInconsistent(ctx, cephmsgr.OSDInconsistentRequest{MapEpoch: route.Epoch, PG: target, Interval: interval, Snapsets: snapsets, MaxReturn: 16, MaxOutputBytes: 64 << 10})
	}
	native := func() poolReceipt { return nativePool(t, ctx, "inconsistent", map[string]any{"pgid": route.PGID}) }
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if _, err := list(0, false); !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -2 {
		t.Fatal("an unscrubbed PG must be a known -ENOENT", err)
	}
	if n := native(); n.ScrubObjects.Code != -2 {
		t.Fatal("native list before any scrub", n.ScrubObjects)
	}

	var replica int32 = -1
	for _, id := range route.Acting {
		if id != route.ID {
			replica = id
			break
		}
	}
	r, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: replica, Address: poolPlaceOn(t, ctx, 1, object, "", &replica).Address})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// BlueStore turns an injected data error into a read error only with
	// bluestore_debug_inject_read_err; it is set for the test and removed.
	mon := func(call context.Context, fields map[string]string) error {
		encoded, _ := json.Marshal(fields)
		_, err := c.MonCommand(call, cephmsgr.Command{JSON: encoded})
		return err
	}
	if err := mon(ctx, map[string]string{"prefix": "config set", "who": "osd", "name": "bluestore_debug_inject_read_err", "value": "true"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := mon(restore, map[string]string{"prefix": "config rm", "who": "osd", "name": "bluestore_debug_inject_read_err"}); err != nil {
			t.Error("restore", err)
		}
	})
	time.Sleep(2 * time.Second)
	inject, _ := json.Marshal(map[string]string{"prefix": "injectdataerr", "pool": "ceph-msgr-owned-pool-1", "objname": object})
	if result, err := r.Command(ctx, cephmsgr.Command{JSON: inject}); err != nil || result.Code != 0 {
		t.Fatal("inject a data error on the replica", result.Code, err)
	}
	scrub, _ := json.Marshal(map[string]string{"prefix": "pg deep-scrub", "pgid": route.PGID})
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: scrub}); err != nil {
		t.Fatal("deep scrub", err)
	}
	var found cephmsgr.OSDInconsistentReply
	deadline := time.Now().Add(150 * time.Second)
	for {
		found, err = list(0, false)
		if err == nil && len(found.Values) > 0 {
			break
		}
		if err != nil && (!errors.As(err, &server) || server.Code != -2) {
			t.Fatal("listing while the scrub runs", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the deep scrub never reported the injected error", err)
		}
		time.Sleep(3 * time.Second)
	}
	n := native()
	if len(found.Values) != 1 || !bytes.Contains(found.Values[0], []byte(object)) || n.ScrubObjects.Code != 0 || !slices.Equal(n.ScrubObjects.Names, []string{object}) || n.ScrubObjects.Interval != found.Interval {
		t.Fatalf("Go %d values (interval %d), native %+v", len(found.Values), found.Interval, n.ScrubObjects)
	}
	snapsets, err := list(found.Interval, true)
	if err != nil || len(snapsets.Values) != 0 || snapsets.Interval != found.Interval || n.ScrubSnapsets.Code != 0 || len(n.ScrubSnapsets.Names) != 0 {
		t.Fatal("snapset listing", len(snapsets.Values), err, n.ScrubSnapsets)
	}
	if _, err := list(found.Interval+1, false); !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -11 {
		t.Fatal("a stale interval must be a known -EAGAIN", err)
	}
	t.Logf("osd.%d data error listed by primary osd.%d: interval %d, %d raw bytes", replica, route.ID, found.Interval, len(found.Values[0]))
}
