package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Three objects of one PG get a data error injected on a replica and a deep
// scrub lists them. SCRUBLS pages of one value each, every request starting
// after the object the previous page returned, together list each object
// exactly once, matching the native list-inconsistent-obj names.
func TestCephOSDScrubListPagingIntegration(t *testing.T) {
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
	first := "scrub-page-0"
	route := poolPlace(t, ctx, 1, first, "")
	if len(route.Acting) != 3 {
		t.Fatalf("expected three replicas: %+v", route)
	}
	objects := []string{first}
	for i := 1; len(objects) < 3; i++ {
		if i == 400 {
			t.Fatal("no further object name maps to the subject's PG")
		}
		name := fmt.Sprintf("scrub-page-%d", i)
		if poolPlace(t, ctx, 1, name, "").PGID == route.PGID {
			objects = append(objects, name)
		}
	}
	for _, name := range objects {
		poolSeed(t, ctx, 1, name, "", bytes.Repeat([]byte(name), 100), nil)
	}
	var replica int32 = -1
	for _, id := range route.Acting {
		if id != route.ID {
			replica = id
			break
		}
	}
	r, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: replica, Address: poolPlaceOn(t, ctx, 1, first, "", &replica).Address})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
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
	for _, name := range objects {
		inject, _ := json.Marshal(map[string]string{"prefix": "injectdataerr", "pool": "ceph-msgr-owned-pool-1", "objname": name})
		if result, err := r.Command(ctx, cephmsgr.Command{JSON: inject}); err != nil || result.Code != 0 {
			t.Fatal("inject a data error on the replica", name, result.Code, err)
		}
	}
	scrub, _ := json.Marshal(map[string]string{"prefix": "pg deep-scrub", "pgid": route.PGID})
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: scrub}); err != nil {
		t.Fatal("deep scrub", err)
	}

	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	target := cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.PG, Primary: route.ID}
	list := func(interval uint32, after cephmsgr.OSDScrubObject, max uint64) (cephmsgr.OSDInconsistentReply, error) {
		return o.ListInconsistent(ctx, cephmsgr.OSDInconsistentRequest{MapEpoch: route.Epoch, PG: target, Interval: interval, StartAfter: after, MaxReturn: max, MaxOutputBytes: 64 << 10})
	}
	var server *cephmsgr.OSDError
	var all cephmsgr.OSDInconsistentReply
	for deadline := time.Now().Add(150 * time.Second); ; {
		all, err = list(0, cephmsgr.OSDScrubObject{}, 16)
		if err == nil && len(all.Values) == len(objects) {
			break
		}
		if err != nil && (!errors.As(err, &server) || server.Code != -2) {
			t.Fatal("listing while the scrub runs", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the deep scrub never reported every injected error", len(all.Values), err)
		}
		time.Sleep(3 * time.Second)
	}
	nameOf := func(value []byte) string {
		var match string
		for _, name := range objects {
			// Names share a prefix; the longest contained name is the object.
			if bytes.Contains(value, []byte(name)) && len(name) > len(match) {
				match = name
			}
		}
		return match
	}
	var order []string
	for _, value := range all.Values {
		order = append(order, nameOf(value))
	}

	var paged []string
	after := cephmsgr.OSDScrubObject{}
	for page := 0; ; page++ {
		if page > len(objects) {
			t.Fatal("paging did not end", paged)
		}
		reply, err := list(all.Interval, after, 1)
		if err != nil {
			t.Fatal("page", page, err)
		}
		if len(reply.Values) == 0 {
			break
		}
		if len(reply.Values) != 1 || reply.Interval != all.Interval {
			t.Fatalf("page %d: %d values, interval %d", page, len(reply.Values), reply.Interval)
		}
		name := nameOf(reply.Values[0])
		paged = append(paged, name)
		after = cephmsgr.OSDScrubObject{Name: name, Snap: ^uint64(1)} // CEPH_NOSNAP, the head
	}
	n := nativePool(t, ctx, "inconsistent", map[string]any{"pgid": route.PGID})
	sortedNative := slices.Clone(n.ScrubObjects.Names)
	slices.Sort(sortedNative)
	sortedPaged := slices.Clone(paged)
	slices.Sort(sortedPaged)
	want := slices.Clone(objects)
	slices.Sort(want)
	if !slices.Equal(paged, order) || !slices.Equal(sortedPaged, want) || !slices.Equal(sortedNative, want) {
		t.Fatalf("paged %v, one listing %v, native %v, injected %v", paged, order, n.ScrubObjects.Names, objects)
	}
	t.Logf("PG %s: %d single-value pages in listing order %v (interval %d)", route.PGID, len(paged), paged, all.Interval)
}
