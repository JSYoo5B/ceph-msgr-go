package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// On a k=2,m=1 pool: PG listing addresses the primary shard; a read through a
// degraded PG (one non-primary shard's OSD killed) reconstructs the bytes; after
// the primary is killed the new primary holds a different shard, and only a
// request carrying that new shard is answered.
func TestCephOSDErasureFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("k=2,m=1 with overwrites needs three BlueStore OSDs")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true})
	if created.PoolType != 3 {
		t.Fatal("pool is not erasure-coded")
	}
	payloads := map[string][]byte{}
	var routes = map[string]osdFixtureOracle{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("ec-object-%d", i)
		data := bytes.Repeat([]byte{byte(i), 0xff, 0, byte(0x40 + i)}, 2048+i)
		route := poolPlace(t, ctx, 1, name, "")
		o := mutationConnection(t, c, ctx, route)
		r := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003000}
		if _, err := o.Request(ctx, r); err != nil {
			t.Fatal("seed", name, err)
		}
		o.Close()
		payloads[name], routes[name] = data, route
	}
	subject := "ec-object-0"
	route := routes[subject]

	t.Run("PG listing of the primary shard", func(t *testing.T) {
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		target := cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.PG, Primary: route.ID}
		if !hash {
			target.Seed = route.Hash
		} else {
			target.Erasure, target.Shard = true, uint8(route.Shard)
		}
		reply, err := o.ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: route.Epoch, PG: target, Namespace: "", Count: 64, MaxOutputBytes: 64 << 10})
		if err != nil {
			t.Fatal("list", err)
		}
		listed := map[string]bool{}
		for _, e := range reply.Entries {
			listed[e.Object] = true
		}
		for name, r := range routes {
			if (r.PGID == route.PGID) != listed[name] {
				t.Fatalf("listing of %s disagrees with native placement: listed=%v pg=%s vs %s", name, listed[name], r.PGID, route.PGID)
			}
		}
		t.Logf("PG %s listed %d objects, end=%v", route.PGID, len(reply.Entries), reply.End)
	})

	kill := func(t *testing.T, id int32) func() {
		t.Helper()
		name := strconv.FormatInt(int64(id), 10)
		if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
			t.Fatal("kill", id, err)
		}
		var state struct {
			ExitCode *int `json:"exit_code"`
		}
		if data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json")); err != nil || json.Unmarshal(data, &state) != nil || state.ExitCode == nil || *state.ExitCode != 137 {
			t.Fatal("expected SIGKILL evidence", err)
		}
		// The fixture object was not changed, so restart, wait for clean PGs and
		// republish its routing without the legacy-only object restoration.
		return func() {
			recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
			defer stop()
			if err := testcluster.ControlDaemon(recover, dir, "start", "osd", name); err != nil {
				t.Error("restart killed OSD", err)
				return
			}
			waitOSDFixtureClean(t, recover, base)
			fresh := nativeMutation(t, recover, map[string]any{"compare": false, "placement": true}).Placement
			publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
		}
	}
	read := func(t *testing.T, r osdFixtureOracle, edit func(*cephmsgr.OSDRequest), timeout time.Duration) ([]byte, error) {
		t.Helper()
		o := mutationConnection(t, c, ctx, r)
		defer o.Close()
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(payloads[subject]) + 64)})
		if edit != nil {
			edit(&req)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		reply, err := o.Request(callCtx, req)
		if err != nil {
			return nil, err
		}
		return reply.Operations[0].Data, nil
	}

	t.Run("degraded read reconstructs the object", func(t *testing.T) {
		var victim int32 = -1
		for _, id := range route.Acting {
			if id != route.ID {
				victim = id
				break
			}
		}
		restore := kill(t, victim)
		defer restore()
		// Read only once the map has dropped the victim from the acting set, so
		// the primary must reconstruct the missing shard from the others.
		var degraded osdFixtureOracle
		deadline := time.Now().Add(120 * time.Second)
		for {
			degraded = poolPlace(t, ctx, 1, subject, "")
			if degraded.Acting[route.Shard] == route.ID && !slicesContains(degraded.Acting, victim) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the map never marked the victim down", degraded.Acting)
			}
			time.Sleep(2 * time.Second)
		}
		data, err := read(t, degraded, nil, 60*time.Second)
		if err != nil || !bytes.Equal(data, payloads[subject]) {
			t.Fatal("degraded read", err)
		}
		if n := poolObserve(t, ctx, 1, subject, "", ""); !bytes.Equal(n.Data, payloads[subject]) {
			t.Fatal("native degraded read differs")
		}
		t.Logf("read with osd.%d down: primary osd.%d shard %d acting %v", victim, degraded.ID, degraded.Shard, degraded.Acting)
	})

	t.Run("primary failover changes the shard", func(t *testing.T) {
		old := poolPlace(t, ctx, 1, subject, "")
		restore := kill(t, old.ID)
		defer restore()
		var fresh osdFixtureOracle
		deadline := time.Now().Add(120 * time.Second)
		for {
			fresh = poolPlace(t, ctx, 1, subject, "")
			if fresh.ID != old.ID && fresh.ID >= 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no new primary was chosen")
			}
			time.Sleep(2 * time.Second)
		}
		t.Logf("primary osd.%d shard %d -> osd.%d shard %d (acting %v -> %v)", old.ID, old.Shard, fresh.ID, fresh.Shard, old.Acting, fresh.Acting)
		data, err := read(t, fresh, nil, 60*time.Second)
		if err != nil || !bytes.Equal(data, payloads[subject]) {
			t.Fatal("read through the new primary", err)
		}
		if hash && fresh.Shard != old.Shard {
			if _, err := read(t, fresh, func(r *cephmsgr.OSDRequest) { r.Placement.Shard = uint8(old.Shard) }, 5*time.Second); err == nil {
				t.Fatal("the new primary answered a request carrying the old shard")
			}
		}
	})
}

func slicesContains(ids []int32, id int32) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
