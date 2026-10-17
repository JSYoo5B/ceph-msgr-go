package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Client.Statfs sends the MStatfs that librados cluster_stat and libcephfs
// statfs send. The independent native client asks for the same figures.
func TestCephMonStatfsIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	for _, name := range []string{"statfs-a", "statfs-b", "statfs-c"} {
		poolSeed(t, ctx, 1, name, "", []byte("statfs payload "+name), nil)
	}
	agree := func() (cephmsgr.StatfsResult, poolReceipt) {
		deadline := time.Now().Add(120 * time.Second)
		for {
			goStat, err := c.Statfs(ctx, nil)
			if err != nil {
				t.Fatal("cluster statfs", err)
			}
			native := nativePool(t, ctx, "clusterstat", nil)
			if goStat.KB == native.KB && goStat.KBUsed == native.KBUsed && goStat.KBAvail == native.KBAvail && goStat.Objects == native.NumObjects {
				return goStat, native
			}
			if time.Now().After(deadline) {
				t.Fatalf("Go and native statfs never agreed: go=%+v native=%+v", goStat, native)
			}
			time.Sleep(2 * time.Second)
		}
	}
	t.Run("cluster figures equal the native client's", func(t *testing.T) {
		goStat, native := agree()
		if goStat.Version == 0 || goStat.KB == 0 || goStat.KBUsed > goStat.KB || goStat.KBAvail > goStat.KB || goStat.Objects == 0 {
			t.Fatalf("implausible cluster statfs %+v / %+v", goStat, native)
		}
		t.Logf("cluster statfs: %+v", goStat)
	})
	t.Run("pool figures", func(t *testing.T) {
		cluster, _ := agree()
		for _, id := range []int64{base.Pool, pools.ids[1]} {
			id := id
			deadline := time.Now().Add(120 * time.Second)
			for {
				pool, err := c.Statfs(ctx, &id)
				if err != nil {
					t.Fatal("pool statfs", id, err)
				}
				if pool.Objects <= cluster.Objects && pool.KBAvail <= cluster.KBAvail*4 && pool.Version >= cluster.Version {
					if id == pools.ids[1] && pool.Objects < 3 {
						// The three seeded objects reach the stat service after its next report.
						if time.Now().After(deadline) {
							t.Fatalf("pool %d never reported its three objects: %+v", id, pool)
						}
						time.Sleep(2 * time.Second)
						continue
					}
					t.Logf("pool %d statfs: %+v", id, pool)
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("pool %d statfs inconsistent with the cluster: %+v vs %+v", id, pool, cluster)
				}
				time.Sleep(2 * time.Second)
			}
		}
	})
	t.Run("an unknown pool is ignored by the MON", func(t *testing.T) {
		absent := int64(987654)
		waitCtx, stop := context.WithTimeout(ctx, 4*time.Second)
		defer stop()
		_, err := c.Statfs(waitCtx, &absent)
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("a request the MON ignores must end as an unknown outcome", err)
		}
		if _, err := c.Statfs(ctx, nil); err != nil {
			t.Fatal("the session did not survive the ignored request", err)
		}
	})
	// An identity whose only MON grant is the fsid command lacks "pg" read: the
	// MON drops the request without a reply, and the session stays usable.
	t.Run("a capability-denied request is ignored by the MON", func(t *testing.T) {
		options := integrationOptions(t)
		encoded, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "configwatch.key"))
		if err != nil {
			t.Fatal(err)
		}
		if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
			t.Fatal(err)
		}
		options.Identity = "client.configwatch"
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		restricted, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer restricted.Close()
		for _, pool := range []*int64{nil, &base.Pool} {
			waitCtx, stop := context.WithTimeout(ctx, 4*time.Second)
			_, err := restricted.Statfs(waitCtx, pool)
			stop()
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &unknown) {
				t.Fatal("a capability-denied statfs must end as an unknown outcome", pool, err)
			}
		}
		result, err := restricted.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"fsid","format":"json"}`)})
		if err != nil || result.Code != 0 {
			t.Fatal("the restricted session did not survive the ignored requests", result.Code, err)
		}
		// Control: the admin identity gets the figures on the same MON.
		if _, err := c.Statfs(ctx, nil); err != nil {
			t.Fatal(err)
		}
	})
}
