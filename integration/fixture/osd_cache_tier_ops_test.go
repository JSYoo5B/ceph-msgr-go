package main

import (
	"context"
	"testing"
)

func TestCacheTierOpsSelectorReusesExclusiveCachePoolProfile(t *testing.T) {
	config := map[string]string{"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_RUN": cacheTierOpsSelector}
	p, err := loadProfile(func(k string) string { return config[k] })
	if err != nil || p.testRun != cacheTierOpsSelector || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_CACHE_POOLS"] != "1" {
		t.Fatal("tier-ops selector did not reach the cache-pool fixture", p.testRun, err)
	}
	for _, selector := range []string{"^TestCephOSD(IgnoreCache|CacheTierOps)Integration$", "^TestCephOSDCacheTierOpsIntegration", "TestCephOSDCacheTierOps"} {
		config["CEPH_MSGR_TEST_RUN"] = selector
		if _, err := loadProfile(func(k string) string { return config[k] }); err == nil {
			t.Fatal("non-exclusive cache selector accepted", selector)
		}
	}
	config["CEPH_MSGR_TEST_RUN"] = cacheTierOpsSelector
	config["CEPH_MSGR_TEST_OSD_CACHE_POOLS"] = "0"
	p, err = loadProfile(func(k string) string { return config[k] })
	if err == nil && p.osdCachePools != "0" {
		t.Fatal("tier-ops selector enabled cache pools implicitly")
	}
}

func TestCacheTierOpsControlIsClosedAndSelectedOnce(t *testing.T) {
	x := &osdCachePoolScope{Out: t.TempDir(), Plan: ignoreCacheProbePlan{Token: "owned"}, LastID: 9}
	c := &fixtureCluster{profile: profile{osdCachePools: "1"}, cachePools: x}
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"10", "owned", cacheTierOpsAction, "extra"}); err == nil || x.LastID != 9 || x.TierOps {
		t.Fatal("tier-ops accepted an argument", err)
	}
	// Without owned resources nothing is selected or executed.
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"10", "owned", cacheTierOpsAction}); err == nil || x.TierOps || len(x.Steps) != 0 {
		t.Fatal("tier-ops ran without prepared resources", err)
	}
	x.Resources = &cachePoolResources{}
	x.TierOps = true
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"11", "owned", cacheTierOpsAction}); err == nil || len(x.Steps) != 0 {
		t.Fatal("tier-ops checkpoint was reissued", err)
	}
	x.FinishAttempted = true
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"12", "owned", cacheTierOpsAction}); err == nil || x.LastID != 11 {
		t.Fatal("tier-ops accepted after finish", err)
	}
}
