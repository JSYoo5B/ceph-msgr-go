package main

import (
	"context"
	"testing"
)

func TestCacheOpGoControlIsClosed(t *testing.T) {
	for _, fields := range [][]string{
		{"1", "token", "go-ops", "prepare"},
		{"2", "token", "go-ops", "observe", "flushed"},
		{"2", "token", "go-ops", "observe", "lost-flushed"},
		{"2", "token", "go-ops", "observe", "lost-flush-settled"},
		{"3", "token", "go-ops", "finish"},
	} {
		if _, _, ok := cacheOpGoControl(fields); !ok {
			t.Fatal("closed Go cache-op control refused", fields)
		}
	}
	for _, fields := range [][]string{
		{"1", "token", "go-ops"},
		{"1", "token", "go-ops", "observe"},
		{"1", "token", "go-ops", "observe", "foreign"},
		{"1", "token", "go-ops", "prepare", "flushed"},
		{"1", "token", "go-ops", "cache-flush"},
		{"1", "token", "go-ops", "observe", "lost-flush"},
		{"1", "token", "go-ops", "lost-flushed"},
		{"1", "token", "go-ops", "finish", "extra"},
	} {
		if _, _, ok := cacheOpGoControl(fields); ok {
			t.Fatal("foreign Go cache-op control admitted", fields)
		}
	}
}

func TestCacheOpGoSelectorReusesExclusiveCachePoolProfile(t *testing.T) {
	config := map[string]string{"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_RUN": cacheOpGoSelector}
	p, err := loadProfile(func(k string) string { return config[k] })
	if err != nil || p.testRun != cacheOpGoSelector || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_CACHE_POOLS"] != "1" {
		t.Fatal("Go cache-op selector did not reach the cache-pool fixture", p.testRun, err)
	}
	config["CEPH_MSGR_TEST_RUN"] = "^TestCephOSD(CacheOpGo|CacheTierOps)Integration$"
	if _, err := loadProfile(func(k string) string { return config[k] }); err == nil {
		t.Fatal("non-exclusive cache selector accepted")
	}
}

func TestCacheOpGoControlNeedsPreparedResources(t *testing.T) {
	x := &osdCachePoolScope{Out: t.TempDir(), Plan: ignoreCacheProbePlan{Token: "owned"}, LastID: 9}
	c := &fixtureCluster{profile: profile{osdCachePools: "1"}, cachePools: x}
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"10", "owned", cacheOpGoAction, "observe", "foreign"}); err == nil || x.LastID != 9 {
		t.Fatal("foreign Go cache-op label accepted", err)
	}
	if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{"10", "owned", cacheOpGoAction, "prepare"}); err == nil || len(x.Steps) != 0 {
		t.Fatal("Go cache-op observer ran without prepared resources", err)
	}
}
