package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCachePoolLiveProfileIsExclusiveAndDistinctFromNativeProbe(t *testing.T) {
	defaults := map[string]string{"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}
	for _, runtime := range []string{"host", "container"} {
		config := map[string]string{}
		for k, v := range defaults {
			config[k] = v
		}
		config["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return config[k] })
		if err != nil || p.testRun != "^TestCephOSDIgnoreCacheIntegration$" || p.containerEnvironment()["CEPH_MSGR_TEST_OSD_CACHE_POOLS"] != "1" || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_CACHE_POOLS"] != "1" {
			t.Fatal("exclusive live selection/environment", p, err)
		}
		if validateIgnoreCacheProbeProfile(p) == nil {
			t.Fatal("native-only command accepted a live product profile")
		}
	}
	for name, value := range map[string]string{"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "yes", "CEPH_MSGR_TEST_MGR_COUNT": "0", "CEPH_MSGR_TEST_OSD": "0", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "memstore", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8", "CEPH_MSGR_TEST_MON_PING": "1", "CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_MON_FREEZE": "1", "CEPH_MSGR_TEST_AUTH_EPOCH": "1", "CEPH_MSGR_TEST_RUN": "^TestCeph.*Integration$"} {
		config := map[string]string{}
		for k, v := range defaults {
			config[k] = v
		}
		config[name] = value
		if _, err := loadProfile(func(k string) string { return config[k] }); err == nil {
			t.Fatal("mixed live cache profile accepted", name, value)
		}
	}
}

func TestCachePoolSharedCapsKeepNativeOnlyGrants(t *testing.T) {
	native := cachePoolClientCaps("owned", "rw", "scope", false)
	live := cachePoolClientCaps("owned", "rw", "scope", true)
	if native.Mon != "allow r" || native.MGR != "" || native.OSD != "allow rw pool=owned namespace=scope" || live.Mon != native.Mon || live.OSD != native.OSD || live.MGR != "allow r" {
		t.Fatal("live commands widened original native object grants", native, live)
	}
}

func TestCachePoolMutationPublishesIntentAndRetainsUncertainty(t *testing.T) {
	x := &osdCachePoolScope{Out: t.TempDir()}
	cause := errors.New("unknown native operation")
	called := false
	err := x.mutate("once", func() error {
		raw, err := os.ReadFile(filepath.Join(x.Out, "osd-cache-pools-fixture.json"))
		if err != nil || !strings.Contains(string(raw), `"state": "intended"`) {
			t.Fatal("operation ran before intended receipt", err)
		}
		called = true
		return cause
	})
	if !called || !errors.Is(err, cause) || len(x.Steps) != 1 || x.Steps[0].State != "unknown-or-failed" {
		t.Fatal("unknown operation lost", err, x.Steps)
	}
	x.FinishAttempted = true
	x.FinishError = cause
	c := &fixtureCluster{cachePools: x}
	if err := c.finishOSDCachePools(context.Background()); !errors.Is(err, cause) || len(x.Steps) != 1 {
		t.Fatal("cached uncertain cleanup was replayed", err)
	}
}

func TestCachePoolControlRefusesForeignScopeBeforeNativeExecution(t *testing.T) {
	x := &osdCachePoolScope{Out: t.TempDir(), Plan: ignoreCacheProbePlan{Token: "owned"}, LastID: 9}
	c := &fixtureCluster{profile: profile{osdCachePools: "1"}, cachePools: x}
	for _, fields := range [][]string{{"10", "foreign", "finish"}, {"9", "owned", "finish"}, {"10", "owned", "native", "arbitrary"}, {"10", "owned", "observe", "stat-normal", "extra"}, {"10", "owned", "finish", "extra"}} {
		if err := c.osdCachePoolsControl(context.Background(), x.Out, fields); err == nil || x.LastID != 9 || x.FinishAttempted || len(x.Steps) != 0 {
			t.Fatal("foreign control reached native execution", fields, err)
		}
	}
}
