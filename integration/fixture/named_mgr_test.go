package main

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestNamedManagerOracleRejectsUnboundedSelectorsBeforeNativeWork(t *testing.T) {
	for _, fields := range [][]string{nil, {"1"}, {"1", "anything"}, {"1", "before", "extra"}} {
		c := fixtureCluster{}
		c.profile.mgrCount = "2"
		if err := c.namedManagerOracle(context.Background(), t.TempDir(), fields); err == nil {
			t.Fatal("accepted arbitrary control selector", fields)
		}
	}
	c := fixtureCluster{}
	c.profile.mgrCount = "0"
	if err := c.namedManagerOracle(context.Background(), t.TempDir(), []string{"1", "before"}); err == nil {
		t.Fatal("accepted absent MGR fixture")
	}
	c.profile.mgrCount = "2"
	if err := c.namedManagerOracle(context.Background(), t.TempDir(), []string{"1", "after"}); err == nil {
		t.Fatal("accepted after observation without baseline")
	}
}

func TestNamedManagerOracleAfterRequiresNewAvailableIdentity(t *testing.T) {
	initial := managerPreparationMap{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 100}, Epoch: 2}
	baseline := namedManagerOracleReceipt{Phase: "before", Before: initial, After: initial}
	if !namedManagerOracleReady(initial, nil) || namedManagerOracleReady(initial, &baseline) {
		t.Fatal("initial identity accepted as replacement")
	}
	for _, current := range []managerPreparationMap{
		{ManagerStatus: ceph.ManagerStatus{Available: false, ActiveName: "b", ActiveGID: 101}, Epoch: 3},
		{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 101}, Epoch: 3},
		{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: "b", ActiveGID: 100}, Epoch: 3},
		{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: "b", ActiveGID: 101}, Epoch: 2},
	} {
		if namedManagerOracleReady(current, &baseline) {
			t.Fatal("accepted stale or incomplete replacement", current)
		}
	}
	current := managerPreparationMap{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: "b", ActiveGID: 101}, Epoch: 3}
	if !namedManagerOracleReady(current, &baseline) {
		t.Fatal("rejected available replacement")
	}
}
