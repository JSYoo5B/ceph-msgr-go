package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

// Only the container ID is needed by the real ownership writer. Other
// container operations are deliberately unavailable in this test substitute.
type managerOwnershipContainer struct {
	testcontainers.Container
	id string
}

func (c managerOwnershipContainer) GetContainerID() string { return c.id }

func TestPartialManagerCreationIsPersistedAndIndependentlyInspected(t *testing.T) {
	dir := t.TempDir()
	creationErr := errors.New("manager registration failed after container creation")
	owned := map[string]string{"mon.a": "monitor"}
	calls, persists := 0, 0
	err := createManagersWithOwnership(context.Background(), func(_ context.Context, name string) (*ceph.ManagerContainer, error) {
		calls++
		return &ceph.ManagerContainer{Container: managerOwnershipContainer{id: "late-owned"}, DaemonName: name}, creationErr
	}, func(created *ceph.ManagerContainer) error {
		persists++
		addManagerOwnership(owned, created)
		return writeFixtureOwnership(dir, "", "owned-network", owned)
	})
	if !errors.Is(err, creationErr) || calls != 1 || persists != 1 {
		t.Fatal("creation was retried or original error/inventory lost", err, calls, persists)
	}
	data, err := os.ReadFile(filepath.Join(dir, "fixture-ownership.json"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := readColdBootstrapOwnership(data)
	if err != nil || inventory.Containers["mgr.a"] != "late-owned" {
		t.Fatal("failed creation's actual ID missing from saved inventory", inventory, err)
	}
	for _, lateResult := range []error{nil, errors.New("late container inspection unavailable")} {
		seenLate := false
		err := checkColdBootstrapAbsence(inventory, func(kind, name string) error {
			if kind == "container" && name == "late-owned" {
				seenLate = true
				return lateResult
			}
			return errdefs.ErrNotFound
		})
		if err == nil || !seenLate {
			t.Fatal("partial creation was not inspected or falsely certified absent", err, seenLate)
		}
	}
	seenLate := false
	if err := checkColdBootstrapAbsence(inventory, func(kind, name string) error {
		if kind == "container" && name == "late-owned" {
			seenLate = true
		}
		return errdefs.ErrNotFound
	}); err != nil || !seenLate {
		t.Fatal("actual removed partial creation was not accepted", err, seenLate)
	}
}

func TestManagerCreationPreservesCreationAndInventoryErrors(t *testing.T) {
	creationErr, inventoryErr := errors.New("creation failed"), errors.New("inventory failed")
	calls, persists := 0, 0
	err := createManagersWithOwnership(context.Background(), func(context.Context, string) (*ceph.ManagerContainer, error) { calls++; return nil, creationErr }, func(*ceph.ManagerContainer) error { persists++; return inventoryErr })
	if !errors.Is(err, creationErr) || !errors.Is(err, inventoryErr) || calls != 1 || persists != 1 {
		t.Fatal(err, calls, persists)
	}
}

func TestManagerOwnershipSkipsDescriptorsWithoutContainers(t *testing.T) {
	owned := map[string]string{"mon.a": "monitor"}
	addManagerOwnership(owned, nil, &ceph.ManagerContainer{DaemonName: "a"}, &ceph.ManagerContainer{Container: managerOwnershipContainer{}, DaemonName: "b"})
	if len(owned) != 1 || owned["mon.a"] != "monitor" {
		t.Fatal("nil/empty container descriptor entered inventory", owned)
	}
	creationErr := errors.New("failed before container creation")
	err := createManagersWithOwnership(context.Background(), func(_ context.Context, name string) (*ceph.ManagerContainer, error) {
		return &ceph.ManagerContainer{DaemonName: name}, creationErr
	}, func(created *ceph.ManagerContainer) error { addManagerOwnership(owned, created); return nil })
	if !errors.Is(err, creationErr) || len(owned) != 1 {
		t.Fatal("nil container descriptor lost creation failure or corrupted inventory", err, owned)
	}
}
