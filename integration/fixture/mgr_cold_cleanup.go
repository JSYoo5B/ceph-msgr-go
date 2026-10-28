package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type coldBootstrapOwnership struct {
	Containers map[string]string `json:"containers"`
	Network    string            `json:"network"`
}

func readColdBootstrapOwnership(data []byte) (coldBootstrapOwnership, error) {
	var owned coldBootstrapOwnership
	if len(data) > 1<<20 || json.Unmarshal(data, &owned) != nil || len(owned.Containers) == 0 || len(owned.Containers) > 32 || owned.Network == "" {
		return owned, errors.New("cold-bootstrap cleanup has no bounded ownership inventory")
	}
	ids := map[string]bool{}
	for name, id := range owned.Containers {
		if name == "" || id == "" || ids[id] {
			return owned, errors.New("cold-bootstrap cleanup ownership is missing or repeated")
		}
		ids[id] = true
	}
	return owned, nil
}

// Only an explicit Docker not-found result confirms absence. A still-present
// resource or an unavailable inspector cannot certify cleanup.
func checkColdBootstrapAbsence(owned coldBootstrapOwnership, inspect func(string, string) error) error {
	var names []string
	for name := range owned.Containers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := inspect("container", owned.Containers[name]); !errdefs.IsNotFound(err) {
			return fmt.Errorf("cold-bootstrap owned container %s absence unconfirmed: %w", name, errors.Join(err, errors.New("expected not-found")))
		}
	}
	if err := inspect("network", owned.Network); !errdefs.IsNotFound(err) {
		return fmt.Errorf("cold-bootstrap owned network absence unconfirmed: %w", errors.Join(err, errors.New("expected not-found")))
	}
	return nil
}

func confirmColdBootstrapCleanup(ctx context.Context, out string) error {
	file, err := os.Open(filepath.Join(out, "fixture-ownership.json"))
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	owned, err := readColdBootstrapOwnership(data)
	if err != nil {
		return err
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	err = checkColdBootstrapAbsence(owned, func(kind, name string) error {
		if kind == "container" {
			_, err := docker.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
			return err
		}
		_, err := docker.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
		return err
	})
	if err == nil {
		fmt.Printf("MGR_COLD_BOOTSTRAP_CLEANUP independent inspection confirms containers=%d network=removed\n", len(owned.Containers))
	}
	return err
}
