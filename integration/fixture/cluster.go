package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const fixtureStartupTimeout = 180 * time.Second

// imageArchitecture uses the same provider and registry credentials as
// Testcontainers. Inspect before pulling so a caller's local development image
// need not exist in a registry. The caller bounds pull and inspection with ctx.
func imageArchitecture(ctx context.Context, image string) (architecture string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return "", fmt.Errorf("create Docker image provider: %w", err)
	}
	defer func() { err = errors.Join(err, provider.Close()) }()
	inspected, err := provider.Client().ImageInspect(ctx, image)
	if errdefs.IsNotFound(err) {
		if err := provider.PullImage(ctx, image); err != nil {
			return "", fmt.Errorf("pull fixture image: %w", err)
		}
		inspected, err = provider.Client().ImageInspect(ctx, image)
	}
	if err != nil {
		return "", fmt.Errorf("inspect fixture image: %w", err)
	}
	if inspected.Os != "linux" || inspected.Architecture == "" {
		return "", fmt.Errorf("fixture image must describe a Linux architecture, got %q/%q", inspected.Os, inspected.Architecture)
	}
	return inspected.Architecture, nil
}

func proxyEndpoint(ctx context.Context, c *fixtureCluster) (string, error) {
	port, err := c.MappedPort(ctx, "40000/tcp")
	if err != nil {
		return "", fmt.Errorf("resolve fixture relay port: %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve fixture relay host: %w", err)
	}
	// The relay is deliberately published only on the engine's IPv4 loopback.
	// Docker Desktop reports localhost; use the numeric binding for Windows
	// evidence and avoid selecting ::1 for an IPv4-only published socket.
	if host != "localhost" {
		address, err := netip.ParseAddr(host)
		if err != nil || !address.IsLoopback() {
			return "", fmt.Errorf("fixture relay requires a local Docker engine, got host %q", host)
		}
	}
	return net.JoinHostPort("127.0.0.1", port.Port()), nil
}

// inspectImageMetadata reports the exact image actually used by the created
// container, not a mutable tag looked up again after startup.
func inspectImageMetadata(ctx context.Context, c *fixtureCluster) (id, architecture string, err error) {
	created, err := c.Inspect(ctx)
	if err != nil {
		return "", "", fmt.Errorf("inspect fixture container image: %w", err)
	}
	if created.Image == "" {
		return "", "", errors.New("fixture container inspection has no image ID")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return "", "", fmt.Errorf("create Docker image inspector: %w", err)
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	inspected, err := docker.ImageInspect(ctx, created.Image)
	if err != nil {
		return "", "", fmt.Errorf("inspect running fixture image: %w", err)
	}
	if inspected.ID != created.Image || inspected.Os != "linux" || inspected.Architecture == "" {
		return "", "", errors.New("running fixture image metadata is incomplete or mismatched")
	}
	return inspected.ID, inspected.Architecture, nil
}

func nativeVersion(ctx context.Context, c *fixtureCluster) (string, error) {
	code, output, err := c.Exec(ctx, []string{"ceph", "--version"}, tcexec.Multiplexed())
	if err != nil {
		return "", fmt.Errorf("query native Ceph version: %w", err)
	}
	data, err := io.ReadAll(output)
	if err != nil {
		return "", fmt.Errorf("read native Ceph version: %w", err)
	}
	if code != 0 {
		return "", fmt.Errorf("native Ceph version exited %d: %s", code, strings.TrimSpace(string(data)))
	}
	version := strings.TrimSpace(string(data))
	if version == "" {
		return "", errors.New("native Ceph version response is empty")
	}
	return version, nil
}
