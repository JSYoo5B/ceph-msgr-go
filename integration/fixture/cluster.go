package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const fixtureStartupTimeout = 180 * time.Second

// startCluster delegates container/network ownership and cleanup to the released
// fixture module. One owned container runs three native MONs and zero or two
// MGRs, plus the optional OSD, through our protocol-profile bootstrap. The
// module's topology helpers do not own or manage these native processes; their
// control and oracle protocol remains in cluster.sh.
//
// A non-nil result must be terminated even when startup fails. The caller owns
// out and must retain it until the cluster has been terminated: the writable
// bind mount carries live control requests, acknowledgements, and oracle files.
func startCluster(ctx context.Context, p profile, out string) (*ceph.Container, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := filepath.Abs(out)
	if err != nil {
		return nil, fmt.Errorf("resolve fixture output directory: %w", err)
	}
	info, err := os.Stat(out)
	if err != nil {
		return nil, fmt.Errorf("stat fixture output directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("fixture output path must be a directory")
	}
	if p.runtime != "host" && p.runtime != "container" {
		return nil, fmt.Errorf("unsupported fixture runtime %q", p.runtime)
	}
	reserved := "33300,33301,33302,36800,36801"
	if p.osd == "1" {
		reserved += ",36900,36901,36902,36903,36904,36905,36906,36907,36908,36909,36910"
	}
	return ceph.Run(ctx, p.image,
		ceph.WithNoInitialManagers(),
		ceph.WithNoInitialOSDs(),
		ceph.WithStartupTimeout(fixtureStartupTimeout),
		testcontainers.WithEntrypoint("/bin/sh", "/out/provider-cluster.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithEnv(p.containerEnvironment()),
		testcontainers.WithLabels(map[string]string{"ceph-msgr-go.integration": "true"}),
		// Replace the module's MON ports rather than append to them. Also give
		// container runtime an explicit port: an empty request makes
		// Testcontainers automatically publish every port exposed by the image.
		// This unused relay binding stays loopback-only in container runtime.
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.ExposedPorts = []string{"40000/tcp"}
			return nil
		}),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.Mounts = append(host.Mounts, mount.Mount{Type: mount.TypeBind, Source: out, Target: "/out"})
			if host.Sysctls == nil {
				host.Sysctls = make(map[string]string)
			}
			host.Sysctls["net.ipv4.ip_local_reserved_ports"] = reserved
			host.PortBindings = network.PortMap{
				network.MustParsePort("40000/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "0"}},
			}
		}),
		// The ordinary WithWaitStrategy option has a 60-second outer deadline,
		// independent of the Exec strategy's timeout. Replace both here.
		testcontainers.WithWaitStrategyAndDeadline(fixtureStartupTimeout,
			wait.ForExec([]string{"test", "-f", "/out/ready"}).WithStartupTimeout(fixtureStartupTimeout)),
	)
}

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

func proxyEndpoint(ctx context.Context, c *ceph.Container) (string, error) {
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
func inspectImageMetadata(ctx context.Context, c *ceph.Container) (id, architecture string, err error) {
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

func nativeVersion(ctx context.Context, c *ceph.Container) (string, error) {
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
