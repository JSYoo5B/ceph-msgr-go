package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
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
		if err := pullFixtureImage(ctx, image, func(ctx context.Context) error {
			return provider.PullImage(ctx, image)
		}, waitImageFetch, os.Stderr); err != nil {
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

// registryFetchStatus intentionally recognizes only the registry status text
// returned by this pinned Testcontainers/Moby stack. Moby v0.6.2's httpError is
// private and exposes an errdefs class, not the original registry status code.
// IsInternal alone is therefore insufficient: it also describes permanent
// daemon failures. Testcontainers v0.44.0's attemptToPullImage treats that class
// as permanent and does not retry JSON stream errors returned by io.Copy.
func registryFetchStatus(err error) int {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errdefs.IsNotFound(err) || errdefs.IsInvalidArgument(err) || errdefs.IsUnauthorized(err) ||
		errdefs.IsPermissionDenied(err) || errdefs.IsNotImplemented(err) {
		return 0
	}
	const prefix = "received unexpected HTTP status: "
	message := err.Error()
	i := strings.Index(message, prefix)
	if i < 0 {
		return 0
	}
	status := message[i+len(prefix):]
	if len(status) < 3 || len(status) > 3 && status[3] != ' ' && status[3] != '\n' && status[3] != '\r' {
		return 0
	}
	switch status[:3] {
	case "502":
		return 502
	case "503":
		return 503
	case "504":
		return 504
	default:
		return 0
	}
}

func waitImageFetch(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// pullFixtureImage retries only initial image fetches, always through the same
// provider callback and exact reference. It never changes tag/digest/platform
// or retries daemon startup, authentication, native commands or object I/O.
// Testcontainers can internally retry some Docker API errors; this helper bounds
// its own provider calls, while the parent context bounds the entire operation.
func pullFixtureImage(ctx context.Context, image string, pull func(context.Context) error,
	wait func(context.Context, time.Duration) error, diagnostic io.Writer,
) error {
	const maximumAttempts = 3
	var failures []error
	attempts := 0
	failed := func(extra error) error {
		if extra != nil {
			failures = append(failures, extra)
		}
		return fmt.Errorf("fetch fixture image %q failed after %d attempts: %w", image, attempts, errors.Join(failures...))
	}
	for attempt := 1; attempt <= maximumAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if attempts == 0 {
				return err
			}
			return failed(err)
		}
		attempts = attempt
		err := pull(ctx)
		if err == nil {
			if err := ctx.Err(); err != nil {
				return failed(err)
			}
			if attempt > 1 {
				fmt.Fprintf(diagnostic, "fixture image fetch recovered after %d provider attempts for %q; same reference retained\n", attempt, image)
			}
			return nil
		}
		failures = append(failures, fmt.Errorf("image fetch attempt %d: %w", attempt, err))
		status := registryFetchStatus(err)
		if status == 0 || attempt == maximumAttempts {
			return failed(nil)
		}
		if err := ctx.Err(); err != nil {
			return failed(err)
		}
		if attempt == 1 {
			// Quote and truncate the first error. Do not dump response headers,
			// credential/config objects or unbounded registry HTML into CI logs.
			message := err.Error()
			if len(message) > 256 {
				message = message[:256] + "..."
			}
			fmt.Fprintf(diagnostic, "fixture image fetch attempt 1 for %q failed with registry HTTP %d: %q; retrying same reference (maximum %d provider attempts)\n", image, status, message, maximumAttempts)
		}
		if err := wait(ctx, time.Duration(attempt)*time.Second); err != nil {
			return failed(err)
		}
	}
	panic("unreachable fixture image fetch attempt")
}

func proxyEndpoint(ctx context.Context, c *fixtureCluster) (string, error) {
	port, err := c.MappedPort(ctx, relayContainerPort)
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
