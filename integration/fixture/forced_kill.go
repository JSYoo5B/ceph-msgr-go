package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// forcedKillTimeout bounds the wait for Docker to report the killed OSD exited.
const forcedKillTimeout = 30 * time.Second

// dockerKill sends SIGKILL to one owned container through the fixture's own
// Docker client. ContainerKill delivers exactly the requested signal, unlike
// ContainerStop, which sends the image stop signal (SIGTERM) first.
func dockerKill(id string) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			return fmt.Errorf("create Docker kill client: %w", err)
		}
		defer func() { err = errors.Join(err, docker.Close()) }()
		_, err = docker.ContainerKill(ctx, id, client.ContainerKillOptions{Signal: "SIGKILL"})
		return err
	}
}

// forceKill sends SIGKILL once, waits until the container stopped and returns
// its state only for a non-OOM exit 137 receipt. The kill is never repeated.
func forceKill(parent context.Context, kill func(context.Context) error, state func(context.Context) (*container.State, error), timeout time.Duration) (*container.State, error) {
	if timeout <= 0 {
		return nil, errors.New("forced OSD kill requires a bounded wait")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := kill(ctx); err != nil {
		return nil, fmt.Errorf("SIGKILL owned OSD: %w", err)
	}
	for {
		current, err := state(ctx)
		if err != nil {
			return nil, err
		}
		if current != nil && !current.Running && !current.Restarting {
			if current.ExitCode != 137 || current.OOMKilled {
				return nil, fmt.Errorf("OSD forced crash did not produce a non-OOM SIGKILL receipt (exit %d, oom %t)", current.ExitCode, current.OOMKilled)
			}
			return current, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("OSD forced crash did not stop after SIGKILL: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
