package testcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// RestartMonitor requests a fixture MON restart and waits for its acknowledgement.
func RestartMonitor(ctx context.Context, control, name string) error {
	id := time.Now().UnixNano()
	file, err := os.CreateTemp(control, "restart-mon-request-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := fmt.Fprintf(file, "%d %s\n", id, name); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, "restart-mon")); err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("mon-restarted.%d", id))
	defer os.Remove(ack)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(ack); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ControlDaemon requests a fixture daemon action and waits for its acknowledgement.
func ControlDaemon(ctx context.Context, control, action, daemon, name string) error {
	id := time.Now().UnixNano()
	file, err := os.CreateTemp(control, action+"-"+daemon+"-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := fmt.Fprintf(file, "%d %s\n", id, name); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, action+"-"+daemon)); err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("%s-%s.%d", daemon, action, id))
	defer os.Remove(ack)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(ack); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
