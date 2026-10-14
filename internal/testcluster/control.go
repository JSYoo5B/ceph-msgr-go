package testcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RequestControl publishes non-secret control metadata atomically. The private
// fixture directory bounds access, while 0644 lets a non-root Linux host read
// files produced by the root container test process. Keys remain separate.
func RequestControl(ctx context.Context, control, request, response string, args ...string) error {
	id := time.Now().UnixNano()
	file, err := os.CreateTemp(control, request+"-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := fmt.Fprintf(file, "%d %s\n", id, strings.Join(args, " ")); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(0644); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, request)); err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("%s.%d", response, id))
	defer os.Remove(ack)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(ack); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Preserve the first controller/oracle error instead of hiding a dead
		// worker behind repeated per-test context deadlines.
		for _, name := range []string{"provider.error", "native-oracles.error"} {
			data, err := os.ReadFile(filepath.Join(control, name))
			if err == nil {
				return fmt.Errorf("fixture %s: %s", name, strings.TrimSpace(string(data)))
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func RestartMonitor(ctx context.Context, control, name string) error {
	return RequestControl(ctx, control, "restart-mon", "mon-restarted", name)
}

func ControlDaemon(ctx context.Context, control, action, daemon, name string) error {
	return RequestControl(ctx, control, action+"-"+daemon, daemon+"-"+action, name)
}
