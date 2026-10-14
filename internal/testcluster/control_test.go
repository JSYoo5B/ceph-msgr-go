package testcluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestControlRequestIsHostReadableAndAcknowledged(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- ControlDaemon(ctx, dir, "stop", "mgr", "b") }()
	for {
		path := filepath.Join(dir, "stop-mgr")
		data, err := os.ReadFile(path)
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) != 2 || fields[1] != "b" {
				t.Fatal("partially published request", string(data))
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "windows" && info.Mode().Perm() != 0644 {
				t.Fatal("container-created control must be readable by non-root host", info.Mode())
			}
			if err := os.WriteFile(filepath.Join(dir, "mgr-stop."+fields[0]), nil, 0600); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestControllerFailureReachesWaitingTest(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"provider.error", "native-oracles.error"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("control permission denied\n"), 0600); err != nil {
			t.Fatal(err)
		}
		err := RequestControl(context.Background(), dir, "restart-mon", "mon-restarted", "a")
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("fixture %s: control permission denied", name)) {
			t.Fatal("worker failure became an unbounded wait", err)
		}
		os.Remove(filepath.Join(dir, name))
	}
}
