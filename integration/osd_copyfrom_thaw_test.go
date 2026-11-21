package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A missing resume acknowledgement must release the cell at its pause
// deadline. Recovery uses a separate request even if the controller already
// consumed the original and its acknowledgement arrives during cleanup.
func TestCopyFromThawAcknowledgementDeadline(t *testing.T) {
	for _, stage := range []string{"resume pending", "late acknowledgement", "lost acknowledgement"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			r := copyPrimaryRecovery{dir: dir, frozen: map[int32]bool{}, killed: map[int32]bool{}}
			parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := func(action string) string {
				t.Helper()
				path := filepath.Join(dir, action)
				for {
					data, err := os.ReadFile(path)
					if err == nil {
						fields := strings.Fields(string(data))
						if len(fields) != 2 || fields[1] != "2" {
							t.Fatalf("unexpected %s request: %q", action, data)
						}
						return fields[0]
					}
					if !errors.Is(err, os.ErrNotExist) || parent.Err() != nil {
						t.Fatalf("waiting for %s: %v, %v", action, err, parent.Err())
					}
					time.Sleep(time.Millisecond)
				}
			}
			ack := func(name string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(300 * time.Millisecond)
			// The pause is acknowledged once published; freeze waits for it.
			pauseID := make(chan string, 1)
			go func() {
				for parent.Err() == nil {
					if data, err := os.ReadFile(filepath.Join(dir, "pause-osd")); err == nil {
						if fields := strings.Fields(string(data)); len(fields) == 2 && os.WriteFile(filepath.Join(dir, "osd-pause."+fields[0]), nil, 0600) == nil {
							pauseID <- fields[0]
							return
						}
					}
					time.Sleep(time.Millisecond)
				}
				close(pauseID)
			}()
			r.freeze(t, parent, 2, deadline, 0)
			if id, ok := <-pauseID; !ok || id == "" {
				t.Fatal("the pause was not published")
			}
			err := r.thaw(2)
			if !errors.Is(err, context.DeadlineExceeded) || time.Now().Before(deadline) ||
				time.Since(deadline) > time.Second || parent.Err() != nil || !r.frozen[2] {
				t.Fatalf("resume wait: err=%v parent=%v time since cutoff=%v frozen=%v", err, parent.Err(), time.Since(deadline), r.frozen)
			}
			resumeID := request("resume-osd")
			if stage != "resume pending" {
				if err := os.Remove(filepath.Join(dir, "resume-osd")); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { done <- r.recoverFrozen(parent, 2) }()
			recoveryID := request("recover-paused-osd")
			switch stage {
			case "resume pending":
				if got := request("resume-osd"); got != resumeID {
					t.Fatalf("recovery replaced the pending resume %s with %s", resumeID, got)
				}
			case "late acknowledgement":
				ack("osd-resume." + resumeID)
			}
			ack("osd-pause-recovered." + recoveryID)
			ack("osd-kill." + request("kill-osd"))
			if err := <-done; err != nil || r.frozen[2] || !r.killed[2] {
				t.Fatal("uncertain replica was not scheduled for restart", err, r.frozen, r.killed)
			}
			if stage != "resume pending" {
				if _, err := os.Stat(filepath.Join(dir, "resume-osd")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("cleanup published a second ordinary resume", err)
				}
			}
		})
	}
}
