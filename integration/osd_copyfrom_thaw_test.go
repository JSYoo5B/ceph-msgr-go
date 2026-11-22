package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A frozen replica is thawed in place only when the test's own resume ended
// its lease before the deadline. When the fixture ended the lease first, at
// its deadline, or no ended acknowledgement arrived by the deadline, the
// replica stays recorded as frozen, and its recovery waits for the lease's
// ended acknowledgement before it kills the replica, so no pause of that
// lease can run after the kill. The test plays the fixture on a real control
// directory.
func TestCopyFromThawSettlesOnTheFixtureEnd(t *testing.T) {
	for _, stage := range []string{"resumed", "expired first", "no acknowledgement"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			r := copyPrimaryRecovery{dir: dir, frozen: map[int32]bool{}, killed: map[int32]bool{}}
			parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// request waits for the one pending envelope of action and
			// returns its fields; the loop only yields to the publisher.
			request := func(action string) []string {
				t.Helper()
				for {
					paths, err := filepath.Glob(filepath.Join(dir, action+".*.request"))
					if err == nil && len(paths) == 1 {
						data, err := os.ReadFile(paths[0])
						if err != nil {
							t.Fatal(err)
						}
						if err := os.Remove(paths[0]); err != nil {
							t.Fatal(err)
						}
						return strings.Fields(string(data))
					}
					if err != nil || len(paths) > 1 || parent.Err() != nil {
						t.Fatalf("waiting for %s: %v %v %v", action, paths, err, parent.Err())
					}
					time.Sleep(time.Millisecond)
				}
			}
			record := func(fields []string, state, reason string) {
				t.Helper()
				data, _ := json.Marshal(testcluster.DaemonLeaseRecord{Lease: fields[1], Daemon: "osd.2", State: state, Reason: reason, PausedAt: 1, EndedAt: 2, Deadline: 3})
				if err := os.WriteFile(filepath.Join(dir, "daemon-lease-"+state+"."+fields[1]), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(300 * time.Millisecond)
			paused := make(chan []string, 1)
			go func() {
				pause := request("pause-daemon")
				record(pause, testcluster.DaemonLeasePaused, "")
				paused <- pause
			}()
			r.freeze(t, parent, 2, deadline)
			pause := <-paused
			if len(pause) != 5 || pause[2] != "osd" || pause[3] != "2" || pause[4] != strconv.FormatInt(deadline.UnixNano(), 10) {
				t.Fatal("pause request", pause)
			}
			thawed := make(chan error, 1)
			go func() { thawed <- r.thaw(2) }()
			end := request("end-daemon-lease")
			if end[1] != pause[1] || end[2] != "resume" {
				t.Fatal("the thaw does not end its lease", end, pause)
			}
			switch stage {
			case "resumed":
				record(end, testcluster.DaemonLeaseEnded, testcluster.DaemonLeaseResumed)
				if err := <-thawed; err != nil || r.frozen[2] {
					t.Fatal("a resumed replica was not thawed", err, r.frozen)
				}
				return
			case "expired first":
				record(end, testcluster.DaemonLeaseEnded, testcluster.DaemonLeaseExpired)
				var ended *testcluster.DaemonLeaseEndedError
				if err := <-thawed; !errors.As(err, &ended) || !r.frozen[2] {
					t.Fatal("a thaw after the fixture's expiry was accepted", err, r.frozen)
				}
			case "no acknowledgement":
				err := <-thawed
				if !errors.Is(err, context.DeadlineExceeded) || time.Now().Before(deadline) || time.Since(deadline) > time.Second || !r.frozen[2] {
					t.Fatalf("resume wait: err=%v time since the deadline=%v frozen=%v", err, time.Since(deadline), r.frozen)
				}
			}
			done := make(chan error, 1)
			go func() { done <- r.recoverFrozen(parent, 2) }()
			if stage == "no acknowledgement" {
				// The recovery ends the lease again and waits for the
				// fixture's ended acknowledgement before the kill.
				again := request("end-daemon-lease")
				if again[1] != pause[1] {
					t.Fatal("the recovery ended another lease", again)
				}
				select {
				case err := <-done:
					t.Fatal("the recovery killed the replica before its lease ended", err)
				default:
				}
				record(again, testcluster.DaemonLeaseEnded, testcluster.DaemonLeaseResumed)
			}
			kill := request("kill-osd")
			if err := os.WriteFile(filepath.Join(dir, "osd-kill."+kill[0]), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil || r.frozen[2] || !r.killed[2] {
				t.Fatal("uncertain replica was not scheduled for restart", err, r.frozen, r.killed)
			}
		})
	}
}
