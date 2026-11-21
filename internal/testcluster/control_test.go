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

// envelopes reads the pending envelopes of request in dir by request ID.
func envelopes(t *testing.T, dir, request string) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, request+".*.request"))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), request+"."), ".request")
		if fields := strings.Fields(string(data)); len(fields) == 0 || fields[0] != id {
			t.Fatalf("envelope %s holds %q", filepath.Base(path), data)
		}
		found[id] = string(data)
	}
	return found
}

func TestControlRequestIsHostReadableAndAcknowledged(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- ControlDaemon(ctx, dir, "stop", "mgr", "b") }()
	for {
		paths, _ := filepath.Glob(filepath.Join(dir, "stop-mgr.*.request"))
		if len(paths) == 1 {
			path := paths[0]
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
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
		// The worker fails while the request is waiting for its acknowledgement.
		go func() {
			time.Sleep(150 * time.Millisecond)
			_ = os.WriteFile(filepath.Join(dir, name), []byte("control permission denied\n"), 0600)
		}()
		err := RequestControl(context.Background(), dir, "restart-mon", "mon-restarted", "a")
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("fixture %s: control permission denied", name)) {
			t.Fatal("worker failure became an unbounded wait", err)
		}
		os.Remove(filepath.Join(dir, name))
	}
}

func TestControllerFailureRecordedBeforeRequestIsMarkedCascading(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "provider.error"), []byte("originating failure\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err := RequestControl(context.Background(), dir, "restart-mon", "mon-restarted", "a")
	if err == nil || !strings.Contains(err.Error(), "cascading failure") || !strings.Contains(err.Error(), "not executed") || !strings.Contains(err.Error(), "originating failure") {
		t.Fatal("later request did not identify the earlier controller failure", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatal("a request that cannot run was published", entries)
	}
}

// Each request writes its input under its own request ID and names it after
// its first line; an abandoned request leaves its input for the fixture,
// which may already have claimed it, and the next request's input never
// replaces it.
func TestControlInputIsRequestScoped(t *testing.T) {
	dir := t.TempDir()
	abandoned, stop := context.WithCancel(context.Background())
	stop()
	first := ControlInput{Name: "osd-example-input.json", Data: []byte("first")}
	if err := ControlDaemonInput(abandoned, dir, "verify", "osd-example", "observe", first); err == nil {
		t.Fatal("an abandoned request reported its acknowledgement")
	}
	pending := envelopes(t, dir, "verify-osd-example")
	if len(pending) != 1 {
		t.Fatal("the abandoned request's envelope", pending)
	}
	var firstID string
	for id, data := range pending {
		firstID = id
		if _, rest, _ := strings.Cut(data, "\n"); rest != "input osd-example-input.json\n" {
			t.Fatalf("request input lines %q", rest)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- ControlDaemonInput(ctx, dir, "verify", "osd-example", "observe", ControlInput{Name: "osd-example-input.json", Data: []byte("second"), Private: true})
	}()
	var secondID string
	for secondID == "" {
		for id := range envelopes(t, dir, "verify-osd-example") {
			if id != firstID {
				secondID = id
			}
		}
		if secondID != "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	for id, want := range map[string]string{firstID: "first", secondID: "second"} {
		data, err := os.ReadFile(filepath.Join(dir, "osd-example-input.json."+id))
		if err != nil || string(data) != want {
			t.Fatal("request", id, "input", string(data), err)
		}
	}
	if info, err := os.Stat(filepath.Join(dir, "osd-example-input.json."+secondID)); err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatal("private input mode", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "osd-example-input.json")); err == nil {
		t.Fatal("a request wrote the plain input name the fixture claims")
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-example-verify."+secondID), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]ControlInput{{{Name: "../x-input.json"}}, {{Name: "x-input.json"}, {Name: "x-input.json"}}} {
		if err := RequestControlInput(ctx, dir, "verify-bad", "bad-verify", bad, "x"); err == nil {
			t.Fatal("accepted inputs", bad)
		}
	}
}

// Two publications of one control that the fixture has not polled yet keep
// their own envelopes, whatever their order: the watchdogs of two pause
// leases that end together both reach the fixture.
func TestConcurrentPublicationsOfOneControlKeepTheirEnvelopes(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 2)
	for _, name := range []string{"2", "3"} {
		go func() { finished <- RequestControl(ctx, dir, "recover-paused-osd", "osd-pause-recovered", name) }()
	}
	var pending map[string]string
	for len(pending) < 2 {
		pending = envelopes(t, dir, "recover-paused-osd")
		select {
		case <-ctx.Done():
			t.Fatal("pending envelopes", pending, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	names := map[string]bool{}
	for id, data := range pending {
		fields := strings.Fields(data)
		names[fields[1]] = true
		if err := os.WriteFile(filepath.Join(dir, "osd-pause-recovered."+id), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if !names["2"] || !names["3"] {
		t.Fatal("the publications do not name both OSDs", pending)
	}
	for range 2 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
}
