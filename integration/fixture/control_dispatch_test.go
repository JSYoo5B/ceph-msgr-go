package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// writeControlRequest publishes value as one request of the control name, in
// the envelope of the request ID that value starts with
// (internal/testcluster RequestEnvelope).
func writeControlRequest(t *testing.T, out, name, value string) {
	t.Helper()
	if err := writeRequestFile(out, name, value); err != nil {
		t.Fatal(err)
	}
}

// writeRequestFile writes value as one request envelope of the control name
// in dir. Its envelope carries the request ID value starts with, or 1 when
// value does not start with one, so a malformed value is still published.
func writeRequestFile(dir, name, value string) error {
	id, _, _ := strings.Cut(strings.TrimSpace(value), " ")
	if n, err := strconv.ParseInt(id, 10, 64); err != nil || n <= 0 {
		id = "1"
	}
	return os.WriteFile(filepath.Join(dir, requestEnvelopeName(name, id)), []byte(value), 0600)
}

// requestEnvelopeName is the envelope of request ID id of the control name.
func requestEnvelopeName(name, id string) string { return name + "." + id + ".request" }

// requestPending says whether a request of the control name is pending.
func requestPending(t *testing.T, out, name string) bool {
	t.Helper()
	pending, err := pendingRequests(out, name)
	if err != nil {
		t.Fatal(err)
	}
	return len(pending) != 0
}

func TestControllerPreservesEarlierRoleAndFailureGateOrder(t *testing.T) {
	for _, workerFailed := range []bool{false, true} {
		t.Run(map[bool]string{false: "earlier-role-refusal", true: "worker-failure-before-role"}[workerFailed], func(t *testing.T) {
			out := t.TempDir()
			d := &controlledDaemon{}
			c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
			// An MDS request precedes the generic daemon actions, and the
			// worker failure gate precedes both. Neither can be reordered.
			writeControlRequest(t, out, "mds-sessions", "1 invalid")
			writeControlRequest(t, out, "stop-mon", "2 a")
			wanted := "invalid owned MDS sessions control scope"
			if workerFailed {
				if err := os.WriteFile(filepath.Join(out, "native-oracles.error"), []byte("synthetic failure"), 0600); err != nil {
					t.Fatal(err)
				}
				wanted = "native oracle worker failed"
			}
			err := c.controls(context.Background(), out)
			if err == nil || !strings.Contains(err.Error(), wanted) || d.stops != 0 {
				t.Fatal("later action ran before an earlier failure", err, d.stops)
			}
			if !requestPending(t, out, "stop-mon") || fileExists(filepath.Join(out, "mon-stop.2")) {
				t.Fatal("a request after the first failure was consumed or acknowledged")
			}
			if requestPending(t, out, "mds-sessions") != workerFailed {
				t.Fatal("the worker failure checkpoint moved relative to the MDS request")
			}
		})
	}
}

func TestControllerRetainsEarlierMutationAndAckAfterLaterRoleRefusal(t *testing.T) {
	out := t.TempDir()
	d := &controlledDaemon{}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "stop-mon", "1 a")
	writeControlRequest(t, out, "verify-mgr-keyring", "2 invalid")
	err := c.controls(context.Background(), out)
	if err == nil || !strings.Contains(err.Error(), "native MGR keyring gate") || d.stops != 1 {
		t.Fatal("later MGR refusal moved before the earlier daemon mutation", err, d.stops)
	}
	if !fileExists(filepath.Join(out, "mon-stop.1")) || fileExists(filepath.Join(out, "mgr-keyring-verify.2")) {
		t.Fatal("earlier acknowledgement was lost or a refused action was acknowledged")
	}
	if requestPending(t, out, "stop-mon") || requestPending(t, out, "verify-mgr-keyring") {
		t.Fatal("consumed requests were retained after partial polling progress")
	}
	if err := c.controls(context.Background(), out); err != nil || d.stops != 1 {
		t.Fatal("a later poll replayed the completed earlier mutation", err, d.stops)
	}
}

type uncertainControlledDaemon struct {
	testcontainers.Container
	stops   int
	failure error
}

func (d *uncertainControlledDaemon) Stop(context.Context, *time.Duration) error {
	d.stops++
	return d.failure
}

func TestControllerDoesNotReplayConsumedUncertainMutation(t *testing.T) {
	out := t.TempDir()
	uncertain := errors.New("synthetic stop outcome unknown")
	d := &uncertainControlledDaemon{failure: uncertain}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "stop-mon", "1 a")
	writeControlRequest(t, out, "verify-mgr-keyring", "2 invalid")
	if err := c.controls(context.Background(), out); err != uncertain || d.stops != 1 {
		t.Fatal("failed mutation lost its first error or was retried", err, d.stops)
	}
	if requestPending(t, out, "stop-mon") || fileExists(filepath.Join(out, "mon-stop.1")) || !requestPending(t, out, "verify-mgr-keyring") {
		t.Fatal("uncertain action was acknowledged or its later request was consumed")
	}
	if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "native MGR keyring gate") || d.stops != 1 {
		t.Fatal("subsequent polling replayed uncertainty instead of reaching the pending request", err, d.stops)
	}
	if fileExists(filepath.Join(out, "mon-stop.1")) {
		t.Fatal("uncertain mutation acquired a later success acknowledgement")
	}
}

// hookRequestReads replaces the fixture's reads of the control directory for
// the rest of the test. read is told the number of each read, from 1, and
// the directory's entries, and returns what that read returns; it may also
// publish requests, which a later read sees.
func hookRequestReads(t *testing.T, read func(n int, entries []os.DirEntry) []os.DirEntry) {
	t.Helper()
	n := 0
	readRequestDir = func(dir string) ([]os.DirEntry, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		n++
		return read(n, entries), nil
	}
	t.Cleanup(func() { readRequestDir = os.ReadDir })
}

// withoutEntry is entries without the file name.
func withoutEntry(entries []os.DirEntry, name string) []os.DirEntry {
	var kept []os.DirEntry
	for _, entry := range entries {
		if entry.Name() != name {
			kept = append(kept, entry)
		}
	}
	return kept
}

// A request and its undo published one after the other are ordered by their
// request IDs whatever the moment of their publication: between the
// fixture's reads of the control directory, or during one read that misses
// the earlier request and returns the later one.
func TestPendingRequestsOfSeveralControlsComeFromOneSnapshot(t *testing.T) {
	for _, interleaving := range []string{"published after the first read", "one read misses the earlier request"} {
		t.Run(interleaving, func(t *testing.T) {
			out := t.TempDir()
			if interleaving == "one read misses the earlier request" {
				writeControlRequest(t, out, "stop-osd", "10 1")
				writeControlRequest(t, out, "start-osd", "11 1")
			}
			hookRequestReads(t, func(n int, entries []os.DirEntry) []os.DirEntry {
				if n != 1 {
					return entries
				}
				if interleaving == "published after the first read" {
					writeControlRequest(t, out, "stop-osd", "10 1")
					writeControlRequest(t, out, "start-osd", "11 1")
					return entries
				}
				return withoutEntry(entries, requestEnvelopeName("stop-osd", "10"))
			})
			var served []string
			for poll := 0; poll < 2; poll++ {
				pending, err := pendingRequests(out, "start-osd", "stop-osd")
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range pending {
					if _, err := claim(out, r); err != nil {
						t.Fatal(err)
					}
					served = append(served, r.name)
				}
			}
			if !slices.Equal(served, []string{"stop-osd", "start-osd"}) {
				t.Fatal("the requests were served out of publication order", served)
			}
		})
	}
}
