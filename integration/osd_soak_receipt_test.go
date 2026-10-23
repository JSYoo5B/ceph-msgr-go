package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// These workload-specific receipts deliberately do not implement the shared
// successful-evidence bundle. Every stage has a distinct create-once name.
type soakReceipts struct {
	dir, label, test string
	duration         time.Duration
	setupStarted     time.Time
	owned            []string
	started          time.Time
	workloadNS       int64
	mu               sync.Mutex
	firstError       string
	unresolved       map[string]cephmsgr.OSDSubmission
	counters         func() map[string]int64
}

func writeSoakReceiptOnce(dir, name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > 512<<10 {
		return errors.New("invalid or oversized soak receipt")
	}
	file, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	// Container tests can run as root; the host collector still needs read
	// access inside its private fixture directory. Never truncate a receipt.
	err = file.Chmod(0o644)
	if err == nil {
		_, err = file.Write(append(data, '\n'))
	}
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func (r *soakReceipts) write(t *testing.T, stage string, fields map[string]any) {
	t.Helper()
	fields["schema_version"], fields["test"], fields["kind"] = 1, r.test, stage
	if !r.started.IsZero() {
		fields["after_workload_start_ns"] = time.Since(r.started).Nanoseconds()
	}
	if err := writeSoakReceiptOnce(r.dir, "osd-soak-v1-"+r.label+"-"+stage+".json", fields); err != nil {
		r.failure("immutable receipt publication failed")
		t.Error("immutable soak receipt", stage, err)
	}
}

func newSoakReceipts(t *testing.T, label string, duration time.Duration) *soakReceipts {
	t.Helper()
	r := &soakReceipts{dir: os.Getenv("CEPH_MSGR_CONTROL_DIR"), label: label, test: t.Name(), duration: duration, setupStarted: time.Now(), unresolved: map[string]cephmsgr.OSDSubmission{}}
	manifest, err := os.ReadFile(filepath.Join(r.dir, "fixture-manifest.json"))
	if err != nil {
		t.Fatal("soak fixture manifest", err)
	}
	sum := sha256.Sum256(manifest)
	r.write(t, "started", map[string]any{"configured_duration_ns": int64(duration), "configured_source_sha": os.Getenv("CEPH_MSGR_TEST_SOURCE_SHA"), "fixture_manifest_sha256": hex.EncodeToString(sum[:])})
	// Registered before the owned-object cleanup, so this final test verdict
	// observes cleanup failures too. Native fixture termination is separate.
	t.Cleanup(func() {
		r.mu.Lock()
		keys := make([]string, 0, len(r.unresolved))
		for key := range r.unresolved {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		unknown := make([]cephmsgr.OSDSubmission, 0, len(keys))
		for _, key := range keys {
			unknown = append(unknown, r.unresolved[key])
		}
		first := r.firstError
		r.mu.Unlock()
		status := "passed"
		if t.Failed() || len(unknown) != 0 || r.workloadNS < int64(duration) {
			status = "failed"
			if first == "" {
				first = "test failed or required evidence incomplete; original test log retains the failure"
			}
		}
		counters := map[string]int64{}
		if r.counters != nil {
			counters = r.counters()
		}
		r.write(t, "workload", map[string]any{"configured_duration_ns": int64(duration), "actual_workload_ns": r.workloadNS, "test_elapsed_ns": time.Since(r.setupStarted).Nanoseconds(), "status": status, "first_error": first, "counters": counters, "unresolved_outcomes": unknown})
	})
	return r
}

func (r *soakReceipts) beginWorkload(t *testing.T) {
	r.write(t, "setup", map[string]any{"elapsed_ns": time.Since(r.setupStarted).Nanoseconds(), "owned_objects": r.owned})
	if t.Failed() {
		t.Fatal("soak setup evidence could not be preserved")
	}
	r.started = time.Now()
}

func (r *soakReceipts) failure(message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.firstError == "" {
		r.firstError = message
	}
}

func (r *soakReceipts) finishWorkload() {
	if r.workloadNS == 0 && !r.started.IsZero() {
		r.workloadNS = time.Since(r.started).Nanoseconds()
	}
}

func (r *soakReceipts) workerDone() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workloadNS = max(r.workloadNS, time.Since(r.started).Nanoseconds())
}

func (r *soakReceipts) mutation(reply cephmsgr.OSDReply, err error) {
	s := reply.Submission
	key := fmt.Sprintf("%d/%d/%d", s.GlobalID, s.ClientIncarnation, s.Transaction)
	r.mu.Lock()
	defer r.mu.Unlock()
	var unknown *cephmsgr.OutcomeUnknownError
	if errors.As(err, &unknown) {
		if _, exists := r.unresolved[key]; !exists {
			r.unresolved[key] = s
		}
	} else if err == nil {
		delete(r.unresolved, key)
	}
}

func (r *soakReceipts) observe(t *testing.T, worker int, expected []byte, attr string, native omapWriteReceipt, elapsed time.Duration) bool {
	t.Helper()
	state := native.State
	expectedSum, nativeSum := sha256.Sum256(expected), sha256.Sum256(state.Data)
	matched := state.Code == 0 && state.Version != 0 && len(expected) <= 128<<10 && len(state.Data) <= 128<<10 &&
		bytes.Equal(expected, state.Data) && xattrInventory(state.nativeXattrSnapshot)["soak"] == attr
	r.write(t, fmt.Sprintf("worker-%d", worker), map[string]any{"worker": worker, "object": native.Object,
		"native_observation_elapsed_ns": int64(elapsed),
		"expected_data":                 expected, "expected_xattr": attr, "native": state.nativeXattrSnapshot, "matched": matched,
		"expected_length": len(expected), "expected_sha256": hex.EncodeToString(expectedSum[:]), "native_length": len(state.Data), "native_sha256": hex.EncodeToString(nativeSum[:]),
		"native_source_commit": native.Commit, "native_daemon_version": native.DaemonVersion, "independent_native_c_api": native.Scope.Independent})
	return matched
}

func TestOSDSoakImmutableReceipts(t *testing.T) {
	dir := t.TempDir()
	name := "osd-soak-v1-steady-worker-0.json"
	if err := writeSoakReceiptOnce(dir, name, map[string]int{"value": 1}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSoakReceiptOnce(dir, name, map[string]int{"value": 2}); !errors.Is(err, os.ErrExist) {
		t.Fatal("existing observation was replaceable", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("observation changed after duplicate publication", err)
	}
	r := &soakReceipts{unresolved: map[string]cephmsgr.OSDSubmission{}}
	reply := cephmsgr.OSDReply{Submission: cephmsgr.OSDSubmission{GlobalID: 10, Transaction: 20, MapEpoch: 30}}
	r.mutation(reply, &cephmsgr.OutcomeUnknownError{})
	r.mutation(reply, &cephmsgr.OutcomeUnknownError{})
	if len(r.unresolved) != 1 {
		t.Fatal("same unknown submission was counted twice")
	}
	r.mutation(cephmsgr.OSDReply{Submission: cephmsgr.OSDSubmission{GlobalID: 11, Transaction: 20}}, nil)
	if len(r.unresolved) != 1 {
		t.Fatal("another client's reply resolved the original outcome")
	}
	r.mutation(reply, nil)
	if len(r.unresolved) != 0 {
		t.Fatal("confirmed original submission remains unknown")
	}
}
