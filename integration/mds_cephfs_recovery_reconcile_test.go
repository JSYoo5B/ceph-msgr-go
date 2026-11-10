package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A failure to set up the fresh cleanup client of the restart reconciliation
// is returned to the reconciliation, which records it: the failure of the
// restart leaf stays the first reported failure, the setup error is added to
// it, and the receipt is not completed. A reconciliation that stops without
// recording an error (a helper ending the goroutine) is not completed either.
func TestCephFSRecoveryReconcileKeepsSetupFailure(t *testing.T) {
	t.Setenv("CEPH_MSGR_TEST_KEY_TYPE", "aes")
	dir := t.TempDir()
	// A synthetic AES credential: type 1, zero creation time, 16 key bytes.
	raw := binary.LittleEndian.AppendUint16(nil, 1)
	raw = append(raw, make([]byte, 8)...)
	raw = binary.LittleEndian.AppendUint16(raw, 16)
	raw = append(raw, []byte("0123456789abcdef")...)
	keyFile := filepath.Join(dir, "key")
	if err := os.WriteFile(keyFile, []byte(base64.StdEncoding.EncodeToString(raw)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	refused := errors.New("injected MON dial refusal")
	options := cephmsgr.Options{Monitors: []string{"127.0.0.1:3300"}, ExpectedFSID: "00000000-0000-4000-8000-000000000000",
		DialContext: func(context.Context, string, string) (net.Conn, error) { return nil, refused }}
	const identity = "client.mds-cephfs-recovery-test"
	configure := func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS = true, true }
	dial := func(keyPath string) error {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		client, release, err := dialRecoveryClient(ctx, options, identity, keyPath, newRecoveryDials(false), configure)
		if err == nil {
			release()
			t.Fatal("a recovery client was set up without a cluster", client.Snapshot())
		}
		return err
	}
	for name, tc := range map[string]struct {
		keyPath, want string
		is            error
	}{
		"credential":         {filepath.Join(dir, "missing"), "read recovery credential", os.ErrNotExist},
		"MON authentication": {keyFile, "authenticate recovery client", refused},
	} {
		t.Run(name, func(t *testing.T) {
			// The leaf's own failure is reported first; reconciliation only adds.
			reported := []string{"restart leaf: injected failure after restart-fire"}
			report := func(message string) { reported = append(reported, "reconcile: "+message) }
			r := &recoveryReconcileEvidence{FailedAfterPhase: "fired", Started: time.Now()}
			func() {
				defer r.finish(report)
				if err := dial(tc.keyPath); err != nil {
					r.failf(report, "set up a fresh cleanup client: %v", err)
					if !errors.Is(err, tc.is) {
						t.Fatal("setup error lost its cause", err)
					}
					return
				}
			}()
			if len(reported) != 2 || reported[0] != "restart leaf: injected failure after restart-fire" || !strings.Contains(reported[1], "set up a fresh cleanup client: "+tc.want) {
				t.Fatal("the leaf failure and the setup failure are not both retained", reported)
			}
			if r.Completed || len(r.Errors) != 1 || r.SessionClosed || r.OwnedSessionsChecked {
				t.Fatal("a reconciliation without a cleanup client was recorded as completed", r)
			}
		})
	}
	// A helper that ends the goroutine (t.Fatal in a callee) runs the deferred
	// finish with no recorded error and no terminal verification.
	t.Run("stopped without an error", func(t *testing.T) {
		var reported []string
		report := func(message string) { reported = append(reported, message) }
		r := &recoveryReconcileEvidence{FailedAfterPhase: "fired", Started: time.Now()}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer r.finish(report)
			runtime.Goexit()
		}()
		<-done
		if r.Completed || len(r.Errors) != 1 || len(reported) != 1 || !strings.Contains(reported[0], "stopped before its terminal verification") {
			t.Fatal("a reconciliation that verified nothing was recorded as completed", r, reported)
		}
	})
	// Completed needs every terminal check, and no error.
	verified := func() *recoveryReconcileEvidence {
		return &recoveryReconcileEvidence{Started: time.Now(), SessionClosed: true, RecoveryDirAbsent: true, PlanDirAbsent: true, OwnedSessionsChecked: true}
	}
	none := func(message string) { t.Fatal("a verified reconciliation reported", message) }
	if r := verified(); func() bool { r.finish(none); return !r.Completed || len(r.Errors) != 0 }() {
		t.Fatal("a fully verified reconciliation is not completed", r)
	}
	for name, unset := range map[string]func(*recoveryReconcileEvidence){
		"session":        func(r *recoveryReconcileEvidence) { r.SessionClosed = false },
		"recovery dir":   func(r *recoveryReconcileEvidence) { r.RecoveryDirAbsent = false },
		"plan dir":       func(r *recoveryReconcileEvidence) { r.PlanDirAbsent = false },
		"sessions read":  func(r *recoveryReconcileEvidence) { r.OwnedSessionsChecked = false },
		"sessions left":  func(r *recoveryReconcileEvidence) { r.OwnedSessionsLeft = []uint64{42} },
		"recorded error": func(r *recoveryReconcileEvidence) { r.Errors = []string{"release caps: injected"} },
	} {
		r := verified()
		unset(r)
		if r.finish(func(string) {}); r.Completed || len(r.Errors) == 0 {
			t.Fatal("an unverified reconciliation is completed", name, r)
		}
	}
}
