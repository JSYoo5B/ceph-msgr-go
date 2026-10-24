package cephmsgr

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"
)

// The holder is the independent oracle a later Go ServiceMap lifecycle cell
// needs: a native librados client registers one synthetic service daemon and
// keeps it alive across fixture control requests, a fresh native handle sees it
// with the holder's own GID, and only an explicit release ends it. This test
// verifies that fixture prerequisite natively. It starts no Go ServiceMap
// watch and does not stop any MON.
func TestCephServiceMapNativeHolderIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("the native ServiceMap holder requires the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("%x", nonce)

	t.Cleanup(func() {
		// The cleanup phase kills only a holder that is still alive, once.
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		receipt, err := observeNativeServiceHolder(cleanup, control, token, "cleanup")
		if err != nil {
			t.Error("native ServiceMap holder cleanup", err)
			return
		}
		t.Logf("cleanup state=%s killed=%t holder=%+v", receipt.State, receipt.Killed, receipt.Holder)
	})

	started, err := observeNativeServiceHolder(ctx, control, token, "start")
	if err != nil {
		t.Fatal("native ServiceMap holder start", err)
	}
	ready, err := serviceHolderReady(started)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("holder %s registered: pid=%d gid=%d lifetime cap=%ds baseline epoch=%d", started.Daemon, ready.PID, ready.GID, ready.LifetimeSeconds, *started.Baseline.Epoch)

	// The start request returned while the holder stays registered, so these
	// later requests prove the fixture control loop is not blocked by it.
	waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
	present := waitServiceHolder(t, waitCtx, control, token, "a fresh native view of the owned daemon", func(r nativeServiceHolderReceipt) bool {
		return serviceHolderPresent(r, ready)
	})
	stop()
	if present.Holder == nil || !present.Holder.Alive || present.Holder.Exited != nil {
		t.Fatalf("holder is not alive while its daemon is registered: %+v", present.Holder)
	}
	t.Logf("fresh native view: service map epoch %d, gid %d, start epoch %s", *present.View.Epoch, *present.View.Entry.GID, optionalEpoch(present.View.Entry.StartEpoch))
	time.Sleep(3 * time.Second)
	again, err := observeNativeServiceHolder(ctx, control, token, "observe")
	if err != nil {
		t.Fatal(err)
	}
	if !serviceHolderPresent(again, ready) || !again.Holder.Alive {
		t.Fatalf("registration was not retained while held: view=%+v holder=%+v", again.View, again.Holder)
	}

	release, err := observeNativeServiceHolder(ctx, control, token, "release")
	if err != nil {
		t.Fatal("native ServiceMap holder release", err)
	}
	if release.State != "released" || release.Exited == nil || release.Exited.Reason != "released" || !release.Exited.ShutdownReturned {
		t.Fatalf("holder did not report a released shutdown: %+v", release)
	}

	// A returned shutdown is not removal evidence: require a readable fresh dump
	// without the owned daemon on two observations at least two seconds apart.
	absentCtx, stopAbsent := context.WithTimeout(ctx, 90*time.Second)
	defer stopAbsent()
	var firstAbsent time.Time
	waitServiceHolder(t, absentCtx, control, token, "stable native absence", func(r nativeServiceHolderReceipt) bool {
		if !serviceHolderAbsent(r) {
			firstAbsent = time.Time{}
			return false
		}
		if firstAbsent.IsZero() {
			firstAbsent = time.Now()
		}
		return time.Since(firstAbsent) >= 2*time.Second
	})
	final, err := observeNativeServiceHolder(ctx, control, token, "cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if !serviceHolderRestored(final) || final.Holder == nil || final.Holder.Alive {
		t.Fatalf("released holder was not restored to its own baseline without a kill: %+v", final)
	}
}

func optionalEpoch(value *uint32) string {
	if value == nil {
		return "unreported"
	}
	return fmt.Sprint(*value)
}
