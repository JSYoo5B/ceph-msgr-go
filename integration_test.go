package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

// These read-only tests are opt-in and use actual daemon responses as the
// oracle. No Ceph executable is invoked by the client or by the Go test.
func integrationOptions(t *testing.T) Options {
	t.Helper()
	monitors := os.Getenv("CEPH_MSGR_MONITORS")
	if monitors == "" {
		t.Skip("set CEPH_MSGR_MONITORS, CEPH_MSGR_KEY_FILE and CEPH_MSGR_IDENTITY for a real cluster")
	}
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	key, err := ParseKey(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	return Options{Monitors: strings.Split(monitors, ","), Identity: os.Getenv("CEPH_MSGR_IDENTITY"), Key: key, ExpectedFSID: os.Getenv("CEPH_MSGR_FSID")}
}

func TestCephIntegration(t *testing.T) {
	options := integrationOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if cipher := os.Getenv("CEPH_MSGR_TEST_SERVICE_CIPHER"); cipher != "" {
		want := uint16(cephx.AES256K)
		// Tentacle KeyServer uses the lower key type of the credential
		// and rotating service secret for the MGR session key.
		if cipher == "aes" || options.Key.value.Type() == cephx.AES {
			want = cephx.AES
		}
		if got := c.snapshotAuth().Tickets[cephx.ServiceMgr].Key.Type(); got != want {
			t.Fatalf("MGR service ticket cipher: got %d want %d", got, want)
		}
		t.Logf("credential type=%d MGR service ticket type=%d", options.Key.value.Type(), want)
	}
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("MON status", err, string(result.Data))
	}
	result, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)})
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("MGR pg stat", err, string(result.Data))
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call, prefix := c.MonCommand, "status"
			if i%2 == 1 {
				call, prefix = c.MgrCommand, "pg stat"
			}
			cmd, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
			if result, err := call(ctx, Command{JSON: cmd}); err != nil || !json.Valid(result.Data) {
				t.Errorf("concurrent %s: %v", prefix, err)
			}
		}(i)
	}
	wg.Wait()
}

// Faults require the disposable fixture's control directory. Ordinary external
// cluster credentials alone cannot trigger mutations or stop any daemon.
func TestCephRecoveryIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("requires the disposable integration/run.sh fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	initial := c.snapshotAuth()
	// The fixture issues 12-second tickets. Observe genuine renewal with the
	// same global ID and newly issued future ticket lifetimes.
	for {
		c.mu.Lock()
		renewed := c.auth.GlobalID == initial.GlobalID
		for service, old := range initial.Tickets {
			renewed = renewed && c.auth.Tickets[service].Expires.After(old.Expires)
		}
		c.mu.Unlock()
		if renewed {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("ticket renewal", ctx.Err())
		}
	}
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}); err != nil {
		t.Fatal("renewed MGR ticket", err)
	}
	c.mu.Lock()
	oldID, name := c.mgrMap.GlobalID, c.mgrMap.Name
	c.mu.Unlock()
	cmd, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := c.MonCommand(ctx, Command{JSON: cmd}); err != nil {
		t.Fatal("MGR fault injection", err)
	}
	for {
		c.mu.Lock()
		changed, signal := c.mgrMap.Available && c.mgrMap.GlobalID != oldID, c.changed
		c.mu.Unlock()
		if changed {
			break
		}
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("MGR takeover", ctx.Err())
		}
	}
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}); err != nil {
		t.Fatal("command after MGR takeover", err)
	}
	// Initial Dial and renewal choose MON a while it is available.
	c.mu.Lock()
	oldMonitor := c.mon
	c.mu.Unlock()
	if err := os.WriteFile(filepath.Join(control, "stop-mon-a"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldMonitor.Done():
	case <-ctx.Done():
		t.Fatal("MON fault injection", ctx.Err())
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("command after MON failover", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.MonCommand(canceled, Command{JSON: []byte(`{"prefix":"status"}`)}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation", err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("command after cancellation", err)
	}
}
