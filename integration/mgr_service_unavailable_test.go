package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type mgrServiceNativeMap struct {
	Epoch     uint32 `json:"epoch"`
	Flags     uint64 `json:"flags"`
	Available bool   `json:"available"`
	Name      string `json:"active_name"`
	GID       uint64 `json:"active_gid"`
}

func waitMgrServiceNativeMap(t *testing.T, ctx context.Context, down bool) mgrServiceNativeMap {
	t.Helper()
	for {
		state := nativePool(t, ctx, "mgrstate", map[string]any{}).ManagerMap
		if state == nil || state.Epoch == 0 {
			t.Fatal("native MGR state is missing")
		}
		if down && state.Flags&1 != 0 && !state.Available && state.GID == 0 && state.Name == "" ||
			!down && state.Flags&1 == 0 && state.Available && state.GID != 0 && state.Name != "" {
			return *state
		}
		select {
		case <-ctx.Done():
			t.Fatal("native MGR state did not settle", down)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Done signals that OpenMgrService has entered its context-aware setup. It
// leaves cancellation semantics unchanged and avoids guessing goroutine start.
type mgrServiceWaitContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *mgrServiceWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func testMgrServiceInitiallyUnavailable(t *testing.T, ctx context.Context, admin *cephmsgr.Client) {
	t.Helper()
	receipt := struct {
		Before            mgrServiceNativeMap `json:"before"`
		Down              mgrServiceNativeMap `json:"down"`
		RestoredMap       mgrServiceNativeMap `json:"after"`
		KnownDeadline     bool                `json:"known_deadline"`
		KnownCancel       bool                `json:"known_cancel"`
		KnownClientClose  bool                `json:"known_client_close"`
		DialsBefore       uint32              `json:"dials_before_restore"`
		DialsAfter        uint32              `json:"dials_after_explicit_reuse"`
		CanceledAbsent    bool                `json:"canceled_identities_absent"`
		WaitingGID        uint64              `json:"waiting_gid"`
		WaitingManagerGID uint64              `json:"waiting_manager_gid"`
		ExplicitReuse     bool                `json:"explicit_identity_reuse"`
		Removed           bool                `json:"removed"`
		Restored          bool                `json:"restored"`
	}{}
	t.Cleanup(func() {
		data, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "mgr-service-unavailable.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve unavailable MGR evidence", err)
		}
	})
	setDown := func(call context.Context, client *cephmsgr.Client, value string) error {
		command, err := newCommand("mgr set", map[string]any{"var": "down", "val": value})
		if err != nil {
			return err
		}
		_, err = client.MonCommand(call, command)
		return err
	}
	receipt.Before = waitMgrServiceNativeMap(t, ctx, false)
	clearNeeded := true
	t.Cleanup(func() {
		if !clearNeeded {
			return
		}
		clearNeeded = false // Never retry an uncertain restoration mutation.
		stop, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		client, err := cephmsgr.Dial(stop, integrationOptions(t))
		if err != nil {
			t.Error("open MON-only restoration client", err)
			return
		}
		defer client.Close()
		if err := setDown(stop, client, "false"); err != nil {
			t.Error("restore MGR activation", err)
			return
		}
		receipt.RestoredMap = waitMgrServiceNativeMap(t, stop, false)
		receipt.Restored = true
	})
	if err := setDown(ctx, admin, "true"); err != nil {
		t.Fatal("disable fixture MGR activation", err)
	}
	receipt.Down = waitMgrServiceNativeMap(t, ctx, true)
	if receipt.Down.Epoch <= receipt.Before.Epoch {
		t.Fatal("native MGR down epoch did not advance")
	}
	options := integrationOptions(t)
	options.EnableMgrService = true
	dial := options.DialContext
	var dials atomic.Uint32
	options.DialContext = func(call context.Context, network, address string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(address)
		if role == "mgr.a" || role == "mgr.b" {
			dials.Add(1)
		}
		return dial(call, network, address)
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitDown := func(c *cephmsgr.Client) {
		t.Helper()
		waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.Manager.MapEpoch >= receipt.Down.Epoch && !s.Manager.Available && s.Manager.GlobalID == 0
		})
	}
	waitDown(client)
	const service = "ceph-msgr-wait"
	identity := func(daemon string) cephmsgr.MgrServiceOptions {
		return cephmsgr.MgrServiceOptions{Service: service, Daemon: daemon, Metadata: map[string]string{"phase": daemon}}
	}
	type opened struct {
		svc *cephmsgr.MgrService
		err error
	}
	start := func(c *cephmsgr.Client, call context.Context, daemon string) <-chan opened {
		t.Helper()
		entered := &mgrServiceWaitContext{Context: call, entered: make(chan struct{})}
		result := make(chan opened, 1)
		go func() {
			svc, err := c.OpenMgrService(entered, identity(daemon))
			result <- opened{svc, err}
		}()
		select {
		case <-entered.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("registration did not enter setup", daemon)
		}
		return result
	}
	known := func(result <-chan opened, want error) bool {
		t.Helper()
		select {
		case got := <-result:
			if got.svc != nil || !errors.Is(got.err, want) || errors.Is(got.err, cephmsgr.ErrMgrServiceOutcomeUnknown) {
				t.Fatal("unavailable setup was not a known local failure", got.err)
			}
			return true
		case <-time.After(5 * time.Second):
			t.Fatal("unavailable registration did not return")
			return false
		}
	}
	deadline, stopDeadline := context.WithTimeout(ctx, 600*time.Millisecond)
	defer stopDeadline()
	receipt.KnownDeadline = known(start(client, deadline, "deadline"), context.DeadlineExceeded)
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	canceled := start(client, cancelCtx, "cancel")
	cancel()
	receipt.KnownCancel = known(canceled, context.Canceled)
	closing, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer closing.Close()
	waitDown(closing)
	closed := start(closing, ctx, "client-close")
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	receipt.KnownClientClose = known(closed, cephmsgr.ErrClosed)
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	waiting := start(client, waitCtx, "waiting")
	select {
	case got := <-waiting:
		t.Fatal("registration returned before activation", got.err)
	case <-time.After(200 * time.Millisecond):
	}
	receipt.DialsBefore = dials.Load()
	if receipt.DialsBefore != 0 {
		t.Fatal("unavailable MGR was dialed", receipt.DialsBefore)
	}
	clearNeeded = false // Submit the inverse mutation once, including on error.
	if err := setDown(ctx, admin, "false"); err != nil {
		t.Fatal("restore fixture MGR activation", err)
	}
	receipt.RestoredMap = waitMgrServiceNativeMap(t, ctx, false)
	receipt.Restored = true
	var svc *cephmsgr.MgrService
	select {
	case got := <-waiting:
		if got.err != nil || got.svc == nil {
			t.Fatal("pending initial registration did not resume", got.err)
		}
		svc = got.svc
	case <-ctx.Done():
		t.Fatal("pending registration did not finish after activation")
	}
	cleanupService := func(s *cephmsgr.MgrService) {
		stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Close(stop)
	}
	defer cleanupService(svc)
	visible := serviceWatch(t, ctx, service, "waiting", "present", map[string]string{"phase": "waiting"}, nil, nil)
	var entry struct {
		GID uint64 `json:"gid"`
	}
	if !visible.ServiceMatched || json.Unmarshal(visible.ServiceEntry, &entry) != nil || entry.GID != client.Snapshot().GlobalID || dials.Load() != 1 {
		t.Fatal("resumed registration identity or dial count differs")
	}
	receipt.WaitingGID = entry.GID
	bound := waitClientState(t, client, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.MapEpoch >= receipt.RestoredMap.Epoch
	})
	if bound.Manager.Name != receipt.RestoredMap.Name || bound.Manager.GlobalID != receipt.RestoredMap.GID {
		t.Fatal("resumed registration manager differs from native map")
	}
	receipt.WaitingManagerGID = bound.Manager.GlobalID
	receipt.CanceledAbsent = true
	for _, daemon := range []string{"deadline", "cancel", "client-close"} {
		absence := serviceWatchInput(service, daemon, "absent", nil, nil, nil)
		absence.WaitSeconds, absence.StableSeconds = 10, 1
		receipt.CanceledAbsent = receipt.CanceledAbsent && nativePool(t, ctx, "servicewatch", absence).ServiceMatched
	}
	if !receipt.CanceledAbsent || dials.Load() != 1 {
		t.Fatal("canceled registration appeared after activation")
	}
	if err := svc.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reused, err := client.OpenMgrService(ctx, identity("deadline"))
	if err != nil {
		t.Fatal("explicit call did not reuse canceled reservation", err)
	}
	defer cleanupService(reused)
	receipt.ExplicitReuse = serviceWatch(t, ctx, service, "deadline", "present", map[string]string{"phase": "deadline"}, nil, nil).ServiceMatched
	if err := reused.Close(ctx); err != nil {
		t.Fatal(err)
	}
	receipt.Removed = serviceWatch(t, ctx, service, "waiting", "absent", nil, nil, nil).ServiceMatched &&
		serviceWatch(t, ctx, service, "deadline", "absent", nil, nil, nil).ServiceMatched
	receipt.DialsAfter = dials.Load()
	if !receipt.ExplicitReuse || !receipt.Removed || receipt.DialsAfter != 2 {
		t.Fatal("explicit reuse or removal did not match")
	}
	t.Logf("initial unavailable MGR: known deadline/cancel/close, no dial before activation, epochs %d/%d/%d, two explicit registrations removed", receipt.Before.Epoch, receipt.Down.Epoch, receipt.RestoredMap.Epoch)
}
