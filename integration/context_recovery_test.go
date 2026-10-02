package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type recoverySnapshotChecks struct {
	client                 *cephmsgr.Client
	fsid                   string
	globalID, managerID    uint64
	callbacks, calls       [2]atomic.Uint64
	unready                [2]atomic.Bool
	changedManagerCallback atomic.Bool
	invalid                atomic.Bool
}

type recoverySnapshotContext struct {
	context.Context
	checks *recoverySnapshotChecks
	role   int
}

func (ctx *recoverySnapshotContext) Err() error {
	state := ctx.checks.client.Snapshot()
	ctx.checks.callbacks[ctx.role].Add(1)
	if state.FSID != ctx.checks.fsid || state.GlobalID != ctx.checks.globalID || state.AuthRejection != nil {
		ctx.checks.invalid.Store(true)
	}
	ready := state.Monitor.Ready
	if ctx.role == 1 {
		ready = state.Manager.Ready
		if state.Manager.GlobalID != 0 && state.Manager.GlobalID != ctx.checks.managerID {
			ctx.checks.changedManagerCallback.Store(true)
		}
	}
	if !ready {
		ctx.checks.unready[ctx.role].Store(true)
	}
	return ctx.Context.Err()
}

func TestCephContextRecoveryIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, 35*time.Second)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	options := integrationOptions(t)
	options.ConnectTimeout = time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 500*time.Millisecond, 2*time.Second
	// Hold fresh MON setup while the real daemon is paused so reentrant
	// readiness cancellation deterministically exercises unavailable admission.
	var holdMonitor atomic.Bool
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(endpoint)
		if holdMonitor.Load() && (port == "33300" || port == "33301" || port == "33302") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return dial(ctx, network, endpoint)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	work, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	var closing atomic.Bool
	var closedCalls [2]atomic.Uint32
	t.Cleanup(func() { stop(); holdMonitor.Store(false); closing.Store(true); c.Close(); workers.Wait() })
	initial := waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.Manager.Available })
	if initial.Manager.Ready || initial.GlobalID == 0 {
		t.Fatal("context recovery must begin before cold MGR setup")
	}
	checks := &recoverySnapshotChecks{client: c, fsid: initial.FSID, globalID: initial.GlobalID, managerID: initial.Manager.GlobalID}

	input := []byte{0, 255, 13, 10, 1, 0}
	key := fmt.Sprintf("native-context-recovery-%d", time.Now().UnixNano())
	encoded, _ := json.Marshal(map[string]string{"prefix": "config-key set", "key": key})
	if _, err := observer.MonCommand(ctx, cephmsgr.Command{JSON: encoded, Input: input}); err != nil {
		t.Fatal("seed independent raw response marker", err)
	}
	read, _ := json.Marshal(map[string]string{"prefix": "config-key get", "key": key})
	queries := [2]cephmsgr.Command{
		{JSON: []byte(`{"prefix":"status","format":"json"}`)},
		{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)},
	}
	binary := [2]cephmsgr.Command{{JSON: read}, {JSON: []byte(`{"prefix":"pg getmap"}`)}}
	validOutput := func(result cephmsgr.Result, role int, raw bool) bool {
		if result.Code != 0 {
			return false
		}
		if !raw {
			return json.Valid(result.Data)
		}
		if role == 0 {
			return bytes.Equal(result.Data, input)
		}
		return len(result.Data) >= 16 && !json.Valid(result.Data) && bytes.ContainsRune(result.Data, 0)
	}
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(role int) {
			defer workers.Done()
			call := c.MonCommand
			if role == 1 {
				call = c.MgrCommand
			}
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for iteration := 0; work.Err() == nil; iteration++ {
				operation, cancel := context.WithTimeout(work, 5*time.Second)
				callback := &recoverySnapshotContext{Context: operation, checks: checks, role: role}
				command := queries[role]
				raw := iteration%2 != 0
				if raw {
					command = binary[role]
				}
				result, err := call(callback, command)
				cancel()
				if err == nil {
					if !validOutput(result, role, raw) {
						t.Errorf("recovery changed raw command response: role=%d raw=%t code=%d size=%d", role, raw, result.Code, len(result.Data))
						stop()
						return
					}
					checks.calls[role].Add(1)
				} else if errors.Is(err, cephmsgr.ErrClosed) && closing.Load() && testcluster.IsRecoveryError(err, cephmsgr.ErrClosed) {
					closedCalls[role].Add(1)
					return
				} else if !testcluster.IsRecoveryError(err, context.Canceled, context.DeadlineExceeded, cephmsgr.ErrManagerChanged, cephmsgr.ErrKeepaliveTimeout, session.ErrRetired) {
					// Uncertainty alone never accepts an authentication, command,
					// protocol or crypto failure, including mixed joined causes.
					t.Errorf("unexpected reentrant-context recovery error: role=%d error=%v", role, err)
					stop()
					return
				}
				select {
				case <-work.Done():
					return
				case <-ticker.C:
				}
			}
		}(i % 2)
	}
	waitClientState(t, c, work, func(s cephmsgr.State) bool { return s.Monitor.Ready && s.Manager.Ready })
	if !checks.unready[1].Load() {
		t.Fatal("custom contexts bypassed cold MGR setup")
	}
	renewed := waitClientState(t, c, work, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Ready && s.AuthTicket.Expires.After(initial.AuthTicket.Expires) && s.MgrTicket.Expires.After(initial.MgrTicket.Expires)
	})
	_, port, _ := net.SplitHostPort(renewed.Monitor.Endpoint)
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	if name == "" {
		t.Fatal("unrecognized context recovery MON", renewed.Monitor.Endpoint)
	}
	holdMonitor.Store(true)
	if err := testcluster.ControlDaemon(work, control, "pause", "mon", name); err != nil {
		t.Fatal("pause context recovery MON", err)
	}
	paused := true
	t.Cleanup(func() {
		if paused {
			resume, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := testcluster.ControlDaemon(resume, control, "resume", "mon", name); err != nil {
				t.Error("resume context recovery MON", err)
			}
		}
	})
	waitClientState(t, c, work, func(s cephmsgr.State) bool { return !s.Monitor.Ready })
	for role, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		operation, cancel := context.WithTimeout(work, 30*time.Millisecond)
		callback := &recoverySnapshotContext{Context: operation, checks: checks, role: role}
		err := wait(callback)
		cancel()
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
			t.Fatal("reentrant unavailable readiness wait lost known cancellation", role, err)
		}
	}
	holdMonitor.Store(false)
	recovered := waitClientState(t, c, work, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Ready && s.Monitor.Endpoint != renewed.Monitor.Endpoint
	})
	if err := testcluster.ControlDaemon(work, control, "resume", "mon", name); err != nil {
		t.Fatal("resume context recovery MON", err)
	}
	paused = false

	encoded, _ = json.Marshal(map[string]string{"prefix": "mgr fail", "who": recovered.Manager.Name})
	// The independent administrator sends this mutation once. Only the four
	// read-only workers repeat operations whose recovery outcome is uncertain.
	if _, err := observer.MonCommand(work, cephmsgr.Command{JSON: encoded}); err != nil {
		t.Fatal("fail context recovery MGR", err)
	}
	replacement := waitClientState(t, c, work, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Ready && s.Manager.Available && s.Manager.GlobalID != recovered.Manager.GlobalID
	})
	if replacement.Manager.MapEpoch <= recovered.Manager.MapEpoch {
		t.Fatal("context recovery did not observe a newer active MGR map")
	}
	for role, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MonCommand, c.MgrCommand} {
		callback := &recoverySnapshotContext{Context: work, checks: checks, role: role}
		result, err := call(callback, binary[role])
		if err != nil || !validOutput(result, role, true) {
			t.Fatal("raw response after reentrant recovery", role, result.Code, len(result.Data), err)
		}
	}
	closing.Store(true)
	if err := c.Close(); err != nil {
		t.Fatal("Close during reentrant recovery workload", err)
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("Close left reentrant context callers waiting", ctx.Err())
	}
	for role, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MonCommand, c.MgrCommand} {
		callback := &recoverySnapshotContext{Context: ctx, checks: checks, role: role}
		_, err := call(callback, queries[role])
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) || closedCalls[role].Load() != 2 || checks.calls[role].Load() < 10 {
			t.Fatal("reentrant shutdown lost completed work or known rejection", role, err, closedCalls[role].Load(), checks.calls[role].Load())
		}
	}
	if checks.invalid.Load() || !checks.unready[0].Load() || !checks.changedManagerCallback.Load() {
		t.Fatal("reentrant snapshots lost identity or bypassed recovery states", checks.invalid.Load(), checks.unready[0].Load(), checks.changedManagerCallback.Load())
	}
	t.Logf("Err->Snapshot callbacks survived cold MGR setup, real ticket renewal, paused MON failover, MGR takeover, readiness cancellation and concurrent Close; MON/MGR completed=%d/%d callbacks=%d/%d", checks.calls[0].Load(), checks.calls[1].Load(), checks.callbacks[0].Load(), checks.callbacks[1].Load())
}
