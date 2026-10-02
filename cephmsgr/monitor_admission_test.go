package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func monitorAdmissionClient(t *testing.T, candidate func(func(msgr.MessageData) error) error) (*Client, context.Context, <-chan struct{}, *atomic.Uint32) {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	// Decoding and admission failures must be observed without imposing the
	// short wait used by the intentionally missing-MonMap scenario below.
	options.ConnectTimeout = time.Second
	started := make(chan struct{})
	var monitors, mutations atomic.Uint32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		cfg := peerConfig{role: 1, release: 20, fsid: [16]byte{1}}
		if strings.HasSuffix(endpoint, ":6800") {
			cfg.role, cfg.id = 16, 99
			cfg.command = func(m msgr.MessageData) {
				if strings.Contains(string(m.Front), "block mutation") && mutations.Add(1) == 1 {
					close(started)
				}
			}
		} else if strings.HasSuffix(endpoint, ":6809") {
			cfg.role, cfg.id = 16, 108
		} else if strings.HasSuffix(endpoint, ":3300") {
			if monitors.Add(1) > 1 {
				cfg.initialMaps = candidate
			}
		} else {
			return nil, errors.New("unverified MGR map selected an unexpected endpoint")
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, cfg)
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	return c, ctx, started, &mutations
}

func pendingAdmissionMutation(t *testing.T, c *Client, ctx context.Context, started <-chan struct{}) (context.CancelFunc, <-chan error) {
	t.Helper()
	operation, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	finished := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(operation, Command{JSON: []byte(`{"prefix":"block mutation"}`), Input: []byte{0, 255, 1}})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("existing MGR did not receive the mutation", ctx.Err())
	}
	return cancel, finished
}

func admissionResult(t *testing.T, ctx context.Context, finished <-chan error) error {
	t.Helper()
	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
		t.Fatal("admission operation did not complete", ctx.Err())
		return ctx.Err()
	}
}

func TestDialAcceptsManagerMapBeforeMonitorMap(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":3300") {
			return dial(ctx, network, endpoint)
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, initialMaps: func(send func(msgr.MessageData) error) error {
			if err := send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(1, 99, 6800)}); err != nil {
				return err
			}
			return send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(20, [16]byte{1})})
		}})
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("valid mgrmap-first bootstrap failed", err)
	}
	defer c.Close()
	if state := c.Snapshot(); !state.Monitor.Ready || state.Manager.GlobalID != 99 || state.Manager.MapEpoch != 1 {
		t.Fatal("Dial returned before publishing the buffered MGR map", state)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("buffered map did not discover the MGR", err)
	}
	input := []byte{0, 255, 1}
	result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: input})
	if err != nil || !bytes.Equal(result.Data, input) || result.Message != "status text" {
		t.Fatal("discovered MGR lost the raw command result", result, err)
	}
}

func TestRejectedMonitorDoesNotPublishUnverifiedManagerMap(t *testing.T) {
	for _, failure := range []string{"FSID", "release", "malformed", "missing", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			missingMonMap := make(chan struct{})
			candidate := func(send func(msgr.MessageData) error) error {
				if err := send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(9, 4000, 6810)}); err != nil {
					return err
				}
				mon := mockMonMap(20, [16]byte{1})
				switch failure {
				case "FSID":
					mon = mockMonMap(20, [16]byte{2})
				case "release":
					mon = mockMonMap(19, [16]byte{1})
				case "malformed":
					mon = []byte{0}
				case "missing":
					return io.EOF
				case "timeout":
					close(missingMonMap)
					return nil // The live authenticated peer never sends MonMap.
				}
				return send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mon})
			}
			c, ctx, started, mutations := monitorAdmissionClient(t, candidate)
			cancel, finished := pendingAdmissionMutation(t, c, ctx, started)
			before := c.Snapshot()
			c.mu.Lock()
			mon, mgr := c.mon, c.mgr
			c.mu.Unlock()
			admissionCtx := ctx
			stopAdmission := func() {}
			if failure == "timeout" {
				admissionCtx, stopAdmission = context.WithTimeout(ctx, 500*time.Millisecond)
			}
			admissionErr := c.connectMonitor(admissionCtx)
			stopAdmission()
			if admissionErr == nil {
				t.Fatal("unverified candidate was admitted", failure)
			}
			if failure == "timeout" {
				if !errors.Is(admissionErr, context.DeadlineExceeded) {
					t.Fatal("missing MonMap did not terminate at the candidate deadline", admissionErr)
				}
				select {
				case <-missingMonMap:
				default:
					t.Fatal("candidate timed out before its authenticated MGR map was delivered", admissionErr)
				}
			}
			after := c.Snapshot()
			c.mu.Lock()
			monRestored, mgrPreserved := c.mon == mon && c.monReady, c.mgr == mgr
			c.mu.Unlock()
			if !monRestored || !mgrPreserved || after.Manager.GlobalID != before.Manager.GlobalID || after.Manager.MapEpoch != before.Manager.MapEpoch || mgr.Err() != nil {
				t.Fatal("rejected candidate changed the accepted MGR", failure, monRestored, mgrPreserved, before.Manager.GlobalID, after.Manager.GlobalID, mgr.Err())
			}
			select {
			case err := <-finished:
				t.Fatal("rejected candidate terminated an existing mutation", err)
			default:
			}
			if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || !json.Valid(result.Data) {
				t.Fatal("accepted MGR was unusable after candidate rejection", err)
			}
			cancel()
			var unknown *OutcomeUnknownError
			if err := admissionResult(t, ctx, finished); !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) || mutations.Load() != 1 {
				t.Fatal("existing mutation lost local cancellation or was replayed", err, mutations.Load())
			}
		})
	}
}

func TestMonitorAdmissionPublishesLatestBufferedManagerMap(t *testing.T) {
	early, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	c, ctx, started, mutations := monitorAdmissionClient(t, func(send func(msgr.MessageData) error) error {
		for _, mgr := range []msgr.MessageData{
			{Type: msgr.MgrMapMessage, Front: mockMgrMap(3, 102, 6803)},
			{Type: msgr.MgrMapMessage, Front: mockMgrMap(9, 108, 6809)},
			{Type: msgr.MgrMapMessage, Front: mockMgrMap(4, 103, 6804)},
		} {
			if err := send(mgr); err != nil {
				return err
			}
		}
		close(early)
		<-release
		return send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(20, [16]byte{1})})
	})
	_, mutation := pendingAdmissionMutation(t, c, ctx, started)
	c.mu.Lock()
	oldMonitor := c.mon
	c.mu.Unlock()
	connected := make(chan error, 1)
	go func() { connected <- c.connectMonitor(ctx) }()
	select {
	case <-early:
	case <-ctx.Done():
		t.Fatal("candidate did not send mgrmap-first subscriptions", ctx.Err())
	}
	select {
	case err := <-mutation:
		t.Fatal("unverified early MGR maps interrupted a valid mutation", err)
	case <-time.After(30 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if c.Snapshot().Manager.GlobalID != 99 {
		t.Fatal("early map escaped the candidate admission gate")
	}
	select {
	case release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("candidate could not finish its MonMap", ctx.Err())
	}
	if err := admissionResult(t, ctx, connected); err != nil {
		t.Fatal("valid mgrmap-first candidate could not connect", err)
	}
	state := c.Snapshot()
	if state.Manager.GlobalID != 108 || state.Manager.MapEpoch != 9 || state.Manager.Ready {
		t.Fatal("candidate did not publish its highest buffered MGR epoch", state.Manager)
	}
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, ctx, mutation); !errors.Is(err, ErrManagerChanged) || !errors.As(err, &unknown) || mutations.Load() != 1 {
		t.Fatal("admitted MGR change lost uncertainty or replayed a mutation", err, mutations.Load())
	}
	for _, front := range [][]byte{mockMgrMap(100, 4000, 6810), {0}} {
		if accepted, err := c.handleMap(oldMonitor, msgr.MessageData{Type: msgr.MgrMapMessage, Front: front}); accepted || err != nil {
			t.Fatal("retired monitor's manager update was processed", accepted, err)
		}
	}
	if state := c.Snapshot(); state.Manager.GlobalID != 108 || state.Manager.MapEpoch != 9 {
		t.Fatal("retired monitor replaced the admitted MGR map", state.Manager)
	}
	if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || !json.Valid(result.Data) {
		t.Fatal("highest buffered MGR map could not route a new command", err)
	}
}

func TestMalformedManagerMapBeforeMonitorAdmissionFailsSession(t *testing.T) {
	c, ctx, _, _ := monitorAdmissionClient(t, func(send func(msgr.MessageData) error) error {
		return send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: []byte{0}})
	})
	before := c.Snapshot()
	if err := c.connectMonitor(ctx); !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("early invalid MGR map lost protocol/decode provenance", err)
	}
	after := c.Snapshot()
	if !after.Manager.Ready || after.Manager.GlobalID != before.Manager.GlobalID {
		t.Fatal("early invalid MGR map damaged the accepted session")
	}
}
