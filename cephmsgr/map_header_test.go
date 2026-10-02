package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMapMessageHeaderCompatibility(t *testing.T) {
	for _, typ := range []uint16{msgr.MonMapMessage, msgr.MgrMapMessage} {
		for _, header := range []struct {
			name            string
			version, compat uint16
			unsupported     bool
		}{
			{"supported", 1, 1, false},
			{"newer compatible", 2, 1, false},
			{"unsupported compatibility", 2, 2, true},
		} {
			name := "MON/" + header.name
			front := mockMonMap(20, [16]byte{1})
			if typ == msgr.MgrMapMessage {
				name, front = "MGR/"+header.name, mockMgrMap(9, 9999, 6899)
			}
			t.Run(name, func(t *testing.T) {
				message := msgr.MessageData{Sequence: 1, Type: typ, Version: header.version, CompatVersion: header.compat, Front: front}
				var transport bytes.Buffer
				if err := msgr.NewWriter(&transport, 0).Write(message.Frame()); err != nil {
					t.Fatal(err)
				}
				frame, err := msgr.NewReader(&transport, 0).Read()
				if err != nil {
					t.Fatal(err)
				}
				message, err = msgr.DecodeMessage(frame)
				if err != nil {
					t.Fatal(err)
				}
				c := &Client{changed: make(chan struct{}), mon: new(session.Session)}
				beforeMon, beforeMgr := c.monMap, c.mgrMap
				accepted, err := c.handleMap(c.mon, message)
				if header.unsupported {
					if accepted || !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, wire.ErrVersion) || retryableSetup(err) || !reflect.DeepEqual(c.monMap, beforeMon) || !reflect.DeepEqual(c.mgrMap, beforeMgr) {
						t.Fatal("unsupported header updated maps or lost protocol/version provenance", accepted, err, c.monMap, c.mgrMap)
					}
					return
				}
				if err != nil || accepted != (typ == msgr.MonMapMessage) {
					t.Fatal("compatible map header was rejected", accepted, err)
				}
				if typ == msgr.MonMapMessage && (c.fsid != [16]byte{1} || c.monMap.Epoch != 1) || typ == msgr.MgrMapMessage && (c.mgrMap.Epoch != 9 || c.mgrMap.GlobalID != 9999) {
					t.Fatal("compatible map was not published", c.monMap, c.mgrMap)
				}
			})
		}
	}
}

func TestMapHeaderValidationPreservesStaleSourceAndReleasePolicy(t *testing.T) {
	c := &Client{changed: make(chan struct{}), mon: new(session.Session)}
	for _, typ := range []uint16{msgr.MonMapMessage, msgr.MgrMapMessage} {
		if accepted, err := c.handleMap(new(session.Session), msgr.MessageData{Type: typ, Version: 2, CompatVersion: 2}); accepted || err != nil {
			t.Fatal("retired monitor's incompatible message was processed", accepted, err)
		}
	}
	_, err := c.handleMap(c.mon, msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, CompatVersion: 1, Front: mockMonMap(19, [16]byte{1})})
	if !errors.Is(err, maps.ErrRelease) || errors.Is(err, msgr.ErrFrame) || retryableSetup(err) {
		t.Fatal("valid unsupported release lost its policy error", err)
	}
}

// Unlike mockDaemon's normal version-1 sender, this peer preserves the exact
// map headers under test after completing CephX and secure authentication.
func mapHeaderPeer(conn net.Conn, messages []msgr.MessageData) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	r, w, err := mockAuthenticate(conn, peerConfig{role: 1, release: 20, fsid: [16]byte{1}})
	if err != nil {
		return
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, err := r.Read(); err != nil {
				return
			}
		}
	}()
	defer func() { conn.Close(); <-drained }()
	for i, message := range messages {
		message.Sequence, message.Priority = uint64(i+1), 127
		if err := w.Write(message.Frame()); err != nil {
			return
		}
	}
	<-drained
}

func TestDialAcceptsCompatibleMapHeaders(t *testing.T) {
	for _, version := range []uint16{1, 2} {
		t.Run(map[uint16]string{1: "supported", 2: "newer compatible"}[version], func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			dial := options.DialContext
			options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if !strings.HasSuffix(endpoint, ":3300") {
					return dial(ctx, network, endpoint)
				}
				client, peer := net.Pipe()
				go mapHeaderPeer(peer, []msgr.MessageData{
					{Type: msgr.MgrMapMessage, Version: version, CompatVersion: 1, Front: mockMgrMap(1, 99, 6800)},
					{Type: msgr.MonMapMessage, Version: version, CompatVersion: 1, Front: mockMonMap(20, [16]byte{1})},
				})
				return client, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal("compatible map header prevented admission", err)
			}
			defer c.Close()
			if state := c.Snapshot(); !state.Monitor.Ready || state.Manager.GlobalID != 99 || state.Manager.MapEpoch != 1 {
				t.Fatal("buffered compatible MGR map was not published", state)
			}
			if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || !json.Valid(result.Data) {
				t.Fatal("compatible map did not route the MGR command", err)
			}
		})
	}
}

func TestIncompatibleMapCandidatePreservesAcceptedManagerAndMutation(t *testing.T) {
	for _, badType := range []uint16{msgr.MonMapMessage, msgr.MgrMapMessage} {
		name := "MON"
		if badType == msgr.MgrMapMessage {
			name = "buffered MGR"
		}
		t.Run(name, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			started := make(chan struct{})
			var monitors, mutations atomic.Uint32
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				client, peer := net.Pipe()
				cfg := peerConfig{role: 1, release: 20, fsid: [16]byte{1}}
				if strings.HasSuffix(endpoint, ":3300") && monitors.Add(1) > 1 {
					messages := []msgr.MessageData{
						{Type: msgr.MgrMapMessage, Version: 1, CompatVersion: 1, Front: mockMgrMap(9, 108, 6809)},
						{Type: msgr.MonMapMessage, Version: 1, CompatVersion: 1, Front: mockMonMap(20, [16]byte{1})},
					}
					for i := range messages {
						if messages[i].Type == badType {
							messages[i].Version, messages[i].CompatVersion = 2, 2
						}
					}
					go mapHeaderPeer(peer, messages)
					return client, nil
				}
				if strings.HasSuffix(endpoint, ":6800") {
					cfg.role, cfg.id = 16, 99
					cfg.command = func(m msgr.MessageData) {
						if strings.Contains(string(m.Front), "block mutation") && mutations.Add(1) == 1 {
							close(started)
						}
					}
				}
				go mockDaemon(peer, cfg)
				return client, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.WaitMgrReady(ctx); err != nil {
				t.Fatal(err)
			}
			stopMutation, finished := pendingAdmissionMutation(t, c, ctx, started)
			before := c.Snapshot()
			c.mu.Lock()
			oldMon, oldMgr := c.mon, c.mgr
			c.mu.Unlock()
			if err := c.connectMonitor(ctx); !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, wire.ErrVersion) || retryableSetup(err) {
				t.Fatal("incompatible map candidate was admitted or misclassified", err)
			}
			c.mu.Lock()
			preserved := c.mon == oldMon && c.monReady && c.mgr == oldMgr
			c.mu.Unlock()
			after := c.Snapshot()
			if !preserved || !reflect.DeepEqual(after.Monitor, before.Monitor) || !reflect.DeepEqual(after.Manager, before.Manager) || oldMgr.Err() != nil {
				t.Fatal("unsupported map changed the accepted sessions or routing", preserved, before, after, oldMgr.Err())
			}
			select {
			case err := <-finished:
				t.Fatal("unsupported map interrupted an existing mutation", err)
			default:
			}
			if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || !json.Valid(result.Data) {
				t.Fatal("accepted manager was unusable after map rejection", err)
			}
			stopMutation()
			var unknown *OutcomeUnknownError
			if err := admissionResult(t, ctx, finished); !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) || mutations.Load() != 1 {
				t.Fatal("pending mutation lost cancellation uncertainty or was replayed", err, mutations.Load())
			}
		})
	}
}
