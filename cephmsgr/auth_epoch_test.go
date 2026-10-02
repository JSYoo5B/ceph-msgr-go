package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func epochMonMap(epoch, authEpoch uint32) []byte {
	return mockMonMapAuthEpoch(20, [16]byte{1}, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, epoch, authEpoch)
}

type epochFixture struct {
	monDials, mgrDials, mutations atomic.Uint32
	started                       chan struct{}
}

func epochClient(t *testing.T, initialEpoch uint32, onMonDial func(context.Context, uint32, *peerConfig) error) (*Client, context.Context, *epochFixture) {
	t.Helper()
	fixture := &epochFixture{started: make(chan struct{})}
	options := mockOptions(t, 20, [16]byte{1})
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		cfg := peerConfig{role: 1, release: 20, fsid: [16]byte{1}, monMap: epochMonMap(1, initialEpoch)}
		switch {
		case strings.HasSuffix(endpoint, ":3300"):
			round := fixture.monDials.Add(1)
			if onMonDial != nil {
				if err := onMonDial(ctx, round, &cfg); err != nil {
					return nil, err
				}
			}
		case strings.HasSuffix(endpoint, ":6800"):
			fixture.mgrDials.Add(1)
			cfg.role, cfg.id = 16, 99
			cfg.command = func(m msgr.MessageData) {
				if strings.Contains(string(m.Front), "block mutation") && fixture.mutations.Add(1) == 1 {
					close(fixture.started)
				}
			}
		default:
			return nil, errors.New("unexpected auth-epoch fixture endpoint")
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
	return c, ctx, fixture
}

func waitEpochTickets(t *testing.T, c *Client, ctx context.Context) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state := c.Snapshot()
		if state.Monitor.Ready && state.AuthTicket.Expires.After(time.Now()) && state.MgrTicket.Expires.After(time.Now()) {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("auth-epoch renewal did not publish fresh tickets", ctx.Err())
		}
	}
}

func TestAuthEpochInvalidatesNewAuthorizersAndPreservesEstablishedManager(t *testing.T) {
	for _, established := range []bool{false, true} {
		name := "cold manager"
		if established {
			name = "established manager"
		}
		t.Run(name, func(t *testing.T) {
			renewing, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			c, ctx, fixture := epochClient(t, 0, func(ctx context.Context, round uint32, cfg *peerConfig) error {
				if round == 2 {
					close(renewing)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				if round > 1 {
					cfg.monMap = epochMonMap(2, 7)
				}
				return nil
			})
			var mutation <-chan error
			var cancelMutation context.CancelFunc
			if established {
				if err := c.WaitMgrReady(ctx); err != nil {
					t.Fatal(err)
				}
				cancelMutation, mutation = pendingAdmissionMutation(t, c, ctx, fixture.started)
			}
			before := c.snapshotAuth()
			c.mu.Lock()
			mon, mgr := c.mon, c.mgr
			c.mu.Unlock()
			if accepted, err := c.handleMap(mon, msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: epochMonMap(2, 7)}); !accepted || err != nil {
				t.Fatal("valid auth-epoch notification was rejected", accepted, err)
			}
			invalidated := c.snapshotAuth()
			if _, err := invalidated.Authorizer(cephx.ServiceMgr); !errors.Is(err, cephx.ErrTicket) {
				t.Fatal("new MGR authorizer admitted a wiped service ticket", err)
			}
			if invalidated.GlobalID != before.GlobalID || !bytes.Equal(invalidated.Tickets[cephx.ServiceAuth].Blob, before.Tickets[cephx.ServiceAuth].Blob) || invalidated.Tickets[cephx.ServiceAuth].SecretID != before.Tickets[cephx.ServiceAuth].SecretID {
				t.Fatal("auth-epoch notification discarded the reclaimable AUTH proof")
			}
			select {
			case <-renewing:
			case <-ctx.Done():
				t.Fatal("auth-epoch notification did not wake ticket renewal", ctx.Err())
			}
			if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("ticket invalidation interrupted the healthy MON", err)
			}
			if established {
				if mgr.Err() != nil {
					t.Fatal("ticket invalidation failed the established MGR", mgr.Err())
				}
				select {
				case err := <-mutation:
					t.Fatal("ticket invalidation interrupted a started mutation", err)
				default:
				}
			} else {
				wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				_, err := c.MgrCommand(wait, Command{JSON: []byte(`{"prefix":"status"}`)})
				cancel()
				var unknown *OutcomeUnknownError
				if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || fixture.mgrDials.Load() != 0 {
					t.Fatal("cold MGR did not wait locally for renewed service ticket", err, fixture.mgrDials.Load())
				}
			}
			release <- struct{}{}
			waitEpochTickets(t, c, ctx)
			if c.Snapshot().GlobalID != before.GlobalID || fixture.monDials.Load() != 2 {
				t.Fatal("auth-epoch renewal reset identity or repeated authentication")
			}
			input := []byte{0, 255, 1}
			if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: input}); err != nil || !bytes.Equal(result.Data, input) {
				t.Fatal("renewed epoch could not serve a new raw MGR command", result, err)
			}
			if established {
				c.mu.Lock()
				preserved := c.mgr == mgr
				c.mu.Unlock()
				if !preserved || mgr.Err() != nil {
					t.Fatal("successful ticket renewal replaced the healthy MGR")
				}
				cancelMutation()
				var unknown *OutcomeUnknownError
				if err := admissionResult(t, ctx, mutation); !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) || fixture.mutations.Load() != 1 {
					t.Fatal("established mutation lost uncertainty or was replayed", err, fixture.mutations.Load())
				}
			}
		})
	}
}

func TestAuthEpochCandidateCannotRestoreInvalidTickets(t *testing.T) {
	for _, lagging := range []bool{false, true} {
		name, initialEpoch := "advance after authentication", uint32(7)
		if lagging {
			name, initialEpoch = "lagging candidate map", 8
		}
		t.Run(name, func(t *testing.T) {
			refreshing, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			c, ctx, fixture := epochClient(t, initialEpoch, func(ctx context.Context, round uint32, cfg *peerConfig) error {
				if round == 2 {
					cfg.monMap = epochMonMap(2, 8)
					if lagging {
						cfg.monMap = epochMonMap(1, 7)
					}
				}
				if round == 3 {
					close(refreshing)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					cfg.monMap = epochMonMap(2, 8)
				}
				return nil
			})
			initial := c.snapshotAuth()
			if _, err := initial.Authorizer(cephx.ServiceMgr); err != nil || fixture.monDials.Load() != 1 {
				t.Fatal("initial nonzero auth epoch invalidated newly authenticated tickets", err)
			}
			c.mu.Lock()
			firstMonitor := c.mon
			c.mu.Unlock()
			if lagging {
				if _, err := c.handleMap(firstMonitor, msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: epochMonMap(2, 8)}); err != nil {
					t.Fatal(err)
				}
			}
			// Trigger one ordinary renewal. Its AUTH exchange completes before
			// the candidate sends the changed/lagging MonMap over the wire.
			updated := c.snapshotAuth()
			ticket := updated.Tickets[cephx.ServiceAuth]
			ticket.RenewAfter = time.Now().Add(-time.Second)
			updated.Tickets[cephx.ServiceAuth] = ticket
			c.mu.Lock()
			c.auth = updated
			c.mu.Unlock()
			c.wakeMonitor()
			select {
			case <-refreshing:
			case <-ctx.Done():
				t.Fatal("candidate publication resurrected older-epoch tickets", ctx.Err(), fixture.monDials.Load())
			}
			if _, err := c.snapshotAuth().Authorizer(cephx.ServiceMgr); !errors.Is(err, cephx.ErrTicket) {
				t.Fatal("candidate restored invalidated MGR authorizer admission", err)
			}
			wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			err := c.WaitMgrReady(wait)
			cancel()
			var unknown *OutcomeUnknownError
			if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || fixture.mgrDials.Load() != 0 {
				t.Fatal("candidate let a cold MGR use older-epoch credentials", err)
			}
			select {
			case release <- struct{}{}:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			waitEpochTickets(t, c, ctx)
			if c.Snapshot().GlobalID != initial.GlobalID || fixture.monDials.Load() != 3 {
				t.Fatal("epoch fencing reset identity or caused repeated invalidation")
			}
			c.mu.Lock()
			auth := c.auth
			c.mu.Unlock()
			if accepted, err := c.handleMap(firstMonitor, msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: epochMonMap(99, 99)}); accepted || err != nil {
				t.Fatal("superseded MON could invalidate current tickets", accepted, err)
			}
			c.mu.Lock()
			preserved := c.auth == auth
			c.mu.Unlock()
			if !preserved {
				t.Fatal("superseded MON changed current epoch credentials")
			}
			if err := c.WaitMgrReady(ctx); err != nil || fixture.mgrDials.Load() != 1 {
				t.Fatal("fenced renewal could not establish a new MGR", err)
			}
		})
	}
}
