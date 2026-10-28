package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func managerSetupCall(ctx context.Context, c *Client, kind string) error {
	if kind == "service" {
		svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "waiting", DisableBeacon: true})
		if err != nil {
			return err
		}
		return svc.Close(ctx)
	}
	_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
	return err
}

func TestManagerSetupGateWaitPreservesOwnershipAndKnownOutcome(t *testing.T) {
	for _, kind := range []string{"command", "service"} {
		for _, ending := range []string{"cancel", "client-close"} {
			t.Run(kind+"/"+ending, func(t *testing.T) {
				mgr := &serviceMGR{ack: true}
				c, ctx := dialService(t, mgr)
				// Keep another setup's admission occupied without starting a
				// handshake. The waiting call must neither dial nor release it.
				c.mgrGate <- struct{}{}
				held := true
				defer func() {
					if held {
						<-c.mgrGate
					}
				}()
				call, cancel := context.WithCancel(ctx)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- managerSetupCall(call, c, kind) }()
				waitFor(t, ctx, "MGR call admission", func() bool {
					if kind == "command" {
						return len(c.calls) == 1
					}
					c.mu.Lock()
					defer c.mu.Unlock()
					return len(c.mgrServices) == 1
				})
				want := context.Canceled
				if ending == "client-close" {
					want = ErrClosed
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				select {
				case err := <-result:
					var unknown *OutcomeUnknownError
					if !errors.Is(err, want) || errors.As(err, &unknown) || errors.Is(err, ErrMgrServiceOutcomeUnknown) {
						t.Fatal("gate wait lost known-unsent classification", err)
					}
				case <-ctx.Done():
					t.Fatal("gate wait did not finish", ctx.Err())
				}
				mgr.mu.Lock()
				dials := mgr.connects
				mgr.mu.Unlock()
				c.mu.Lock()
				reserved := len(c.mgrServices)
				c.mu.Unlock()
				if len(c.mgrGate) != 1 || len(c.calls) != 0 || reserved != 0 || dials != 0 {
					t.Fatal("gate owner or call capacity changed", len(c.mgrGate), len(c.calls), reserved, dials)
				}
				if ending == "cancel" {
					<-c.mgrGate
					held = false
					if err := managerSetupCall(ctx, c, kind); err != nil {
						t.Fatal("explicit next call could not reuse setup capacity", err)
					}
					if len(c.mgrGate) != 0 {
						t.Fatal("completed setup retained the gate")
					}
				}
			})
		}
	}
}

func TestManagerSetupAfterCloseIsKnownUnsent(t *testing.T) {
	for _, kind := range []string{"command", "service"} {
		t.Run(kind, func(t *testing.T) {
			mgr := &serviceMGR{ack: true}
			c, ctx := dialService(t, mgr)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			call, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			err := managerSetupCall(call, c, kind)
			var unknown *OutcomeUnknownError
			if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) || errors.Is(err, ErrMgrServiceOutcomeUnknown) {
				t.Fatal("closed client admitted MGR setup", err)
			}
			if len(c.mgrGate) != 0 {
				t.Fatal("refused setup retained the gate")
			}
			mgr.mu.Lock()
			defer mgr.mu.Unlock()
			if mgr.connects != 0 {
				t.Fatal("closed client dialed the MGR", mgr.connects)
			}
		})
	}
}
