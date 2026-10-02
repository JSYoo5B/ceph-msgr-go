package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephReadinessIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, time.Minute)
	if err := c.WaitMonReady(ctx); err != nil {
		t.Fatal("MON preparation", err)
	}
	initial := c.Snapshot()
	if !initial.Monitor.Ready || initial.Manager.Ready {
		t.Fatal("MON preparation opened a lazy MGR connection", initial)
	}
	prepared := make(chan error, 8)
	for i := 0; i < cap(prepared); i++ {
		go func() { prepared <- c.WaitMgrReady(ctx) }()
	}
	for i := 0; i < cap(prepared); i++ {
		select {
		case err := <-prepared:
			if err != nil {
				t.Fatal("concurrent MGR preparation", err)
			}
		case <-ctx.Done():
			t.Fatal("MGR preparation left a caller waiting", ctx.Err())
		}
	}
	waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.Monitor.Ready && s.Manager.Ready })

	// Exercise genuine background ticket renewal on connections prepared
	// without any management command from this client.
	renewed := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Ready && s.AuthTicket.Expires.After(initial.AuthTicket.Expires) && s.MgrTicket.Expires.After(initial.MgrTicket.Expires)
	})
	if renewed.GlobalID != initial.GlobalID {
		t.Fatal("idle preparation lost the authenticated identity", renewed)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); err != nil {
			t.Fatal("preparation after idle renewal", err)
		}
	}

	// A separate administrator causes the actual active MGR takeover.
	observer, _ := fixtureClient(t, time.Minute)
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": renewed.Manager.Name})
	if _, err := observer.MonCommand(ctx, cephmsgr.Command{JSON: encoded}); err != nil {
		t.Fatal("fail active MGR", err)
	}
	replacement := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.GlobalID != renewed.Manager.GlobalID
	})
	if replacement.Manager.Ready || replacement.Manager.MapEpoch <= renewed.Manager.MapEpoch {
		t.Fatal("advertised replacement was already reported as connected", replacement)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare replacement MGR", err)
	}
	waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.Ready && s.Manager.GlobalID == replacement.Manager.GlobalID
	})

	canceled, stop := context.WithCancel(ctx)
	stop()
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *cephmsgr.OutcomeUnknownError
		if err := wait(canceled); !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("canceled preparation claimed uncertain execution", err)
		}
		if err := wait(ctx); err != nil {
			t.Fatal("canceling preparation canceled the real client", err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *cephmsgr.OutcomeUnknownError
		if err := wait(ctx); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("closed preparation lost known non-execution", err)
		}
	}
	t.Logf("MON/MGR preparation, idle ticket renewal, takeover, local cancellation and Close passed without client management commands; MGR %s -> %s", renewed.Manager.Name, replacement.Manager.Name)
}
