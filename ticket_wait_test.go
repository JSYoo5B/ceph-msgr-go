package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestManagerWaitsForDelayedTicketRenewal(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	renewing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var monDials, mgrDials atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":3300") {
			if monDials.Add(1) > 1 {
				once.Do(func() { close(renewing) })
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		} else {
			mgrDials.Add(1)
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer close(release)
	updated := c.snapshotAuth()
	ticket := updated.Tickets[cephx.ServiceMgr]
	ticket.Expires, ticket.RenewAfter = time.Now().Add(-time.Second), time.Now().Add(-time.Second)
	updated.Tickets[cephx.ServiceMgr] = ticket
	c.mu.Lock()
	c.auth = updated
	c.mu.Unlock()
	c.wakeMonitor()
	select {
	case <-renewing:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("manager call returned before renewal completed", err)
	case <-time.After(20 * time.Millisecond):
	}
	if mgrDials.Load() != 0 {
		t.Fatal("expired ticket started an MGR handshake")
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("waiting for ticket renewal blocked MON commands", err)
	}
	canceled, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	_, err = c.MgrCommand(canceled, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var uncertain *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &uncertain) {
		t.Fatal("untransmitted ticket wait did not respect context", err)
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("renewed ticket did not release MGR request", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if mgrDials.Load() != 1 {
		t.Fatal("MGR dispatched more than once", mgrDials.Load())
	}
}
