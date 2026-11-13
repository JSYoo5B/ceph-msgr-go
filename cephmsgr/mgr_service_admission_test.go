package cephmsgr

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// The setup admission of dialMgrService ends when the dial returns, and the
// handle's workers are admitted afterwards. These tests drive the two steps of
// OpenMgrService separately, so Client.Close runs exactly between them without
// a sleep or a hook in the product.
func dialedMgrService(t *testing.T, mgr *serviceMGR, daemon string) (*Client, *MgrService) {
	t.Helper()
	c, ctx := dialService(t, mgr)
	svc := c.newMgrService(MgrServiceOptions{Service: "svc", Daemon: daemon, DisableBeacon: true})
	if err := c.reserveMgrService(svc); err != nil {
		t.Fatal(err)
	}
	if err := c.dialMgrService(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if len(c.mgrGate) != 0 {
		t.Fatal("a returned dial kept its setup admission")
	}
	return c, svc
}

func reservedMgrServices(c *Client) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.mgrServices)
}

func TestMgrServiceWorkersAreNotAdmittedAfterClose(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, svc := dialedMgrService(t, mgr, "shutdown-wins")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	err := c.startMgrServiceWorkers(svc)
	if !errors.Is(err, ErrClosed) || errors.Is(err, ErrMgrServiceOutcomeUnknown) {
		t.Fatal("workers were admitted after Close returned, or the refusal is not a known local close:", err)
	}
	select {
	case <-svc.Done():
	default:
		t.Fatal("the refused handle is not terminal")
	}
	if got := svc.Err(); !errors.Is(got, ErrClosed) {
		t.Fatal("terminal cause of the refused handle:", got)
	}
	if n := reservedMgrServices(c); n != 0 {
		t.Fatal("the refused open kept its reserved identity:", n)
	}
	if opens := mgr.messages(msgr.MgrOpenMessage); len(opens) != 0 {
		t.Fatal("a registration reached the MGR:", len(opens))
	}
}

func TestMgrServiceWorkersAdmittedBeforeCloseEndWithIt(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, svc := dialedMgrService(t, mgr, "admission-wins")
	if err := c.startMgrServiceWorkers(svc); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Close returned, so the client's worker group is empty: a second wait
	// must not block on a worker that was admitted above.
	c.wg.Wait()
	select {
	case <-svc.Done():
	default:
		t.Fatal("Close returned before the admitted handle ended")
	}
	if got := svc.Err(); !errors.Is(got, ErrClosed) {
		t.Fatal("terminal cause of the handle:", got)
	}
	if n := reservedMgrServices(c); n != 0 {
		t.Fatal("the closed client kept a reserved identity:", n)
	}
	if opens := mgr.messages(msgr.MgrOpenMessage); len(opens) != 0 {
		t.Fatal("a registration reached the MGR:", len(opens))
	}
}
