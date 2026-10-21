package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// serviceMGR is a synthetic MGR for service registration. It shares the Go
// frame codec, so it checks client behavior and routing, not wire compatibility;
// the byte layouts are pinned separately in internal/msgr/mgr_service_test.go.
type serviceMGR struct {
	mu        sync.Mutex
	received  []msgr.MessageData
	period    uint32 // seconds in the configure push
	silent    bool   // never answers the open
	ack       bool   // echoes MMgrClose
	closeConn bool   // drops the connection after the open
	connects  int
}

func (m *serviceMGR) messages(typ uint16) []msgr.MessageData {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []msgr.MessageData
	for _, got := range m.received {
		if got.Type == typ {
			out = append(out, got)
		}
	}
	return out
}

func (m *serviceMGR) handle(got msgr.MessageData, send func(msgr.MessageData) error) (bool, error) {
	switch got.Type {
	case msgr.MgrOpenMessage, msgr.MgrUpdateMessage, msgr.MgrReportMessage, msgr.MgrCloseMessage:
	default:
		return false, nil
	}
	m.mu.Lock()
	m.received = append(m.received, got)
	period, silent, ack, drop := m.period, m.silent, m.ack, m.closeConn
	m.mu.Unlock()
	switch {
	case got.Type == msgr.MgrOpenMessage && drop:
		return true, errors.New("synthetic MGR drops the connection")
	case got.Type == msgr.MgrOpenMessage && !silent:
		front := binary.LittleEndian.AppendUint32(nil, period)
		front = binary.LittleEndian.AppendUint32(front, 3)
		front = binary.LittleEndian.AppendUint32(front, 0)
		front = append(front, 0)
		return true, send(msgr.MessageData{Type: msgr.MgrConfigureMessage, Version: 4, CompatVersion: 1, Front: front})
	case got.Type == msgr.MgrCloseMessage && ack:
		return true, send(msgr.MessageData{Type: msgr.MgrCloseMessage, Version: 1, CompatVersion: 1, Front: got.Front})
	}
	return true, nil
}

func serviceOptions(t *testing.T, mgr *serviceMGR) Options {
	t.Helper()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMgrService = true
	base := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		var id uint64
		switch {
		case strings.HasSuffix(endpoint, ":6800"):
			id = 99
		case strings.HasSuffix(endpoint, ":6801"):
			id = 100
		default:
			return base(ctx, network, endpoint)
		}
		mgr.mu.Lock()
		mgr.connects++
		mgr.mu.Unlock()
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 16, id: id, service: mgr.handle})
		return client, nil
	}
	return options
}

func dialService(t *testing.T, mgr *serviceMGR) (*Client, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	c, err := Dial(ctx, serviceOptions(t, mgr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, ctx
}

func waitFor(t *testing.T, ctx context.Context, what string, ok func() bool) {
	t.Helper()
	for !ok() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestMgrServiceOpenUpdateReportAndAcknowledgedClose(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true,
		Metadata: map[string]string{"zone": "a"}, Status: map[string]string{"state": "up"}})
	if err != nil {
		t.Fatal(err)
	}
	wantOpen, _ := msgr.MgrOpen("svc", "d1", map[string]string{"zone": "a"}, map[string]string{"state": "up"})
	opens := mgr.messages(msgr.MgrOpenMessage)
	if len(opens) != 1 || !bytes.Equal(opens[0].Front, wantOpen.Front) || opens[0].Version != 3 {
		t.Fatalf("open = %+v", opens)
	}
	if cfg := svc.Configure(); cfg.StatsPeriod != 0 || cfg.StatsThreshold != 3 || cfg.Extension != nil {
		t.Fatalf("configure = %+v", cfg)
	}
	if err := svc.UpdateMetadata(ctx, map[string]string{"zone": "b"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Report(ctx, map[string]string{"state": "busy"}, map[string]string{"task": "x"}); err != nil {
		t.Fatal(err)
	}
	wantUpdate, _ := msgr.MgrUpdate("svc", "d1", map[string]string{"zone": "b"})
	wantReport, _ := msgr.MgrReport("svc", "d1", map[string]string{"state": "busy"}, map[string]string{"task": "x"})
	waitFor(t, ctx, "update and report", func() bool {
		return len(mgr.messages(msgr.MgrUpdateMessage)) == 1 && len(mgr.messages(msgr.MgrReportMessage)) == 1
	})
	if !bytes.Equal(mgr.messages(msgr.MgrUpdateMessage)[0].Front, wantUpdate.Front) || !bytes.Equal(mgr.messages(msgr.MgrReportMessage)[0].Front, wantReport.Front) {
		t.Fatal("update or report bytes differ")
	}
	if err := svc.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(mgr.messages(msgr.MgrCloseMessage)) != 1 || svc.Err() != nil {
		t.Fatal("close was not sent once or recorded an error")
	}
	select {
	case <-svc.Done():
	default:
		t.Fatal("Done is open after Close")
	}
	if err := svc.Report(ctx, nil, nil); !errors.Is(err, ErrMgrServiceClosed) {
		t.Fatal("report after close:", err)
	}
	if err := svc.Close(ctx); err != nil {
		t.Fatal("Close is not idempotent:", err)
	}
	// The identity is free again for an explicit new registration.
	again, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal("re-registration after close:", err)
	}
	again.Close(ctx)
}

func TestMgrServiceRequiresOptInAndValidIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, [16]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d"}); !errors.Is(err, ErrMgrServiceDisabled) {
		t.Fatal("opt-in not required:", err)
	}
	mgr := &serviceMGR{}
	enabled, ctx2 := dialService(t, mgr)
	for _, bad := range []MgrServiceOptions{{Daemon: "d"}, {Service: "s"}, {Service: "s\x00", Daemon: "d"}} {
		if _, err := enabled.OpenMgrService(ctx2, bad); !errors.Is(err, msgr.ErrServiceRegistration) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
	if mgr.connects != 0 {
		t.Fatal("a refused registration dialed the MGR")
	}
}

func TestMgrServiceDuplicateAndBoundAreRefusedBeforeSending(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, ctx := dialService(t, mgr)
	first, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	opens := len(mgr.messages(msgr.MgrOpenMessage))
	if _, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1"}); !errors.Is(err, ErrMgrServiceExists) {
		t.Fatal("duplicate:", err)
	}
	if len(mgr.messages(msgr.MgrOpenMessage)) != opens {
		t.Fatal("a duplicate registration sent an open")
	}
	var open []*MgrService
	for i := 1; i < MaxMgrServices; i++ {
		s, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "n" + string(rune('a'+i)), DisableBeacon: true})
		if err != nil {
			t.Fatal(i, err)
		}
		open = append(open, s)
	}
	if _, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "over", DisableBeacon: true}); !errors.Is(err, ErrMgrServiceExists) {
		t.Fatal("bound:", err)
	}
	first.Close(ctx)
	for _, s := range open {
		s.Close(ctx)
	}
}

func TestMgrServiceBeaconFollowsTheConfiguredPeriod(t *testing.T) {
	mgr := &serviceMGR{ack: true, period: 1}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close(ctx)
	if svc.Configure().StatsPeriod != time.Second {
		t.Fatal("period not recorded", svc.Configure())
	}
	waitFor(t, ctx, "a beacon", func() bool { return len(mgr.messages(msgr.MgrReportMessage)) >= 1 })
	want, _ := msgr.MgrReport("svc", "d1", nil, nil)
	if got := mgr.messages(msgr.MgrReportMessage)[0]; !bytes.Equal(got.Front, want.Front) {
		t.Fatal("beacon is not the empty report")
	}
}

func TestMgrServiceZeroPeriodAndDisabledBeaconSendNoReports(t *testing.T) {
	mgr := &serviceMGR{ack: true, period: 0}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if n := len(mgr.messages(msgr.MgrReportMessage)); n != 0 {
		t.Fatalf("%d reports with a zero period", n)
	}
	svc.Close(ctx)
}

func TestMgrServiceUnconfirmedOpenAndCloseKeepUncertainty(t *testing.T) {
	silent := &serviceMGR{silent: true}
	c, ctx := dialService(t, silent)
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := c.OpenMgrService(short, MgrServiceOptions{Service: "svc", Daemon: "d1"}); !errors.Is(err, ErrMgrServiceOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("silent MGR:", err)
	}
	if len(silent.messages(msgr.MgrOpenMessage)) != 1 {
		t.Fatal("the open was not sent exactly once")
	}
	// The failed handle released its identity, but nothing re-opens it implicitly.
	time.Sleep(100 * time.Millisecond)
	if len(silent.messages(msgr.MgrOpenMessage)) != 1 {
		t.Fatal("the library resent the open")
	}

	noAck := &serviceMGR{ack: false}
	c2, ctx2 := dialService(t, noAck)
	svc, err := c2.OpenMgrService(ctx2, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	closeCtx, cancelClose := context.WithTimeout(ctx2, 200*time.Millisecond)
	defer cancelClose()
	if err := svc.Close(closeCtx); !errors.Is(err, ErrMgrServiceCloseUnconfirmed) {
		t.Fatal("unacknowledged close:", err)
	}
	select {
	case <-svc.Done():
	default:
		t.Fatal("handle still open after an unconfirmed close")
	}
}

func TestMgrServiceFailsWhenTheConnectionOrActiveManagerEnds(t *testing.T) {
	dropping := &serviceMGR{closeConn: true}
	c, ctx := dialService(t, dropping)
	if _, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1"}); !errors.Is(err, ErrMgrServiceOutcomeUnknown) {
		t.Fatal("dropped connection:", err)
	}

	mgr := &serviceMGR{ack: true}
	c2, ctx2 := dialService(t, mgr)
	svc, err := c2.OpenMgrService(ctx2, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	// The MON publishes a new active MGR: the handle is bound to the old one.
	if _, err := c2.MonCommand(ctx2, Command{JSON: []byte(`{"prefix":"switch"}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx2, "the handle to fail", func() bool {
		select {
		case <-svc.Done():
			return true
		default:
			return false
		}
	})
	if err := svc.Err(); !errors.Is(err, ErrManagerChanged) {
		t.Fatal("failure cause:", err)
	}
	if err := svc.Report(ctx2, nil, nil); !errors.Is(err, ErrMgrServiceClosed) || !errors.Is(err, ErrManagerChanged) {
		t.Fatal("report on a failed handle:", err)
	}
	if err := svc.Close(ctx2); !errors.Is(err, ErrManagerChanged) {
		t.Fatal("close on a failed handle:", err)
	}
	connects := mgr.connects
	time.Sleep(100 * time.Millisecond)
	if mgr.connects != connects || len(mgr.messages(msgr.MgrOpenMessage)) != 1 {
		t.Fatal("the library re-registered on its own after failover")
	}
}

func TestMgrServiceDoesNotChangeOrdinaryManagerCommands(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close(ctx)
	got, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
	if err != nil || string(got.Data) != `{"ok":true}` {
		t.Fatal("ordinary MGR command beside a service session:", got, err)
	}
	if mgr.connects < 2 {
		t.Fatal("the registration did not use its own connection", mgr.connects)
	}
}

func TestMgrServiceCloseOfTheClientEndsHandles(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	select {
	case <-svc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("handle survived Client.Close")
	}
	if _, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d2"}); err == nil {
		t.Fatal("registration on a closed client")
	}
}

func TestMgrServiceConcurrentCloseHonorsEachContextAndSharesTheResult(t *testing.T) {
	mgr := &serviceMGR{ack: false}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancelFirst()
	first := make(chan error, 1)
	go func() { first <- svc.Close(firstCtx) }()
	waitFor(t, ctx, "the first Close to send MMgrClose", func() bool { return len(mgr.messages(msgr.MgrCloseMessage)) == 1 })

	// A second caller with a short deadline must return on its own deadline,
	// as unconfirmed, while the first caller is still waiting.
	secondCtx, cancelSecond := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancelSecond()
	began := time.Now()
	err = svc.Close(secondCtx)
	if elapsed := time.Since(began); elapsed > 300*time.Millisecond {
		t.Fatalf("second Close ignored its deadline for %v", elapsed)
	}
	if !errors.Is(err, ErrMgrServiceCloseUnconfirmed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("second Close:", err)
	}
	select {
	case err := <-first:
		t.Fatal("the first Close ended early:", err)
	default:
	}

	// The first call's terminal result is unconfirmed, and later and
	// concurrent callers receive that same result rather than success.
	if err := <-first; !errors.Is(err, ErrMgrServiceCloseUnconfirmed) {
		t.Fatal("first Close:", err)
	}
	if err := svc.Close(ctx); !errors.Is(err, ErrMgrServiceCloseUnconfirmed) {
		t.Fatal("a later Close turned an unconfirmed close into success:", err)
	}
	if n := len(mgr.messages(msgr.MgrCloseMessage)); n != 1 {
		t.Fatalf("%d MMgrClose messages were sent", n)
	}
}

func TestMgrServiceConcurrentCloseOfAnAcknowledgedCloseSucceedsForEveryCaller(t *testing.T) {
	mgr := &serviceMGR{ack: true}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "d1", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	for i := 0; i < cap(results); i++ {
		go func() { results <- svc.Close(ctx) }()
	}
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			t.Fatal("concurrent Close:", err)
		}
	}
	if n := len(mgr.messages(msgr.MgrCloseMessage)); n != 1 {
		t.Fatalf("%d MMgrClose messages were sent", n)
	}
}
