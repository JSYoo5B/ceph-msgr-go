package cephmsgr

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func serviceAutomaticReports(mgr *serviceMGR) int {
	empty, _ := msgr.MgrReport("svc", "reconfigure", nil, nil)
	n := 0
	for _, report := range mgr.messages(msgr.MgrReportMessage) {
		if bytes.Equal(report.Front, empty.Front) {
			n++
		}
	}
	return n
}

// The peer processes requests and sends pushes on one goroutine. Triggering
// through a nonempty test report avoids concurrently writing its frame codec.
func pushServiceConfigure(t *testing.T, ctx context.Context, mgr *serviceMGR, svc *MgrService, pushes ...msgr.MessageData) {
	t.Helper()
	status := map[string]string{"configure": fmt.Sprint(len(mgr.messages(msgr.MgrReportMessage)))}
	request, _ := msgr.MgrReport("svc", "reconfigure", status, nil)
	mgr.mu.Lock()
	if mgr.reconfigure == nil {
		mgr.reconfigure = make(map[string][]msgr.MessageData)
	}
	mgr.reconfigure[string(request.Front)] = pushes
	mgr.mu.Unlock()
	if err := svc.Report(ctx, status, nil); err != nil {
		t.Fatal("request synthetic configure push", err)
	}
	last, err := msgr.DecodeMgrConfigure(pushes[len(pushes)-1], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "latest configure push", func() bool {
		cfg := svc.Configure()
		return cfg.StatsPeriod == time.Duration(last.StatsPeriod)*time.Second && cfg.StatsThreshold == last.StatsThreshold
	})
}

func TestMgrServiceReconfigurationControlsBeaconAndKeepsSession(t *testing.T) {
	mgr := &serviceMGR{ack: true, period: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Dial(ctx, serviceOptions(t, mgr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "reconfigure"})
	if err != nil {
		t.Fatal(err)
	}
	// Exercise concurrent snapshots while receive and timer goroutines process
	// the changes; -race checks their shared state separately from byte parity.
	readDone := make(chan struct{})
	defer func() { <-readDone }()
	go func() {
		defer close(readDone)
		for {
			select {
			case <-svc.Done():
				return
			case <-time.After(time.Millisecond):
				_ = svc.Configure()
			}
		}
	}()
	defer svc.Close(ctx)
	waitFor(t, ctx, "initial automatic report", func() bool { return serviceAutomaticReports(mgr) > 0 })
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(0, 7))
	// A report can already have passed local admission when the push arrives.
	// Let that report settle before starting the no-new-report observation.
	time.Sleep(1100 * time.Millisecond)
	before := serviceAutomaticReports(mgr)
	time.Sleep(1200 * time.Millisecond)
	if got := serviceAutomaticReports(mgr); got != before {
		t.Fatal("zero period admitted another automatic report after settling", before, got)
	}
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(2, 7))
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(2, 9)) // threshold-only replacement
	waitFor(t, ctx, "resumed two-second report", func() bool { return serviceAutomaticReports(mgr) > before })
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(0, 10), serviceConfigure(3, 11), serviceConfigure(0, 12), serviceConfigure(1, 13))
	before = serviceAutomaticReports(mgr)
	waitFor(t, ctx, "latest rapid-push period report", func() bool { return serviceAutomaticReports(mgr) > before })
	if err := svc.Close(ctx); err != nil {
		t.Fatal("close reconfigured session", err)
	}
	before = serviceAutomaticReports(mgr)
	time.Sleep(1100 * time.Millisecond)
	mgr.mu.Lock()
	dials := mgr.connects
	mgr.mu.Unlock()
	if serviceAutomaticReports(mgr) != before || dials != 1 || len(mgr.messages(msgr.MgrOpenMessage)) != 1 || len(mgr.messages(msgr.MgrCloseMessage)) != 1 {
		t.Fatal("reconfiguration reopened the session or reports survived Close", dials)
	}
}

func TestMgrServiceReconfigurationCannotEnableDisabledBeacons(t *testing.T) {
	mgr := &serviceMGR{ack: true, period: 1}
	c, ctx := dialService(t, mgr)
	svc, err := c.OpenMgrService(ctx, MgrServiceOptions{Service: "svc", Daemon: "reconfigure", DisableBeacon: true})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close(ctx)
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(0, 6))
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(2, 6))
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(2, 8))
	pushServiceConfigure(t, ctx, mgr, svc, serviceConfigure(0, 9), serviceConfigure(1, 10))
	time.Sleep(1200 * time.Millisecond)
	if got := serviceAutomaticReports(mgr); got != 0 {
		t.Fatal("configure enabled disabled automatic beacons", got)
	}
	if err := svc.Report(ctx, map[string]string{"explicit": "still-usable"}, nil); err != nil {
		t.Fatal("explicit report after reconfiguration", err)
	}
	want, _ := msgr.MgrReport("svc", "reconfigure", map[string]string{"explicit": "still-usable"}, nil)
	waitFor(t, ctx, "explicit report with automatic beacons disabled", func() bool {
		for _, got := range mgr.messages(msgr.MgrReportMessage) {
			if bytes.Equal(got.Front, want.Front) {
				return true
			}
		}
		return false
	})
}
