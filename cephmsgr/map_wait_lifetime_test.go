package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestWaitMapSameAndStaleEpochsDoNotSatisfyWait(t *testing.T) {
	f := mapWaitTestFixture(t)
	ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
	defer cancel()
	mon := mapWaitTestStart(t, f, ctx, false, 1)
	mgr := mapWaitTestStart(t, f, ctx, true, 1)
	members := []namedTestMember{{"same", mapWaitTestPrimaryAddresses(f)}}
	active, err := maps.DecodeMgr(mockMgrMap(1, 99, 6800))
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []uint32{0, 1} {
		f.primary.send(t, f.ctx, memberStateMap(epoch, members))
		f.primary.send(t, f.ctx, mapWaitTestManagerMessage(t, active, epoch))
	}
	f.c.mu.Lock()
	f.c.signal() // An unrelated local wake also cannot manufacture a newer map.
	f.c.mu.Unlock()
	for _, result := range []<-chan mapWaitTestResult{mon, mgr} {
		got := mapWaitTestReceive(t, f, result)
		if !errors.Is(got.err, context.DeadlineExceeded) || !reflect.DeepEqual(got.mon, MonitorState{}) || !reflect.DeepEqual(got.mgr, ManagerState{}) {
			t.Fatal("same/stale map or unrelated wake satisfied a strict epoch wait", got)
		}
	}
	mapWaitTestLocalOnly(t, f, 0)
}

func TestWaitMapCancellationLeavesOtherWaitsAndClientAlive(t *testing.T) {
	f := mapWaitTestFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	canceled := mapWaitTestStart(t, f, ctx, false, 1)
	mon := mapWaitTestStart(t, f, f.ctx, false, 1)
	mgr := mapWaitTestStart(t, f, f.ctx, true, 1)
	cancel()
	if got := mapWaitTestReceive(t, f, canceled); !errors.Is(got.err, context.Canceled) {
		t.Fatal("operation cancellation did not release its wait", got)
	}
	if _, err := f.c.WaitMgrMap(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled operation consumed an existing map", err)
	}
	f.primary.send(t, f.ctx, memberStateMap(2, []namedTestMember{{"a", mapWaitTestPrimaryAddresses(f)}}))
	f.primary.send(t, f.ctx, msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: mockMgrMap(2, 99, 6800)})
	if got := mapWaitTestReceive(t, f, mon); got.err != nil || got.mon.MapEpoch != 2 {
		t.Fatal("one canceled wait terminated another MON wait", got)
	}
	if got := mapWaitTestReceive(t, f, mgr); got.err != nil || got.mgr.MapEpoch != 2 {
		t.Fatal("one canceled wait terminated the MGR wait", got)
	}
	if s := f.c.Snapshot(); !s.Monitor.Ready || s.Closed || s.Manager.Ready {
		t.Fatal("wait cancellation changed client lifetime or dialed MGR", s)
	}
	mapWaitTestLocalOnly(t, f, 0)
}

func TestWaitMapCloseReleasesBothRoles(t *testing.T) {
	f := mapWaitTestFixture(t)
	mon := mapWaitTestStart(t, f, f.ctx, false, ^uint32(0))
	mgr := mapWaitTestStart(t, f, f.ctx, true, ^uint32(0))
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, result := range []<-chan mapWaitTestResult{mon, mgr} {
		got := mapWaitTestReceive(t, f, result)
		if !errors.Is(got.err, ErrClosed) || !reflect.DeepEqual(got.mon, MonitorState{}) || !reflect.DeepEqual(got.mgr, ManagerState{}) {
			t.Fatal("Close did not release an unsatisfied map wait", got)
		}
	}
	if _, err := f.c.WaitMonMap(f.ctx, 0); !errors.Is(err, ErrClosed) {
		t.Fatal("closed client returned retained MON map as success", err)
	}
	mapWaitTestLocalOnly(t, f, 0)
}
