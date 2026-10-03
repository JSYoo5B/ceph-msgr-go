package cephmsgr

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type mapWaitTestContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *mapWaitTestContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type mapWaitTestResult struct {
	mon MonitorState
	mgr ManagerState
	err error
}

func mapWaitTestFixture(t *testing.T) *namedTestFixture {
	t.Helper()
	f := namedTestFixtureFor(t, namedTestAddressB(), func(o *Options, _ []namedTestMember) { o.MaxInFlight = 1 }, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 1 })
	return f
}

func mapWaitTestStart(t *testing.T, f *namedTestFixture, ctx context.Context, mgr bool, after uint32) <-chan mapWaitTestResult {
	t.Helper()
	observed := &mapWaitTestContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan mapWaitTestResult, 1)
	go func() {
		var got mapWaitTestResult
		if mgr {
			got.mgr, got.err = f.c.WaitMgrMap(observed, after)
		} else {
			got.mon, got.err = f.c.WaitMonMap(observed, after)
		}
		result <- got
	}()
	select {
	case <-observed.waiting:
	case got := <-result:
		t.Fatal("map wait returned before a newer epoch", got)
	case <-f.ctx.Done():
		t.Fatal("map wait did not begin", f.ctx.Err())
	}
	return result
}

func mapWaitTestReceive(t *testing.T, f *namedTestFixture, results <-chan mapWaitTestResult) mapWaitTestResult {
	t.Helper()
	select {
	case got := <-results:
		return got
	case <-f.ctx.Done():
		t.Fatal("map wait did not settle", f.ctx.Err())
		return mapWaitTestResult{}
	}
}

func mapWaitTestLocalOnly(t *testing.T, f *namedTestFixture, occupied int) {
	t.Helper()
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != occupied {
		t.Fatal("map wait dialed, sent a request or changed command slots", f.monDials.Load(), f.namedDials.Load(), f.mgrDials.Load(), len(f.primary.requests), len(f.c.calls))
	}
}

func mapWaitTestManagerMessage(t *testing.T, active maps.Mgr, epoch uint32) msgr.MessageData {
	t.Helper()
	options := map[string]ManagerModuleOption{"map-key": {
		Name: "reported-name", Type: 255, Level: 254, Flags: 0x80000021,
		DefaultValue: " 9283 ", EnumAllowed: []string{"a", "b"},
		Tags: []string{"metrics"}, SeeAlso: []string{"other-option"},
	}}
	schema := moduleOptionStateMap(active, epoch, options)
	plain := mockMgrMapWithStandbys(active, epoch, nil)
	// The schema helper writes its prefix, one enabled "custom" string, and
	// an empty services count before the available_modules vector.
	availableOffset := len(plain.Front) - 12 - mockMgrMapTailBytes + 4 + 4 + len("custom") + 4
	standby := mockMgrMapWithStandbys(active, epoch, []StandbyManager{{Name: "standby", GlobalID: 101}})
	body := wire.Encoder{}
	body.Raw(standby.Front[6 : len(standby.Front)-12-mockMgrMapTailBytes])
	body.U32(1)
	body.String("custom")
	body.U32(1)
	body.String("custom")
	body.String("https://[::1]:9283/path?x=1")
	body.Raw(schema.Front[availableOffset:])
	front := wire.Encoder{}
	front.Struct(14, 6, body.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: front.Data}
}

func TestWaitMapInitialAndConcurrentMetadataUpdates(t *testing.T) {
	f := mapWaitTestFixture(t)
	before := f.c.Snapshot()
	mon, err := f.c.WaitMonMap(f.ctx, 0)
	if err != nil || !reflect.DeepEqual(mon, before.Monitor) {
		t.Fatal("initial MON map was not returned", mon, err)
	}
	mgr, err := f.c.WaitMgrMap(f.ctx, 0)
	if err != nil || !reflect.DeepEqual(mgr, before.Manager) || mgr.Ready {
		t.Fatal("initial MGR map required a ready daemon", mgr, err)
	}
	// No command slot is available. Map observation must still progress.
	f.c.calls <- struct{}{}
	defer func() { <-f.c.calls }()
	var managerResults []<-chan mapWaitTestResult
	var monitorResults []<-chan mapWaitTestResult
	for range 3 {
		managerResults = append(managerResults, mapWaitTestStart(t, f, f.ctx, true, 1))
		monitorResults = append(monitorResults, mapWaitTestStart(t, f, f.ctx, false, 1))
	}
	active, err := maps.DecodeMgr(mockMgrMap(1, 99, 6800))
	if err != nil {
		t.Fatal(err)
	}
	f.primary.send(t, f.ctx, mapWaitTestManagerMessage(t, active, 2))
	var managers []ManagerState
	for _, result := range managerResults {
		got := mapWaitTestReceive(t, f, result)
		if got.err != nil || got.mgr.MapEpoch != 2 || got.mgr.Name != mgr.Name || got.mgr.GlobalID != mgr.GlobalID || !reflect.DeepEqual(got.mgr.Endpoints, mgr.Endpoints) || got.mgr.Ready || !reflect.DeepEqual(got.mgr.Standbys, []StandbyManager{{Name: "standby", GlobalID: 101}}) || got.mgr.Services["custom"] != "https://[::1]:9283/path?x=1" || len(got.mgr.AvailableModules) != 1 {
			t.Fatal("metadata-only update lost map values or required MGR connection", got)
		}
		managers = append(managers, got.mgr)
	}
	members := []namedTestMember{{"replacement", mapWaitTestPrimaryAddresses(f)}, {"b", []msgr.Address{namedTestAddressB()}}}
	f.primary.send(t, f.ctx, memberStateMap(2, members))
	var monitors []MonitorState
	for _, result := range monitorResults {
		got := mapWaitTestReceive(t, f, result)
		if got.err != nil || got.mon.MapEpoch != 2 || !reflect.DeepEqual(got.mon.Members, []MonitorMember{{Name: "replacement", Rank: 0}, {Name: "b", Rank: 1}}) {
			t.Fatal("MON membership update did not release all readers", got)
		}
		monitors = append(monitors, got.mon)
	}
	want := f.c.Snapshot()
	managers[0].Standbys[0].Name = "caller"
	managers[0].Endpoints[0] = "caller"
	managers[0].EnabledModules[0] = "caller"
	managers[0].Services["custom"] = "caller"
	option := managers[0].AvailableModules[0].Options["map-key"]
	option.EnumAllowed[0], option.Tags[0], option.SeeAlso[0] = "caller", "caller", "caller"
	managers[0].AvailableModules[0].Options["map-key"] = option
	monitors[0].Members[0].Name = "caller"
	monitors[0].Endpoints[0] = "caller"
	if !reflect.DeepEqual(managers[1], want.Manager) || !reflect.DeepEqual(managers[2], want.Manager) || !reflect.DeepEqual(monitors[1], want.Monitor) || !reflect.DeepEqual(monitors[2], want.Monitor) || !reflect.DeepEqual(f.c.Snapshot(), want) {
		t.Fatal("concurrent map results shared mutable collection storage")
	}
	mapWaitTestLocalOnly(t, f, 1)
}

func mapWaitTestPrimaryAddresses(f *namedTestFixture) []msgr.Address {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	return append([]msgr.Address(nil), f.c.monMap.Members[0].Addresses...)
}
