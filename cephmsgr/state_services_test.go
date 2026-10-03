package cephmsgr

import (
	"reflect"
	"sort"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func serviceStateMap(active maps.Mgr, epoch uint32, services map[string]string) msgr.MessageData {
	base := mockMgrMapWithStandbys(active, epoch, nil)
	e := wire.Encoder{}
	e.Raw(base.Front[6 : len(base.Front)-12]) // Replace the three empty collections.
	e.U32(1)
	e.String("dashboard")
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	e.U32(uint32(len(names)))
	for _, name := range names {
		e.String(name)
		e.String(services[name])
	}
	e.U32(1)
	info := wire.Encoder{}
	info.String("dashboard")
	info.U8(1)
	info.String("raw diagnostic")
	info.U32(0) // module options
	e.Struct(2, 1, info.Data)
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: out.Data}
}

func TestSnapshotManagerServicesOwnershipReplacementAndClose(t *testing.T) {
	active, err := maps.DecodeMgr(mockMgrMap(1, 99, 6800))
	if err != nil {
		t.Fatal(err)
	}
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	want := map[string]string{"dashboard": " https://[::1]:8443/path?x=1 \n", "empty": "", "custom": "raw URI\x00诊断"}
	f.primary.send(t, f.ctx, serviceStateMap(active, 2, want))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 2 })
	before := f.c.Snapshot()
	modules := []ManagerModule{{Name: "dashboard", CanRun: true, ErrorString: "raw diagnostic"}}
	if !reflect.DeepEqual(before.Manager.Services, want) || !reflect.DeepEqual(before.Manager.EnabledModules, []string{"dashboard"}) || !reflect.DeepEqual(before.Manager.AvailableModules, modules) || before.Manager.Ready || !before.Monitor.Ready {
		t.Fatal("same-epoch services lost raw values or module metadata", before)
	}
	owned := f.c.Snapshot()
	owned.Manager.Services["dashboard"] = "caller mutation"
	delete(owned.Manager.Services, "empty")
	owned.Manager.Services["caller added"] = "independent"
	if got := f.c.Snapshot().Manager.Services; !reflect.DeepEqual(got, want) {
		t.Fatal("caller changed retained service advertisements", got)
	}
	replacement := map[string]string{"dashboard": "http://new-address:123/"}
	f.primary.send(t, f.ctx, serviceStateMap(active, 3, replacement))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 3 })
	if got := f.c.Snapshot(); !reflect.DeepEqual(got.Manager.Services, replacement) || !reflect.DeepEqual(got.Manager.AvailableModules, modules) || !reflect.DeepEqual(before.Manager.Services, want) {
		t.Fatal("full replacement merged old services or changed an older snapshot", got)
	}
	f.primary.send(t, f.ctx, serviceStateMap(active, 4, nil))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 4 })
	empty := f.c.Snapshot()
	if empty.Manager.Services == nil || len(empty.Manager.Services) != 0 || !reflect.DeepEqual(empty.Manager.AvailableModules, modules) {
		t.Fatal("empty services replacement retained old values or lost modules", empty)
	}
	empty.Manager.Services["caller added to empty map"] = "local"
	if len(f.c.Snapshot().Manager.Services) != 0 {
		t.Fatal("empty snapshot map shares client storage")
	}
	active.Name, active.GlobalID = "b", 100
	replacement = map[string]string{"dashboard": "https://promoted-manager:8443/"}
	f.primary.send(t, f.ctx, serviceStateMap(active, 5, replacement))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 5 })
	after := f.c.Snapshot()
	if after.Manager.Name != "b" || after.Manager.GlobalID != 100 || !reflect.DeepEqual(after.Manager.Services, replacement) || !reflect.DeepEqual(after.Manager.AvailableModules, modules) || after.Manager.Ready {
		t.Fatal("active transition lost its matching advertisements", after)
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("service discovery dialed or sent a command", f.monDials.Load(), f.namedDials.Load(), f.mgrDials.Load(), len(f.primary.requests), len(f.c.calls))
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Manager.Ready || closed.Manager.MapEpoch != 5 || !reflect.DeepEqual(closed.Manager.Services, replacement) || !reflect.DeepEqual(closed.Manager.AvailableModules, modules) {
		t.Fatal("Close discarded the last authenticated services or modules", closed)
	}
	closed.Manager.Services["dashboard"] = "caller changed closed snapshot"
	if got := f.c.Snapshot().Manager.Services; !reflect.DeepEqual(got, replacement) {
		t.Fatal("closed snapshots share service storage", got)
	}
}
