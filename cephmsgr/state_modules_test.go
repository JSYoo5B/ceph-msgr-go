package cephmsgr

import (
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func moduleStateMap(active maps.Mgr, epoch uint32, standbys []StandbyManager, enabled []string, available []ManagerModule) msgr.MessageData {
	base := mockMgrMapWithStandbys(active, epoch, standbys)
	e := wire.Encoder{}
	// Replace the helper's three empty collections inside its 6-byte envelope.
	e.Raw(base.Front[6 : len(base.Front)-12])
	e.U32(uint32(len(enabled)))
	for _, name := range enabled {
		e.String(name)
	}
	e.U32(0) // services
	e.U32(uint32(len(available)))
	for _, module := range available {
		info := wire.Encoder{}
		info.String(module.Name)
		if module.CanRun {
			info.U8(1)
		} else {
			info.U8(0)
		}
		info.String(module.ErrorString)
		info.U32(0) // module options, opaque to public Snapshot metadata
		e.Struct(2, 1, info.Data)
	}
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: out.Data}
}

func TestSnapshotManagerModulesOwnershipReplacementAndClose(t *testing.T) {
	active, err := maps.DecodeMgr(mockMgrMap(1, 99, 6800))
	if err != nil {
		t.Fatal(err)
	}
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	standbys := []StandbyManager{{Name: "b", GlobalID: 100}}
	enabled := []string{"balancer", "custom"}
	available := []ManagerModule{{Name: "custom", CanRun: false, ErrorString: "  missing dependency\n诊断  "}, {Name: "balancer", CanRun: true}}
	f.primary.send(t, f.ctx, moduleStateMap(active, 2, standbys, enabled, available))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 2 })
	before := f.c.Snapshot()
	if !reflect.DeepEqual(before.Manager.EnabledModules, enabled) || !reflect.DeepEqual(before.Manager.AvailableModules, available) || !reflect.DeepEqual(before.Manager.Standbys, standbys) || before.Manager.Ready || !before.Monitor.Ready {
		t.Fatal("same-epoch metadata lost raw load reports or claimed readiness", before)
	}
	owned := f.c.Snapshot()
	owned.Manager.EnabledModules[0] = "caller renamed"
	owned.Manager.EnabledModules = append(owned.Manager.EnabledModules[:0], "caller replaced")
	owned.Manager.AvailableModules[0] = ManagerModule{Name: "caller replaced", CanRun: true}
	if got := f.c.Snapshot(); !reflect.DeepEqual(got.Manager.EnabledModules, enabled) || !reflect.DeepEqual(got.Manager.AvailableModules, available) {
		t.Fatal("caller changed retained module collections", got)
	}
	// A metadata-only full replacement removes both lists while retaining the
	// same active daemon and standby. No previous values may be merged into it.
	f.primary.send(t, f.ctx, moduleStateMap(active, 3, standbys, nil, nil))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 3 })
	empty := f.c.Snapshot()
	if len(empty.Manager.EnabledModules) != 0 || len(empty.Manager.AvailableModules) != 0 || !reflect.DeepEqual(empty.Manager.Standbys, standbys) || empty.Manager.Name != before.Manager.Name || empty.Manager.GlobalID != before.Manager.GlobalID {
		t.Fatal("empty metadata replacement retained old modules or changed identity", empty)
	}
	if !reflect.DeepEqual(before.Manager.EnabledModules, enabled) || !reflect.DeepEqual(before.Manager.AvailableModules, available) {
		t.Fatal("later map replacement modified an older snapshot", before)
	}
	// An active-MGR replacement naturally carries that daemon's module report.
	active.Name, active.GlobalID = "b", 100
	standbys = []StandbyManager{{Name: "a", GlobalID: 99}}
	enabled = []string{"custom"}
	available = []ManagerModule{{Name: "custom", CanRun: true, ErrorString: "raw diagnostic"}}
	f.primary.send(t, f.ctx, moduleStateMap(active, 4, standbys, enabled, available))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 4 })
	after := f.c.Snapshot()
	if after.Manager.Name != "b" || after.Manager.GlobalID != 100 || !reflect.DeepEqual(after.Manager.Standbys, standbys) || !reflect.DeepEqual(after.Manager.EnabledModules, enabled) || !reflect.DeepEqual(after.Manager.AvailableModules, available) || after.Manager.Ready {
		t.Fatal("active map replacement lost its matching module metadata", after)
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("module discovery dialed or sent a command", f.monDials.Load(), f.namedDials.Load(), f.mgrDials.Load(), len(f.primary.requests), len(f.c.calls))
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Manager.Ready || closed.Manager.MapEpoch != 4 || !reflect.DeepEqual(closed.Manager.EnabledModules, enabled) || !reflect.DeepEqual(closed.Manager.AvailableModules, available) {
		t.Fatal("Close discarded the last authenticated module metadata", closed)
	}
	closed.Manager.EnabledModules[0] = "caller changed closed snapshot"
	closed.Manager.AvailableModules[0].ErrorString = "caller changed closed diagnostic"
	if got := f.c.Snapshot(); !reflect.DeepEqual(got.Manager.EnabledModules, enabled) || !reflect.DeepEqual(got.Manager.AvailableModules, available) {
		t.Fatal("closed snapshots share module storage", got)
	}
}
