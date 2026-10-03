package cephmsgr

import (
	"os"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func mockMgrMapWithStandbys(active maps.Mgr, epoch uint32, standbys []StandbyManager) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(epoch)
	msgr.EncodeAddresses(&e, active.Addresses)
	e.U64(active.GlobalID)
	e.U8(1)
	e.String(active.Name)
	e.U32(uint32(len(standbys)))
	for _, standby := range standbys {
		e.U64(standby.GlobalID)
		info := wire.Encoder{}
		info.U64(standby.GlobalID)
		info.String(standby.Name)
		info.U32(0) // available modules
		info.U64(0) // features
		e.Struct(4, 1, info.Data)
	}
	e.U32(0) // enabled modules
	e.U32(0) // services
	e.U32(0) // available modules
	mockMgrMapTail(&e)
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: out.Data}
}

func TestSnapshotManagerStandbysFollowAuthenticatedMap(t *testing.T) {
	blob, err := os.ReadFile("../internal/maps/testdata/mgrmap-ipv6-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	active, err := maps.DecodeMgr(blob)
	if err != nil {
		t.Fatal(err)
	}
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	// This daemon-generated fixture follows the same authenticated MON reader
	// as live maps. The independently decoded standby is a with global ID 4114.
	f.primary.send(t, f.ctx, msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: blob})
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 4 })
	want := []StandbyManager{{Name: "a", GlobalID: 4114}}
	before := f.c.Snapshot()
	if !reflect.DeepEqual(before.Manager.Standbys, want) || !before.Monitor.Ready || before.Manager.Ready || before.Manager.Name != "b" || before.Manager.GlobalID != 4111 || !before.Manager.Available {
		t.Fatal("standby discovery lost native map values or claimed readiness", before)
	}
	owned := f.c.Snapshot()
	owned.Manager.Standbys[0] = StandbyManager{Name: "caller changed", GlobalID: 1}
	owned.Manager.Standbys = append(owned.Manager.Standbys[:0], StandbyManager{Name: "caller replaced", GlobalID: 2})
	if got := f.c.Snapshot().Manager.Standbys; !reflect.DeepEqual(got, want) {
		t.Fatal("caller modified retained standby discovery", got)
	}
	// Keep every active-MGR field intact while replacing only standby members.
	want = []StandbyManager{{Name: "a-renamed", GlobalID: 4114}, {Name: "c", GlobalID: 4120}}
	f.primary.send(t, f.ctx, mockMgrMapWithStandbys(active, 5, want))
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 5 })
	after := f.c.Snapshot()
	if !reflect.DeepEqual(after.Manager.Standbys, want) || after.Manager.Ready || after.Manager.Name != before.Manager.Name || after.Manager.GlobalID != before.Manager.GlobalID || !reflect.DeepEqual(after.Manager.Endpoints, before.Manager.Endpoints) || !reflect.DeepEqual(before.Manager.Standbys, []StandbyManager{{Name: "a", GlobalID: 4114}}) {
		t.Fatal("standby-only replacement changed active state or an older snapshot", before, after)
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("standby discovery dialed or sent a command", f.monDials.Load(), f.namedDials.Load(), f.mgrDials.Load(), len(f.primary.requests), len(f.c.calls))
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || closed.Manager.MapEpoch != 5 || !reflect.DeepEqual(closed.Manager.Standbys, want) {
		t.Fatal("Close discarded the last authenticated standby members", closed)
	}
	closed.Manager.Standbys[0].Name = "caller changed after Close"
	if got := f.c.Snapshot().Manager.Standbys; !reflect.DeepEqual(got, want) {
		t.Fatal("closed snapshots share standby storage", got)
	}
}
