package cephmsgr

import (
	"encoding/binary"
	"net/netip"
	"reflect"
	"sync"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func memberStateMap(epoch uint32, members []namedTestMember) msgr.MessageData {
	front := namedTestMap([16]byte{1}, 20, members)
	// The independent wire fixture puts the epoch after bufferlist U32,
	// envelope U8/U8/U32 and FSID16. No product map encoder is used.
	binary.LittleEndian.PutUint32(front[4+6+16:], epoch)
	return msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: front}
}

func TestSnapshotMonitorMembersDiscoveryOwnershipAndClose(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	want := []MonitorMember{{Name: "a", Rank: 0}, {Name: "b", Rank: 1}, {Name: "c", Rank: 2}}
	state := f.c.Snapshot()
	if !reflect.DeepEqual(state.Monitor.Members, want) || !state.Monitor.Ready || state.Manager.Ready {
		t.Fatal("authenticated names and ranks were not discovered locally", state)
	}
	// Both element mutation and caller replacement of the outer slice must be
	// independent of the retained map and other snapshots.
	state.Monitor.Members[0] = MonitorMember{Name: "caller changed name", Rank: 91}
	state.Monitor.Members = append(state.Monitor.Members[:1], MonitorMember{Name: "caller appended", Rank: 92})
	for range 10 {
		if got := f.c.Snapshot(); !reflect.DeepEqual(got.Monitor.Members, want) || got.Manager.Ready {
			t.Fatal("caller-owned member slice altered the client", got)
		}
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("member discovery performed network work", f.monDials.Load(), f.namedDials.Load(), f.mgrDials.Load(), len(f.primary.requests), len(f.c.calls))
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || !reflect.DeepEqual(closed.Monitor.Members, want) {
		t.Fatal("Close discarded the last known members or claimed readiness", closed)
	}
	closed.Monitor.Members[1].Name = "caller changed closed snapshot"
	if got := f.c.Snapshot().Monitor.Members; !reflect.DeepEqual(got, want) {
		t.Fatal("closed snapshots share mutable member storage", got)
	}
}

func TestSnapshotMonitorMembersFollowPrimaryMap(t *testing.T) {
	address := namedTestAddressB()
	f := namedTestFixtureFor(t, address, nil, nil)
	before := f.c.Snapshot()
	// Keep the same authenticated peer while renaming a, removing b and
	// moving c from rank 2 to rank 0. Map membership does not imply a dial.
	members := []namedTestMember{
		{"c", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.3:3302")}}},
		{"replacement-a", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}},
	}
	f.primary.send(t, f.ctx, memberStateMap(2, members))
	// A reply from the same ordered reader proves that it processed the map.
	namedTestPrimaryHealthy(t, f)
	want := []MonitorMember{{Name: "c", Rank: 0}, {Name: "replacement-a", Rank: 1}}
	after := f.c.Snapshot()
	if after.Monitor.MapEpoch != 2 || !reflect.DeepEqual(after.Monitor.Members, want) || after.GlobalID != before.GlobalID || after.FSID != before.FSID || f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 {
		t.Fatal("member names and ranks did not follow the admitted primary map", before, after)
	}
	if !reflect.DeepEqual(before.Monitor.Members, []MonitorMember{{Name: "a", Rank: 0}, {Name: "b", Rank: 1}, {Name: "c", Rank: 2}}) {
		t.Fatal("map replacement modified an older snapshot", before)
	}
}

func TestSnapshotMonitorMembersIgnoreRejectedAndStaleMaps(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	before := f.c.Snapshot()
	f.c.mu.Lock()
	current := f.c.mon
	f.c.mu.Unlock()
	members := []namedTestMember{{"unpublished", []msgr.Address{namedTestAddressB()}}}
	for _, invalid := range []struct {
		name    string
		message msgr.MessageData
	}{
		{"unsupported release", msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: namedTestMap([16]byte{1}, 19, members)}},
		{"different FSID", msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: namedTestMap([16]byte{2}, 20, members)}},
		{"unsupported header", msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, CompatVersion: 2, Front: namedTestMap([16]byte{1}, 20, members)}},
		{"truncated map", msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: []byte{1}}},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			accepted, err := f.c.handleMap(current, invalid.message)
			if accepted || err == nil || !reflect.DeepEqual(f.c.Snapshot().Monitor, before.Monitor) {
				t.Fatal("rejected map published member discovery", accepted, err, f.c.Snapshot().Monitor)
			}
		})
	}
	if accepted, err := f.c.handleMap(new(session.Session), memberStateMap(9, members)); accepted || err != nil {
		t.Fatal("stale session published a newer map", accepted, err)
	}
	if accepted, err := f.c.handleMap(current, memberStateMap(0, members)); !accepted || err != nil {
		t.Fatal("valid older map was not handled", accepted, err)
	}
	if got := f.c.Snapshot().Monitor; !reflect.DeepEqual(got, before.Monitor) {
		t.Fatal("stale source or old epoch changed last known members", before.Monitor, got)
	}
}

func TestSnapshotMonitorMembersIgnorePrivateTellMap(t *testing.T) {
	address := namedTestAddressB()
	f := namedTestFixtureFor(t, address, nil, func(p *namedTestPeer) {
		// An independently admitted map keeps the requested b binding while
		// giving it rank 0 and replacing all other members.
		p.mapMessage = memberStateMap(7, []namedTestMember{
			{"b", []msgr.Address{address}},
			{"private-only", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}},
		})
	})
	before := f.c.Snapshot().Monitor
	result, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"version"}`)})
	if err != nil || result.Code != 0 {
		t.Fatal("independent target admission failed", result, err)
	}
	p := namedTestTarget(t, f)
	p.next(t, f.ctx, msgr.MonGetMapMessage)
	p.next(t, f.ctx, msgr.TellCommandMessage)
	if got := f.c.Snapshot().Monitor; !reflect.DeepEqual(got, before) || p.tellCount.Load() != 1 || f.monDials.Load() != 1 || f.namedDials.Load() != 1 || f.mgrDials.Load() != 0 {
		t.Fatal("private target map became public discovery state", before, got)
	}
}

func TestSnapshotMonitorMembersConcurrentUpdateAndClose(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	want := map[uint32][]MonitorMember{
		1: {{Name: "a", Rank: 0}, {Name: "b", Rank: 1}, {Name: "c", Rank: 2}},
		2: {{Name: "renamed", Rank: 0}},
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				state := f.c.Snapshot()
				if !reflect.DeepEqual(state.Monitor.Members, want[state.Monitor.MapEpoch]) {
					t.Error("snapshot mixed epochs or exposed caller mutation", state.Monitor)
					return
				}
				state.Monitor.Members[0] = MonitorMember{Name: "owned by reader", Rank: 77}
				if state.Closed {
					return
				}
			}
		}()
	}
	t.Cleanup(func() { f.c.Close(); readers.Wait() })
	f.primary.send(t, f.ctx, memberStateMap(2, []namedTestMember{{"renamed", []msgr.Address{namedTestAddressB()}}}))
	ctx := &namedTestSnapshotContext{Context: f.ctx, client: f.c}
	if _, err := f.c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal(err)
	}
	if ctx.calls.Load() == 0 || ctx.drift.Load() {
		t.Fatal("reentrant context callback did not safely observe discovery", ctx.calls.Load())
	}
	f.c.Close()
	finished := make(chan struct{})
	go func() { readers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-f.ctx.Done():
		t.Fatal("snapshot readers outlived Close", f.ctx.Err())
	}
	if got := f.c.Snapshot(); !got.Closed || got.Monitor.MapEpoch != 2 || !reflect.DeepEqual(got.Monitor.Members, want[2]) {
		t.Fatal("concurrent Close discarded the final admitted members", got)
	}
}
