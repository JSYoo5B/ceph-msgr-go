package cephmsgr

import (
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Build a complete current MgrMap independently of the metadata helpers, whose
// prefixes do not describe the fields between module reports and policy.
func modulePolicyStateMap(active maps.Mgr, epoch uint32, always map[uint32][]string, disabled []string) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(epoch)
	msgr.EncodeAddresses(&e, active.Addresses)
	e.U64(active.GlobalID)
	e.U8(1)
	e.String(active.Name)
	e.U32(0) // standbys
	e.U32(2) // explicit enabled modules, kept separate from both policies
	e.String("balancer")
	e.String("custom")
	e.U32(0) // services
	e.U32(0) // available modules
	e.U32(123)
	e.U32(456) // active_change timestamp
	releases := make([]uint32, 0, len(always))
	for release := range always {
		releases = append(releases, release)
	}
	sort.Slice(releases, func(i, j int) bool { return releases[i] < releases[j] })
	strings := func(values []string) {
		e.U32(uint32(len(values)))
		for _, value := range values {
			e.String(value)
		}
	}
	e.U32(uint32(len(releases)))
	for _, release := range releases {
		e.U32(release)
		strings(always[release])
	}
	e.U64(0) // active_mgr_features
	e.U32(0) // last_failure_osd_epoch
	e.U32(0) // clients_addrs
	e.U32(0) // clients_names
	e.U64(0) // flags
	strings(disabled)
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: out.Data}
}

func TestSnapshotManagerModulePolicyOwnershipReplacementAndClose(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	initial := waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 1 })
	if initial.Manager.AlwaysOnModules != nil || initial.Manager.ForceDisabledModules != nil {
		t.Fatal("absent policy is not nil", initial.Manager)
	}
	f.c.mu.Lock()
	active := f.c.mgrMap
	f.c.mu.Unlock()
	want := map[uint32][]string{20: {"balancer", "dashboard"}, 99: {"future-a", "future-b"}, 100: nil}
	disabled := []string{"balancer", "raw module"}
	replacement := map[uint32][]string{21: {"newer"}, 99: nil}
	replacementDisabled := []string{"custom"}
	matches := func(m ManagerState, always map[uint32][]string, force []string) bool {
		return reflect.DeepEqual(m.AlwaysOnModules, always) && reflect.DeepEqual(m.ForceDisabledModules, force)
	}
	check := func(s State, always map[uint32][]string, force []string) {
		t.Helper()
		if !matches(s.Manager, always, force) || !reflect.DeepEqual(s.Manager.EnabledModules, []string{"balancer", "custom"}) || s.Manager.Ready || !s.Monitor.Ready || s.Manager.Name != active.Name || s.Manager.GlobalID != active.GlobalID || !reflect.DeepEqual(s.Manager.Endpoints, initial.Manager.Endpoints) {
			t.Fatal("policy snapshot changed raw collections, identity or readiness", s)
		}
	}
	publish := func(epoch uint32, always map[uint32][]string, force []string) State {
		t.Helper()
		f.primary.send(t, f.ctx, modulePolicyStateMap(active, epoch, always, force))
		s := waitClientState(t, f.c, f.ctx, func(s State) bool {
			return s.Manager.MapEpoch == epoch && matches(s.Manager, always, force)
		})
		check(s, always, force)
		return s
	}
	before := publish(2, want, disabled)
	mutate := func(m ManagerState) {
		for release, names := range m.AlwaysOnModules {
			if len(names) != 0 {
				names[0] = "caller changed nested slice"
			}
			delete(m.AlwaysOnModules, release)
		}
		m.AlwaysOnModules[777] = []string{"caller added release"}
		m.ForceDisabledModules[0] = "caller changed disabled"
	}
	mutate(f.c.Snapshot().Manager)
	check(f.c.Snapshot(), want, disabled)
	check(before, want, disabled)

	// Both policy collections belong to the same received map. Readers mutate
	// their copies while the authenticated MON replaces metadata at one epoch.
	var readers sync.WaitGroup
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				s := f.c.Snapshot()
				if !matches(s.Manager, want, disabled) && !matches(s.Manager, replacement, replacementDisabled) {
					t.Error("concurrent snapshot mixed policy fields or caller mutation", s.Manager)
					return
				}
				mutate(s.Manager)
				if s.Closed {
					return
				}
			}
		}()
	}
	t.Cleanup(func() { f.c.Close(); readers.Wait() })
	for range 3 {
		publish(2, replacement, replacementDisabled)
		publish(2, want, disabled)
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || closed.Manager.MapEpoch != 2 || !matches(closed.Manager, want, disabled) {
		t.Fatal("Close discarded the last authenticated policy", closed)
	}
	mutate(closed.Manager)
	if got := f.c.Snapshot(); !matches(got.Manager, want, disabled) || !matches(before.Manager, want, disabled) {
		t.Fatal("closed or concurrent snapshots share policy storage", got.Manager, before.Manager)
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("policy discovery dialed, sent a command or used a command slot")
	}
}

func TestSnapshotManagerModulePolicyEmptyReplacement(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 1 })
	f.c.mu.Lock()
	active := f.c.mgrMap
	f.c.mu.Unlock()
	f.primary.send(t, f.ctx, modulePolicyStateMap(active, 2, map[uint32][]string{20: {"balancer"}}, []string{"custom"}))
	before := waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 2 })
	f.primary.send(t, f.ctx, modulePolicyStateMap(active, 3, nil, nil))
	empty := waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == 3 })
	if empty.Manager.AlwaysOnModules != nil || empty.Manager.ForceDisabledModules != nil || !reflect.DeepEqual(empty.Manager.EnabledModules, []string{"balancer", "custom"}) || !reflect.DeepEqual(before.Manager.AlwaysOnModules, map[uint32][]string{20: {"balancer"}}) || !reflect.DeepEqual(before.Manager.ForceDisabledModules, []string{"custom"}) || empty.Manager.Ready || !empty.Monitor.Ready {
		t.Fatal("empty full replacement retained policy or changed explicit modules", empty, before)
	}
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("empty policy discovery dialed, sent a command or used a command slot")
	}
}
