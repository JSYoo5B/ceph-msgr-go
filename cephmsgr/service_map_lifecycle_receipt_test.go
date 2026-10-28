package cephmsgr

import (
	"testing"
)

// Native and Go maps may be sampled at different epochs. An entry's start
// epoch must match only at the same map epoch; identity must match at either.
func TestServiceLifecyclePresenceRequiresNativeIdentity(t *testing.T) {
	gid, nativeEpoch, startEpoch := uint64(42), uint32(9), uint32(4)
	code := int32(0)
	role, daemon := "holder", "holder-0123456789abcdef"
	r := nativeServiceHolderReceipt{Phase: "observe", Service: "ceph-msgr", Daemon: daemon, GIDMatchesHolder: true,
		View: &nativeServiceHolderView{Code: &code, Epoch: &nativeEpoch, Present: true,
			Entry: &nativeServiceHolderEntry{GID: &gid, StartEpoch: &startEpoch, Role: &role, Daemon: &daemon}}}
	ready := nativeServiceHolderReady{GID: gid}
	makeMap := func() ServiceMap {
		return ServiceMap{Epoch: nativeEpoch, Raw: []byte{1}, Services: map[string]ServiceEntry{"ceph-msgr": {
			Daemons: map[string]ServiceDaemon{daemon: {GID: gid, StartEpoch: startEpoch, Metadata: map[string]string{"ceph_msgr_role": role, "ceph_msgr_daemon": daemon}}},
		}}}
	}
	if !serviceLifecyclePresent(makeMap(), r, ready) {
		t.Fatal("matching native/Go identity was refused")
	}
	m := makeMap()
	d := m.Services["ceph-msgr"].Daemons[daemon]
	d.StartEpoch++
	m.Services["ceph-msgr"].Daemons[daemon] = d
	if serviceLifecyclePresent(m, r, ready) {
		t.Fatal("same-epoch start-epoch mismatch accepted")
	}
	m.Epoch++
	if !serviceLifecyclePresent(m, r, ready) {
		t.Fatal("different epochs incorrectly require an unchanged start epoch")
	}
	d.GID++
	m.Services["ceph-msgr"].Daemons[daemon] = d
	if serviceLifecyclePresent(m, r, ready) {
		t.Fatal("another daemon GID accepted at a different epoch")
	}
	r.View.Code = nil
	if serviceLifecyclePresent(makeMap(), r, ready) {
		t.Fatal("unreadable native observation accepted")
	}
}

func TestServiceLifecycleAbsenceRequiresNewerOwnedMap(t *testing.T) {
	m := ServiceMap{Epoch: 9, Raw: []byte{1}}
	if serviceLifecycleAbsent(m, 9, "a", "b") {
		t.Fatal("stale pre-release absence accepted")
	}
	m.Epoch++
	if !serviceLifecycleAbsent(m, 9, "a", "b") {
		t.Fatal("new full map with missing owned service refused")
	}
	m.Services = map[string]ServiceEntry{"ceph-msgr": {Daemons: map[string]ServiceDaemon{"b": {GID: 7}}}}
	if serviceLifecycleAbsent(m, 9, "a", "b") {
		t.Fatal("partial removal accepted")
	}
	delete(m.Services["ceph-msgr"].Daemons, "b")
	m.Raw = nil
	if serviceLifecycleAbsent(m, 9, "a", "b") {
		t.Fatal("map without owned raw bytes accepted")
	}
}
