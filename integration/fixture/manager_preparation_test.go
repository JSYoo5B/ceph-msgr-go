package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestManagerAuthRejectionRequiresLatestNativeProcess(t *testing.T) {
	const start = "0 ceph version 20.2.4 tentacle, process ceph-mgr\n"
	const rejected = "-1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied\n"
	for _, tc := range []struct {
		name, log string
		want      bool
	}{
		{"latest refuses", start + rejected, true},
		{"old refusal then healthy respawn", start + rejected + start, false},
		{"unattributed truncated log", rejected, false},
		{"other daemon error", start + "cephx: verify_authorizer could not get service secret for service mgr secret_id=14\n", false},
		{"different auth code", start + "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (5) Input/output error\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if line, _ := managerAuthRejection(tc.log); (line != "") != tc.want {
				t.Fatalf("rejection=%q want=%t", line, tc.want)
			}
		})
	}
}

func TestManagerAuthRejectionReturnsLatestLineWithinLatestProcess(t *testing.T) {
	const start = "0 ceph version 20.2.4 tentacle, process ceph-mgr\n"
	const rejected = " monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"
	log := start + "old" + rejected + "\n" + start + "first" + rejected + "\nlast" + rejected + "\n"
	if got, process := managerAuthRejection(log); got != "last"+rejected || process+"\n" != start {
		t.Fatalf("matched refusal %q from process %q must be latest line from latest process", got, process)
	}
}

func TestManagerAuthRejectionReturnsTimestampedProcessStart(t *testing.T) {
	const log = "2026-10-14T23:51:56.039+0000 7ff1dc32f100  0 ceph version 20.2.4 (old) tentacle, process ceph-mgr, pid 1\n" +
		"2026-10-14T23:52:04.137+0000 7f663f410100  0 ceph version 20.2.4 (7f79373) tentacle (stable), process ceph-mgr, pid 1\n" +
		"2026-10-14T23:52:20.502+0000 7f663d3b6640 -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied\n"
	line, start := managerAuthRejection(log)
	if line == "" {
		t.Fatal("latest refusal not observed")
	}
	started, message, err := cephLogEntryTime(start)
	want := time.Date(2026, 10, 14, 23, 52, 4, 137e6, time.UTC)
	if err != nil || !started.Equal(want) || !strings.HasPrefix(message, "ceph version 20.2.4 (7f79373)") {
		t.Fatal("latest process start not returned with its native timestamp", start, started, err)
	}
	if _, _, err := cephLogEntryTime("0 ceph version 20.2.4 tentacle, process ceph-mgr"); err == nil {
		t.Fatal("untimestamped process start accepted for MON attribution")
	}
}

func TestManagerFailoverReadyRequiresExactOwnedActiveAndStandby(t *testing.T) {
	names := map[string]bool{"a": true, "b": true}
	status := ceph.ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 100}
	if managerFailoverReady(status, names) {
		t.Fatal("available active alone cannot support failover")
	}
	status.Standbys = append(status.Standbys, struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	}{Name: "b", GID: 200})
	if !managerFailoverReady(status, names) {
		t.Fatal("healthy owned active and standby rejected")
	}
	for _, invalid := range []string{"a", "unknown", ""} {
		status.Standbys[0].Name = invalid
		if managerFailoverReady(status, names) {
			t.Fatal("invalid standby accepted", invalid)
		}
	}
	status.Standbys[0].Name, status.Standbys[0].GID = "b", 0
	if managerFailoverReady(status, names) {
		t.Fatal("unregistered standby accepted")
	}
	status.Standbys[0].GID = status.ActiveGID
	if managerFailoverReady(status, names) {
		t.Fatal("same GID in two daemon slots accepted")
	}
}

type managerPreparationFrame struct {
	state   managerPreparationMap
	lines   map[string]string
	refused map[string][]uint64 // MON-attributed reclaim refusals per daemon.
}

type fakeManagerPreparation struct {
	frames   []managerPreparationFrame
	index    int
	events   []string
	receipts []managerPreparationReceipt
	startErr error
}

func managerMap(epoch uint32, active, standby uint64) managerPreparationMap {
	state := managerPreparationMap{Epoch: epoch}
	if active != 0 {
		state.Available, state.ActiveName, state.ActiveGID = true, "a", active
	}
	if standby != 0 {
		state.Standbys = append(state.Standbys, struct {
			Name string `json:"name"`
			GID  uint64 `json:"gid"`
		}{Name: "b", GID: standby})
	}
	return state
}

func (f *fakeManagerPreparation) backend() managerPreparationBackend {
	return managerPreparationBackend{
		rejection: func(ctx context.Context, name string) (managerRefusal, error) {
			f.events = append(f.events, "log:"+name)
			frame := f.frames[f.index]
			return managerRefusal{Line: frame.lines[name], Refused: frame.refused[name]}, ctx.Err()
		},
		observe: func(ctx context.Context) (managerPreparationMap, error) {
			f.events = append(f.events, "map")
			return f.frames[f.index].state, ctx.Err()
		},
		stop: func(ctx context.Context, name string) error {
			f.events = append(f.events, "stop:"+name)
			return ctx.Err()
		},
		start: func(ctx context.Context, name string) error {
			f.events = append(f.events, "start:"+name)
			return errors.Join(f.startErr, ctx.Err())
		},
		record: func(receipt managerPreparationReceipt) error {
			f.events = append(f.events, "record:"+receipt.Daemon)
			f.receipts = append(f.receipts, receipt)
			return nil
		},
		wait: func(ctx context.Context) error {
			f.events = append(f.events, "wait")
			f.index++
			if f.index == len(f.frames) {
				return errors.New("script exhausted without readiness")
			}
			return ctx.Err()
		},
	}
}

func (f *fakeManagerPreparation) controls() []string {
	var events []string
	for _, event := range f.events {
		if strings.HasPrefix(event, "stop:") || strings.HasPrefix(event, "start:") {
			events = append(events, event)
		}
	}
	return events
}

func TestManagerPreparationRejectsStaleSlotsThenRestartsEachOnceAndRequiresNewGIDs(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 200), map[string]string{"a": "first-a-refusal", "b": "first-b-refusal"}, map[string][]uint64{"a": {100, 101}, "b": {200}}},
		{managerMap(45, 0, 0), map[string]string{"a": "latest-a-refusal", "b": "latest-b-refusal"}, map[string][]uint64{"a": {100, 101}, "b": {200}}},
		{managerMap(46, 100, 200), nil, nil}, // Clean process logs cannot bless the old registered GIDs.
		{managerMap(47, 300, 400), nil, nil},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err != nil {
		t.Fatal(err)
	}
	if f.index != 3 || !reflect.DeepEqual(f.controls(), []string{"stop:a", "start:a", "stop:b", "start:b"}) {
		t.Fatal("stale membership passed or restart repeated", f.index, f.controls())
	}
	if len(f.receipts) != 4 {
		t.Fatal("first registered refusals and missing restart intents not retained", f.receipts)
	}
	for i, receipt := range f.receipts[:2] {
		if !receipt.Registered || receipt.Epoch != 44 || receipt.GID != uint64(100*(i+1)) || receipt.NativeRejection != []string{"first-a-refusal", "first-b-refusal"}[i] ||
			receipt.Action != managerActionRegisteredRefusal || !slices.Contains(receipt.RefusedGlobalIDs, receipt.GID) {
			t.Fatal("first refusal identity was replaced", receipt)
		}
	}
	for i, receipt := range f.receipts[2:] {
		if receipt.Registered || receipt.Epoch != 45 || receipt.NativeRejection != f.receipts[i].NativeRejection ||
			receipt.Action != managerActionRestoreMissing || !reflect.DeepEqual(receipt.RefusedGlobalIDs, f.receipts[i].RefusedGlobalIDs) {
			t.Fatal("restart lost first native refusal", receipt)
		}
	}
	if !reflect.DeepEqual(f.events[:3], []string{"log:a", "log:b", "map"}) {
		t.Fatal("readiness checked before latest native logs", f.events)
	}
}

func TestManagerPreparationHealthyMembershipPerformsNoRestartOrReceipt(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{{state: managerMap(44, 100, 200)}}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err != nil {
		t.Fatal(err)
	}
	if len(f.controls()) != 0 || len(f.receipts) != 0 || f.index != 0 {
		t.Fatal("healthy fixture mutated", f)
	}
}

func TestManagerPreparationNewGIDWithSameProcessRefusalDoesNotInferHealth(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 200), map[string]string{"a": "native-refusal"}, map[string][]uint64{"a": {100}}},
		{managerMap(45, 300, 200), map[string]string{"a": "native-refusal"}, map[string][]uint64{"a": {100}}},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err == nil {
		t.Fatal("same process refusal plus new GID incorrectly admitted")
	}
	if len(f.controls()) != 0 || len(f.receipts) != 1 || f.receipts[0].GID != 100 {
		t.Fatal("present refused daemon restarted or first evidence overwritten", f)
	}
}

func TestManagerPreparationRejectsOutsideAndDuplicateIdentitiesWithoutControls(t *testing.T) {
	valid := managerMap(44, 100, 200)
	for _, fault := range []string{"outside active", "outside standby", "same name", "same gid", "zero epoch", "duplicate owned"} {
		state := valid
		state.Standbys = append(state.Standbys[:0:0], valid.Standbys...)
		owned := []string{"a", "b"}
		switch fault {
		case "outside active":
			state.ActiveName = "other"
		case "outside standby":
			state.Standbys[0].Name = "other"
		case "same name":
			state.Standbys[0].Name = "a"
		case "same gid":
			state.Standbys[0].GID = 100
		case "zero epoch":
			state.Epoch = 0
		case "duplicate owned":
			owned = []string{"a", "a"}
		}
		f := fakeManagerPreparation{frames: []managerPreparationFrame{{state: state}}}
		if err := prepareManagerFailoverWithBackend(context.Background(), owned, f.backend(), time.Second); err == nil || len(f.controls()) != 0 || len(f.receipts) != 0 {
			t.Fatal("invalid ownership allowed preparation", fault, err, f)
		}
	}
}

func TestManagerPreparationFailedStartRetainsFirstRefusalWithoutReplay(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{{managerMap(45, 100, 0), map[string]string{"b": "native-refusal"}, nil}}, startErr: errors.New("start failed")}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err == nil || !strings.Contains(err.Error(), "start failed") {
		t.Fatal("failed start not returned", err)
	}
	if !reflect.DeepEqual(f.controls(), []string{"stop:b", "start:b"}) || len(f.receipts) != 1 || f.receipts[0].NativeRejection != "native-refusal" || f.receipts[0].Registered {
		t.Fatal("failed start lost original receipt or replayed control", f)
	}
}

func TestManagerPreparationMissingWithoutExplicitRefusalIsNeverRestarted(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{{state: managerMap(45, 100, 0)}}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err == nil {
		t.Fatal("missing standby admitted")
	}
	if len(f.controls()) != 0 || len(f.receipts) != 0 {
		t.Fatal("absence without exact latest-process refusal authorized restoration", f)
	}
}

func TestManagerPreparationCancelledLateHealthyObservationCannotPublishReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := fakeManagerPreparation{frames: []managerPreparationFrame{{state: managerMap(44, 100, 200)}}}
	backend := f.backend()
	backend.observe = func(context.Context) (managerPreparationMap, error) {
		cancel()
		return managerMap(44, 100, 200), nil
	}
	if err := prepareManagerFailoverWithBackend(ctx, []string{"a", "b"}, backend, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal("late map published readiness after cancellation", err)
	}
	if len(f.controls()) != 0 || len(f.receipts) != 0 {
		t.Fatal("late observation triggered controls/receipts", f)
	}
}

func TestManagerPreparationParentCancellationAndChildDeadlineBoundObservations(t *testing.T) {
	for _, cancelled := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		f := fakeManagerPreparation{frames: []managerPreparationFrame{{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, nil}}}
		backend := f.backend()
		backend.wait = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
		err := prepareManagerFailoverWithBackend(ctx, []string{"a", "b"}, backend, 20*time.Millisecond)
		cancel()
		want := context.DeadlineExceeded
		if cancelled {
			want = context.Canceled
		}
		if !errors.Is(err, want) || len(f.controls()) != 0 || cancelled && len(f.events) != 0 {
			t.Fatal("cancel/deadline lost or native controls used", cancelled, err, f)
		}
	}
}

// CI job 116165091788: MON b/c refused mgr.b's other client 15059 while the
// registered beacon client 5280 stayed in MgrMap. That is not a stale slot.
func TestManagerPreparationRetainsRegisteredSlotWhenMONsRefusedAnotherClient(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, map[string][]uint64{"b": {300, 301}}},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err != nil {
		t.Fatal(err)
	}
	if f.index != 0 || len(f.controls()) != 0 || len(f.receipts) != 1 {
		t.Fatal("non-registered MonClient refusal blocked or mutated healthy slots", f)
	}
	receipt := f.receipts[0]
	if receipt.Daemon != "mgr.b" || !receipt.Registered || receipt.GID != 200 || receipt.Epoch != 44 || receipt.NativeRejection != "native-refusal" ||
		receipt.Action != managerActionOtherClient || !reflect.DeepEqual(receipt.RefusedGlobalIDs, []uint64{300, 301}) {
		t.Fatal("retained-slot receipt lost its attribution", receipt)
	}
}

func TestManagerPreparationUnattributedRegisteredRefusalStillFails(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, nil},
		{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, nil},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err == nil {
		t.Fatal("refusal without MON attribution admitted")
	}
	if len(f.controls()) != 0 || len(f.receipts) != 1 || f.receipts[0].Action != managerActionRegisteredRefusal || f.receipts[0].RefusedGlobalIDs != nil {
		t.Fatal("unattributed present refusal restarted or masked", f)
	}
}

// Ceph logs asynchronously, so MON attribution may trail the daemon line.
func TestManagerPreparationLateAttributionToAnotherClientReleasesBlock(t *testing.T) {
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, nil},
		{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}, map[string][]uint64{"b": {300}}},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err != nil {
		t.Fatal(err)
	}
	if f.index != 1 || len(f.controls()) != 0 || len(f.receipts) != 2 || f.receipts[1].Action != managerActionOtherClient {
		t.Fatal("late attribution handling", f)
	}
}

func TestManagerPreparationLaterRegisteredRefusalBecomesStaleOnce(t *testing.T) {
	other := map[string][]uint64{"a": {300}}
	f := fakeManagerPreparation{frames: []managerPreparationFrame{
		{managerMap(44, 100, 0), map[string]string{"a": "first"}, other},
		{managerMap(44, 100, 0), map[string]string{"a": "first"}, other}, // Same evidence: no new receipt.
		{managerMap(45, 100, 200), map[string]string{"a": "second"}, map[string][]uint64{"a": {100, 300}}},
		{managerMap(46, 400, 200), map[string]string{"a": "second"}, map[string][]uint64{"a": {300}}}, // Stale stays stale
		{managerMap(46, 400, 200), map[string]string{"a": "second"}, map[string][]uint64{"a": {300}}}, // across a new GID.
		{managerMap(47, 0, 200), map[string]string{"a": "second"}, map[string][]uint64{"a": {100, 300}}},
		{managerMap(48, 500, 200), nil, nil},
	}}
	if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err != nil {
		t.Fatal(err)
	}
	actions := make([]string, 0, len(f.receipts))
	for _, receipt := range f.receipts {
		actions = append(actions, receipt.Action)
	}
	want := []string{managerActionOtherClient, managerActionRegisteredRefusal, managerActionRestoreMissing}
	if f.index != 6 || !reflect.DeepEqual(f.controls(), []string{"stop:a", "start:a"}) || !reflect.DeepEqual(actions, want) {
		t.Fatal("registered refusal after a retained slot did not take the once-only stale path", f.index, f.controls(), actions)
	}
	if f.receipts[2].NativeRejection != "second" || !reflect.DeepEqual(f.receipts[2].RefusedGlobalIDs, []uint64{100, 300}) {
		t.Fatal("restart receipt lost the stale evidence", f.receipts[2])
	}
}

func TestManagerPreparationRejectsInconsistentAttribution(t *testing.T) {
	for _, tc := range []struct {
		line    string
		refused []uint64
	}{{"", []uint64{300}}, {"native-refusal", []uint64{0}}} {
		f := fakeManagerPreparation{frames: []managerPreparationFrame{{managerMap(44, 100, 200), map[string]string{"b": tc.line}, map[string][]uint64{"b": tc.refused}}}}
		if err := prepareManagerFailoverWithBackend(context.Background(), []string{"a", "b"}, f.backend(), time.Second); err == nil || len(f.controls()) != 0 || len(f.receipts) != 0 {
			t.Fatal("inconsistent refusal attribution accepted", tc, err, f)
		}
	}
}

const reclaimProcessStart = "2026-10-14T23:52:04.137+0000"

func reclaimSince(t *testing.T) time.Time {
	t.Helper()
	since, err := cephLogTime(reclaimProcessStart)
	if err != nil {
		t.Fatal(err)
	}
	return since
}

func TestScanMonitorReclaimRefusalsCollectsLatestProcessEntityIDs(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-14T23:52:03.000+0000 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 1 using bad ticket", // Before process start.
		"2026-10-14T23:52:03.500+0000 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 01x",                // Earlier process.
		reclaimProcessStart + " 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket",   // At start.
		"2026-10-14T23:52:20.500+0000 7ff7ec39b640  0 cephx: ceph_decode_ticket could not get service secret for service_id=auth secret_id=13",
		"2026-10-14T23:52:20.500+0000 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket",
		"2026-10-14T23:52:20.500+0000 7ff7ec39b640  0 cephx server mgr.b:  could not verify old ticket",
		"2026-10-14T23:52:20.501123+0000 7ff7f63a5640  0 cephx server mgr.b:  attempt to reclaim global_id 5280 without presenting ticket",
		"2026-10-15T08:52:20.502+0900 7ff7f63a5640 -1 cephx server mgr.b:  attempt to reclaim global_id 18446744073709551615 using mismatching ticket\r",
		"2026-10-15T08:52:03.000+0900 7ff7f63a5640  0 cephx server mgr.b:  attempt to reclaim global_id 2 using bad ticket", // Before start in another zone.
		"2026-10-14T23:52:20.502+0000 7ff7f63a5640  0 cephx server mgr.a:  attempt to reclaim global_id 15056 using bad ticket",
		"2026-10-14T23:52:20.502+0000 7ff7f63a5640  0 cephx server mgr.bb:  attempt to reclaim global_id 7 using bad ticket",
		"2026-10-14T23:52:20.502+0000 7ff7f63a5640  0 cephx server client.admin:  attempt to reclaim global_id 8 using bad ticket",
		"2026-10-14T23:52:20.502+0000 7ff7f63a5640  1 mon.c@2(peon).auth v26 stopping due to failed auth ((13) Permission denied)",
		"untimestamped entrypoint output",
	}, "\n") + "\n"
	got, err := scanMonitorReclaimRefusals(strings.NewReader(log), "b", reclaimSince(t), len(log))
	if err != nil || !reflect.DeepEqual(got, []uint64{5280, 15059, 18446744073709551615}) {
		t.Fatal("MON reclaim attribution", got, err)
	}
	if got, err := scanMonitorReclaimRefusals(strings.NewReader(log), "c", reclaimSince(t), len(log)); err != nil || len(got) != 0 {
		t.Fatal("other daemon attributed", got, err)
	}
}

func TestScanMonitorReclaimRefusalsRejectsMalformedAttribution(t *testing.T) {
	const prefix = reclaimProcessStart + " 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id "
	for _, line := range []string{
		prefix + "15059abc using bad ticket",
		prefix + "-1 using bad ticket",
		prefix + "+1 using bad ticket",
		prefix + "0 using bad ticket",
		prefix + "015059 using bad ticket",
		prefix + " using bad ticket",
		prefix + "18446744073709551616 using bad ticket",
		prefix + "15059",
		prefix + "15059 using other ticket",
		prefix + "15059 using bad ticket trailing",
		"2026-10-14 23:52:20.500 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket",
		reclaimProcessStart + " 7ff7ec39b640  0 audit cmd=[cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket]",
	} {
		if got, err := scanMonitorReclaimRefusals(strings.NewReader(line+"\n"), "b", reclaimSince(t), 1<<20); err == nil {
			t.Fatal("malformed MON attribution accepted", line, got)
		}
	}
}

func TestScanMonitorReclaimRefusalsEnforcesByteBound(t *testing.T) {
	log := reclaimProcessStart + " 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket\n"
	if got, err := scanMonitorReclaimRefusals(strings.NewReader(log), "b", reclaimSince(t), len(log)); err != nil || !reflect.DeepEqual(got, []uint64{15059}) {
		t.Fatal("log at bound rejected", got, err)
	}
	if got, err := scanMonitorReclaimRefusals(strings.NewReader(log+"x"), "b", reclaimSince(t), len(log)); err == nil || !strings.Contains(err.Error(), "bound") {
		t.Fatal("log beyond bound accepted", got, err)
	}
	if got, err := scanMonitorReclaimRefusals(strings.NewReader(strings.Repeat("y", 4096)), "b", reclaimSince(t), 1024); err == nil || !strings.Contains(err.Error(), "bound") {
		t.Fatal("single oversized line accepted", got, err)
	}
	for _, scope := range []struct {
		name  string
		since time.Time
		limit int
	}{{"", reclaimSince(t), 1}, {"b", time.Time{}, 1}, {"b", reclaimSince(t), 0}} {
		if _, err := scanMonitorReclaimRefusals(strings.NewReader(log), scope.name, scope.since, scope.limit); err == nil {
			t.Fatal("unbounded or unscoped attribution accepted", scope)
		}
	}
}

type closeRecorder struct {
	io.Reader
	closed *int
}

func (c closeRecorder) Close() error { *c.closed++; return nil }

func TestMonitorReclaimRefusalsUnionsOwnedMONsAndFailsExplicitly(t *testing.T) {
	closed := 0
	source := func(name, log string) monitorLogSource {
		return monitorLogSource{name: name, logs: func(context.Context) (io.ReadCloser, error) {
			return closeRecorder{strings.NewReader(log), &closed}, nil
		}}
	}
	line := func(id string) string {
		return reclaimProcessStart + " 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id " + id + " using bad ticket\n"
	}
	got, err := monitorReclaimRefusals(context.Background(), []monitorLogSource{source("a", ""), source("b", line("15059")), source("c", line("15059")+line("200"))}, "b", reclaimSince(t))
	if err != nil || !reflect.DeepEqual(got, []uint64{200, 15059}) || closed != 3 {
		t.Fatal("owned MON attribution union", got, err, closed)
	}
	if _, err := monitorReclaimRefusals(context.Background(), nil, "b", reclaimSince(t)); err == nil {
		t.Fatal("missing MON evidence treated as no refusal")
	}
	failing := monitorLogSource{name: "c", logs: func(context.Context) (io.ReadCloser, error) { return nil, errors.New("logs unavailable") }}
	if _, err := monitorReclaimRefusals(context.Background(), []monitorLogSource{source("a", ""), failing}, "b", reclaimSince(t)); err == nil || !strings.Contains(err.Error(), "mon.c") {
		t.Fatal("unreadable MON log masked", err)
	}
	if _, err := monitorReclaimRefusals(context.Background(), []monitorLogSource{source("a", line("x"))}, "b", reclaimSince(t)); err == nil || !strings.Contains(err.Error(), "mon.a") {
		t.Fatal("malformed MON log masked", err)
	}
}
