package main

import (
	"context"
	"errors"
	"reflect"
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
			if got := managerAuthRejection(tc.log) != ""; got != tc.want {
				t.Fatalf("rejection=%t want=%t", got, tc.want)
			}
		})
	}
}

func TestManagerAuthRejectionReturnsLatestLineWithinLatestProcess(t *testing.T) {
	const start = "0 ceph version 20.2.4 tentacle, process ceph-mgr\n"
	const rejected = " monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"
	log := start + "old" + rejected + "\n" + start + "first" + rejected + "\nlast" + rejected + "\n"
	if got := managerAuthRejection(log); got != "last"+rejected {
		t.Fatalf("matched refusal %q must be latest line from latest process", got)
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
	state managerPreparationMap
	lines map[string]string
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
		rejection: func(ctx context.Context, name string) (string, error) {
			f.events = append(f.events, "log:"+name)
			return f.frames[f.index].lines[name], ctx.Err()
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
		{managerMap(44, 100, 200), map[string]string{"a": "first-a-refusal", "b": "first-b-refusal"}},
		{managerMap(45, 0, 0), map[string]string{"a": "latest-a-refusal", "b": "latest-b-refusal"}},
		{managerMap(46, 100, 200), nil}, // Clean process logs cannot bless the old registered GIDs.
		{managerMap(47, 300, 400), nil},
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
		if !receipt.Registered || receipt.Epoch != 44 || receipt.GID != uint64(100*(i+1)) || receipt.NativeRejection != []string{"first-a-refusal", "first-b-refusal"}[i] {
			t.Fatal("first refusal identity was replaced", receipt)
		}
	}
	for i, receipt := range f.receipts[2:] {
		if receipt.Registered || receipt.Epoch != 45 || receipt.NativeRejection != f.receipts[i].NativeRejection {
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
		{managerMap(44, 100, 200), map[string]string{"a": "native-refusal"}},
		{managerMap(45, 300, 200), map[string]string{"a": "native-refusal"}},
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
	f := fakeManagerPreparation{frames: []managerPreparationFrame{{managerMap(45, 100, 0), map[string]string{"b": "native-refusal"}}}, startErr: errors.New("start failed")}
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
		f := fakeManagerPreparation{frames: []managerPreparationFrame{{managerMap(44, 100, 200), map[string]string{"b": "native-refusal"}}}}
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
