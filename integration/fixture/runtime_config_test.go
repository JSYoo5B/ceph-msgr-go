package main

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// fakeRuntimeDaemon records every native step of a runtimeConfig in order.
type fakeRuntimeDaemon struct {
	calls  []string
	values map[string]string
	// setErr and getErr fail the named option's set or get once reached.
	setErr, getErr map[string]error
	// readback overrides what get reports for an option.
	readback map[string]string
	windows  []*fakeDebugWindow
	// window configures the next window debug opens.
	window func(w *fakeDebugWindow) error
}

type fakeDebugWindow struct {
	daemon  *fakeRuntimeDaemon
	receipt ceph.DebugLevelReceipt
	restore func(w *fakeDebugWindow) error
}

func (w *fakeDebugWindow) Receipt() ceph.DebugLevelReceipt { return w.receipt }

func (w *fakeDebugWindow) Restore(context.Context) error {
	w.daemon.calls = append(w.daemon.calls, "restore-window "+w.receipt.Name)
	if w.restore != nil {
		return w.restore(w)
	}
	w.daemon.values[w.receipt.Name] = w.receipt.Prior
	w.receipt.State, w.receipt.Restored = ceph.DebugLevelRestored, w.receipt.Prior
	return nil
}

func newFakeRuntimeDaemon() *fakeRuntimeDaemon {
	return &fakeRuntimeDaemon{values: map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "false"},
		setErr: map[string]error{}, getErr: map[string]error{}, readback: map[string]string{}}
}

func (f *fakeRuntimeDaemon) config(guard bool) *runtimeConfig {
	r := &runtimeConfig{daemon: "osd.0", report: "DATA OSD", scope: mdsCephFSDataOSDConfigNames, scopeErr: errors.New("DATA OSD setting is outside closed scope"),
		set: func(_ context.Context, name, value string) error {
			f.calls = append(f.calls, "set "+name+"="+value)
			if err := f.setErr[name]; err != nil {
				return err
			}
			f.values[name] = value
			return nil
		},
		get: func(_ context.Context, name string) (string, error) {
			f.calls = append(f.calls, "get "+name)
			if err := f.getErr[name]; err != nil {
				return "", err
			}
			if value, ok := f.readback[name]; ok {
				return value, nil
			}
			return f.values[name], nil
		},
		debug: func(_ context.Context, name, level string) (runtimeDebugWindow, error) {
			f.calls = append(f.calls, "open-window "+name+"="+level)
			w := &fakeDebugWindow{daemon: f, receipt: ceph.DebugLevelReceipt{Daemon: "osd.0", Name: name, Requested: level, Prior: f.values[name], State: ceph.DebugLevelApplied, Applied: level}}
			var err error
			if f.window != nil {
				err = f.window(w)
			}
			if w.receipt.State != ceph.DebugLevelNotApplied {
				f.values[name] = w.receipt.Applied
			}
			f.windows = append(f.windows, w)
			return w, err
		},
		debugNames: []string{"debug_ms", "debug_osd"},
	}
	if guard {
		r.guard = func(context.Context) error { f.calls = append(f.calls, "guard"); return f.setErr["guard"] }
	}
	return r
}

var (
	runtimeDropOrder    = []string{"debug_ms", "ms_blackhole_client"}
	runtimeRestoreOrder = []string{"ms_blackhole_client", "debug_ms"}
	runtimeDropSettings = map[string]string{"debug_ms": "10/10", "ms_blackhole_client": "true"}
	runtimeDropBaseline = map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "false"}
)

func TestRuntimeConfigKeepsOrderAndReadbacks(t *testing.T) {
	f := newFakeRuntimeDaemon()
	r := f.config(false)
	var readback, restored map[string]string
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(readback, runtimeDropSettings) || len(r.windows) != 1 {
		t.Fatalf("readback %v windows %v", readback, r.windows)
	}
	if err := r.restore(context.Background(), runtimeRestoreOrder, runtimeDropBaseline, &restored); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(restored, runtimeDropBaseline) || len(r.windows) != 0 || !maps.Equal(f.values, runtimeDropBaseline) {
		t.Fatalf("restored %v windows %v values %v", restored, r.windows, f.values)
	}
	want := []string{"open-window debug_ms=10/10", "set ms_blackhole_client=true", "get ms_blackhole_client",
		"set ms_blackhole_client=false", "get ms_blackhole_client", "restore-window debug_ms"}
	if !slices.Equal(f.calls, want) {
		t.Fatalf("calls %q, want %q", f.calls, want)
	}
	// A second drop after a verified restoration opens a new window.
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err != nil || len(f.windows) != 2 {
		t.Fatalf("second apply: %v, windows %d", err, len(f.windows))
	}
}

func TestRuntimeConfigGuardRunsAroundEachStep(t *testing.T) {
	f := newFakeRuntimeDaemon()
	r := f.config(true)
	r.debug = nil
	var readback map[string]string
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err != nil {
		t.Fatal(err)
	}
	want := []string{"guard", "set debug_ms=10/10", "get debug_ms", "guard", "set ms_blackhole_client=true", "get ms_blackhole_client", "guard"}
	if !slices.Equal(f.calls, want) {
		t.Fatalf("calls %q, want %q", f.calls, want)
	}
	f.calls, f.setErr["guard"] = nil, errors.New("incarnation changed")
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err == nil || !slices.Equal(f.calls, []string{"guard"}) {
		t.Fatalf("failed guard: %v, calls %q", err, f.calls)
	}
}

func TestRuntimeConfigRefusesOutsideScopeWithoutNativeCall(t *testing.T) {
	for _, values := range []map[string]string{{"debug_ms": ""}, {"debug_osd": "1/5"}} {
		f := newFakeRuntimeDaemon()
		r := f.config(true)
		var readback map[string]string
		err := r.apply(context.Background(), slices.Sorted(maps.Keys(values)), values, &readback)
		if err == nil || err.Error() != "DATA OSD setting is outside closed scope" || len(f.calls) != 0 {
			t.Fatalf("%v: %v, calls %q", values, err, f.calls)
		}
	}
}

func TestRuntimeConfigReadbackMismatchFails(t *testing.T) {
	f := newFakeRuntimeDaemon()
	f.readback["ms_blackhole_client"] = "false"
	r := f.config(false)
	var readback map[string]string
	err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback)
	if err == nil || !strings.Contains(err.Error(), "DATA OSD reports ms_blackhole_client=false after setting true") || readback["ms_blackhole_client"] != "false" {
		t.Fatalf("direct mismatch: %v, readback %v", err, readback)
	}

	// A window whose verified readback differs from the exact requested
	// text fails the step too.
	f = newFakeRuntimeDaemon()
	f.window = func(w *fakeDebugWindow) error { w.receipt.Applied = "10/5"; return nil }
	r = f.config(false)
	err = r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback)
	if err == nil || !strings.Contains(err.Error(), "debug_ms=10/5 after setting 10/10") || readback["debug_ms"] != "10/5" || slices.Contains(f.calls, "set ms_blackhole_client=true") {
		t.Fatalf("window mismatch: %v, readback %v, calls %q", err, readback, f.calls)
	}

	// A restoration that reads back another value than the baseline fails.
	f = newFakeRuntimeDaemon()
	r = f.config(false)
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err != nil {
		t.Fatal(err)
	}
	f.windows[0].receipt.Prior = "1/5"
	err = r.restore(context.Background(), runtimeRestoreOrder, runtimeDropBaseline, &readback)
	if err == nil || !strings.Contains(err.Error(), "debug_ms=1/5 after setting 0/5") || readback["debug_ms"] != "1/5" {
		t.Fatalf("restore mismatch: %v, readback %v", err, readback)
	}
}

func TestRuntimeConfigFailedStepIsNotRetried(t *testing.T) {
	f := newFakeRuntimeDaemon()
	f.setErr["ms_blackhole_client"] = errors.New("lost reply")
	r := f.config(false)
	var readback map[string]string
	err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback)
	want := []string{"open-window debug_ms=10/10", "set ms_blackhole_client=true"}
	if err == nil || err.Error() != "lost reply" || !slices.Equal(f.calls, want) || !maps.Equal(readback, map[string]string{"debug_ms": "10/10"}) {
		t.Fatalf("apply: %v, calls %q, readback %v", err, f.calls, readback)
	}
	// The applied window stays owned; a second apply of the same option is
	// refused before any native call.
	f.calls = nil
	err = r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback)
	if err == nil || !strings.Contains(err.Error(), "already has an open debug_ms window") || len(f.calls) != 0 {
		t.Fatalf("replay: %v, calls %q", err, f.calls)
	}
	// Restoration still owns the window: it is restored, never reopened.
	f.calls, f.setErr["ms_blackhole_client"] = nil, nil
	if err := r.restore(context.Background(), runtimeRestoreOrder, runtimeDropBaseline, &readback); err != nil {
		t.Fatal(err)
	}
	want = []string{"set ms_blackhole_client=false", "get ms_blackhole_client", "restore-window debug_ms"}
	if !slices.Equal(f.calls, want) || len(f.windows) != 1 || len(r.windows) != 0 {
		t.Fatalf("restore calls %q, windows %d/%d", f.calls, len(f.windows), len(r.windows))
	}
}

func TestRuntimeConfigKeepsUncertainWindow(t *testing.T) {
	f := newFakeRuntimeDaemon()
	f.window = func(w *fakeDebugWindow) error {
		w.receipt.State, w.receipt.Applied = ceph.DebugLevelApplyUncertain, ""
		return errors.New("set debug_ms of osd.0: context deadline exceeded")
	}
	r := f.config(false)
	var readback map[string]string
	err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback)
	if err == nil || !strings.Contains(err.Error(), "deadline") || len(readback) != 0 || len(r.windows) != 1 || slices.Contains(f.calls, "set ms_blackhole_client=true") {
		t.Fatalf("uncertain apply: %v, readback %v, windows %d, calls %q", err, readback, len(r.windows), f.calls)
	}
	// An unconfirmed restoration keeps the window for a later attempt, which
	// reconciles through the same window.
	f.windows[0].restore = func(w *fakeDebugWindow) error {
		w.receipt.State = ceph.DebugLevelUnconfirmed
		w.receipt.Transitions = append(w.receipt.Transitions, ceph.DebugLevelTransition{State: ceph.DebugLevelUnconfirmed, Readback: "10/10"})
		return errors.New("debug_ms of osd.0 still reads back the window's \"10/10\"")
	}
	err = r.restore(context.Background(), runtimeRestoreOrder, runtimeDropBaseline, &readback)
	if err == nil || len(r.windows) != 1 || readback["debug_ms"] != "10/10" {
		t.Fatalf("unconfirmed restore: %v, windows %d, readback %v", err, len(r.windows), readback)
	}
	f.windows[0].restore = nil
	if err := r.restore(context.Background(), runtimeRestoreOrder, runtimeDropBaseline, &readback); err != nil || len(r.windows) != 0 || len(f.windows) != 1 {
		t.Fatalf("second restore: %v, windows %d/%d", err, len(r.windows), len(f.windows))
	}
}

func TestRuntimeConfigRestoreWithoutWindowOnlyObserves(t *testing.T) {
	f := newFakeRuntimeDaemon()
	f.window = func(w *fakeDebugWindow) error {
		w.receipt.State, w.receipt.Applied = ceph.DebugLevelNotApplied, ""
		return errors.New("a window for this daemon and option is open")
	}
	r := f.config(false)
	var readback map[string]string
	if err := r.apply(context.Background(), runtimeDropOrder, runtimeDropSettings, &readback); err == nil || len(r.windows) != 0 {
		t.Fatalf("refused window: %v, windows %d", err, len(r.windows))
	}
	f.calls = nil
	if err := r.restore(context.Background(), []string{"debug_ms"}, runtimeDropBaseline, &readback); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.calls, []string{"get debug_ms"}) || readback["debug_ms"] != "0/5" {
		t.Fatalf("calls %q, readback %v", f.calls, readback)
	}
}

func TestOSDRuntimeConfigRefusesWithoutCluster(t *testing.T) {
	r := osdRuntimeConfig(nil, "osd.0", "DATA OSD", mdsCephFSDataOSDConfigNames, nil)
	var readback map[string]string
	err := r.apply(context.Background(), []string{"debug_ms"}, runtimeDropSettings, &readback)
	// override.SetDebugLevel refuses a nil cluster as not applied, so
	// nothing is owned for restoration.
	if !errors.Is(err, ceph.ErrInvalidRequest) || len(r.windows) != 0 || len(readback) != 0 {
		t.Fatalf("apply: %v, windows %d, readback %v", err, len(r.windows), readback)
	}
	if err := r.apply(context.Background(), []string{"ms_blackhole_client"}, runtimeDropSettings, &readback); err == nil || !strings.Contains(err.Error(), "no cluster") {
		t.Fatalf("direct: %v", err)
	}
}

func TestMDSAdminSocketRuntimeConfigKeepsNativePath(t *testing.T) {
	var calls []string
	values := map[string]string{"mds_log_pause": "false"}
	r := mdsAdminSocketRuntimeConfig("DATA daemon", func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		if args[1] == "set" {
			values[args[2]] = args[3]
			return []byte("{}"), nil
		}
		return []byte(`{"` + args[2] + `":"` + values[args[2]] + `"}`), nil
	})
	var readback map[string]string
	if err := r.apply(context.Background(), []string{"mds_log_pause"}, map[string]string{"mds_log_pause": "true"}, &readback); err != nil || readback["mds_log_pause"] != "true" {
		t.Fatalf("apply: %v, readback %v", err, readback)
	}
	if err := r.restore(context.Background(), []string{"mds_log_pause"}, map[string]string{"mds_log_pause": "false"}, &readback); err != nil || readback["mds_log_pause"] != "false" {
		t.Fatalf("restore: %v, readback %v", err, readback)
	}
	want := []string{"config set mds_log_pause true", "config get mds_log_pause", "config set mds_log_pause false", "config get mds_log_pause"}
	if !slices.Equal(calls, want) {
		t.Fatalf("calls %q, want %q", calls, want)
	}
}

func TestDataOSDRuntimeStaysBoundToItsOSD(t *testing.T) {
	f := newFakeRuntimeDaemon()
	c := &fixtureCluster{mdsCephFSData: &mdsCephFSDataScope{osdRuntime: f.config(false)}}
	var readback map[string]string
	if err := c.mdsCephFSDataOSDApply(context.Background(), "osd.0", runtimeDropOrder, runtimeDropSettings, &readback); err != nil {
		t.Fatal(err)
	}
	if baseline, err := c.mdsCephFSDataOSDConfig(context.Background(), "osd.0"); err != nil || !maps.Equal(baseline, runtimeDropSettings) {
		t.Fatalf("config: %v %v", baseline, err)
	}
	readback = map[string]string{"stale": "x"}
	if err := c.mdsCephFSDataOSDRestore(context.Background(), "osd.1", runtimeRestoreOrder, runtimeDropBaseline, &readback); err == nil || len(readback) != 0 {
		t.Fatalf("other OSD: %v, readback %v", err, readback)
	}
	if err := c.mdsCephFSDataOSDRestore(context.Background(), "osd.0", runtimeRestoreOrder, runtimeDropBaseline, &readback); err != nil || !maps.Equal(readback, runtimeDropBaseline) {
		t.Fatalf("restore: %v, readback %v", err, readback)
	}
}
