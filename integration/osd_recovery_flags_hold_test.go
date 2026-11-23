package integration_test

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// flagHoldRecorder is a testing.TB whose Fatal ends only the calling
// goroutine and whose cleanups run when the recorded test ends.
type flagHoldRecorder struct {
	testing.TB
	failures []string
	cleanups []func()
}

func (r *flagHoldRecorder) Helper()           {}
func (r *flagHoldRecorder) Cleanup(f func())  { r.cleanups = append(r.cleanups, f) }
func (r *flagHoldRecorder) Fatal(args ...any) { r.fail(fmt.Sprint(args...)) }
func (r *flagHoldRecorder) Fatalf(f string, args ...any) {
	r.fail(fmt.Sprintf(f, args...))
}
func (r *flagHoldRecorder) fail(message string) {
	r.failures = append(r.failures, message)
	runtime.Goexit()
}

// run runs body as the recorded test, then its cleanups in reverse order,
// each in its own goroutine as the testing package does.
func (r *flagHoldRecorder) run(body func()) {
	runGoroutine := func(f func()) {
		done := make(chan struct{})
		go func() { defer close(done); f() }()
		<-done
	}
	runGoroutine(body)
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		runGoroutine(r.cleanups[i])
	}
}

// fakeOSDFlags is the native flag state as mode_osdflags changes and reports
// it; keepOnUnset leaves the named flags set after an unset, and dropOnSet
// leaves the named flag unset after a set.
type fakeOSDFlags struct {
	flags       []string
	actions     []string
	keepOnUnset map[string]bool
	dropOnSet   string
}

func (f *fakeOSDFlags) run(_ context.Context, action string) []string {
	f.actions = append(f.actions, action)
	switch action {
	case "set":
		for _, name := range recoveryFlagNames {
			if name != f.dropOnSet && !slices.Contains(f.flags, name) {
				f.flags = append(f.flags, name)
			}
		}
	case "unset":
		f.flags = slices.DeleteFunc(f.flags, func(flag string) bool { return slices.Contains(recoveryFlagNames, flag) && !f.keepOnUnset[flag] })
	}
	return slices.Clone(f.flags)
}

// A hold sets and clears only flags it set, refuses a dirty baseline without
// sending a command that changes it, rejects a set that left a flag out, and
// stays armed through a failed or unverified release so that its cleanup
// clears the flags.
func TestRecoveryFlagHoldClearsOnlyVerifiedFlags(t *testing.T) {
	ctx := context.Background()
	t.Run("set-then-released", func(t *testing.T) {
		r, f := &flagHoldRecorder{}, &fakeOSDFlags{flags: []string{"sortbitwise", "noscrub"}}
		r.run(func() {
			h := newRecoveryFlagHoldWith(r, f.run)
			h.hold(ctx)
			h.release(ctx)
		})
		if len(r.failures) != 0 || !slices.Equal(f.actions, []string{"show", "set", "unset", "show"}) || !slices.Equal(f.flags, []string{"sortbitwise", "noscrub"}) {
			t.Fatal("a clean hold and release differ", r.failures, f.actions, f.flags)
		}
	})
	for _, dirty := range recoveryFlagNames {
		t.Run("dirty-baseline-"+dirty, func(t *testing.T) {
			r, f := &flagHoldRecorder{}, &fakeOSDFlags{flags: []string{"sortbitwise", dirty}}
			r.run(func() { newRecoveryFlagHoldWith(r, f.run).hold(ctx) })
			if len(r.failures) != 1 || !strings.Contains(r.failures[0], "already set") || !slices.Equal(f.actions, []string{"show"}) || !slices.Equal(f.flags, []string{"sortbitwise", dirty}) {
				t.Fatal("a dirty baseline was touched or accepted", r.failures, f.actions, f.flags)
			}
		})
	}
	t.Run("substring-is-not-a-flag", func(t *testing.T) {
		if recoveryFlagsAre([]string{"norecover-x", "nobackfill"}, true) || !recoveryFlagsAre([]string{"xnorecover", "nobackfillx"}, false) || recoveryFlagsAre([]string{"norecover", "norecover", "nobackfill"}, true) {
			t.Fatal("recovery flags are not compared as whole tokens")
		}
	})
	for _, missing := range recoveryFlagNames {
		t.Run("set-without-"+missing, func(t *testing.T) {
			r, f := &flagHoldRecorder{}, &fakeOSDFlags{dropOnSet: missing}
			r.run(func() { newRecoveryFlagHoldWith(r, f.run).hold(ctx) })
			if len(r.failures) != 1 || !strings.Contains(r.failures[0], "not both set") || !slices.Equal(f.actions, []string{"show", "set", "unset", "show"}) || len(f.flags) != 0 {
				t.Fatal("an incomplete set was accepted or not cleared by the cleanup", r.failures, f.actions, f.flags)
			}
		})
	}
	t.Run("unset-failure-stays-armed", func(t *testing.T) {
		f := &fakeOSDFlags{keepOnUnset: map[string]bool{"nobackfill": true}}
		r := &flagHoldRecorder{}
		var h *recoveryFlagHold
		r.run(func() {
			h = newRecoveryFlagHoldWith(r, f.run)
			h.hold(ctx)
			h.release(ctx)
		})
		// The release failed and the cleanup tried again; its failure is
		// reported and the hold is still armed.
		if len(r.failures) != 2 || !strings.Contains(r.failures[0], "still set after the unset") || !h.armed || !slices.Equal(f.actions, []string{"show", "set", "unset", "unset"}) {
			t.Fatal("a failed unset disarmed the hold", r.failures, f.actions, h.armed)
		}
		// Once the cluster lets the flag go, the cleanup clears it.
		f.keepOnUnset, f.actions, r.failures = nil, nil, nil
		r.run(func() {})
		if len(r.failures) != 0 || h.armed || len(f.flags) != 0 || !slices.Equal(f.actions, []string{"unset", "show"}) {
			t.Fatal("the cleanup did not clear the flags", r.failures, f.actions, f.flags)
		}
	})
	t.Run("readback-failure-stays-armed", func(t *testing.T) {
		f := &fakeOSDFlags{}
		r := &flagHoldRecorder{}
		var h *recoveryFlagHold
		r.run(func() {
			h = newRecoveryFlagHoldWith(r, func(ctx context.Context, action string) []string {
				flags := f.run(ctx, action)
				if action == "show" && len(f.actions) > 1 {
					// Another client set the flag again after the unset.
					f.flags = append(f.flags, "norecover")
					return slices.Clone(f.flags)
				}
				return flags
			})
			h.hold(ctx)
			h.release(ctx)
		})
		if len(r.failures) == 0 || !strings.Contains(r.failures[0], "set again in the readback") || !h.armed {
			t.Fatal("an unverified readback disarmed the hold", r.failures, f.actions, h.armed)
		}
	})
	t.Run("release-without-hold-does-nothing", func(t *testing.T) {
		r, f := &flagHoldRecorder{}, &fakeOSDFlags{flags: []string{"norecover"}}
		r.run(func() { newRecoveryFlagHoldWith(r, f.run).release(ctx) })
		if len(r.failures) != 0 || len(f.actions) != 0 || !slices.Equal(f.flags, []string{"norecover"}) {
			t.Fatal("a release cleared flags the hold never set", r.failures, f.actions, f.flags)
		}
	})
}
