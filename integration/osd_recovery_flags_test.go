package integration_test

import (
	"context"
	"testing"
	"time"
)

// recoveryFlagNames are the OSD map flags a recovery hold sets and clears
// (tools/osd_pool_oracle.py mode_osdflags RECOVERY_FLAGS).
var recoveryFlagNames = []string{"norecover", "nobackfill"}

// recoveryFlagsAre reports whether flags, one native OSD map flag list,
// holds every recovery flag exactly once (set) or none of them (!set). Flags
// are compared as whole tokens.
func recoveryFlagsAre(flags []string, set bool) bool {
	for _, name := range recoveryFlagNames {
		n := 0
		for _, flag := range flags {
			if flag == name {
				n++
			}
		}
		if set && n != 1 || !set && n != 0 {
			return false
		}
	}
	return true
}

// recoveryFlagHold sets norecover and nobackfill for one test and clears
// them again, and only flags it set itself: hold refuses a cluster where
// either is already set, without touching it. The hold is armed before the
// set command is sent (a set whose reply was lost may still apply) and stays
// armed until a release has read both flags back cleared, so a failed or
// unverified release leaves the test's cleanup to clear them.
type recoveryFlagHold struct {
	t testing.TB
	// flags runs one owned-pool osdflags action (set, unset or show) and
	// returns the native OSD map flags it reports afterwards.
	flags func(ctx context.Context, action string) []string
	armed bool
}

// newRecoveryFlagHold prepares a hold on the native flags of the fixture,
// registering its cleanup now, before anything is set. A test that restores
// the cluster itself (for example restarts an OSD and waits for clean PGs)
// calls release first in that restore.
func newRecoveryFlagHold(t *testing.T) *recoveryFlagHold {
	t.Helper()
	return newRecoveryFlagHoldWith(t, func(ctx context.Context, action string) []string {
		return nativePool(t, ctx, "osdflags", map[string]any{"action": action}).OSDMapFlags
	})
}

func newRecoveryFlagHoldWith(t testing.TB, flags func(context.Context, string) []string) *recoveryFlagHold {
	h := &recoveryFlagHold{t: t, flags: flags}
	t.Cleanup(func() {
		if h.armed {
			cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			h.release(cleanup)
		}
	})
	return h
}

// hold reads the flags, refuses a baseline with either recovery flag set,
// sets both and requires both exactly once in the reported flags.
func (h *recoveryFlagHold) hold(ctx context.Context) {
	h.t.Helper()
	if before := h.flags(ctx, "show"); !recoveryFlagsAre(before, false) {
		h.t.Fatalf("norecover or nobackfill is already set (%v); a recovery hold sets and clears only flags it set itself", before)
	}
	h.armed = true
	if after := h.flags(ctx, "set"); !recoveryFlagsAre(after, true) {
		h.t.Fatalf("norecover and nobackfill are not both set after the set (%v)", after)
	}
}

// release clears both flags and disarms the hold only after the flags the
// unset reports and a separate read both show neither flag. A release of a
// hold that is not armed does nothing.
func (h *recoveryFlagHold) release(ctx context.Context) {
	h.t.Helper()
	if !h.armed {
		return
	}
	if after := h.flags(ctx, "unset"); !recoveryFlagsAre(after, false) {
		h.t.Fatalf("norecover or nobackfill is still set after the unset (%v)", after)
	}
	if shown := h.flags(ctx, "show"); !recoveryFlagsAre(shown, false) {
		h.t.Fatalf("norecover or nobackfill is set again in the readback (%v)", shown)
	}
	h.armed = false
}
