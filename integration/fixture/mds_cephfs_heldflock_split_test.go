package main

import (
	"context"
	"encoding/json"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestMDSCephFSHeldFlockSplitSelectorIsExclusive(t *testing.T) {
	env := mdsCephFSCompletionProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = mdsCephFSHeldFlockSplitSelector
	p, err := loadProfile(func(n string) string { return env[n] })
	if err != nil || p.testRun != mdsCephFSHeldFlockSplitSelector {
		t.Fatal("exclusive held-lock split selector refused", p.testRun, err)
	}
	for _, selector := range []string{"^TestCephMDSCephFS(MultiReconnect|HeldFlockSplitReconnect)Integration$", "TestCephMDSCephFSHeldFlockSplitReconnectIntegration",
		"^TestCephMDSCephFSHeldFlockSplitReconnectIntegrationExtended$"} {
		env["CEPH_MSGR_TEST_RUN"] = selector
		if _, err := loadProfile(func(n string) string { return env[n] }); err == nil {
			t.Fatal("broad or foreign held-lock split selector admitted", selector)
		}
	}
	// The probe, the level-15 window and the per-capability log read belong to
	// the two held-lock split selectors only; every other completion selector
	// refuses them.
	for _, action := range mdsCephFSHeldFlockSplitActions {
		for _, selector := range []string{mdsCephFSHeldFlockSplitSelector, mdsCephFSHeldFlockSplitFragmentsSelector} {
			if err := admitMDSCephFSHeldFlockSplit(action, selector); err != nil {
				t.Fatal("held-lock split action refused under its selector", action, selector, err)
			}
		}
		for _, selector := range []string{"", "^TestCephMDSCephFSCompletionIntegration$", "^TestCephMDSCephFSMultiReconnectIntegration$"} {
			if err := admitMDSCephFSHeldFlockSplit(action, selector); err == nil {
				t.Fatal("held-lock split action admitted under another selector", action, selector)
			}
		}
	}
	for _, action := range []string{"list", "debug-on", "reconnect-log", "restart-arm"} {
		if err := admitMDSCephFSHeldFlockSplit(action, "^TestCephMDSCephFSMultiReconnectIntegration$"); err != nil {
			t.Fatal("an existing completion action was bound to the held-lock selector", action, err)
		}
	}
}

// The two-fragment acceptance has its own exact selector, and the lock probe
// of the last split file is admitted under that selector alone.
func TestMDSCephFSHeldFlockSplitFragmentsSelectorIsExclusive(t *testing.T) {
	env := mdsCephFSCompletionProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = mdsCephFSHeldFlockSplitFragmentsSelector
	p, err := loadProfile(func(n string) string { return env[n] })
	if err != nil || p.testRun != mdsCephFSHeldFlockSplitFragmentsSelector || mdsCephFSHeldFlockSplitFragmentsSelector == mdsCephFSHeldFlockSplitSelector {
		t.Fatal("exclusive two-fragment held-lock selector refused", p.testRun, err)
	}
	for _, selector := range []string{"^TestCephMDSCephFSHeldFlockSplit(Fragments)?ReconnectIntegration$", "TestCephMDSCephFSHeldFlockSplitFragmentsIntegration",
		"^TestCephMDSCephFSHeldFlockSplitFragmentsIntegrationExtended$", "^TestCephMDSCephFSHeldFlockSplitFragments"} {
		env["CEPH_MSGR_TEST_RUN"] = selector
		if _, err := loadProfile(func(n string) string { return env[n] }); err == nil {
			t.Fatal("broad or foreign two-fragment held-lock selector admitted", selector)
		}
	}
	if !slices.Equal(mdsCephFSHeldFlockSplitFragmentsActions, []string{"split-final-flock-ex-nb"}) || slices.Contains(mdsCephFSHeldFlockSplitActions, "split-final-flock-ex-nb") {
		t.Fatal("the final-fragment probe is not the one action of its selector", mdsCephFSHeldFlockSplitFragmentsActions)
	}
	if err := admitMDSCephFSHeldFlockSplit("split-final-flock-ex-nb", mdsCephFSHeldFlockSplitFragmentsSelector); err != nil {
		t.Fatal("final-fragment lock probe refused under its selector", err)
	}
	// The merged single-lock selector does not gain the second probe.
	for _, selector := range []string{"", "^TestCephMDSCephFSCompletionIntegration$", "^TestCephMDSCephFSMultiReconnectIntegration$", mdsCephFSHeldFlockSplitSelector} {
		if err := admitMDSCephFSHeldFlockSplit("split-final-flock-ex-nb", selector); err == nil {
			t.Fatal("final-fragment lock probe admitted under another selector", selector)
		}
	}
}

func TestMDSCephFSHeldFlockSplitFinalProbeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	first, last := plan.splitLockFile(), plan.splitFinalLockFile()
	if last != plan.SplitFiles[31] || last != "split-031-"+strings.Repeat("s", 215) || first == last || len(plan.SplitFiles) != 32 ||
		(mdsCephFSCompletionPlan{}).splitFinalLockFile() != "" || (mdsCephFSCompletionPlan{SplitFiles: []string{"only"}}).splitFinalLockFile() != "" {
		t.Fatal("the predetermined final lock file is not the last of the 32 split files", last)
	}
	if targets := plan.splitLockTargets(); len(targets) != 2 || targets["split-flock-ex-nb"] != first || targets["split-final-flock-ex-nb"] != last {
		t.Fatal("the closed lock probes do not map to their two files", targets)
	}
	id, action, err := parseMDSCephFSCompletionRequest([]string{"4", token, "split-final-flock-ex-nb"}, token, plan, 3)
	if err != nil || id != 4 || action != (mdsCephFSCompletionAction{name: "split-final-flock-ex-nb", target: last}) {
		t.Fatal("closed final-fragment lock probe refused", action, err)
	}
	for _, args := range [][]string{{last}, {first}, {plan.SplitFiles[30]}, {"dir"}, {"31"}, {"owner", "1"}} {
		if _, _, err := parseMDSCephFSCompletionRequest(append([]string{"4", token, "split-final-flock-ex-nb"}, args...), token, plan, 3); err == nil {
			t.Fatal("caller-selected lock target or argument accepted", args)
		}
	}
	for _, name := range []string{"split-final-flock-sh-nb", "split-final-flock-ex", "split-last-flock-ex-nb", "split-flock-ex-nb-31", "split-001-flock-ex-nb"} {
		if _, _, err := parseMDSCephFSCompletionRequest([]string{"4", token, name}, token, plan, 3); err == nil {
			t.Fatal("unimplemented native lock action admitted", name)
		}
	}
	// A plan without a distinct last file has no final probe.
	short := plan
	short.SplitFiles = plan.SplitFiles[:1]
	if _, _, err := parseMDSCephFSCompletionRequest([]string{"4", token, "split-final-flock-ex-nb"}, token, short, 3); err == nil {
		t.Fatal("final-fragment probe admitted on a plan with one split file")
	}
	// The native step refuses a target that is not the action's own file,
	// before it journals or runs anything.
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: t.TempDir(), token: token, name: "owned"}}
	for _, wrong := range []mdsCephFSCompletionAction{{name: "split-final-flock-ex-nb", target: first}, {name: "split-flock-ex-nb", target: last},
		{name: "split-final-flock-ex-nb", target: plan.SplitFiles[30]}, {name: "split-final-flock-ex-nb"}} {
		c.mdsCephFSCompletion = &mdsCephFSCompletionScope{out: c.mdsFresh.out, plan: plan, owned: map[string]uint64{}, current: mdsFreshDaemon{Name: "owned-0", GID: 40}}
		var result mdsCephFSCompletionResult
		if err := c.mdsCephFSCompletionNative(context.Background(), 5, wrong, &result); err == nil || !strings.Contains(err.Error(), "not the action's file") ||
			len(c.mdsCephFSCompletion.mutations) != 0 || result.Mutation != nil {
			t.Fatal("a lock probe ran against a file other than its own", wrong, err)
		}
	}
}

func TestMDSCephFSHeldFlockSplitActionsAreClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	lock := plan.splitLockFile()
	if lock != plan.SplitFiles[0] || lock != "split-000-"+strings.Repeat("s", 215) || (mdsCephFSCompletionPlan{}).splitLockFile() != "" {
		t.Fatal("the predetermined lock file is not the first split file", lock)
	}
	id, action, err := parseMDSCephFSCompletionRequest([]string{"4", token, "split-flock-ex-nb"}, token, plan, 3)
	if err != nil || id != 4 || action != (mdsCephFSCompletionAction{name: "split-flock-ex-nb", target: lock}) {
		t.Fatal("closed split lock probe refused", action, err)
	}
	for _, args := range [][]string{{lock}, {plan.SplitFiles[1]}, {"dir"}, {"lease"}, {"owner", "1"}} {
		if _, _, err := parseMDSCephFSCompletionRequest(append([]string{"4", token, "split-flock-ex-nb"}, args...), token, plan, 3); err == nil {
			t.Fatal("caller-selected lock target or argument accepted", args)
		}
	}
	for _, name := range []string{"split-flock-sh-nb", "split-flock-ex", "flock-ex-nb", "split-flock-unlock"} {
		if _, _, err := parseMDSCephFSCompletionRequest([]string{"4", token, name}, token, plan, 3); err == nil {
			t.Fatal("unimplemented native lock action admitted", name)
		}
	}
	for _, name := range []string{"debug-caps-on", "debug-caps-off"} {
		if _, got, err := parseMDSCephFSCompletionRequest([]string{"4", token, name}, token, plan, 3); err != nil || got != (mdsCephFSCompletionAction{name: name}) {
			t.Fatal("closed debug window action refused", name, err)
		}
		if _, _, err := parseMDSCephFSCompletionRequest([]string{"4", token, name, "20/20"}, token, plan, 3); err == nil {
			t.Fatal("caller-selected debug level accepted", name)
		}
	}
	if _, got, err := parseMDSCephFSCompletionRequest([]string{"4", token, "reconnect-cap-log", "4210", "77"}, token, plan, 3); err != nil ||
		got != (mdsCephFSCompletionAction{name: "reconnect-cap-log", gid: 4210, nonce: 77}) {
		t.Fatal("closed per-capability log read refused", got, err)
	}
	for _, args := range [][]string{{}, {"4210"}, {"0", "77"}, {"4210", "077"}} {
		if _, _, err := parseMDSCephFSCompletionRequest(append([]string{"4", token, "reconnect-cap-log"}, args...), token, plan, 3); err == nil {
			t.Fatal("unscoped per-capability log read admitted", args)
		}
	}
	// Both debug kinds change the same options: one at a time per incarnation.
	if mdsCephFSCompletionRestoreOf("debug-caps-off") != "debug-caps-on" || mdsCephFSCompletionRuntimeKinds["debug-caps-on"].settings["debug_mds"] != "15/15" {
		t.Fatal("level-15 debug window mapping differs")
	}
	out := t.TempDir()
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}}
	for _, pair := range [][2]string{{"debug-on", "debug-caps-on"}, {"debug-caps-on", "debug-on"}} {
		c.mdsCephFSCompletion = &mdsCephFSCompletionScope{out: out, plan: plan, owned: map[string]uint64{}, current: mdsFreshDaemon{Name: "owned-0", GID: 40},
			runtime: []*mdsCephFSCompletionRuntime{{Kind: pair[0], IncarnationGID: 40, State: "applied"}}}
		var result mdsCephFSCompletionResult
		if err := c.mdsCephFSCompletionDaemonAct(context.Background(), 5, mdsCephFSCompletionAction{name: pair[1]}, 40, &result); err == nil ||
			!strings.Contains(err.Error(), "debug levels are already changed") {
			t.Fatal("a second debug window was opened over the first", pair, err)
		}
	}
}

func TestMDSCephFSHeldFlockSplitReceiptRejectsUnreleasedOrChangedState(t *testing.T) {
	plan := mdsCephFSCompletionPlanFor(strings.Repeat("a1", 16))
	for _, file := range []string{plan.splitLockFile(), plan.splitFinalLockFile()} {
		checkMDSCephFSHeldFlockSplitReceipts(t, plan, file)
	}
	// Only the two predetermined files are lock files, and a receipt is
	// bound to the path the driver was started with.
	first, _ := plan.path(plan.splitLockFile())
	last, _ := plan.path(plan.splitFinalLockFile())
	state := mdsCephFSFlockState{Ino: 7, MTime: 10}
	receipt := func(path string) []byte {
		data, err := json.Marshal(mdsCephFSNative{Action: "flock-ex-nb", Path: path, NativeGlobalID: 9,
			Flock: &mdsCephFSFlockProbe{Owner: mdsCephFSFlockOwner, Errno: 11, ElapsedNS: 1, Before: state, After: state}})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, name := range plan.SplitFiles[1:31] {
		path, _ := plan.path(name)
		if _, err := decodeMDSCephFSCompletionNative("flock-ex-nb", path, plan, receipt(path)); err == nil {
			t.Fatal("a lock receipt of a split file that is no lock file was accepted", name)
		}
	}
	if _, err := decodeMDSCephFSCompletionNative("flock-ex-nb", last, plan, receipt(first)); err == nil {
		t.Fatal("a receipt of the first lock file was accepted for the probe of the last one")
	}
	if _, err := decodeMDSCephFSCompletionNative("flock-ex-nb", first, plan, receipt(last)); err == nil {
		t.Fatal("a receipt of the last lock file was accepted for the probe of the first one")
	}
	single := plan
	single.SplitFiles = plan.SplitFiles[:1]
	if _, err := decodeMDSCephFSCompletionNative("flock-ex-nb", last, single, receipt(last)); err == nil {
		t.Fatal("a final-fragment lock receipt was accepted on a plan without that file")
	}
}

func checkMDSCephFSHeldFlockSplitReceipts(t *testing.T, plan mdsCephFSCompletionPlan, file string) {
	t.Helper()
	path, _ := plan.path(file)
	other, _ := plan.path(plan.SplitFiles[1])
	// A never-written file may report native version 0; it is compared, not required.
	for _, state := range []mdsCephFSFlockState{{Ino: 7, MTime: 10}, {Ino: 7, Version: 3, MTime: 10}} {
		for _, blocked := range []bool{false, true} {
			fresh := func() mdsCephFSNative {
				p := &mdsCephFSFlockProbe{Owner: mdsCephFSFlockOwner, Acquired: !blocked, Unlocked: !blocked, ElapsedNS: 1, Before: state, After: state}
				if blocked {
					p.Errno = 11
				}
				return mdsCephFSNative{Action: "flock-ex-nb", Path: path, NativeGlobalID: 9, Flock: p}
			}
			check := func(n mdsCephFSNative) error {
				data, err := json.Marshal(n)
				if err != nil {
					t.Fatal(err)
				}
				_, err = decodeMDSCephFSCompletionNative("flock-ex-nb", n.Path, plan, data)
				return err
			}
			if err := check(fresh()); err != nil {
				t.Fatal("valid nonblocking split receipt refused", blocked, err)
			}
			for _, mutate := range []func(*mdsCephFSNative){
				func(n *mdsCephFSNative) { n.Flock.Owner++ },
				func(n *mdsCephFSNative) { n.Flock.Unlocked = !n.Flock.Unlocked },
				func(n *mdsCephFSNative) { n.Flock.Acquired = !n.Flock.Acquired },
				func(n *mdsCephFSNative) { n.Flock.Errno = 13 },
				func(n *mdsCephFSNative) { n.Flock.ElapsedNS = 0 },
				func(n *mdsCephFSNative) { n.Flock.After.Version++ },
				func(n *mdsCephFSNative) { n.Flock.After.MTime++ },
				func(n *mdsCephFSNative) { n.Flock.After.Ino++ },
				func(n *mdsCephFSNative) { n.Flock.Before.Ino, n.Flock.After.Ino = 0, 0 },
				func(n *mdsCephFSNative) { n.Flock.Before.Size, n.Flock.After.Size = 1, 1 },
				func(n *mdsCephFSNative) { n.Flock.Before.DataHex, n.Flock.After.DataHex = "00", "00" },
				func(n *mdsCephFSNative) { n.Stat = &mdsCephFSStat{Exists: true, Ino: 7, Mode: 0o100644} },
				func(n *mdsCephFSNative) { n.Written = 1 },
				func(n *mdsCephFSNative) { n.Path = other },
				func(n *mdsCephFSNative) { n.Flock = nil },
			} {
				n := fresh()
				mutate(&n)
				if err := check(n); err == nil {
					t.Fatal("ambiguous, foreign or changed split lock receipt accepted", n)
				}
			}
		}
	}
	// The receipt does not broaden the other completion actions.
	stat, _ := json.Marshal(mdsCephFSNative{Action: "stat", Path: path, NativeGlobalID: 9, Stat: &mdsCephFSStat{Exists: true, Ino: 7, Mode: 0o100644}, Flock: &mdsCephFSFlockProbe{}})
	if _, err := decodeMDSCephFSCompletionNative("stat", path, plan, stat); err == nil {
		t.Fatal("a lock receipt was accepted on another completion action")
	}
}

func TestMDSCephFSReconnectCapLogAttributesLinesToOneClientFragment(t *testing.T) {
	lines := []string{
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000009 [2,head] /d/early auth]",
		"t  7 mds.0.server handle_client_reconnect client.42 (more)",
		"t 10 mds.0.server  reconnect_start 1s delay 0.1",
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000001 [2,head] /d/a auth]",
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000002 [2,head] /d/b auth]",
		"t  7 mds.0.server handle_client_reconnect client.420 (more)",
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000007 [2,head] /d/foreign auth]",
		"t  7 mds.0.server handle_client_reconnect client.42 foreign",
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000008 [2,head] /d/unattributed auth]",
		"t  7 mds.0.server handle_client_reconnect client.42",
		"t 15 mds.0.server open snaprealm (w inode) on [inode 0x1 [...2,head] / auth]",
		"t 15 mds.0.server open cap realm 0x1 on [inode 0x10000000003 [2,head] /d/c auth]",
		"t 10 mds.0.cache rejoin_recovered_client client.42",
	}
	kept, dropped := filterMDSCephFSReconnectCaps(lines, 42)
	if want := []string{lines[1], lines[3], lines[4], lines[9], lines[11]}; !slices.Equal(kept, want) || dropped != 0 {
		t.Fatal("per-capability reconnect lines changed client, fragment or order", kept, dropped)
	}
	many := []string{"x handle_client_reconnect client.1"}
	for range mdsCephFSCompletionLogLines + 2 {
		many = append(many, "x open cap realm 0x1 on [inode 0x2 "+strings.Repeat("y", mdsCephFSCompletionLogLineBytes))
	}
	kept, dropped = filterMDSCephFSReconnectCaps(many, 1)
	if len(kept) != mdsCephFSCompletionLogLines || dropped != 3 || len(kept[1]) != mdsCephFSCompletionLogLineBytes {
		t.Fatal("per-capability reconnect log bounds differ", len(kept), dropped)
	}
}

// The driver runs inside the Ceph image. Here only its syntax and the fixed
// lock path are checked with the local interpreter.
func TestMDSCephFSCompletionNativeDriverCompilesAndNamesTheLockFile(t *testing.T) {
	plan := mdsCephFSCompletionPlanFor(strings.Repeat("a1", 16))
	if !strings.Contains(mdsCephFSCompletionNativePython, "path in ('/' + directory + '/split-000-' + 's' * 215, '/' + directory + '/split-031-' + 's' * 215) and not extra") ||
		strings.Count(mdsCephFSCompletionNativePython, "fs.flock(") != 2 || strings.Contains(mdsCephFSCompletionNativePython, "LOCK_SH") ||
		strings.Count(mdsCephFSCompletionNativePython, "/split-") != 2 ||
		plan.splitLockFile() != "split-000-"+strings.Repeat("s", 215) || plan.splitFinalLockFile() != "split-031-"+strings.Repeat("s", 215) {
		t.Fatal("the native driver no longer fixes one exclusive nonblocking lock on the two planned files", plan.splitLockFile(), plan.splitFinalLockFile())
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	command := exec.Command(python, "-c", "import sys; compile(sys.stdin.read(), 'driver', 'exec')")
	command.Stdin = strings.NewReader(mdsCephFSCompletionNativePython)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatal("native completion driver does not compile", err, string(output))
	}
}
