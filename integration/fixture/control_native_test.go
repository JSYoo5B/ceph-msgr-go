package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// oracleModes are the modes the owned-pool oracle of this checkout declares,
// read by the same rule the fixture applies to the staged sources.
var oracleModes = func() map[string]bool {
	modes, err := ownedPoolOracleModes(filepath.Join("..", "..", "tools"))
	if err != nil {
		panic(err)
	}
	return modes
}()

func sortedModes(modes map[string]bool) []string {
	names := make([]string, 0, len(modes))
	for mode := range modes {
		names = append(names, mode)
	}
	sort.Strings(names)
	return names
}

// A mode is declared beside its handler only. The fixture admits every
// declared mode, including one in its own module, on an OSD fixture.
func TestOwnedPoolOracleModesAreDeclaredBesideTheirHandlers(t *testing.T) {
	for _, mode := range append(slices.Clone(legacyOwnedPoolModes), "objecterrequests", "pgstate", "watchers") {
		if !oracleModes[mode] {
			t.Fatal("an owned-pool oracle mode is no longer declared", mode)
		}
	}
	modules, err := filepath.Glob(filepath.Join("..", "..", "tools", "osd_pool_mode_*_oracle.py"))
	if err != nil {
		t.Fatal(err)
	}
	osd := profile{osd: "1"}
	for _, path := range modules {
		mode := ownedPoolModeModule.FindStringSubmatch(filepath.Base(path))[1]
		if !nativePoolModeAllowed(osd, oracleModes, mode) {
			t.Fatal("the mode of its own module is not admitted on an OSD fixture", mode)
		}
	}
	// The merge proof (tools/test_merge_proof.py) reads this line to see the
	// modes of a merged tree.
	t.Logf("declared owned-pool oracle modes: %s", strings.Join(sortedModes(oracleModes), " "))
}

func writeOracleSources(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestOwnedPoolOracleModesFollowTheDeclarationRule(t *testing.T) {
	const main = "HANDLERS = {}\n\n\n@oracle_mode\ndef mode_create(rados, cluster, request, out):\n    return {}\n\n\n" +
		"# cleanup removes what create made.\n@oracle_mode\ndef mode_cleanup(rados, cluster, request, out):\n    return {}\n"
	module := func(mode string) string {
		return "import osd_pool_oracle as pool\n\n\n@oracle_mode\ndef mode_" + mode + "(rados, cluster, request, out):\n    return {}\n"
	}
	modes, err := ownedPoolOracleModes(writeOracleSources(t, map[string]string{"osd_pool_oracle.py": main, "osd_pool_mode_extra1_oracle.py": module("extra1")}))
	if err != nil || strings.Join(sortedModes(modes), " ") != "cleanup create extra1" {
		t.Fatal(modes, err)
	}
	for name, files := range map[string]map[string]string{
		"missing oracle":     {"osd_pool_mode_extra1_oracle.py": module("extra1")},
		"no mode":            {"osd_pool_oracle.py": "HANDLERS = {}\n"},
		"other decorator":    {"osd_pool_oracle.py": main + "\n\n@other\ndef mode_hidden(rados, cluster, request, out):\n    return {}\n"},
		"detached decorator": {"osd_pool_oracle.py": main + "\n\n@oracle_mode\n# comment\ndef mode_hidden(rados, cluster, request, out):\n    return {}\n"},
		"indented":           {"osd_pool_oracle.py": main + "\n\nif True:\n    @oracle_mode\n    def mode_hidden(rados, cluster, request, out):\n        return {}\n"},
		"twice":              {"osd_pool_oracle.py": main + "\n\n@oracle_mode\ndef mode_create(rados, cluster, request, out):\n    return {}\n"},
		"module other mode":  {"osd_pool_oracle.py": main, "osd_pool_mode_extra1_oracle.py": module("extra2")},
		"module two modes":   {"osd_pool_oracle.py": main, "osd_pool_mode_extra1_oracle.py": module("extra1") + "\n\n" + module("extra2")[len("import osd_pool_oracle as pool\n"):]},
		"module no mode":     {"osd_pool_oracle.py": main, "osd_pool_mode_extra1_oracle.py": "import osd_pool_oracle as pool\n"},
		"module repeats":     {"osd_pool_oracle.py": main, "osd_pool_mode_create_oracle.py": module("create")},
		"module bad name":    {"osd_pool_oracle.py": main, "osd_pool_mode_Extra_oracle.py": module("Extra")},
	} {
		if modes, err := ownedPoolOracleModes(writeOracleSources(t, files)); err == nil {
			t.Fatal(name, "accepted", modes)
		}
	}
}

// The control refuses an undeclared mode before it runs anything, and it
// reads the modes from the oracle it would run.
func TestControlOwnedPoolRequestAdmitsOnlyDeclaredModes(t *testing.T) {
	main := "@oracle_mode\ndef mode_create(rados, cluster, request, out):\n    return {}\n"
	for _, tc := range []struct {
		name, source, mode, want string
	}{
		{"undeclared", main, "bogus", "invalid native owned-pool oracle request"},
		{"declared elsewhere only", main, "cleanup", "invalid native owned-pool oracle request"},
		{"no oracle", "", "create", "owned-pool oracle source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := t.TempDir()
			if tc.source != "" {
				if err := os.WriteFile(filepath.Join(out, "osd_pool_oracle.py"), []byte(tc.source), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			writeControlRequest(t, out, "verify-osd-pool", "1 "+tc.mode)
			c := &fixtureCluster{profile: profile{osd: "1"}}
			err := c.controlOwnedPoolRequest(context.Background(), out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatal(err)
			}
			if fileExists(filepath.Join(out, "osd-pool-command.json")) || fileExists(filepath.Join(out, "osd-pool-verify.1")) {
				t.Fatal("a refused mode ran or was acknowledged")
			}
		})
	}
}

// An owned-pool oracle request that is still running when its test's pause
// lease ends must not keep the pause: the control loop serves the pause
// recovery and the resume while the request runs and leaves every other
// request pending, and the request published after the outstanding one
// reads only its own input, also when it was written while the outstanding
// request was running. The fake oracle blocks until the test releases it,
// so nothing here depends on timing.
func TestOwnedPoolOracleRunsBesideThePauseEndingControls(t *testing.T) {
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "osd_pool_oracle.py"), []byte("@oracle_mode\ndef mode_observe(rados, cluster, request, out):\n    return {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	type started struct {
		input   string
		release chan error
	}
	runs := make(chan started)
	c := &fixtureCluster{profile: profile{osd: "1"}, runOwnedPool: func(ctx context.Context, out string, args []string, budget time.Duration) error {
		data, err := os.ReadFile(filepath.Join(out, "osd-pool-input.json"))
		if err != nil {
			return err
		}
		run := started{string(data), make(chan error)}
		runs <- run
		return <-run.release
	}}
	publish := func(id, input string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(out, "osd-pool-input.json."+id), []byte(input), 0o600); err != nil {
			t.Fatal(err)
		}
		writeControlRequest(t, out, "verify-osd-pool", id+" observe\ninput osd-pool-input.json\n")
	}
	poll := func() {
		t.Helper()
		if err := c.controls(context.Background(), out); err != nil {
			t.Fatal(err)
		}
	}
	// The pass starts the request in its own goroutine and returns.
	publish("1", `{"request":1}`)
	pass := make(chan struct{})
	go func() { defer close(pass); poll() }()
	first := <-runs
	<-pass
	if first.input != `{"request":1}` {
		t.Fatal("the first request read", first.input)
	}
	// While it runs, the next request is published with its own input.
	publish("2", `{"request":2}`)
	writeControlRequest(t, out, "pause-osd", "3 1")
	poll()
	if !fileExists(filepath.Join(out, "verify-osd-pool")) || !fileExists(filepath.Join(out, "pause-osd")) || fileExists(filepath.Join(out, "osd-pool-verify.1")) {
		t.Fatal("a request other than a pause-ending one was served while the oracle request ran")
	}
	if err := os.Remove(filepath.Join(out, "pause-osd")); err != nil {
		t.Fatal(err)
	}
	// The pause recovery is served while the oracle request is outstanding.
	writeControlRequest(t, out, "recover-paused-osd", "4 1")
	poll()
	if !fileExists(filepath.Join(out, "osd-pause-recovered.4")) || fileExists(filepath.Join(out, "recover-paused-osd")) {
		t.Fatal("the pause recovery waited for the outstanding oracle request")
	}
	if !fileExists(filepath.Join(out, "verify-osd-pool")) || fileExists(filepath.Join(out, "osd-pool-verify.1")) {
		t.Fatal("the next oracle request was claimed before the outstanding one was settled")
	}
	first.release <- nil
	<-c.ownedPool.done
	pass = make(chan struct{})
	go func() { defer close(pass); poll() }()
	second := <-runs
	<-pass
	if !fileExists(filepath.Join(out, "osd-pool-verify.1")) || fileExists(filepath.Join(out, "osd-pool-verify.2")) {
		t.Fatal("the settled request was not acknowledged before the next one ran")
	}
	if second.input != `{"request":2}` {
		t.Fatal("the next request read", second.input)
	}
	second.release <- nil
	<-c.ownedPool.done
	poll()
	if !fileExists(filepath.Join(out, "osd-pool-verify.2")) {
		t.Fatal("the second request was not acknowledged")
	}
	// A failed request stops the loop when it is settled.
	publish("5", `{"request":5}`)
	pass = make(chan struct{})
	go func() { defer close(pass); poll() }()
	third := <-runs
	<-pass
	third.release <- errors.New("oracle failed")
	<-c.ownedPool.done
	if err := c.controls(context.Background(), out); err == nil || err.Error() != "oracle failed" || fileExists(filepath.Join(out, "osd-pool-verify.5")) {
		t.Fatal("a failed oracle request was not reported:", err)
	}
}
