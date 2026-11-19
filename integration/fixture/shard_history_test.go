package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// fakeShardHistory records every engine and reader step in order and can
// fail one of them or answer as another store would.
type fakeShardHistory struct {
	events    []string
	fail      string
	isolated  bool
	whoami    string
	layout    string
	toolCode  int
	toolErr   string
	noResume  bool
	noReader  bool
	removeErr bool
}

func newFakeShardHistory() *fakeShardHistory {
	return &fakeShardHistory{isolated: true, whoami: "2\n"}
}

func (f *fakeShardHistory) step(name string) error {
	f.events = append(f.events, name)
	if f.fail == name {
		return errors.New("injected " + name + " failure")
	}
	return nil
}

func (f *fakeShardHistory) reader(context.Context) (shardHistoryReader, bool, error) {
	err := f.step("reader")
	if f.noReader {
		return nil, false, err
	}
	return f, f.isolated, err
}

func (f *fakeShardHistory) pause(context.Context, int) (func(context.Context) error, error) {
	err := f.step("pause")
	if f.noResume {
		return nil, err
	}
	return func(context.Context) error { return f.step("resume") }, err
}

func (f *fakeShardHistory) copyStore(context.Context, int, shardHistoryReader) (int64, error) {
	return 4096, f.step("copy")
}

func (f *fakeShardHistory) run(_ context.Context, argv []string, _ int) (int, []byte, []byte, error) {
	switch {
	case argv[0] == "cat":
		return 0, []byte(f.whoami), nil, f.step("whoami " + argv[1])
	case argv[0] == "sh":
		return 0, []byte(f.layout), nil, f.step("layout " + argv[len(argv)-1])
	}
	op := argv[len(argv)-1]
	if err := f.step(op + " " + strings.Join(argv[:len(argv)-1], " ")); err != nil {
		return 0, nil, nil, err
	}
	return f.toolCode, []byte(`{"dump":"` + op + `"}`), []byte(f.toolErr), nil
}

func (f *fakeShardHistory) remove(context.Context) error {
	f.events = append(f.events, "remove")
	if f.removeErr {
		return errors.New("injected remove failure")
	}
	return nil
}

const shardHistoryTool = "ceph-objectstore-tool --no-mon-config --data-path /shard/ceph-2 --type bluestore --pgid 4.2s1 --format json --op"

func TestCaptureShardHistoryCopiesPausedStoreAndReadsOnlyTheCopy(t *testing.T) {
	out := t.TempDir()
	engine := newFakeShardHistory()
	shard := captureShardHistory(context.Background(), engine, out, "4.2", 1, 2)
	want := []string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "layout /shard/ceph-2",
		"info " + shardHistoryTool, "log " + shardHistoryTool, "remove"}
	if !slices.Equal(engine.events, want) {
		t.Fatalf("steps = %q, want %q", engine.events, want)
	}
	zero := 0
	shard.PausedMillis = 0
	if !reflect.DeepEqual(shard, shardHistoryShard{Index: 1, OSD: 2, SPG: "4.2s1", ReaderIsolated: true, Paused: true, Resumed: true, ArchiveBytes: 4096,
		Whoami: "2", InfoCode: &zero, InfoBytes: 15, InfoFile: "osd-shard-history-s1-info.json",
		LogCode: &zero, LogBytes: 14, LogFile: "osd-shard-history-s1-log.json", ReaderRemoved: true}) {
		t.Fatalf("capture record = %+v", shard)
	}
	for name, content := range map[string]string{"osd-shard-history-s1-info.json": `{"dump":"info"}`, "osd-shard-history-s1-log.json": `{"dump":"log"}`} {
		if raw, err := os.ReadFile(filepath.Join(out, name)); err != nil || string(raw) != content {
			t.Fatal("dump file", name, string(raw), err)
		}
	}
	if got := shardHistoryToolArgv(2, "4.2s1", "log"); strings.Join(got, " ") != shardHistoryTool+" log" {
		t.Fatal("offline reader arguments", got)
	}
}

// Every failure leaves the OSD resumed (once it was paused), the reader and
// its copy removed, and no later step executed.
func TestCaptureShardHistoryFailuresResumeAndRemove(t *testing.T) {
	for name, c := range map[string]struct {
		change func(*fakeShardHistory)
		steps  []string
		reason string
	}{
		"reader does not start": {func(f *fakeShardHistory) { f.fail = "reader" }, []string{"reader", "remove"}, "start the isolated reader"},
		"reader has a network":  {func(f *fakeShardHistory) { f.isolated = false }, []string{"reader", "remove"}, "the reader has a network"},
		"pause refused":         {func(f *fakeShardHistory) { f.fail, f.noResume = "pause", true }, []string{"reader", "pause", "remove"}, "pause the OSD"},
		"pause uncertain":       {func(f *fakeShardHistory) { f.fail = "pause" }, []string{"reader", "pause", "resume", "remove"}, "pause the OSD"},
		"copy fails":            {func(f *fakeShardHistory) { f.fail = "copy" }, []string{"reader", "pause", "copy", "resume", "remove"}, "copy the paused OSD's data directory"},
		"resume fails":          {func(f *fakeShardHistory) { f.fail = "resume" }, []string{"reader", "pause", "copy", "resume", "remove"}, "resume the OSD"},
		"another OSD's store": {func(f *fakeShardHistory) { f.whoami = "3\n" },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "remove"}, `its whoami is "3", not osd.2`},
		"whoami with a suffix": {func(f *fakeShardHistory) { f.whoami = "21\n" },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "remove"}, `its whoami is "21", not osd.2`},
		"no whoami": {func(f *fakeShardHistory) { f.fail = "whoami /shard/ceph-2/whoami" },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "remove"}, "read the copy's whoami"},
		"separate DB device": {func(f *fakeShardHistory) { f.layout = "block.db \n" },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "layout /shard/ceph-2", "remove"}, "check the copy's block layout: block.db"},
		"info refused": {func(f *fakeShardHistory) { f.toolCode, f.toolErr = 1, "PG '4.2s1' not found" },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "layout /shard/ceph-2", "info " + shardHistoryTool, "remove"},
			"offline info of 4.2s1: exit 1: PG '4.2s1' not found"},
		"log does not run": {func(f *fakeShardHistory) { f.fail = "log " + shardHistoryTool },
			[]string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "layout /shard/ceph-2", "info " + shardHistoryTool, "log " + shardHistoryTool, "remove"},
			"offline log of 4.2s1"},
	} {
		engine := newFakeShardHistory()
		c.change(engine)
		shard := captureShardHistory(context.Background(), engine, t.TempDir(), "4.2", 1, 2)
		if !slices.Equal(engine.events, c.steps) {
			t.Errorf("%s: steps = %q, want %q", name, engine.events, c.steps)
		}
		if !strings.Contains(shard.Failure, c.reason) || !shard.ReaderRemoved || shard.LogCode != nil {
			t.Errorf("%s: record = %+v", name, shard)
		}
		if paused := slices.Contains(engine.events, "copy"); paused && (shard.Resumed != (engine.fail != "resume")) {
			t.Errorf("%s: resumed = %t", name, shard.Resumed)
		}
	}

	// A reader that cannot be removed is a failure of its own, also after a
	// complete read; a reader that never existed has nothing to remove.
	engine := newFakeShardHistory()
	engine.removeErr = true
	if shard := captureShardHistory(context.Background(), engine, t.TempDir(), "4.2", 1, 2); shard.ReaderRemoved || !strings.Contains(shard.Failure, "remove the reader and its copy") {
		t.Fatalf("unremoved reader accepted: %+v", shard)
	}
	engine = newFakeShardHistory()
	engine.fail, engine.noReader = "reader", true
	if shard := captureShardHistory(context.Background(), engine, t.TempDir(), "4.2", 1, 2); shard.ReaderRemoved || !slices.Equal(engine.events, []string{"reader"}) {
		t.Fatalf("missing reader: %+v %q", shard, engine.events)
	}
}

func TestShardHistoryPlanReadsTheStoredBracket(t *testing.T) {
	owned := []int{0, 1, 2, 3}
	pgid, acting, err := shardHistoryPlan([]byte(`{"epoch":28,"pgid":"4.2","acting":[1,3,2,0],"up":[1,3,2,0],"primary_id":1}`), owned)
	if err != nil || pgid != "4.2" || !slices.Equal(acting, []int{1, 3, 2, 0}) {
		t.Fatal("plan", pgid, acting, err)
	}
	for name, before := range map[string]string{
		"empty":                    ``,
		"truncated":                `{"pgid":"4.2","acting":[1,3`,
		"no PG":                    `{"acting":[1,3,2,0]}`,
		"shard-qualified PG":       `{"pgid":"4.2s0","acting":[1,3,2,0]}`,
		"PG with a path":           `{"pgid":"4.2/../x","acting":[1,3,2,0]}`,
		"PG with an option":        `{"pgid":"4.2 --op","acting":[1,3,2,0]}`,
		"pool zero":                `{"pgid":"0.2","acting":[1,3,2,0]}`,
		"uppercase seed":           `{"pgid":"4.A","acting":[1,3,2,0]}`,
		"no acting vector":         `{"pgid":"4.2"}`,
		"single member":            `{"pgid":"4.2","acting":[1]}`,
		"more members than OSDs":   `{"pgid":"4.2","acting":[1,3,2,0,4]}`,
		"repeated member":          `{"pgid":"4.2","acting":[1,3,1,0]}`,
		"member that is not owned": `{"pgid":"4.2","acting":[1,3,2,7]}`,
		"empty acting position":    `{"pgid":"4.2","acting":[1,3,2,2147483647]}`,
		"oversized":                `{"pgid":"4.2","acting":[1,3,2,0],"pad":"` + strings.Repeat("x", shardHistoryOutputLimit) + `"}`,
	} {
		if pgid, acting, err := shardHistoryPlan([]byte(before), owned); err == nil {
			t.Errorf("%s accepted: %s %v", name, pgid, acting)
		}
	}
}

func TestCaptureShardHistoriesStopsAtTheFirstFailedShard(t *testing.T) {
	before := []byte(`{"pgid":"4.2","acting":[1,2,3,0]}`)
	out := t.TempDir()
	engine := newFakeShardHistory()
	// Index 1 is osd.2, the store the fake answers for; index 0 is osd.1.
	receipt, err := captureShardHistories(context.Background(), engine, out, before, []int{0, 1, 2, 3})
	if err == nil || receipt.Complete || len(receipt.Shards) != 1 || receipt.Shards[0].Failure == "" || !strings.Contains(err.Error(), "4.2s0 on osd.1") {
		t.Fatalf("a wrong first store continued: %+v %v", receipt, err)
	}
	if strings.Count(strings.Join(engine.events, ","), "pause") != 1 {
		t.Fatal("a later OSD was paused after the failure", engine.events)
	}
	if receipt.Schema != 1 || receipt.Source != nativeEncoderCommit || receipt.PGID != "4.2" || len(receipt.BeforeSHA256) != 64 {
		t.Fatalf("receipt provenance: %+v", receipt)
	}
	if _, err := captureShardHistories(context.Background(), newFakeShardHistory(), out, []byte(`{"pgid":"4.2","acting":[9,2]}`), []int{0, 1, 2, 3}); err == nil {
		t.Fatal("an acting member that is not owned was captured")
	}

	engine = newFakeShardHistory()
	engine.whoami = "2"
	receipt, err = captureShardHistories(context.Background(), engine, out, []byte(`{"pgid":"4.2","acting":[2,2]}`), []int{2})
	if err == nil || len(receipt.Shards) != 0 {
		t.Fatal("a repeated acting member was captured", receipt, err)
	}
}

// shardHistoryRows are the four-OSD catalog rows that declare the capture,
// each with its declared capabilities.
func shardHistoryRows(t *testing.T) map[string]string {
	t.Helper()
	rows := map[string]string{}
	for _, row := range ciRepositoryCatalog(t) {
		if strings.Contains(","+row.Params["capabilities"]+",", ",osd-shard-history,") {
			rows[row.Params["test_run"]] = row.Params["capabilities"]
		}
	}
	if len(rows) == 0 {
		t.Fatal("no catalog row declares osd-shard-history")
	}
	return rows
}

func TestShardHistoryCaptureAdmission(t *testing.T) {
	base := profile{osd: "1", osdStore: "bluestore", osdReplicas: "4", osdProfile: "replicated-pg-v8"}
	rows := shardHistoryRows(t)
	intervals := 0
	for selector, capabilities := range rows {
		p := base
		p.testRun, p.capabilities = selector, capabilities
		if !p.shardHistorySelected() || p.testEnvironment("/out", "")[shardHistoryMarker] != "1" || !p.admits(capOSDErasureWidth) {
			t.Fatal("the selected four-BlueStore profile does not admit the capture for", selector)
		}
		// The oracle mode that observes one PG and can end its interval is
		// admitted where it is declared alone, on the capture's topology.
		want := strings.Contains(capabilities, "osd-pg-interval")
		if want {
			intervals++
		}
		if p.pgIntervalSelected() != want || nativePoolModeAllowed(p, oracleModes, "pginterval") != want {
			t.Error("interval mode admission differs for", selector)
		}
		if !nativePoolModeAllowed(p, oracleModes, "shardhistory") || !nativePoolModeAllowed(p, oracleModes, "place") {
			t.Error("other oracle modes refused for", selector)
		}
		for name, change := range map[string]func(*profile){
			"no selector":        func(p *profile) { p.testRun = "" },
			"broad selector":     func(p *profile) { p.testRun = "^TestCephOSD.*Integration$" },
			"two selectors":      func(p *profile) { p.testRun = selector + "|^TestCephOSDErasureOptimizedK2M2Integration$" },
			"a common prefix":    func(p *profile) { p.testRun = "^TestCephOSDErasureOptimizedK2M2PartialWrite" },
			"undeclared capture": func(p *profile) { p.capabilities = "osd-erasure-width" },
			"no declaration":     func(p *profile) { p.capabilities = "" },
			"three OSDs":         func(p *profile) { p.osdReplicas = "3" },
			"one OSD":            func(p *profile) { p.osdReplicas = "1" },
			"MemStore":           func(p *profile) { p.osdStore = "memstore" },
			"no OSD acceptance":  func(p *profile) { p.osd = "0" },
		} {
			changed := p
			change(&changed)
			if changed.shardHistorySelected() || changed.testEnvironment("/out", "")[shardHistoryMarker] != "" ||
				changed.pgIntervalSelected() || nativePoolModeAllowed(changed, oracleModes, "pginterval") {
				t.Error("capture or interval mode admitted for", name, selector)
			}
		}
		// The interval mode needs the capture it extends.
		alone := p
		alone.capabilities = "osd-erasure-width,osd-pg-interval"
		if alone.pgIntervalSelected() || nativePoolModeAllowed(alone, oracleModes, "pginterval") {
			t.Error("interval mode admitted without the capture for", selector)
		}
	}
	if intervals == 0 || intervals == len(rows) {
		t.Fatal("the catalog no longer separates capture-only rows from interval rows", intervals, len(rows))
	}
	// The control refuses before it reads anything when it is not selected.
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "capture-osd-shard-history"), []byte("7 \n"), 0644); err != nil {
		t.Fatal(err)
	}
	c := &fixtureCluster{profile: profile{osd: "1", osdStore: "bluestore", osdReplicas: "4", testRun: "^TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration$", capabilities: "osd-erasure-width"}}
	if err := c.controlOSDShardHistoryRequest(context.Background(), out); err == nil || !strings.Contains(err.Error(), "exclusive four-BlueStore selector") {
		t.Fatal("unselected capture request:", err)
	}
	if err := os.WriteFile(filepath.Join(out, "capture-osd-shard-history"), []byte("7 4.2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for selector, capabilities := range shardHistoryRows(t) {
		c.profile = base
		c.profile.testRun, c.profile.capabilities = selector, capabilities
		break
	}
	if err := c.controlOSDShardHistoryRequest(context.Background(), out); err == nil || !strings.Contains(err.Error(), "takes no argument") {
		t.Fatal("capture request with an argument:", err)
	}
	if err := c.controlOSDShardHistoryRequest(context.Background(), out); err != nil {
		t.Fatal("no request must be a no-op:", err)
	}
}

func TestRemoveShardHistoryOutputsLeavesOtherReceipts(t *testing.T) {
	out := t.TempDir()
	stale := []string{"osd-shard-history-s0-info.json", "osd-shard-history-s3-log.json", shardHistoryCaptureFile}
	kept := []string{shardHistoryBeforeFile, "osd-pool-result.json"}
	for _, name := range append(slices.Clone(stale), kept...) {
		if err := os.WriteFile(filepath.Join(out, name), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeShardHistoryOutputs(out); err != nil {
		t.Fatal(err)
	}
	for _, name := range stale {
		if _, err := os.Stat(filepath.Join(out, name)); !os.IsNotExist(err) {
			t.Error("stale output kept:", name)
		}
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Error("unrelated file removed:", name)
		}
	}
	if err := removeShardHistoryOutputs(out); err != nil {
		t.Fatal("second removal:", err)
	}
}

func TestShardHistoryCommandRequiresItsDiagnostics(t *testing.T) {
	type step struct {
		code int
		out  []byte
		err  error
	}
	run := func(steps ...step) (int, []byte, []byte, error, [][]string) {
		var calls [][]string
		code, stdout, stderr, err := runShardHistoryCommand(func(argv []string, limit int) (int, []byte, error) {
			calls = append(calls, argv)
			if len(calls) == 2 && limit != shardHistoryStderrLimit {
				t.Errorf("standard error is collected with limit %d", limit)
			}
			s := steps[len(calls)-1]
			return s.code, s.out, s.err
		}, []string{"tool", "--op", "log"}, 64)
		return code, stdout, stderr, err, calls
	}
	code, stdout, stderr, err, calls := run(step{0, []byte("{}"), nil}, step{0, []byte("warning"), nil})
	if err != nil || code != 0 || string(stdout) != "{}" || string(stderr) != "warning" || len(calls) != 2 {
		t.Fatal("a read command with diagnostics:", code, stdout, stderr, err)
	}
	if got := strings.Join(calls[0], " "); got != `sh -c "$@" 2>/tmp/shard-history-stderr sh tool --op log` {
		t.Fatal("wrapped command:", got)
	}
	if got := strings.Join(calls[1], " "); got != "head -c 4096 /tmp/shard-history-stderr" {
		t.Fatal("collection command:", got)
	}
	// The tool's own exit code is the caller's to judge, with its diagnostics.
	if code, _, stderr, err, _ := run(step{1, nil, nil}, step{0, []byte("PG not found"), nil}); err != nil || code != 1 || string(stderr) != "PG not found" {
		t.Fatal("a refused tool run:", code, stderr, err)
	}
	// A collection that fails without a transport error returned no usable
	// diagnostics: a successful tool run must not be reported with an empty
	// standard error.
	for name, collection := range map[string]step{
		"killed before any byte": {137, nil, nil},
		"killed after some":      {137, []byte("warn"), nil},
		"file missing":           {1, nil, nil},
		"negative code":          {-1, nil, nil},
		"transport error":        {0, nil, errors.New("exec stream closed")},
	} {
		code, stdout, stderr, err, calls := run(step{0, []byte("{}"), nil}, collection)
		if err == nil || stdout != nil || stderr != nil || len(calls) != 2 {
			t.Errorf("%s: code %d stdout %q stderr %q err %v", name, code, stdout, stderr, err)
		}
	}
	// A failed command is not followed by a collection.
	if _, stdout, stderr, err, calls := run(step{0, nil, errors.New("exec failed")}); err == nil || stdout != nil || stderr != nil || len(calls) != 1 {
		t.Fatal("a command that did not run:", err, len(calls))
	}
}
