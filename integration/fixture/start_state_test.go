package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
)

// Fake native output in the shape Tentacle's `--format json` commands print,
// reduced to the fields the start-state check reads plus a few neighbours.
const (
	fakeStartStatus = `{"fsid":"8c1d6f2e-1111-4a4a-9e9e-000000000001","health":{"status":"HEALTH_OK","checks":{}},
"quorum_names":["a","b","c"],"mgrmap":{"available":true,"num_standbys":1},
"osdmap":{"epoch":21,"num_osds":2,"num_up_osds":2,"num_in_osds":2},
"pgmap":{"pgs_by_state":[{"state_name":"active+clean","count":9}],"num_pools":2,"num_pgs":9}}`
	fakeStartOSDDump = `{"epoch":21,"fsid":"8c1d6f2e-1111-4a4a-9e9e-000000000001",
"flags":"sortbitwise,recovery_deletes,purged_snapdirs,pglog_hardlimit",
"pools":[{"pool":1,"pool_name":".mgr","flags_names":"hashpspool","size":2},
{"pool":2,"pool_name":"rbd","flags_names":"hashpspool,selfmanaged_snaps","size":2}],
"osds":[{"osd":0,"up":1,"in":1,"state":["exists","up"]},{"osd":1,"up":1,"in":1,"state":["exists","up"]}],
"pg_upmap":[],"pg_upmap_items":[],"pg_temp":[],"primary_temp":[]}`
	fakeStartProfiles = `["default"]`
	fakeStartRules    = `["replicated_rule"]`
	fakeStartConfig   = `[{"section":"global","name":"mon_allow_pool_delete","value":"true","level":"advanced","can_update_at_runtime":true,"mask":""},
{"section":"osd","name":"osd_recovery_sleep","value":"0.000000","level":"advanced","can_update_at_runtime":true,"mask":""}]`
)

const fakeStartSession = "f00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeed"

func fakeStartContainer(id, name, state string, labels map[string]string) container.Summary {
	return container.Summary{ID: id, Names: []string{"/" + name}, State: container.ContainerState(state), Labels: labels}
}

func fakeStartContainers() []container.Summary {
	ours := map[string]string{"org.testcontainers": "true", "org.testcontainers.sessionId": fakeStartSession}
	var items []container.Summary
	for _, name := range []string{"mon-a", "mon-b", "mon-c", "mgr-x", "mgr-y", "osd-0", "osd-1", "oracles", "control"} {
		items = append(items, fakeStartContainer("id-"+name, name, "running", ours))
	}
	// An unlabelled container of another tool is not a Testcontainers
	// container and never counts as foreign.
	return append(items, fakeStartContainer("id-unrelated", "unrelated", "running", nil))
}

var fakeStartOwned = map[string]string{
	"mon.a": "id-mon-a", "mon.b": "id-mon-b", "mon.c": "id-mon-c", "mgr.x": "id-mgr-x", "mgr.y": "id-mgr-y",
	"osd.0": "id-osd-0", "osd.1": "id-osd-1", "messenger-oracles": "id-oracles", "control": "id-control",
}

func fakeStartRaw() startStateRaw {
	return startStateRaw{Status: []byte(fakeStartStatus), OSDDump: []byte(fakeStartOSDDump), Profiles: []byte(fakeStartProfiles),
		Rules: []byte(fakeStartRules), Config: []byte(fakeStartConfig), Containers: fakeStartContainers()}
}

func fakeStartSnapshot(t *testing.T) startStateSnapshot {
	t.Helper()
	raw := fakeStartRaw()
	view := decodeStartState(raw)
	if len(view.Unobserved) != 0 {
		t.Fatal(view.Unobserved)
	}
	return newStartStateSnapshot(view, fakeStartOwned, raw.Containers, fakeStartSession)
}

func decodeFakeStart(t *testing.T, raw startStateRaw) (startStateView, error) {
	t.Helper()
	view := decodeStartState(raw)
	if len(view.Unobserved) != 0 {
		return view, errors.New(view.Unobserved[0].Detail)
	}
	return view, nil
}

func replaceOnce(t *testing.T, data []byte, old, replacement string) []byte {
	t.Helper()
	if strings.Count(string(data), old) != 1 {
		t.Fatalf("fake native output does not contain %q exactly once", old)
	}
	return []byte(strings.Replace(string(data), old, replacement, 1))
}

func setContainerState(items []container.Summary, id, state string) []container.Summary {
	items = slices.Clone(items)
	for i := range items {
		if items[i].ID == id {
			items[i].State = container.ContainerState(state)
		}
	}
	return items
}

func startConditions(failures []startStateFailure) []string {
	var names []string
	for _, f := range failures {
		if !slices.Contains(names, f.Condition) {
			names = append(names, f.Condition)
		}
	}
	return names
}

func TestStartStateAcceptsTheRecordedStartState(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	view := decodeStartState(fakeStartRaw())
	if failures := compareStartState(snapshot, view); len(failures) != 0 {
		t.Fatal("rejected the start state", failures)
	}
	if len(snapshot.Daemons) != 7 || len(snapshot.Others) != 2 || snapshot.Others["control"].State != "running" {
		t.Fatal("daemons of the profile are not separated from the other owned containers", snapshot.Daemons, snapshot.Others)
	}
	if slices.Contains(snapshot.Owned, "id-unrelated") {
		t.Fatal("an unlabelled container of another session became owned")
	}
}

// Each case changes the fake native output the way a leftover of that kind
// shows it, and must fail with exactly the named conditions.
func TestStartStateNamesEachFailedCondition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *startStateRaw)
		want   []string
		detail string
	}{
		{"stopped OSD", func(t *testing.T, raw *startStateRaw) {
			raw.Containers = setContainerState(raw.Containers, "id-osd-1", "exited")
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `{"osd":1,"up":1`, `{"osd":1,"up":0`)
		}, []string{startConditionDaemonRunning, startConditionOSDUp}, "osd.1"},
		{"paused MON", func(t *testing.T, raw *startStateRaw) {
			raw.Containers = setContainerState(raw.Containers, "id-mon-b", "paused")
			raw.Status = replaceOnce(t, raw.Status, `["a","b","c"]`, `["a","c"]`)
		}, []string{startConditionDaemonRunning, startConditionMonQuorum}, "mon.b"},
		{"out OSD", func(t *testing.T, raw *startStateRaw) {
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"up":1,"in":1,"state":["exists","up"]}]`, `"up":1,"in":0,"state":["exists","up"]}]`)
		}, []string{startConditionOSDIn}, "osd.1 is out"},
		{"non-clean PG", func(t *testing.T, raw *startStateRaw) {
			raw.Status = replaceOnce(t, raw.Status, `[{"state_name":"active+clean","count":9}]`,
				`[{"state_name":"active+clean","count":8},{"state_name":"active+undersized+degraded","count":1}]`)
		}, []string{startConditionPGClean}, "1xactive+undersized+degraded"},
		{"scrubbing PG", func(t *testing.T, raw *startStateRaw) {
			raw.Status = replaceOnce(t, raw.Status, `"active+clean","count":9`, `"active+clean+scrubbing","count":9`)
		}, []string{startConditionPGClean}, "active+clean+scrubbing"},
		{"missing PG statistics", func(t *testing.T, raw *startStateRaw) {
			raw.Status = replaceOnce(t, raw.Status, `[{"state_name":"active+clean","count":9}],"num_pools":2,"num_pgs":9`, `[],"num_pools":2,"num_pgs":0`)
		}, []string{startConditionPGClean}, "no PG statistics"},
		{"pg_temp", func(t *testing.T, raw *startStateRaw) {
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"pg_temp":[]`, `"pg_temp":[{"pgid":"2.3","osds":[1,0]}]`)
		}, []string{startConditionPGTemp}, "2.3->[1 0]"},
		{"leftover pool", func(t *testing.T, raw *startStateRaw) {
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"size":2}]`, `"size":2},{"pool":7,"pool_name":"owned-ec","flags_names":"hashpspool,ec_overwrites","size":4}]`)
		}, []string{startConditionPool}, "owned-ec"},
		{"leftover pool flag", func(t *testing.T, raw *startStateRaw) {
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"hashpspool,selfmanaged_snaps"`, `"hashpspool,selfmanaged_snaps,nodelete"`)
		}, []string{startConditionPoolFlags}, "pool rbd"},
		{"leftover erasure-code profile and rule", func(t *testing.T, raw *startStateRaw) {
			raw.Profiles = []byte(`["default","owned-k2m2"]`)
			raw.Rules = []byte(`["owned-k2m2","replicated_rule"]`)
		}, []string{startConditionErasureProfile, startConditionCrushRule}, "owned-k2m2"},
		{"leftover flag", func(t *testing.T, raw *startStateRaw) {
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
		}, []string{startConditionOSDFlags}, "set [noout]"},
		{"changed config option", func(t *testing.T, raw *startStateRaw) {
			raw.Config = replaceOnce(t, raw.Config, `"value":"0.000000"`, `"value":"0.100000"`)
		}, []string{startConditionConfig}, `osd/osd_recovery_sleep is "0.100000", was "0.000000"`},
		{"added config option", func(t *testing.T, raw *startStateRaw) {
			raw.Config = replaceOnce(t, raw.Config, `"mask":""}]`, `"mask":""},{"section":"osd.1","name":"debug_ms","value":"20/20","mask":""}]`)
		}, []string{startConditionConfig}, "osd.1/debug_ms set to"},
		{"removed config option", func(t *testing.T, raw *startStateRaw) {
			raw.Config = []byte(`[{"section":"global","name":"mon_allow_pool_delete","value":"true","mask":""}]`)
		}, []string{startConditionConfig}, "osd/osd_recovery_sleep removed"},
		{"stopped single MON", func(t *testing.T, raw *startStateRaw) {
			// The MON reads fail; the Docker listing still names the
			// stopped daemon.
			raw.Status, raw.OSDDump, raw.Profiles, raw.Rules, raw.Config = nil, nil, nil, nil, nil
			raw.NativeErr = errors.New("ceph status: synthetic timeout")
			raw.Containers = setContainerState(raw.Containers, "id-mon-a", "exited")
		}, []string{startConditionObservation, startConditionDaemonRunning}, "mon.a container id-mon-a is exited"},
		{"failed Docker listing", func(t *testing.T, raw *startStateRaw) {
			raw.Containers, raw.DockerErr = nil, errors.New("synthetic daemon socket failure")
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
		}, []string{startConditionObservation, startConditionOSDFlags}, "list Docker containers: synthetic daemon socket failure"},
		{"foreign container", func(t *testing.T, raw *startStateRaw) {
			raw.Containers = append(slices.Clone(raw.Containers), fakeStartContainer("id-left-behind", "netem-helper", "exited",
				map[string]string{"org.testcontainers": "true", "org.testcontainers.sessionId": fakeStartSession}))
		}, []string{startConditionForeignContainer}, "netem-helper"},
		{"foreign session container", func(t *testing.T, raw *startStateRaw) {
			raw.Containers = append(slices.Clone(raw.Containers), fakeStartContainer("id-other-run", "other-run", "running",
				map[string]string{"org.testcontainers": "true", "org.testcontainers.sessionId": "another"}))
		}, []string{startConditionForeignContainer}, "other-run"},
		{"removed oracle container", func(t *testing.T, raw *startStateRaw) {
			raw.Containers = slices.DeleteFunc(slices.Clone(raw.Containers), func(c container.Summary) bool { return c.ID == "id-oracles" })
		}, []string{startConditionContainerState}, "messenger-oracles"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := fakeStartSnapshot(t)
			raw := fakeStartRaw()
			tc.change(t, &raw)
			view := decodeStartState(raw)
			failures := compareStartState(snapshot, view)
			if got := startConditions(failures); !slices.Equal(got, tc.want) {
				t.Fatalf("conditions %v, want %v: %v", got, tc.want, failures)
			}
			var text []string
			for _, f := range failures {
				text = append(text, f.String())
			}
			if joined := strings.Join(text, "; "); !strings.Contains(joined, tc.detail) {
				t.Fatalf("failure %q does not name %q", joined, tc.detail)
			}
		})
	}
}

// Malformed output of one source is an observation failure of that source
// only: the others are still decoded.
func TestStartStateDecodingRejectsMalformedNativeOutput(t *testing.T) {
	for name, change := range map[string]func(*startStateRaw){
		"status":   func(raw *startStateRaw) { raw.Status = []byte("{") },
		"osd dump": func(raw *startStateRaw) { raw.OSDDump = []byte("[]") },
		"profiles": func(raw *startStateRaw) { raw.Profiles = []byte(`{"default":{}}`) },
		"rules":    func(raw *startStateRaw) { raw.Rules = nil },
		"config":   func(raw *startStateRaw) { raw.Config = []byte(`{}`) },
	} {
		raw := fakeStartRaw()
		change(&raw)
		view := decodeStartState(raw)
		if view.Native || len(view.Unobserved) != 1 || !view.Docker {
			t.Fatal("malformed", name, "was accepted or hid another source", view.Unobserved, view.Docker)
		}
	}
}

// The on-disk snapshot leaves out credential-like values and is readable by
// its owner only; the comparison still uses the values kept in memory.
func TestStartStateSnapshotFileOmitsSensitiveValues(t *testing.T) {
	const secret = "c2VjcmV0LXN0YXJ0LXN0YXRl"
	raw := fakeStartRaw()
	raw.Config = replaceOnce(t, raw.Config, `"mask":""}]`,
		`"mask":""},{"section":"client.rgw","name":"rgw_crypt_s3_kms_encryption_keys","value":"testkey=`+secret+`","mask":""}]`)
	view := decodeStartState(raw)
	snapshot := newStartStateSnapshot(view, fakeStartOwned, raw.Containers, fakeStartSession)
	out := t.TempDir()
	if err := writeStartStateSnapshot(out, snapshot); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(out, "start-state-snapshot.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), secret) || strings.Count(string(data), `"value_redacted": true`) != 1 ||
		!strings.Contains(string(data), `"value": "0.000000"`) {
		t.Fatal("unexpected snapshot file", string(data))
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("snapshot file mode", info, err)
	}
	changed := raw
	changed.Config = replaceOnce(t, raw.Config, "testkey="+secret, "testkey=other")
	failures := compareStartState(snapshot, decodeStartState(changed))
	if got := startConditions(failures); !slices.Equal(got, []string{startConditionConfig}) {
		t.Fatal("a changed credential-like value was not detected", failures)
	}
	for _, f := range failures {
		if strings.Contains(f.Detail, secret) || strings.Contains(f.Detail, "other") || !strings.Contains(f.Detail, "<redacted>") {
			t.Fatal("a credential-like value reached the report", f)
		}
	}
}

func TestStartStateReportRedactsAndBoundsConfigValues(t *testing.T) {
	changes := configChanges(nil, []startConfigEntry{
		{Section: "client.rgw", Name: "rgw_crypt_s3_kms_encryption_keys", Value: "testkey=c2VjcmV0"},
		{Section: "global", Name: "mon_host", Value: strings.Repeat("v", 4*startStateValueLimit)},
	})
	if len(changes) != 2 || strings.Contains(changes[0], "c2VjcmV0") || !strings.Contains(changes[0], "<redacted>") {
		t.Fatal("a credential-like value reached the report", changes)
	}
	if len(changes[1]) > 2*startStateValueLimit {
		t.Fatal("an unbounded value reached the report", len(changes[1]))
	}
}

func TestWaitStartStateAcceptsAPassingLaterPoll(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	polls := 0
	report := waitStartState(context.Background(), 5*time.Second, time.Millisecond, &snapshot, nil, func(context.Context) (startStateView, error) {
		polls++
		raw := fakeStartRaw()
		switch polls {
		case 1:
			return startStateView{}, errors.New("synthetic status failure")
		case 2:
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"pg_temp":[]`, `"pg_temp":[{"pgid":"1.0","osds":[0]}]`)
		}
		return decodeFakeStart(t, raw)
	})
	if !report.Passed || report.Polls != 3 || len(report.Failures) != 0 {
		t.Fatal("did not pass once the leftover cleared", report)
	}
}

func TestWaitStartStateIsBoundedAndReportsTheLastPoll(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	started := time.Now()
	report := waitStartState(context.Background(), 200*time.Millisecond, 20*time.Millisecond, &snapshot, nil, func(context.Context) (startStateView, error) {
		raw := fakeStartRaw()
		raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
		return decodeFakeStart(t, raw)
	})
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatal("the check outlived its budget", elapsed)
	}
	if report.Passed || report.Polls < 2 || !slices.Equal(startConditions(report.Failures), []string{startConditionOSDFlags}) {
		t.Fatal("unexpected bounded report", report)
	}
	// An observation that keeps failing is a named condition, not a pass.
	report = waitStartState(context.Background(), 50*time.Millisecond, 10*time.Millisecond, &snapshot, nil, func(context.Context) (startStateView, error) {
		return startStateView{}, errors.New("synthetic CLI failure")
	})
	if report.Passed || !slices.Equal(startConditions(report.Failures), []string{startConditionObservation}) {
		t.Fatal("an observation failure was not reported by name", report)
	}
}

// A poll cut short by the end of the budget must not hide the condition the
// previous completed poll found.
func TestWaitStartStateKeepsTheLastComparisonWhenTheBudgetEndsMidPoll(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	polls := 0
	report := waitStartState(context.Background(), 100*time.Millisecond, time.Millisecond, &snapshot, nil, func(ctx context.Context) (startStateView, error) {
		polls++
		if polls > 1 {
			<-ctx.Done()
			return startStateView{}, ctx.Err()
		}
		raw := fakeStartRaw()
		raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
		return decodeFakeStart(t, raw)
	})
	if report.Passed || report.Polls != 2 || !slices.Equal(startConditions(report.Failures), []string{startConditionOSDFlags}) {
		t.Fatal("an interrupted poll replaced the completed comparison", report)
	}
	if report.ElapsedMS < 100 {
		t.Fatal("the report does not record the elapsed time", report.ElapsedMS)
	}
}

func TestWaitStartStateWithoutSnapshotFailsAtOnce(t *testing.T) {
	report := waitStartState(context.Background(), time.Minute, time.Millisecond, nil, errors.New("synthetic snapshot failure"), func(context.Context) (startStateView, error) {
		t.Fatal("observed without a snapshot")
		return startStateView{}, nil
	})
	if report.Passed || report.Polls != 0 || len(report.Failures) != 1 || report.Failures[0].Condition != startConditionSnapshot ||
		!strings.Contains(report.Failures[0].Detail, "synthetic snapshot failure") {
		t.Fatal("unexpected report without a snapshot", report)
	}
}

func TestStartStateBudgetIsBounded(t *testing.T) {
	if budget, err := parseStartStateBudget([]string{"1"}); err != nil || budget != startStateDefaultBudget {
		t.Fatal("default budget", budget, err)
	}
	if budget, err := parseStartStateBudget([]string{"1", "45"}); err != nil || budget != 45*time.Second {
		t.Fatal("explicit budget", budget, err)
	}
	for _, fields := range [][]string{{"1", "0"}, {"1", "301"}, {"1", "+5"}, {"1", "05"}, {"1", "1.5"}, {"1", "5", "6"}} {
		if _, err := parseStartStateBudget(fields); err == nil {
			t.Fatal("accepted budget", fields)
		}
	}
}

// A failed check is a result, not a fixture failure: the control writes the
// report, acknowledges and keeps serving.
func TestStartStateControlReportsWithoutStoppingTheFixture(t *testing.T) {
	out := t.TempDir()
	c := &fixtureCluster{cluster: &ceph.Container{Container: &controlledDaemon{}}, startStateErr: errors.New("synthetic snapshot failure")}
	writeControlRequest(t, out, "verify-start-state", "7 2")
	if err := c.controls(context.Background(), out); err != nil {
		t.Fatal("a failed check stopped the fixture", err)
	}
	if !fileExists(filepath.Join(out, "start-state-verify.7")) || fileExists(filepath.Join(out, "verify-start-state")) {
		t.Fatal("the request was not consumed and acknowledged")
	}
	data, err := os.ReadFile(filepath.Join(out, "start-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report startStateReport
	if err := json.Unmarshal(data, &report); err != nil || report.Passed || report.BudgetMS != 2000 ||
		len(report.Failures) != 1 || report.Failures[0].Condition != startConditionSnapshot {
		t.Fatal("unexpected written report", string(data), err)
	}
	writeControlRequest(t, out, "verify-start-state", "8 0")
	if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid start-state budget") {
		t.Fatal("accepted an unbounded request", err)
	}
}
