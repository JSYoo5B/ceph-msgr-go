package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
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

// fakeStartDiff is an admin socket `config diff` reply in the pinned shape:
// one runtime override present at start (the override level), and options
// set by the MON database and the configuration file, which are not
// overrides.
const fakeStartDiff = `{"diff":{"log_max_recent":{"default":10000,"override":10000,"final":10000},
"osd_recovery_sleep":{"default":0,"mon":0,"final":0},"log_to_stderr":{"default":true,"file":false,"final":false}}}`

const fakeStartEmptyDiff = `{"diff":{"debug_ms":{"default":"0/5","file":"0/5","final":"0/5"}}}`

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
	raw := startStateRaw{Status: []byte(fakeStartStatus), OSDDump: []byte(fakeStartOSDDump), Profiles: []byte(fakeStartProfiles),
		Rules: []byte(fakeStartRules), Config: []byte(fakeStartConfig), Containers: fakeStartContainers(), Overrides: map[string][]byte{}}
	for name := range fakeStartOwned {
		if startDaemonName(name) {
			raw.Overrides[name] = []byte(fakeStartEmptyDiff)
		}
	}
	raw.Overrides["mgr.x"] = []byte(fakeStartDiff)
	return raw
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
	if len(snapshot.View.Overrides) != 1 || snapshot.View.Overrides[0] != (startConfigEntry{Section: "mgr.x", Name: "log_max_recent", Value: "10000"}) {
		t.Fatal("the start overrides are not exactly the override level of config diff", snapshot.View.Overrides)
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
		{"leftover daemon override", func(t *testing.T, raw *startStateRaw) {
			// ceph tell mgr.y config set log_max_recent 10002, not restored.
			raw.Overrides["mgr.y"] = []byte(`{"diff":{"log_max_recent":{"default":10000,"override":10002,"final":10002}}}`)
		}, []string{startConditionDaemonConfig}, `mgr.y runtime override log_max_recent set to "10002", absent at start`},
		{"changed daemon override", func(t *testing.T, raw *startStateRaw) {
			raw.Overrides["mgr.x"] = replaceOnce(t, raw.Overrides["mgr.x"], `"override":10000,"final":10000`, `"override":10002,"final":10002`)
		}, []string{startConditionDaemonConfig}, `mgr.x runtime override log_max_recent is "10002", was "10000"`},
		{"removed daemon override", func(t *testing.T, raw *startStateRaw) {
			// config unset of an override that existed at start.
			raw.Overrides["mgr.x"] = replaceOnce(t, raw.Overrides["mgr.x"], `"override":10000,`, ``)
		}, []string{startConditionDaemonConfig}, "mgr.x runtime override log_max_recent removed"},
		{"string override on an OSD", func(t *testing.T, raw *startStateRaw) {
			// injectargs and admin socket config set land on the same level.
			raw.Overrides["osd.0"] = []byte(`{"diff":{"debug_ms":{"default":"0/5","file":"0/5","override":"20/20","final":"20/20"}}}`)
		}, []string{startConditionDaemonConfig}, `osd.0 runtime override debug_ms set to "20/20"`},
		{"MON database level in config diff", func(t *testing.T, raw *startStateRaw) {
			// A MON database change shows on the mon level of config diff
			// and is reported once, by config.
			raw.Overrides["mgr.x"] = replaceOnce(t, raw.Overrides["mgr.x"], `"mon":0,"final":0`, `"mon":0.25,"final":0.25`)
			raw.Config = replaceOnce(t, raw.Config, `"value":"0.000000"`, `"value":"0.250000"`)
		}, []string{startConditionConfig}, "osd/osd_recovery_sleep"},
		{"stopped single MON", func(t *testing.T, raw *startStateRaw) {
			// The MON reads fail; Docker and the other admin sockets still
			// name the stopped daemon and a leftover override.
			raw.Status, raw.OSDDump, raw.Profiles, raw.Rules, raw.Config = nil, nil, nil, nil, nil
			raw.NativeErrs = map[string]error{}
			for _, read := range startNativeReads {
				raw.NativeErrs[read.name] = errors.New("synthetic timeout")
			}
			raw.Containers = setContainerState(raw.Containers, "id-mon-a", "exited")
			delete(raw.Overrides, "mon.a")
			raw.Overrides["osd.1"] = []byte(`{"diff":{"debug_osd":{"default":"1/5","override":"20/20","final":"20/20"}}}`)
		}, []string{startConditionObservation, startConditionDaemonRunning, startConditionDaemonConfig}, "mon.a container id-mon-a is exited"},
		{"failed Docker listing", func(t *testing.T, raw *startStateRaw) {
			raw.Containers, raw.DockerErr = nil, errors.New("synthetic daemon socket failure")
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
		}, []string{startConditionObservation, startConditionOSDFlags}, "Docker container list: synthetic daemon socket failure"},
		{"failed admin socket", func(t *testing.T, raw *startStateRaw) {
			delete(raw.Overrides, "osd.0")
			raw.OverrideErrs = map[string]error{"osd.0": errors.New("synthetic admin socket failure")}
			raw.Overrides["mgr.x"] = replaceOnce(t, raw.Overrides["mgr.x"], `"override":10000,"final":10000`, `"override":10002,"final":10002`)
		}, []string{startConditionObservation, startConditionDaemonConfig}, "osd.0 admin socket config diff: synthetic admin socket failure"},
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

// Malformed output of one read is an observation failure of that read only:
// every other read is still decoded.
func TestStartStateDecodingRejectsMalformedNativeOutput(t *testing.T) {
	for read, change := range map[string]func(*startStateRaw){
		startReadStatus:   func(raw *startStateRaw) { raw.Status = []byte("{") },
		startReadOSDDump:  func(raw *startStateRaw) { raw.OSDDump = []byte("[]") },
		startReadProfiles: func(raw *startStateRaw) { raw.Profiles = []byte(`{"default":{}}`) },
		startReadRules:    func(raw *startStateRaw) { raw.Rules = nil },
		startReadConfig:   func(raw *startStateRaw) { raw.Config = []byte(`{}`) },
	} {
		raw := fakeStartRaw()
		change(&raw)
		view := decodeStartState(raw)
		if view.Observed[read] || len(view.Unobserved) != 1 || !strings.HasPrefix(view.Unobserved[0].Detail, read+": decode: ") ||
			len(view.Observed) != len(startNativeReads)-1+1+7 || len(view.Read) != 7 {
			t.Fatal("malformed", read, "was accepted or hid another read", view.Unobserved, view.Observed)
		}
	}
	for name, diff := range map[string]string{"not JSON": "{", "no diff object": `{"log_max_recent":{"override":1}}`} {
		raw := fakeStartRaw()
		raw.Overrides["osd.1"] = []byte(diff)
		view := decodeStartState(raw)
		if !view.Observed[startReadStatus] || slices.Contains(view.Read, "osd.1") || len(view.Unobserved) != 1 || !strings.Contains(view.Unobserved[0].Detail, "osd.1") {
			t.Fatal("malformed config diff", name, "was accepted", view.Unobserved, view.Read)
		}
	}
}

// The review case: a completed `ceph status` with peering PGs still fails
// pg-clean when the next MON read, `ceph osd dump`, times out.
func TestStartStateKeepsTheStatusWhenOSDDumpFails(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	raw := fakeStartRaw()
	raw.Status = replaceOnce(t, raw.Status, `"active+clean","count":9`, `"peering","count":9`)
	raw.OSDDump, raw.NativeErrs = nil, map[string]error{startReadOSDDump: context.DeadlineExceeded}
	failures := compareStartState(snapshot, decodeStartState(raw))
	if got := startConditions(failures); !slices.Equal(got, []string{startConditionObservation, startConditionPGClean}) {
		t.Fatal("conditions", got, failures)
	}
	if failures[0].Detail != "ceph osd dump: context deadline exceeded" || failures[1].Detail != "PGs not active+clean: 9xpeering" {
		t.Fatal("details", failures)
	}
}

// startStateLeftovers changes every read of a fake observation the way a
// leftover shows in it, and names the condition each read then fails.
func startStateLeftovers(t *testing.T, raw *startStateRaw) map[string]string {
	t.Helper()
	raw.Status = replaceOnce(t, raw.Status, `"active+clean","count":9`, `"peering","count":9`)
	raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
	raw.Profiles = []byte(`["default","owned-k2m2"]`)
	raw.Rules = []byte(`["owned-k2m2","replicated_rule"]`)
	raw.Config = replaceOnce(t, raw.Config, `"value":"0.000000"`, `"value":"0.100000"`)
	raw.Containers = append(slices.Clone(raw.Containers), fakeStartContainer("id-left-behind", "netem-helper", "exited",
		map[string]string{"org.testcontainers": "true", "org.testcontainers.sessionId": fakeStartSession}))
	raw.Overrides["mgr.y"] = []byte(`{"diff":{"log_max_recent":{"default":10000,"override":10002,"final":10002}}}`)
	return map[string]string{
		startReadStatus: startConditionPGClean, startReadOSDDump: startConditionOSDFlags, startReadProfiles: startConditionErasureProfile,
		startReadRules: startConditionCrushRule, startReadConfig: startConditionConfig, startReadDocker: startConditionForeignContainer,
		startReadAdminSocket("mgr.y"): startConditionDaemonConfig,
	}
}

// For each read that fails, the conditions of every other read, all with a
// leftover, are still reported beside the failed read's observation.
func TestStartStateFailedReadKeepsTheOtherReads(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	for _, failed := range []string{startReadStatus, startReadOSDDump, startReadProfiles, startReadRules, startReadConfig,
		startReadDocker, startReadAdminSocket("mgr.y")} {
		t.Run(failed, func(t *testing.T) {
			raw := fakeStartRaw()
			leftovers := startStateLeftovers(t, &raw)
			cause := errors.New("synthetic failure")
			switch failed {
			case startReadDocker:
				raw.Containers, raw.DockerErr = nil, cause
			case startReadAdminSocket("mgr.y"):
				delete(raw.Overrides, "mgr.y")
				raw.OverrideErrs = map[string]error{"mgr.y": cause}
			default:
				raw.NativeErrs = map[string]error{failed: cause}
			}
			failures := compareStartState(snapshot, decodeStartState(raw))
			want := []string{startConditionObservation}
			for _, condition := range startConditionOrder {
				for read, leftover := range leftovers {
					if leftover == condition && read != failed {
						want = append(want, condition)
					}
				}
			}
			if got := startConditions(failures); !slices.Equal(got, want) {
				t.Fatalf("conditions %v, want %v: %v", got, want, failures)
			}
			if observations := slices.DeleteFunc(slices.Clone(failures), func(f startStateFailure) bool { return f.Condition != startConditionObservation }); len(observations) != 1 ||
				observations[0].Detail != failed+": synthetic failure" {
				t.Fatal("the observation failure does not name the failed read", observations)
			}
		})
	}
}

// The snapshot needs every read, including the admin socket of a daemon
// whose container was not running.
func TestStartStateSnapshotNeedsEveryRead(t *testing.T) {
	daemons := map[string]string{}
	for name, id := range fakeStartOwned {
		if startDaemonName(name) {
			daemons[name] = id
		}
	}
	if err := startSnapshotComplete(decodeStartState(fakeStartRaw()), daemons); err != nil {
		t.Fatal("rejected a complete observation", err)
	}
	raw := fakeStartRaw()
	delete(raw.Overrides, "osd.1")
	raw.NativeErrs = map[string]error{startReadRules: errors.New("synthetic failure")}
	err := startSnapshotComplete(decodeStartState(raw), daemons)
	if err == nil || !strings.Contains(err.Error(), "ceph osd crush rule ls: synthetic failure") ||
		!strings.Contains(err.Error(), "osd.1 admin socket config diff: not read") {
		t.Fatal("accepted an incomplete observation", err)
	}
}

// A daemon whose container the listing shows not running is not read; every
// daemon is read when the listing failed.
func TestStartStateReadsTheAdminSocketOfRunningDaemons(t *testing.T) {
	daemons := map[string]string{"mon.a": "id-mon-a", "osd.0": "id-osd-0", "osd.1": "id-osd-1"}
	containers := setContainerState(fakeStartContainers(), "id-osd-1", "exited")
	if got := startDaemonsToRead(daemons, containers, nil); !slices.Equal(got, []string{"mon.a", "osd.0"}) {
		t.Fatal("read daemons", got)
	}
	if got := startDaemonsToRead(daemons, nil, errors.New("synthetic")); !slices.Equal(got, []string{"mon.a", "osd.0", "osd.1"}) {
		t.Fatal("read daemons without a listing", got)
	}
}

// The on-disk snapshot leaves out credential-like values and is readable by
// its owner only; the comparison still uses the values kept in memory.
func TestStartStateSnapshotFileOmitsSensitiveValues(t *testing.T) {
	const secret = "c2VjcmV0LXN0YXJ0LXN0YXRl"
	raw := fakeStartRaw()
	raw.Config = replaceOnce(t, raw.Config, `"mask":""}]`,
		`"mask":""},{"section":"client.rgw","name":"rgw_crypt_s3_kms_encryption_keys","value":"testkey=`+secret+`","mask":""}]`)
	raw.Overrides["osd.0"] = []byte(`{"diff":{"rgw_keystone_admin_password":{"default":"","override":"` + secret + `","final":"` + secret + `"}}}`)
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
	if strings.Contains(string(data), secret) || strings.Count(string(data), `"value_redacted": true`) != 2 ||
		!strings.Contains(string(data), `"value": "0.000000"`) || !strings.Contains(string(data), `"value": "10000"`) {
		t.Fatal("unexpected snapshot file", string(data))
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("snapshot file mode", info, err)
	}
	changed := raw
	changed.Config = replaceOnce(t, raw.Config, "testkey="+secret, "testkey=other")
	changed.Overrides = maps.Clone(raw.Overrides)
	changed.Overrides["osd.0"] = []byte(`{"diff":{"rgw_keystone_admin_password":{"default":"","override":"other","final":"other"}}}`)
	failures := compareStartState(snapshot, decodeStartState(changed))
	if got := startConditions(failures); !slices.Equal(got, []string{startConditionConfig, startConditionDaemonConfig}) {
		t.Fatal("a changed credential-like value was not detected", failures)
	}
	for _, f := range failures {
		if strings.Contains(f.Detail, secret) || strings.Contains(f.Detail, "other") || !strings.Contains(f.Detail, "<redacted>") {
			t.Fatal("a credential-like value reached the report", f)
		}
	}
}

func TestStartStateReportRedactsAndBoundsConfigValues(t *testing.T) {
	changes := configChanges("", nil, []startConfigEntry{
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

// A poll that the end of the budget cut short reports each read it completed
// and keeps, for each read it did not, the failures the latest completed read
// found; a read that has no failure to keep is reported as its observation.
func TestWaitStartStateMergesTheReadsOfAPollTheBudgetCut(t *testing.T) {
	snapshot := fakeStartSnapshot(t)
	polls := 0
	report := waitStartState(context.Background(), 100*time.Millisecond, time.Millisecond, &snapshot, nil, func(ctx context.Context) (startStateView, error) {
		polls++
		raw := fakeStartRaw()
		if polls == 1 {
			// Completed: osd-flags from osd dump and crush-rule from the rule list.
			raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
			raw.Rules = []byte(`["owned-k2m2","replicated_rule"]`)
			return decodeStartState(raw), nil
		}
		// Cut: the status completes with peering PGs and a clean rule list;
		// osd dump and the config dump do not complete.
		<-ctx.Done()
		raw.Status = replaceOnce(t, raw.Status, `"active+clean","count":9`, `"peering","count":9`)
		raw.OSDDump, raw.Config = nil, nil
		raw.NativeErrs = map[string]error{startReadOSDDump: ctx.Err(), startReadConfig: ctx.Err()}
		return decodeStartState(raw), nil
	})
	var got []string
	for _, f := range report.Failures {
		got = append(got, f.String())
	}
	want := []string{"observation: ceph config dump: context deadline exceeded", "pg-clean: PGs not active+clean: 9xpeering",
		"osd-flags: OSDMap flags set [noout], cleared []"}
	if report.Passed || report.Polls != 2 || !slices.Equal(got, want) {
		t.Fatalf("report %v, want %v", got, want)
	}
	// A read that failed in a poll the budget did not cut reports its
	// observation, not an earlier poll's failures.
	latest := map[string][]startStateFailure{}
	raw := fakeStartRaw()
	raw.OSDDump = replaceOnce(t, raw.OSDDump, `"flags":"sortbitwise,`, `"flags":"noout,sortbitwise,`)
	view := decodeStartState(raw)
	mergeStartPoll(compareStartState(snapshot, view), view.Observed, latest, false)
	raw = fakeStartRaw()
	raw.NativeErrs = map[string]error{startReadOSDDump: errors.New("synthetic failure")}
	view = decodeStartState(raw)
	if merged := mergeStartPoll(compareStartState(snapshot, view), view.Observed, latest, false); len(merged) != 1 ||
		merged[0].Detail != "ceph osd dump: synthetic failure" {
		t.Fatal("an uncut failed read kept an earlier poll's failures", merged)
	}
	if merged := mergeStartPoll(compareStartState(snapshot, view), view.Observed, latest, true); len(merged) != 1 ||
		merged[0].Condition != startConditionObservation {
		t.Fatal("a cut read kept failures the latest completed read did not find", merged)
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
