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
)

// fakeDaemonCommand is one daemon process: a runtime override layer over the
// default 10000 for the option cells and the scrubber flag for the pg cell,
// as the admin-socket handlers present them.
type fakeDaemonCommand struct {
	daemon      string
	incarnation uint64
	address     string
	override    *string
	flag        bool
	primary     bool
	calls       []string
	failSet     error
	mapErr      error
	// restartAfter replaces the daemon incarnation after this many map reads.
	restartAfter int
	mapReads     int
}

func (f *fakeDaemonCommand) backend() daemonCommandBackend {
	return daemonCommandBackend{
		identity: func(context.Context) daemonCommandIdentity {
			f.mapReads++
			if f.mapErr != nil {
				return daemonCommandOSDIdentity(strings.TrimPrefix(f.daemon, "osd."), nil, f.mapErr)
			}
			incarnation, address := f.incarnation, f.address
			if f.restartAfter != 0 && f.mapReads > f.restartAfter {
				incarnation, address = incarnation+1, "192.0.2.7:6800/2"
			}
			return daemonCommandIdentity{OK: true, Daemon: f.daemon, Up: true, Incarnation: incarnation, Address: address}
		},
		adminSocket: func(_ context.Context, args ...string) ([]byte, error) {
			f.calls = append(f.calls, strings.Join(args, " "))
			switch {
			case slices.Equal(args, []string{"config", "get", "log_max_recent"}):
				value := "10000"
				if f.override != nil {
					value = *f.override
				}
				return []byte(`{"log_max_recent":"` + value + `"}`), nil
			case slices.Equal(args, []string{"config", "diff", "get", "log_max_recent"}):
				// Unrelated entries of other JSON types must not reach the receipt.
				if f.override == nil {
					return []byte(`{"diff":{"osd_objectstore":{"default":"bluestore","file":"memstore"},"ms_bind_ipv4":{"default":true,"final":false}}}`), nil
				}
				return []byte(`{"diff":{"log_max_recent":{"default":10000,"override":"` + *f.override + `","final":"` + *f.override + `"}}}`), nil
			case len(args) == 4 && args[0] == "config" && args[1] == "set" && args[2] == "log_max_recent":
				if f.failSet != nil {
					return nil, f.failSet
				}
				f.override = &args[3]
				return []byte(`{"success":""}`), nil
			case slices.Equal(args, []string{"config", "unset", "log_max_recent"}):
				f.override = nil
				return []byte(`{"success":""}`), nil
			case slices.Equal(args, []string{"pg", "2.0", "query"}):
				scrubber := ""
				if f.primary {
					scrubber = `,"scrubber":{"active":false,"must_scrub":false}`
					if f.flag {
						scrubber = `,"scrubber":{"active":false,"must_scrub":false,"test_sequence":3}`
					}
				}
				// Peering details of other JSON types must not reach the receipt.
				return []byte(`{"snap_trimq":"[]","state":"active+clean","epoch":21,"up":[0],"acting":[0],"info":{"pgid":"2.0","last_update":"19'4"}` + scrubber + `}`), nil
			case slices.Equal(args, []string{"scrubdebug", "2.0", "unset", "sessions"}):
				if f.failSet != nil {
					return nil, f.failSet
				}
				f.flag = false
				return []byte(`{"result":{"success":true}}`), nil
			}
			return nil, errors.New("unexpected admin socket command")
		},
	}
}

func newFakeDaemonCommand(daemon string) *fakeDaemonCommand {
	return &fakeDaemonCommand{daemon: daemon, incarnation: 5, address: "192.0.2.7:6800/1", primary: true}
}

func daemonCommandRequest(t *testing.T, f *fakeDaemonCommand, out string, id int64, token, phase string, target daemonCommandTarget) (daemonCommandReceipt, error) {
	t.Helper()
	var receipt daemonCommandReceipt
	if err := daemonCommandOutcome(context.Background(), out, id, token, phase, target, f.backend()); err != nil {
		return receipt, err
	}
	data, err := os.ReadFile(filepath.Join(out, daemonCommandPrefix+token+"."+phase+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt, nil
}

func daemonCommandState(receipt daemonCommandReceipt) string {
	if query := receipt.Views.PGQuery; query != nil {
		if !query.Valid || !query.ScrubberPresent {
			return "unreadable"
		}
		if query.FlagPresent {
			return "listed"
		}
		return "unlisted"
	}
	if receipt.Views.ConfigGet == nil || receipt.Views.ConfigDiff == nil {
		return "unreadable"
	}
	effective := mgrTellEffective(*receipt.Views.ConfigGet)
	if effective == nil || !receipt.Views.ConfigDiff.Valid {
		return "unreadable"
	}
	if entry := receipt.Views.ConfigDiff.Entry; entry != nil && entry.OverridePresent && entry.Override != nil {
		return *effective + "/override=" + *entry.Override
	}
	return *effective + "/absent"
}

func TestDaemonCommandTargetIsClosed(t *testing.T) {
	for _, fields := range [][]string{nil, {"osd"}, {"osd", "0", "2.0"}, {"osd", "00"}, {"osd", "-1"}, {"osd", "a"}, {"osd", "12345"},
		{"pg", "0"}, {"pg", "0", "2.0", "x"}, {"pg", "0", "2"}, {"pg", "0", "2.G"}, {"pg", "0", "02.0"}, {"pg", "a", "2.0"}, {"pg", "0", "2.0s1"},
		{"mds", ""}, {"mds", "a", "b"}, {"mds", "a/b"}, {"mds", "../x"}, {"mds", strings.Repeat("a", 65)}, {"mon", "a"}, {"mgr", "a"}, {"OSD", "0"}} {
		if target, err := parseDaemonCommandTarget(fields); err == nil {
			t.Fatal("open target admitted", fields, target)
		}
	}
	for fields, want := range map[string]daemonCommandTarget{
		"osd 0": {Kind: "osd", ID: "0"}, "osd 12": {Kind: "osd", ID: "12"}, "pg 3 12.1f": {Kind: "pg", ID: "3", PGID: "12.1f"}, "mds fs-a.x_1": {Kind: "mds", ID: "fs-a.x_1"},
	} {
		if target, err := parseDaemonCommandTarget(strings.Fields(fields)); err != nil || target != want {
			t.Fatal("closed target refused", fields, target, err)
		}
	}
	if (daemonCommandTarget{Kind: "pg", ID: "3", PGID: "1.0"}).daemon() != "osd.3" || (daemonCommandTarget{Kind: "mds", ID: "a"}).daemon() != "mds.a" {
		t.Fatal("target daemon name")
	}
}

func TestDaemonCommandOutcomeRejectsOpenRequestsBeforeNativeWork(t *testing.T) {
	token := strings.Repeat("a", 32)
	target := daemonCommandTarget{Kind: "osd", ID: "0"}
	f := newFakeDaemonCommand("osd.0")
	out := t.TempDir()
	for _, request := range []struct {
		id           int64
		token, phase string
	}{{0, token, "initial"}, {-1, token, "initial"}, {1, "short", "initial"}, {1, strings.ToUpper(token), "initial"}, {1, token, ""},
		{1, token, "overwritten"}, {1, token, "cleanup"}, {1, token, "bystander-initial"}} {
		if err := daemonCommandOutcome(context.Background(), out, request.id, request.token, request.phase, target, f.backend()); err == nil {
			t.Fatal("open request admitted", request)
		}
	}
	// No phase runs before an initial baseline, and none for another target.
	for _, phase := range []string{"applied", "overwrite", "recovered", "restore", "restored"} {
		if _, err := daemonCommandRequest(t, f, out, 1, token, phase, target); err == nil {
			t.Fatal("phase ran without a baseline", phase)
		}
	}
	if len(f.calls) != 0 || f.mapReads != 0 {
		t.Fatal("rejected requests reached the native surface", f.calls, f.mapReads)
	}
	if _, err := daemonCommandRequest(t, f, out, 2, token, "initial", target); err != nil {
		t.Fatal(err)
	}
	calls := len(f.calls)
	for _, other := range []daemonCommandTarget{{Kind: "osd", ID: "1"}, {Kind: "pg", ID: "0", PGID: "2.0"}, {Kind: "mds", ID: "0"}} {
		for _, phase := range []string{"applied", "overwrite", "restore"} {
			if _, err := daemonCommandRequest(t, f, out, 3, token, phase, other); err == nil {
				t.Fatal("another target used this token's baseline", other, phase)
			}
		}
	}
	if len(f.calls) != calls || f.override != nil {
		t.Fatal("a request for another target reached the daemon", f.calls)
	}
}

func TestDaemonCommandOptionCellOverwritesAndRestoresOnce(t *testing.T) {
	for _, target := range []daemonCommandTarget{{Kind: "osd", ID: "0"}, {Kind: "mds", ID: "fs-a"}} {
		for _, original := range []*string{nil, new(string)} {
			f := newFakeDaemonCommand(target.daemon())
			want := "10000/absent"
			if original != nil {
				*original = "10000"
				value := *original
				f.override, want = &value, "10000/override=10000"
			}
			out, token := t.TempDir(), strings.Repeat("b", 32)
			initial, err := daemonCommandRequest(t, f, out, 1, token, "initial", target)
			if err != nil || !initial.Restorable || initial.Baseline == nil || initial.Baseline.Present != (original != nil) || daemonCommandState(initial) != want {
				t.Fatal("baseline", target, initial, err)
			}
			// A repeated initial request cannot replace the first baseline.
			if _, err := daemonCommandRequest(t, f, out, 2, token, "initial", target); err == nil {
				t.Fatal("a second initial request replaced the baseline")
			}
			// The Go client's A, then one native overwrite B.
			a := "10001"
			f.override = &a
			if applied, err := daemonCommandRequest(t, f, out, 3, token, "applied", target); err != nil || daemonCommandState(applied) != "10001/override=10001" || applied.Mutation != nil {
				t.Fatal("applied", applied, err)
			}
			overwritten, err := daemonCommandRequest(t, f, out, 4, token, "overwrite", target)
			if err != nil || overwritten.Mutation == nil || !overwritten.Mutation.OK || !slices.Equal(overwritten.Mutation.Args, []string{"config", "set", "log_max_recent", "10002"}) ||
				daemonCommandState(overwritten) != "10002/override=10002" {
				t.Fatal("overwrite", overwritten, err)
			}
			if _, err := daemonCommandRequest(t, f, out, 5, token, "overwrite", target); err == nil || *f.override != "10002" {
				t.Fatal("the overwrite was submitted twice", err)
			}
			restored, err := daemonCommandRequest(t, f, out, 6, token, "restore", target)
			if err != nil || restored.Mutation == nil || !restored.Mutation.OK || daemonCommandState(restored) != want {
				t.Fatal("restore", restored, err)
			}
			if _, err := daemonCommandRequest(t, f, out, 7, token, "restore", target); err == nil {
				t.Fatal("the restoration was submitted twice")
			}
			if final, err := daemonCommandRequest(t, f, out, 8, token, "restored", target); err != nil || daemonCommandState(final) != want || final.Mutation != nil {
				t.Fatal("restored", final, err)
			}
			sets := 0
			for _, call := range f.calls {
				if strings.HasPrefix(call, "config set") || strings.HasPrefix(call, "config unset") {
					sets++
				}
			}
			if sets != 2 {
				t.Fatal("native mutations were not exactly one overwrite and one restoration", f.calls)
			}
			journal, err := os.ReadFile(filepath.Join(out, "daemon-command-outcome-native.jsonl"))
			if err != nil || strings.Contains(string(journal), "osd_objectstore") || strings.Contains(string(journal), "ms_bind_ipv4") {
				t.Fatal("the journal is missing or persisted another option", err)
			}
		}
	}
}

func TestDaemonCommandPGCellObservesOnlyTheScrubberFlag(t *testing.T) {
	target := daemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.0"}
	f := newFakeDaemonCommand("osd.0")
	out, token := t.TempDir(), strings.Repeat("c", 32)
	initial, err := daemonCommandRequest(t, f, out, 1, token, "initial", target)
	if err != nil || !initial.Restorable || initial.Baseline != nil || daemonCommandState(initial) != "unlisted" || initial.Views.ConfigGet != nil {
		t.Fatal("baseline", initial, err)
	}
	f.flag = true // the Go client's scrubdebug set
	if applied, err := daemonCommandRequest(t, f, out, 2, token, "applied", target); err != nil || daemonCommandState(applied) != "listed" {
		t.Fatal("applied", applied, err)
	}
	overwritten, err := daemonCommandRequest(t, f, out, 3, token, "overwrite", target)
	if err != nil || overwritten.Mutation == nil || !overwritten.Mutation.OK || !slices.Equal(overwritten.Mutation.Args, []string{"scrubdebug", "2.0", "unset", "sessions"}) || daemonCommandState(overwritten) != "unlisted" {
		t.Fatal("overwrite", overwritten, err)
	}
	f.flag = true // a replay of the Go client's set would be listed again
	if replayed, err := daemonCommandRequest(t, f, out, 4, token, "recovered", target); err != nil || daemonCommandState(replayed) != "listed" {
		t.Fatal("a replayed set is not visible after the overwrite", replayed, err)
	}
	if restored, err := daemonCommandRequest(t, f, out, 5, token, "restore", target); err != nil || restored.Mutation == nil || !restored.Mutation.OK || daemonCommandState(restored) != "unlisted" {
		t.Fatal("restore", restored, err)
	}
	journal, err := os.ReadFile(filepath.Join(out, "daemon-command-outcome-native.jsonl"))
	if err != nil || strings.Contains(string(journal), "last_update") || strings.Contains(string(journal), "snap_trimq") {
		t.Fatal("the journal is missing or persisted other query output", err)
	}
	// A set flag, a PG without a scrubber section (not the active primary) or
	// another PG's reply is no restorable baseline, and nothing is recorded.
	for name, change := range map[string]func(*fakeDaemonCommand){
		"flag already set": func(f *fakeDaemonCommand) { f.flag = true },
		"not the primary":  func(f *fakeDaemonCommand) { f.primary = false },
	} {
		f := newFakeDaemonCommand("osd.0")
		change(f)
		out := t.TempDir()
		receipt, err := daemonCommandRequest(t, f, out, 1, token, "initial", target)
		if err != nil || receipt.Restorable {
			t.Fatal("an unrestorable PG baseline was recorded:", name, err)
		}
		if _, err := daemonCommandRequest(t, f, out, 2, token, "overwrite", target); err == nil {
			t.Fatal("a mutation ran without a baseline:", name)
		}
	}
	if query := daemonCommandPGQuery([]byte(`{"state":"active+clean","info":{"pgid":"3.0"},"scrubber":{}}`), nil); !query.Valid || query.PGID != "3.0" {
		t.Fatal("query projection", query)
	}
	other := daemonCommandViews{DaemonBefore: f.backend().identity(context.Background()), DaemonAfter: f.backend().identity(context.Background()),
		PGQuery: daemonCommandPGQuery([]byte(`{"state":"active+clean","info":{"pgid":"3.0"},"scrubber":{}}`), nil)}
	if ok, _ := daemonCommandRestorable(other, target); ok {
		t.Fatal("another PG's query was accepted as this PG's baseline")
	}
	for _, data := range []string{``, `[]`, `{"info":{"pgid":"../x"}}`, `{"state":1,"info":{"pgid":"2.0"}}`} {
		if query := daemonCommandPGQuery([]byte(data), nil); query.Valid {
			t.Fatal("an invalid query reply was projected", data)
		}
	}
}

func TestDaemonCommandOutcomeRequiresOneStableIncarnation(t *testing.T) {
	target := daemonCommandTarget{Kind: "osd", ID: "0"}
	token := strings.Repeat("d", 32)
	for name, change := range map[string]func(*fakeDaemonCommand){
		"map read failed":       func(f *fakeDaemonCommand) { f.mapErr = errors.New("timeout reading /private/path") },
		"restarted in between":  func(f *fakeDaemonCommand) { f.restartAfter = 1 },
		"another daemon's view": func(f *fakeDaemonCommand) { f.daemon = "osd.1" },
		"no address":            func(f *fakeDaemonCommand) { f.address = "" },
		"no incarnation":        func(f *fakeDaemonCommand) { f.incarnation = 0 },
		"out of bound":          func(f *fakeDaemonCommand) { v := "19999"; f.override = &v },
	} {
		f := newFakeDaemonCommand("osd.0")
		change(f)
		out := t.TempDir()
		receipt, err := daemonCommandRequest(t, f, out, 1, token, "initial", target)
		if err != nil || receipt.Restorable || receipt.Baseline != nil {
			t.Fatal("an unstable or unbounded baseline was recorded:", name, receipt, err)
		}
		if strings.Contains(receipt.Views.DaemonBefore.Error, "/private") {
			t.Fatal("a path reached the receipt", receipt.Views.DaemonBefore.Error)
		}
		if _, err := os.Stat(filepath.Join(out, daemonCommandPrefix+token+".baseline.json")); err == nil {
			t.Fatal("a baseline file was written:", name)
		}
	}
	// A failed native mutation is recorded as it returned and never repeated.
	f := newFakeDaemonCommand("osd.0")
	out := t.TempDir()
	if _, err := daemonCommandRequest(t, f, out, 1, token, "initial", target); err != nil {
		t.Fatal(err)
	}
	f.failSet = errors.New("admin socket of osd.0: exit 22")
	failed, err := daemonCommandRequest(t, f, out, 2, token, "overwrite", target)
	if err != nil || failed.Mutation == nil || failed.Mutation.OK || failed.Mutation.Error == "" || daemonCommandState(failed) != "10000/absent" {
		t.Fatal("a failed overwrite was not recorded as it returned", failed, err)
	}
	f.failSet = nil
	if _, err := daemonCommandRequest(t, f, out, 3, token, "overwrite", target); err == nil || f.override != nil {
		t.Fatal("a failed overwrite was repeated")
	}
}

func TestDaemonCommandIdentityProjections(t *testing.T) {
	osd := daemonCommandOSDIdentity("1", []byte(`{"epoch":9,"osds":[{"osd":0,"up":1,"up_from":4,"public_addrs":{"addrvec":[{"type":"v2","addr":"192.0.2.6:6800","nonce":7}]}},
		{"osd":1,"up":1,"up_from":5,"public_addrs":{"addrvec":[{"type":"v1","addr":"192.0.2.7:6801","nonce":9},{"type":"v2","addr":"192.0.2.7:6800","nonce":9}]}}]}`), nil)
	if osd != (daemonCommandIdentity{OK: true, Daemon: "osd.1", Up: true, Incarnation: 5, Address: "192.0.2.7:6800/9"}) {
		t.Fatal("osd identity", osd)
	}
	if down := daemonCommandOSDIdentity("0", []byte(`{"osds":[{"osd":0,"up":0,"up_from":4}]}`), nil); !down.OK || down.Up || down.Address != "" {
		t.Fatal("a down OSD", down)
	}
	if absent := daemonCommandOSDIdentity("3", []byte(`{"osds":[]}`), nil); !absent.OK || absent.Up || absent.Incarnation != 0 {
		t.Fatal("an absent OSD", absent)
	}
	dump := `{"filesystems":[{"mdsmap":{"info":{"gid_4242":{"gid":4242,"name":"fs-a","rank":0,"state":"up:active","addrs":{"addrvec":[{"type":"v2","addr":"192.0.2.9:6800","nonce":11}]}},
		"gid_4300":{"gid":4300,"name":"fs-b","rank":1,"state":"up:replay","addrs":{"addrvec":[{"type":"v2","addr":"192.0.2.10:6800","nonce":12}]}}}}}]}`
	if mds := daemonCommandMDSIdentity("fs-a", []byte(dump), nil); mds != (daemonCommandIdentity{OK: true, Daemon: "mds.fs-a", Up: true, Incarnation: 4242, Address: "192.0.2.9:6800/11"}) {
		t.Fatal("mds identity", mds)
	}
	if replay := daemonCommandMDSIdentity("fs-b", []byte(dump), nil); !replay.OK || replay.Up || replay.Incarnation != 4300 {
		t.Fatal("an MDS that is not active", replay)
	}
	duplicated := strings.Replace(dump, `"name":"fs-b"`, `"name":"fs-a"`, 1)
	if twice := daemonCommandMDSIdentity("fs-a", []byte(duplicated), nil); twice.OK || twice.Up {
		t.Fatal("a name listed twice identified a daemon", twice)
	}
	for _, identity := range []daemonCommandIdentity{daemonCommandOSDIdentity("0", []byte(`x`), nil), daemonCommandMDSIdentity("a", []byte(`x`), nil),
		daemonCommandOSDIdentity("0", nil, errors.New("exit 1")), daemonCommandMDSIdentity("a", nil, errors.New("exit 1"))} {
		if identity.OK || identity.Up || identity.Error == "" {
			t.Fatal("a failed or invalid map read identified a daemon", identity)
		}
	}
}

func TestMDSCommandsProfileAdmitsOnlyTheExplicitAppliedCommandSelector(t *testing.T) {
	env := mdsCommandsProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = daemonCommandMDSSelector
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || p.testRun != daemonCommandMDSSelector || p.mdsCommands != "1" || !p.mdsFreshOSDAllowed() {
		t.Fatal("explicit applied-command selector was not admitted on the command fixture", p, err)
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_COMMANDS": "0"},
		{"CEPH_MSGR_TEST_RUN": "^TestCeph(MDSCommands|AppliedMDSCommandIsNotReplayed)Integration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephAppliedMDSCommandIsNotReplayedIntegration"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
	} {
		env := mdsCommandsProfileEnv()
		env["CEPH_MSGR_TEST_RUN"] = daemonCommandMDSSelector
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe applied-command profile accepted", change, p, err)
		}
	}
}
