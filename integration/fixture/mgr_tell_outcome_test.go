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

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// fakeMgrTell is one manager process per name with a runtime override layer
// over the default 10000, as the admin-socket handlers present it.
type fakeMgrTell struct {
	active    string
	gid       uint64
	overrides map[string]string
	calls     []string
	failSet   error
	mapErr    error
	// swap replaces the active manager after this many MgrMap reads.
	swapAfter int
	// restartAfter gives the same name a new GID after this many MgrMap reads.
	restartAfter int
	mapReads     int
}

func (f *fakeMgrTell) backend() mgrTellBackend {
	return mgrTellBackend{
		managerMap: func(context.Context) (managerPreparationMap, error) {
			f.mapReads++
			if f.mapErr != nil {
				return managerPreparationMap{}, f.mapErr
			}
			name, gid := f.active, f.gid
			if f.restartAfter != 0 && f.mapReads > f.restartAfter {
				gid += 832
			}
			if f.swapAfter != 0 && f.mapReads > f.swapAfter {
				name, gid = map[string]string{"a": "b", "b": "a"}[name], gid+1
			}
			standby := map[string]string{"a": "b", "b": "a"}[name]
			state := managerPreparationMap{ManagerStatus: ceph.ManagerStatus{Available: true, ActiveName: name, ActiveGID: gid}, Epoch: 4}
			state.Standbys = append(state.Standbys, struct {
				Name string `json:"name"`
				GID  uint64 `json:"gid"`
			}{standby, gid + 100})
			return state, nil
		},
		adminSocket: func(_ context.Context, target string, args ...string) ([]byte, error) {
			f.calls = append(f.calls, target+":"+strings.Join(args, " "))
			override, present := f.overrides[target]
			switch strings.Join(args[:min(2, len(args))], " ") {
			case "config get":
				value := "10000"
				if present {
					value = override
				}
				return []byte(`{"log_max_recent":"` + value + `"}`), nil
			case "config diff":
				// Unrelated entries of other JSON types must not reach the receipt.
				if !present {
					return []byte(`{"diff":{"mgr_initial_modules":{"default":"","file":"dashboard"},"ms_bind_ipv4":{"default":true,"final":false}}}`), nil
				}
				return []byte(`{"diff":{"log_max_recent":{"default":10000,"override":"` + override + `","final":"` + override + `"},"ms_bind_ipv4":{"default":true,"final":false}}}`), nil
			case "config set":
				if f.failSet != nil {
					return nil, f.failSet
				}
				f.overrides[target] = args[3]
				return []byte(`{"success":""}`), nil
			case "config unset":
				delete(f.overrides, target)
				return []byte(`{"success":""}`), nil
			}
			return nil, errors.New("unexpected admin socket command")
		},
	}
}

func mgrTellRequest(t *testing.T, f *fakeMgrTell, out, id, token, phase, target string) (mgrTellReceipt, error) {
	t.Helper()
	var receipt mgrTellReceipt
	if err := mgrTellOutcome(context.Background(), out, []string{id, token, phase, target}, f.backend()); err != nil {
		return receipt, err
	}
	data, err := os.ReadFile(filepath.Join(out, mgrTellPrefix+token+"."+phase+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt, nil
}

func mgrTellState(receipt mgrTellReceipt) string {
	effective := mgrTellEffective(receipt.Views.ConfigGet)
	if effective == nil || !receipt.Views.ConfigDiff.Valid {
		return "unreadable"
	}
	if entry := receipt.Views.ConfigDiff.Entry; entry != nil && entry.OverridePresent && entry.Override != nil {
		return *effective + "/override=" + *entry.Override
	}
	return *effective + "/absent"
}

func TestMgrTellOutcomeRejectsOpenRequestsBeforeNativeWork(t *testing.T) {
	token := strings.Repeat("a", 32)
	for _, fields := range [][]string{nil, {"1"}, {"1", token, "initial"}, {"1", token, "initial", "a", "extra"},
		{"0", token, "initial", "a"}, {"x", token, "initial", "a"}, {"1", "short", "initial", "a"}, {"1", strings.ToUpper(token), "initial", "a"},
		{"1", token, "cleanup", "a"}, {"1", token, "overwritten", "a"}, {"1", token, "initial", "c"}, {"1", token, "initial", "mgr.a"}} {
		f := &fakeMgrTell{active: "a", gid: 4167, overrides: map[string]string{}}
		out := t.TempDir()
		if err := mgrTellOutcome(context.Background(), out, fields, f.backend()); err == nil {
			t.Fatal("accepted an open control request", fields)
		}
		if entries, _ := os.ReadDir(out); len(f.calls) != 0 || f.mapReads != 0 || len(entries) != 0 {
			t.Fatal("a refused request reached the daemon or published files", fields, f.calls, entries)
		}
	}
	// No phase runs without this token's own baseline, and a bystander phase
	// cannot name the baseline's target nor a subject phase the other manager.
	f := &fakeMgrTell{active: "a", gid: 4167, overrides: map[string]string{}}
	out := t.TempDir()
	for _, phase := range []string{"applied", "overwrite", "restore", "bystander-initial"} {
		if _, err := mgrTellRequest(t, f, out, "1", token, phase, "a"); err == nil || len(f.calls) != 0 {
			t.Fatal("a phase ran without a baseline", phase, err, f.calls)
		}
	}
	if _, err := mgrTellRequest(t, f, out, "1", token, "initial", "a"); err != nil {
		t.Fatal(err)
	}
	calls := len(f.calls)
	for _, request := range [][2]string{{"bystander-initial", "a"}, {"overwrite", "b"}, {"restore", "b"}, {"applied", "b"}} {
		if _, err := mgrTellRequest(t, f, out, "2", token, request[0], request[1]); err == nil || len(f.calls) != calls {
			t.Fatal("a phase crossed to the wrong manager", request, err)
		}
	}
	if _, err := mgrTellRequest(t, f, out, "2", strings.Repeat("b", 32), "overwrite", "a"); err == nil || len(f.calls) != calls {
		t.Fatal("another token used this baseline", err)
	}
}

func TestMgrTellOutcomeOverwritesAndRestoresOnceFromItsOwnBaseline(t *testing.T) {
	for _, tc := range []struct {
		name, initial, restoreCall string
		overrides                  map[string]string
	}{
		{"absent override", "10000/absent", "a:config unset log_max_recent", map[string]string{}},
		{"existing override", "12000/override=12000", "a:config set log_max_recent 12000", map[string]string{"a": "12000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := strings.Repeat("c", 32)
			f := &fakeMgrTell{active: "a", gid: 4167, overrides: tc.overrides}
			out := t.TempDir()
			initial, err := mgrTellRequest(t, f, out, "1", token, "initial", "a")
			if err != nil || initial.Baseline == nil || mgrTellState(initial) != tc.initial || initial.Baseline.Present != (len(tc.overrides) != 0) {
				t.Fatal("baseline", err, initial.Baseline, mgrTellState(initial))
			}
			if before := initial.Views.ManagerBefore; !before.OK || before.ActiveName != "a" || before.ActiveGID != 4167 || before.Epoch != 4 || !slices.Equal(before.Standbys, []string{"b"}) {
				t.Fatal("manager bracket", before)
			}
			if second, err := mgrTellRequest(t, f, out, "2", token, "initial", "a"); err == nil {
				t.Fatal("a repeated initial request replaced the baseline", second)
			}
			base := initial.Baseline.Effective
			// The Go client's A is simulated by a direct change of the daemon.
			f.overrides["a"] = map[string]string{"10000": "10001", "12000": "12001"}[base]
			applied, err := mgrTellRequest(t, f, out, "3", token, "applied", "a")
			if err != nil || applied.Mutation != nil || mgrTellState(applied) != f.overrides["a"]+"/override="+f.overrides["a"] {
				t.Fatal("applied", err, mgrTellState(applied))
			}
			wantB := map[string]string{"10000": "10002", "12000": "12002"}[base]
			overwritten, err := mgrTellRequest(t, f, out, "4", token, "overwrite", "a")
			if err != nil || overwritten.Mutation == nil || !overwritten.Mutation.OK || overwritten.Mutation.Action != "overwrite" ||
				!slices.Equal(overwritten.Mutation.Args, []string{"config", "set", "log_max_recent", wantB}) || mgrTellState(overwritten) != wantB+"/override="+wantB {
				t.Fatal("overwrite", err, overwritten.Mutation, mgrTellState(overwritten))
			}
			sets := func() (n int) {
				for _, call := range f.calls {
					if strings.Contains(call, "config set") || strings.Contains(call, "config unset") {
						n++
					}
				}
				return n
			}
			if _, err := mgrTellRequest(t, f, out, "5", token, "overwrite", "a"); err == nil || sets() != 1 {
				t.Fatal("the overwrite was submitted twice", err, sets())
			}
			bystander, err := mgrTellRequest(t, f, out, "6", token, "bystander-final", "b")
			if err != nil || bystander.Mutation != nil || mgrTellState(bystander) != "10000/absent" || bystander.Views.ManagerBefore.ActiveName != "a" {
				t.Fatal("bystander", err, mgrTellState(bystander))
			}
			restored, err := mgrTellRequest(t, f, out, "7", token, "restore", "a")
			if err != nil || restored.Mutation == nil || !restored.Mutation.OK || f.calls[len(f.calls)-3] != tc.restoreCall || mgrTellState(restored) != tc.initial {
				t.Fatal("restore", err, restored.Mutation, f.calls, mgrTellState(restored))
			}
			if _, err := mgrTellRequest(t, f, out, "8", token, "restore", "a"); err == nil || sets() != 2 {
				t.Fatal("the restoration was submitted twice", err, sets())
			}
			fresh, err := mgrTellRequest(t, f, out, "9", token, "restored", "a")
			if err != nil || fresh.Mutation != nil || mgrTellState(fresh) != tc.initial {
				t.Fatal("restored", err, mgrTellState(fresh))
			}
			journal, err := os.ReadFile(filepath.Join(out, "mgr-tell-outcome-native.jsonl"))
			if err != nil || strings.Count(string(journal), "-started-outcome-unknown") != 2 || strings.Contains(string(journal), "ms_bind_ipv4") || strings.Contains(string(journal), "dashboard") {
				t.Fatal("journal lost a claimed mutation or kept unrelated configuration", err)
			}
		})
	}
}

func TestMgrTellOutcomeKeepsAFailedMutationUncertain(t *testing.T) {
	token := strings.Repeat("d", 32)
	f := &fakeMgrTell{active: "a", gid: 4167, overrides: map[string]string{}}
	out := t.TempDir()
	if _, err := mgrTellRequest(t, f, out, "1", token, "initial", "a"); err != nil {
		t.Fatal(err)
	}
	f.failSet = errors.New("exit 124: /run/ceph/ceph-mgr.a.asok timed out")
	receipt, err := mgrTellRequest(t, f, out, "2", token, "overwrite", "a")
	if err != nil || receipt.Mutation == nil || receipt.Mutation.OK || strings.Contains(receipt.Mutation.Error, "/run/") || !strings.Contains(receipt.Mutation.Error, "<path>") {
		t.Fatal("a failed mutation was hidden, retried or leaked a path", err, receipt.Mutation)
	}
	f.failSet = nil
	if _, err := mgrTellRequest(t, f, out, "3", token, "overwrite", "a"); err == nil || len(f.overrides) != 0 {
		t.Fatal("an uncertain overwrite was repeated", err, f.overrides)
	}
}

func TestMgrTellBaselineRefusesUnrestorableStates(t *testing.T) {
	token := strings.Repeat("e", 32)
	for name, f := range map[string]*fakeMgrTell{
		"above the bound":         {active: "a", gid: 4167, overrides: map[string]string{"a": "19999"}},
		"zero":                    {active: "a", gid: 4167, overrides: map[string]string{"a": "0"}},
		"target is the standby":   {active: "b", gid: 4167, overrides: map[string]string{}},
		"manager changed":         {active: "a", gid: 4167, overrides: map[string]string{}, swapAfter: 1},
		"manager map unavailable": {active: "a", gid: 4167, overrides: map[string]string{}, mapErr: errors.New("mgr dump failed")},
	} {
		out := t.TempDir()
		receipt, err := mgrTellRequest(t, f, out, "1", token, "initial", "a")
		if err != nil || receipt.Baseline != nil || fileExists(filepath.Join(out, mgrTellPrefix+token+".baseline.json")) {
			t.Fatal("an unrestorable state became a baseline:", name, err, receipt.Baseline)
		}
		if _, err := mgrTellRequest(t, f, out, "2", token, "overwrite", "a"); err == nil {
			t.Fatal("a mutation ran without a recorded baseline:", name)
		}
	}
	for _, baseline := range []mgrTellBaseline{{Effective: "19999"}, {Effective: "0"}, {Effective: "x"}, {Present: true, Value: "500", Effective: "501"}, {Value: "500", Effective: "500"}} {
		if args, err := mgrTellRestoreArgs(baseline); err == nil {
			t.Fatal("a baseline outside the bound produced a restoration", baseline, args)
		}
	}
	if args, err := mgrTellOverwriteArgs(mgrTellBaseline{Effective: "19998"}); err != nil || args[3] != "20000" {
		t.Fatal("the highest admitted baseline must raise to the ceiling", args, err)
	}
	if _, err := mgrTellOverwriteArgs(mgrTellBaseline{Effective: "19999"}); err == nil {
		t.Fatal("an overwrite above the ceiling was produced")
	}
}

func TestMgrTellOptionDiffProjectsOnlyTheOwnedOption(t *testing.T) {
	for data, want := range map[string]string{
		`{"diff":{}}`: "valid/absent",
		`{"diff":{"ms_bind_ipv4":{"default":true,"final":false}}}`:                        "valid/absent",
		`{"diff":{"log_max_recent":{"default":10000,"override":10001,"final":10001}}}`:    "valid/override=10001/final=10001",
		`{"diff":{"log_max_recent":{"default":"10000","override":"010","final":"10"}}}`:   "valid/override=10/final=10",
		`{"diff":{"log_max_recent":{"default":"10000","file":"500","final":"500"}}}`:      "valid/absent",
		`{"log_max_recent":{"default":"10000","override":"7","final":"7"}}`:               "valid/override=7/final=7",
		`{"diff":{"log_max_recent":{"default":"10000","override":true,"final":"10000"}}}`: "valid/override=unreadable/final=10000",
		`{"diff":{"log_max_recent":"10001"}}`:                                             "invalid",
		`{"diff":[]}`:                                                                     "invalid",
		`{"other":{}}`:                                                                    "invalid",
		`[]`:                                                                              "invalid",
		`not json`:                                                                        "invalid",
	} {
		diff := mgrTellOptionDiff([]byte(data), nil)
		got := "invalid"
		if diff.Valid {
			got = "valid/absent"
			if diff.Entry != nil && diff.Entry.OverridePresent {
				override, final := "unreadable", "none"
				if diff.Entry.Override != nil {
					override = *diff.Entry.Override
				}
				if diff.Entry.Final != nil {
					final = *diff.Entry.Final
				}
				got = "valid/override=" + override + "/final=" + final
			}
		}
		if got != want || !diff.OK {
			t.Fatal("diff projection", data, got, want)
		}
	}
	if diff := mgrTellOptionDiff([]byte(`{"diff":{}}`), errors.New("exit 22")); diff.OK || diff.Valid {
		t.Fatal("a failed diff call established override absence")
	}
}

func mgrTellMutatingCalls(calls []string) (mutating []string) {
	for _, call := range calls {
		if strings.Contains(call, ":config set ") || strings.Contains(call, ":config unset ") {
			mutating = append(mutating, call)
		}
	}
	return mutating
}

// The overwrite and restoration values come from one manager process. When
// that process is replaced under the same name, fails over, or cannot be
// verified, neither mutation is sent: the replacement keeps its own override.
func TestMgrTellMutationsAreBoundToTheBaselineIncarnation(t *testing.T) {
	token := strings.Repeat("e", 32)
	for name, change := range map[string]func(f *fakeMgrTell, out string){
		"replaced under the same name": func(f *fakeMgrTell, _ string) { f.gid = 4999 },
		"failed over to the standby":   func(f *fakeMgrTell, _ string) { f.active = "b" },
		"map read failed":              func(f *fakeMgrTell, _ string) { f.mapErr = errors.New("timeout") },
		"owner record without a GID": func(_ *fakeMgrTell, out string) {
			path := filepath.Join(out, mgrTellPrefix+token+".baseline.json")
			var owner mgrTellOwner
			data, err := os.ReadFile(path)
			if err != nil || json.Unmarshal(data, &owner) != nil || owner.GID != 4167 {
				t.Fatal("the baseline did not record its incarnation", owner, err)
			}
			owner.GID = 0
			data, _ = json.Marshal(owner)
			if err := os.WriteFile(path, data, 0644); err != nil {
				t.Fatal(err)
			}
		},
	} {
		for _, phase := range []string{"overwrite", "restore"} {
			f := &fakeMgrTell{active: "a", gid: 4167, overrides: map[string]string{"a": "12000"}}
			out := t.TempDir()
			if _, err := mgrTellRequest(t, f, out, "1", token, "initial", "a"); err != nil {
				t.Fatal(err)
			}
			// The replacement process has its own override.
			f.overrides["a"] = "15000"
			change(f, out)
			receipt, err := mgrTellRequest(t, f, out, "2", token, phase, "a")
			if err != nil || receipt.Mutation == nil || !receipt.Mutation.Refused || receipt.Mutation.OK || receipt.Mutation.SameIncarnationAfter ||
				receipt.Mutation.Error != mgrTellRefusal || receipt.Mutation.Action != phase {
				t.Fatal("a mutation for a stale or unverifiable owner was not refused:", name, phase, receipt.Mutation, err)
			}
			if sent := mgrTellMutatingCalls(f.calls); len(sent) != 0 || f.overrides["a"] != "15000" {
				t.Fatal("the replacement received a mutating call:", name, phase, sent, f.overrides["a"])
			}
			// A refusal claims nothing: it is not an uncertain mutation.
			if _, err := os.Stat(filepath.Join(out, mgrTellPrefix+token+"."+phase+"-started.json")); err == nil {
				t.Fatal("a refused mutation was claimed:", name, phase)
			}
		}
	}
	// The same incarnation passes the check, and a replacement that happens
	// after the check is reported by the reads after the call.
	for _, restartAfter := range []int{0, 3} {
		f := &fakeMgrTell{active: "a", gid: 4167, overrides: map[string]string{"a": "12000"}, restartAfter: restartAfter}
		out := t.TempDir()
		if _, err := mgrTellRequest(t, f, out, "1", token, "initial", "a"); err != nil {
			t.Fatal(err)
		}
		receipt, err := mgrTellRequest(t, f, out, "2", token, "restore", "a")
		if err != nil || receipt.Mutation == nil || receipt.Mutation.Refused || !receipt.Mutation.OK || receipt.Mutation.SameIncarnationAfter != (restartAfter == 0) ||
			len(mgrTellMutatingCalls(f.calls)) != 1 {
			t.Fatal("the incarnation checks around a sent mutation", restartAfter, receipt.Mutation, err, f.calls)
		}
	}
}
