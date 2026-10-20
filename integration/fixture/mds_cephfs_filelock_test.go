package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMDSCephFSFlockActionsAreClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, name := range []string{"flock-ex-nb", "flock-sh-nb"} {
		if id, action, err := parseMDSCephFSRequest([]string{"10", token, name, "go-file"}, token, 9); err != nil || id != 10 || action.name != name || action.target != "go-file" {
			t.Fatal("owned fixed flock action refused", action, err)
		}
		for _, args := range [][]string{{}, {"native-0"}, {"dir"}, {"/go-file"}, {"go-file", "owner"}} {
			fields := append([]string{"10", token, name}, args...)
			if _, _, err := parseMDSCephFSRequest(fields, token, 9); err == nil {
				t.Fatal("caller-selected flock target or argument accepted", fields)
			}
		}
	}
	if _, _, err := parseMDSCephFSRequest([]string{"10", token, "flock-block", "go-file"}, token, 9); err == nil {
		t.Fatal("unimplemented blocking native action admitted")
	}
}

func TestMDSCephFSFlockReceiptRejectsUnreleasedOrChangedState(t *testing.T) {
	plan := mdsCephFSPlanFor(strings.Repeat("a1", 16))
	path, _ := plan.path("go-file")
	state := mdsCephFSFlockState{Ino: 7, Version: 2, MTime: 10}
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
			_, err = decodeMDSCephFSNative("flock-ex-nb", path, plan, data)
			return err
		}
		if err := check(fresh()); err != nil {
			t.Fatal("valid nonblocking receipt refused", blocked, err)
		}
		for _, mutate := range []func(*mdsCephFSNative){
			func(n *mdsCephFSNative) { n.Flock.Owner++ },
			func(n *mdsCephFSNative) { n.Flock.Unlocked = !n.Flock.Unlocked },
			func(n *mdsCephFSNative) { n.Flock.Errno = 13 },
			func(n *mdsCephFSNative) { n.Flock.After.Version++ },
			func(n *mdsCephFSNative) { n.Flock.After.MTime++ },
			func(n *mdsCephFSNative) { n.Flock.Before.Size, n.Flock.After.Size = 65, 65 },
			func(n *mdsCephFSNative) { n.Flock.Before.DataHex, n.Flock.After.DataHex = "00", "00" },
			func(n *mdsCephFSNative) { n.Stat = &mdsCephFSStat{} },
		} {
			n := fresh()
			mutate(&n)
			if err := check(n); err == nil {
				t.Fatal("ambiguous or changed flock receipt accepted", n)
			}
		}
	}
	wrongAction, _ := json.Marshal(mdsCephFSNative{Action: "stat", Path: path, NativeGlobalID: 9,
		Stat: &mdsCephFSStat{Exists: true, Ino: 7, Mode: 0o100644}, Flock: &mdsCephFSFlockProbe{}})
	if _, err := decodeMDSCephFSNativeOutput("stat", path, wrongAction); err == nil {
		t.Fatal("adding the flock receipt broadened other native action schemas")
	}
}
