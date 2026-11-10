package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestObserveNativeMgrTellRejectsOpenRequestsBeforePublishing(t *testing.T) {
	control := t.TempDir()
	token := strings.Repeat("a", 32)
	for _, request := range [][3]string{
		{"short", "initial", "a"}, {strings.ToUpper(token), "initial", "a"}, {token + " x", "initial", "a"},
		{token, "cleanup", "a"}, {token, "overwritten", "a"}, {token, "", "a"}, {token, "initial", "mgr.a"}, {token, "initial", "c"},
		{token, "initial", "a extra"}, {token, "overwrite 10002", "a"}, {token, "initial", ""},
	} {
		if _, err := observeNativeMgrTell(context.Background(), control, request[0], request[1], request[2]); err == nil {
			t.Fatal("open request was delegated", request)
		}
	}
	if entries, err := os.ReadDir(control); err != nil || len(entries) != 0 {
		t.Fatal("rejected requests published control files", entries, err)
	}
}

// Receipt shape published by integration/fixture/mgr_tell_outcome.go for an
// absent override, effective value 500 and one bracketing active manager.
const nativeMgrTellReceiptSample = `{"request_id":3,"token":"cccccccccccccccccccccccccccccccc","phase":"applied","target":"a","baseline":{"present":false,"value":"","effective":"500"},"views":{"manager_before":{"ok":true,"error":"","available":true,"active_name":"a","active_gid":4167,"epoch":4,"standbys":["b"]},"config_get":{"ok":true,"error":"","data":"eyJsb2dfbWF4X3JlY2VudCI6IjUwMCJ9"},"config_diff":{"ok":true,"error":"","data_bytes":12,"valid":true,"entry":null,"top_level_keys":["diff"],"diff_entries":3},"manager_after":{"ok":true,"error":"","available":true,"active_name":"a","active_gid":4167,"epoch":4,"standbys":["b"]}}}`

func TestNativeMgrTellReceiptBindsStateToTheNamedActiveDaemon(t *testing.T) {
	var receipt nativeMgrTellReceipt
	if err := json.Unmarshal([]byte(nativeMgrTellReceiptSample), &receipt); err != nil {
		t.Fatal(err)
	}
	daemon := nativeMgrTellDaemon{"a", 4167}
	absent := nativeMonTellState{value: 500}
	if !nativeMgrTellMatches(receipt, daemon, absent) {
		t.Fatal("absent override with effective 500 on the bracketed daemon was not recognized")
	}
	override := int64(500)
	// The same effective value does not establish an override, and a lost
	// application (another value) never matches.
	if nativeMgrTellMatches(receipt, daemon, nativeMonTellState{500, &override}) || nativeMgrTellMatches(receipt, daemon, nativeMonTellState{value: 501}) {
		t.Fatal("effective value alone was accepted as an override, or another value matched")
	}
	// Wrong target: another name, the same name under another GID, or a
	// receipt taken from another daemon's admin socket.
	if nativeMgrTellMatches(receipt, nativeMgrTellDaemon{"b", 4167}, absent) || nativeMgrTellMatches(receipt, nativeMgrTellDaemon{"a", 4168}, absent) {
		t.Fatal("another manager name or GID matched")
	}
	other := receipt
	other.Target = "b"
	if nativeMgrTellMatches(other, daemon, absent) {
		t.Fatal("state read from another daemon's admin socket matched the named daemon")
	}
	text := "500"
	present := receipt
	present.Views.ConfigDiff.Entry = &nativeMonTellEntry{OverridePresent: true, Override: &text}
	if !nativeMgrTellMatches(present, daemon, nativeMonTellState{500, &override}) || nativeMgrTellMatches(present, daemon, absent) {
		t.Fatal("override presence was not compared exactly")
	}
	replayed := int64(501)
	if nativeMgrTellMatches(present, daemon, nativeMonTellState{500, &replayed}) {
		t.Fatal("a different override value matched")
	}
	for name, change := range map[string]func(*nativeMgrTellViews){
		"failed get":         func(v *nativeMgrTellViews) { v.ConfigGet.OK = false },
		"failed diff":        func(v *nativeMgrTellViews) { v.ConfigDiff.OK = false },
		"unparsed diff":      func(v *nativeMgrTellViews) { v.ConfigDiff.Valid = false },
		"failed map read":    func(v *nativeMgrTellViews) { v.ManagerAfter.OK = false },
		"unavailable before": func(v *nativeMgrTellViews) { v.ManagerBefore.Available = false },
		"gid changed":        func(v *nativeMgrTellViews) { v.ManagerAfter.ActiveGID = 4168 },
		"name changed":       func(v *nativeMgrTellViews) { v.ManagerAfter.ActiveName = "b" },
		"override unreadable": func(v *nativeMgrTellViews) {
			v.ConfigDiff.Entry = &nativeMonTellEntry{OverridePresent: true}
		},
	} {
		changed := receipt
		change(&changed.Views)
		if nativeMgrTellMatches(changed, daemon, absent) {
			t.Fatal("an unbracketed or unreadable observation established the state:", name)
		}
	}
	// An epoch-only map change keeps the same daemon identity.
	advanced := receipt
	advanced.Views.ManagerAfter.Epoch = 5
	if !nativeMgrTellMatches(advanced, daemon, absent) {
		t.Fatal("an epoch-only MgrMap change broke the daemon bracket")
	}
}
