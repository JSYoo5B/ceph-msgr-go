package main

import "testing"

// The owned CHARMAP requirement is a charmap-run action only, and its guards
// read the native images exactly: a session-dump feature image declares bit
// 22 only in its least significant block, and the map must require charmap
// alone (or nothing) to count as settled.
func TestMDSCephFSCharmapRequirementIsOwnedAndExact(t *testing.T) {
	charmap := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSCharmapIntegration$", capabilities: capMDSCharmap.name}
	other := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSCompletionIntegration$"}
	for _, action := range []string{"charmap-require-on", "charmap-require-off"} {
		if err := admitMDSCephFSCharmap(action, charmap); err != nil {
			t.Fatal("charmap requirement refused under its capability", action, err)
		}
		if err := admitMDSCephFSCharmap(action, other); err == nil {
			t.Fatal("charmap requirement admitted without its capability", action)
		}
	}
	for printed, want := range map[string]bool{
		"0x00000000005cb7ff":                 true,  // the Go opt-in
		"0x0000000000ffffff":                 true,  // the stock client
		"0x00000000001cb7ff":                 false, // the default CephFS profile
		"0x00000000000005ff":                 false, // the session-only profile
		"0x00000000004000000000000000000000": false, // bit 86, not 22
		"0x000000000000000100000000005cb7ff": true,
		"0x5cb7ff":                           false,
		"00000000005cb7ff":                   false,
		"0x":                                 false,
		"0x00000000005cb7fg":                 false,
	} {
		if got := mdsCephFSCharmapDeclares(printed); got != want {
			t.Fatal("charmap declaration of a native feature image differs", printed, got)
		}
	}
	for _, tc := range []struct {
		required map[string]string
		on, off  bool
	}{
		{nil, false, true},
		{map[string]string{}, false, true},
		{map[string]string{"feature_22": "charmap"}, true, false},
		{map[string]string{"feature_22": "charmap", "feature_9": "reply_encoding"}, false, false},
		{map[string]string{"feature_22": "blockdiff"}, false, false},
		{map[string]string{"feature_23": "charmap"}, false, false},
	} {
		m := mdsSessionsMap{Required: tc.required}
		if mdsCephFSCharmapRequiresOnly(m, true) != tc.on || mdsCephFSCharmapRequiresOnly(m, false) != tc.off {
			t.Fatal("charmap requirement settle check differs", tc.required)
		}
	}
}
