package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func gateSessions(beacons ...managerSession) []monitorManagerSessions {
	return []monitorManagerSessions{
		{Monitor: "a", Status: queryComplete, Beacons: beacons, Modules: []managerSession{{"mgr.a", 4218}, {"mgr.a", 4224}}},
		{Monitor: "b", Status: queryComplete, Beacons: []managerSession{}, Modules: []managerSession{}},
		{Monitor: "c", Status: queryComplete, Beacons: []managerSession{}, Modules: []managerSession{}},
	}
}

func TestShortTicketGateAttributesRefusalsToTheRegisteredSlot(t *testing.T) {
	const line = "2026-10-15T23:16:01.157+0000 7f -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"
	owned := []string{"a", "b"}
	state := gateState("a", 24110, "b", 24120)
	live := gateSessions(managerSession{"mgr.a", 24110}, managerSession{"mgr.b", 24120})
	for _, test := range []struct {
		name     string
		state    managerPreparationMap
		refusals map[string]managerRefusal
		sessions []monitorManagerSessions
		stale    string
		refusal  string
	}{
		{"clean", state, map[string]managerRefusal{"a": {}, "b": {}}, live, "", ""},
		{"other MonClient reclaim only", state, map[string]managerRefusal{"a": {Refused: []uint64{4218}}, "b": {}}, live, "", gateRefusalModuleClient},
		// The pinned v0.2.0 gate excused a Go -13 on this line alone.
		{"module MonClient -13 with live beacon", state, map[string]managerRefusal{"a": {Line: line, Refused: []uint64{4218}}, "b": {}}, live, "", gateRefusalModuleClient},
		{"unattributed -13 with live beacon", state, map[string]managerRefusal{"a": {}, "b": {Line: line}}, live, "", gateRefusalUnattributed},
		{"MgrMap GID reclaim refused", state, map[string]managerRefusal{"a": {Refused: []uint64{24110}}, "b": {}}, live, "mgr.a (" + gateSlotRefused + ")", gateRefusalRegistered},
		{"refused GID among module IDs", state, map[string]managerRefusal{"a": {Line: line, Refused: []uint64{4218, 24110}}, "b": {}}, live, "mgr.a (" + gateSlotRefused + ")", gateRefusalRegistered},
		{"no beacon session", state, map[string]managerRefusal{"a": {}, "b": {}}, gateSessions(managerSession{"mgr.a", 24110}), "mgr.b (" + gateSlotNoBeacon + ")", ""},
		{"beacon with another global ID", state, map[string]managerRefusal{"a": {}, "b": {}}, gateSessions(managerSession{"mgr.a", 24110}, managerSession{"mgr.b", 4300}), "mgr.b (" + gateSlotNoBeacon + ")", ""},
		{"module session with the GID is no beacon", state, map[string]managerRefusal{"a": {}, "b": {}}, []monitorManagerSessions{
			{Monitor: "a", Status: queryComplete, Beacons: []managerSession{{"mgr.a", 24110}}, Modules: []managerSession{{"mgr.b", 24120}}},
		}, "mgr.b (" + gateSlotNoBeacon + ")", ""},
		{"absent from the MgrMap", gateState("a", 24110, "", 0), map[string]managerRefusal{"a": {}, "b": {Refused: []uint64{8}}}, live, "mgr.b (" + gateSlotAbsent + ")", gateRefusalModuleClient},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := evaluateShortTicketGate("pre-dial", test.state, owned, test.refusals, test.sessions)
			if err != nil {
				t.Fatal(err)
			}
			if (receipt.FixtureError != "") != (test.stale != "") || !strings.Contains(receipt.FixtureError, test.stale) {
				t.Fatalf("gate %q, want stale %q", receipt.FixtureError, test.stale)
			}
			if test.stale != "" && !strings.HasPrefix(receipt.FixtureError, "fixture error: ") {
				t.Fatal("stale native MGR must be reported as a fixture error", receipt.FixtureError)
			}
			if len(receipt.Daemons) != 2 || receipt.Daemons[0].GID != 24110 || receipt.Epoch != 9 || len(receipt.Sessions) != len(test.sessions) {
				t.Fatal("gate receipt lost MgrMap identities or sessions", receipt)
			}
			attributed := receipt.Daemons[0].Refusal
			if test.refusals["b"].Line != "" || len(test.refusals["b"].Refused) != 0 {
				attributed = receipt.Daemons[1].Refusal
			}
			if attributed != test.refusal {
				t.Fatalf("refusal attribution %q, want %q", attributed, test.refusal)
			}
		})
	}
	receipt, _ := evaluateShortTicketGate("pre-dial", state, owned, map[string]managerRefusal{"a": {}, "b": {}}, live)
	if got := receipt.Daemons[0]; got.Slot != gateSlotRegistered || len(got.BeaconMonitors) != 1 || got.BeaconMonitors[0] != "a" || len(got.ModuleGlobalIDs) != 2 {
		t.Fatal("registered slot lost its beacon or module sessions", got)
	}
	for name, call := range map[string]func() error{
		"missing refusal observation": func() error {
			_, err := evaluateShortTicketGate("pre-dial", state, owned, map[string]managerRefusal{"a": {}}, live)
			return err
		},
		"unknown label": func() error {
			_, err := evaluateShortTicketGate("later", state, owned, map[string]managerRefusal{"a": {}, "b": {}}, live)
			return err
		},
		"outside MgrMap identity": func() error {
			_, err := evaluateShortTicketGate("pre-dial", gateState("c", 1, "", 0), owned, map[string]managerRefusal{"a": {}, "b": {}}, live)
			return err
		},
		"no session observation": func() error {
			_, err := evaluateShortTicketGate("pre-dial", state, owned, map[string]managerRefusal{"a": {}, "b": {}}, nil)
			return err
		},
		"incomplete session observation": func() error {
			sessions := gateSessions(managerSession{"mgr.a", 24110}, managerSession{"mgr.b", 24120})
			sessions[2] = monitorManagerSessions{Monitor: "c", Status: queryTimedOut}
			_, err := evaluateShortTicketGate("pre-dial", state, owned, map[string]managerRefusal{"a": {}, "b": {}}, sessions)
			return err
		},
	} {
		if call() == nil {
			t.Fatal(name, "accepted as a clean observation")
		}
	}
	receipt, err := evaluateShortTicketGate("post-failure", state, owned, map[string]managerRefusal{"a": {}, "b": {}}, live)
	data, _ := json.Marshal(receipt)
	if err != nil || !strings.Contains(string(data), `"label":"post-failure"`) || !strings.Contains(string(data), `"monitor_sessions"`) {
		t.Fatal("gate receipt does not carry the label and sessions the test checks", string(data), err)
	}
}

func TestDecodeManagerSessionsSplitsBeaconsAndModules(t *testing.T) {
	data := []byte(`[{"entity_name":"mgr.a","con_type":"mgr","global_id":4161},{"entity_name":"mgr.a","con_type":"client","global_id":4179},` +
		`{"entity_name":"client.admin","con_type":"client","global_id":4500},{"entity_name":"mgr.b","con_type":"mgr","global_id":4170}]`)
	observed, err := decodeManagerSessions("b", data)
	if err != nil || !observed.complete() || len(observed.Beacons) != 2 || len(observed.Modules) != 1 || observed.Modules[0] != (managerSession{"mgr.a", 4179}) {
		t.Fatal("sessions split", observed, err)
	}
	if got := beaconMonitors([]monitorManagerSessions{observed}, "a", 4161); len(got) != 1 || got[0] != "b" {
		t.Fatal("beacon source", got)
	}
	if beaconMonitors([]monitorManagerSessions{observed}, "a", 4179) != nil || beaconMonitors([]monitorManagerSessions{observed}, "a", 0) != nil {
		t.Fatal("module client or zero GID counted as a beacon")
	}
	if empty, err := decodeManagerSessions("c", []byte("[]")); err != nil || !empty.complete() || len(empty.Beacons) != 0 {
		t.Fatal("an empty session list is an answer", empty, err)
	}
	for _, bad := range []string{"", "null", "{}", `[{"entity_name":"mgr.a","con_type":"mgr"}]`, `[{"entity_name":"mgr.a"`} {
		if _, err := decodeManagerSessions("a", []byte(bad)); err == nil {
			t.Fatalf("%q decoded as an observation", bad)
		}
	}
}
