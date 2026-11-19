package main

import (
	"strings"
	"testing"
	"time"
)

// The two native tests that declare mon-forced-kill (their //scenario:row
// declarations).
const (
	monConnectedKillSelector   = "^TestCephMonMapVersionConnectedKillIntegration$"
	monUnprocessedKillSelector = "^TestCephMonMapVersionUnprocessedKillIntegration$"
)

func monForcedKillEnv(overrides map[string]string) map[string]string {
	values := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1",
		"CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_RUN": monConnectedKillSelector,
		"CEPH_MSGR_TEST_CAPABILITIES": "mon-forced-kill"}
	for key, value := range overrides {
		values[key] = value
	}
	return values
}

func monForcedKillProfile(t *testing.T, overrides map[string]string) profile {
	t.Helper()
	values := monForcedKillEnv(overrides)
	p, err := loadProfile(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// SIGKILL of a MON is admitted only for one exact test that declares
// mon-forced-kill, on the single-BlueStore legacy-v6 fixture. Broad OSD
// selections, which also match those test names, and either cell without
// the declaration keep the kill control restricted to OSDs.
func TestMonForcedKillIsAdmittedOnlyForItsExclusiveSelection(t *testing.T) {
	for _, selector := range []string{monConnectedKillSelector, monUnprocessedKillSelector} {
		selected := monForcedKillProfile(t, map[string]string{"CEPH_MSGR_TEST_RUN": selector})
		if !selected.monForcedKillSelected() || !selected.forcedKillAdmitted("mon") || !selected.forcedKillAdmitted("osd") || selected.forcedKillAdmitted("mgr") {
			t.Fatal("selected profile admission", selected.testRun)
		}
	}
	for name, overrides := range map[string]map[string]string{
		"three replicas": {"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		"memstore":       {"CEPH_MSGR_TEST_OSD_STORE": "memstore"},
		"pg-v8":          {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
		"both cells":     {"CEPH_MSGR_TEST_RUN": "^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$"},
		"wider":          {"CEPH_MSGR_TEST_RUN": "^TestCephMonMapVersion.*Integration$"},
		"broad":          {"CEPH_MSGR_TEST_RUN": ""},
	} {
		values := monForcedKillEnv(overrides)
		if p, err := loadProfile(func(key string) string { return values[key] }); err == nil {
			t.Fatal("MON forced kill capability accepted outside its topology or one exact test", name, p.testRun)
		}
	}
	for name, overrides := range map[string]map[string]string{
		"broad":            {"CEPH_MSGR_TEST_RUN": "", "CEPH_MSGR_TEST_CAPABILITIES": ""},
		"undeclared cell":  {"CEPH_MSGR_TEST_RUN": monUnprocessedKillSelector, "CEPH_MSGR_TEST_CAPABILITIES": ""},
		"undeclared pair":  {"CEPH_MSGR_TEST_RUN": "^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$", "CEPH_MSGR_TEST_CAPABILITIES": ""},
		"wider":            {"CEPH_MSGR_TEST_RUN": "^TestCephMonMapVersion.*Integration$", "CEPH_MSGR_TEST_CAPABILITIES": ""},
		"three replicas":   {"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_CAPABILITIES": ""},
		"other capability": {"CEPH_MSGR_TEST_CAPABILITIES": "osd-copy-from-late-error"},
	} {
		p := monForcedKillProfile(t, overrides)
		if p.monForcedKillSelected() || p.forcedKillAdmitted("mon") || !p.forcedKillAdmitted("osd") || p.forcedKillAdmitted("mgr") {
			t.Fatal("MON forced kill admitted outside its exclusive selection", name)
		}
		if p.testEnvironment("/out", "")[monForcedKillMarker] != "" {
			t.Fatal("marker reached an unselected suite", name)
		}
		if auth, service := fixtureTicketTTLs(p); auth != 12*time.Second || service != 12*time.Second {
			t.Fatal("unselected profile lost the ordinary ticket validity", name, auth, service)
		}
	}
	if (profile{testRun: monConnectedKillSelector, capabilities: "mon-forced-kill"}).forcedKillAdmitted("mon") {
		t.Fatal("MON forced kill admitted without the OSD fixture")
	}
}

// The marker comes from the selection, never from an inherited variable, and
// stays out of the daemon environment. The selected fixture also keeps AUTH
// and service proofs valid across the window in which no MON is in quorum.
func TestMonForcedKillMarkerAndTicketValidityFollowSelection(t *testing.T) {
	t.Setenv(monForcedKillMarker, "1")
	selected := monForcedKillProfile(t, map[string]string{monForcedKillMarker: "1"})
	broad := monForcedKillProfile(t, map[string]string{monForcedKillMarker: "1", "CEPH_MSGR_TEST_RUN": "", "CEPH_MSGR_TEST_CAPABILITIES": ""})
	for _, test := range []struct {
		p    profile
		want string
	}{{selected, "1"}, {broad, ""}} {
		inputs := test.p.testEnvironment("/out", "")
		count := 0
		for _, field := range environment(inputs, true) {
			if strings.HasPrefix(field, monForcedKillMarker+"=") {
				count++
				if field != monForcedKillMarker+"="+test.want {
					t.Fatal("inherited marker decided the admission", field)
				}
			}
		}
		if count != 1 || inputs[monForcedKillMarker] != test.want {
			t.Fatal("marker absent, duplicated or wrong", count, inputs[monForcedKillMarker])
		}
		if _, leaked := test.p.containerEnvironment()[monForcedKillMarker]; leaked {
			t.Fatal("marker leaked to the daemon environment")
		}
	}
	if auth, service := fixtureTicketTTLs(selected); auth != 120*time.Second || service != 120*time.Second {
		t.Fatal("selected profile ticket validity", auth, service)
	}
}
