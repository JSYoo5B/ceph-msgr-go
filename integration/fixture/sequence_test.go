package main

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"
)

func fourOSDEnvironment(change map[string]string) func(string) string {
	env := map[string]string{
		"CEPH_MSGR_TEST_KEY_TYPE": "aes256k", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes256k", "CEPH_MSGR_TEST_IP_FAMILY": "4",
		"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "4",
		"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8", "CEPH_MSGR_TEST_CAPABILITIES": "osd-erasure-width",
	}
	for key, value := range change {
		env[key] = value
	}
	return func(name string) string { return env[name] }
}

const (
	sequenceOptimized   = "^TestCephOSDErasureOptimizedK2M2Integration$"
	sequenceCalibration = "^TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration$"
)

func TestSequenceLoadsTheOneProfileOfItsMembers(t *testing.T) {
	sequence := sequenceOptimized + " " + sequenceCalibration
	union := sequenceOptimized + "|" + sequenceCalibration
	p, err := loadProfile(fourOSDEnvironment(map[string]string{sequenceEnv: sequence, "CEPH_MSGR_TEST_RUN": union}))
	if err != nil {
		t.Fatal(err)
	}
	single, err := loadProfile(fourOSDEnvironment(map[string]string{"CEPH_MSGR_TEST_RUN": sequenceOptimized}))
	if err != nil {
		t.Fatal(err)
	}
	if p.testRun != union || p.sequence != sequence || p.osdTopology().erasureCapacity == "" {
		t.Fatalf("sequence profile %+v", p)
	}
	single.testRun, single.sequence = p.testRun, p.sequence
	if p != single {
		t.Fatalf("sequence profile differs from its first member's:\n%+v\n%+v", p, single)
	}
	for name, change := range map[string]map[string]string{
		"one member":       {sequenceEnv: sequenceOptimized, "CEPH_MSGR_TEST_RUN": sequenceOptimized},
		"repeated member":  {sequenceEnv: sequenceOptimized + " " + sequenceOptimized, "CEPH_MSGR_TEST_RUN": sequenceOptimized + "|" + sequenceOptimized},
		"run is not union": {sequenceEnv: sequence, "CEPH_MSGR_TEST_RUN": sequenceOptimized},
		"run is empty":     {sequenceEnv: sequence, "CEPH_MSGR_TEST_RUN": ""},
		"subtest member":   {sequenceEnv: sequenceOptimized + " ^TestA$/b", "CEPH_MSGR_TEST_RUN": sequenceOptimized + "|^TestA$/b"},
		"invalid member":   {sequenceEnv: sequenceOptimized + " ^TestA($", "CEPH_MSGR_TEST_RUN": sequenceOptimized + "|^TestA($"},
		"broad member":     {sequenceEnv: sequenceOptimized + " ^TestCephOSD.*$", "CEPH_MSGR_TEST_RUN": sequenceOptimized + "|^TestCephOSD.*$"},
		"default member":   {sequenceEnv: "^TestCephOSDErasureK2M2Integration$ " + sequenceOptimized, "CEPH_MSGR_TEST_RUN": "^TestCephOSDErasureK2M2Integration$|" + sequenceOptimized},
		"refused profile":  {sequenceEnv: sequence, "CEPH_MSGR_TEST_RUN": union, "CEPH_MSGR_TEST_OSD_STORE": "memstore"},
		"no capability":    {sequenceEnv: sequence, "CEPH_MSGR_TEST_RUN": union, "CEPH_MSGR_TEST_CAPABILITIES": ""},
	} {
		if p, err := loadProfile(fourOSDEnvironment(change)); err == nil || p != (profile{}) {
			t.Errorf("%s: sequence accepted: %+v", name, p)
		}
	}
	// Members whose own profiles differ (here by the capability-admitted
	// fixture mode) cannot share one fixture.
	broad := func(change map[string]string) func(string) string {
		env := map[string]string{"CEPH_MSGR_TEST_KEY_TYPE": "aes", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes", "CEPH_MSGR_TEST_IP_FAMILY": "4",
			"CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc"}
		for key, value := range change {
			env[key] = value
		}
		return func(name string) string { return env[name] }
	}
	ok := "^TestCephAppliedNamedManagerTellIsNotReplayedIntegration$ ^TestCephSentTellCancellationIntegration$"
	if _, err := loadProfile(broad(map[string]string{sequenceEnv: ok, "CEPH_MSGR_TEST_RUN": strings.ReplaceAll(ok, " ", "|")})); err != nil {
		t.Fatal("ordinary fixture sequence refused", err)
	}
}

// A capability's mode is read at run time through admits; a sequence of
// members that each declare it must get the mode and the test environment
// each member gets alone.
func TestSequenceKeepsTheCapabilityModesOfItsMembers(t *testing.T) {
	const connected, unprocessed = "^TestCephMonMapVersionConnectedKillIntegration$", "^TestCephMonMapVersionUnprocessedKillIntegration$"
	env := func(change map[string]string) func(string) string {
		values := map[string]string{"CEPH_MSGR_TEST_KEY_TYPE": "aes", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes", "CEPH_MSGR_TEST_IP_FAMILY": "4",
			"CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_OSD": "1",
			"CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6",
			"CEPH_MSGR_TEST_CAPABILITIES": "mon-forced-kill"}
		maps.Copy(values, change)
		return func(name string) string { return values[name] }
	}
	alone, err := loadProfile(env(map[string]string{"CEPH_MSGR_TEST_RUN": connected}))
	if err != nil || !alone.monForcedKillSelected() {
		t.Fatal("the member alone", err)
	}
	p, err := loadProfile(env(map[string]string{sequenceEnv: connected + " " + unprocessed, "CEPH_MSGR_TEST_RUN": connected + "|" + unprocessed}))
	if err != nil {
		t.Fatal(err)
	}
	if !p.monForcedKillSelected() || !maps.Equal(p.testEnvironment("/out", ""), alone.testEnvironment("/out", "")) {
		t.Fatal("the sequence lost its members' capability mode", p.testEnvironment("/out", ""))
	}
	a, s := fixtureTicketTTLsPair(alone), fixtureTicketTTLsPair(p)
	if a != s {
		t.Fatal("ticket lifetimes differ", a, s)
	}
	// Without the capability the same sequence gets no mode either.
	plain := profile{sequence: connected + " " + unprocessed, testRun: connected + "|" + unprocessed}
	if plain.declares(capMONForcedKill) {
		t.Fatal("an undeclared capability was declared")
	}
	broad := profile{sequence: connected + " ^TestCephMonMap.*$", capabilities: "mon-forced-kill"}
	if broad.declares(capMONForcedKill) {
		t.Fatal("a sequence with a non-exact member declares a capability")
	}
}

func fixtureTicketTTLsPair(p profile) [2]time.Duration {
	auth, service := fixtureTicketTTLs(p)
	return [2]time.Duration{auth, service}
}

type fakeSequence struct {
	listing map[string]string // suite+" "+selector -> listing
	failRun map[string]bool   // suite/test
	checks  []bool            // results of the successive checks
	events  []string
	record  sequenceRecord
	out     bytes.Buffer
}

func (f *fakeSequence) steps() sequenceSteps {
	return sequenceSteps{
		alive: func() error { return nil },
		list: func(_ context.Context, suite, selector string) (string, error) {
			f.events = append(f.events, "list "+suite+" "+selector)
			return f.listing[suite+" "+selector], nil
		},
		run: func(_ context.Context, suite, test string) error {
			f.events = append(f.events, "run "+suite+"/"+test)
			if f.failRun[suite+"/"+test] {
				return errors.New("exit status 1")
			}
			return nil
		},
		check: func(context.Context) (startStateReport, error) {
			f.events = append(f.events, "check")
			passed := len(f.checks) == 0 || f.checks[0]
			if len(f.checks) != 0 {
				f.checks = f.checks[1:]
			}
			if passed {
				return startStateReport{Passed: true, Polls: 1}, nil
			}
			return startStateReport{Failures: []startStateFailure{{Condition: "config", Detail: "mon/mon_allow_pool_delete set, absent at start"}}}, nil
		},
		record: func(r sequenceRecord) error { f.record = r; return nil },
		stdout: &f.out,
	}
}

func TestSequenceRunsEachTestAloneInOrderWithACheckAfterEach(t *testing.T) {
	f := &fakeSequence{listing: map[string]string{
		"api ^TestB$|^TestA$":    "TestB\nTestA\n",
		"client ^TestB$|^TestA$": "",
		"api ^TestA$|^TestC$":    "TestA\nok  \tsome noise\n",
		"client ^TestA$|^TestC$": "TestC\n",
	}}
	p := profile{sequence: "^TestB$|^TestA$ ^TestA$|^TestC$"}
	if err := runSequence(context.Background(), p, f.steps()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"list api ^TestB$|^TestA$", "run api/TestB", "check", "run api/TestA",
		"list client ^TestB$|^TestA$",
		"list api ^TestA$|^TestC$",
		"list client ^TestA$|^TestC$", "check", "run client/TestC",
		"check",
	}
	if strings.Join(f.events, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events:\n%s", strings.Join(f.events, "\n"))
	}
	if len(f.record.Tests) != 3 || f.record.Stopped != "" || f.record.Tests[2].StartStateAfter == nil || !f.record.Tests[2].StartStateAfter.Passed {
		t.Fatalf("record %+v", f.record)
	}
	// Every test is announced under its suite, as the result gate reads it.
	if got := strings.Count(f.out.String(), "Running Ceph test suite: api\n"); got != 2 {
		t.Fatal("suite lines", f.out.String())
	}
}

func TestSequenceStopsAtAFailedCheckAndNamesThePreviousTest(t *testing.T) {
	f := &fakeSequence{listing: map[string]string{"api ^TestA$": "TestA\n", "api ^TestB$": "TestB\n"}, checks: []bool{false}}
	err := runSequence(context.Background(), profile{sequence: "^TestA$ ^TestB$"}, f.steps())
	if err == nil || !strings.Contains(err.Error(), "after api/TestA") {
		t.Fatal("failed check not attributed", err)
	}
	if strings.Contains(strings.Join(f.events, " "), "TestB") && strings.Contains(strings.Join(f.events, " "), "run api/TestB") {
		t.Fatal("the sequence went on after a failed check", f.events)
	}
	line := "Fixture start state after api/TestA: FAILED: config: mon/mon_allow_pool_delete set, absent at start\n"
	if !strings.Contains(f.out.String(), line) {
		t.Fatal("report line", f.out.String())
	}
	if f.record.Stopped == "" || len(f.record.Tests) != 1 || f.record.Tests[0].StartStateAfter.Passed {
		t.Fatalf("record %+v", f.record)
	}
	// The check after the last test is required as well.
	last := &fakeSequence{listing: map[string]string{"api ^TestA$": "TestA\n", "api ^TestB$": "TestB\n"}, checks: []bool{true, false}}
	if err := runSequence(context.Background(), profile{sequence: "^TestA$ ^TestB$"}, last.steps()); err == nil || !strings.Contains(err.Error(), "after api/TestB") {
		t.Fatal("failed final check not attributed", err)
	}
}

func TestSequenceStopsAtAFailedTest(t *testing.T) {
	f := &fakeSequence{listing: map[string]string{"api ^TestA$": "TestA\n", "api ^TestB$": "TestB\n"}, failRun: map[string]bool{"api/TestA": true}}
	err := runSequence(context.Background(), profile{sequence: "^TestA$ ^TestB$"}, f.steps())
	if err == nil || !strings.Contains(err.Error(), "api/TestA failed") {
		t.Fatal(err)
	}
	if strings.Join(f.events, " ") != "list api ^TestA$ run api/TestA" {
		t.Fatal("the sequence went on after a failed test", f.events)
	}
	if len(f.record.Tests) != 1 || f.record.Tests[0].Passed || f.record.Stopped == "" {
		t.Fatalf("record %+v", f.record)
	}
}

func TestSequenceThatSelectsNothingFails(t *testing.T) {
	f := &fakeSequence{listing: map[string]string{}}
	if err := runSequence(context.Background(), profile{sequence: "^TestA$ ^TestB$"}, f.steps()); err == nil {
		t.Fatal("an empty sequence passed")
	}
}

// validate passes the planner's sequence jobs through the fixture; a job the
// fixture would refuse at run time is reported before any run.
func TestPlannedSequenceJobsAreAdmittedOrReported(t *testing.T) {
	job := map[string]string{"scenario": "osd.a", "scenarios": "osd.a osd.b", "sequence_rows": "2",
		"key": "aes256k", "service": "aes256k", "family": "4", "stress": "", "runtime": "container", "osd": "1",
		"osd_store": "bluestore", "osd_replicas": "4", "osd_profile": "replicated-pg-v8", "capabilities": "osd-erasure-width",
		"test_run": sequenceOptimized + "|" + sequenceCalibration, "test_sequence": sequenceOptimized + " " + sequenceCalibration}
	if problems := plannedJobAdmissionProblems([]map[string]string{job}); len(problems) != 0 {
		t.Fatal(problems)
	}
	refused := maps.Clone(job)
	refused["test_sequence"] = "^TestCephOSDErasureK2M2Integration$ " + sequenceOptimized
	refused["test_run"] = "^TestCephOSDErasureK2M2Integration$|" + sequenceOptimized
	problems := plannedJobAdmissionProblems([]map[string]string{refused})
	if len(problems) != 1 || !strings.Contains(problems[0], "planned sequence osd.a osd.b: the fixture refuses it") {
		t.Fatal(problems)
	}
}
