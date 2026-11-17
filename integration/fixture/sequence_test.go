package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
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
	if p.testRun != union || p.sequence != sequence || !p.osdTopology().erasureCapacity {
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
