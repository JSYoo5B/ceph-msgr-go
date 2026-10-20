package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func transitionTestFrozen() transitionFrozen {
	return transitionFrozen{
		Map: selectTransitionMap(gateState("a", 4161, "b", 4170), nil),
		Instances: []transitionInstance{
			{Name: "a", Status: queryComplete, ContainerID: "cid-a", StartedAt: "t0-a", Running: true},
			{Name: "b", Status: queryComplete, ContainerID: "cid-b", StartedAt: "t0-b", Running: true},
		},
	}
}

func transitionTestSample(state transitionMap, beacons []managerSession, instances ...transitionInstance) transitionSample {
	if instances == nil {
		instances = []transitionInstance{
			{Name: "a", Status: queryComplete, ContainerID: "cid-a", StartedAt: "t1-a", Running: true},
			{Name: "b", Status: queryComplete, ContainerID: "cid-b", StartedAt: "t1-b", Running: true},
		}
	}
	return transitionSample{Before: state, After: state, Instances: instances, Monitors: []monitorManagerSessions{
		{Monitor: "a", Status: queryComplete, Beacons: beacons, Modules: []managerSession{{"mgr.b", 4400}}},
		{Monitor: "b", Status: queryComplete, Beacons: []managerSession{}, Modules: []managerSession{}},
		{Monitor: "c", Status: queryComplete, Beacons: []managerSession{}, Modules: []managerSession{}},
	}}
}

func TestClassifyTransitionSampleRequiresNewRegistrationsWithBeacons(t *testing.T) {
	frozen := transitionTestFrozen()
	managers, monitors := []string{"a", "b"}, []string{"a", "b", "c"}
	restarted := selectTransitionMap(gateState("b", 4400, "a", 4410), nil)
	beacons := []managerSession{{"mgr.b", 4400}, {"mgr.a", 4410}}
	accepted := transitionTestSample(restarted, beacons)
	if state, reason := classifyTransitionSample(frozen, managers, monitors, accepted); state != sampleAccepted {
		t.Fatal("restarted MGRs not accepted", state, reason)
	}
	moved := accepted
	moved.After = selectTransitionMap(gateState("b", 4400, "a", 4420), nil)
	stale := selectTransitionMap(gateState("a", 4161, "b", 4170), nil)
	half := selectTransitionMap(gateState("b", 4400, "a", 4161), nil)
	for name, test := range map[string]struct {
		sample transitionSample
		state  string
		reason string
	}{
		// The pre-v0.3.0 waitReady accepted this: available with one standby.
		"stale pre-stop map":     {transitionTestSample(stale, []managerSession{{"mgr.a", 4161}, {"mgr.b", 4170}}), samplePending, "pre-stop GID"},
		"one stale registration": {transitionTestSample(half, []managerSession{{"mgr.b", 4400}, {"mgr.a", 4161}}), samplePending, "mgr.a still holds"},
		"module session only":    {transitionTestSample(restarted, []managerSession{{"mgr.a", 4410}}), samplePending, "beacon of mgr.b"},
		"map moved":              {moved, sampleTransitional, "moved"},
		"no standby yet":         {transitionTestSample(selectTransitionMap(gateState("b", 4400, "", 0), nil), beacons), samplePending, "standbys"},
		"not restarted":          {transitionTestSample(restarted, beacons, transitionInstance{Name: "a", Status: queryComplete, ContainerID: "cid-a", StartedAt: "t0-a", Running: true}, transitionInstance{Name: "b", Status: queryComplete, ContainerID: "cid-b", StartedAt: "t1-b", Running: true}), samplePending, "container of mgr.a"},
		"container replaced":     {transitionTestSample(restarted, beacons, transitionInstance{Name: "a", Status: queryComplete, ContainerID: "other", StartedAt: "t1-a", Running: true}, transitionInstance{Name: "b", Status: queryComplete, ContainerID: "cid-b", StartedAt: "t1-b", Running: true}), sampleViolated, "replaced"},
		"unowned active":         {transitionTestSample(selectTransitionMap(gateState("x", 1, "a", 4410), nil), beacons), sampleViolated, "unowned"},
		"failed map read":        {transitionSample{Before: selectTransitionMap(managerPreparationMap{}, errors.New("exec ceph: exit 1")), After: restarted}, sampleIncomplete, "mgr dump"},
		"failed sessions read": {func() transitionSample {
			s := accepted
			s.Monitors = append([]monitorManagerSessions(nil), s.Monitors...)
			s.Monitors[1] = monitorManagerSessions{Monitor: "b", Status: queryTimedOut}
			return s
		}(), sampleIncomplete, "sessions of mon.b"},
		"missing MON observation":    {func() transitionSample { s := accepted; s.Monitors = s.Monitors[:2]; return s }(), sampleIncomplete, "2 of 3 MONs"},
		"malformed map is no answer": {transitionSample{Before: selectTransitionMap(managerPreparationMap{}, nil), After: restarted}, sampleIncomplete, "mgr dump"},
	} {
		state, reason := classifyTransitionSample(frozen, managers, monitors, test.sample)
		if state != test.state || !strings.Contains(reason, test.reason) {
			t.Errorf("%s: %s %q, want %s containing %q", name, state, reason, test.state, test.reason)
		}
	}
}

func TestAwaitTransitionKeepsBoundedReceipt(t *testing.T) {
	frozen := transitionTestFrozen()
	maps := []managerPreparationMap{gateState("a", 4161, "b", 4170), gateState("a", 4161, "b", 4170), gateState("b", 4400, "a", 4410), gateState("b", 4400, "a", 4410)}
	reads := 0
	backend := transitionBackend{
		managerMap: func(context.Context) (managerPreparationMap, error) {
			state := maps[min(reads, len(maps)-1)]
			reads++
			return state, nil
		},
		sessions: func(context.Context) []monitorManagerSessions {
			return transitionTestSample(transitionMap{}, []managerSession{{"mgr.b", 4400}, {"mgr.a", 4410}}).Monitors
		},
		instance: func(_ context.Context, name string) transitionInstance {
			return transitionInstance{Name: name, Status: queryComplete, ContainerID: "cid-" + name, StartedAt: "t1-" + name, Running: true}
		},
		wait: func(context.Context) error { return nil },
	}
	receipt := transitionReceipt{Managers: []string{"a", "b"}, Monitors: []string{"a", "b", "c"}, Frozen: frozen}
	if err := awaitTransition(context.Background(), backend, &receipt, time.Minute); err != nil {
		t.Fatal(err)
	}
	if receipt.Result != sampleAccepted || receipt.Counts[samplePending] != 1 || receipt.Counts[sampleAccepted] != 1 || len(receipt.Samples) != 2 {
		t.Fatal("receipt", receipt.Result, receipt.Counts, len(receipt.Samples))
	}
	// A deadline without an accepted sample fails with the receipt so far,
	// which keeps only the last few samples.
	reads = 0
	maps = maps[:1]
	receipt = transitionReceipt{Managers: []string{"a", "b"}, Monitors: []string{"a", "b", "c"}, Frozen: frozen}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	backend.wait = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	}
	err := awaitTransition(ctx, backend, &receipt, time.Minute)
	if err == nil || !strings.Contains(receipt.Result, "pre-stop GID") || len(receipt.Samples) > transitionRetained || receipt.Counts[samplePending] < 2 {
		t.Fatal("stale map accepted or receipt unbounded", err, receipt.Result, len(receipt.Samples), receipt.Counts)
	}
	backend.instance = func(_ context.Context, name string) transitionInstance {
		return transitionInstance{Name: name, Status: queryComplete, ContainerID: "replaced", StartedAt: "t1", Running: true}
	}
	receipt = transitionReceipt{Managers: []string{"a", "b"}, Monitors: []string{"a", "b", "c"}, Frozen: frozen}
	if err := awaitTransition(context.Background(), backend, &receipt, time.Minute); err == nil || !strings.HasPrefix(receipt.Result, "violated") || len(receipt.Samples) != 1 {
		t.Fatal("violation did not fail at once", err, receipt.Result)
	}
	if _, err := freezeTransition(context.Background(), transitionBackend{
		managerMap: func(context.Context) (managerPreparationMap, error) { return gateState("a", 1, "b", 2), nil },
		instance: func(_ context.Context, name string) transitionInstance {
			return transitionInstance{Name: name, Status: queryFailed, Error: "inspect failed"}
		},
	}, []string{"a", "b"}); err == nil {
		t.Fatal("baseline without container identities accepted")
	}
}
