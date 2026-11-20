package main

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// recordedControlOrder is the polling order of the fixture's controls when
// the hand-written sequence in controls became declarations. Each entry names
// the method a poll calls, the request file of one acknowledged request that a
// grouped method used to poll, or the worker failure checkpoint by its file
// name. It is frozen: a control added later never joins it, because it takes
// no rank (controlAfterRanked).
var recordedControlOrder = []string{
	"controlCopyFromErrorRequest",
	"controlCachePoolsRequest",
	"native-oracles.error",
	"controlNamedManagerRequest",
	"mds-commands-native",
	"mds-sessions",
	"mds-cephfs",
	"mds-cephfs-ranks",
	"mds-cephfs-completion",
	"mds-cephfs-data",
	"rbd-base",
	"rbd-write",
	"rbd-lock",
	"rbd-clone",
	"rbd-journal",
	"controlRBDMirrorRequest",
	"controlMDSFreshRequest",
	"controlDaemonRequests",
	"controlMonitorElectionRequest",
	"controlOSDReadyRequest",
	"controlStartStateRequest",
	"controlOSDCompoundRequest",
	"controlManagerAuthRequest",
	"controlManagerKeyringRequest",
	"controlAuthBundleRequest",
	"controlManagerFailoverRequest",
	"controlOSDMutationRequest",
	"controlOSDXattrLimitRequest",
	"controlOSDOMAPRequest",
	"controlOSDOMAPWriteRequest",
	"controlOSDBackoffRequest",
	"controlOSDObjectBackoffRequest",
	"controlOSDUpmapRequest",
	"controlMDSMapsRequest",
	"controlOSDSplitRequest",
	"controlOSDWatchNotifyRequest",
	"controlOSDSparseReadRequest",
	"controlOSDPGListRequest",
	"controlOSDU64XattrRequest",
	"controlOSDWriteSameRequest",
	"controlOSDChecksumCompoundRequest",
	"controlOSDEnforceSnapContextRequest",
	"controlOSDSetChunkRequest",
	"controlOSDTierOpRequest",
	"controlManagerServiceConfigRequest",
	"controlOwnedPoolRequest",
	"controlOSDShardHistoryRequest",
	"controlOSDChecksumRequest",
	"controlOSDMapExtRangeRequest",
	"controlOSDMapExtRequest",
	"controlOSDWatchersRequest",
	"controlOSDCommandRequest",
	"controlOSDXattrNativeRequest",
	"controlOSDXattrRequest",
	"controlMonitorRestartRequest",
	"controlMonitorQuorumRequests",
	"controlLegacyMonitorStopRequest",
	"controlManagerStartRequest",
	"controlManagerSecurityRequest",
	"controlOSDEpochRequest",
	"controlManagerTellRequest",
	"controlDaemonCommandRequest",
}

// controlIdentity names a declared control the way recordedControlOrder does:
// the method a control poll calls, the request file of a request control, or
// the worker failure checkpoint by its file name.
func controlIdentity(control *fixtureControl) string {
	method := control.source[strings.LastIndex(control.source, ".")+1:]
	switch {
	case method == "checkNativeOracleWorker":
		return "native-oracles.error"
	case strings.HasPrefix(method, "control"):
		return method
	}
	return control.name
}

func TestDeclaredControlsKeepTheRecordedOrder(t *testing.T) {
	var ranked, later []string
	for _, control := range controlSequence {
		if control.order.rank != 0 {
			if len(later) != 0 {
				t.Fatal("ranked control", control.name, "runs after an unranked one")
			}
			ranked = append(ranked, controlIdentity(control))
		} else {
			later = append(later, control.name)
		}
	}
	if !slices.Equal(ranked, recordedControlOrder) {
		t.Fatalf("ranked controls poll\n%q\nrecorded\n%q", ranked, recordedControlOrder)
	}
	if !slices.IsSorted(later) {
		t.Fatal("controls added after the recorded order do not run by name", later)
	}
	// tools/test_merge_proof.py reads this line to see that a merged tree
	// polls every branch's control.
	t.Logf("declared fixture controls after the recorded order: %s", strings.Join(later, " "))
}

func TestRequestControlConsumesAndAcknowledgesItsRequest(t *testing.T) {
	var seen []string
	declaredBefore := declaredControls
	t.Cleanup(func() { declaredControls = declaredBefore })
	defineRequestControl("example-request", controlAfterRanked, func(_ *fixtureCluster, _ context.Context, _ string, fields []string) error {
		seen = fields
		return nil
	})
	control := declaredControls[len(declaredControls)-1]
	out := t.TempDir()
	c := &fixtureCluster{}
	if err := control.poll(c, context.Background(), out); err != nil || seen != nil {
		t.Fatal("a control without a pending request ran", err, seen)
	}
	writeControlRequest(t, out, "example-request", "7 a b")
	if err := control.poll(c, context.Background(), out); err != nil || !slices.Equal(seen, []string{"7", "a", "b"}) {
		t.Fatal("request fields did not reach the handler", err, seen)
	}
	if fileExists(filepath.Join(out, "example-request")) || !fileExists(filepath.Join(out, "example-request-completed.7")) {
		t.Fatal("request was not consumed and acknowledged")
	}
	failed := errors.New("synthetic handler failure")
	defineRequestControl("example-failure", controlAfterRanked, func(*fixtureCluster, context.Context, string, []string) error { return failed })
	control = declaredControls[len(declaredControls)-1]
	writeControlRequest(t, out, "example-failure", "8")
	if err := control.poll(c, context.Background(), out); err != failed || fileExists(filepath.Join(out, "example-failure-completed.8")) {
		t.Fatal("a failed request was acknowledged or lost its error", err)
	}
}

func (c *fixtureCluster) exampleControlA(context.Context, string) error { return nil }
func (c *fixtureCluster) exampleControlB(context.Context, string) error { return nil }

func exampleControl(name string, order controlOrder, poll controlPoll) *fixtureControl {
	return &fixtureControl{name: name, order: order, poll: poll, source: functionName(poll)}
}

func TestControlSequenceRefusesBadDeclarations(t *testing.T) {
	a, b := (*fixtureCluster).exampleControlA, (*fixtureCluster).exampleControlB
	for _, tc := range []struct {
		name     string
		declared []*fixtureControl
		refusal  string
	}{
		{"duplicate name", []*fixtureControl{exampleControl("x", controlAfterRanked, a), exampleControl("x", controlAfterRanked, b)}, "declared twice"},
		{"function declared twice", []*fixtureControl{exampleControl("x", controlAfterRanked, a), exampleControl("y", controlAfterRanked, a)}, "is declared as fixture controls"},
		{"nil function", []*fixtureControl{{name: "x"}}, "has no function"},
		{"bad name", []*fixtureControl{exampleControl("Bad_Name", controlAfterRanked, a)}, "lower-case words"},
		{"shared rank", []*fixtureControl{exampleControl("x", frozenControlRank(1), a), exampleControl("y", frozenControlRank(1), b)}, "share rank 1"},
	} {
		if _, err := sequenceControls(tc.declared); err == nil || !strings.Contains(err.Error(), tc.refusal) {
			t.Error(tc.name, "was not refused:", err)
		}
	}
	for _, refused := range []func(){
		func() { defineControl("x", controlAfterRanked, nil) },
		func() { defineRequestControl("x", controlAfterRanked, nil) },
		func() { frozenControlRank(0) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("declaration without a function or with a non-positive rank was accepted")
				}
			}()
			refused()
		}()
	}
	sequence, err := sequenceControls([]*fixtureControl{
		exampleControl("zeta", controlAfterRanked, a),
		exampleControl("alpha", controlAfterRanked, b),
		exampleControl("ranked", frozenControlRank(1), (*fixtureCluster).checkNativeOracleWorker),
	})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, control := range sequence {
		names = append(names, control.name)
	}
	if !slices.Equal(names, []string{"ranked", "alpha", "zeta"}) {
		t.Fatal("ranked controls do not precede the others, ordered by name", names)
	}
}
