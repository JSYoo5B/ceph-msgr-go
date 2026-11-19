package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"testing"
)

// recordedControlOrder is the polling order of the fixture's controls when
// the hand-written sequence in controls was recorded. Each entry names the
// method a poll calls, the request file of one acknowledged request inside a
// grouped method, or the worker failure checkpoint by its file name. It is
// frozen: a control added later never joins it.
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

// groupedControls are the methods whose body polls several acknowledged
// requests one after another.
var groupedControls = map[string]string{
	"controlMDSRequests": "control_mds.go",
	"controlRBDRequests": "control_rbd.go",
}

func fixtureMethod(t *testing.T, file, name string) *ast.FuncDecl {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv != nil && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatal("no method", name, "in", file)
	return nil
}

func controlCall(stmt ast.Stmt) (string, bool) {
	s, ok := stmt.(*ast.IfStmt)
	if !ok || s.Init == nil {
		return "", false
	}
	assign, ok := s.Init.(*ast.AssignStmt)
	if !ok || len(assign.Rhs) != 1 {
		return "", false
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	return sel.Sel.Name, true
}

func requestLiterals(t *testing.T, fn *ast.FuncDecl) []string {
	t.Helper()
	var names []string
	for _, stmt := range fn.Body.List {
		s, ok := stmt.(*ast.IfStmt)
		if !ok {
			continue
		}
		assign := s.Init.(*ast.AssignStmt)
		call := assign.Rhs[0].(*ast.CallExpr)
		if call.Fun.(*ast.SelectorExpr).Sel.Name != "request" {
			t.Fatal("grouped control polls something other than a request")
		}
		name, err := strconv.Unquote(call.Args[1].(*ast.BasicLit).Value)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

func TestHandWrittenControlSequenceMatchesTheRecordedOrder(t *testing.T) {
	var got []string
	for _, stmt := range fixtureMethod(t, "control.go", "controls").Body.List {
		if name, ok := controlCall(stmt); ok {
			if file, grouped := groupedControls[name]; grouped {
				got = append(got, requestLiterals(t, fixtureMethod(t, file, name))...)
			} else {
				got = append(got, name)
			}
			continue
		}
		if s, ok := stmt.(*ast.IfStmt); ok {
			if call, ok := s.Cond.(*ast.CallExpr); ok && len(call.Args) == 1 {
				if join, ok := call.Args[0].(*ast.CallExpr); ok && len(join.Args) == 2 {
					if lit, ok := join.Args[1].(*ast.BasicLit); ok && lit.Value == `"native-oracles.error"` {
						got = append(got, "native-oracles.error")
						continue
					}
				}
			}
		}
		if _, ok := stmt.(*ast.ReturnStmt); ok {
			continue
		}
		t.Fatal("unrecognized statement in controls at", stmt.Pos())
	}
	if !slices.Equal(got, recordedControlOrder) {
		t.Fatalf("controls polls\n%q\nrecorded\n%q", got, recordedControlOrder)
	}
}
