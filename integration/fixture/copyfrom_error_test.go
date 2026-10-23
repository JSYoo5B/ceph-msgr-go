package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCopyFromErrorScopeObservesRuntimeApplicationAndRestoresOnce(t *testing.T) {
	value := "false"
	var mutations []string
	scope, err := beginCopyFromError(context.Background(), 7, 0, func(ctx context.Context, daemon string, args ...string) ([]byte, error) {
		if _, ok := ctx.Deadline(); !ok || daemon != "osd.7" {
			t.Fatal("native option call is not bounded/owned")
		}
		if reflect.DeepEqual(args, []string{"config", "get", "osd_copyfrom_max_chunk"}) {
			return []byte(`{"osd_copyfrom_max_chunk":"8388608"}`), nil
		}
		if len(args) == 4 && args[1] == "set" {
			value = args[3]
			mutations = append(mutations, value)
			return []byte(`{}`), nil
		}
		return []byte(fmt.Sprintf(`{"osd_debug_inject_copyfrom_error":%q}`, value)), nil
	})
	if err != nil || scope.receipt.Before != "false" || scope.receipt.Applied != "true" || scope.receipt.MaxChunk != 8388608 {
		t.Fatal("effective runtime gate differs", err)
	}
	if err = scope.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = scope.Restore(context.Background()); err != nil || !reflect.DeepEqual(mutations, []string{"true", "false"}) || scope.receipt.Restored != "false" {
		t.Fatal("restoration was not observed or replayed a mutation", err, mutations)
	}
}

func TestCopyFromErrorScopeRetainsUncertainApplyAndDoesNotReplayRestore(t *testing.T) {
	lost := errors.New("lost apply acknowledgement")
	value, sets := "false", 0
	scope, err := beginCopyFromError(context.Background(), 0, 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[2] == "osd_copyfrom_max_chunk" {
			return []byte(`{"osd_copyfrom_max_chunk":8388608}`), nil
		}
		if len(args) == 4 {
			sets++
			value = args[3]
			if sets == 1 {
				return nil, lost
			}
			return nil, errors.New("lost restore acknowledgement")
		}
		return []byte(fmt.Sprintf(`{"osd_debug_inject_copyfrom_error":%s}`, value)), nil
	})
	if !errors.Is(err, lost) || !scope.receipt.ApplyAttempted || scope.receipt.ApplyReturned {
		t.Fatal("lost apply was treated as absent", err)
	}
	if err = scope.Restore(context.Background()); err == nil {
		t.Fatal("restore acknowledgement error disappeared")
	}
	if err = scope.Restore(context.Background()); err != nil || sets != 2 || scope.receipt.Restored != "false" {
		t.Fatal("failed restore was replayed instead of independently observed", err, sets)
	}
}

func TestCopyFromErrorScopeRejectsInvalidEffectiveValuesBeforeMutation(t *testing.T) {
	for _, raw := range []string{`{}`, `{"osd_debug_inject_copyfrom_error":"unknown"}`, `{"osd_debug_inject_copyfrom_error":0}`, `{"osd_debug_inject_copyfrom_error":true}`, `{"osd_debug_inject_copyfrom_error":false,"secret":"x"}`} {
		t.Run(raw, func(t *testing.T) {
			sets := 0
			scope, err := beginCopyFromError(context.Background(), 0, 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
				if len(args) == 4 {
					sets++
				}
				return []byte(raw), nil
			})
			if err == nil || scope.receipt.ApplyAttempted || sets != 0 {
				t.Fatal("invalid/originally-enabled option admitted", err, sets)
			}
		})
	}
}

func TestCopyFromErrorScopeRequiresAnEffectiveRuntimeChange(t *testing.T) {
	scope, err := beginCopyFromError(context.Background(), 0, 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[2] == "osd_copyfrom_max_chunk" {
			return []byte(`{"osd_copyfrom_max_chunk":8388608}`), nil
		}
		return []byte(`{"osd_debug_inject_copyfrom_error":false}`), nil
	})
	if err == nil || !scope.receipt.ApplyReturned || scope.receipt.Applied != "false" {
		t.Fatal("successful command without effective runtime mutation admitted", err)
	}
}

func TestCopyFromErrorControlRequiresExclusiveSelector(t *testing.T) {
	for _, p := range []profile{{}, {osd: "1", osdStore: "bluestore", osdReplicas: "3", testRun: copyFromLateErrorSelector}, {osd: "1", osdStore: "bluestore", osdReplicas: "1", testRun: ".*"}} {
		c := &fixtureCluster{profile: p}
		if err := c.copyFromErrorControl(context.Background(), t.TempDir(), []string{"1", "apply"}); err == nil {
			t.Fatal("nonexclusive fault injection admitted")
		}
	}
}

func TestCopyFromErrorMarkerOnlyReachesExclusiveTestEnvironment(t *testing.T) {
	// Caller-supplied markers cannot enable a broad suite or another row.
	t.Setenv(copyFromLateErrorMarker, "1")
	for _, runtime := range []string{"host", "container"} {
		for _, selector := range []string{copyFromLateErrorSelector, "", "^TestCeph.*Integration$", "^TestCeph(OSD|MonOSDMaps).*Integration$", "^TestCephOSDSetChunkGoIntegration$"} {
			t.Run(runtime+"/"+selector, func(t *testing.T) {
				p, err := loadProfile(func(key string) string {
					return map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1", "CEPH_MSGR_TEST_RUNTIME": runtime, "CEPH_MSGR_TEST_RUN": selector, copyFromLateErrorMarker: "1"}[key]
				})
				if err != nil {
					t.Fatal(err)
				}
				want := ""
				if selector == copyFromLateErrorSelector {
					want = "1"
				}
				inputs := p.testEnvironment("/out", "")
				if inputs[copyFromLateErrorMarker] != want {
					t.Fatal("fault marker did not match exclusive selection", selector)
				}
				env := envList(inputs)
				if runtime == "host" {
					env = environment(inputs, true)
				}
				count := 0
				for _, field := range env {
					if strings.HasPrefix(field, copyFromLateErrorMarker+"=") {
						count++
						if field != copyFromLateErrorMarker+"="+want {
							t.Fatal("inherited marker enabled an unrelated suite", field)
						}
					}
				}
				if count != 1 {
					t.Fatal("test marker was absent or duplicated", count)
				}
				if _, leaked := p.containerEnvironment()[copyFromLateErrorMarker]; leaked {
					t.Fatal("fault marker leaked to daemon environment")
				}
			})
		}
	}
}

func TestCopyFromErrorScopeLowersChunkBeforeInjectorAndRestoresBoth(t *testing.T) {
	values := map[string]string{copyFromErrorOption: "false", copyFromChunkOption: "8388608"}
	var mutations []string
	scope, err := beginCopyFromError(context.Background(), 0, copyFromMultiChunkLimit, func(ctx context.Context, daemon string, args ...string) ([]byte, error) {
		if _, ok := ctx.Deadline(); !ok || daemon != "osd.0" {
			t.Fatal("native option call is not bounded/owned")
		}
		if len(args) == 4 && args[1] == "set" {
			values[args[2]] = args[3]
			mutations = append(mutations, args[2]+"="+args[3])
			return []byte(`{}`), nil
		}
		if args[2] == copyFromErrorOption {
			return []byte(fmt.Sprintf(`{%q:%q}`, args[2], values[args[2]])), nil
		}
		return []byte(fmt.Sprintf(`{%q:%s}`, args[2], values[args[2]])), nil
	})
	if err != nil || scope.receipt.MaxChunk != 8388608 || scope.receipt.ChunkApplied != 32768 || scope.receipt.Applied != "true" {
		t.Fatal("effective chunk/injector gate differs", err, scope.receipt)
	}
	if err = scope.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = scope.Restore(context.Background()); err != nil || scope.receipt.ChunkRestored != 8388608 || scope.receipt.Restored != "false" {
		t.Fatal("chunk restoration was not observed", err)
	}
	want := []string{copyFromChunkOption + "=32768", copyFromErrorOption + "=true", copyFromErrorOption + "=false", copyFromChunkOption + "=8388608"}
	if !reflect.DeepEqual(mutations, want) {
		t.Fatal("mutation order or replay differs", mutations)
	}
}

func TestCopyFromErrorScopeKeepsUncertainChunkRestorationWithoutReplay(t *testing.T) {
	chunk, sets := "8388608", 0
	scope, err := beginCopyFromError(context.Background(), 0, copyFromMultiChunkLimit, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 4 {
			sets++
			if args[2] == copyFromChunkOption {
				chunk = args[3]
			}
			// The chunk set applies but loses its acknowledgement; the
			// later restore acknowledgement is lost as well.
			return nil, errors.New("lost acknowledgement")
		}
		if args[2] == copyFromErrorOption {
			return []byte(`{"osd_debug_inject_copyfrom_error":"false"}`), nil
		}
		return []byte(`{"osd_copyfrom_max_chunk":"` + chunk + `"}`), nil
	})
	if err == nil || !scope.receipt.ChunkApplyAttempted || scope.receipt.ChunkApplyReturned || scope.receipt.ApplyAttempted {
		t.Fatal("uncertain chunk apply admitted the injector", err)
	}
	if err = scope.Restore(context.Background()); err == nil {
		t.Fatal("lost chunk restore acknowledgement disappeared")
	}
	if err = scope.Restore(context.Background()); err != nil || sets != 2 || scope.receipt.ChunkRestored != 8388608 {
		t.Fatal("chunk restoration replayed instead of observed", err, sets)
	}
}

func TestCopyFromErrorScopeRejectsNonLoweringChunk(t *testing.T) {
	sets := 0
	_, err := beginCopyFromError(context.Background(), 0, 8388608, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 4 {
			sets++
		}
		if args[2] == copyFromErrorOption {
			return []byte(`{"osd_debug_inject_copyfrom_error":"false"}`), nil
		}
		return []byte(`{"osd_copyfrom_max_chunk":"8388608"}`), nil
	})
	if err == nil || sets != 0 {
		t.Fatal("non-lowering chunk override admitted", err, sets)
	}
}

func TestCopyFromErrorObservationsRequireMultiChunkSelector(t *testing.T) {
	single := profile{osd: "1", osdStore: "bluestore", osdReplicas: "1", osdProfile: "legacy-v6", testRun: copyFromLateErrorSelector}
	multi := single
	multi.testRun = copyFromMultiChunkSelector
	broad := multi
	broad.testRun = "^TestCephOSD.*Integration$"
	for _, tc := range []struct {
		p      profile
		fields []string
	}{
		{single, []string{"1", "history"}},
		{single, []string{"1", "staging", "2.0"}},
		{broad, []string{"1", "history"}},
		{multi, []string{"1", "staging"}},
		{multi, []string{"1", "history", "2.0"}},
		{multi, []string{"1", "apply", "2.0"}},
	} {
		c := &fixtureCluster{profile: tc.p}
		if err := c.copyFromErrorControl(context.Background(), t.TempDir(), tc.fields); err == nil {
			t.Fatal("copy staging observation admitted", tc.p.testRun, tc.fields)
		}
	}
	if !multi.copyFromMultiChunkSelected() || multi.copyFromLateErrorSelected() || single.copyFromMultiChunkSelected() {
		t.Fatal("copy-error selectors are not exclusive")
	}
}

func TestCopyFromMultiChunkBootstrapRetainsShortOpHistory(t *testing.T) {
	multi := profile{osd: "1", osdStore: "bluestore", osdReplicas: "1", osdProfile: "legacy-v6", testRun: copyFromMultiChunkSelector}
	if sections := bootstrapSections(t, fixtureBootstrapConfig(multi)); sections["osd"]["osd_op_history_size"] != "1024" {
		t.Fatal("multi-chunk cell lacks its enlarged op history", sections)
	}
	single := multi
	single.testRun = copyFromLateErrorSelector
	if strings.Contains(fixtureBootstrapConfig(single), "osd_op_history_size") {
		t.Fatal("op history enlargement leaked to another cell")
	}
}

func TestCopyStagingListSeparatesTemporaryEntries(t *testing.T) {
	raw := "#2:0e1c3ad4:::src:head#\n#-4:0e1c3ad4:::temp_2.0_0_4201_1:head#\n\n"
	receipt, err := decodeCopyStaging([]byte(raw))
	if err != nil || receipt.Objects != 2 || !receipt.Complete || !reflect.DeepEqual(receipt.Temp, []string{"#-4:0e1c3ad4:::temp_2.0_0_4201_1:head#"}) {
		t.Fatal("staging enumeration differs", err, receipt)
	}
	full := strings.Repeat("#2:00000000:::x:head#\n", copyStagingMax)
	if receipt, err = decodeCopyStaging([]byte(full)); err != nil || receipt.Complete {
		t.Fatal("bounded enumeration claimed completeness", err)
	}
	for _, bad := range []string{"2:0:::x:head#", "#x:0:::y:head#", "#2:0:::y:head", "No such collection"} {
		if _, err := decodeCopyStaging([]byte(bad)); err == nil {
			t.Fatal("invalid native entry admitted", bad)
		}
	}
	if _, err := decodeCopyStaging([]byte(strings.Repeat("#-4:0:::t:head#\n", copyStagingMaxTemp+1))); err == nil {
		t.Fatal("unbounded temporary list admitted")
	}
}

func TestCopyGetHistoryRetainsOnlyCopyGets(t *testing.T) {
	raw := `{"size":20,"duration":600,"ops":[{"description":"osd_op(osd.0.0:3 2.0 2:0e1c3ad4:::src:head [copy-get max 32768] snapc 0=[] ondisk+read e13)","initiated_at":"t1"},{"description":"osd_op(client.4:3 2.0 2:1:::dst:head [copy-from2 ver 3] e13)","initiated_at":"t2"}]}`
	receipt, err := decodeCopyGetHistory([]byte(raw))
	if err != nil || receipt.Ops != 2 || len(receipt.CopyGets) != 1 || receipt.CopyGets[0].InitiatedAt != "t1" || string(receipt.Size) != "20" {
		t.Fatal("copy-get history differs", err, receipt)
	}
	for _, bad := range []string{`{}`, `{"ops":null}`, `[]`} {
		if _, err := decodeCopyGetHistory([]byte(bad)); err == nil {
			t.Fatal("invalid history shape admitted", bad)
		}
	}
}

func TestCopyFromMultiChunkMarkerOnlyReachesExclusiveTestEnvironment(t *testing.T) {
	t.Setenv(copyFromMultiChunkMarker, "1")
	for _, selector := range []string{copyFromMultiChunkSelector, copyFromLateErrorSelector, "", "^TestCeph.*Integration$"} {
		p, err := loadProfile(func(key string) string {
			return map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_RUN": selector, copyFromMultiChunkMarker: "1"}[key]
		})
		if err != nil {
			t.Fatal(err)
		}
		inputs := p.testEnvironment("/out", "")
		want, other := "", ""
		if selector == copyFromMultiChunkSelector {
			want = "1"
		}
		if selector == copyFromLateErrorSelector {
			other = "1"
		}
		if inputs[copyFromMultiChunkMarker] != want || inputs[copyFromLateErrorMarker] != other {
			t.Fatal("copy fault markers did not match exclusive selection", selector)
		}
		count := 0
		for _, field := range environment(inputs, true) {
			if strings.HasPrefix(field, copyFromMultiChunkMarker+"=") {
				count++
				if field != copyFromMultiChunkMarker+"="+want {
					t.Fatal("inherited marker enabled an unrelated suite", field)
				}
			}
		}
		if count != 1 {
			t.Fatal("multi-chunk marker was absent or duplicated", count)
		}
		if _, leaked := p.containerEnvironment()[copyFromMultiChunkMarker]; leaked {
			t.Fatal("fault marker leaked to daemon environment")
		}
	}
}
