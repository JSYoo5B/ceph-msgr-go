package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestCopyFromErrorScopeObservesRuntimeApplicationAndRestoresOnce(t *testing.T) {
	value := "false"
	var mutations []string
	scope, err := beginCopyFromError(context.Background(), 7, func(ctx context.Context, daemon string, args ...string) ([]byte, error) {
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
	scope, err := beginCopyFromError(context.Background(), 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
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
			scope, err := beginCopyFromError(context.Background(), 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
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
	scope, err := beginCopyFromError(context.Background(), 0, func(_ context.Context, _ string, args ...string) ([]byte, error) {
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
