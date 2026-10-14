package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

type fakeXattrRestore func(context.Context) error

func (f fakeXattrRestore) Restore(ctx context.Context) error { return f(ctx) }

func TestXattrLimitsOwnEveryOSDAndRestoreUncertainApply(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "uncertain second apply"}[uncertain], func(t *testing.T) {
			before := map[string]map[string]string{"osd.1": {xattrLimitNames[0]: "0", xattrLimitNames[1]: "512"}, "osd.3": {xattrLimitNames[0]: "64", xattrLimitNames[1]: "512"}}
			current := make(map[string]map[string]string)
			for daemon, options := range before {
				current[daemon] = map[string]string{}
				for name, value := range options {
					current[daemon][name] = value
				}
			}
			reads := make(map[string]int)
			var restoreOrder []string
			backend := xattrLimitsBackend{ids: []int{1, 3}, owned: map[string]string{"osd.1": "owned1", "osd.3": "owned3"}, configuration: func(context.Context) ([]ceph.ConfigEntry, error) {
				return []ceph.ConfigEntry{{Section: "osd", Name: xattrLimitNames[1], Value: "512"}, {Section: "client", Name: "unrelated_private", Value: "do not retain"}}, nil
			}, get: func(ctx context.Context, daemon, name string) ([]byte, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
					t.Fatal("unbounded effective call")
				}
				reads[daemon+"/"+name]++
				return json.Marshal(map[string]string{name: current[daemon][name]})
			}, temporary: func(ctx context.Context, setting ceph.ConfigSetting) (xattrLimitRestorer, error) {
				if setting.Section != "osd" || setting.Mask != "" || setting.Value != "32" {
					t.Fatal("wrong scoped setting", setting)
				}
				for daemon := range current {
					current[daemon][setting.Name] = "32"
				}
				handle := fakeXattrRestore(func(context.Context) error {
					restoreOrder = append(restoreOrder, setting.Name)
					for daemon := range current {
						current[daemon][setting.Name] = before[daemon][setting.Name]
					}
					return nil
				})
				if uncertain && setting.Name == xattrLimitNames[1] {
					return handle, errors.New("lost config reply")
				}
				return handle, nil
			}}
			scope, err := beginXattrLimitsWith(context.Background(), backend)
			if (err != nil) != uncertain || scope == nil || len(scope.handles) != 2 {
				t.Fatal("uncertain apply ownership lost", err)
			}
			if len(scope.Receipt().Prior) != 2 || scope.Receipt().Prior[xattrLimitNames[0]] != nil || scope.Receipt().Prior[xattrLimitNames[1]].Value != "512" {
				t.Fatal("exact entry/absence selection lost")
			}
			if err := scope.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(current, before) || !reflect.DeepEqual(scope.Receipt().Restored, before) {
				t.Fatal("effective restoration did not retain every daemon original")
			}
			if !reflect.DeepEqual(restoreOrder, []string{xattrLimitNames[1], xattrLimitNames[0]}) {
				t.Fatal("owned reverse restoration order", restoreOrder)
			}
			if err := scope.Restore(context.Background()); err != nil || len(restoreOrder) != 2 {
				t.Fatal("non-idempotent restoration", err)
			}
			for daemon := range before {
				for _, name := range xattrLimitNames {
					if reads[daemon+"/"+name] < 2 {
						t.Fatal("owned daemon missed effective before/restore gate", daemon)
					}
				}
			}
		})
	}
}

func TestXattrEffectiveGateHonorsContextAndDoesNotClaimApplied(t *testing.T) {
	backend := xattrLimitsBackend{ids: []int{2}, get: func(ctx context.Context, _, name string) ([]byte, error) {
		return json.Marshal(map[string]string{name: "64"})
	}}
	scope := &xattrLimitsScope{backend: backend}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := scope.waitEffective(ctx, map[string]map[string]string{"osd.2": {xattrLimitNames[0]: "32", xattrLimitNames[1]: "32"}})
	if !errors.Is(err, context.Canceled) || scope.Receipt().Applied != nil {
		t.Fatal("canceled/mismatched gate advertised success", err)
	}
}

func TestDecodeXattrLimitBoundedSelectedFields(t *testing.T) {
	name := xattrLimitNames[0]
	for _, data := range []string{`{"osd_max_attr_size":"00032"}`, `{"osd_max_attr_size":32}`} {
		if got, err := decodeXattrLimit([]byte(data), name); err != nil || got != "32" {
			t.Fatal(got, err)
		}
	}
	for _, data := range []string{`{}`, `{"osd_max_attr_size":"-1"}`, `{"osd_max_attr_size":32.5}`, `{"osd_max_attr_size":"32","unrelated_private":"secret"}`, `{"wrong":32}`} {
		if _, err := decodeXattrLimit([]byte(data), name); err == nil {
			t.Fatal("accepted invalid selected reply", data)
		}
	}
	if _, err := decodeXattrLimit(make([]byte, 4097), name); err == nil {
		t.Fatal("accepted oversized selected reply")
	}
}

func TestXattrRestoreAttemptsAllOwnedHandlesAndPreservesFailure(t *testing.T) {
	want := errors.New("outside config edit refused")
	var calls []int
	s := &xattrLimitsScope{handles: []xattrLimitRestorer{
		fakeXattrRestore(func(context.Context) error { calls = append(calls, 0); return nil }),
		fakeXattrRestore(func(context.Context) error { calls = append(calls, 1); return want }),
	}}
	if err := s.Restore(context.Background()); !errors.Is(err, want) {
		t.Fatal("restoration refusal was lost", err)
	}
	if !reflect.DeepEqual(calls, []int{1, 0}) || s.restored || s.Receipt().Restored != nil {
		t.Fatal("partial restoration claimed completion or skipped other owned handle")
	}
}
