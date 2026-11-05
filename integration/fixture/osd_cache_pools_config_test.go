package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// poolRemovalFake records every independent read, central change, restore and
// removal of one pool-removal window in order.
type poolRemovalFake struct {
	events     []string
	gets       []string
	dumps      []string
	allowErr   error
	noHandle   bool
	restoreErr error
	removeErr  error
	setting    ceph.ConfigSetting
}

type poolRemovalFakeHandle struct{ f *poolRemovalFake }

func (h poolRemovalFakeHandle) Restore(context.Context) error {
	h.f.events = append(h.f.events, "restore")
	return h.f.restoreErr
}

func (f *poolRemovalFake) central() poolRemovalConfig {
	return poolRemovalConfig{
		observe: func(_ context.Context, argv ...string) ([]byte, error) {
			command := strings.Join(argv, " ")
			f.events = append(f.events, command)
			var queue *[]string
			switch command {
			case "config get mon mon_allow_pool_delete":
				queue = &f.gets
			case "config dump --format json":
				queue = &f.dumps
			default:
				return nil, errors.New("unexpected observation " + command)
			}
			if len(*queue) == 0 {
				return nil, errors.New("observation unavailable")
			}
			answer := (*queue)[0]
			*queue = (*queue)[1:]
			return []byte(answer), nil
		},
		allow: func(_ context.Context, setting ceph.ConfigSetting) (centralConfigRestorer, error) {
			f.events = append(f.events, "allow")
			f.setting = setting
			if f.noHandle {
				return nil, f.allowErr
			}
			return poolRemovalFakeHandle{f}, f.allowErr
		},
	}
}

func (f *poolRemovalFake) run(t *testing.T) (*osdCachePoolScope, string, error) {
	t.Helper()
	x := &osdCachePoolScope{Out: t.TempDir()}
	baseline, err := withOwnedPoolRemoval(context.Background(), f.central(), x.mutate, func() error {
		f.events = append(f.events, "remove")
		return f.removeErr
	})
	return x, baseline, err
}

func poolRemovalSteps(x *osdCachePoolScope) []string {
	var steps []string
	for _, step := range x.Steps {
		steps = append(steps, step.Action+"="+step.State)
	}
	return steps
}

const (
	poolDeleteAbsentDump  = `[{"section":"global","name":"osd_pool_default_size","value":"1"}]`
	poolDeletePresentDump = `[{"section":"mon","name":"mon_allow_pool_delete","value":"true"}]`
)

var poolRemovalPrecheck = []string{"config get mon mon_allow_pool_delete", "config dump --format json"}

func TestCachePoolRemovalWindowDelegatesCentralConfigAndKeepsIndependentChecks(t *testing.T) {
	f := &poolRemovalFake{gets: []string{"false\n", "false\n"}, dumps: []string{poolDeleteAbsentDump}}
	x, baseline, err := f.run(t)
	if err != nil || baseline != "false" {
		t.Fatal("pool-removal window failed", baseline, err)
	}
	want := append(slices.Clone(poolRemovalPrecheck), "allow", "remove", "restore", "config get mon mon_allow_pool_delete")
	if !slices.Equal(f.events, want) {
		t.Fatal("pool-removal window order changed", f.events)
	}
	if f.setting != (ceph.ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"}) {
		t.Fatal("pool-removal window changed another central entry", f.setting)
	}
	if steps := poolRemovalSteps(x); !slices.Equal(steps, []string{"enable-owned-pool-removal=completed", "restore-pool-removal-baseline=completed"}) {
		t.Fatal("pool-removal step journal changed", steps)
	}
}

func TestCachePoolRemovalWindowRefusesUnexpectedBaselineBeforeChange(t *testing.T) {
	for name, f := range map[string]*poolRemovalFake{
		"effective-true":   {gets: []string{"true"}, dumps: []string{poolDeleteAbsentDump}},
		"central-override": {gets: []string{"false"}, dumps: []string{poolDeletePresentDump}},
		"invalid-dump":     {gets: []string{"false"}, dumps: []string{"not json"}},
		"get-failure":      {},
	} {
		x, _, err := f.run(t)
		if err == nil || slices.Contains(f.events, "allow") || slices.Contains(f.events, "remove") || len(x.Steps) != 0 {
			t.Fatal(name, "unexpected baseline reached the central change", f.events, err)
		}
	}
}

func TestCachePoolRemovalWindowRefusedOrUncertainChangeIsNotReplayed(t *testing.T) {
	cause := errors.New("config set reply lost")
	for name, f := range map[string]*poolRemovalFake{
		"refused":   {allowErr: cause, noHandle: true},
		"uncertain": {allowErr: cause},
	} {
		f.gets, f.dumps = []string{"false"}, []string{poolDeleteAbsentDump}
		x, _, err := f.run(t)
		want := append(slices.Clone(poolRemovalPrecheck), "allow")
		if !errors.Is(err, cause) || !slices.Equal(f.events, want) {
			t.Fatal(name, "change was retried, restored or followed by removal", f.events, err)
		}
		if len(x.Steps) != 1 || x.Steps[0].Action != "enable-owned-pool-removal" || x.Steps[0].State != "unknown-or-failed" || x.Steps[0].Error != cause.Error() {
			t.Fatal(name, "uncertain change lost from the step journal", x.Steps)
		}
	}
	f := &poolRemovalFake{gets: []string{"false"}, dumps: []string{poolDeleteAbsentDump}, noHandle: true}
	if _, _, err := f.run(t); err == nil || slices.Contains(f.events, "remove") {
		t.Fatal("change without a handle was accepted", f.events, err)
	}
}

func TestCachePoolRemovalWindowRealSetConfigRefusalIsNotApplied(t *testing.T) {
	central := clusterPoolRemovalConfig(nil)
	handle, err := central.allow(context.Background(), poolDeleteSetting)
	if !errors.Is(err, ceph.ErrInvalidRequest) || handle == nil {
		t.Fatal("nil cluster was not refused by override.SetConfig", handle, err)
	}
	if err := handle.Restore(context.Background()); err != nil {
		t.Fatal("refused handle restore changed state", err)
	}
}

func TestCachePoolRemovalWindowFailedRemovalKeepsEntryForCluster(t *testing.T) {
	cause := errors.New("pool rm reply lost")
	f := &poolRemovalFake{gets: []string{"false"}, dumps: []string{poolDeleteAbsentDump}, removeErr: cause}
	x, _, err := f.run(t)
	want := append(slices.Clone(poolRemovalPrecheck), "allow", "remove")
	if !errors.Is(err, cause) || !slices.Equal(f.events, want) || len(x.Steps) != 1 {
		t.Fatal("failed removal was followed by another mutation", f.events, err)
	}
}

func TestCachePoolRemovalWindowRestoreFailureAndUnrestoredBaseline(t *testing.T) {
	cause := errors.New("configuration entry changed outside this override; refusing restoration")
	f := &poolRemovalFake{gets: []string{"false", "false"}, dumps: []string{poolDeleteAbsentDump}, restoreErr: cause}
	x, _, err := f.run(t)
	want := append(slices.Clone(poolRemovalPrecheck), "allow", "remove", "restore")
	if !errors.Is(err, cause) || !slices.Equal(f.events, want) {
		t.Fatal("failed restore was retried or trusted a later readback", f.events, err)
	}
	if steps := poolRemovalSteps(x); !slices.Equal(steps, []string{"enable-owned-pool-removal=completed", "restore-pool-removal-baseline=unknown-or-failed"}) {
		t.Fatal("failed restore lost from the step journal", steps)
	}
	// A restore reported as successful is still checked independently.
	f = &poolRemovalFake{gets: []string{"false", "true"}, dumps: []string{poolDeleteAbsentDump}}
	if _, _, err := f.run(t); err == nil || !strings.Contains(err.Error(), "pool deletion baseline not restored") {
		t.Fatal("provider restore receipt replaced the independent readback", err)
	}
	f = &poolRemovalFake{gets: []string{"false"}, dumps: []string{poolDeleteAbsentDump}}
	if _, _, err := f.run(t); err == nil || !strings.Contains(err.Error(), "pool deletion baseline not restored") {
		t.Fatal("unavailable post-restore readback was accepted", err)
	}
}

func TestCachePoolDeleteOverrideInventoryMatchesEverySection(t *testing.T) {
	if err := poolDeleteOverrideAbsent([]byte(poolDeleteAbsentDump), "invalid", "present"); err != nil {
		t.Fatal(err)
	}
	for _, dump := range []string{poolDeletePresentDump, `[{"section":"global","mask":"host:x","name":"mon_allow_pool_delete","value":"false"}]`} {
		if err := poolDeleteOverrideAbsent([]byte(dump), "invalid", "present"); err == nil || err.Error() != "present" {
			t.Fatal("pool deletion entry missed", dump, err)
		}
	}
	if err := poolDeleteOverrideAbsent([]byte("{}"), "invalid", "present"); err == nil || err.Error() != "invalid" {
		t.Fatal("invalid inventory accepted", err)
	}
}
