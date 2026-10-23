package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestMgrServiceConfigDiffRetainsOnlyOwnedOptionsAndOverridePresence(t *testing.T) {
	values, err := parseMgrServiceConfigDiff([]byte(`{"diff":{"private":{"override":"omit"},"mgr_stats_period":{"file":1,"override":2,"final":2},"mgr_stats_threshold":{"default":5,"final":5},"mgr_stats_period_autotune":{"override":false,"final":false}}}`))
	want := map[string]mgrServiceConfigValue{
		"mgr_stats_period": {Override: "2", Present: true}, "mgr_stats_threshold": {}, "mgr_stats_period_autotune": {Override: "false", Present: true},
	}
	if err != nil || !reflect.DeepEqual(values, want) {
		t.Fatal("native configuration projection", values, err)
	}
	for _, raw := range []string{`{}`, `{"diff":null}`, `{"diff":{"mgr_stats_period":{"override":true}}}`, `{"diff":{"mgr_stats_period":{"override":-1}}}`, `{"diff":{"mgr_stats_period":{"override":1.5}}}`, `{"diff":{"mgr_stats_threshold":{"override":12}}}`, `{"diff":{"mgr_stats_period_autotune":{"override":"false"}}}`} {
		if _, err := parseMgrServiceConfigDiff([]byte(raw)); err == nil {
			t.Fatal("accepted invalid native configuration", raw)
		}
	}
}

func TestMgrServiceConfigRestoreRemovesAbsentOverridesAndNeverRetries(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		s := &mgrServiceConfigScope{out: t.TempDir(), dirty: map[string]bool{"mgr_stats_period": true, "mgr_stats_period_autotune": true}, Baseline: map[string]mgrServiceConfigValue{
			"mgr_stats_period": {Effective: "1"}, "mgr_stats_period_autotune": {Effective: "true", Override: "true", Present: true},
		}}
		var calls []string
		set := func(action, name, value string) error {
			calls = append(calls, action+" "+name+" "+value)
			if uncertain {
				return errors.New("synthetic uncertain restoration")
			}
			return nil
		}
		err := restoreMgrServiceConfig(s, set)
		if (err != nil) != uncertain || !reflect.DeepEqual(calls, []string{"unset mgr_stats_period ", "set mgr_stats_period_autotune true"}) {
			t.Fatal("restoration replaced absence, touched an unchanged setting, or stopped before the other setting", calls, err)
		}
		if err := restoreMgrServiceConfig(s, set); err == nil || len(calls) != 2 {
			t.Fatal("unconfirmed restoration was retried", calls, err)
		}
	}
}
