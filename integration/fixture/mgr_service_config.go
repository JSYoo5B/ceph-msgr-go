package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"time"
)

var mgrServiceConfigNames = []string{"mgr_stats_period_autotune", "mgr_stats_period", "mgr_stats_threshold"}

// Only native daemon configuration is projected. The full diff may contain
// private paths and addresses and must never be retained as public evidence.
type mgrServiceConfigValue struct {
	Effective string `json:"effective"`
	Override  string `json:"override,omitempty"`
	Present   bool   `json:"override_present"`
}

type mgrServiceConfigAction struct {
	Action string `json:"action"`
	Name   string `json:"name"`
	Value  string `json:"value,omitempty"`
	State  string `json:"state"`
}

type mgrServiceConfigScope struct {
	Manager  string                           `json:"manager"`
	GID      uint64                           `json:"gid"`
	Epoch    uint32                           `json:"epoch"`
	Baseline map[string]mgrServiceConfigValue `json:"baseline"`
	Current  map[string]mgrServiceConfigValue `json:"current"`
	Actions  []mgrServiceConfigAction         `json:"actions"`
	Restored bool                             `json:"restored"`
	out      string
	dirty    map[string]bool
	attempt  bool
	step     int
}

func mgrServiceConfigScalar(raw json.RawMessage, name string) (string, error) {
	if name == "mgr_stats_period_autotune" {
		var value bool
		if string(raw) != "true" && string(raw) != "false" || json.Unmarshal(raw, &value) != nil {
			return "", errors.New("native MGR autotune value is not a boolean")
		}
		return strconv.FormatBool(value), nil
	}
	var value int64
	if json.Unmarshal(raw, &value) != nil || value < 0 || value > 1<<32-1 || name == "mgr_stats_threshold" && value > 11 {
		return "", errors.New("native MGR period/threshold value is outside its contract")
	}
	return strconv.FormatInt(value, 10), nil
}

func parseMgrServiceConfigDiff(data []byte) (map[string]mgrServiceConfigValue, error) {
	var raw struct {
		Diff map[string]map[string]json.RawMessage `json:"diff"`
	}
	if len(data) == 0 || len(data) > 4<<20 || json.Unmarshal(data, &raw) != nil || raw.Diff == nil {
		return nil, errors.New("native MGR config diff is missing or exceeds its bound")
	}
	selected := make(map[string]mgrServiceConfigValue)
	for _, name := range mgrServiceConfigNames {
		entry := raw.Diff[name]
		value := mgrServiceConfigValue{}
		if override, ok := entry["override"]; ok {
			var err error
			value.Override, err = mgrServiceConfigScalar(override, name)
			if err != nil {
				return nil, err
			}
			value.Present = true
		}
		selected[name] = value
	}
	return selected, nil
}

func (s *mgrServiceConfigScope) publish() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.out, "mgr-service-config-native.json"), append(data, '\n'), 0o644)
}

func (c *fixtureCluster) mgrServiceConfigSocket(ctx context.Context, action, name, value string) ([]byte, error) {
	if !slices.Contains(mgrServiceConfigNames, name) && action != "diff" {
		return nil, errors.New("MGR configuration option is outside the closed scope")
	}
	args := []string{"config", action}
	if action != "diff" {
		args = append(args, name)
	}
	if action == "set" {
		args = append(args, value)
	}
	call, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	return c.cluster.AdminSocket(call, "mgr."+c.mgrServiceConfig.Manager, args...)
}

func (c *fixtureCluster) observeMgrServiceConfig(ctx context.Context) error {
	s := c.mgrServiceConfig
	before, err := c.observeManagerMap(ctx)
	if err != nil || !before.Available || before.ActiveName != s.Manager || before.ActiveGID != s.GID {
		return errors.Join(err, errors.New("native active MGR identity changed during configuration scope"))
	}
	data, err := c.mgrServiceConfigSocket(ctx, "diff", "", "")
	if err != nil {
		return err
	}
	values, err := parseMgrServiceConfigDiff(data)
	if err != nil {
		return err
	}
	for _, name := range mgrServiceConfigNames {
		data, err := c.mgrServiceConfigSocket(ctx, "get", name, "")
		if err != nil {
			return err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return err
		}
		v := values[name]
		v.Effective = value
		if v.Present && v.Effective != v.Override {
			return errors.New("native MGR runtime override differs from effective readback")
		}
		values[name] = v
	}
	after, err := c.observeManagerMap(ctx)
	if err != nil || !after.Available || after.ActiveName != before.ActiveName || after.ActiveGID != before.ActiveGID || after.Epoch != before.Epoch {
		return errors.Join(err, errors.New("native MGR map changed across configuration readback"))
	}
	s.Current = values
	return nil
}

// Restoration is attempted once per option, including an uncertain mutation.
// Absent overrides are unset, rather than replaced with their effective value.
func restoreMgrServiceConfig(s *mgrServiceConfigScope, set func(string, string, string) error) error {
	if s.attempt {
		if s.Restored {
			return nil
		}
		return errors.New("MGR config restoration was already attempted without confirmation")
	}
	s.attempt = true
	var errs []error
	for i := len(mgrServiceConfigNames) - 1; i >= 0; i-- {
		name := mgrServiceConfigNames[i]
		if !s.dirty[name] {
			continue
		}
		original := s.Baseline[name]
		action, value := "unset", ""
		if original.Present {
			action, value = "set", original.Override
		}
		s.Actions = append(s.Actions, mgrServiceConfigAction{action, name, value, "restore-intended"})
		if err := s.publish(); err != nil {
			errs = append(errs, err)
			continue
		}
		err := set(action, name, value)
		state := "restore-applied"
		if err != nil {
			state = "restore-uncertain"
		}
		s.Actions[len(s.Actions)-1].State = state
		errs = append(errs, err, s.publish())
	}
	return errors.Join(errs...)
}

func (c *fixtureCluster) restoreMgrServiceConfig(ctx context.Context) error {
	s := c.mgrServiceConfig
	if s == nil || s.Restored {
		return nil
	}
	// A replacement with the same daemon name does not inherit our runtime
	// settings. Refuse to mutate it without the original native GID.
	if err := c.observeMgrServiceConfig(ctx); err != nil {
		return err
	}
	err := restoreMgrServiceConfig(s, func(action, name, value string) error {
		_, err := c.mgrServiceConfigSocket(ctx, action, name, value)
		return err
	})
	if err != nil {
		return err
	}
	if err := c.observeMgrServiceConfig(ctx); err != nil {
		return err
	}
	s.Restored = reflect.DeepEqual(s.Baseline, s.Current)
	if !s.Restored {
		return errors.New("native MGR effective values or override presence were not restored")
	}
	return s.publish()
}

func (c *fixtureCluster) mgrServiceConfigControl(ctx context.Context, out, action string) error {
	if c.profile.mgrService != "1" {
		return errors.New("MGR configuration control requires the exclusive service fixture")
	}
	if action == "begin" {
		if c.mgrServiceConfig != nil {
			return errors.New("MGR configuration scope was already selected")
		}
		state, err := c.observeManagerMap(ctx)
		owned, _, ownErr := c.ownedManagers()
		if err != nil || ownErr != nil || !state.Available || state.ActiveGID == 0 || owned[state.ActiveName] == nil {
			return errors.Join(err, ownErr, errors.New("MGR configuration requires a ready owned active daemon"))
		}
		s := &mgrServiceConfigScope{Manager: state.ActiveName, GID: state.ActiveGID, Epoch: state.Epoch,
			out: out, dirty: make(map[string]bool), Actions: []mgrServiceConfigAction{}}
		c.mgrServiceConfig = s
		if err := c.observeMgrServiceConfig(ctx); err != nil {
			return err
		}
		s.Baseline = s.Current
		return s.publish()
	}
	if c.mgrServiceConfig == nil {
		return errors.New("MGR configuration scope has not begun")
	}
	if action == "restore" {
		return c.restoreMgrServiceConfig(ctx)
	}
	steps := []struct{ action, name, value string }{
		{"autotune-off", "mgr_stats_period_autotune", "false"},
		{"one-second", "mgr_stats_period", "1"},
		{"two-seconds", "mgr_stats_period", "2"},
		{"threshold-only", "mgr_stats_threshold", "6"},
		{"one-second-again", "mgr_stats_period", "1"},
	}
	s := c.mgrServiceConfig
	if s.attempt || s.step >= len(steps) || action != steps[s.step].action {
		return fmt.Errorf("MGR configuration action %q is outside the ordered scope", action)
	}
	step := steps[s.step]
	if err := c.observeMgrServiceConfig(ctx); err != nil {
		return err
	}
	// Journal before the native command; no failed/uncertain set is repeated.
	s.dirty[step.name] = true
	s.Actions = append(s.Actions, mgrServiceConfigAction{action, step.name, step.value, "intended"})
	if err := s.publish(); err != nil {
		return err
	}
	_, err := c.mgrServiceConfigSocket(ctx, "set", step.name, step.value)
	if err != nil {
		s.Actions[len(s.Actions)-1].State = "uncertain"
		return errors.Join(err, s.publish())
	}
	s.step++
	if err := c.observeMgrServiceConfig(ctx); err != nil {
		return err
	}
	s.Actions[len(s.Actions)-1].State = "applied"
	if s.Current[step.name].Effective != step.value || !s.Current[step.name].Present {
		return errors.New("MGR configuration readback did not match the applied runtime override")
	}
	return s.publish()
}
