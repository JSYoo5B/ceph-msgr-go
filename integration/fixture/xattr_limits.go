package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var xattrLimitNames = []string{"osd_max_attr_size", "osd_max_attr_name_len"}

type xattrLimitRestorer interface{ Restore(context.Context) error }
type xattrLimitsBackend struct {
	ids           []int
	owned         map[string]string
	configuration func(context.Context) ([]ceph.ConfigEntry, error)
	temporary     func(context.Context, ceph.ConfigSetting) (xattrLimitRestorer, error)
	get           func(context.Context, string, string) ([]byte, error)
}

type xattrLimitsReceipt struct {
	Schema   int                          `json:"schema_version"`
	Owned    map[string]string            `json:"owned_osds"`
	Section  string                       `json:"section"`
	Prior    map[string]*ceph.ConfigEntry `json:"prior_entries"`
	Before   map[string]map[string]string `json:"effective_before"`
	Applied  map[string]map[string]string `json:"effective_applied"`
	Restored map[string]map[string]string `json:"effective_restored"`
}

// Only selected non-secret keys are retained. Public TemporaryConfig owns the
// exact entry/absence restoration and refuses conflicting outside edits.
type xattrLimitsScope struct {
	backend  xattrLimitsBackend
	handles  []xattrLimitRestorer
	receipt  xattrLimitsReceipt
	restored bool
}

func beginXattrLimits(ctx context.Context, cluster *ceph.Container) (*xattrLimitsScope, error) {
	backend := xattrLimitsBackend{configuration: cluster.Configuration, owned: make(map[string]string)}
	for _, osd := range cluster.OSDs() {
		backend.ids = append(backend.ids, osd.ID)
		backend.owned["osd."+strconv.Itoa(osd.ID)] = osd.Container.GetContainerID()
	}
	backend.temporary = func(ctx context.Context, setting ceph.ConfigSetting) (xattrLimitRestorer, error) {
		handle, err := cluster.TemporaryConfig(ctx, setting)
		if handle == nil {
			return nil, err
		}
		return handle, err
	}
	backend.get = func(ctx context.Context, daemon, name string) ([]byte, error) {
		return cluster.AdminSocket(ctx, daemon, "config", "get", name)
	}
	return beginXattrLimitsWith(ctx, backend)
}

func beginXattrLimitsWith(ctx context.Context, backend xattrLimitsBackend) (*xattrLimitsScope, error) {
	s := &xattrLimitsScope{backend: backend, receipt: xattrLimitsReceipt{Schema: 1, Section: "osd", Owned: backend.owned, Prior: make(map[string]*ceph.ConfigEntry)}}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	seen := make(map[int]bool)
	for _, id := range backend.ids {
		if id < 0 || seen[id] || backend.owned["osd."+strconv.Itoa(id)] == "" {
			return s, errors.New("invalid owned OSD identity for xattr gate")
		}
		seen[id] = true
	}
	if len(seen) == 0 {
		return s, errors.New("xattr limits require owned OSDs")
	}
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	entries, err := backend.configuration(call)
	stop()
	if err != nil {
		return s, fmt.Errorf("read selected xattr central entries: %w", err)
	}
	for _, name := range xattrLimitNames {
		s.receipt.Prior[name] = nil
		for _, entry := range entries {
			if entry.Section == "osd" && entry.Mask == "" && entry.Name == name {
				if s.receipt.Prior[name] != nil {
					return s, errors.New("duplicate selected xattr configuration entry")
				}
				copy := entry
				s.receipt.Prior[name] = &copy
			}
		}
	}
	s.receipt.Before, err = s.effective(ctx)
	if err != nil {
		return s, err
	}
	for _, name := range xattrLimitNames {
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		handle, applyErr := backend.temporary(call, ceph.ConfigSetting{Section: "osd", Name: name, Value: "32"})
		stop()
		// A non-nil error does not establish that the mutation was absent.
		if handle != nil {
			s.handles = append(s.handles, handle)
		}
		if applyErr != nil {
			return s, fmt.Errorf("apply selected xattr limit %s: %w", name, applyErr)
		}
		if handle == nil {
			return s, errors.New("xattr limit apply lacks restoration ownership")
		}
	}
	target := make(map[string]map[string]string)
	for daemon := range s.receipt.Before {
		target[daemon] = map[string]string{xattrLimitNames[0]: "32", xattrLimitNames[1]: "32"}
	}
	s.receipt.Applied, err = s.waitEffective(ctx, target)
	return s, err
}

func (s *xattrLimitsScope) Receipt() xattrLimitsReceipt { return s.receipt }

func (s *xattrLimitsScope) Restore(ctx context.Context) error {
	if s == nil || s.restored {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var errs []error
	for i := len(s.handles) - 1; i >= 0; i-- {
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		err := s.handles[i].Restore(call)
		stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("restore owned xattr configuration: %w", err)
	}
	if s.receipt.Before != nil {
		var err error
		s.receipt.Restored, err = s.waitEffective(ctx, s.receipt.Before)
		if err != nil {
			return err
		}
	}
	s.restored = true
	return nil
}

func (s *xattrLimitsScope) effective(ctx context.Context) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)
	for _, id := range s.backend.ids {
		daemon := "osd." + strconv.Itoa(id)
		result[daemon] = make(map[string]string)
		for _, name := range xattrLimitNames {
			call, stop := context.WithTimeout(ctx, 5*time.Second)
			data, err := s.backend.get(call, daemon, name)
			stop()
			if err != nil {
				return nil, fmt.Errorf("selected effective xattr gate %s %s: %w", daemon, name, err)
			}
			value, err := decodeXattrLimit(data, name)
			if err != nil {
				return nil, fmt.Errorf("selected effective xattr gate %s %s: %w", daemon, name, err)
			}
			result[daemon][name] = value
		}
	}
	return result, nil
}

func (s *xattrLimitsScope) waitEffective(ctx context.Context, target map[string]map[string]string) (map[string]map[string]string, error) {
	for {
		current, err := s.effective(ctx)
		if err != nil {
			return nil, err
		}
		match := len(current) == len(target)
		for daemon, options := range target {
			for name, value := range options {
				if current[daemon][name] != value {
					match = false
				}
			}
		}
		if match {
			return current, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("all-owned-OSD xattr effective gate: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func decodeXattrLimit(data []byte, name string) (string, error) {
	if len(data) > 4096 {
		return "", errors.New("selected config get exceeds bound")
	}
	// CephContext opens config_get as the first object section (ceph_context.cc
	// 574,613); JSONFormatter::print_name (Formatter.cc 210) omits the root
	// section name. AdminSocket v0.2.0 returns raw stdout unchanged.
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil || len(result) != 1 || result[name] == nil {
		return "", errors.New("selected config get has invalid shape")
	}
	var text string
	if err := json.Unmarshal(result[name], &text); err != nil {
		text = string(result[name])
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return "", errors.New("selected config get lacks nonnegative integer")
	}
	return strconv.FormatUint(value, 10), nil
}
