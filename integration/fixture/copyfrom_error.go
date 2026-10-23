package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const copyFromErrorOption = "osd_debug_inject_copyfrom_error"
const copyFromLateErrorSelector = "^TestCephOSDCopyFromLateErrorIntegration$"

type copyFromErrorReceipt struct {
	Schema           int    `json:"schema_version"`
	Source           string `json:"source_commit"`
	OSD              int    `json:"osd"`
	MaxChunk         uint64 `json:"max_chunk"`
	Before           string `json:"effective_before"`
	Applied          string `json:"effective_applied"`
	Restored         string `json:"effective_restored"`
	ApplyAttempted   bool   `json:"apply_attempted"`
	ApplyReturned    bool   `json:"apply_returned_success"`
	RestoreAttempted bool   `json:"restore_attempted"`
	RestoreReturned  bool   `json:"restore_returned_success"`
}

type copyFromErrorScope struct {
	admin    func(context.Context, string, ...string) ([]byte, error)
	receipt  copyFromErrorReceipt
	restored bool
}

// This local daemon option belongs only to an exclusive disposable fixture.
// Its declaration is not runtime-mutability evidence: apply and restore each
// require a separate effective config get. No mutation is automatically retried.
func beginCopyFromError(ctx context.Context, id int, admin func(context.Context, string, ...string) ([]byte, error)) (*copyFromErrorScope, error) {
	s := &copyFromErrorScope{admin: admin, receipt: copyFromErrorReceipt{Schema: 1, Source: nativeEncoderCommit, OSD: id}}
	if id < 0 {
		return s, errors.New("invalid owned copy-error OSD")
	}
	var err error
	s.receipt.Before, err = s.get(ctx, copyFromErrorOption)
	if err != nil {
		return s, err
	}
	if s.receipt.Before != "false" {
		return s, errors.New("copy-error calibration requires an initially disabled injector")
	}
	chunk, err := s.get(ctx, "osd_copyfrom_max_chunk")
	if err != nil {
		return s, err
	}
	s.receipt.MaxChunk, err = strconv.ParseUint(chunk, 10, 64)
	if err != nil || s.receipt.MaxChunk == 0 {
		return s, errors.New("invalid native copy chunk limit")
	}
	s.receipt.ApplyAttempted = true
	if err = s.set(ctx, "true"); err != nil {
		return s, err
	}
	s.receipt.ApplyReturned = true
	s.receipt.Applied, err = s.get(ctx, copyFromErrorOption)
	if err == nil && s.receipt.Applied != "true" {
		err = errors.New("copy-error option did not become effective at runtime")
	}
	return s, err
}

func (s *copyFromErrorScope) get(ctx context.Context, option string) (string, error) {
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	raw, err := s.admin(call, "osd."+strconv.Itoa(s.receipt.OSD), "config", "get", option)
	if err != nil {
		return "", err
	}
	if option != copyFromErrorOption {
		return decodeXattrLimit(raw, option)
	}
	if len(raw) > 4096 {
		return "", errors.New("copy-error config get exceeds bound")
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil || len(fields) != 1 || fields[option] == nil {
		return "", errors.New("invalid copy-error config get shape")
	}
	var value string
	if err = json.Unmarshal(fields[option], &value); err != nil {
		value = string(fields[option])
	}
	if value != "true" && value != "false" {
		return "", errors.New("copy-error config get lacks a boolean")
	}
	return value, nil
}

func (s *copyFromErrorScope) set(ctx context.Context, value string) error {
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	raw, err := s.admin(call, "osd."+strconv.Itoa(s.receipt.OSD), "config", "set", copyFromErrorOption, value)
	if len(raw) > 4096 {
		return errors.Join(err, errors.New("copy-error config set exceeds bound"))
	}
	return err
}

func (s *copyFromErrorScope) Restore(ctx context.Context) error {
	if s == nil || s.restored || !s.receipt.ApplyAttempted {
		return nil
	}
	// Even when the apply call failed, its execution may be uncertain. Observe
	// effective state before deciding whether one restoration is necessary.
	current, err := s.get(ctx, copyFromErrorOption)
	if err != nil {
		return err
	}
	if current != s.receipt.Before {
		if s.receipt.RestoreAttempted {
			return errors.New("copy-error restoration was already attempted; refusing mutation replay")
		}
		s.receipt.RestoreAttempted = true
		if err = s.set(ctx, s.receipt.Before); err != nil {
			return err
		}
		s.receipt.RestoreReturned = true
	}
	s.receipt.Restored, err = s.get(ctx, copyFromErrorOption)
	if err == nil && s.receipt.Restored != s.receipt.Before {
		err = errors.New("copy-error restoration readback differs")
	}
	if err == nil {
		s.restored = true
	}
	return err
}

func (s *copyFromErrorScope) save(out string) error {
	raw, err := json.Marshal(s.receipt)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "osd-copyfrom-error-option.json"), raw, 0644)
}

func (c *fixtureCluster) copyFromErrorControl(ctx context.Context, out string, fields []string) error {
	if len(fields) != 2 || c.profile.osd != "1" || c.profile.osdStore != "bluestore" || c.profile.osdReplicas != "1" || c.profile.testRun != copyFromLateErrorSelector {
		return errors.New("copy-error injection requires its exclusive single-BlueStore selector")
	}
	var err error
	switch fields[1] {
	case "apply":
		if c.copyFromError != nil {
			return errors.New("copy-error scope already owned")
		}
		osds := c.cluster.OSDs()
		if len(osds) != 1 {
			return errors.New("copy-error injection requires exactly one owned OSD")
		}
		c.copyFromError, err = beginCopyFromError(ctx, osds[0].ID, c.cluster.AdminSocket)
		c.copyFromErrorOut = out
	case "restore":
		if c.copyFromError == nil {
			return errors.New("copy-error scope is not owned")
		}
		err = c.copyFromError.Restore(ctx)
	default:
		return errors.New("invalid copy-error scope action")
	}
	if c.copyFromError != nil {
		err = errors.Join(err, c.copyFromError.save(out))
	}
	if err != nil {
		return fmt.Errorf("owned copy-error option: %w", err)
	}
	return publishAck(out, "osd-copyfrom-error."+fields[0])
}
