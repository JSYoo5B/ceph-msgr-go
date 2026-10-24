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
const copyFromChunkOption = "osd_copyfrom_max_chunk"
const copyFromLateErrorSelector = "^TestCephOSDCopyFromLateErrorIntegration$"
const copyFromLateErrorMarker = "CEPH_MSGR_NATIVE_COPYFROM_LATE_ERROR"

// The multi-chunk cell lowers the owned daemon's copy chunk below its 64 KiB
// source so that the bounded native observer still covers every byte.
const copyFromMultiChunkSelector = "^TestCephOSDCopyFromMultiChunkLateErrorIntegration$"
const copyFromMultiChunkMarker = "CEPH_MSGR_NATIVE_COPYFROM_MULTICHUNK_ERROR"
const copyFromMultiChunkLimit = 32 << 10

func (p profile) copyFromErrorFixture() bool {
	return p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "1" && p.osdProfile == "legacy-v6"
}

func (p profile) copyFromLateErrorSelected() bool {
	return p.testRun == copyFromLateErrorSelector && p.copyFromErrorFixture()
}

func (p profile) copyFromMultiChunkSelected() bool {
	return p.testRun == copyFromMultiChunkSelector && p.copyFromErrorFixture()
}

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
	// The chunk fields stay zero/false unless a selected cell requests an
	// override. MaxChunk is always the original effective limit.
	ChunkOverride         uint64 `json:"chunk_override,omitempty"`
	ChunkApplied          uint64 `json:"chunk_applied,omitempty"`
	ChunkRestored         uint64 `json:"chunk_restored,omitempty"`
	ChunkApplyAttempted   bool   `json:"chunk_apply_attempted,omitempty"`
	ChunkApplyReturned    bool   `json:"chunk_apply_returned_success,omitempty"`
	ChunkRestoreAttempted bool   `json:"chunk_restore_attempted,omitempty"`
	ChunkRestoreReturned  bool   `json:"chunk_restore_returned_success,omitempty"`
}

type copyFromErrorScope struct {
	admin         func(context.Context, string, ...string) ([]byte, error)
	receipt       copyFromErrorReceipt
	restored      bool
	chunkRestored bool
}

// This local daemon option belongs only to an exclusive disposable fixture.
// Its declaration is not runtime-mutability evidence: apply and restore each
// require a separate effective config get. No mutation is automatically retried.
// A nonzero chunk lowers osd_copyfrom_max_chunk first, with its own readback,
// before the injector is enabled; zero leaves the chunk limit untouched.
func beginCopyFromError(ctx context.Context, id int, chunk uint64, admin func(context.Context, string, ...string) ([]byte, error)) (*copyFromErrorScope, error) {
	s := &copyFromErrorScope{admin: admin, receipt: copyFromErrorReceipt{Schema: 1, Source: nativeEncoderCommit, OSD: id, ChunkOverride: chunk}}
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
	s.receipt.MaxChunk, err = s.chunk(ctx)
	if err != nil {
		return s, err
	}
	if chunk != 0 {
		if chunk >= s.receipt.MaxChunk {
			return s, errors.New("copy chunk override must lower the effective limit")
		}
		s.receipt.ChunkApplyAttempted = true
		if err = s.set(ctx, copyFromChunkOption, strconv.FormatUint(chunk, 10)); err != nil {
			return s, err
		}
		s.receipt.ChunkApplyReturned = true
		if s.receipt.ChunkApplied, err = s.chunk(ctx); err != nil {
			return s, err
		}
		if s.receipt.ChunkApplied != chunk {
			return s, errors.New("copy chunk override did not become effective at runtime")
		}
	}
	s.receipt.ApplyAttempted = true
	if err = s.set(ctx, copyFromErrorOption, "true"); err != nil {
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

func (s *copyFromErrorScope) chunk(ctx context.Context) (uint64, error) {
	text, err := s.get(ctx, copyFromChunkOption)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("invalid native copy chunk limit")
	}
	return value, nil
}

func (s *copyFromErrorScope) set(ctx context.Context, option, value string) error {
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	raw, err := s.admin(call, "osd."+strconv.Itoa(s.receipt.OSD), "config", "set", option, value)
	if len(raw) > 4096 {
		return errors.Join(err, errors.New("copy-error config set exceeds bound"))
	}
	return err
}

// Restore returns the injector first, then any lowered chunk limit. Each
// setting is observed before at most one restoring set.
func (s *copyFromErrorScope) Restore(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return errors.Join(s.restoreInjector(ctx), s.restoreChunk(ctx))
}

func (s *copyFromErrorScope) restoreInjector(ctx context.Context) error {
	if s.restored || !s.receipt.ApplyAttempted {
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
		if err = s.set(ctx, copyFromErrorOption, s.receipt.Before); err != nil {
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

// The lowered chunk limit is restored with the same observe-then-set-once
// policy. An uncertain chunk apply still owns its restoration.
func (s *copyFromErrorScope) restoreChunk(ctx context.Context) error {
	if s.chunkRestored || !s.receipt.ChunkApplyAttempted {
		return nil
	}
	current, err := s.chunk(ctx)
	if err != nil {
		return err
	}
	if current != s.receipt.MaxChunk {
		if s.receipt.ChunkRestoreAttempted {
			return errors.New("copy chunk restoration was already attempted; refusing mutation replay")
		}
		s.receipt.ChunkRestoreAttempted = true
		if err = s.set(ctx, copyFromChunkOption, strconv.FormatUint(s.receipt.MaxChunk, 10)); err != nil {
			return err
		}
		s.receipt.ChunkRestoreReturned = true
	}
	s.receipt.ChunkRestored, err = s.chunk(ctx)
	if err == nil && s.receipt.ChunkRestored != s.receipt.MaxChunk {
		err = errors.New("copy chunk restoration readback differs")
	}
	if err == nil {
		s.chunkRestored = true
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

// Fields: request identity, action and, for staging only, the owned PG ID.
// history and staging are read-only observations of the multi-chunk cell.
func (c *fixtureCluster) copyFromErrorControl(ctx context.Context, out string, fields []string) error {
	multi := c.profile.copyFromMultiChunkSelected()
	if len(fields) < 2 || (!multi && !c.profile.copyFromLateErrorSelected()) {
		return errors.New("copy-error injection requires its exclusive single-BlueStore selector")
	}
	observation := fields[1] == "history" || fields[1] == "staging"
	if observation && !multi {
		return errors.New("copy staging observations require the multi-chunk selector")
	}
	if (fields[1] == "staging") != (len(fields) == 3) || len(fields) > 3 {
		return errors.New("invalid copy-error control arguments")
	}
	osds := c.cluster.OSDs()
	if len(osds) != 1 {
		return errors.New("copy-error injection requires exactly one owned OSD")
	}
	var err error
	switch fields[1] {
	case "apply":
		if c.copyFromError != nil {
			return errors.New("copy-error scope already owned")
		}
		var chunk uint64
		if multi {
			chunk = copyFromMultiChunkLimit
		}
		c.copyFromError, err = beginCopyFromError(ctx, osds[0].ID, chunk, c.cluster.AdminSocket)
		c.copyFromErrorOut = out
	case "restore":
		if c.copyFromError == nil {
			return errors.New("copy-error scope is not owned")
		}
		err = c.copyFromError.Restore(ctx)
	case "history":
		err = observeCopyGetHistory(ctx, out, osds[0].ID, c.cluster.AdminSocket)
	case "staging":
		err = observeCopyStaging(ctx, out, osds[0].ID, fields[2], c.cluster.AdminSocket)
	default:
		return errors.New("invalid copy-error scope action")
	}
	if c.copyFromError != nil && !observation {
		err = errors.Join(err, c.copyFromError.save(out))
	}
	if err != nil {
		return fmt.Errorf("owned copy-error option: %w", err)
	}
	return publishAck(out, "osd-copyfrom-error."+fields[0])
}
