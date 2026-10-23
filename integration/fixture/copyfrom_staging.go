package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Read-only native observations for the multi-chunk late-copy cell. Both use
// only the owned OSD's local admin socket and never mutate daemon state.
const (
	copyHistoryLimit    = 8 << 20
	copyHistoryMaxOps   = 64
	copyHistoryCapacity = 1024
	copyHistoryMaxAll   = copyHistoryCapacity
	copyHistoryMaxText  = 4096
	copyStagingMax      = 4096
	copyStagingLimit    = 4 << 20
	copyStagingMaxTemp  = 64
	copyCollectionLimit = 1 << 20
)

var copyStagingPG = regexp.MustCompile(`^[0-9]+\.[0-9a-f]+$`)

type copyGetOp struct {
	Description string `json:"description"`
	InitiatedAt string `json:"initiated_at"`
}

type copyGetHistoryReceipt struct {
	Schema   int             `json:"schema_version"`
	Source   string          `json:"source_commit"`
	OSD      int             `json:"osd"`
	Size     json.RawMessage `json:"history_size,omitempty"`
	Duration json.RawMessage `json:"history_duration,omitempty"`
	Ops      int             `json:"ops"`
	All      []copyGetOp     `json:"all_ops"`
	CopyGets []copyGetOp     `json:"copy_gets"`
}

// observeCopyGetHistory retains every completed copy-get description from the
// owned daemon's op history. The OSD issues one copy-get per chunk to the
// source (PrimaryLogPG::_copy_some), so the count and "max" field are native
// cursor evidence. Absence from a bounded history is not proof of absence.
func observeCopyGetHistory(ctx context.Context, out string, id int, admin func(context.Context, string, ...string) ([]byte, error)) error {
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	raw, err := admin(call, "osd."+strconv.Itoa(id), "dump_historic_ops")
	if err != nil {
		return err
	}
	receipt, err := decodeCopyGetHistory(raw)
	if err != nil {
		return err
	}
	receipt.OSD = id
	return writeCopyReceipt(out, "osd-copyfrom-staging-history.json", receipt)
}

func decodeCopyGetHistory(raw []byte) (copyGetHistoryReceipt, error) {
	receipt := copyGetHistoryReceipt{Schema: 1, Source: nativeEncoderCommit, CopyGets: []copyGetOp{}}
	if len(raw) > copyHistoryLimit {
		return receipt, errors.New("native op history exceeds bound")
	}
	var dump struct {
		Size     json.RawMessage `json:"size"`
		Duration json.RawMessage `json:"duration"`
		Ops      *[]copyGetOp    `json:"ops"`
	}
	if err := json.Unmarshal(raw, &dump); err != nil || dump.Ops == nil {
		return receipt, errors.New("invalid native op history shape")
	}
	receipt.Size, receipt.Duration, receipt.Ops = dump.Size, dump.Duration, len(*dump.Ops)
	if len(*dump.Ops) > copyHistoryMaxAll {
		return receipt, errors.New("native op history exceeds entry bound")
	}
	// Retain every bounded description so an absent copy-get remains
	// diagnosable against what the daemon actually recorded.
	receipt.All = *dump.Ops
	for _, op := range *dump.Ops {
		if len(op.Description) > copyHistoryMaxText {
			return receipt, errors.New("native op description exceeds bound")
		}
		if strings.Contains(op.Description, "[copy-get ") {
			receipt.CopyGets = append(receipt.CopyGets, op)
		}
	}
	if len(receipt.CopyGets) > copyHistoryMaxOps {
		return receipt, errors.New("native copy-get history exceeds bound")
	}
	return receipt, nil
}

type copyStagingReceipt struct {
	Schema      int      `json:"schema_version"`
	Source      string   `json:"source_commit"`
	OSD         int      `json:"osd"`
	Collection  string   `json:"collection"`
	Collections int      `json:"collections"`
	Max         int      `json:"max"`
	Objects     int      `json:"objects"`
	Complete    bool     `json:"complete"`
	Temp        []string `json:"temp"`
}

// observeCopyStaging enumerates the owned PG collection through BlueStore's
// "bluestore list" admin command (BlueAdmin.cc). Starting at GHMIN includes
// the temporary range, where generate_temp_object places partial copies
// (pool -2-pool). Fewer than max entries establishes a complete enumeration.
func observeCopyStaging(ctx context.Context, out string, id int, pg string, admin func(context.Context, string, ...string) ([]byte, error)) error {
	if !copyStagingPG.MatchString(pg) {
		return errors.New("invalid owned staging PG")
	}
	daemon := "osd." + strconv.Itoa(id)
	collection := pg + "_head"
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	raw, err := admin(call, daemon, "bluestore", "collections")
	if err != nil {
		return err
	}
	if len(raw) > copyCollectionLimit {
		return errors.New("native collection list exceeds bound")
	}
	names := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names[line] = true
		}
	}
	if !names[collection] {
		return errors.New("owned PG collection is absent from the native store")
	}
	raw, err = admin(call, daemon, "bluestore", "list", collection, "GHMIN", strconv.Itoa(copyStagingMax))
	if err != nil {
		return err
	}
	receipt, err := decodeCopyStaging(raw)
	if err != nil {
		return err
	}
	receipt.OSD, receipt.Collection, receipt.Collections = id, collection, len(names)
	return writeCopyReceipt(out, "osd-copyfrom-staging-list.json", receipt)
}

func decodeCopyStaging(raw []byte) (copyStagingReceipt, error) {
	receipt := copyStagingReceipt{Schema: 1, Source: nativeEncoderCommit, Max: copyStagingMax, Temp: []string{}}
	if len(raw) > copyStagingLimit {
		return receipt, errors.New("native object list exceeds bound")
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line == "{}" {
			continue
		}
		// ghobject_t prints #pool:hash:namespace:key:name:snap# for an
		// unsharded object without generation (hobject.cc operator<<).
		pool, _, ok := strings.Cut(strings.TrimPrefix(line, "#"), ":")
		value, err := strconv.ParseInt(pool, 10, 64)
		if !ok || err != nil || !strings.HasPrefix(line, "#") || !strings.HasSuffix(line, "#") {
			return receipt, errors.New("invalid native object list entry")
		}
		receipt.Objects++
		if value <= -2 {
			receipt.Temp = append(receipt.Temp, line)
		}
	}
	if len(receipt.Temp) > copyStagingMaxTemp {
		return receipt, errors.New("native temporary objects exceed bound")
	}
	receipt.Complete = receipt.Objects < receipt.Max
	return receipt, nil
}

func writeCopyReceipt(out, name string, receipt any) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(out, "copy-receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(raw); err == nil {
		err = file.Chmod(0644)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(out, name))
}
