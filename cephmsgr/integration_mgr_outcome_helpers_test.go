package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only receipts of tools/mgr_command_outcome_oracle.py. Command schemas
// and their interpretation stay in this test file, not in the product API.
type nativeMgrIdentity struct {
	Code       int32   `json:"code"`
	Status     string  `json:"status"`
	ActiveName *string `json:"active_name"`
	ActiveGID  *uint64 `json:"active_gid"`
	Epoch      *uint64 `json:"epoch"`
	Available  *bool   `json:"available"`
}

type nativeMgrOptionEntry struct {
	Section string `json:"section"`
	Name    string `json:"name"`
	Value   string `json:"value"`
	Mask    string `json:"mask"`
}

type nativeMgrEntries struct {
	Code       int32                  `json:"code"`
	Status     string                 `json:"status"`
	DataBytes  int                    `json:"data_bytes"`
	DataSHA256 string                 `json:"data_sha256"`
	Entries    []nativeMgrOptionEntry `json:"entries"`
}

type nativeMgrViews struct {
	ManagerBefore  nativeMgrIdentity      `json:"manager_before"`
	BalancerStatus nativeMonOutcomeResult `json:"balancer_status"`
	ConfigGet      nativeMonOutcomeResult `json:"config_get"`
	ConfigEntries  nativeMgrEntries       `json:"config_entries"`
	ManagerAfter   nativeMgrIdentity      `json:"manager_after"`
}

type nativeMgrBaseline struct {
	Present bool   `json:"present"`
	Value   string `json:"value"`
}

type nativeMgrOutcomeReceipt struct {
	RequestID   int64                   `json:"request_id"`
	Token       string                  `json:"token"`
	Phase       string                  `json:"phase"`
	Baseline    *nativeMgrBaseline      `json:"baseline"`
	Views       nativeMgrViews          `json:"views"`
	Restoration *nativeMonOutcomeResult `json:"restoration,omitempty"`
}

func observeNativeMgrOutcome(ctx context.Context, control, token, phase string) (nativeMgrOutcomeReceipt, error) {
	var receipt nativeMgrOutcomeReceipt
	if err := testcluster.RequestControl(ctx, control, "observe-mgr-command", "mgr-command-observed", token, phase); err != nil {
		return receipt, err
	}
	if err := readNativeReceiptFile(filepath.Join(control, "native-mgr-no-replay-"+token+"."+phase+".json"), 256<<10, errors.New("native MGR observation exceeds its receipt bound"), &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase {
		return receipt, errors.New("native MGR observation has a mismatched identity")
	}
	return receipt, nil
}

// nativeMgrMode returns the active balancer module's mode from the raw native
// balancer status bytes, or "" when the signed code or JSON does not carry one.
func nativeMgrMode(views nativeMgrViews) string {
	var status struct {
		Mode string `json:"mode"`
	}
	if views.BalancerStatus.Code != 0 || json.Unmarshal(views.BalancerStatus.Data, &status) != nil {
		return ""
	}
	return status.Mode
}

// nativeMgrStable reports whether the active MGR name/GID bracket the views.
func nativeMgrStable(views nativeMgrViews) (string, uint64, bool) {
	before, after := views.ManagerBefore, views.ManagerAfter
	if before.Code != 0 || after.Code != 0 || before.ActiveName == nil || after.ActiveName == nil ||
		before.ActiveGID == nil || after.ActiveGID == nil || before.Available == nil || after.Available == nil ||
		!*before.Available || !*after.Available || *before.ActiveName != *after.ActiveName || *before.ActiveGID != *after.ActiveGID {
		return "", 0, false
	}
	return *before.ActiveName, *before.ActiveGID, true
}

// nativeMgrState is the complete independent state that one receipt must show:
// MGR effective mode, MON effective value and the explicit entry set.
type nativeMgrState struct {
	mode    string
	entries []nativeMgrOptionEntry
}

func explicitMgrMode(value string) []nativeMgrOptionEntry {
	return []nativeMgrOptionEntry{{Section: "mgr", Name: "mgr/balancer/mode", Value: value}}
}

func nativeMgrMatches(views nativeMgrViews, want nativeMgrState) bool {
	entries := views.ConfigEntries.Entries
	if entries == nil {
		entries = []nativeMgrOptionEntry{}
	}
	wanted := want.entries
	if wanted == nil {
		wanted = []nativeMgrOptionEntry{}
	}
	return nativeMgrMode(views) == want.mode && views.ConfigGet.Code == 0 &&
		bytes.Equal(views.ConfigGet.Data, []byte(want.mode+"\n")) && views.ConfigEntries.Code == 0 &&
		views.ConfigEntries.Entries != nil && reflect.DeepEqual(entries, wanted)
}

// requireNativeMgrState repeats only read observations until one bracketed
// receipt shows the wanted state. Any third mode fails immediately.
func requireNativeMgrState(t *testing.T, ctx context.Context, control, token, phase string, want nativeMgrState, previous string, gid func(uint64) bool, finished <-chan error) nativeMgrOutcomeReceipt {
	t.Helper()
	for {
		receipt, err := observeNativeMgrOutcome(ctx, control, token, phase)
		if err != nil {
			t.Fatal("native MGR observation", phase, err)
		}
		_, active, stable := nativeMgrStable(receipt.Views)
		if stable && gid(active) && nativeMgrMatches(receipt.Views, want) {
			return receipt
		}
		if mode := nativeMgrMode(receipt.Views); mode != "" && mode != want.mode && mode != previous {
			t.Fatalf("native MGR %s: unexpected mode; %s", phase, nativeMgrSummary(receipt))
		}
		select {
		case err := <-finished:
			t.Fatal("original caller ended before the native state was observed", phase, err)
		case <-ctx.Done():
			t.Fatalf("native MGR %s did not reach the wanted state: %v; %s", phase, ctx.Err(), nativeMgrSummary(receipt))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func nativeMgrSummary(receipt nativeMgrOutcomeReceipt) string {
	views := receipt.Views
	name, gid, stable := nativeMgrStable(views)
	return fmt.Sprintf("phase=%s request=%d active=%s gid=%d bracketed=%t status_code=%d status_bytes=%d mode=%q config_get_code=%d config_get=%q config_get_status=%q entries_code=%d entries=%+v",
		receipt.Phase, receipt.RequestID, name, gid, stable, views.BalancerStatus.Code, len(views.BalancerStatus.Data),
		nativeMgrMode(views), views.ConfigGet.Code, views.ConfigGet.Data, views.ConfigGet.Status,
		views.ConfigEntries.Code, views.ConfigEntries.Entries)
}

// Test-only local submission metadata. Write counts are local net.Conn facts,
// never server execution counts.
type mgrOutcomeIdentity struct {
	ClientGlobalID   uint64           `json:"client_global_id"`
	OriginalManager  string           `json:"original_manager"`
	OriginalGID      uint64           `json:"original_manager_gid"`
	Submitted        *monOutcomeWrite `json:"submitted,omitempty"`
	PaddedWrites     int32            `json:"padded_local_writes"`
	OutcomeUnknown   bool             `json:"outcome_unknown"`
	RecoveryManager  string           `json:"recovery_manager,omitempty"`
	RecoveryGID      uint64           `json:"recovery_manager_gid,omitempty"`
	Recovery         *monOutcomeWrite `json:"recovery,omitempty"`
	RecoveryGlobalID uint64           `json:"recovery_client_global_id,omitempty"`
}

func writeMgrOutcomeIdentity(t *testing.T, control string, value mgrOutcomeIdentity) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret metadata must also be readable by the non-root controller.
	if err := os.WriteFile(filepath.Join(control, "mgr-command-outcome-identity.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
