package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only receipts of tools/mon_tell_outcome_oracle.py. The command schema
// of the one daemon-local option stays in test files, not in the product API.
const (
	monTellOption = "log_max_recent"
	// log_max_recent is a positive in-memory crash-log retention count. The
	// cell only raises it, by at most two slots, and never above this ceiling.
	monTellCeiling = 20000
)

var (
	monTellTokenPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	monTellPhases       = map[string]bool{"initial": true, "applied": true, "overwritten": true, "recovered": true, "restore": true, "restored": true}
	monTellTargets      = map[string]bool{"a": true, "b": true, "c": true}
)

// monTellOutcomeValues returns the distinct candidates A=baseline+1 and
// B=baseline+2. It refuses a baseline that could push B above the ceiling or
// below the option's positive minimum, so no retained evidence is truncated.
func monTellOutcomeValues(baseline int64) (a, b int64, err error) {
	if baseline < 1 || baseline > monTellCeiling-2 {
		return 0, 0, fmt.Errorf("baseline %d is outside the bounded range 1..%d for %s", baseline, monTellCeiling-2, monTellOption)
	}
	return baseline + 1, baseline + 2, nil
}

func monTellSetCommand(value int64, padding int) Command {
	encoded, _ := json.Marshal(map[string]any{"prefix": "config set", "var": monTellOption, "val": []string{strconv.FormatInt(value, 10)}, "format": "json"})
	return Command{JSON: append(encoded, []byte(strings.Repeat(" ", padding))...)}
}

func monTellGetCommand() Command {
	return Command{JSON: []byte(`{"prefix":"config get","var":"` + monTellOption + `","format":"json"}`)}
}

// parseMonTellEffective decodes the raw config get reply of the owned option.
func parseMonTellEffective(data []byte) (int64, bool) {
	var reply map[string]json.RawMessage
	if json.Unmarshal(data, &reply) != nil || len(reply) != 1 {
		return 0, false
	}
	raw, ok := reply[monTellOption]
	if !ok {
		return 0, false
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		var number int64
		if json.Unmarshal(raw, &number) != nil {
			return 0, false
		}
		return number, true
	}
	number, err := strconv.ParseInt(text, 10, 64)
	return number, err == nil
}

type nativeMonTellIdentity struct {
	Code   int32   `json:"code"`
	Status string  `json:"status"`
	Name   *string `json:"name"`
	Rank   *int64  `json:"rank"`
	State  *string `json:"state"`
}

type nativeMonTellEntry struct {
	Sources         []string `json:"sources"`
	OverridePresent bool     `json:"override_present"`
	Override        *string  `json:"override"`
	Final           *string  `json:"final"`
}

type nativeMonTellDiff struct {
	Code      int32               `json:"code"`
	Status    string              `json:"status"`
	DataBytes int                 `json:"data_bytes"`
	Valid     bool                `json:"valid"`
	Entry     *nativeMonTellEntry `json:"entry"`
	// Structural facts about the reply; never other options.
	TopLevelKeys []string `json:"top_level_keys"`
	DiffEntries  int      `json:"diff_entries"`
}

type nativeMonTellViews struct {
	DaemonBefore nativeMonTellIdentity  `json:"daemon_before"`
	ConfigGet    nativeMonOutcomeResult `json:"config_get"`
	ConfigDiff   nativeMonTellDiff      `json:"config_diff"`
	DaemonAfter  nativeMonTellIdentity  `json:"daemon_after"`
}

type nativeMonTellBaseline struct {
	Present   bool   `json:"present"`
	Value     string `json:"value"`
	Effective string `json:"effective"`
}

type nativeMonTellReceipt struct {
	RequestID   int64                   `json:"request_id"`
	Token       string                  `json:"token"`
	Phase       string                  `json:"phase"`
	Target      string                  `json:"target"`
	Baseline    *nativeMonTellBaseline  `json:"baseline"`
	Views       nativeMonTellViews      `json:"views"`
	Restoration *nativeMonOutcomeResult `json:"restoration,omitempty"`
}

// observeNativeMonTell requests one closed observation. The token, phase and
// target are validated before a request file is published, so an invalid
// closed-action request is never delegated to the fixture.
func observeNativeMonTell(ctx context.Context, control, token, phase, target string) (nativeMonTellReceipt, error) {
	var receipt nativeMonTellReceipt
	if !monTellTokenPattern.MatchString(token) || !monTellPhases[phase] || !monTellTargets[target] {
		return receipt, errors.New("invalid native MON Tell observation request")
	}
	if err := testcluster.RequestControl(ctx, control, "observe-mon-tell", "mon-tell-observed", token, phase, target); err != nil {
		return receipt, err
	}
	file, err := os.Open(filepath.Join(control, "native-mon-tell-no-replay-"+token+"."+phase+".json"))
	if err != nil {
		return receipt, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return receipt, errors.New("native MON Tell observation exceeds its receipt bound")
	}
	if err = json.Unmarshal(data, &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase || receipt.Target != target {
		return receipt, errors.New("native MON Tell observation has a mismatched identity")
	}
	return receipt, nil
}

// nativeMonTellStable reports whether the same named daemon brackets the views.
func nativeMonTellStable(views nativeMonTellViews, target string) (int64, bool) {
	before, after := views.DaemonBefore, views.DaemonAfter
	if before.Code != 0 || after.Code != 0 || before.Name == nil || after.Name == nil || before.Rank == nil || after.Rank == nil ||
		*before.Name != target || *after.Name != target || *before.Rank != *after.Rank {
		return 0, false
	}
	return *before.Rank, true
}

// nativeMonTellState is the complete state one receipt must show: the daemon's
// effective value and whether (and with which value) a runtime override exists.
type nativeMonTellState struct {
	value    int64
	override *int64
}

func nativeMonTellMatches(views nativeMonTellViews, target string, want nativeMonTellState) bool {
	if _, stable := nativeMonTellStable(views, target); !stable {
		return false
	}
	effective, ok := parseMonTellEffective(views.ConfigGet.Data)
	diff := views.ConfigDiff
	if views.ConfigGet.Code != 0 || !ok || effective != want.value || diff.Code != 0 || !diff.Valid {
		return false
	}
	if want.override == nil {
		return diff.Entry == nil || !diff.Entry.OverridePresent
	}
	text := strconv.FormatInt(*want.override, 10)
	return diff.Entry != nil && diff.Entry.OverridePresent && diff.Entry.Override != nil && *diff.Entry.Override == text
}

func nativeMonTellSummary(receipt nativeMonTellReceipt) string {
	views := receipt.Views
	rank, stable := nativeMonTellStable(views, receipt.Target)
	effective, _ := parseMonTellEffective(views.ConfigGet.Data)
	override := "absent"
	if entry := views.ConfigDiff.Entry; entry != nil && entry.OverridePresent {
		override = "present"
		if entry.Override != nil {
			override += "=" + *entry.Override
		}
	}
	return fmt.Sprintf("phase=%s request=%d target=mon.%s rank=%d bracketed=%t get_code=%d get=%q effective=%d diff_code=%d diff_valid=%t diff_keys=%v diff_entries=%d override=%s",
		receipt.Phase, receipt.RequestID, receipt.Target, rank, stable, views.ConfigGet.Code, views.ConfigGet.Data, effective,
		views.ConfigDiff.Code, views.ConfigDiff.Valid, views.ConfigDiff.TopLevelKeys, views.ConfigDiff.DiffEntries, override)
}

// requireNativeMonTellState repeats only read observations until one bracketed
// receipt shows the wanted state. A value that is neither the wanted nor the
// previous one fails immediately.
func requireNativeMonTellState(t *testing.T, ctx context.Context, control, token, phase, target string, want nativeMonTellState, previous *int64, finished <-chan error) nativeMonTellReceipt {
	t.Helper()
	for {
		receipt, err := observeNativeMonTell(ctx, control, token, phase, target)
		if err != nil {
			t.Fatal("native MON Tell observation", phase, err)
		}
		if nativeMonTellMatches(receipt.Views, target, want) {
			return receipt
		}
		if value, ok := parseMonTellEffective(receipt.Views.ConfigGet.Data); ok && value != want.value && (previous == nil || value != *previous) {
			t.Fatalf("native MON Tell %s: unexpected value; %s", phase, nativeMonTellSummary(receipt))
		}
		select {
		case err := <-finished:
			t.Fatal("original caller ended before the native state was observed", phase, err)
		case <-ctx.Done():
			t.Fatalf("native MON Tell %s did not reach the wanted state: %v; %s", phase, ctx.Err(), nativeMonTellSummary(receipt))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Test-only local submission metadata. Write counts are local net.Conn facts,
// never server execution counts.
type monTellOutcomeIdentity struct {
	// The primary MON session's authenticated global ID. The directed Tell
	// connection authenticates separately with its own CephX client; its global
	// ID is not observed here.
	PrimaryGlobalID             uint64           `json:"primary_client_global_id"`
	Target                      string           `json:"target"`
	Baseline                    int64            `json:"baseline"`
	Submitted                   *monOutcomeWrite `json:"submitted,omitempty"`
	PaddedWrites                int32            `json:"padded_local_writes"`
	OutcomeUnknown              bool             `json:"outcome_unknown"`
	Recovery                    *monOutcomeWrite `json:"recovery,omitempty"`
	RecoveryPrimaryGlobalID     uint64           `json:"recovery_primary_client_global_id,omitempty"`
	TellSessionGlobalIDObserved bool             `json:"tell_session_global_id_observed"`
}

func writeMonTellOutcomeIdentity(t *testing.T, control string, value monTellOutcomeIdentity) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret metadata must also be readable by the non-root controller.
	if err := os.WriteFile(filepath.Join(control, "mon-tell-outcome-identity.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
