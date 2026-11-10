package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only receipts of the fixture's closed observe-daemon-command control
// (integration/fixture/daemon_command_outcome.go). It reads and changes owned
// state of one OSD, one PG of an OSD or one MDS through that daemon's admin
// socket, independently of every Messenger connection. The command schemas
// stay in test files.
var (
	daemonCommandPhases     = map[string]bool{"initial": true, "applied": true, "overwrite": true, "recovered": true, "restore": true, "restored": true}
	daemonCommandOSDPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,3})$`)
	daemonCommandPGPattern  = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})\.[0-9a-f]{1,8}$`)
	daemonCommandMDSPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// nativeDaemonCommandTarget is the closed target of one cell: an OSD, one PG
// of an OSD, or an MDS.
type nativeDaemonCommandTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	PGID string `json:"pgid,omitempty"`
}

func (t nativeDaemonCommandTarget) valid() bool {
	switch t.Kind {
	case "osd":
		return daemonCommandOSDPattern.MatchString(t.ID) && t.PGID == ""
	case "pg":
		return daemonCommandOSDPattern.MatchString(t.ID) && daemonCommandPGPattern.MatchString(t.PGID)
	case "mds":
		return daemonCommandMDSPattern.MatchString(t.ID) && t.PGID == ""
	}
	return false
}

func (t nativeDaemonCommandTarget) daemon() string {
	if t.Kind == "mds" {
		return "mds." + t.ID
	}
	return "osd." + t.ID
}

// nativeDaemonCommandIdentity is one fresh native map read of the daemon.
type nativeDaemonCommandIdentity struct {
	OK          bool   `json:"ok"`
	Error       string `json:"error"`
	Daemon      string `json:"daemon"`
	Up          bool   `json:"up"`
	Incarnation uint64 `json:"incarnation"`
	Address     string `json:"address"`
}

type nativeDaemonCommandPG struct {
	OK              bool   `json:"ok"`
	Error           string `json:"error"`
	DataBytes       int    `json:"data_bytes"`
	Valid           bool   `json:"valid"`
	PGID            string `json:"pgid"`
	State           string `json:"state"`
	ScrubberPresent bool   `json:"scrubber_present"`
	FlagPresent     bool   `json:"flag_present"`
}

type nativeDaemonCommandViews struct {
	DaemonBefore nativeDaemonCommandIdentity `json:"daemon_before"`
	ConfigGet    *nativeMgrTellCall          `json:"config_get,omitempty"`
	ConfigDiff   *nativeMgrTellDiff          `json:"config_diff,omitempty"`
	PGQuery      *nativeDaemonCommandPG      `json:"pg_query,omitempty"`
	DaemonAfter  nativeDaemonCommandIdentity `json:"daemon_after"`
}

type nativeDaemonCommandReceipt struct {
	RequestID  int64                     `json:"request_id"`
	Token      string                    `json:"token"`
	Phase      string                    `json:"phase"`
	Target     nativeDaemonCommandTarget `json:"target"`
	Restorable bool                      `json:"restorable"`
	Baseline   *nativeMonTellBaseline    `json:"baseline,omitempty"`
	Views      nativeDaemonCommandViews  `json:"views"`
	Mutation   *nativeMgrTellMutation    `json:"mutation,omitempty"`
}

// observeNativeDaemonCommand requests one closed observation. The token, phase
// and target are validated before a request file is published, so an invalid
// closed-action request is never delegated to the fixture.
func observeNativeDaemonCommand(ctx context.Context, control, token, phase string, target nativeDaemonCommandTarget) (nativeDaemonCommandReceipt, error) {
	var receipt nativeDaemonCommandReceipt
	if !monTellTokenPattern.MatchString(token) || !daemonCommandPhases[phase] || !target.valid() {
		return receipt, errors.New("invalid native daemon command observation request")
	}
	args := []string{token, phase, target.Kind, target.ID}
	if target.Kind == "pg" {
		args = append(args, target.PGID)
	}
	if err := testcluster.RequestControl(ctx, control, "observe-daemon-command", "daemon-command-observed", args...); err != nil {
		return receipt, err
	}
	if err := readNativeReceiptFile(filepath.Join(control, "native-daemon-command-no-replay-"+token+"."+phase+".json"), 256<<10, errors.New("native daemon command observation exceeds its receipt bound"), &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase || receipt.Target != target {
		return receipt, errors.New("native daemon command observation has a mismatched identity")
	}
	return receipt, nil
}

// nativeDaemonIncarnation is the daemon identity one observation must be
// bracketed by: the up daemon, its map incarnation (the OSD's up_from epoch or
// the MDS GID) and its public v2 address with nonce.
type nativeDaemonIncarnation struct {
	daemon      string
	incarnation uint64
	address     string
}

// nativeDaemonCommandBracket reports the one daemon incarnation that both
// fresh map reads around an observation show.
func nativeDaemonCommandBracket(views nativeDaemonCommandViews) (nativeDaemonIncarnation, bool) {
	before, after := views.DaemonBefore, views.DaemonAfter
	if !before.OK || !after.OK || !before.Up || !after.Up || before.Daemon == "" || before.Incarnation == 0 || before.Address == "" ||
		before.Daemon != after.Daemon || before.Incarnation != after.Incarnation || before.Address != after.Address {
		return nativeDaemonIncarnation{}, false
	}
	return nativeDaemonIncarnation{before.Daemon, before.Incarnation, before.Address}, true
}

// nativeDaemonCommandState reads the complete owned state of one receipt. The
// option cells show the effective value and runtime override; the pg cell
// shows 1 when the primary's query lists the scrubber's test counter and 0
// when it does not, and is unreadable unless the scrubber section exists.
func nativeDaemonCommandState(target nativeDaemonCommandTarget, views nativeDaemonCommandViews) (nativeMonTellState, bool) {
	if target.Kind == "pg" {
		query := views.PGQuery
		if query == nil || !query.OK || !query.Valid || query.PGID != target.PGID || !query.ScrubberPresent || views.ConfigGet != nil || views.ConfigDiff != nil {
			return nativeMonTellState{}, false
		}
		if query.FlagPresent {
			return nativeMonTellState{value: 1}, true
		}
		return nativeMonTellState{}, true
	}
	if views.ConfigGet == nil || views.ConfigDiff == nil || views.PGQuery != nil {
		return nativeMonTellState{}, false
	}
	return nativeMgrTellState(nativeMgrTellViews{ConfigGet: *views.ConfigGet, ConfigDiff: *views.ConfigDiff})
}

// nativeDaemonCommandMatches requires the wanted state on the admin socket of
// the target daemon while that same incarnation brackets the reads.
func nativeDaemonCommandMatches(receipt nativeDaemonCommandReceipt, target nativeDaemonCommandTarget, daemon nativeDaemonIncarnation, want nativeMonTellState) bool {
	bracket, stable := nativeDaemonCommandBracket(receipt.Views)
	state, ok := nativeDaemonCommandState(target, receipt.Views)
	return stable && ok && receipt.Target == target && daemon.daemon == target.daemon() && bracket == daemon && sameMonTellState(state, want)
}

func nativeDaemonCommandSummary(receipt nativeDaemonCommandReceipt) string {
	views := receipt.Views
	bracket, stable := nativeDaemonCommandBracket(views)
	state := "unreadable"
	if value, ok := nativeDaemonCommandState(receipt.Target, views); ok {
		state = fmt.Sprintf("value=%d override=absent", value.value)
		if value.override != nil {
			state = fmt.Sprintf("value=%d override=%d", value.value, *value.override)
		}
	}
	detail := ""
	if query := views.PGQuery; query != nil {
		detail = fmt.Sprintf(" query_ok=%t query_valid=%t query_bytes=%d pg_state=%q scrubber=%t counter_listed=%t", query.OK, query.Valid, query.DataBytes, query.State, query.ScrubberPresent, query.FlagPresent)
	} else if views.ConfigGet != nil && views.ConfigDiff != nil {
		detail = fmt.Sprintf(" get_ok=%t get=%q diff_ok=%t diff_valid=%t diff_keys=%v diff_entries=%d", views.ConfigGet.OK, views.ConfigGet.Data,
			views.ConfigDiff.OK, views.ConfigDiff.Valid, views.ConfigDiff.TopLevelKeys, views.ConfigDiff.DiffEntries)
	}
	mutation := receipt.Mutation.summary()
	return fmt.Sprintf("phase=%s request=%d target=%s pgid=%q daemon=%s incarnation=%d address=%s bracketed=%t restorable=%t %s%s mutation=%s",
		receipt.Phase, receipt.RequestID, receipt.Target.daemon(), receipt.Target.PGID, bracket.daemon, bracket.incarnation, bracket.address, stable,
		receipt.Restorable, state, detail, mutation)
}

// requireNativeDaemonCommand takes one observation and requires its state. The
// observation follows an explicit barrier (a withheld reply, or a native
// mutation that returned), so it is not repeated until it agrees.
func requireNativeDaemonCommand(t *testing.T, ctx context.Context, control, token, phase string, target nativeDaemonCommandTarget, daemon nativeDaemonIncarnation, want nativeMonTellState) nativeDaemonCommandReceipt {
	t.Helper()
	receipt, err := observeNativeDaemonCommand(ctx, control, token, phase, target)
	if err != nil {
		t.Fatal("native daemon command observation", phase, err)
	}
	if !nativeDaemonCommandMatches(receipt, target, daemon, want) {
		t.Fatalf("native daemon command %s does not show the wanted state on the target daemon; %s", phase, nativeDaemonCommandSummary(receipt))
	}
	return receipt
}

// Test-only local submission metadata. Write counts are local net.Conn facts,
// never server execution counts.
type daemonCommandOutcomeIdentity struct {
	API               string                    `json:"api"`
	Target            nativeDaemonCommandTarget `json:"target"`
	ClientGlobalID    uint64                    `json:"client_global_id"`
	Incarnation       uint64                    `json:"daemon_incarnation"`
	Address           string                    `json:"daemon_address"`
	Baseline          int64                     `json:"baseline"`
	OverridePresent   bool                      `json:"baseline_override_present"`
	NearMiss          *monOutcomeWrite          `json:"near_miss_passed,omitempty"`
	Submitted         *monOutcomeWrite          `json:"submitted,omitempty"`
	WithheldReply     *tellHeldReply            `json:"withheld_reply,omitempty"`
	MatchingWrites    int                       `json:"matching_local_writes"`
	PaddedWrites      int32                     `json:"padded_local_writes"`
	OutcomeUnknown    bool                      `json:"outcome_unknown"`
	CallerTransaction uint64                    `json:"caller_submission_transaction,omitempty"`
	Recovery          *monOutcomeWrite          `json:"recovery,omitempty"`
}

func writeDaemonCommandOutcomeIdentity(t *testing.T, control string, value daemonCommandOutcomeIdentity) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret metadata must also be readable by the non-root controller.
	if err := os.WriteFile(filepath.Join(control, "daemon-command-outcome-identity-"+value.Target.Kind+".json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
