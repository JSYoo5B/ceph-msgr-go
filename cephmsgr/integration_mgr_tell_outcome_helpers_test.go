package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only receipts of the fixture's closed observe-mgr-tell control
// (integration/fixture/mgr_tell_outcome.go). It reads and changes the one
// daemon-local option log_max_recent through the owned manager's admin socket,
// independently of every Messenger connection. The option's command schema is
// shared with the MON Tell cell and stays in test files.
var (
	// The first two phases record the baseline and read the applied value. The
	// overwrite and restore phases each submit their one native mutation at
	// most once; every other phase only reads.
	mgrTellPhases = map[string]bool{"initial": true, "applied": true, "overwrite": true, "recovered": true, "restore": true, "restored": true,
		"bystander-initial": true, "bystander-final": true}
	mgrTellTargets = map[string]bool{"a": true, "b": true}
)

// nativeMgrTellManager is one fresh native MgrMap read.
type nativeMgrTellManager struct {
	OK         bool     `json:"ok"`
	Error      string   `json:"error"`
	Available  bool     `json:"available"`
	ActiveName string   `json:"active_name"`
	ActiveGID  uint64   `json:"active_gid"`
	Epoch      uint32   `json:"epoch"`
	Standbys   []string `json:"standbys"`
}

// nativeMgrTellCall is one admin-socket command: whether its CLI exited
// successfully, and for config get the daemon's original output bytes.
type nativeMgrTellCall struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	Data  []byte `json:"data"`
}

type nativeMgrTellDiff struct {
	OK        bool                `json:"ok"`
	Error     string              `json:"error"`
	DataBytes int                 `json:"data_bytes"`
	Valid     bool                `json:"valid"`
	Entry     *nativeMonTellEntry `json:"entry"`
	// Structural facts about the reply; never other options.
	TopLevelKeys []string `json:"top_level_keys"`
	DiffEntries  int      `json:"diff_entries"`
}

type nativeMgrTellViews struct {
	ManagerBefore nativeMgrTellManager `json:"manager_before"`
	ConfigGet     nativeMgrTellCall    `json:"config_get"`
	ConfigDiff    nativeMgrTellDiff    `json:"config_diff"`
	ManagerAfter  nativeMgrTellManager `json:"manager_after"`
}

// nativeMgrTellMutation is the one native overwrite or restoration. Its
// arguments are derived by the fixture from its own recorded baseline.
type nativeMgrTellMutation struct {
	Action string   `json:"action"`
	Args   []string `json:"args"`
	// Refused: the fixture could not verify, immediately before the call, the
	// daemon incarnation that supplied the baseline, and sent nothing.
	Refused     bool   `json:"refused"`
	OK          bool   `json:"ok"`
	Error       string `json:"error"`
	OutputBytes int    `json:"output_bytes"`
	// SameIncarnationAfter: the map reads after the call still showed the
	// baseline's incarnation.
	SameIncarnationAfter bool `json:"same_incarnation_after"`
}

// applied reports that this is the named mutation, that it was sent to the
// baseline's daemon incarnation and returned successfully, and that the same
// incarnation was still there afterwards.
func (m *nativeMgrTellMutation) applied(action string) bool {
	return m != nil && m.Action == action && !m.Refused && m.OK && m.SameIncarnationAfter
}

func (m *nativeMgrTellMutation) summary() string {
	if m == nil {
		return "none"
	}
	return fmt.Sprintf("%s args=%v refused=%t ok=%t same_incarnation_after=%t output_bytes=%d error=%q", m.Action, m.Args, m.Refused, m.OK, m.SameIncarnationAfter, m.OutputBytes, m.Error)
}

type nativeMgrTellReceipt struct {
	RequestID int64                  `json:"request_id"`
	Token     string                 `json:"token"`
	Phase     string                 `json:"phase"`
	Target    string                 `json:"target"`
	Baseline  *nativeMonTellBaseline `json:"baseline"`
	Views     nativeMgrTellViews     `json:"views"`
	Mutation  *nativeMgrTellMutation `json:"mutation,omitempty"`
}

// observeNativeMgrTell requests one closed observation. The token, phase and
// target are validated before a request file is published, so an invalid
// closed-action request is never delegated to the fixture.
func observeNativeMgrTell(ctx context.Context, control, token, phase, target string) (nativeMgrTellReceipt, error) {
	var receipt nativeMgrTellReceipt
	if !monTellTokenPattern.MatchString(token) || !mgrTellPhases[phase] || !mgrTellTargets[target] {
		return receipt, errors.New("invalid native MGR Tell observation request")
	}
	if err := testcluster.RequestControl(ctx, control, "observe-mgr-tell", "mgr-tell-observed", token, phase, target); err != nil {
		return receipt, err
	}
	if err := readNativeReceiptFile(filepath.Join(control, "native-mgr-tell-no-replay-"+token+"."+phase+".json"), 256<<10, errors.New("native MGR Tell observation exceeds its receipt bound"), &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase || receipt.Target != target {
		return receipt, errors.New("native MGR Tell observation has a mismatched identity")
	}
	return receipt, nil
}

// nativeMgrTellDaemon is the manager identity one observation must be
// bracketed by: the available active daemon's exact name and GID.
type nativeMgrTellDaemon struct {
	name string
	gid  uint64
}

// nativeMgrTellBracket reports the one active manager that both fresh MgrMap
// reads around an observation show. Any unavailable, failed or differing read
// yields no bracket.
func nativeMgrTellBracket(views nativeMgrTellViews) (nativeMgrTellDaemon, bool) {
	before, after := views.ManagerBefore, views.ManagerAfter
	if !before.OK || !after.OK || !before.Available || !after.Available || before.ActiveName == "" || before.ActiveGID == 0 ||
		before.ActiveName != after.ActiveName || before.ActiveGID != after.ActiveGID {
		return nativeMgrTellDaemon{}, false
	}
	return nativeMgrTellDaemon{before.ActiveName, before.ActiveGID}, true
}

// nativeMgrTellState reads the complete state of one receipt: the daemon's
// effective value and whether (and with which value) a runtime override exists.
func nativeMgrTellState(views nativeMgrTellViews) (nativeMonTellState, bool) {
	effective, ok := parseMonTellEffective(views.ConfigGet.Data)
	diff := views.ConfigDiff
	if !views.ConfigGet.OK || !ok || !diff.OK || !diff.Valid {
		return nativeMonTellState{}, false
	}
	state := nativeMonTellState{value: effective}
	if diff.Entry != nil && diff.Entry.OverridePresent {
		if diff.Entry.Override == nil {
			return nativeMonTellState{}, false
		}
		override, err := strconv.ParseInt(*diff.Entry.Override, 10, 64)
		if err != nil {
			return nativeMonTellState{}, false
		}
		state.override = &override
	}
	return state, true
}

func sameMonTellState(a, b nativeMonTellState) bool {
	return a.value == b.value && (a.override == nil) == (b.override == nil) && (a.override == nil || *a.override == *b.override)
}

// nativeMgrTellMatches requires the wanted state on the admin socket of the
// named daemon while that same daemon, by name and GID, is the available
// active manager before and after the reads.
func nativeMgrTellMatches(receipt nativeMgrTellReceipt, daemon nativeMgrTellDaemon, want nativeMonTellState) bool {
	bracket, stable := nativeMgrTellBracket(receipt.Views)
	state, ok := nativeMgrTellState(receipt.Views)
	return stable && ok && receipt.Target == daemon.name && bracket == daemon && sameMonTellState(state, want)
}

func nativeMgrTellSummary(receipt nativeMgrTellReceipt) string {
	views := receipt.Views
	bracket, stable := nativeMgrTellBracket(views)
	effective, _ := parseMonTellEffective(views.ConfigGet.Data)
	override := "absent"
	if entry := views.ConfigDiff.Entry; entry != nil && entry.OverridePresent {
		override = "present"
		if entry.Override != nil {
			override += "=" + *entry.Override
		}
	}
	mutation := receipt.Mutation.summary()
	return fmt.Sprintf("phase=%s request=%d target=mgr.%s active=%s gid=%d epochs=%d..%d bracketed=%t get_ok=%t get=%q effective=%d diff_ok=%t diff_valid=%t diff_keys=%v diff_entries=%d override=%s mutation=%s",
		receipt.Phase, receipt.RequestID, receipt.Target, bracket.name, bracket.gid, views.ManagerBefore.Epoch, views.ManagerAfter.Epoch, stable,
		views.ConfigGet.OK, views.ConfigGet.Data, effective, views.ConfigDiff.OK, views.ConfigDiff.Valid, views.ConfigDiff.TopLevelKeys,
		views.ConfigDiff.DiffEntries, override, mutation)
}

// requireNativeMgrTell takes one observation and requires its state. The
// observation follows an explicit barrier (a withheld reply, or a native
// mutation that returned), so it is not repeated until it agrees.
func requireNativeMgrTell(t *testing.T, ctx context.Context, control, token, phase string, daemon nativeMgrTellDaemon, want nativeMonTellState) nativeMgrTellReceipt {
	t.Helper()
	receipt, err := observeNativeMgrTell(ctx, control, token, phase, daemon.name)
	if err != nil {
		t.Fatal("native MGR Tell observation", phase, err)
	}
	if !nativeMgrTellMatches(receipt, daemon, want) {
		t.Fatalf("native MGR Tell %s does not show the wanted state on the named daemon; %s", phase, nativeMgrTellSummary(receipt))
	}
	return receipt
}

// awaitNativeMgrTell repeats one read-only observation until the bracketed
// daemon shows the wanted state. It is for the mode without a decoded reply
// barrier: the wait ends on the state itself. One observation is two separate
// admin-socket calls, so while the daemon applies the change it can show the
// previous value with the wanted override or the reverse; only such a state is
// read again. Any other value or override, an unreadable or unbracketed read,
// or the end of the original caller fails immediately.
func awaitNativeMgrTell(t *testing.T, ctx context.Context, control, token, phase string, daemon nativeMgrTellDaemon, want, previous nativeMonTellState, finished <-chan error) nativeMgrTellReceipt {
	t.Helper()
	for {
		receipt, err := observeNativeMgrTell(ctx, control, token, phase, daemon.name)
		if err != nil {
			t.Fatal("native MGR Tell observation", phase, err)
		}
		if nativeMgrTellMatches(receipt, daemon, want) {
			return receipt
		}
		if !nativeMgrTellBetween(receipt, daemon, previous, want) {
			t.Fatalf("native MGR Tell %s shows a state outside the change from the previous to the wanted one on the receiving daemon; %s", phase, nativeMgrTellSummary(receipt))
		}
		select {
		case err := <-finished:
			t.Fatal("original caller ended before the native state was observed", phase, err)
		case <-ctx.Done():
			t.Fatalf("native MGR Tell %s did not reach the wanted state: %v; %s", phase, ctx.Err(), nativeMgrTellSummary(receipt))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// nativeMgrTellBetween reports whether a bracketed, readable receipt of the
// named daemon shows a value and an override that each belong to the previous
// or to the wanted state.
func nativeMgrTellBetween(receipt nativeMgrTellReceipt, daemon nativeMgrTellDaemon, previous, want nativeMonTellState) bool {
	bracket, stable := nativeMgrTellBracket(receipt.Views)
	state, ok := nativeMgrTellState(receipt.Views)
	if !stable || !ok || receipt.Target != daemon.name || bracket != daemon {
		return false
	}
	override := func(of nativeMonTellState) bool {
		return sameMonTellState(nativeMonTellState{override: state.override}, nativeMonTellState{override: of.override})
	}
	return (state.value == previous.value || state.value == want.value) && (override(previous) || override(want))
}

// Test-only local submission metadata. Write counts are local net.Conn facts,
// never server execution counts.
type mgrTellOutcomeIdentity struct {
	// API is the public method under test. FramesDecoded is false in secure
	// modes, where no write, transaction or reply is identified.
	API            string `json:"api"`
	FramesDecoded  bool   `json:"crc_frames_decoded"`
	ClientGlobalID uint64 `json:"client_global_id"`
	Manager        string `json:"manager"`
	ManagerGID     uint64 `json:"manager_gid"`
	// Receiver is the fixture role of the endpoint whose connection carried the
	// mutation; HeldConnection is that connection.
	Receiver       string `json:"receiver,omitempty"`
	HeldConnection uint64 `json:"held_connection,omitempty"`
	// LaterDials lists, in secure modes, every connection dialed to the
	// receiver after the held one; none is claimed to have carried the
	// recovery read.
	LaterDials       []uint64         `json:"later_dials_to_receiver,omitempty"`
	Baseline         int64            `json:"baseline"`
	OverridePresent  bool             `json:"baseline_override_present"`
	NearMiss         *monOutcomeWrite `json:"near_miss_passed,omitempty"`
	Submitted        *monOutcomeWrite `json:"submitted,omitempty"`
	WithheldReply    *tellHeldReply   `json:"withheld_reply,omitempty"`
	MatchingWrites   int              `json:"matching_local_writes"`
	PaddedWrites     int32            `json:"padded_local_writes"`
	OutcomeUnknown   bool             `json:"outcome_unknown"`
	Recovery         *monOutcomeWrite `json:"recovery,omitempty"`
	RecoveryGlobalID uint64           `json:"recovery_client_global_id,omitempty"`
}

func writeMgrTellOutcomeIdentity(t *testing.T, control string, current bool, value mgrTellOutcomeIdentity) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret metadata must also be readable by the non-root controller.
	name := "mgr-tell-outcome-identity.json"
	if current {
		name = "mgr-current-tell-outcome-identity.json"
	}
	if err := os.WriteFile(filepath.Join(control, name), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
