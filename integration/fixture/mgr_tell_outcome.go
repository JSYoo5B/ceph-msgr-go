package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Closed, test-only native observations of one manager-local runtime option.
//
// Every request names one owned manager ("a" or "b") and reaches it through
// that daemon's admin socket handle, a local Unix socket that is independent of
// every Messenger connection of the Go client under test. An observation
// records only: two fresh native MgrMap reads around it (availability, active
// name and GID, epoch, standby names), the daemon's effective log_max_recent
// (config get) and that option's entry from config diff get (override
// presence/value and final value). A configuration dump is never persisted.
//
// The consumer supplies a token, a phase and a target, never a command, an
// option or a value. The two mutations come from this file's own recorded
// baseline and are each submitted at most once per token:
//
//   - overwrite sets the runtime override to baseline+2, the distinct value B
//     that makes a replay of the Go client's baseline+1 visible;
//   - restore unsets the runtime override when none existed, or sets the one
//     original override again.
//
// A mutation whose CLI did not exit successfully is recorded as it returned
// and is never repeated: its outcome stays uncertain for the caller.
//
// Native behavior relied upon (Tentacle v20.2.4): the common admin-socket
// "config set/get/unset" and "config diff get" handlers registered in every
// daemon process (src/common/ceph_context.cc), and the runtime override layer
// they change (src/common/config.cc).
const (
	mgrTellOption = "log_max_recent"
	mgrTellPrefix = "native-mgr-tell-no-replay-"
	// log_max_recent is a positive in-memory crash-log retention count. The
	// cell only raises it by two slots: a baseline above the ceiling minus two
	// is refused, so no retained evidence is truncated.
	mgrTellCeiling         = 20000
	mgrTellHighestBaseline = mgrTellCeiling - 2
	mgrTellReplyLimit      = 1 << 20
	mgrTellCallTimeout     = 10 * time.Second
	mgrTellRequestTimeout  = 45 * time.Second
)

var (
	mgrTellTokenPattern  = regexp.MustCompile(`^[0-9a-f]{32}$`)
	mgrTellNumberPattern = regexp.MustCompile(`^[0-9]{1,18}$`)
	mgrTellPathPattern   = regexp.MustCompile(`\S*/\S*`)
	// Phases bound to the baseline's own target, and the two read-only phases
	// of the other owned manager.
	mgrTellSubjectPhases   = map[string]bool{"initial": true, "applied": true, "overwrite": true, "recovered": true, "restore": true, "restored": true}
	mgrTellBystanderPhases = map[string]bool{"bystander-initial": true, "bystander-final": true}
	mgrTellTargets         = map[string]bool{"a": true, "b": true}
)

type mgrTellManager struct {
	OK         bool     `json:"ok"`
	Error      string   `json:"error"`
	Available  bool     `json:"available"`
	ActiveName string   `json:"active_name"`
	ActiveGID  uint64   `json:"active_gid"`
	Epoch      uint32   `json:"epoch"`
	Standbys   []string `json:"standbys"`
}

type mgrTellCall struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	Data  []byte `json:"data"`
}

type mgrTellEntry struct {
	Sources         []string `json:"sources"`
	OverridePresent bool     `json:"override_present"`
	Override        *string  `json:"override"`
	Final           *string  `json:"final"`
}

type mgrTellDiff struct {
	OK        bool          `json:"ok"`
	Error     string        `json:"error"`
	DataBytes int           `json:"data_bytes"`
	Valid     bool          `json:"valid"`
	Entry     *mgrTellEntry `json:"entry"`
	// Structural facts about the reply; never other options.
	TopLevelKeys []string `json:"top_level_keys"`
	DiffEntries  int      `json:"diff_entries"`
}

type mgrTellViews struct {
	ManagerBefore mgrTellManager `json:"manager_before"`
	ConfigGet     mgrTellCall    `json:"config_get"`
	ConfigDiff    mgrTellDiff    `json:"config_diff"`
	ManagerAfter  mgrTellManager `json:"manager_after"`
}

type mgrTellBaseline struct {
	Present   bool   `json:"present"`
	Value     string `json:"value"`
	Effective string `json:"effective"`
}

type mgrTellMutation struct {
	Action      string   `json:"action"`
	Args        []string `json:"args"`
	OK          bool     `json:"ok"`
	Error       string   `json:"error"`
	OutputBytes int      `json:"output_bytes"`
}

type mgrTellReceipt struct {
	RequestID int64            `json:"request_id"`
	Token     string           `json:"token"`
	Phase     string           `json:"phase"`
	Target    string           `json:"target"`
	Baseline  *mgrTellBaseline `json:"baseline"`
	Views     mgrTellViews     `json:"views"`
	Mutation  *mgrTellMutation `json:"mutation,omitempty"`
	State     string           `json:"state,omitempty"`
}

type mgrTellOwner struct {
	Token    string          `json:"token"`
	Target   string          `json:"target"`
	Baseline mgrTellBaseline `json:"baseline"`
}

// mgrTellBackend is the native surface of one request: a fresh MgrMap read and
// one admin-socket command of one owned manager.
type mgrTellBackend struct {
	managerMap  func(context.Context) (managerPreparationMap, error)
	adminSocket func(ctx context.Context, target string, args ...string) ([]byte, error)
}

func (c *fixtureCluster) controlManagerTellRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "observe-mgr-tell")
	if err != nil || fields == nil {
		return err
	}
	// Only the ordinary two-manager fixture owns this control; fixtures that
	// rotate tickets, expire sessions or restrict modes keep their own cells.
	if c.profile.mgrCount != "2" || c.profile.authEpoch == "1" || c.profile.idleSessions == "1" || c.profile.shortTickets == "1" || c.profile.expireTickets == "1" || c.profile.modeRejection != "" {
		return errors.New("invalid owned MGR Tell observation: not the ordinary two-manager fixture")
	}
	backend := mgrTellBackend{
		managerMap: c.observeManagerMap,
		adminSocket: func(call context.Context, target string, args ...string) ([]byte, error) {
			return c.adminSocket(call, "mgr."+target, args...)
		},
	}
	call, cancel := context.WithTimeout(ctx, mgrTellRequestTimeout)
	defer cancel()
	if err := mgrTellOutcome(call, out, fields, backend); err != nil {
		return err
	}
	return publishAck(out, "mgr-tell-observed."+fields[0])
}

// mgrTellDiagnostic keeps a bounded printable error with every path-like
// token masked, so a failed call is attributable without private locations.
func mgrTellDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	text := []rune(mgrTellPathPattern.ReplaceAllString(err.Error(), "<path>"))
	for i, r := range text {
		if r < 0x20 || r > 0x7e {
			text[i] = '?'
		}
	}
	if len(text) > 200 {
		text = text[:200]
	}
	return string(text)
}

// mgrTellNumber returns the canonical decimal text of a bounded non-negative
// integer or digit string.
func mgrTellNumber(raw json.RawMessage) *string {
	var text string
	if json.Unmarshal(raw, &text) != nil {
		var number json.Number
		if json.Unmarshal(raw, &number) != nil {
			return nil
		}
		text = number.String()
	}
	if !mgrTellNumberPattern.MatchString(text) {
		return nil
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil
	}
	canonical := strconv.FormatInt(value, 10)
	return &canonical
}

func mgrTellInBound(text string) bool {
	value, err := strconv.ParseInt(text, 10, 64)
	return err == nil && value >= 1 && value <= mgrTellHighestBaseline && strconv.FormatInt(value, 10) == text
}

// mgrTellEffective decodes a config get reply that holds only the owned option.
func mgrTellEffective(call mgrTellCall) *string {
	var reply map[string]json.RawMessage
	if !call.OK || json.Unmarshal(call.Data, &reply) != nil || len(reply) != 1 {
		return nil
	}
	raw, ok := reply[mgrTellOption]
	if !ok {
		return nil
	}
	return mgrTellNumber(raw)
}

// mgrTellOptionDiff keeps only the owned option's diff entry. The native reply
// can enumerate unrelated settings of any JSON type; they stay in memory.
func mgrTellOptionDiff(data []byte, err error) mgrTellDiff {
	observed := mgrTellDiff{OK: err == nil, Error: mgrTellDiagnostic(err), DataBytes: len(data)}
	var value map[string]json.RawMessage
	if err != nil || len(data) > mgrTellReplyLimit || json.Unmarshal(data, &value) != nil {
		return observed
	}
	for key := range value {
		if len(key) <= 32 {
			observed.TopLevelKeys = append(observed.TopLevelKeys, key)
		}
	}
	sort.Strings(observed.TopLevelKeys)
	if len(observed.TopLevelKeys) > 8 {
		observed.TopLevelKeys = observed.TopLevelKeys[:8]
	}
	// The pinned reply nests entries under "diff"; accept the option object
	// directly only when it names the owned option.
	var diff map[string]json.RawMessage
	if nested, ok := value["diff"]; ok {
		if json.Unmarshal(nested, &diff) != nil || diff == nil {
			return observed
		}
	} else if _, ok := value[mgrTellOption]; ok {
		diff = value
	} else {
		return observed
	}
	observed.DiffEntries, observed.Valid = len(diff), true
	raw, ok := diff[mgrTellOption]
	if !ok {
		return observed
	}
	var entry map[string]json.RawMessage
	if json.Unmarshal(raw, &entry) != nil || entry == nil {
		// The owned option is listed in a form that cannot show its override.
		observed.Valid = false
		return observed
	}
	projected := &mgrTellEntry{Sources: []string{}}
	for source := range entry {
		if len(source) <= 32 {
			projected.Sources = append(projected.Sources, source)
		}
	}
	sort.Strings(projected.Sources)
	if override, ok := entry["override"]; ok {
		projected.OverridePresent, projected.Override = true, mgrTellNumber(override)
	}
	if final, ok := entry["final"]; ok {
		projected.Final = mgrTellNumber(final)
	}
	observed.Entry = projected
	return observed
}

func (b mgrTellBackend) manager(ctx context.Context) mgrTellManager {
	state, err := b.managerMap(ctx)
	observed := mgrTellManager{OK: err == nil, Error: mgrTellDiagnostic(err), Standbys: []string{}}
	if err != nil {
		return observed
	}
	observed.Available, observed.ActiveName, observed.ActiveGID, observed.Epoch = state.Available, state.ActiveName, state.ActiveGID, state.Epoch
	for _, standby := range state.Standbys {
		if len(standby.Name) <= 64 && len(observed.Standbys) < 8 {
			observed.Standbys = append(observed.Standbys, standby.Name)
		}
	}
	sort.Strings(observed.Standbys)
	return observed
}

func (b mgrTellBackend) call(ctx context.Context, target string, args ...string) ([]byte, error) {
	call, cancel := context.WithTimeout(ctx, mgrTellCallTimeout)
	defer cancel()
	return b.adminSocket(call, target, args...)
}

func (b mgrTellBackend) views(ctx context.Context, target string) mgrTellViews {
	var views mgrTellViews
	views.ManagerBefore = b.manager(ctx)
	data, err := b.call(ctx, target, "config", "get", mgrTellOption)
	views.ConfigGet = mgrTellCall{OK: err == nil, Error: mgrTellDiagnostic(err)}
	if err == nil && len(data) <= 4096 {
		views.ConfigGet.Data = data
	} else if err == nil {
		views.ConfigGet.OK, views.ConfigGet.Error = false, "config get reply exceeds its bound"
	}
	views.ConfigDiff = mgrTellOptionDiff(b.call(ctx, target, "config", "diff", "get", mgrTellOption))
	views.ManagerAfter = b.manager(ctx)
	return views
}

// mgrTellRestorable returns the baseline one closed mutation can restore: the
// named daemon is the same available active manager around the reads, and its
// in-bound effective value is either not overridden at runtime or carried by
// one runtime override equal to it.
func mgrTellRestorable(views mgrTellViews, target string) *mgrTellBaseline {
	before, after := views.ManagerBefore, views.ManagerAfter
	if !before.OK || !after.OK || !before.Available || !after.Available || before.ActiveName != target || after.ActiveName != target ||
		before.ActiveGID == 0 || before.ActiveGID != after.ActiveGID {
		return nil
	}
	effective := mgrTellEffective(views.ConfigGet)
	diff := views.ConfigDiff
	if effective == nil || !mgrTellInBound(*effective) || !diff.OK || !diff.Valid {
		return nil
	}
	if diff.Entry == nil || !diff.Entry.OverridePresent {
		return &mgrTellBaseline{Effective: *effective}
	}
	if diff.Entry.Override != nil && *diff.Entry.Override == *effective && (diff.Entry.Final == nil || *diff.Entry.Final == *effective) {
		return &mgrTellBaseline{Present: true, Value: *effective, Effective: *effective}
	}
	return nil
}

func mgrTellOverwriteArgs(baseline mgrTellBaseline) ([]string, error) {
	if !mgrTellInBound(baseline.Effective) {
		return nil, errors.New("recorded MGR Tell baseline is outside the bound")
	}
	value, _ := strconv.ParseInt(baseline.Effective, 10, 64)
	return []string{"config", "set", mgrTellOption, strconv.FormatInt(value+2, 10)}, nil
}

func mgrTellRestoreArgs(baseline mgrTellBaseline) ([]string, error) {
	if !mgrTellInBound(baseline.Effective) || baseline.Present && baseline.Value != baseline.Effective || !baseline.Present && baseline.Value != "" {
		return nil, errors.New("recorded MGR Tell baseline is outside the bound")
	}
	if baseline.Present {
		return []string{"config", "set", mgrTellOption, baseline.Value}, nil
	}
	return []string{"config", "unset", mgrTellOption}, nil
}

func mgrTellWriteExclusive(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(data, '\n'))
	return errors.Join(writeErr, file.Close())
}

func mgrTellPublish(out, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(out, "mgr-tell-outcome-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(append(data, '\n'))
	if err = errors.Join(writeErr, file.Chmod(0644), file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(out, name))
}

// mgrTellJournal appends only closed owned-option views to its metadata file:
// never a configuration, a manager map or credentials.
func mgrTellJournal(out string, receipt mgrTellReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(out, "mgr-tell-outcome-native.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(data, '\n'))
	return errors.Join(writeErr, file.Close())
}

func mgrTellOutcome(ctx context.Context, out string, fields []string, backend mgrTellBackend) error {
	if len(fields) != 4 || !mgrTellTokenPattern.MatchString(fields[1]) || !mgrTellTargets[fields[3]] ||
		!mgrTellSubjectPhases[fields[2]] && !mgrTellBystanderPhases[fields[2]] {
		return errors.New("invalid owned MGR Tell observation")
	}
	requestID, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || requestID <= 0 {
		return errors.New("invalid owned MGR Tell observation")
	}
	token, phase, target := fields[1], fields[2], fields[3]
	receipt := mgrTellReceipt{RequestID: requestID, Token: token, Phase: phase, Target: target}
	ownerPath := filepath.Join(out, mgrTellPrefix+token+".baseline.json")
	if phase == "initial" {
		receipt.Views = backend.views(ctx, target)
		receipt.Baseline = mgrTellRestorable(receipt.Views, target)
		if receipt.Baseline != nil {
			// A repeated initial request cannot replace the first baseline.
			if err := mgrTellWriteExclusive(ownerPath, mgrTellOwner{token, target, *receipt.Baseline}); err != nil {
				return fmt.Errorf("record the MGR Tell baseline: %w", err)
			}
		}
	} else {
		data, err := os.ReadFile(ownerPath)
		var owner mgrTellOwner
		if err != nil || len(data) > 4096 || json.Unmarshal(data, &owner) != nil || owner.Token != token || !mgrTellTargets[owner.Target] ||
			(owner.Target == target) != mgrTellSubjectPhases[phase] {
			return errors.New("MGR Tell outcome has no matching baseline receipt for this target")
		}
		receipt.Baseline = &owner.Baseline
		if phase == "overwrite" || phase == "restore" {
			var args []string
			if phase == "overwrite" {
				args, err = mgrTellOverwriteArgs(owner.Baseline)
			} else {
				args, err = mgrTellRestoreArgs(owner.Baseline)
			}
			if err != nil {
				return err
			}
			// Claim the one allowed mutation before crossing the native call.
			// A call that does not return leaves this marker uncertain; no
			// retry may repeat it.
			started := receipt
			started.State = phase + "-started-outcome-unknown"
			if err := mgrTellWriteExclusive(filepath.Join(out, mgrTellPrefix+token+"."+phase+"-started.json"), started); err != nil {
				return fmt.Errorf("MGR Tell %s was already claimed: %w", phase, err)
			}
			if err := mgrTellJournal(out, started); err != nil {
				return err
			}
			output, callErr := backend.call(ctx, target, args...)
			receipt.Mutation = &mgrTellMutation{Action: phase, Args: args, OK: callErr == nil, Error: mgrTellDiagnostic(callErr), OutputBytes: len(output)}
		}
		receipt.Views = backend.views(ctx, target)
	}
	if err := mgrTellPublish(out, mgrTellPrefix+token+"."+phase+".json", receipt); err != nil {
		return err
	}
	return mgrTellJournal(out, receipt)
}
