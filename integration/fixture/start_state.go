package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The start-state check compares the running fixture with the snapshot the
// fixture recorded once it was ready, just before its control loop started.
// See FIXTURE_START_STATE.md for each condition and its native source.
const (
	startStateDefaultBudget = 30 * time.Second
	startStateMaxBudget     = 300 * time.Second
	// startStatePollTimeout bounds one native command or Docker listing.
	startStatePollTimeout  = 10 * time.Second
	startStatePollInterval = time.Second
	// startStateValueLimit bounds a configuration value kept in a report.
	startStateValueLimit = 128
	// startStateAdminSocketLimit bounds one MDS admin socket reply.
	startStateAdminSocketLimit = 1 << 20
)

// Condition names reported by the start-state check. They are stable: tests
// and the grouped runner match on them.
const (
	startConditionSnapshot         = "start-snapshot"
	startConditionObservation      = "observation"
	startConditionDaemonRunning    = "daemon-running"
	startConditionContainerState   = "container-state"
	startConditionMonQuorum        = "mon-quorum"
	startConditionMgrAvailable     = "mgr-available"
	startConditionOSDSet           = "osd-set"
	startConditionOSDUp            = "osd-up"
	startConditionOSDIn            = "osd-in"
	startConditionPGClean          = "pg-clean"
	startConditionPGTemp           = "pg-temp"
	startConditionPool             = "pool"
	startConditionPoolFlags        = "pool-flags"
	startConditionErasureProfile   = "erasure-code-profile"
	startConditionCrushRule        = "crush-rule"
	startConditionOSDFlags         = "osd-flags"
	startConditionConfig           = "config"
	startConditionDaemonConfig     = "daemon-config"
	startConditionForeignContainer = "foreign-container"
)

// The reads of one observation. Each read is independent: a read that fails
// hides only the conditions computed from it.
const (
	startReadDocker   = "Docker container list"
	startReadStatus   = "ceph status"
	startReadOSDDump  = "ceph osd dump"
	startReadProfiles = "ceph osd erasure-code-profile ls"
	startReadRules    = "ceph osd crush rule ls"
	startReadConfig   = "ceph config dump"
)

// startReadAdminSocket names the admin socket `config diff` read of a daemon.
func startReadAdminSocket(daemon string) string { return daemon + " admin socket config diff" }

// startNativeRead is one MON read: its command, where its output is kept, how
// it is decoded and the conditions computed from it alone.
type startNativeRead struct {
	name    string
	args    []string
	output  func(*startStateRaw) *[]byte
	decode  func([]byte, *startStateView) error
	compare func(startStateSnapshot, startStateView) []startStateFailure
}

var startNativeReads = []startNativeRead{
	{startReadStatus, []string{"status"}, func(r *startStateRaw) *[]byte { return &r.Status }, decodeStartStatus, compareStartStatus},
	{startReadOSDDump, []string{"osd", "dump"}, func(r *startStateRaw) *[]byte { return &r.OSDDump }, decodeStartOSDDump, compareStartOSDDump},
	{startReadProfiles, []string{"osd", "erasure-code-profile", "ls"}, func(r *startStateRaw) *[]byte { return &r.Profiles }, decodeStartProfiles, compareStartProfiles},
	{startReadRules, []string{"osd", "crush", "rule", "ls"}, func(r *startStateRaw) *[]byte { return &r.Rules }, decodeStartRules, compareStartRules},
	{startReadConfig, []string{"config", "dump"}, func(r *startStateRaw) *[]byte { return &r.Config }, decodeStartConfig, compareStartConfig},
}

// startStateRaw is the output of one observation. A read that failed carries
// its error instead of output, and every other read is still decoded and
// compared.
type startStateRaw struct {
	Status   []byte
	OSDDump  []byte
	Profiles []byte
	Rules    []byte
	Config   []byte
	// NativeErrs maps each MON read that failed (startReadStatus and the
	// others of startNativeReads) to its error.
	NativeErrs map[string]error
	Containers []container.Summary
	DockerErr  error
	// Overrides maps each daemon read to its admin socket `config diff`
	// output, or OverrideErrs to the read's error. A daemon whose container
	// the Docker listing shows not running is not read.
	Overrides    map[string][]byte
	OverrideErrs map[string]error
}

type startConfigEntry struct {
	Section string `json:"section"`
	Name    string `json:"name"`
	Value   string `json:"value"`
	Mask    string `json:"mask,omitempty"`
}

func (e startConfigEntry) key() string {
	if e.Mask != "" {
		return e.Section + "/" + e.Mask + "/" + e.Name
	}
	return e.Section + "/" + e.Name
}

// sensitive reports whether the option name suggests a credential.
func (e startConfigEntry) sensitive() bool {
	name := strings.ToLower(e.Name)
	for _, secret := range []string{"key", "secret", "password", "token"} {
		if strings.Contains(name, secret) {
			return true
		}
	}
	return false
}

// reportValue keeps reports bounded and never repeats a value whose option
// name suggests a credential.
func (e startConfigEntry) reportValue() string {
	if e.sensitive() {
		return "<redacted>"
	}
	if len(e.Value) > startStateValueLimit {
		return strconv.Quote(e.Value[:startStateValueLimit]) + "..."
	}
	return strconv.Quote(e.Value)
}

// MarshalJSON writes the entry to the on-disk snapshot. The value of a
// credential-like option is left out; the check compares the value kept in
// memory.
func (e startConfigEntry) MarshalJSON() ([]byte, error) {
	type entry struct {
		Section  string  `json:"section"`
		Name     string  `json:"name"`
		Value    *string `json:"value,omitempty"`
		Redacted bool    `json:"value_redacted,omitempty"`
		Mask     string  `json:"mask,omitempty"`
	}
	out := entry{Section: e.Section, Name: e.Name, Mask: e.Mask, Redacted: e.sensitive()}
	if !out.Redacted {
		out.Value = &e.Value
	}
	return json.Marshal(out)
}

type startOSD struct {
	ID int `json:"osd"`
	Up int `json:"up"`
	In int `json:"in"`
}

// startStateView is the decoded part of one observation the check compares.
type startStateView struct {
	Quorum       []string           `json:"quorum"`
	MgrAvailable bool               `json:"mgr_available"`
	NumPGs       int                `json:"num_pgs"`
	PGStates     map[string]int     `json:"pg_states"`
	OSDs         []startOSD         `json:"osds"`
	PGTemp       []string           `json:"pg_temp,omitempty"`
	Flags        []string           `json:"osd_flags"`
	Pools        map[string]string  `json:"pools"`
	Profiles     []string           `json:"erasure_code_profiles"`
	Rules        []string           `json:"crush_rules"`
	Config       []startConfigEntry `json:"config"`
	// Overrides holds every runtime override (the `override` level of
	// `config diff`) of each daemon read, with the daemon name as Section.
	Overrides []startConfigEntry `json:"daemon_overrides"`
	// Containers maps every listed container ID to its state; Labelled
	// holds the IDs carrying the Testcontainers base label.
	Containers map[string]string `json:"-"`
	Names      map[string]string `json:"-"`
	Labelled   []string          `json:"-"`
	// Observed holds each read that completed and decoded; Read holds the
	// daemons whose overrides were read. Unobserved has an observation
	// failure for each other read.
	Observed   map[string]bool     `json:"-"`
	Read       []string            `json:"-"`
	Unobserved []startStateFailure `json:"-"`
}

// startStateSnapshot is the fixture's recorded start state.
type startStateSnapshot struct {
	Taken time.Time      `json:"taken_at"`
	View  startStateView `json:"view"`
	// Daemons maps every daemon of the profile (mon., mgr., osd., mds.) to
	// its container ID; Others holds the other owned containers (the native
	// oracle client and the control container) with their start state.
	Daemons map[string]string         `json:"daemons"`
	Others  map[string]startContainer `json:"other_containers"`
	// Owned is every container ID that belongs to the fixture at start.
	Owned []string `json:"owned_containers"`
}

type startContainer struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

type startStateFailure struct {
	Condition string `json:"condition"`
	Detail    string `json:"detail"`
	// read names the read the failure was computed from.
	read string
}

// startConditionOrder is the fixed order of the failures in a report.
var startConditionOrder = []string{
	startConditionSnapshot, startConditionObservation, startConditionDaemonRunning, startConditionContainerState,
	startConditionMonQuorum, startConditionMgrAvailable, startConditionOSDSet, startConditionOSDUp, startConditionOSDIn,
	startConditionPGClean, startConditionPGTemp, startConditionPool, startConditionPoolFlags, startConditionErasureProfile,
	startConditionCrushRule, startConditionOSDFlags, startConditionConfig, startConditionDaemonConfig, startConditionForeignContainer,
}

func sortStartFailures(failures []startStateFailure) {
	slices.SortStableFunc(failures, func(a, b startStateFailure) int {
		return slices.Index(startConditionOrder, a.Condition) - slices.Index(startConditionOrder, b.Condition)
	})
}

// startFailures collects the failures computed from one read.
type startFailures []startStateFailure

func (f *startFailures) fail(condition, format string, args ...any) {
	*f = append(*f, startStateFailure{Condition: condition, Detail: fmt.Sprintf(format, args...)})
}

// from records the read each failure was computed from.
func (f startFailures) from(read string) []startStateFailure {
	for i := range f {
		f[i].read = read
	}
	return f
}

func (f startStateFailure) String() string { return f.Condition + ": " + f.Detail }

// decodeStartState decodes each read of one observation on its own: a read
// that failed or printed output that does not decode becomes an observation
// failure and leaves the other reads' fields in place.
func decodeStartState(raw startStateRaw) startStateView {
	v := startStateView{Observed: map[string]bool{}}
	unobserved := func(read string, err error) {
		v.Unobserved = append(v.Unobserved, startStateFailure{Condition: startConditionObservation, Detail: fmt.Sprintf("%s: %v", read, err), read: read})
	}
	for _, read := range startNativeReads {
		if err := raw.NativeErrs[read.name]; err != nil {
			unobserved(read.name, err)
		} else if err := read.decode(*read.output(&raw), &v); err != nil {
			unobserved(read.name, fmt.Errorf("decode: %w", err))
		} else {
			v.Observed[read.name] = true
		}
	}
	if raw.DockerErr != nil {
		unobserved(startReadDocker, raw.DockerErr)
	} else {
		v.Observed[startReadDocker] = true
		v.Containers, v.Names = map[string]string{}, map[string]string{}
		for _, item := range raw.Containers {
			v.Containers[item.ID] = string(item.State)
			if len(item.Names) > 0 {
				v.Names[item.ID] = strings.TrimPrefix(item.Names[0], "/")
			}
			if item.Labels["org.testcontainers"] == "true" {
				v.Labelled = append(v.Labelled, item.ID)
			}
		}
		slices.Sort(v.Labelled)
	}
	for _, daemon := range slices.Sorted(maps.Keys(raw.OverrideErrs)) {
		unobserved(startReadAdminSocket(daemon), raw.OverrideErrs[daemon])
	}
	for _, daemon := range slices.Sorted(maps.Keys(raw.Overrides)) {
		overrides, err := decodeStartOverrides(daemon, raw.Overrides[daemon])
		if err != nil {
			unobserved(startReadAdminSocket(daemon), err)
			continue
		}
		v.Observed[startReadAdminSocket(daemon)] = true
		v.Read = append(v.Read, daemon)
		v.Overrides = append(v.Overrides, overrides...)
	}
	slices.SortFunc(v.Overrides, func(a, b startConfigEntry) int { return strings.Compare(a.key(), b.key()) })
	return v
}

// Each MON read's decoder fills its own fields of the view, and only once
// its whole output decoded.

func decodeStartStatus(data []byte, v *startStateView) error {
	var status struct {
		Quorum []string `json:"quorum_names"`
		osdCleanStatus
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return err
	}
	v.Quorum = slices.Sorted(slices.Values(status.Quorum))
	v.MgrAvailable = status.MgrMap.Available
	v.NumPGs = status.PGMap.NumPGs
	v.PGStates = map[string]int{}
	for _, state := range status.PGMap.PGsByState {
		v.PGStates[state.StateName] += state.Count
	}
	return nil
}

func decodeStartOSDDump(data []byte, v *startStateView) error {
	var dump struct {
		Flags string     `json:"flags"`
		OSDs  []startOSD `json:"osds"`
		Pools []struct {
			Name  string `json:"pool_name"`
			Flags string `json:"flags_names"`
		} `json:"pools"`
		PGTemp []struct {
			PGID string `json:"pgid"`
			OSDs []int  `json:"osds"`
		} `json:"pg_temp"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return err
	}
	v.Flags = splitStartList(dump.Flags)
	v.OSDs = slices.SortedFunc(slices.Values(dump.OSDs), func(a, b startOSD) int { return a.ID - b.ID })
	v.Pools = map[string]string{}
	for _, pool := range dump.Pools {
		v.Pools[pool.Name] = strings.Join(splitStartList(pool.Flags), ",")
	}
	for _, temp := range dump.PGTemp {
		v.PGTemp = append(v.PGTemp, fmt.Sprintf("%s->%v", temp.PGID, temp.OSDs))
	}
	slices.Sort(v.PGTemp)
	return nil
}

func decodeStartProfiles(data []byte, v *startStateView) error {
	var profiles []string
	if err := json.Unmarshal(data, &profiles); err != nil {
		return err
	}
	v.Profiles = slices.Sorted(slices.Values(profiles))
	return nil
}

func decodeStartRules(data []byte, v *startStateView) error {
	var rules []string
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}
	v.Rules = slices.Sorted(slices.Values(rules))
	return nil
}

func decodeStartConfig(data []byte, v *startStateView) error {
	var config []startConfigEntry
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	slices.SortFunc(config, func(a, b startConfigEntry) int { return strings.Compare(a.key(), b.key()) })
	v.Config = config
	return nil
}

// decodeStartOverrides returns the runtime overrides in one daemon's admin
// socket `config diff` reply, {"diff": {option: {level: value, ...}}}. Only
// the `override` level is kept: `ceph tell <daemon> config set`, an admin
// socket `config set` and `injectargs` set it, while the MON configuration
// database (the `mon` level) is compared through `config dump`. A value is
// kept as its JSON text, a string unquoted.
func decodeStartOverrides(daemon string, data []byte) ([]startConfigEntry, error) {
	var reply struct {
		Diff map[string]map[string]json.RawMessage `json:"diff"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if reply.Diff == nil {
		return nil, errors.New("reply has no diff object")
	}
	var out []startConfigEntry
	for name, levels := range reply.Diff {
		value, ok := levels["override"]
		if !ok {
			continue
		}
		text := string(value)
		var str string
		if json.Unmarshal(value, &str) == nil {
			text = str
		}
		out = append(out, startConfigEntry{Section: daemon, Name: name, Value: text})
	}
	return out, nil
}

func splitStartList(list string) []string {
	var out []string
	for item := range strings.SplitSeq(list, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	slices.Sort(out)
	return out
}

// newStartStateSnapshot records the fixture's start state. owned maps every
// owned container name to its ID (see ownedContainers). session is the
// fixture's Testcontainers session: a labelled container of that session that
// is listed at start also belongs to the fixture (for example a reaper).
func newStartStateSnapshot(view startStateView, owned map[string]string, containers []container.Summary, session string) startStateSnapshot {
	s := startStateSnapshot{Taken: time.Now().UTC(), View: view, Daemons: map[string]string{}, Others: map[string]startContainer{}}
	ids := map[string]bool{}
	for name, id := range owned {
		ids[id] = true
		if startDaemonName(name) {
			s.Daemons[name] = id
		} else {
			s.Others[name] = startContainer{id, view.Containers[id]}
		}
	}
	for _, item := range containers {
		if session != "" && item.Labels["org.testcontainers.sessionId"] == session {
			ids[item.ID] = true
		}
	}
	s.Owned = slices.Sorted(maps.Keys(ids))
	return s
}

func startDaemonName(name string) bool {
	kind, _, ok := strings.Cut(name, ".")
	return ok && slices.Contains(daemonControlKinds, kind)
}

// compareStartState returns every failed condition, in the fixed order of
// startConditionOrder. Each condition is computed from the read it depends
// on, so a read that failed hides only its own conditions; it is reported as
// an observation failure.
func compareStartState(s startStateSnapshot, v startStateView) []startStateFailure {
	failures := slices.Clone(v.Unobserved)
	if v.Observed[startReadDocker] {
		docker := startFailures(compareStartContainers(s, v))
		owned := map[string]bool{}
		for _, id := range s.Owned {
			owned[id] = true
		}
		for _, id := range v.Labelled {
			if !owned[id] {
				docker.fail(startConditionForeignContainer, "container %s (%s) is %s", startShortID(id), v.Names[id], v.Containers[id])
			}
		}
		failures = append(failures, docker.from(startReadDocker)...)
	}
	for _, read := range startNativeReads {
		if v.Observed[read.name] {
			failures = append(failures, startFailures(read.compare(s, v)).from(read.name)...)
		}
	}
	// Overrides are compared for each daemon read now; a daemon not read is
	// either not running (daemon-running) or unobserved.
	of := func(entries []startConfigEntry, daemon string) []startConfigEntry {
		return slices.DeleteFunc(slices.Clone(entries), func(e startConfigEntry) bool { return e.Section != daemon })
	}
	for _, daemon := range v.Read {
		var overrides startFailures
		for _, change := range configChanges("runtime override ", of(s.View.Overrides, daemon), of(v.Overrides, daemon)) {
			overrides.fail(startConditionDaemonConfig, "%s", change)
		}
		failures = append(failures, overrides.from(startReadAdminSocket(daemon))...)
	}
	sortStartFailures(failures)
	return failures
}

func startShortID(id string) string { return id[:min(len(id), 12)] }

// compareStartContainers compares the daemon and other owned containers.
func compareStartContainers(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	fail := failures.fail
	shortID := startShortID
	for _, name := range slices.Sorted(maps.Keys(s.Daemons)) {
		id := s.Daemons[name]
		state, ok := v.Containers[id]
		switch {
		case !ok:
			fail(startConditionDaemonRunning, "%s container %s no longer exists", name, shortID(id))
		case state != "running":
			fail(startConditionDaemonRunning, "%s container %s is %s", name, shortID(id), state)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(s.Others)) {
		id, want := s.Others[name].ID, s.Others[name].State
		if state, ok := v.Containers[id]; !ok {
			fail(startConditionContainerState, "%s container %s no longer exists", name, shortID(id))
		} else if state != want {
			fail(startConditionContainerState, "%s container %s is %s, was %s", name, shortID(id), state, want)
		}
	}
	return failures
}

// compareStartStatus compares the conditions of `ceph status`.
func compareStartStatus(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	fail := failures.fail
	if !slices.Equal(v.Quorum, s.View.Quorum) {
		fail(startConditionMonQuorum, "quorum [%s], was [%s]", strings.Join(v.Quorum, ","), strings.Join(s.View.Quorum, ","))
	}
	if s.View.MgrAvailable && !v.MgrAvailable {
		fail(startConditionMgrAvailable, "no available manager")
	}
	clean, other := 0, []string{}
	for _, state := range slices.Sorted(maps.Keys(v.PGStates)) {
		if state == "active+clean" {
			clean += v.PGStates[state]
		} else {
			other = append(other, fmt.Sprintf("%dx%s", v.PGStates[state], state))
		}
	}
	switch {
	case len(other) > 0:
		fail(startConditionPGClean, "PGs not active+clean: %s", strings.Join(other, ","))
	case clean != v.NumPGs:
		fail(startConditionPGClean, "PG states cover %d of %d PGs", clean, v.NumPGs)
	case v.NumPGs == 0 && s.View.NumPGs > 0:
		fail(startConditionPGClean, "no PG statistics, %d PGs at start", s.View.NumPGs)
	}
	return failures
}

// compareStartOSDDump compares the conditions of `ceph osd dump`.
func compareStartOSDDump(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	fail := failures.fail
	start, now := map[int]startOSD{}, map[int]startOSD{}
	for _, osd := range s.View.OSDs {
		start[osd.ID] = osd
	}
	for _, osd := range v.OSDs {
		now[osd.ID] = osd
	}
	if added, removed := setDifference(slices.Collect(maps.Keys(now)), slices.Collect(maps.Keys(start))); len(added)+len(removed) > 0 {
		fail(startConditionOSDSet, "OSDs added %v, removed %v", added, removed)
	}
	for _, osd := range v.OSDs {
		if osd.Up != 1 {
			fail(startConditionOSDUp, "osd.%d is down", osd.ID)
		}
	}
	for _, osd := range v.OSDs {
		if osd.In != 1 {
			fail(startConditionOSDIn, "osd.%d is out", osd.ID)
		}
	}
	if len(v.PGTemp) > 0 {
		fail(startConditionPGTemp, "pg_temp %s", strings.Join(v.PGTemp, " "))
	}
	if added, removed := setDifference(slices.Collect(maps.Keys(v.Pools)), slices.Collect(maps.Keys(s.View.Pools))); len(added)+len(removed) > 0 {
		fail(startConditionPool, "pools added %v, removed %v", added, removed)
	}
	for _, name := range slices.Sorted(maps.Keys(v.Pools)) {
		if want, ok := s.View.Pools[name]; ok && v.Pools[name] != want {
			fail(startConditionPoolFlags, "pool %s flags [%s], were [%s]", name, v.Pools[name], want)
		}
	}
	if added, removed := setDifference(v.Flags, s.View.Flags); len(added)+len(removed) > 0 {
		fail(startConditionOSDFlags, "OSDMap flags set %v, cleared %v", added, removed)
	}
	return failures
}

func compareStartProfiles(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	if added, removed := setDifference(v.Profiles, s.View.Profiles); len(added)+len(removed) > 0 {
		failures.fail(startConditionErasureProfile, "erasure-code profiles added %v, removed %v", added, removed)
	}
	return failures
}

func compareStartRules(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	if added, removed := setDifference(v.Rules, s.View.Rules); len(added)+len(removed) > 0 {
		failures.fail(startConditionCrushRule, "CRUSH rules added %v, removed %v", added, removed)
	}
	return failures
}

func compareStartConfig(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures startFailures
	for _, change := range configChanges("", s.View.Config, v.Config) {
		failures.fail(startConditionConfig, "%s", change)
	}
	return failures
}

// setDifference returns the sorted items of now missing from start and of
// start missing from now.
func setDifference[T interface{ ~int | ~string }](now, start []T) (added, removed []T) {
	for _, item := range now {
		if !slices.Contains(start, item) {
			added = append(added, item)
		}
	}
	for _, item := range start {
		if !slices.Contains(now, item) {
			removed = append(removed, item)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	return added, removed
}

// configChanges describes each entry added, changed or removed; label
// prefixes the option name, after its section.
func configChanges(label string, start, now []startConfigEntry) []string {
	before, after := map[string]startConfigEntry{}, map[string]startConfigEntry{}
	for _, e := range start {
		before[e.key()] = e
	}
	for _, e := range now {
		after[e.key()] = e
	}
	var changes []string
	name := func(e startConfigEntry) string {
		if label == "" {
			return e.key()
		}
		return e.Section + " " + label + e.Name
	}
	for _, key := range slices.Sorted(maps.Keys(after)) {
		e := after[key]
		if old, ok := before[key]; !ok {
			changes = append(changes, fmt.Sprintf("%s set to %s, absent at start", name(e), e.reportValue()))
		} else if old.Value != e.Value {
			changes = append(changes, fmt.Sprintf("%s is %s, was %s", name(e), e.reportValue(), old.reportValue()))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(before)) {
		if _, ok := after[key]; !ok {
			changes = append(changes, fmt.Sprintf("%s removed, was %s", name(before[key]), before[key].reportValue()))
		}
	}
	return changes
}

// startStateReport is the result of one check, written to start-state.json.
type startStateReport struct {
	Started   time.Time           `json:"started_at"`
	BudgetMS  int64               `json:"budget_ms"`
	ElapsedMS int64               `json:"elapsed_ms"`
	Polls     int                 `json:"polls"`
	Passed    bool                `json:"passed"`
	Failures  []startStateFailure `json:"failures"`
}

// waitStartState polls until no condition fails or the budget ends, and
// returns the failures of the last poll. An observation error is a failed
// condition of that poll, not a fixture failure. A read that the end of the
// budget cut short does not replace the failures its latest completed read
// found; the poll's other reads still replace theirs. A missing snapshot
// fails at once.
func waitStartState(ctx context.Context, budget, interval time.Duration, snapshot *startStateSnapshot, snapshotErr error,
	observe func(context.Context) (startStateView, error)) (report startStateReport) {
	report = startStateReport{Started: time.Now().UTC(), BudgetMS: budget.Milliseconds()}
	defer func() { report.ElapsedMS = time.Since(report.Started).Milliseconds() }()
	if snapshot == nil {
		detail := "the fixture recorded no start state"
		if snapshotErr != nil {
			detail += ": " + snapshotErr.Error()
		}
		report.Failures = []startStateFailure{{Condition: startConditionSnapshot, Detail: detail}}
		return report
	}
	deadline, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	latest := map[string][]startStateFailure{}
	for {
		report.Polls++
		view, err := observe(deadline)
		cut := deadline.Err() != nil
		if err != nil {
			// No read of this poll is known.
			if !cut || report.Failures == nil {
				report.Failures = []startStateFailure{{Condition: startConditionObservation, Detail: err.Error()}}
				clear(latest)
			}
		} else {
			failures := compareStartState(*snapshot, view)
			if len(failures) == 0 {
				report.Passed, report.Failures = true, nil
				return report
			}
			report.Failures = mergeStartPoll(failures, view.Observed, latest, cut)
		}
		timer := time.NewTimer(interval)
		select {
		case <-deadline.Done():
			timer.Stop()
			return report
		case <-timer.C:
		}
	}
}

// mergeStartPoll returns the failures to report for one compared poll and
// records, in latest, the failures of each read the poll completed. A read
// the poll did not complete is reported as its observation failure, except
// that a read cut short by the end of the budget keeps the failures its
// latest completed read found.
func mergeStartPoll(failures []startStateFailure, observed map[string]bool, latest map[string][]startStateFailure, cut bool) []startStateFailure {
	byRead := map[string][]startStateFailure{}
	for _, f := range failures {
		byRead[f.read] = append(byRead[f.read], f)
	}
	for read := range observed {
		latest[read] = byRead[read]
	}
	var out []startStateFailure
	for _, f := range failures {
		if f.Condition != startConditionObservation {
			out = append(out, f)
			continue
		}
		if earlier := latest[f.read]; cut && len(earlier) > 0 {
			out = append(out, earlier...)
			continue
		}
		if !cut {
			delete(latest, f.read)
		}
		out = append(out, f)
	}
	sortStartFailures(out)
	return out
}

// observeStartState reads one observation of the owned cluster. The Docker
// listing is read first, then each MON read and the admin socket of each
// daemon in daemons (name to container ID) run concurrently, each with its
// own bound. A failed read is recorded in the observation and never stops
// another read.
func (c *fixtureCluster) observeStartState(ctx context.Context, daemons map[string]string) (startStateView, []container.Summary) {
	var raw startStateRaw
	raw.Containers, raw.DockerErr = listStartContainers(ctx)
	var wait sync.WaitGroup
	var lock sync.Mutex
	raw.NativeErrs, raw.Overrides, raw.OverrideErrs = map[string]error{}, map[string][]byte{}, map[string]error{}
	for _, read := range startNativeReads {
		wait.Go(func() {
			poll, cancel := context.WithTimeout(ctx, startStatePollTimeout)
			defer cancel()
			data, err := c.cluster.Ceph(poll, append(slices.Clone(read.args), "--format", "json")...)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				raw.NativeErrs[read.name] = err
			} else {
				*read.output(&raw) = data
			}
		})
	}
	for _, name := range startDaemonsToRead(daemons, raw.Containers, raw.DockerErr) {
		wait.Go(func() {
			poll, cancel := context.WithTimeout(ctx, startStatePollTimeout)
			defer cancel()
			data, err := c.startStateAdminSocket(poll, name, "config", "diff")
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				raw.OverrideErrs[name] = err
			} else {
				raw.Overrides[name] = data
			}
		})
	}
	wait.Wait()
	return decodeStartState(raw), raw.Containers
}

// startDaemonsToRead returns the daemons whose admin socket is read: every
// daemon, unless the Docker listing succeeded and shows its container not
// running, which daemon-running reports instead.
func startDaemonsToRead(daemons map[string]string, containers []container.Summary, dockerErr error) []string {
	states := map[string]string{}
	for _, item := range containers {
		states[item.ID] = string(item.State)
	}
	var names []string
	for _, name := range slices.Sorted(maps.Keys(daemons)) {
		if dockerErr == nil && states[daemons[name]] != "running" {
			continue
		}
		names = append(names, name)
	}
	return names
}

func listStartContainers(ctx context.Context) ([]container.Summary, error) {
	poll, cancel := context.WithTimeout(ctx, startStatePollTimeout)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(poll)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	listed, err := docker.ContainerList(poll, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, err
	}
	return listed.Items, nil
}

// startStateAdminSocket runs one admin socket command on an owned MON, MGR,
// OSD or MDS daemon. An MDS socket is reached in the daemon's own container.
func (c *fixtureCluster) startStateAdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	kind, id, _ := strings.Cut(daemon, ".")
	if kind != "mds" {
		return adminSocket(ctx, c.cluster, daemon, args...)
	}
	target, err := c.daemon(ctx, kind, id)
	if err != nil {
		return nil, err
	}
	process, err := c.cluster.RunNativeCommand(ctx, target, ceph.NativeCommand{
		Argv:    append([]string{"ceph", "--admin-daemon", "/var/run/ceph/ceph-mds." + id + ".asok"}, args...),
		Timeout: startStatePollTimeout, MaxStdout: startStateAdminSocketLimit, MaxStderr: 16 << 10, Label: "start-state MDS admin socket"})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return nil, errors.Join(err, fmt.Errorf("MDS admin socket %s did not complete (exit %d, timed out %v)", args[0], process.ExitCode, process.TimedOut))
	}
	return process.Stdout, nil
}

// recordStartState takes the snapshot once the fixture is ready. Every read
// must be observed, including the admin socket of every daemon. A failure is kept and reported by every later check
// instead of failing the fixture.
func (c *fixtureCluster) recordStartState(ctx context.Context, out string) {
	err := func() error {
		_, owned, err := c.ownedContainers(ctx)
		if err != nil {
			return err
		}
		daemons := map[string]string{}
		for name, id := range owned {
			if startDaemonName(name) {
				daemons[name] = id
			}
		}
		view, containers := c.observeStartState(ctx, daemons)
		if err := startSnapshotComplete(view, daemons); err != nil {
			return err
		}
		snapshot := newStartStateSnapshot(view, owned, containers, testcontainers.SessionID())
		if err := writeStartStateSnapshot(out, snapshot); err != nil {
			return err
		}
		c.startState = &snapshot
		return nil
	}()
	c.startStateErr = err
}

// startSnapshotComplete returns an error naming every read the snapshot
// lacks: each read that failed and each daemon whose admin socket was not
// read because its container was not running.
func startSnapshotComplete(view startStateView, daemons map[string]string) error {
	var errs []error
	for _, f := range view.Unobserved {
		errs = append(errs, errors.New(f.Detail))
	}
	for _, name := range slices.Sorted(maps.Keys(daemons)) {
		read := startReadAdminSocket(name)
		if !view.Observed[read] && !slices.ContainsFunc(view.Unobserved, func(f startStateFailure) bool { return f.read == read }) {
			errs = append(errs, fmt.Errorf("%s: not read, the container is not running", read))
		}
	}
	return errors.Join(errs...)
}

// writeStartStateSnapshot writes the snapshot as evidence, owner-readable
// only and without the value of a credential-like option (see
// startConfigEntry.MarshalJSON). The check compares the snapshot in memory.
func writeStartStateSnapshot(out string, snapshot startStateSnapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	return writeStartStateFile(out, "start-state-snapshot.json", data, 0o600)
}

func writeStartStateFile(out, name string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(out, name+"-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(append(data, '\n'))
	err = errors.Join(err, tmp.Chmod(mode), tmp.Close())
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(out, name))
}

// verifyStartState runs one bounded check and records its report.
func (c *fixtureCluster) verifyStartState(ctx context.Context, out string, budget time.Duration) (startStateReport, error) {
	var daemons map[string]string
	if c.startState != nil {
		daemons = c.startState.Daemons
	}
	report := waitStartState(ctx, budget, startStatePollInterval, c.startState, c.startStateErr, func(ctx context.Context) (startStateView, error) {
		view, _ := c.observeStartState(ctx, daemons)
		return view, nil
	})
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	return report, writeStartStateFile(out, "start-state.json", data, 0o644)
}

// parseStartStateBudget reads the optional budget in whole seconds.
func parseStartStateBudget(fields []string) (time.Duration, error) {
	switch len(fields) {
	case 1:
		return startStateDefaultBudget, nil
	case 2:
		seconds, err := strconv.Atoi(fields[1])
		budget := time.Duration(seconds) * time.Second
		if err != nil || strconv.Itoa(seconds) != fields[1] || budget < time.Second || budget > startStateMaxBudget {
			return 0, fmt.Errorf("invalid start-state budget %q: want whole seconds from 1 to %d", fields[1], int(startStateMaxBudget.Seconds()))
		}
		return budget, nil
	default:
		return 0, errors.New("invalid start-state request")
	}
}

// controlStartStateRequest serves `verify-start-state <id> [seconds]`. The
// result, passed or not, is written to start-state.json before the ack
// `start-state-verify.<id>`; a failed condition does not stop the fixture.
func (c *fixtureCluster) controlStartStateRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "verify-start-state")
	if err != nil || fields == nil {
		return err
	}
	budget, err := parseStartStateBudget(fields)
	if err != nil {
		return err
	}
	if _, err := c.verifyStartState(ctx, out, budget); err != nil {
		return err
	}
	return publishAck(out, "start-state-verify."+fields[0])
}
