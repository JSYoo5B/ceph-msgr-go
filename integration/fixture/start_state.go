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
	"time"

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
	startConditionForeignContainer = "foreign-container"
)

// startStateRaw is the output of one observation, from two independent
// sources: the MON reads and the Docker listing. A source that failed carries
// its error instead of output, and the other source is still decoded and
// compared.
type startStateRaw struct {
	Status   []byte
	OSDDump  []byte
	Profiles []byte
	Rules    []byte
	Config   []byte
	// NativeErr is the first failed MON read; the later ones were not sent.
	NativeErr  error
	Containers []container.Summary
	DockerErr  error
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
	// Containers maps every listed container ID to its state; Labelled
	// holds the IDs carrying the Testcontainers base label.
	Containers map[string]string `json:"-"`
	Names      map[string]string `json:"-"`
	Labelled   []string          `json:"-"`
	// Native and Docker report whether that source was observed.
	// Unobserved names each source that failed, as an observation failure.
	Native     bool                `json:"-"`
	Docker     bool                `json:"-"`
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
}

func (f startStateFailure) String() string { return f.Condition + ": " + f.Detail }

// decodeStartState decodes each source of one observation on its own: a
// source that failed or printed output that does not decode becomes an
// observation failure and leaves the other sources' fields in place.
func decodeStartState(raw startStateRaw) startStateView {
	var v startStateView
	unobserved := func(format string, args ...any) {
		v.Unobserved = append(v.Unobserved, startStateFailure{startConditionObservation, fmt.Sprintf(format, args...)})
	}
	if raw.NativeErr != nil {
		unobserved("%v", raw.NativeErr)
	} else if err := decodeStartNative(raw, &v); err != nil {
		unobserved("%v", err)
	} else {
		v.Native = true
	}
	if raw.DockerErr != nil {
		unobserved("list Docker containers: %v", raw.DockerErr)
	} else {
		v.Docker = true
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
	return v
}

func decodeStartNative(raw startStateRaw, v *startStateView) error {
	var status struct {
		Quorum []string `json:"quorum_names"`
		osdCleanStatus
	}
	if err := json.Unmarshal(raw.Status, &status); err != nil {
		return fmt.Errorf("decode ceph status: %w", err)
	}
	v.Quorum = slices.Sorted(slices.Values(status.Quorum))
	v.MgrAvailable = status.MgrMap.Available
	v.NumPGs = status.PGMap.NumPGs
	v.PGStates = map[string]int{}
	for _, state := range status.PGMap.PGsByState {
		v.PGStates[state.StateName] += state.Count
	}
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
	if err := json.Unmarshal(raw.OSDDump, &dump); err != nil {
		return fmt.Errorf("decode ceph osd dump: %w", err)
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
	if err := json.Unmarshal(raw.Profiles, &v.Profiles); err != nil {
		return fmt.Errorf("decode erasure-code profiles: %w", err)
	}
	slices.Sort(v.Profiles)
	if err := json.Unmarshal(raw.Rules, &v.Rules); err != nil {
		return fmt.Errorf("decode CRUSH rules: %w", err)
	}
	slices.Sort(v.Rules)
	if err := json.Unmarshal(raw.Config, &v.Config); err != nil {
		return fmt.Errorf("decode config dump: %w", err)
	}
	slices.SortFunc(v.Config, func(a, b startConfigEntry) int { return strings.Compare(a.key(), b.key()) })
	return nil
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

// compareStartState returns every failed condition, in a fixed order: first
// each source that was not observed, then the conditions of the sources that
// were. A source that failed hides only its own conditions.
func compareStartState(s startStateSnapshot, v startStateView) []startStateFailure {
	failures := slices.Clone(v.Unobserved)
	fail := func(condition, format string, args ...any) {
		failures = append(failures, startStateFailure{condition, fmt.Sprintf(format, args...)})
	}
	if v.Docker {
		failures = append(failures, compareStartContainers(s, v)...)
	}
	if v.Native {
		failures = append(failures, compareStartNative(s, v)...)
	}
	if v.Docker {
		owned := map[string]bool{}
		for _, id := range s.Owned {
			owned[id] = true
		}
		for _, id := range v.Labelled {
			if !owned[id] {
				fail(startConditionForeignContainer, "container %s (%s) is %s", startShortID(id), v.Names[id], v.Containers[id])
			}
		}
	}
	return failures
}

func startShortID(id string) string { return id[:min(len(id), 12)] }

// compareStartContainers compares the daemon and other owned containers.
func compareStartContainers(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures []startStateFailure
	fail := func(condition, format string, args ...any) {
		failures = append(failures, startStateFailure{condition, fmt.Sprintf(format, args...)})
	}
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

// compareStartNative compares the conditions of the MON reads.
func compareStartNative(s startStateSnapshot, v startStateView) []startStateFailure {
	var failures []startStateFailure
	fail := func(condition, format string, args ...any) {
		failures = append(failures, startStateFailure{condition, fmt.Sprintf(format, args...)})
	}
	if !slices.Equal(v.Quorum, s.View.Quorum) {
		fail(startConditionMonQuorum, "quorum [%s], was [%s]", strings.Join(v.Quorum, ","), strings.Join(s.View.Quorum, ","))
	}
	if s.View.MgrAvailable && !v.MgrAvailable {
		fail(startConditionMgrAvailable, "no available manager")
	}
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
	if added, removed := setDifference(v.Profiles, s.View.Profiles); len(added)+len(removed) > 0 {
		fail(startConditionErasureProfile, "erasure-code profiles added %v, removed %v", added, removed)
	}
	if added, removed := setDifference(v.Rules, s.View.Rules); len(added)+len(removed) > 0 {
		fail(startConditionCrushRule, "CRUSH rules added %v, removed %v", added, removed)
	}
	if added, removed := setDifference(v.Flags, s.View.Flags); len(added)+len(removed) > 0 {
		fail(startConditionOSDFlags, "OSDMap flags set %v, cleared %v", added, removed)
	}
	for _, change := range configChanges(s.View.Config, v.Config) {
		fail(startConditionConfig, "%s", change)
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

func configChanges(start, now []startConfigEntry) []string {
	before, after := map[string]startConfigEntry{}, map[string]startConfigEntry{}
	for _, e := range start {
		before[e.key()] = e
	}
	for _, e := range now {
		after[e.key()] = e
	}
	var changes []string
	for _, key := range slices.Sorted(maps.Keys(after)) {
		e := after[key]
		if old, ok := before[key]; !ok {
			changes = append(changes, fmt.Sprintf("%s set to %s, absent at start", key, e.reportValue()))
		} else if old.Value != e.Value {
			changes = append(changes, fmt.Sprintf("%s is %s, was %s", key, e.reportValue(), old.reportValue()))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(before)) {
		if _, ok := after[key]; !ok {
			changes = append(changes, fmt.Sprintf("%s removed, was %s", key, before[key].reportValue()))
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
// returns the failures of the last completed comparison. An observation error
// is a failed condition of that poll, not a fixture failure; an observation
// cut short by the end of the budget does not replace an earlier completed
// comparison. A missing snapshot fails at once.
func waitStartState(ctx context.Context, budget, interval time.Duration, snapshot *startStateSnapshot, snapshotErr error,
	observe func(context.Context) (startStateView, error)) (report startStateReport) {
	report = startStateReport{Started: time.Now().UTC(), BudgetMS: budget.Milliseconds()}
	defer func() { report.ElapsedMS = time.Since(report.Started).Milliseconds() }()
	if snapshot == nil {
		detail := "the fixture recorded no start state"
		if snapshotErr != nil {
			detail += ": " + snapshotErr.Error()
		}
		report.Failures = []startStateFailure{{startConditionSnapshot, detail}}
		return report
	}
	deadline, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	compared := false
	for {
		report.Polls++
		view, err := observe(deadline)
		var failures []startStateFailure
		if err != nil {
			failures = []startStateFailure{{startConditionObservation, err.Error()}}
		} else {
			failures = compareStartState(*snapshot, view)
		}
		if len(failures) == 0 {
			report.Passed, report.Failures = true, nil
			return report
		}
		// A poll whose reads the budget cut short does not replace an
		// earlier comparison.
		cut := deadline.Err() != nil && (err != nil || len(view.Unobserved) > 0)
		if !cut || !compared {
			report.Failures, compared = failures, true
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

// observeStartState reads one observation of the owned cluster from its two
// independent sources, the Docker listing and the MON reads. A failed source
// is recorded in the observation and never stops the other source.
func (c *fixtureCluster) observeStartState(ctx context.Context) (startStateView, []container.Summary) {
	var raw startStateRaw
	raw.Containers, raw.DockerErr = listStartContainers(ctx)
	raw.NativeErr = c.readStartNative(ctx, &raw)
	return decodeStartState(raw), raw.Containers
}

// readStartNative sends the MON reads in order. They share one source, so the
// first failure ends them.
func (c *fixtureCluster) readStartNative(ctx context.Context, raw *startStateRaw) error {
	for _, read := range []struct {
		into *[]byte
		args []string
	}{
		{&raw.Status, []string{"status"}},
		{&raw.OSDDump, []string{"osd", "dump"}},
		{&raw.Profiles, []string{"osd", "erasure-code-profile", "ls"}},
		{&raw.Rules, []string{"osd", "crush", "rule", "ls"}},
		{&raw.Config, []string{"config", "dump"}},
	} {
		poll, cancel := context.WithTimeout(ctx, startStatePollTimeout)
		data, err := c.cluster.Ceph(poll, append(read.args, "--format", "json")...)
		cancel()
		if err != nil {
			return fmt.Errorf("ceph %s: %w", strings.Join(read.args, " "), err)
		}
		*read.into = data
	}
	return nil
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

// recordStartState takes the snapshot once the fixture is ready. Every source
// must be observed. A failure is kept and reported by every later check
// instead of failing the fixture.
func (c *fixtureCluster) recordStartState(ctx context.Context, out string) {
	err := func() error {
		_, owned, err := c.ownedContainers(ctx)
		if err != nil {
			return err
		}
		view, containers := c.observeStartState(ctx)
		if len(view.Unobserved) > 0 {
			var errs []error
			for _, f := range view.Unobserved {
				errs = append(errs, errors.New(f.Detail))
			}
			return errors.Join(errs...)
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
	report := waitStartState(ctx, budget, startStatePollInterval, c.startState, c.startStateErr, func(ctx context.Context) (startStateView, error) {
		view, _ := c.observeStartState(ctx)
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
