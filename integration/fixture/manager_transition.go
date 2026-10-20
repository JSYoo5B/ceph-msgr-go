package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// The secure-mgr control restarts both owned MGRs under a temporary policy. A
// handle's Start waits only for the admin socket, and MgrMonitor keeps a
// registration until its beacon expires or the same name re-registers, so a
// MgrMap that still lists the pre-stop GIDs says nothing about the restarted
// processes. The control therefore acknowledges only a MGR transition receipt
// (after the ceph-testcontainers-go v0.3.0 recipe, MIT,
// internal/integration/manager_transition_integration_helpers_test.go
// classifyMgrSample, and docs/guides/messenger-client-fixtures.md "MGR
// transition receipt"; the code here is written for this fixture): before the
// stop it freezes each MGR's GID, container ID and start time; after the start
// it accepts only a sample bracketed by two identical MgrMap reads in which
// both MGRs are registered (one active, one standby) with new GIDs, the same
// containers with new start times, and a beacon session (con type "mgr",
// MgrMap GID as global ID) on some owned MON.
const (
	transitionReceiptSchema = "ceph-msgr-go/mgr-transition-receipt/v1"
	transitionReceiptFile   = "mgr-transition-receipt.json"
	transitionDeadline      = 90 * time.Second
	transitionQueryBound    = 10 * time.Second
	transitionPause         = time.Second
	transitionRetained      = 4
)

const (
	sampleAccepted     = "accepted"
	samplePending      = "pending"
	sampleTransitional = "transitional"
	sampleIncomplete   = "incomplete"
	sampleViolated     = "violated"
)

type transitionRegistration struct {
	Name string `json:"name"`
	GID  uint64 `json:"gid"`
}

type transitionMap struct {
	Status    string                   `json:"status"`
	Error     string                   `json:"error,omitempty"`
	Epoch     uint32                   `json:"epoch,omitempty"`
	Available bool                     `json:"available"`
	Active    transitionRegistration   `json:"active"`
	Standbys  []transitionRegistration `json:"standbys"`
}

func (m transitionMap) same(other transitionMap) bool {
	return m.Epoch == other.Epoch && m.Available == other.Available && m.Active == other.Active && slices.Equal(m.Standbys, other.Standbys)
}

func (m transitionMap) registered(name string) (uint64, bool) {
	if m.Active.Name == name && m.Active.GID != 0 {
		return m.Active.GID, true
	}
	for _, standby := range m.Standbys {
		if standby.Name == name {
			return standby.GID, true
		}
	}
	return 0, false
}

// selectTransitionMap keeps the selected fields of one MgrMap read; the full
// `mgr dump` holds a few hundred KB of module option descriptions.
func selectTransitionMap(state managerPreparationMap, err error) transitionMap {
	if err != nil {
		return transitionMap{Status: queryStatus(err), Error: queryError(err), Standbys: []transitionRegistration{}}
	}
	if state.Epoch == 0 || (state.ActiveName == "") != (state.ActiveGID == 0) {
		return transitionMap{Status: queryFailed, Error: "MgrMap without its epoch or active identity", Standbys: []transitionRegistration{}}
	}
	selected := transitionMap{Status: queryComplete, Epoch: state.Epoch, Available: state.Available,
		Active: transitionRegistration{state.ActiveName, state.ActiveGID}, Standbys: []transitionRegistration{}}
	for _, standby := range state.Standbys {
		if standby.Name == "" || standby.GID == 0 {
			return transitionMap{Status: queryFailed, Error: "MgrMap standby without its name or GID", Standbys: []transitionRegistration{}}
		}
		selected.Standbys = append(selected.Standbys, transitionRegistration{standby.Name, standby.GID})
	}
	slices.SortFunc(selected.Standbys, func(a, b transitionRegistration) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return selected
}

type transitionInstance struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
	ContainerID string `json:"container_id"`
	StartedAt   string `json:"started_at,omitempty"`
	Running     bool   `json:"running"`
}

type transitionFrozen struct {
	Map       transitionMap        `json:"map"`
	Instances []transitionInstance `json:"instances"`
}

type transitionSample struct {
	Started   time.Time                `json:"started"`
	Finished  time.Time                `json:"finished"`
	Before    transitionMap            `json:"map_before"`
	Monitors  []monitorManagerSessions `json:"monitors"`
	Instances []transitionInstance     `json:"instances"`
	After     transitionMap            `json:"map_after"`
	State     string                   `json:"state"`
	Reason    string                   `json:"reason,omitempty"`
}

func (s transitionSample) instance(name string) (transitionInstance, bool) {
	index := slices.IndexFunc(s.Instances, func(i transitionInstance) bool { return i.Name == name })
	if index < 0 {
		return transitionInstance{}, false
	}
	return s.Instances[index], true
}

type transitionReceipt struct {
	Schema    string             `json:"schema"`
	RequestID string             `json:"request_id"`
	Managers  []string           `json:"managers"`
	Monitors  []string           `json:"monitors"`
	Frozen    transitionFrozen   `json:"frozen"`
	Started   time.Time          `json:"started"`
	Finished  time.Time          `json:"finished"`
	Deadline  string             `json:"deadline"`
	Counts    map[string]int     `json:"counts"`
	Samples   []transitionSample `json:"samples"`
	Result    string             `json:"result"`
}

// classifyTransitionSample judges one post-restart sample against the frozen
// pre-stop identities.
func classifyTransitionSample(frozen transitionFrozen, managers, monitors []string, sample transitionSample) (string, string) {
	for _, query := range append([]transitionMap{sample.Before}, sample.After) {
		if query.Status != queryComplete {
			return sampleIncomplete, "mgr dump: " + query.Status
		}
	}
	for _, monitor := range sample.Monitors {
		if !monitor.complete() {
			return sampleIncomplete, "sessions of mon." + monitor.Monitor + ": " + monitor.Status
		}
	}
	for _, instance := range sample.Instances {
		if instance.Status != queryComplete {
			return sampleIncomplete, "container of mgr." + instance.Name + ": " + instance.Status
		}
	}
	if len(sample.Monitors) != len(monitors) || len(sample.Instances) != len(managers) {
		return sampleIncomplete, fmt.Sprintf("%d of %d MONs and %d of %d MGR containers observed", len(sample.Monitors), len(monitors), len(sample.Instances), len(managers))
	}
	if !sample.Before.same(sample.After) {
		return sampleTransitional, fmt.Sprintf("the MgrMap moved from epoch %d to %d during the sample", sample.Before.Epoch, sample.After.Epoch)
	}
	mgrMap := sample.After
	if mgrMap.Active.Name != "" && !slices.Contains(managers, mgrMap.Active.Name) {
		return sampleViolated, "an unowned mgr." + mgrMap.Active.Name + " is active"
	}
	for _, standby := range mgrMap.Standbys {
		if !slices.Contains(managers, standby.Name) {
			return sampleViolated, "an unowned mgr." + standby.Name + " is a standby"
		}
	}
	for _, name := range managers {
		before, _ := frozenInstance(frozen, name)
		now, _ := sample.instance(name)
		if now.ContainerID != before.ContainerID {
			return sampleViolated, fmt.Sprintf("the container of mgr.%s was replaced", name)
		}
	}
	if !mgrMap.Available || mgrMap.Active.Name == "" || len(mgrMap.Standbys) != 1 {
		return samplePending, fmt.Sprintf("available %t, active %+v, standbys %+v", mgrMap.Available, mgrMap.Active, mgrMap.Standbys)
	}
	for _, name := range managers {
		gid, ok := mgrMap.registered(name)
		if !ok {
			return samplePending, "mgr." + name + " is not registered"
		}
		if old, _ := frozen.Map.registered(name); old != 0 && gid == old {
			return samplePending, fmt.Sprintf("mgr.%s still holds its pre-stop GID %d", name, gid)
		}
		before, _ := frozenInstance(frozen, name)
		if now, _ := sample.instance(name); !now.Running || now.StartedAt == "" || now.StartedAt == before.StartedAt {
			return samplePending, fmt.Sprintf("the container of mgr.%s runs %t since %s, frozen %s", name, now.Running, now.StartedAt, before.StartedAt)
		}
		if len(beaconMonitors(sample.Monitors, name, gid)) == 0 {
			return samplePending, fmt.Sprintf("no MON holds the beacon of mgr.%s GID %d", name, gid)
		}
	}
	return sampleAccepted, ""
}

func frozenInstance(frozen transitionFrozen, name string) (transitionInstance, bool) {
	index := slices.IndexFunc(frozen.Instances, func(i transitionInstance) bool { return i.Name == name })
	if index < 0 {
		return transitionInstance{}, false
	}
	return frozen.Instances[index], true
}

type transitionBackend struct {
	managerMap func(context.Context) (managerPreparationMap, error)
	sessions   func(context.Context) []monitorManagerSessions
	instance   func(context.Context, string) transitionInstance
	wait       func(context.Context) error
}

func (b transitionBackend) mapRead(ctx context.Context) transitionMap {
	call, cancel := context.WithTimeout(ctx, transitionQueryBound)
	defer cancel()
	state, err := b.managerMap(call)
	if err != nil && call.Err() != nil {
		return transitionMap{Status: queryTimedOut, Error: queryError(err), Standbys: []transitionRegistration{}}
	}
	return selectTransitionMap(state, err)
}

// freezeTransition reads the pre-stop identities. Every query must complete:
// a restart without a baseline could not be told from a stale map.
func freezeTransition(ctx context.Context, backend transitionBackend, managers []string) (transitionFrozen, error) {
	frozen := transitionFrozen{Map: backend.mapRead(ctx)}
	if frozen.Map.Status != queryComplete {
		return frozen, fmt.Errorf("freeze MGR transition baseline: mgr dump %s: %s", frozen.Map.Status, frozen.Map.Error)
	}
	for _, name := range managers {
		instance := backend.instance(ctx, name)
		if instance.Status != queryComplete || instance.ContainerID == "" || instance.StartedAt == "" {
			return frozen, fmt.Errorf("freeze MGR transition baseline: container of mgr.%s %s: %s", name, instance.Status, instance.Error)
		}
		frozen.Instances = append(frozen.Instances, instance)
	}
	return frozen, nil
}

// awaitTransition samples until one sample is accepted, one is violated, or
// deadline passes, keeping counts and the last few samples.
func awaitTransition(ctx context.Context, backend transitionBackend, receipt *transitionReceipt, deadline time.Duration) error {
	phase, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	wait := backend.wait
	if wait == nil {
		wait = func(ctx context.Context) error {
			timer := time.NewTimer(transitionPause)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	receipt.Started, receipt.Deadline, receipt.Counts = time.Now().UTC(), deadline.String(), map[string]int{}
	defer func() { receipt.Finished = time.Now().UTC() }()
	for {
		sample := transitionSample{Started: time.Now().UTC(), Before: backend.mapRead(phase)}
		bounded, stop := context.WithTimeout(phase, transitionQueryBound)
		sample.Monitors = backend.sessions(bounded)
		stop()
		for _, name := range receipt.Managers {
			sample.Instances = append(sample.Instances, backend.instance(phase, name))
		}
		sample.After = backend.mapRead(phase)
		sample.Finished = time.Now().UTC()
		sample.State, sample.Reason = classifyTransitionSample(receipt.Frozen, receipt.Managers, receipt.Monitors, sample)
		receipt.Counts[sample.State]++
		receipt.Samples = append(receipt.Samples, sample)
		if len(receipt.Samples) > transitionRetained {
			receipt.Samples = slices.Delete(receipt.Samples, 0, len(receipt.Samples)-transitionRetained)
		}
		switch {
		case sample.State == sampleAccepted:
			receipt.Result = sampleAccepted
			return nil
		case sample.State == sampleViolated:
			receipt.Result = "violated: " + sample.Reason
			return errors.New("MGR transition violated: " + sample.Reason)
		case phase.Err() != nil:
			receipt.Result = fmt.Sprintf("no accepted sample within %v: %s %s", deadline, sample.State, sample.Reason)
			return errors.New("MGR transition " + receipt.Result)
		}
		if err := wait(phase); err != nil {
			receipt.Result = fmt.Sprintf("no accepted sample within %v: %s %s", deadline, sample.State, sample.Reason)
			return errors.New("MGR transition " + receipt.Result)
		}
	}
}

func (c *fixtureCluster) transitionBackend() transitionBackend {
	return transitionBackend{
		managerMap: c.observeManagerMap,
		sessions: func(ctx context.Context) []monitorManagerSessions {
			return c.observeManagerSessions(ctx, transitionQueryBound)
		},
		instance: func(ctx context.Context, name string) transitionInstance {
			call, cancel := context.WithTimeout(ctx, transitionQueryBound)
			defer cancel()
			instance := transitionInstance{Name: name, Status: queryComplete}
			daemon, err := c.daemon("mgr", name)
			if err == nil {
				instance.ContainerID = daemon.GetContainerID()
				state, stateErr := daemon.State(call)
				switch {
				case stateErr != nil:
					err = stateErr
				case state == nil:
					err = errQueryMissing
				default:
					instance.StartedAt, instance.Running = state.StartedAt, state.Running
				}
			}
			if err != nil {
				instance.Status, instance.Error = boundedQueryStatus(err, call.Err() != nil), queryError(err)
			}
			return instance
		},
	}
}

// restartManagersWithReceipt freezes the owned MGRs, stops and starts each
// once (never replaying a failed Stop/Start), and waits for the transition
// receipt. The receipt is written whether or not it was accepted.
func (c *fixtureCluster) restartManagersWithReceipt(ctx context.Context, out, id string) error {
	backend := c.transitionBackend()
	receipt := transitionReceipt{Schema: transitionReceiptSchema, RequestID: id}
	for _, manager := range c.cluster.Managers() {
		receipt.Managers = append(receipt.Managers, manager.DaemonName)
	}
	for _, monitor := range c.cluster.Monitors() {
		receipt.Monitors = append(receipt.Monitors, monitor.DaemonName)
	}
	slices.Sort(receipt.Managers)
	slices.Sort(receipt.Monitors)
	write := func(err error) error {
		data, marshalErr := json.Marshal(receipt)
		if marshalErr != nil {
			return errors.Join(err, marshalErr)
		}
		return errors.Join(err, os.WriteFile(filepath.Join(out, transitionReceiptFile), data, 0644))
	}
	frozen, err := freezeTransition(ctx, backend, receipt.Managers)
	receipt.Frozen = frozen
	if err != nil {
		receipt.Result = err.Error()
		return write(err)
	}
	for _, d := range c.cluster.Managers() {
		if err := d.Stop(ctx, nil); err != nil {
			receipt.Result = "stop mgr." + d.DaemonName + ": " + err.Error()
			return write(err)
		}
		if err := d.Start(ctx); err != nil {
			receipt.Result = "start mgr." + d.DaemonName + ": " + err.Error()
			return write(err)
		}
	}
	return write(awaitTransition(ctx, backend, &receipt, transitionDeadline))
}
