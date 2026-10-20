package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// The short-ticket keyring gate observes native fixture state only, before
// (and, for attribution, after) the Go fractional-renewal attempt. A native MGR
// whose beacon MonClient the MONs refused can no longer refresh its rotating
// service keys (pinned MonClient.cc:1157-1203) and then refuses every
// authorizer minted with a newer MGR secret (RotatingKeyRing.cc:72-94). That is
// a native fixture fault, never evidence about the Go client.
//
// A ceph-mgr runs several MonClients, so a "(13) Permission denied" line in its
// own log does not say which one was refused (ceph-testcontainers-go v0.3.0
// docs/guides/messenger-client-fixtures.md, "MGR MonClients"). The gate
// therefore attributes like the preparation backend (manager_preparation.go,
// managerActionOtherClient): an owned MGR counts as stale only when an owned
// MON refused the reclaim of the GID its MgrMap registration holds, when it is
// missing from the MgrMap, or when no owned MON holds its beacon session (con
// type "mgr", MgrMap GID as global ID). A module client's refusal, attributed
// or not, leaves a registered slot with a live beacon session healthy, so a Go
// MGR -13 is never excused by an unrelated MonClient -13 line.
var shortTicketGateLabels = map[string]bool{"pre-dial": true, "post-failure": true}

const shortTicketGateFile = "manager-keyring-gate.json"

// Attribution of the latest-process -13 evidence of one owned MGR.
const (
	gateRefusalNone         = ""
	gateRefusalRegistered   = "registered-mgr"
	gateRefusalModuleClient = "module-client"
	gateRefusalUnattributed = "unattributed"
)

// State of one owned MGR's registered slot.
const (
	gateSlotRegistered = "registered"
	gateSlotRefused    = "registered-gid-refused"
	gateSlotAbsent     = "absent"
	gateSlotNoBeacon   = "no-beacon-session"
)

type shortTicketGateDaemon struct {
	Daemon           string   `json:"daemon"`
	GID              uint64   `json:"gid"`
	NativeRejection  string   `json:"native_rejection,omitempty"`
	RefusedGlobalIDs []uint64 `json:"refused_global_ids,omitempty"`
	Refusal          string   `json:"refusal_attribution,omitempty"`
	Slot             string   `json:"slot"`
	BeaconMonitors   []string `json:"beacon_monitors"`
	ModuleGlobalIDs  []uint64 `json:"module_global_ids,omitempty"`
	Stale            bool     `json:"stale"`
}

type shortTicketGateReceipt struct {
	ObservedAt   time.Time                `json:"observed_at"`
	Label        string                   `json:"label"`
	Epoch        uint32                   `json:"epoch"`
	Daemons      []shortTicketGateDaemon  `json:"daemons"`
	Sessions     []monitorManagerSessions `json:"monitor_sessions"`
	FixtureError string                   `json:"fixture_error,omitempty"`
}

// evaluateShortTicketGate classifies every owned MGR from its latest-process
// refusal evidence, the MgrMap and every owned MON's sessions. Missing or
// incomplete observations are errors, never a clean result.
func evaluateShortTicketGate(label string, state managerPreparationMap, owned []string, refusals map[string]managerRefusal, sessions []monitorManagerSessions) (shortTicketGateReceipt, error) {
	receipt := shortTicketGateReceipt{ObservedAt: time.Now().UTC(), Label: label, Epoch: state.Epoch, Sessions: sessions}
	if !shortTicketGateLabels[label] || len(owned) == 0 {
		return receipt, errors.New("invalid native MGR keyring gate scope")
	}
	names := make(map[string]bool)
	for _, name := range owned {
		if name == "" || names[name] {
			return receipt, errors.New("invalid native MGR keyring gate identity")
		}
		names[name] = true
	}
	if len(sessions) == 0 {
		return receipt, errors.New("missing native MON session observation")
	}
	for _, monitor := range sessions {
		if !monitor.complete() {
			return receipt, fmt.Errorf("native mon.%s sessions observation %s: %s", monitor.Monitor, monitor.Status, monitor.Error)
		}
	}
	identities, err := managerPreparationIdentities(state, names)
	if err != nil {
		return receipt, err
	}
	var stale []string
	for _, name := range owned {
		refusal, ok := refusals[name]
		if !ok || slices.Contains(refusal.Refused, 0) {
			return receipt, fmt.Errorf("missing native refusal observation for mgr.%s", name)
		}
		gid := identities[name]
		daemon := shortTicketGateDaemon{Daemon: "mgr." + name, GID: gid, NativeRejection: refusal.Line, RefusedGlobalIDs: refusal.Refused,
			BeaconMonitors: beaconMonitors(sessions, name, gid), ModuleGlobalIDs: moduleGlobalIDs(sessions, name)}
		if daemon.BeaconMonitors == nil {
			daemon.BeaconMonitors = []string{}
		}
		switch {
		case gid != 0 && slices.Contains(refusal.Refused, gid):
			daemon.Refusal = gateRefusalRegistered
		case len(refusal.Refused) != 0:
			daemon.Refusal = gateRefusalModuleClient
		case refusal.Line != "":
			daemon.Refusal = gateRefusalUnattributed
		}
		switch {
		case gid == 0:
			daemon.Slot = gateSlotAbsent
		case daemon.Refusal == gateRefusalRegistered:
			daemon.Slot = gateSlotRefused
		case len(daemon.BeaconMonitors) == 0:
			daemon.Slot = gateSlotNoBeacon
		default:
			daemon.Slot = gateSlotRegistered
		}
		daemon.Stale = daemon.Slot != gateSlotRegistered
		if daemon.Stale {
			stale = append(stale, fmt.Sprintf("%s (%s)", daemon.Daemon, daemon.Slot))
		}
		receipt.Daemons = append(receipt.Daemons, daemon)
	}
	if len(stale) != 0 {
		receipt.FixtureError = fmt.Sprintf("fixture error: native %s lost its registered MON session under the short-ticket profile (refused MgrMap GID reclaim, absence from the MgrMap, or no beacon session on any owned MON); its rotating MGR keyring is stale, so a Go MGR -13 is not product evidence", strings.Join(stale, ", "))
	}
	return receipt, nil
}

// shortTicketGateQueryBound bounds each MON sessions read of the gate.
const shortTicketGateQueryBound = 10 * time.Second

func (c *fixtureCluster) verifyShortTicketGate(ctx context.Context, out, label string) error {
	daemons, names, err := c.ownedManagers()
	if err != nil {
		return err
	}
	refusals := make(map[string]managerRefusal)
	for _, name := range names {
		if refusals[name], err = c.nativeManagerRefusal(ctx, name, daemons[name], true); err != nil {
			return fmt.Errorf("native MGR keyring gate: %w", err)
		}
	}
	// The sessions read must belong to one MgrMap: a map that moved meanwhile
	// would correlate registrations of two maps. Such a transitional sample is
	// read again (read-only observation, at most three samples).
	var state managerPreparationMap
	var sessions []monitorManagerSessions
	for sample := 1; ; sample++ {
		if state, err = c.observeManagerMap(ctx); err != nil {
			return fmt.Errorf("native MGR keyring gate: %w", err)
		}
		sessions = c.observeManagerSessions(ctx, shortTicketGateQueryBound)
		after, err := c.observeManagerMap(ctx)
		if err != nil {
			return fmt.Errorf("native MGR keyring gate: %w", err)
		}
		if sameManagerMap(state, after) {
			break
		}
		if sample == 3 {
			return fmt.Errorf("native MGR keyring gate: MgrMap moved from epoch %d to %d during each of %d sessions observations", state.Epoch, after.Epoch, sample)
		}
	}
	receipt, err := evaluateShortTicketGate(label, state, names, refusals, sessions)
	if err != nil {
		return fmt.Errorf("native MGR keyring gate: %w", err)
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, shortTicketGateFile), data, 0644)
}

// sameManagerMap reports whether two MgrMap reads hold the same epoch and
// registrations.
func sameManagerMap(a, b managerPreparationMap) bool {
	return a.Epoch == b.Epoch && a.Available == b.Available && a.ActiveName == b.ActiveName && a.ActiveGID == b.ActiveGID && slices.Equal(a.Standbys, b.Standbys)
}
