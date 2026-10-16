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
// whose MonClient the MONs refused can no longer refresh its rotating service
// keys (pinned MonClient.cc:1157-1203) and then refuses every authorizer
// minted with a newer MGR secret (RotatingKeyRing.cc:72-94). That is a native
// fixture fault, never evidence about the Go client.
var shortTicketGateLabels = map[string]bool{"pre-dial": true, "post-failure": true}

const shortTicketGateFile = "manager-keyring-gate.json"

type shortTicketGateDaemon struct {
	Daemon           string   `json:"daemon"`
	GID              uint64   `json:"gid"`
	NativeRejection  string   `json:"native_rejection,omitempty"`
	RefusedGlobalIDs []uint64 `json:"refused_global_ids,omitempty"`
	Stale            bool     `json:"stale"`
}

type shortTicketGateReceipt struct {
	ObservedAt   time.Time               `json:"observed_at"`
	Label        string                  `json:"label"`
	Epoch        uint32                  `json:"epoch"`
	Daemons      []shortTicketGateDaemon `json:"daemons"`
	FixtureError string                  `json:"fixture_error,omitempty"`
}

// evaluateShortTicketGate marks an owned MGR stale when its latest process
// logged a MonClient -13, or when an owned MON refused to reclaim the GID the
// MgrMap registers for it (that is the beacon MonClient whose keyring the
// MGR's authorizer verifier uses).
func evaluateShortTicketGate(label string, state managerPreparationMap, owned []string, refusals map[string]managerRefusal) (shortTicketGateReceipt, error) {
	receipt := shortTicketGateReceipt{ObservedAt: time.Now().UTC(), Label: label, Epoch: state.Epoch}
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
		daemon := shortTicketGateDaemon{Daemon: "mgr." + name, GID: gid, NativeRejection: refusal.Line, RefusedGlobalIDs: refusal.Refused}
		daemon.Stale = refusal.Line != "" || gid != 0 && slices.Contains(refusal.Refused, gid)
		if daemon.Stale {
			stale = append(stale, daemon.Daemon)
		}
		receipt.Daemons = append(receipt.Daemons, daemon)
	}
	if len(stale) != 0 {
		receipt.FixtureError = fmt.Sprintf("fixture error: native %s lost its MON session (MonClient -13 or refused MgrMap GID reclaim) under the short-ticket profile; its rotating MGR keyring is stale, so a Go MGR -13 is not product evidence", strings.Join(stale, ", "))
	}
	return receipt, nil
}

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
	state, err := c.observeManagerMap(ctx)
	if err != nil {
		return fmt.Errorf("native MGR keyring gate: %w", err)
	}
	receipt, err := evaluateShortTicketGate(label, state, names, refusals)
	if err != nil {
		return fmt.Errorf("native MGR keyring gate: %w", err)
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, shortTicketGateFile), data, 0644)
}
