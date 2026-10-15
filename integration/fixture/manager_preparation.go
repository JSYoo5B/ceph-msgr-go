package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// This backend observes native fixture state only. It never handles a Go
// authentication attempt or repeats the mutation under test.
type managerPreparationBackend struct {
	observe   func(context.Context) (managerPreparationMap, error)
	rejection func(context.Context, string) (managerRefusal, error)
	stop      func(context.Context, string) error
	start     func(context.Context, string) error
	record    func(managerPreparationReceipt) error
	wait      func(context.Context) error
}

// managerRefusal is one latest-process refusal observation for an owned
// daemon: its latest exact MonClient -13 line, and the global IDs whose
// reclaim the owned MONs refused for that daemon's entity since the same
// process started. A ceph-mgr runs several MonClients (the beacon client whose
// global_id is the MgrMap GID, and module clients), so the line alone does not
// say which client lost its MON session.
type managerRefusal struct {
	Line    string
	Refused []uint64
}

type managerPreparationMap struct {
	ceph.ManagerStatus
	Epoch uint32 `json:"epoch"`
}

type managerPreparationReceipt struct {
	ObservedAt       time.Time `json:"observed_at"`
	Daemon           string    `json:"daemon"`
	NativeRejection  string    `json:"native_rejection"`
	RefusedGlobalIDs []uint64  `json:"refused_global_ids"`
	Epoch            uint32    `json:"epoch"`
	Registered       bool      `json:"registered"`
	GID              uint64    `json:"gid"`
	Action           string    `json:"action"`
	ceph.ManagerStatus
}

const (
	managerActionRegisteredRefusal = "observe native refusal in registered slot before independent failover"
	managerActionRestoreMissing    = "restore missing native daemon before independent failover"
	managerActionOtherClient       = "observe native refusal of a non-registered MonClient; registered slot retained"
)

func managerPreparationIdentities(state managerPreparationMap, names map[string]bool) (map[string]uint64, error) {
	if state.Epoch == 0 || (state.ActiveName == "") != (state.ActiveGID == 0) || state.Available && state.ActiveGID == 0 {
		return nil, errors.New("invalid native MGR preparation map identity")
	}
	identities := make(map[string]uint64)
	gids := make(map[uint64]bool)
	add := func(name string, gid uint64) error {
		if !names[name] || gid == 0 || identities[name] != 0 || gids[gid] {
			return errors.New("native MGR preparation map has outside or duplicate identity")
		}
		identities[name], gids[gid] = gid, true
		return nil
	}
	if state.ActiveGID != 0 {
		if err := add(state.ActiveName, state.ActiveGID); err != nil {
			return nil, err
		}
	}
	for _, standby := range state.Standbys {
		if err := add(standby.Name, standby.GID); err != nil {
			return nil, err
		}
	}
	return identities, nil
}

func prepareManagerFailoverWithBackend(parent context.Context, owned []string, backend managerPreparationBackend, budget time.Duration) error {
	if len(owned) != 2 || owned[0] == "" || owned[1] == "" || owned[0] == owned[1] || budget <= 0 || budget > 45*time.Second {
		return errors.New("ordinary MGR failover requires two unique owned daemons and a bounded preparation")
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	names := map[string]bool{owned[0]: true, owned[1]: true}
	type refusalState struct {
		first           managerRefusal
		observedPresent bool
		stale           bool // The MONs refused the registered GID itself.
		restarted       bool
		gids            map[uint64]bool
	}
	refused := make(map[string]*refusalState)
	retained := make(map[string]string)
	wait := backend.wait
	if wait == nil {
		wait = func(call context.Context) error {
			timer := time.NewTimer(250 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-call.Done():
				return call.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("native MGR preparation never restored unrefused active and standby: %w", err)
		}
		// A grace-retained map entry is not a live MonClient health observation.
		// Read latest-process refusals before considering membership ready.
		refusals := make(map[string]managerRefusal)
		for _, name := range owned {
			refusal, err := backend.rejection(ctx, name)
			if err != nil {
				return err
			}
			if refusal.Line == "" && len(refusal.Refused) != 0 || slices.Contains(refusal.Refused, 0) {
				return fmt.Errorf("invalid native refusal attribution for mgr.%s", name)
			}
			refusals[name] = refusal
		}
		state, err := backend.observe(ctx)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		identities, err := managerPreparationIdentities(state, names)
		if err != nil {
			return err
		}
		blocked := false
		for _, name := range owned {
			gid := identities[name]
			refusal := refusals[name]
			line := refusal.Line
			entry := refused[name]
			record := func(evidence managerRefusal, action string) error {
				return backend.record(managerPreparationReceipt{
					ObservedAt: time.Now().UTC(), Daemon: "mgr." + name, NativeRejection: evidence.Line,
					RefusedGlobalIDs: evidence.Refused, Epoch: state.Epoch, Registered: gid != 0, GID: gid,
					Action: action, ManagerStatus: state.ManagerStatus,
				})
			}
			if line == "" {
				// A clean respawn with an old map GID cannot claim restoration.
				blocked = blocked || entry != nil && (gid == 0 || entry.gids[gid])
				continue
			}
			registeredRefused := slices.Contains(refusal.Refused, gid)
			// CephxServiceHandler.cc:111-143 names the global_id whose reclaim
			// a MON refused. When every refused ID belongs to another MonClient
			// of this process, the registered beacon client was not refused.
			// Unattributed (empty) evidence never takes this branch, and neither
			// does a slot already proven stale or a daemon already restarted.
			if gid != 0 && len(refusal.Refused) != 0 && !registeredRefused && (entry == nil || !entry.stale && !entry.restarted) {
				if key := fmt.Sprint(line, refusal.Refused); retained[name] != key {
					retained[name] = key
					if err := record(refusal, managerActionOtherClient); err != nil {
						return err
					}
				}
				continue
			}
			if entry == nil {
				entry = &refusalState{first: refusal, observedPresent: gid != 0, gids: make(map[uint64]bool)}
				refused[name] = entry
				action := managerActionRegisteredRefusal
				if gid == 0 {
					action = managerActionRestoreMissing
				}
				if err := record(refusal, action); err != nil {
					return err
				}
			}
			entry.stale = entry.stale || registeredRefused
			if gid != 0 {
				entry.gids[gid] = true
			}
			// A registered GID the MONs refused, or a refusal they did not
			// attribute, cannot satisfy readiness. A new GID in a process still
			// carrying such a -13 cannot claim it either.
			blocked = true
			if gid != 0 || entry.restarted {
				continue
			}
			// MgrMonitor.cc:747-798 retains registrations until beacon expiry.
			// Actual map absence, rather than a fixed grace sleep, is the barrier.
			if entry.observedPresent {
				if err := record(entry.first, managerActionRestoreMissing); err != nil {
					return err
				}
			}
			entry.restarted = true // Never replay a failed or uncertain Stop/Start.
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := backend.stop(ctx, name); err != nil {
				return fmt.Errorf("stop refused native mgr.%s: %w", name, err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := backend.start(ctx, name); err != nil {
				return fmt.Errorf("start refused native mgr.%s: %w", name, err)
			}
		}
		if !blocked && managerFailoverReady(state.ManagerStatus, names) {
			return nil
		}
		if err := wait(ctx); err != nil {
			return fmt.Errorf("native MGR preparation never restored unrefused active and standby: %w", err)
		}
	}
}
