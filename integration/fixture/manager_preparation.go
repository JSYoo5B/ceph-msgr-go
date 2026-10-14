package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// This backend observes native fixture state only. It never handles a Go
// authentication attempt or repeats the mutation under test.
type managerPreparationBackend struct {
	observe   func(context.Context) (managerPreparationMap, error)
	rejection func(context.Context, string) (string, error)
	stop      func(context.Context, string) error
	start     func(context.Context, string) error
	record    func(managerPreparationReceipt) error
	wait      func(context.Context) error
}

type managerPreparationMap struct {
	ceph.ManagerStatus
	Epoch uint32 `json:"epoch"`
}

type managerPreparationReceipt struct {
	ObservedAt      time.Time `json:"observed_at"`
	Daemon          string    `json:"daemon"`
	NativeRejection string    `json:"native_rejection"`
	Epoch           uint32    `json:"epoch"`
	Registered      bool      `json:"registered"`
	GID             uint64    `json:"gid"`
	Action          string    `json:"action"`
	ceph.ManagerStatus
}

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
		first           string
		observedPresent bool
		restarted       bool
		gids            map[uint64]bool
	}
	refused := make(map[string]*refusalState)
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
		rejections := make(map[string]string)
		for _, name := range owned {
			line, err := backend.rejection(ctx, name)
			if err != nil {
				return err
			}
			rejections[name] = line
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
			line := rejections[name]
			entry := refused[name]
			record := func(rejection, action string) error {
				return backend.record(managerPreparationReceipt{
					ObservedAt: time.Now().UTC(), Daemon: "mgr." + name, NativeRejection: rejection,
					Epoch: state.Epoch, Registered: gid != 0, GID: gid, Action: action, ManagerStatus: state.ManagerStatus,
				})
			}
			if line != "" && entry == nil {
				entry = &refusalState{first: line, observedPresent: gid != 0, gids: make(map[uint64]bool)}
				refused[name] = entry
				action := "observe native refusal in registered slot before independent failover"
				if gid == 0 {
					action = "restore missing native daemon before independent failover"
				}
				if err := record(line, action); err != nil {
					return err
				}
			}
			if entry == nil {
				continue
			}
			if line != "" && gid != 0 {
				entry.gids[gid] = true
			}
			// A clean respawn with an old map GID cannot claim restoration.
			// A new GID in a process still carrying -13 cannot claim it either.
			blocked = blocked || line != "" || gid == 0 || entry.gids[gid]
			if gid != 0 || line == "" || entry.restarted {
				continue
			}
			// MgrMonitor.cc:747-798 retains registrations until beacon expiry.
			// Actual map absence, rather than a fixed grace sleep, is the barrier.
			if entry.observedPresent {
				if err := record(entry.first, "restore missing native daemon before independent failover"); err != nil {
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
