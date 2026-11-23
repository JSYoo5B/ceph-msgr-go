package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
)

// The persistence cell of the writeback recipes (issue #10 checkpoint 2)
// restarts the single owned OSD of the DATA fixture once, after the test
// consumer recorded which object writes completed, and observes the objects
// again. Two closed controls are admitted only by capability
// mds-writeback-persist:
//
//   - osd-store: a read-only admin socket readback of osd_objectstore and
//     osd_fast_shutdown of the owned OSD.
//   - osd-restart: one stop (SIGTERM, then SIGKILL after 30 s) and start of
//     that OSD's container, admitted only for a store the readback shows
//     keeping its objects across the stop. With osd_fast_shutdown=true (the
//     Ceph default) OSD::shutdown calls _exit(0) without unmounting the
//     store unless it has a null freelist manager; BlueStore keeps a
//     committed transaction across that exit. MemStore writes its objects to
//     its directory only in MemStore::umount (_save), which only the orderly
//     shutdown reaches: ceph-testcontainers-go turns osd_fast_shutdown off
//     for its MemStore OSDs for that reason, and the restart of a MemStore
//     OSD is refused unless the readback shows it off. An admitted MemStore
//     restart is an orderly unmount and load, not a crash.
//
// The restart runs only where native work may (nativeReady), never beside an
// OSD drop, and waits until the OSD map shows the OSD up again from a later
// epoch and every PG reports a state beginning with active from an epoch at
// or after the new up_from (the PG stats the OSD sent after it booted).
// Until then every native action, OSD drop and further restart is refused.
// Cleanup first proves that the resolved OSD is the restart's own (name and
// container ID); only then does it start a container that a failed restart
// left stopped, and it reads the OSD back up: the same running container, up
// and in, and a newer incarnation unless the stop was never issued.
//
// References at mdsFreshSource (LGPL-2.1, no code copied): osd/OSD.cc
// shutdown (osd_fast_shutdown and has_null_manager), os/memstore/MemStore.cc
// mount (_load) and umount (_save), mon/PGMap.cc dump_filtered_pg_stats and
// osd/osd_types.cc pg_stat_t::dump.
var capMDSWritebackPersist = defineCapability("mds-writeback-persist",
	"restart the single owned OSD of the MDS CephFS data fixture once, refused unless its store keeps objects across the stop",
	"MDS_FRESH=1 MDS_CEPHFS_DATA=1 +mds-writeback-fence", nil)

// mdsCephFSDataOSDRestartWait bounds the wait for the restarted OSD.
const mdsCephFSDataOSDRestartWait = 75 * time.Second

// mdsCephFSDataOSDStore is the osd-store readback.
type mdsCephFSDataOSDStore struct {
	OSD          string `json:"osd"`
	ObjectStore  string `json:"osd_objectstore"`
	FastShutdown string `json:"osd_fast_shutdown"`
}

// restartable admits a restart only of an OSD whose store keeps committed
// objects across OSD::shutdown: BlueStore, whatever osd_fast_shutdown says,
// or MemStore with osd_fast_shutdown=false, whose orderly shutdown unmounts
// and saves it.
func (s mdsCephFSDataOSDStore) restartable() error {
	switch {
	case s.ObjectStore == "bluestore" && (s.FastShutdown == "true" || s.FastShutdown == "false"):
	case s.ObjectStore == "memstore" && s.FastShutdown == "false":
	default:
		return fmt.Errorf("DATA OSD restart requires a store that keeps its objects across OSD::shutdown: %s reads back osd_objectstore=%s osd_fast_shutdown=%s", s.OSD, s.ObjectStore, s.FastShutdown)
	}
	return nil
}

// mdsCephFSDataOSDState is one native view of the owned OSD: its OSD map
// entry and the PG states the manager reports.
type mdsCephFSDataOSDState struct {
	Epoch        uint32 `json:"epoch"`
	Up           bool   `json:"up"`
	In           bool   `json:"in"`
	UpFrom       uint32 `json:"up_from"`
	PublicAddr   string `json:"public_addr"`
	PGs          int    `json:"pgs"`
	ActivePGs    int    `json:"active_pgs"`
	ReportedFrom uint32 `json:"min_reported_epoch"`
}

// mdsCephFSDataOSDRestart is the receipt of the one OSD restart of a run.
type mdsCephFSDataOSDRestart struct {
	OSD         string                 `json:"osd"`
	RequestID   int64                  `json:"request_id"`
	State       string                 `json:"state"`
	Store       mdsCephFSDataOSDStore  `json:"store"`
	ContainerID string                 `json:"container_id"`
	Before      *mdsCephFSDataOSDState `json:"before,omitempty"`
	After       *mdsCephFSDataOSDState `json:"after,omitempty"`
	StopMillis  int64                  `json:"stop_ms,omitempty"`
	StartMillis int64                  `json:"start_ms,omitempty"`
	WaitMillis  int64                  `json:"wait_ms,omitempty"`
	// Views counts the native views read while waiting; LastView and
	// LastError are the last of them.
	Views     int                    `json:"views,omitempty"`
	LastView  *mdsCephFSDataOSDState `json:"last_view,omitempty"`
	LastError string                 `json:"last_error,omitempty"`
	// CleanupStart records a start issued by cleanup after a failed restart,
	// CleanupView the OSD map entry cleanup read back.
	CleanupStart bool                   `json:"cleanup_start,omitempty"`
	CleanupView  *mdsCephFSDataOSDState `json:"cleanup_view,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

// active reports a restart that is not proven complete: native work waits.
func (r *mdsCephFSDataOSDRestart) active() bool {
	return r != nil && r.State != "restarted"
}

// parseMDSCephFSDataOSDDump reads one OSD's entry of `osd dump`.
func parseMDSCephFSDataOSDDump(data []byte, id int) (mdsCephFSDataOSDState, error) {
	var dump struct {
		Epoch uint32 `json:"epoch"`
		OSDs  []struct {
			OSD        int    `json:"osd"`
			Up         int    `json:"up"`
			In         int    `json:"in"`
			UpFrom     uint32 `json:"up_from"`
			PublicAddr string `json:"public_addr"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Epoch == 0 {
		return mdsCephFSDataOSDState{}, errors.New("DATA OSD map output differs")
	}
	for _, o := range dump.OSDs {
		if o.OSD == id {
			return mdsCephFSDataOSDState{Epoch: dump.Epoch, Up: o.Up == 1, In: o.In == 1, UpFrom: o.UpFrom, PublicAddr: o.PublicAddr}, nil
		}
	}
	return mdsCephFSDataOSDState{}, errors.New("DATA OSD map does not name the owned OSD")
}

// parseMDSCephFSDataPGStates counts the PGs of `pg ls` whose state begins
// with active and the smallest epoch any PG reported its state from.
func parseMDSCephFSDataPGStates(data []byte) (pgs, active int, minReported uint32, err error) {
	type stat struct {
		PGID          string `json:"pgid"`
		State         string `json:"state"`
		ReportedEpoch uint32 `json:"reported_epoch"`
	}
	var wrapped struct {
		Stats []stat `json:"pg_stats"`
	}
	if e := json.Unmarshal(data, &wrapped); e != nil || len(wrapped.Stats) == 0 {
		return 0, 0, 0, errors.New("DATA PG listing output differs")
	}
	for i, s := range wrapped.Stats {
		if s.PGID == "" || s.State == "" {
			return 0, 0, 0, errors.New("DATA PG listing entry differs")
		}
		if s.State == "active" || strings.HasPrefix(s.State, "active+") {
			active++
		}
		if i == 0 || s.ReportedEpoch < minReported {
			minReported = s.ReportedEpoch
		}
	}
	return len(wrapped.Stats), active, minReported, nil
}

func (c *fixtureCluster) mdsCephFSDataOSDStoreReadback(ctx context.Context) (mdsCephFSDataOSDStore, error) {
	_, daemon, err := c.mdsCephFSDataOSD(ctx)
	if err != nil {
		return mdsCephFSDataOSDStore{}, err
	}
	store := mdsCephFSDataOSDStore{OSD: daemon}
	for name, value := range map[string]*string{"osd_objectstore": &store.ObjectStore, "osd_fast_shutdown": &store.FastShutdown} {
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		data, err := adminSocket(call, c.cluster, daemon, "config", "get", name)
		stop()
		if err != nil {
			return store, err
		}
		if *value, err = parseMDSCephFSCompletionConfig(data, name); err != nil {
			return store, err
		}
	}
	return store, nil
}

// mdsCephFSDataOSDView reads the owned OSD's map entry and the PG states,
// each CLI call bounded by 15 seconds.
func (c *fixtureCluster) mdsCephFSDataOSDView(ctx context.Context, id int) (mdsCephFSDataOSDState, error) {
	call, stop := context.WithTimeout(ctx, 15*time.Second)
	data, err := c.cluster.Ceph(call, "osd", "dump", "--format", "json")
	stop()
	if err != nil {
		return mdsCephFSDataOSDState{}, err
	}
	state, err := parseMDSCephFSDataOSDDump(data, id)
	if err != nil {
		return state, err
	}
	call, stop = context.WithTimeout(ctx, 15*time.Second)
	pgs, err := c.cluster.Ceph(call, "pg", "ls", "--format", "json")
	stop()
	if err != nil {
		return state, err
	}
	state.PGs, state.ActivePGs, state.ReportedFrom, err = parseMDSCephFSDataPGStates(pgs)
	return state, err
}

// restartedOSD reports whether a view shows the OSD back from a later epoch
// with every PG active as reported after the new up_from.
func restartedOSD(before, after mdsCephFSDataOSDState) bool {
	return after.Up && after.In && after.UpFrom > before.UpFrom && after.Epoch > before.Epoch &&
		after.PGs > 0 && after.ActivePGs == after.PGs && after.ReportedFrom >= after.UpFrom
}

// osdRestartReady admits the one OSD restart of a run before any native
// change.
func (x *mdsCephFSDataScope) osdRestartReady() error {
	if x.osdRestart != nil {
		return errors.New("DATA OSD restart was already issued")
	}
	if x.osdDrop.active() {
		return errors.New("DATA OSD restart refused while the OSD client drop is not restored")
	}
	return x.nativeReady()
}

func (c *fixtureCluster) mdsCephFSDataPersistAct(ctx context.Context, id int64, a mdsCephFSDataAction, result *mdsCephFSDataResult) error {
	x := c.mdsCephFSData
	if !c.profile.admits(capMDSWritebackPersist) {
		return errors.New("DATA OSD store and restart controls require capability " + capMDSWritebackPersist.name)
	}
	switch a.name {
	case "osd-store":
		if err := x.nativeReady(); err != nil {
			return err
		}
		store, err := c.mdsCephFSDataOSDStoreReadback(ctx)
		result.OSDStore = &store
		return err
	case "osd-restart":
		if err := x.osdRestartReady(); err != nil {
			return err
		}
		d, daemon, err := c.mdsCephFSDataOSD(ctx)
		if err != nil {
			return err
		}
		store, err := c.mdsCephFSDataOSDStoreReadback(ctx)
		if err != nil {
			return err
		}
		if err := store.restartable(); err != nil {
			return err
		}
		before, err := c.mdsCephFSDataOSDView(ctx, d.ID)
		if err != nil {
			return err
		}
		if !before.Up || !before.In || before.ActivePGs != before.PGs {
			return errors.New("DATA OSD restart requires the owned OSD up and in with every PG active")
		}
		r := &mdsCephFSDataOSDRestart{OSD: daemon, RequestID: id, State: "intended", Store: store, ContainerID: d.GetContainerID(), Before: &before}
		x.osdRestart, result.OSDRestart = r, r
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		begin := time.Now()
		timeout := 30 * time.Second
		err = d.Stop(ctx, &timeout)
		r.StopMillis, r.State = time.Since(begin).Milliseconds(), "stopped"
		if err != nil {
			r.State, r.Error = "stop-uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSDataJournal())
		}
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		begin = time.Now()
		err = d.Start(ctx)
		r.StartMillis, r.State = time.Since(begin).Milliseconds(), "started"
		if err != nil {
			r.State, r.Error = "start-uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSDataJournal())
		}
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		return c.mdsCephFSDataAwaitOSD(ctx, r, d.ID)
	}
	return errors.New("DATA persistence action is not admitted")
}

// mdsCephFSDataAwaitOSD waits until the restarted OSD is proven back.
func (c *fixtureCluster) mdsCephFSDataAwaitOSD(ctx context.Context, r *mdsCephFSDataOSDRestart, id int) error {
	begin := time.Now()
	ctx, stop := context.WithTimeout(ctx, mdsCephFSDataOSDRestartWait)
	defer stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		after, err := c.mdsCephFSDataOSDView(ctx, id)
		r.Views++
		r.LastError = ""
		if err != nil {
			r.LastError = err.Error()
		} else {
			r.LastView = &after
			if restartedOSD(*r.Before, after) {
				r.After, r.State, r.WaitMillis = &after, "restarted", time.Since(begin).Milliseconds()
				return c.mdsCephFSDataJournal()
			}
		}
		select {
		case <-ctx.Done():
			r.State, r.WaitMillis = "wait-uncertain", time.Since(begin).Milliseconds()
			r.Error = fmt.Sprintf("DATA OSD did not come back with every PG active: %v", errors.Join(err, ctx.Err()))
			return errors.Join(errors.New(r.Error), c.mdsCephFSDataJournal())
		case <-tick.C:
		}
	}
}

// mdsCephFSDataRestoreOSDRestart runs at cleanup: a restart that stopped the
// OSD and did not start it again is started, and the OSD must read back
// running (restoreOSDRestart). A proven restart needs nothing.
func (c *fixtureCluster) mdsCephFSDataRestoreOSDRestart(ctx context.Context, receipt *mdsCephFSDataCleanup) error {
	r := c.mdsCephFSData.osdRestart
	receipt.OSDRestart = r
	if r == nil || r.State == "restarted" {
		receipt.OSDRunning = true
		return nil
	}
	d, daemon, err := c.mdsCephFSDataOSD(ctx)
	if err != nil {
		return err
	}
	dump := func(ctx context.Context) ([]byte, error) {
		return c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	}
	view, err := restoreOSDRestart(ctx, r, daemon, d.ID, d, dump, 500*time.Millisecond)
	if err == nil {
		r.CleanupView, receipt.OSDRunning = view, true
	}
	return errors.Join(err, c.mdsCephFSDataJournal())
}

// osdRestartCleanupDaemon is the part of the owned OSD's container handle
// that cleanup uses.
type osdRestartCleanupDaemon interface {
	GetContainerID() string
	Start(context.Context) error
	State(context.Context) (*container.State, error)
}

// restoreOSDRestart is cleanup's order of side effects. The resolved daemon
// must be the restart's owned OSD, by name and by container ID, before any
// Start: a replacement container that resolves to the same osd.N is never
// started. Only a restart that may have stopped the OSD without starting it
// again issues the one Start. The live readback proof
// (osdRestartCleanupProof) follows, polled every tick until ctx ends.
func restoreOSDRestart(ctx context.Context, r *mdsCephFSDataOSDRestart, daemon string, id int, d osdRestartCleanupDaemon,
	dump func(context.Context) ([]byte, error), tick time.Duration) (*mdsCephFSDataOSDState, error) {
	if r == nil || daemon != r.OSD || d.GetContainerID() != r.ContainerID {
		return nil, errors.New("DATA OSD restart cleanup cannot prove its owned OSD")
	}
	if slices.Contains([]string{"stopped", "stop-uncertain", "start-uncertain"}, r.State) {
		r.CleanupStart = true
		if err := d.Start(ctx); err != nil {
			return nil, err
		}
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		running, stateErr := d.State(ctx)
		data, dumpErr := dump(ctx)
		var state mdsCephFSDataOSDState
		proof := errors.Join(stateErr, dumpErr)
		if proof == nil {
			state, proof = parseMDSCephFSDataOSDDump(data, id)
		}
		if proof == nil {
			proof = osdRestartCleanupProof(r, d.GetContainerID(), running != nil && running.Running, state)
		}
		if proof == nil {
			return &state, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(errors.New("DATA OSD did not read back running at cleanup"), proof)
		case <-ticker.C:
		}
	}
}

// osdRestartMayHaveStopped reports a restart state in which the stop may
// have been issued. "intended" is stored before the intent is journaled and
// before the stop: a failed journal write leaves it with the OSD untouched.
func osdRestartMayHaveStopped(state string) bool {
	return state != "intended"
}

// osdRestartCleanupProof decides whether cleanup's readback proves the owned
// OSD running: the same owned container, running, up and in. Where the stop
// may have been issued the OSD must also be a newer incarnation (a later
// up_from); an OSD whose stop was never issued is the existing one.
func osdRestartCleanupProof(r *mdsCephFSDataOSDRestart, containerID string, running bool, state mdsCephFSDataOSDState) error {
	switch {
	case r == nil || r.Before == nil:
		return errors.New("DATA OSD restart cleanup has no receipt to prove")
	case containerID != r.ContainerID:
		return errors.New("DATA OSD restart cleanup found another container than the owned OSD's")
	case !running:
		return errors.New("DATA OSD container is not running at cleanup")
	case !state.Up || !state.In:
		return errors.New("DATA OSD is not up and in at cleanup")
	case osdRestartMayHaveStopped(r.State) && state.UpFrom <= r.Before.UpFrom:
		return errors.New("DATA OSD whose stop may have been issued is not a newer incarnation at cleanup")
	}
	return nil
}
