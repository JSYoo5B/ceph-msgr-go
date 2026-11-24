package main

import (
	"context"
	"errors"
)

// The held-lock standby takeover (issue #10) adds one closed native probe to
// the two-rank fixture: a nonblocking exclusive FLOCK of the plan file
// mig/file-0, released at once when it acquired. It runs under its own
// exclusive capability on a fresh fixture, whose one owned takeover hands
// rank 0 to the standby; every probe is journaled before it runs and an
// uncertain native process is never replayed. Native behavior relied upon:
// mds/Server.cc (handle_client_file_setlock, recover_filelocks) and
// pybind/cephfs/cephfs.pyx (flock) at mdsFreshSource (LGPL-2.1 references;
// no code copied).

// capMDSRanksHeldFlock admits one test alone on a fresh two-rank fixture with
// the lock probe.
var capMDSRanksHeldFlock = defineExclusiveCapability(fixtureMDSCephFSRanks, "mds-ranks-held-flock",
	"run one test alone on a fresh two-rank MDS CephFS fixture with the plan file's lock probe",
	"MDS_FRESH=1 MDS_CEPHFS_RANKS=1", func(p profile) bool { return p.mdsCephFSRanksReplay != "1" })

// admitMDSCephFSRanksHeldFlock refuses the lock probe without its capability.
func admitMDSCephFSRanksHeldFlock(action string, p profile) error {
	if action == "flock-ex-nb" && !p.admits(capMDSRanksHeldFlock) {
		return errors.New("MDS CephFS ranks lock probe requires capability " + capMDSRanksHeldFlock.name)
	}
	return nil
}

// flockTarget is the one plan file the lock probe admits.
func (p mdsCephFSRanksPlan) flockTarget() string {
	if p.Migrate == "" || len(p.MigrateFiles) == 0 {
		return ""
	}
	return p.Migrate + "/" + p.MigrateFiles[0]
}

// validMDSCephFSRanksFlock accepts exactly the probe's output: an acquire and
// release or EAGAIN, the unchanged empty file, nothing else.
func validMDSCephFSRanksFlock(result mdsCephFSRanksNative, extra []string) bool {
	p := result.Flock
	return len(extra) == 0 && p != nil && p.Owner == mdsCephFSFlockOwner && p.ElapsedNS > 0 &&
		(p.Errno == 0 && p.Acquired && p.Unlocked || p.Errno == 11 && !p.Acquired && !p.Unlocked) &&
		p.Before == p.After && p.Before.Ino != 0 && p.Before.Size == 0 && p.Before.DataHex == "" &&
		result.Stat == nil && result.Pin == nil && result.PinSet == nil && result.Entries == nil
}

// mdsCephFSRanksFlockProbe runs the lock probe once.
func (c *fixtureCluster) mdsCephFSRanksFlockProbe(ctx context.Context, id int64, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	x := c.mdsCephFSRanks
	if action.target == "" || action.target != x.plan.flockTarget() {
		return errors.New("MDS CephFS ranks lock probe target is not its plan file")
	}
	path, err := x.plan.path(action.target)
	if err != nil {
		return err
	}
	mutation := &mdsCephFSMutation{Kind: "native-flock-ex-nb", RequestID: id, Target: action.target, State: "intended"}
	x.lockProbes = append(x.lockProbes, mutation)
	result.Mutation = mutation
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return err
	}
	process, native, err := c.mdsCephFSRanksRunNative(ctx, "flock-ex-nb", path)
	result.Processes = append(result.Processes, process)
	if err != nil {
		mutation.State, mutation.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSRanksJournal())
	}
	result.Native = &native
	mutation.State, mutation.NativeGlobalID = "applied", native.NativeGlobalID
	return c.mdsCephFSRanksJournal()
}
