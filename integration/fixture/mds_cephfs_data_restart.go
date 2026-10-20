package main

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// DATA keeps its own one-restart journal and target: it does not activate the
// recovery profile or share that profile's mutable scope. The closed flow uses
// the same native semantics and source provenance documented in
// mds_cephfs_recovery.go (mdsFreshSource, LGPL-2.1 references; no native code
// copied): exact-GID fail, respawn, rank-0 adoption and old-incarnation fencing.

func (c *fixtureCluster) mdsCephFSDataRankStatus(ctx context.Context) (mdsCephFSRankStatus, error) {
	s, x := c.mdsFresh, c.mdsCephFSData
	data, err := c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return mdsCephFSRankStatus{}, err
	}
	owned := map[string]bool{}
	for _, daemon := range s.fs.MDSs() {
		if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" {
			owned[daemon.ID] = true
		}
	}
	armed := uint64(0)
	if x.restart != nil {
		armed = x.restart.Armed.GID
	}
	status, err := parseMDSCephFSRankStatus(data, s.oracle, c.profile.ipFamily, func(name string) bool { return owned[name] }, armed)
	if err != nil {
		return status, err
	}
	status.ArmedBlocklist = []mdsSessionsBlock{}
	if x.restart != nil {
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return status, err
		}
		for _, address := range x.restart.ArmedAddresses {
			if entry := mdsSessionsFindBlock(blocks, address); entry != nil {
				status.ArmedBlocklist = append(status.ArmedBlocklist, *entry)
			}
		}
	}
	return status, nil
}

// mdsCephFSDataWriteTarget publishes the current owned rank-0 incarnation. Tests
// read it instead of the startup oracle once a restart was adopted.
func (c *fixtureCluster) mdsCephFSDataWriteTarget() error {
	s, x := c.mdsFresh, c.mdsCephFSData
	restarts := 0
	if x.restart != nil && x.restart.State == "adopted" {
		restarts = 1
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-data-target.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name,
		"mds": x.current, "adopted_restarts": restarts,
	})
}

// mdsCephFSDataRestartAct runs one restart step. arm and adopt run inside the
// control with native identity proofs; fire and status do not, because the
// first is time-critical and the second observes a transitional map.
func (c *fixtureCluster) mdsCephFSDataRestartAct(ctx context.Context, id int64, action mdsCephFSDataAction, result *mdsCephFSDataResult) error {
	s, x := c.mdsFresh, c.mdsCephFSData
	switch action.name {
	case "restart-arm":
		if x.restart != nil {
			return errors.New("MDS CephFS data restart was already armed")
		}
		r, err := newMDSCephFSDataRestart(id, x.current)
		if err != nil {
			return err
		}
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return err
		}
		for _, address := range r.ArmedAddresses {
			if mdsSessionsFindBlock(blocks, address) != nil {
				return errors.New("MDS CephFS data restart target address is already blocklisted")
			}
		}
		x.restart = r
		result.Restart = r
		return c.mdsCephFSDataJournal()
	case "restart-fire":
		result.Restart = x.restart
		return x.fireRestart(ctx, id, c.mdsCephFSDataJournal, func(ctx context.Context, args ...string) ([]byte, error) {
			return c.cluster.Ceph(ctx, args...)
		})
	case "restart-status":
		r := x.restart
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS data restart status requires a fired, unadopted restart")
		}
		status, err := c.mdsCephFSDataRankStatus(ctx)
		if err != nil {
			return err
		}
		result.Restart, result.RankStatus = r, &status
		return nil
	case "restart-adopt":
		r := x.restart
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS data restart adoption requires a fired, unadopted restart")
		}
		// Full native proof: the same owned daemon name holds rank 0 up:active
		// with a new GID, nonce and incarnation; FSCID and pools unchanged.
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		status, err := c.mdsCephFSDataRankStatus(ctx)
		if err != nil {
			return err
		}
		if err := validateMDSCephFSDataAdoption(s.oracle, current, r.Armed, status); err != nil {
			return err
		}
		n := current.MDS
		config, err := c.mdsCephFSDataConfig(ctx, n)
		if err != nil {
			return err
		}
		if err := x.discardRestartRuntime(n.GID, config); err != nil {
			return err
		}
		result.Config = config
		r.OldFencing = status.ArmedBlocklist
		adopted := n
		r.Adopted, r.AdoptRequestID, r.AdoptedUTC, r.State = &adopted, id, time.Now().UTC().Format(time.RFC3339Nano), "adopted"
		x.current = n
		result.Restart, result.RankStatus = r, &status
		result.MDSGID, result.MDSEndpoint, result.MDSNonce, result.MDSIncarnation = n.GID, n.Endpoint, n.Nonce, n.Incarnation
		return errors.Join(c.mdsCephFSDataJournal(), c.mdsCephFSDataWriteTarget())
	}
	return errors.New("MDS CephFS data restart action is not admitted")
}

// blockOwner only attributes a non-baseline exact address; the caller filters
// baseline entries before invoking it. Fencing remains in place while its MDS
// GID is still mapped or its absence cannot be proven natively.
func (x *mdsCephFSDataScope) blockOwner(entry mdsSessionsBlock, armedGone bool) (string, error) {
	if entry.Range || slices.ContainsFunc(x.baselineBlocklist, func(b mdsSessionsBlock) bool { return b.Address == entry.Address && b.Range == entry.Range }) {
		return "", nil
	}
	if x.restart != nil && slices.Contains(x.restart.ArmedAddresses, entry.Address) {
		if !armedGone {
			return "", errors.New("armed MDS incarnation is still mapped; its fencing entry is kept")
		}
		return "mds-restart", nil
	}
	if x.owned[entry.Address] != 0 {
		return "owned-session", nil
	}
	return "", nil
}

// newMDSCephFSDataRestart snapshots the exact rank-0 identity before any
// mutation. No caller-supplied daemon name, GID or address is admitted.
func newMDSCephFSDataRestart(id int64, armed mdsFreshDaemon) (*mdsCephFSRestart, error) {
	if id <= 0 || armed.Name == "" || armed.GID == 0 || armed.Rank != 0 || armed.State != "up:active" || armed.Nonce == 0 || armed.Incarnation < 0 || len(armed.Addresses) == 0 {
		return nil, errors.New("MDS CephFS data restart requires a proven active rank-0 identity")
	}
	r := &mdsCephFSRestart{State: "armed", ArmRequestID: id, Armed: armed, ArmedAddresses: []string{}}
	r.Armed.Addresses = slices.Clone(armed.Addresses)
	selected := false
	for _, address := range armed.Addresses {
		formatted, err := mdsSessionsAddress(address.Addr, address.Nonce)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(r.ArmedAddresses, formatted) {
			r.ArmedAddresses = append(r.ArmedAddresses, formatted)
		}
		selected = selected || address.Type == "v2" && address.Addr == armed.Endpoint && address.Nonce == armed.Nonce
	}
	if !selected {
		return nil, errors.New("MDS CephFS data restart selected endpoint is absent from its native address vector")
	}
	return r, nil
}

// fireRestart makes the durable intent precede the single exact-GID native
// command. Even a journal failure consumes this attempt: no uncertain native
// mutation is automatically repeated. Command injection is private to fixture
// tests so that intent ordering and the exact native arguments can be checked.
func (x *mdsCephFSDataScope) fireRestart(ctx context.Context, id int64, journal func() error, command func(context.Context, ...string) ([]byte, error)) error {
	r := x.restart
	if r == nil || r.State != "armed" {
		return errors.New("MDS CephFS data restart fire requires one armed, unfired restart")
	}
	r.State, r.FireRequestID, r.FireStartedUTC = "intended", id, time.Now().UTC().Format(time.RFC3339Nano)
	if err := journal(); err != nil {
		return err
	}
	output, err := command(ctx, "mds", "fail", strconv.FormatUint(r.Armed.GID, 10))
	r.FiredUTC = time.Now().UTC().Format(time.RFC3339Nano)
	if len(output) != 0 {
		r.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
	}
	if err != nil {
		r.State, r.Error = "uncertain", err.Error()
		return errors.Join(err, journal())
	}
	r.State = "applied"
	return journal()
}

// The stable oracle and a second transitional FSMap observation must agree on
// the new owned rank. No adoption changes FS identity, pools or daemon name.
func validateMDSCephFSDataAdoption(base, current mdsFreshOracle, armed mdsFreshDaemon, status mdsCephFSRankStatus) error {
	n := current.MDS
	if _, err := newMDSCephFSDataRestart(1, n); err != nil {
		return err
	}
	if current.FSID != base.FSID || current.FSCID != base.FSCID || current.FSName != base.FSName || current.MetadataPool != base.MetadataPool || !reflect.DeepEqual(current.DataPools, base.DataPools) ||
		n.Name != armed.Name || n.Rank != 0 || n.State != "up:active" || n.GID == 0 || n.GID == armed.GID || n.Nonce == 0 || n.Nonce == armed.Nonce || n.Incarnation == armed.Incarnation {
		return errors.New("MDS CephFS data restart adoption could not prove a new incarnation of the owned daemon")
	}
	r := status.Rank0
	if status.ArmedPresent || r == nil || r.GID != n.GID || r.Name != n.Name || r.Rank != n.Rank || r.State != n.State || r.Incarnation != n.Incarnation || r.Endpoint != n.Endpoint || r.Nonce != n.Nonce {
		return errors.New("MDS CephFS data restart adoption observations disagree or the armed daemon remains mapped")
	}
	return nil
}
