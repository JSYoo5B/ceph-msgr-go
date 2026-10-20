package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS recovery acceptance extends the CephFS profile with a second
// owned directory, a third identity holding an OSD read grant on the owned
// data pool, one native create+write, one owned client eviction and one owned
// restart of the rank-0 MDS daemon. The restart fails exactly the armed daemon
// GID (`ceph mds fail <gid>`); the daemon respawns in its own container, takes
// rank 0 again with a new GID and address nonce, and the fixture adopts that
// incarnation only after proving it natively. Every mutation is journaled
// before it is issued and never reissued. Cleanup removes only blocklist
// entries it can attribute to those owned mutations. Native behavior relied
// upon: mon/MDSMonitor.cc (fail_mds_gid), mon/OSDMonitor.cc (blocklist),
// mds/MDSDaemon.cc (respawn after removal from the map), mds/MDSRank.cc
// (evict_client), mds/Server.cc (kill_session, reconnect) and
// osd/PrimaryLogPG.cc (do_op blocklist check) at mdsFreshSource (LGPL-2.1
// references; no code copied).

// mdsCephFSRecoveryNames is the closed set of file names in the recovery
// directory. Directory and payload are token-bound.
var mdsCephFSRecoveryNames = []string{
	"flush-plain-0", "flush-plain-1", "flush-plain-2",
	"flush-requested-0", "flush-requested-1", "flush-requested-2",
	"fenced", "held", "replayed", "unanswered",
}

const mdsCephFSFencedBytes = 5000

type mdsCephFSRecoveryPlan struct {
	Directory        string   `json:"directory"`
	FlushPlain       []string `json:"flush_plain"`
	FlushRequested   []string `json:"flush_requested"`
	Fenced           string   `json:"fenced"`
	Held             string   `json:"held"`
	Replayed         string   `json:"replayed"`
	Unanswered       string   `json:"unanswered"`
	FencedPayloadHex string   `json:"fenced_payload_hex"`
}

func mdsCephFSRecoveryPlanFor(token string) mdsCephFSRecoveryPlan {
	// A deterministic binary payload larger than one 4 KiB page.
	var payload []byte
	block := sha256.Sum256([]byte("ceph-msgr-cephfs-fenced:" + token))
	for len(payload) < mdsCephFSFencedBytes {
		payload = append(payload, block[:]...)
		block = sha256.Sum256(block[:])
	}
	return mdsCephFSRecoveryPlan{Directory: "ceph-msgr-cephfs-" + token + "-recovery",
		FlushPlain: []string{"flush-plain-0", "flush-plain-1", "flush-plain-2"}, FlushRequested: []string{"flush-requested-0", "flush-requested-1", "flush-requested-2"},
		Fenced: "fenced", Held: "held", Replayed: "replayed", Unanswered: "unanswered", FencedPayloadHex: hex.EncodeToString(payload[:mdsCephFSFencedBytes])}
}

// path maps a closed recovery target to its absolute path. The directory
// keeps the "ceph-msgr-cephfs-" prefix that the native driver admits.
func (p mdsCephFSRecoveryPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-cephfs-") || !strings.HasSuffix(p.Directory, "-recovery") || strings.ContainsAny(p.Directory, "/ \x00") {
		return "", errors.New("MDS CephFS recovery directory is not owned")
	}
	root := "/" + p.Directory
	switch {
	case target == "dir":
		return root, nil
	case slices.Contains(mdsCephFSRecoveryNames, target):
		return root + "/" + target, nil
	}
	return "", errors.New("MDS CephFS recovery target is not admitted")
}

func mdsCephFSRecoveryCaps(fsName string) ceph.ClientCaps {
	// The recovery Go client opens CephFS sessions and reads the owned data
	// pool through explicit OSD requests; it never writes object data.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName, OSD: "allow r tag cephfs data=" + fsName}
}

// mdsCephFSRestart records the one owned rank-0 restart.
type mdsCephFSRestart struct {
	State          string             `json:"state"` // armed, intended, applied, uncertain, adopted
	ArmRequestID   int64              `json:"arm_request_id"`
	FireRequestID  int64              `json:"fire_request_id,omitempty"`
	AdoptRequestID int64              `json:"adopt_request_id,omitempty"`
	Armed          mdsFreshDaemon     `json:"armed"`
	ArmedAddresses []string           `json:"armed_blocklist_addresses"`
	Output         string             `json:"native_output,omitempty"`
	Error          string             `json:"error,omitempty"`
	FireStartedUTC string             `json:"fire_started_utc,omitempty"`
	FiredUTC       string             `json:"fired_utc,omitempty"`
	Adopted        *mdsFreshDaemon    `json:"adopted,omitempty"`
	AdoptedUTC     string             `json:"adopted_utc,omitempty"`
	OldFencing     []mdsSessionsBlock `json:"old_incarnation_blocklist,omitempty"`
}

func (r *mdsCephFSRestart) pending() bool {
	return r != nil && r.State != "armed" && r.State != "adopted"
}

// mdsCephFSRankDaemon is one daemon of the owned filesystem's FSMap view.
type mdsCephFSRankDaemon struct {
	Name        string `json:"name"`
	GID         uint64 `json:"gid"`
	Rank        int32  `json:"rank"`
	State       string `json:"state"`
	Incarnation int32  `json:"incarnation"`
	Endpoint    string `json:"endpoint,omitempty"`
	Nonce       uint32 `json:"nonce,omitempty"`
}

// mdsCephFSRankStatus is a transitional read of rank 0 during the owned
// restart. It never requires an active daemon.
type mdsCephFSRankStatus struct {
	FSMapEpoch     uint32                `json:"fsmap_epoch"`
	MDSMapEpoch    uint32                `json:"mdsmap_epoch"`
	Failed         []int                 `json:"failed"`
	Rank0          *mdsCephFSRankDaemon  `json:"rank0,omitempty"`
	Daemons        []mdsCephFSRankDaemon `json:"daemons"`
	Standbys       []mdsCephFSRankDaemon `json:"owned_standbys"`
	ArmedPresent   bool                  `json:"armed_gid_present"`
	ArmedBlocklist []mdsSessionsBlock    `json:"armed_blocklist"`
}

func mdsCephFSSelectAddress(vector []mdsFreshAddress, family string) (string, uint32) {
	for _, address := range vector {
		endpoint, err := netip.ParseAddrPort(address.Addr)
		if err != nil || address.Type != "v2" {
			continue
		}
		if (family == "4") == endpoint.Addr().Is4() {
			return address.Addr, address.Nonce
		}
	}
	return "", 0
}

// parseMDSCephFSRankStatus reads one native `fs dump`. Only the owned
// filesystem identity (FSCID, name, pools) must be unchanged; any rank state,
// including a failed rank, is reported as observed.
func parseMDSCephFSRankStatus(dump []byte, oracle mdsFreshOracle, family string, owned func(string) bool, armedGID uint64) (mdsCephFSRankStatus, error) {
	var status mdsCephFSRankStatus
	if len(dump) == 0 || len(dump) > mdsFreshJSONLimit || !json.Valid(dump) {
		return status, errors.New("MDS CephFS restart status exceeds its bound or is invalid")
	}
	type daemon struct {
		mdsFreshDaemon
		Addrs struct {
			Vector []mdsFreshAddress `json:"addrvec"`
		} `json:"addrs"`
	}
	var fsmap struct {
		Epoch       uint32 `json:"epoch"`
		Filesystems []struct {
			ID  int64 `json:"id"`
			Map struct {
				Epoch        uint32            `json:"epoch"`
				Name         string            `json:"fs_name"`
				MaxMDS       int               `json:"max_mds"`
				MetadataPool int64             `json:"metadata_pool"`
				DataPools    []int64           `json:"data_pools"`
				Up           map[string]uint64 `json:"up"`
				Failed       []int             `json:"failed"`
				Info         map[string]daemon `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
		Standbys []daemon `json:"standbys"`
	}
	if err := json.Unmarshal(dump, &fsmap); err != nil || fsmap.Epoch == 0 || len(fsmap.Filesystems) != 1 || len(fsmap.Standbys) > 64 {
		return status, errors.New("MDS CephFS restart status FSMap shape differs")
	}
	fs := fsmap.Filesystems[0]
	m := fs.Map
	if fs.ID != oracle.FSCID || m.Name != oracle.FSName || m.MaxMDS != 1 || m.MetadataPool != oracle.MetadataPool || !reflect.DeepEqual(m.DataPools, oracle.DataPools) || len(m.Info) > 8 {
		return status, errors.New("MDS CephFS restart status changed the owned filesystem identity")
	}
	status.FSMapEpoch, status.MDSMapEpoch, status.Failed = fsmap.Epoch, m.Epoch, m.Failed
	if status.Failed == nil {
		status.Failed = []int{}
	}
	describe := func(d daemon) mdsCephFSRankDaemon {
		endpoint, nonce := mdsCephFSSelectAddress(d.Addrs.Vector, family)
		return mdsCephFSRankDaemon{Name: d.Name, GID: d.GID, Rank: d.Rank, State: d.State, Incarnation: d.Incarnation, Endpoint: endpoint, Nonce: nonce}
	}
	status.Daemons, status.Standbys = []mdsCephFSRankDaemon{}, []mdsCephFSRankDaemon{}
	for _, d := range m.Info {
		status.Daemons = append(status.Daemons, describe(d))
		status.ArmedPresent = status.ArmedPresent || d.GID == armedGID
	}
	slices.SortFunc(status.Daemons, func(a, b mdsCephFSRankDaemon) int { return int(a.Rank) - int(b.Rank) })
	if gid, ok := m.Up["mds_0"]; ok {
		d, present := m.Info["gid_"+strconv.FormatUint(gid, 10)]
		if !present || d.GID != gid || d.Rank != 0 {
			return status, errors.New("MDS CephFS restart status rank 0 entry differs")
		}
		rank0 := describe(d)
		status.Rank0 = &rank0
	}
	for _, d := range fsmap.Standbys {
		status.ArmedPresent = status.ArmedPresent || d.GID == armedGID
		if owned(d.Name) {
			status.Standbys = append(status.Standbys, describe(d))
		}
	}
	return status, nil
}

func (c *fixtureCluster) mdsCephFSRankStatus(ctx context.Context) (mdsCephFSRankStatus, error) {
	s, x := c.mdsFresh, c.mdsCephFS
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

// mdsCephFSWriteTarget publishes the current owned rank-0 incarnation. Tests
// read it instead of the startup oracle once a restart was adopted.
func (c *fixtureCluster) mdsCephFSWriteTarget() error {
	s, x := c.mdsFresh, c.mdsCephFS
	restarts := 0
	if x.restart != nil && x.restart.State == "adopted" {
		restarts = 1
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-target.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name,
		"mds": x.current, "adopted_restarts": restarts,
	})
}

// mdsCephFSRestartAct runs one restart step. arm and adopt run inside the
// ordinary identity-checked control; fire and status do not, because the
// first is time-critical and the second observes a transitional map.
func (c *fixtureCluster) mdsCephFSRestartAct(ctx context.Context, id int64, action mdsCephFSAction, result *mdsCephFSResult) error {
	s, x := c.mdsFresh, c.mdsCephFS
	switch action.name {
	case "restart-arm":
		if x.restart != nil {
			return errors.New("MDS CephFS restart was already armed")
		}
		armed := x.current
		r := &mdsCephFSRestart{State: "armed", ArmRequestID: id, Armed: armed, ArmedAddresses: []string{}}
		for _, address := range armed.Addresses {
			formatted, err := mdsSessionsAddress(address.Addr, address.Nonce)
			if err != nil {
				return err
			}
			r.ArmedAddresses = append(r.ArmedAddresses, formatted)
		}
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return err
		}
		for _, address := range r.ArmedAddresses {
			if mdsSessionsFindBlock(blocks, address) != nil {
				return errors.New("MDS CephFS restart target address is already blocklisted")
			}
		}
		x.restart = r
		result.Restart = r
		return c.mdsCephFSJournal()
	case "restart-fire":
		r := x.restart
		if r == nil || r.State != "armed" {
			return errors.New("MDS CephFS restart fire requires one armed, unfired restart")
		}
		// Journal the intent first; the outcome is recorded once and the
		// command is never reissued, whatever it returns.
		r.State, r.FireRequestID, r.FireStartedUTC = "intended", id, time.Now().UTC().Format(time.RFC3339Nano)
		result.Restart = r
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
		// The armed GID names exactly the proven incarnation: a different
		// daemon in rank 0 cannot be failed by this command.
		output, err := c.cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(r.Armed.GID, 10))
		r.FiredUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if len(output) != 0 {
			r.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
		}
		if err != nil {
			r.State, r.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSJournal())
		}
		r.State = "applied"
		return c.mdsCephFSJournal()
	case "restart-status":
		r := x.restart
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS restart status requires a fired, unadopted restart")
		}
		status, err := c.mdsCephFSRankStatus(ctx)
		if err != nil {
			return err
		}
		result.Restart, result.RankStatus = r, &status
		return nil
	case "restart-adopt":
		r := x.restart
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS restart adoption requires a fired, unadopted restart")
		}
		// Full native proof: the same owned daemon name holds rank 0 up:active
		// with a new GID, nonce and incarnation; FSCID and pools unchanged.
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		a, n := r.Armed, current.MDS
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) ||
			n.Name != a.Name || n.Rank != 0 || n.State != "up:active" || n.GID == a.GID || n.Nonce == a.Nonce || n.Nonce == 0 || n.Incarnation == a.Incarnation {
			return errors.New("MDS CephFS restart adoption could not prove a new incarnation of the owned daemon")
		}
		status, err := c.mdsCephFSRankStatus(ctx)
		if err != nil {
			return err
		}
		if status.ArmedPresent || status.Rank0 == nil || status.Rank0.GID != n.GID {
			return errors.New("MDS CephFS restart adoption found the armed daemon still mapped")
		}
		r.OldFencing = status.ArmedBlocklist
		adopted := n
		r.Adopted, r.AdoptRequestID, r.AdoptedUTC, r.State = &adopted, id, time.Now().UTC().Format(time.RFC3339Nano), "adopted"
		x.current = n
		result.Restart, result.RankStatus = r, &status
		result.MDSGID, result.MDSEndpoint, result.MDSNonce, result.MDSIncarnation = n.GID, n.Endpoint, n.Nonce, n.Incarnation
		return errors.Join(c.mdsCephFSJournal(), c.mdsCephFSWriteTarget())
	}
	return errors.New("MDS CephFS restart action is not admitted")
}

// mdsCephFSEvict evicts one open session of the owned recovery identity and
// waits until the session is gone and its exact address is blocklisted.
func (c *fixtureCluster) mdsCephFSEvict(ctx context.Context, id int64, gid uint64, result *mdsCephFSResult) error {
	s, x := c.mdsFresh, c.mdsCephFS
	if x.recClient == nil {
		return errors.New("MDS CephFS eviction requires the owned recovery identity")
	}
	sessions, _, err := c.mdsSessionsList(ctx)
	if err != nil {
		return err
	}
	var victim *mdsSessionsNative
	for i := range sessions {
		if sessions[i].ID == gid {
			victim = &sessions[i]
		}
	}
	if victim == nil || victim.State != "open" || victim.AuthID != "mds-cephfs-recovery-"+s.token {
		return errors.New("MDS CephFS eviction target is not an open session of the owned recovery identity")
	}
	if slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == "eviction" && m.GlobalID == gid }) {
		return errors.New("MDS CephFS eviction of this GlobalID was already issued")
	}
	before, err := c.mdsSessionsBlocklist(ctx)
	if err != nil {
		return err
	}
	if mdsSessionsFindBlock(before, victim.Address) != nil || mdsSessionsFindBlock(x.baselineBlocklist, victim.Address) != nil {
		return errors.New("MDS CephFS eviction target address is already blocklisted")
	}
	x.owned[victim.Address] = gid
	m := &mdsCephFSMutation{Kind: "eviction", RequestID: id, Target: strconv.FormatUint(gid, 10), State: "intended", GlobalID: gid, Address: victim.Address}
	x.mutations = append(x.mutations, m)
	result.Mutation = m
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	output, err := c.mdsSessionsDaemon(ctx, "session", "evict", "id="+strconv.FormatUint(gid, 10))
	if len(output) != 0 {
		m.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
	}
	if err != nil {
		m.State, m.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSJournal())
	}
	m.State = "requested"
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		sessions, _, err := c.mdsSessionsList(ctx)
		if err != nil {
			return err
		}
		present := slices.ContainsFunc(sessions, func(n mdsSessionsNative) bool { return n.ID == gid })
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return err
		}
		if entry := mdsSessionsFindBlock(blocks, victim.Address); entry != nil && !present {
			m.State, m.Until = "evicted", entry.Until
			return c.mdsCephFSJournal()
		}
		select {
		case <-ctx.Done():
			err := fmt.Errorf("MDS CephFS eviction did not remove the session and blocklist its address: %w", ctx.Err())
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSJournal())
		case <-tick.C:
		}
	}
}

// mdsCephFSRecoveryNative runs the native driver on the recovery directory.
// create-write is one-shot.
func (c *fixtureCluster) mdsCephFSRecoveryNative(ctx context.Context, id int64, action mdsCephFSAction, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	path, err := x.recovery.path(action.target)
	if err != nil {
		return err
	}
	native, extra := strings.TrimPrefix(action.name, "recovery-"), []string(nil)
	var mutation *mdsCephFSMutation
	if action.name == "recovery-write" {
		if x.mutated("native-create-write") {
			return errors.New("MDS CephFS native-create-write was already issued")
		}
		native, extra = "create-write", []string{x.recovery.FencedPayloadHex}
		mutation = &mdsCephFSMutation{Kind: "native-create-write", RequestID: id, Target: x.recovery.Fenced, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
	}
	process, output, err := c.mdsCephFSRunNative(ctx, x.recovery.Directory, native, path, extra...)
	result.Process = process
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSJournal())
		}
		return err
	}
	result.Native = &output
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", output.NativeGlobalID
		return c.mdsCephFSJournal()
	}
	return nil
}

// decodeMDSCephFSRecoveryNative applies the recovery-specific shape on top
// of the generic driver output checks.
func decodeMDSCephFSRecoveryNative(action, path string, plan mdsCephFSRecoveryPlan, output mdsCephFSNative) (mdsCephFSNative, error) {
	if output.Action != action || output.Path != path || output.NativeGlobalID == 0 {
		return mdsCephFSNative{}, errors.New("native CephFS recovery output identity differs")
	}
	ok := false
	switch action {
	case "stat":
		ok = output.Stat != nil && validMDSCephFSStat(*output.Stat)
	case "list":
		ok = len(output.Entries) <= 64
		seen := map[string]bool{}
		for _, entry := range output.Entries {
			ok = ok && slices.Contains(mdsCephFSRecoveryNames, entry.Name) && !seen[entry.Name] && entry.DIno != 0 && validMDSCephFSStat(entry.Stat)
			seen[entry.Name] = true
		}
	case "create-write":
		ok = output.Written == len(plan.FencedPayloadHex)/2 && output.Stat != nil && output.Stat.Exists && validMDSCephFSStat(*output.Stat) && output.Stat.Size == uint64(output.Written)
	}
	if !ok {
		return mdsCephFSNative{}, fmt.Errorf("native CephFS recovery %s output shape differs", action)
	}
	return output, nil
}

// mdsCephFSRestoreBlocklist removes only new entries attributable to owned
// mutations: the eviction entry (exact address and expiry recorded when it
// appeared), the armed MDS incarnation's addresses (after the armed GID left
// the FSMap) and addresses of owned Go sessions observed by the test (for
// example an eviction at the end of a missed reconnect window). Anything else
// is reported and left untouched.
func (c *fixtureCluster) mdsCephFSRestoreBlocklist(ctx context.Context, receipt *mdsCephFSCleanup, fail func(error)) {
	x := c.mdsCephFS
	receipt.BlocklistRestored = true
	armedGone := false
	if x.restart != nil {
		status, err := c.mdsCephFSRankStatus(ctx)
		fail(err)
		armedGone = err == nil && !status.ArmedPresent
	}
	for _, entry := range receipt.NewBlocklist {
		kind, err := x.blockOwner(entry, armedGone)
		fail(err)
		if kind == "" {
			receipt.KeptBlocklist = append(receipt.KeptBlocklist, entry)
			receipt.BlocklistRestored = false
			continue
		}
		if _, err := c.cluster.Ceph(ctx, "osd", "blocklist", "rm", entry.Address); err != nil {
			receipt.BlocklistRestored = false
			fail(err)
			continue
		}
		receipt.RemovedBlocklist = append(receipt.RemovedBlocklist, mdsCephFSRemovedBlock{Kind: kind, Entry: entry})
	}
	if len(receipt.KeptBlocklist) != 0 {
		fail(errors.New("MDS CephFS acceptance kept new blocklist entries it cannot attribute to an owned mutation"))
	}
	final, err := c.mdsSessionsBlocklist(ctx)
	fail(err)
	receipt.FinalBlocklist = final
	if err != nil {
		receipt.BlocklistRestored = false
		return
	}
	for _, removed := range receipt.RemovedBlocklist {
		if mdsSessionsFindBlock(final, removed.Entry.Address) != nil {
			receipt.BlocklistRestored = false
			fail(errors.New("owned blocklist entry remained after removal"))
		}
	}
}

// blockOwner attributes one new blocklist entry to an owned mutation, or
// returns "" when it is not attributable (or attribution is unsafe).
func (x *mdsCephFSScope) blockOwner(entry mdsSessionsBlock, armedGone bool) (string, error) {
	switch {
	case entry.Range:
		return "", nil
	case slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == "eviction" && m.Address == entry.Address }):
		for _, m := range x.mutations {
			if m.Kind == "eviction" && m.Address == entry.Address && m.Until != "" && m.Until != entry.Until {
				return "", errors.New("owned eviction blocklist entry changed outside the fixture; refusing removal")
			}
		}
		return "eviction", nil
	case x.restart != nil && slices.Contains(x.restart.ArmedAddresses, entry.Address):
		if !armedGone {
			return "", errors.New("armed MDS incarnation is still mapped; its fencing entry is kept")
		}
		return "mds-restart", nil
	case x.owned[entry.Address] != 0:
		return "owned-session", nil
	}
	return "", nil
}

type mdsCephFSRemovedBlock struct {
	Kind  string           `json:"kind"`
	Entry mdsSessionsBlock `json:"entry"`
}

// mdsCephFSHistoricOp is one completed MDS request of an owned Go session
// from the daemon's op tracker (dump_historic_ops), reduced to its native
// description, outcome and event names.
type mdsCephFSHistoricOp struct {
	Description string   `json:"description"`
	InitiatedAt string   `json:"initiated_at"`
	Duration    float64  `json:"duration"`
	FlagPoint   string   `json:"flag_point"`
	Result      *int64   `json:"result"`
	Events      []string `json:"events"`
}

// parseMDSCephFSHistoricOps keeps only client requests of one GlobalID.
func parseMDSCephFSHistoricOps(data []byte, gid uint64) ([]mdsCephFSHistoricOp, error) {
	if len(data) == 0 || len(data) > mdsSessionsJSONLimit || !json.Valid(data) {
		return nil, errors.New("MDS historic ops exceed their bound or are invalid")
	}
	var native struct {
		Ops []struct {
			Description string  `json:"description"`
			InitiatedAt string  `json:"initiated_at"`
			Duration    float64 `json:"duration"`
			TypeData    struct {
				FlagPoint string `json:"flag_point"`
				Result    *int64 `json:"result"`
				OpType    string `json:"op_type"`
				Events    []struct {
					Event string `json:"event"`
				} `json:"events"`
			} `json:"type_data"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Ops == nil || len(native.Ops) > 4096 {
		return nil, errors.New("MDS historic ops shape differs")
	}
	prefix := "client_request(client." + strconv.FormatUint(gid, 10) + ":"
	result := []mdsCephFSHistoricOp{}
	for _, op := range native.Ops {
		if !strings.HasPrefix(op.Description, prefix) || op.TypeData.OpType != "client_request" || len(op.Description) > 4096 {
			continue
		}
		if len(result) == 64 {
			return nil, errors.New("MDS historic ops of one client exceed their bound")
		}
		h := mdsCephFSHistoricOp{Description: op.Description, InitiatedAt: op.InitiatedAt, Duration: op.Duration, FlagPoint: op.TypeData.FlagPoint, Result: op.TypeData.Result, Events: []string{}}
		for _, event := range op.TypeData.Events[:min(len(op.TypeData.Events), 64)] {
			h.Events = append(h.Events, event.Event)
		}
		result = append(result, h)
	}
	return result, nil
}

// mdsCephFSHistoricOps reads the op tracker for one observed owned session.
func (c *fixtureCluster) mdsCephFSHistoricOps(ctx context.Context, gid uint64, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	if !slices.Contains(slices.Collect(maps.Values(x.owned)), gid) {
		return errors.New("MDS historic ops are admitted only for an observed owned Go session")
	}
	data, err := c.mdsSessionsDaemon(ctx, "dump_historic_ops", "--format", "json")
	if err != nil {
		return err
	}
	result.HistoricOps, err = parseMDSCephFSHistoricOps(data, gid)
	return err
}

// ownedGoAuthID reports the native auth IDs of the owned Go identities whose
// session addresses cleanup may attribute.
func ownedGoAuthID(token, authID string) bool {
	return authID == "mds-cephfs-"+token || authID == "mds-cephfs-recovery-"+token
}
