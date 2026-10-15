package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS multi-rank acceptance (CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1) runs on
// the owned MDS fresh filesystem created with two active ranks and one
// standby. It drives a closed set of native actions: per-rank observations
// (session ls, get subtrees, the migration and forward counters), a native
// LibCephFS stat, owned export pins on two Go-created directories (set and
// restored with an independent fresh-mount readback, never trusted from the
// mount that set them), a bounded wait for converged subtree authority, and
// one owned standby takeover (`ceph mds fail` of exactly the proven rank-0
// GID). Every mutation is journaled before it is issued and never reissued.
// Cleanup restores the pins it still can, waits for the two-active and
// one-standby topology, removes only blocklist entries attributable to the
// takeover or an observed owned Go session, and removes its two identities.
// Native behavior relied upon: mds/MDBalancer.cc (handle_export_pins),
// mds/Migrator.cc, mds/MDSRank.cc (command_get_subtrees, perf counters,
// forward_message_mds), mds/Server.cc (handle_set_vxattr ceph.dir.pin) and
// mon/MDSMonitor.cc (fail_mds_gid) at mdsFreshSource (LGPL-2.1 references; no
// code copied). The pin recipe follows JSYoo5B/ceph-testcontainers-go#48.

const mdsCephFSRanksJSONLimit = 64 << 10

// mdsCephFSRanksPlan is the fixed namespace of one run. The Go client creates
// it; the fixture only stats it and pins its two subdirectories.
type mdsCephFSRanksPlan struct {
	Directory    string   `json:"directory"`
	Forward      string   `json:"forward"`
	ForwardFile  string   `json:"forward_file"`
	Migrate      string   `json:"migrate"`
	MigrateFiles []string `json:"migrate_files"`
}

func mdsCephFSRanksPlanFor(token string) mdsCephFSRanksPlan {
	return mdsCephFSRanksPlan{Directory: "ceph-msgr-ranks-" + token, Forward: "fwd", ForwardFile: "target", Migrate: "mig", MigrateFiles: []string{"file-0", "file-1", "file-2"}}
}

// targets lists the closed target names of the plan.
func (p mdsCephFSRanksPlan) targets() []string {
	result := []string{"dir", p.Forward, p.Forward + "/" + p.ForwardFile, p.Migrate}
	for _, name := range p.MigrateFiles {
		result = append(result, p.Migrate+"/"+name)
	}
	return result
}

func (p mdsCephFSRanksPlan) pinTarget(target string) bool {
	return target == p.Forward || target == p.Migrate
}

// path maps a closed target name to its absolute path in the owned filesystem.
func (p mdsCephFSRanksPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-ranks-") || strings.ContainsAny(p.Directory, "/ \x00") {
		return "", errors.New("MDS CephFS ranks plan directory is not owned")
	}
	if !slices.Contains(p.targets(), target) {
		return "", errors.New("MDS CephFS ranks target is not admitted")
	}
	if target == "dir" {
		return "/" + p.Directory, nil
	}
	return "/" + p.Directory + "/" + target, nil
}

func mdsCephFSRanksGoCaps(fsName string) ceph.ClientCaps {
	// Metadata only: MON read and a read-write MDS grant bound to the owned
	// filesystem. The Go client never sets layout, quota or pin vxattrs.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName}
}

func mdsCephFSRanksNativeCaps(fsName string) ceph.ClientCaps {
	// The native oracle stats and pins; ceph.dir.pin requires the 'p' flag
	// (Server::handle_set_vxattr checks MAY_SET_VXATTR). No data access.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rwp fsname=" + fsName}
}

// mdsCephFSRanksDaemon is one daemon of the owned filesystem as one native
// FSMap reports it, with the endpoint and nonce of the selected family.
type mdsCephFSRanksDaemon struct {
	Name        string            `json:"name"`
	GID         uint64            `json:"gid"`
	Rank        int32             `json:"rank"`
	State       string            `json:"state"`
	Incarnation int32             `json:"incarnation"`
	Addresses   []mdsFreshAddress `json:"addresses"`
	Endpoint    string            `json:"endpoint,omitempty"`
	Nonce       uint32            `json:"nonce,omitempty"`
}

func (d *mdsCephFSRanksDaemon) same(o *mdsCephFSRanksDaemon) bool {
	return d != nil && o != nil && d.Name == o.Name && d.GID == o.GID && d.Rank == o.Rank && d.Incarnation == o.Incarnation && d.Endpoint == o.Endpoint && d.Nonce == o.Nonce
}

// mdsCephFSRanksTopology is one native `fs dump` of the owned filesystem.
// Ranks is indexed by rank and holds nil for a rank that is not up.
type mdsCephFSRanksTopology struct {
	FSMapEpoch    uint32                  `json:"fsmap_epoch"`
	MDSMapEpoch   uint32                  `json:"mdsmap_epoch"`
	MaxMDS        int                     `json:"max_mds"`
	In            []int                   `json:"in"`
	Failed        []int                   `json:"failed"`
	Ranks         []*mdsCephFSRanksDaemon `json:"ranks"`
	Daemons       []mdsCephFSRanksDaemon  `json:"daemons"`
	Standbys      []mdsCephFSRanksDaemon  `json:"owned_standbys"`
	OtherStandbys int                     `json:"other_standbys"`
}

// parseMDSCephFSRanksTopology reads one native `fs dump`. Only the owned
// filesystem identity (FSCID, name, pools) must be unchanged; rank states are
// reported as observed, so transitional maps parse too.
func parseMDSCephFSRanksTopology(dump []byte, oracle mdsFreshOracle, family string, owned func(string) bool) (mdsCephFSRanksTopology, error) {
	var t mdsCephFSRanksTopology
	if len(dump) == 0 || len(dump) > mdsFreshJSONLimit || !json.Valid(dump) {
		return t, errors.New("MDS CephFS ranks FSMap exceeds its bound or is invalid")
	}
	type daemon struct {
		mdsFreshDaemon
		Addrs struct {
			Vector []mdsFreshAddress `json:"addrvec"`
		} `json:"addrs"`
		JoinFSCID int64 `json:"join_fscid"`
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
				In           []int             `json:"in"`
				Up           map[string]uint64 `json:"up"`
				Failed       []int             `json:"failed"`
				Damaged      []int             `json:"damaged"`
				Info         map[string]daemon `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
		Standbys []daemon `json:"standbys"`
	}
	if err := json.Unmarshal(dump, &fsmap); err != nil || fsmap.Epoch == 0 || len(fsmap.Filesystems) != 1 || len(fsmap.Standbys) > 64 {
		return t, errors.New("MDS CephFS ranks FSMap shape differs")
	}
	fs := fsmap.Filesystems[0]
	m := fs.Map
	if fs.ID != oracle.FSCID || m.Name != oracle.FSName || m.MetadataPool != oracle.MetadataPool || !reflect.DeepEqual(m.DataPools, oracle.DataPools) || len(m.Info) > 8 || len(m.Up) > 8 || len(m.Damaged) != 0 {
		return t, errors.New("MDS CephFS ranks FSMap changed the owned filesystem identity")
	}
	describe := func(d daemon) mdsCephFSRanksDaemon {
		endpoint, nonce := mdsCephFSSelectAddress(d.Addrs.Vector, family)
		return mdsCephFSRanksDaemon{Name: d.Name, GID: d.GID, Rank: d.Rank, State: d.State, Incarnation: d.Incarnation, Addresses: d.Addrs.Vector, Endpoint: endpoint, Nonce: nonce}
	}
	t.FSMapEpoch, t.MDSMapEpoch, t.MaxMDS, t.In, t.Failed = fsmap.Epoch, m.Epoch, m.MaxMDS, m.In, m.Failed
	if t.In == nil {
		t.In = []int{}
	}
	if t.Failed == nil {
		t.Failed = []int{}
	}
	t.Daemons, t.Standbys = []mdsCephFSRanksDaemon{}, []mdsCephFSRanksDaemon{}
	for _, d := range m.Info {
		t.Daemons = append(t.Daemons, describe(d))
	}
	slices.SortFunc(t.Daemons, func(a, b mdsCephFSRanksDaemon) int { return int(a.Rank) - int(b.Rank) })
	if m.MaxMDS < 0 || m.MaxMDS > 8 {
		return t, errors.New("MDS CephFS ranks max_mds exceeds its bound")
	}
	t.Ranks = make([]*mdsCephFSRanksDaemon, m.MaxMDS)
	for rank := range t.Ranks {
		gid, ok := m.Up["mds_"+strconv.Itoa(rank)]
		if !ok {
			continue
		}
		d, present := m.Info["gid_"+strconv.FormatUint(gid, 10)]
		if !present || d.GID != gid || int(d.Rank) != rank {
			return t, errors.New("MDS CephFS ranks FSMap rank entry differs")
		}
		described := describe(d)
		t.Ranks[rank] = &described
	}
	for _, d := range fsmap.Standbys {
		if owned(d.Name) && d.JoinFSCID == fs.ID {
			t.Standbys = append(t.Standbys, describe(d))
		} else {
			t.OtherStandbys++
		}
	}
	slices.SortFunc(t.Standbys, func(a, b mdsCephFSRanksDaemon) int { return strings.Compare(a.Name, b.Name) })
	return t, nil
}

// stable requires the steady topology: ranks 0 and 1 up:active on two owned
// daemons with an address of the selected family, nothing failed, and exactly
// one owned standby.
func (t mdsCephFSRanksTopology) stable(owned func(string) bool) error {
	if t.MaxMDS != 2 || !reflect.DeepEqual(t.In, []int{0, 1}) || len(t.Failed) != 0 || len(t.Daemons) != 2 || len(t.Ranks) != 2 || len(t.Standbys) != 1 || t.OtherStandbys != 0 {
		return errors.New("MDS CephFS ranks topology is not two active ranks and one standby")
	}
	names := map[string]bool{}
	for rank, d := range append(slices.Clone(t.Ranks), &t.Standbys[0]) {
		state := "up:active"
		if rank == 2 {
			state = "up:standby"
		}
		if d == nil || !owned(d.Name) || names[d.Name] || d.GID == 0 || d.State != state || d.Endpoint == "" || d.Nonce == 0 || rank < 2 && int(d.Rank) != rank {
			return errors.New("MDS CephFS ranks topology has an unowned, inactive or unreachable daemon")
		}
		names[d.Name] = true
	}
	return nil
}

// sameAs compares the daemon identities of two stable topologies.
func (t mdsCephFSRanksTopology) sameAs(o mdsCephFSRanksTopology) bool {
	if len(t.Ranks) != 2 || len(o.Ranks) != 2 || len(t.Standbys) != 1 || len(o.Standbys) != 1 {
		return false
	}
	return t.Ranks[0].same(o.Ranks[0]) && t.Ranks[1].same(o.Ranks[1]) && t.Standbys[0].Name == o.Standbys[0].Name && t.Standbys[0].GID == o.Standbys[0].GID &&
		t.Standbys[0].Endpoint == o.Standbys[0].Endpoint && t.Standbys[0].Nonce == o.Standbys[0].Nonce
}

// mdsCephFSRanksSubtree is one `get subtrees` entry of the owned namespace
// (or the root), reduced to its authority and pin fields.
type mdsCephFSRanksSubtree struct {
	Path            string `json:"path"`
	DirFrag         string `json:"dirfrag"`
	IsAuth          bool   `json:"is_auth"`
	AuthFirst       int    `json:"auth_first"`
	AuthSecond      int    `json:"auth_second"`
	ExportPin       int    `json:"export_pin"`
	ExportPinTarget int    `json:"export_pin_target"`
}

// parseMDSCephFSRanksSubtrees keeps the entries of the owned directory tree
// and the root; count is the number of subtrees the daemon reported.
func parseMDSCephFSRanksSubtrees(data []byte, prefix string) ([]mdsCephFSRanksSubtree, int, error) {
	if len(data) == 0 || len(data) > mdsSessionsJSONLimit || !json.Valid(data) {
		return nil, 0, errors.New("MDS subtrees exceed their bound or are invalid")
	}
	var native []struct {
		IsAuth          *bool `json:"is_auth"`
		AuthFirst       *int  `json:"auth_first"`
		AuthSecond      *int  `json:"auth_second"`
		ExportPin       *int  `json:"export_pin"`
		ExportPinTarget *int  `json:"export_pin_target"`
		Dir             struct {
			Path    *string `json:"path"`
			DirFrag string  `json:"dirfrag"`
		} `json:"dir"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native == nil || len(native) > 4096 {
		return nil, 0, errors.New("MDS subtrees shape differs")
	}
	result := []mdsCephFSRanksSubtree{}
	for _, n := range native {
		if n.IsAuth == nil || n.AuthFirst == nil || n.AuthSecond == nil || n.ExportPin == nil || n.ExportPinTarget == nil || n.Dir.Path == nil || n.Dir.DirFrag == "" {
			return nil, 0, errors.New("MDS subtree entry lacks its authority fields")
		}
		path := *n.Dir.Path
		if path != "" && path != prefix && !strings.HasPrefix(path, prefix+"/") {
			continue
		}
		result = append(result, mdsCephFSRanksSubtree{Path: path, DirFrag: n.Dir.DirFrag, IsAuth: *n.IsAuth, AuthFirst: *n.AuthFirst, AuthSecond: *n.AuthSecond,
			ExportPin: *n.ExportPin, ExportPinTarget: *n.ExportPinTarget})
	}
	slices.SortFunc(result, func(a, b mdsCephFSRanksSubtree) int { return strings.Compare(a.Path, b.Path) })
	return result, len(native), nil
}

// mdsCephFSRanksCounters are the daemon's request forward and subtree
// migration counters (MDSRank perf counters "mds").
type mdsCephFSRanksCounters struct {
	Forward        uint64 `json:"forward"`
	Exported       uint64 `json:"exported"`
	ExportedInodes uint64 `json:"exported_inodes"`
	Imported       uint64 `json:"imported"`
	ImportedInodes uint64 `json:"imported_inodes"`
}

func parseMDSCephFSRanksCounters(data []byte) (mdsCephFSRanksCounters, error) {
	var native struct {
		MDS map[string]json.RawMessage `json:"mds"`
	}
	var result mdsCephFSRanksCounters
	if len(data) == 0 || len(data) > mdsSessionsJSONLimit || json.Unmarshal(data, &native) != nil || native.MDS == nil {
		return result, errors.New("MDS perf counters differ")
	}
	for name, target := range map[string]*uint64{"forward": &result.Forward, "exported": &result.Exported, "exported_inodes": &result.ExportedInodes,
		"imported": &result.Imported, "imported_inodes": &result.ImportedInodes} {
		raw, ok := native.MDS[name]
		if !ok || json.Unmarshal(raw, target) != nil {
			return mdsCephFSRanksCounters{}, fmt.Errorf("MDS perf counter %s missing", name)
		}
	}
	return result, nil
}

// parseMDSCephFSRanksDaemonStatus reads one daemon's own `status`: its rank,
// GID, state and applied MDSMap epoch.
func parseMDSCephFSRanksDaemonStatus(data []byte) (int32, uint64, string, uint32, error) {
	var status struct {
		Whoami *int32  `json:"whoami"`
		ID     *uint64 `json:"id"`
		State  string  `json:"state"`
		Epoch  *uint32 `json:"mdsmap_epoch"`
	}
	if len(data) == 0 || len(data) > 64<<10 || json.Unmarshal(data, &status) != nil || status.Whoami == nil || status.ID == nil || status.Epoch == nil || *status.Epoch == 0 || status.State == "" {
		return 0, 0, "", 0, errors.New("MDS daemon status differs")
	}
	return *status.Whoami, *status.ID, status.State, *status.Epoch, nil
}

// mdsCephFSRanksView is one rank's native view: its sessions, the owned
// subtrees it reports and its counters, observed from the proven daemon.
type mdsCephFSRanksView struct {
	Rank        int32  `json:"rank"`
	Name        string `json:"name"`
	GID         uint64 `json:"gid"`
	DaemonEpoch uint32 `json:"daemon_mdsmap_epoch"`
	// Sessions is null when the observation did not list sessions.
	Sessions     []mdsSessionsNative     `json:"sessions"`
	Subtrees     []mdsCephFSRanksSubtree `json:"subtrees"`
	SubtreeCount int                     `json:"subtree_count"`
	Counters     mdsCephFSRanksCounters  `json:"counters"`
}

func (v mdsCephFSRanksView) subtree(path string) *mdsCephFSRanksSubtree {
	for i := range v.Subtrees {
		if v.Subtrees[i].Path == path {
			return &v.Subtrees[i]
		}
	}
	return nil
}

// mdsCephFSRanksPlaced reports converged authority of one owned directory on
// rank: that rank reports it as its own subtree with a single authority and
// no other rank claims it or reports another authority.
func mdsCephFSRanksPlaced(views []mdsCephFSRanksView, path string, rank int32) bool {
	if len(views) != 2 {
		return false
	}
	for _, v := range views {
		e := v.subtree(path)
		if v.Rank == rank {
			if e == nil || !e.IsAuth || e.AuthFirst != int(rank) || e.AuthSecond != -2 {
				return false
			}
		} else if e != nil && (e.IsAuth || e.AuthFirst != int(rank) || e.AuthSecond != -2) {
			return false
		}
	}
	return true
}

// mdsCephFSRanksPin records one owned pin change of one directory.
type mdsCephFSRanksPin struct {
	Kind           string `json:"kind"` // pin or pin-restore
	RequestID      int64  `json:"request_id"`
	Target         string `json:"target"`
	Path           string `json:"path"`
	Inode          uint64 `json:"inode"`
	Previous       string `json:"previous"`
	Value          string `json:"value"`
	State          string `json:"state"` // intended, applied, unchanged, uncertain
	SetterGlobalID uint64 `json:"setter_global_id,omitempty"`
	Readback       string `json:"readback,omitempty"`
	ReaderGlobalID uint64 `json:"reader_global_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

// mdsCephFSRanksPlacement is one bounded wait for converged authority.
type mdsCephFSRanksPlacement struct {
	Target       string `json:"target"`
	Path         string `json:"path"`
	Rank         int32  `json:"rank"`
	Converged    bool   `json:"converged"`
	Polls        int    `json:"polls"`
	WaitedMillis int64  `json:"waited_ms"`
}

// mdsCephFSRanksTakeover records the one owned standby takeover of rank 0.
type mdsCephFSRanksTakeover struct {
	State          string                  `json:"state"` // armed, intended, applied, uncertain, adopted
	ArmRequestID   int64                   `json:"arm_request_id"`
	FireRequestID  int64                   `json:"fire_request_id,omitempty"`
	AdoptRequestID int64                   `json:"adopt_request_id,omitempty"`
	Armed          mdsCephFSRanksDaemon    `json:"armed"`
	Standby        mdsCephFSRanksDaemon    `json:"standby"`
	Rank1          mdsCephFSRanksDaemon    `json:"rank1"`
	ArmedAddresses []string                `json:"armed_blocklist_addresses"`
	Output         string                  `json:"native_output,omitempty"`
	Error          string                  `json:"error,omitempty"`
	FireStartedUTC string                  `json:"fire_started_utc,omitempty"`
	FiredUTC       string                  `json:"fired_utc,omitempty"`
	Adopted        *mdsCephFSRanksTopology `json:"adopted,omitempty"`
	AdoptedUTC     string                  `json:"adopted_utc,omitempty"`
	AdoptPolls     int                     `json:"adopt_polls,omitempty"`
	OldFencing     []mdsSessionsBlock      `json:"old_incarnation_blocklist,omitempty"`
}

func (t *mdsCephFSRanksTakeover) pending() bool {
	return t != nil && t.State != "armed" && t.State != "adopted"
}

// mdsCephFSRanksStatus is a transitional read during the takeover.
type mdsCephFSRanksStatus struct {
	Topology       mdsCephFSRanksTopology `json:"topology"`
	ArmedPresent   bool                   `json:"armed_gid_present"`
	ArmedBlocklist []mdsSessionsBlock     `json:"armed_blocklist"`
}

type mdsCephFSRanksNative struct {
	Action         string         `json:"action"`
	Path           string         `json:"path"`
	NativeGlobalID uint64         `json:"native_global_id"`
	Stat           *mdsCephFSStat `json:"stat,omitempty"`
	Pin            *string        `json:"pin,omitempty"`
	PinSet         *string        `json:"pin_set,omitempty"`
}

// One fixed native LibCephFS driver: stat, read the local export pin, set
// it. The fixture passes only plan paths and admitted values; the driver
// re-checks that the path is inside the owned directory. It always shuts
// down, which closes its CephFS sessions. Source: pybind/cephfs/cephfs.pyx at
// mdsFreshSource; independent driver, no code copied.
const mdsCephFSRanksNativePython = `
import cephfs, json, sys
identity, keyring, fsname, directory, action, path = sys.argv[1:7]
extra = sys.argv[7:]
prefix = '/' + directory
parts = path.split('/')
if not directory.startswith('ceph-msgr-ranks-') or '/' in directory or not (path == prefix or (path.startswith(prefix + '/') and len(parts) <= 4 and '..' not in parts and '' not in parts[1:])):
    raise ValueError('path outside the owned directory')
MASK = 0x30f
fs = cephfs.LibCephFS(conffile='/tmp/ceph-msgr-test/ceph.conf', auth_id=identity.removeprefix('client.'), conf={'keyring': keyring})
def stat(p):
    try:
        s = fs.statx(p, MASK, 0)
    except cephfs.ObjectNotFound as e:
        return {'exists': False, 'errno': abs(e.errno), 'ino': 0, 'mode': 0, 'uid': 0, 'gid': 0, 'nlink': 0, 'size': 0}
    return {'exists': True, 'errno': 0, 'ino': s['ino'], 'mode': s['mode'], 'uid': s['uid'], 'gid': s['gid'], 'nlink': s['nlink'], 'size': s['size']}
out = {'action': action, 'path': path}
try:
    fs.mount(filesystem_name=fsname)
    out['native_global_id'] = fs.get_instance_id()
    if action == 'stat' and not extra:
        out['stat'] = stat(path)
    elif action == 'pin-get' and not extra:
        out['stat'] = stat(path)
        if out['stat']['exists']:
            out['pin'] = fs.getxattr(path, 'ceph.dir.pin').decode('ascii')
    elif action == 'pin-set' and len(extra) == 1 and extra[0] in ('-1', '0', '1'):
        fs.setxattr(path, 'ceph.dir.pin', extra[0].encode('ascii'), 0)
        out['pin_set'] = extra[0]
    else:
        raise ValueError('native CephFS ranks action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSRanksNative accepts exactly the shape of one admitted action.
func decodeMDSCephFSRanksNative(action, path string, extra []string, data []byte) (mdsCephFSRanksNative, error) {
	var result mdsCephFSRanksNative
	if len(data) == 0 || len(data) > mdsCephFSRanksJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS ranks output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSRanksNative{}, errors.New("native CephFS ranks output identity differs")
	}
	ok := false
	switch action {
	case "stat":
		ok = len(extra) == 0 && result.Stat != nil && validMDSCephFSStat(*result.Stat) && result.Pin == nil && result.PinSet == nil
	case "pin-get":
		ok = len(extra) == 0 && result.Stat != nil && validMDSCephFSStat(*result.Stat) && result.PinSet == nil
		if ok && result.Stat.Exists {
			_, err := strconv.Atoi(derefString(result.Pin))
			ok = result.Pin != nil && err == nil && result.Stat.Mode&0o170000 == 0o040000
		} else if ok {
			ok = result.Pin == nil
		}
	case "pin-set":
		ok = len(extra) == 1 && result.PinSet != nil && *result.PinSet == extra[0] && result.Stat == nil && result.Pin == nil
	}
	if !ok {
		return mdsCephFSRanksNative{}, fmt.Errorf("native CephFS ranks %s output shape differs", action)
	}
	return result, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type mdsCephFSRanksScope struct {
	out                    string
	plan                   mdsCephFSRanksPlan
	goClient, native       *ceph.ClientConfig
	goIntent, nativeIntent bool
	baselineReady          bool
	baselineBlocklist      []mdsSessionsBlock
	lastRequestID          int64
	// startup is the topology proven at preparation; current is the one every
	// action proves (startup until the owned takeover is adopted).
	startup, current mdsCephFSRanksTopology
	pins             []*mdsCephFSRanksPin
	// pinBaseline is the value read before the first owned change of each
	// target; pinApplied the last value applied and read back.
	pinBaseline, pinApplied map[string]string
	pinInode                map[string]uint64
	takeover                *mdsCephFSRanksTakeover
	// owned maps session addresses of the owned Go identity, observed through
	// observe, to their GlobalID.
	owned map[string]uint64
}

type mdsCephFSRanksAction struct {
	name, target, value string
	gid                 uint64
}

// The request is <id> <ownership token> <action> [arguments]. Only fixed
// actions, plan targets and pin values exist.
func parseMDSCephFSRanksRequest(fields []string, token string, plan mdsCephFSRanksPlan, previousID int64) (int64, mdsCephFSRanksAction, error) {
	var action mdsCephFSRanksAction
	if len(fields) < 3 || len(fields) > 5 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned MDS CephFS ranks request scope")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid MDS CephFS ranks request identity")
	}
	action.name = fields[2]
	args := fields[3:]
	switch action.name {
	case "observe":
		if len(args) != 1 {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks observation requires one GlobalID")
		}
		gid, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil || gid == 0 || strconv.FormatUint(gid, 10) != args[0] {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks observation requires one canonical GlobalID")
		}
		action.gid = gid
	case "stat":
		if len(args) != 1 || !slices.Contains(plan.targets(), args[0]) {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks stat target is not admitted")
		}
		action.target = args[0]
	case "pin", "placement":
		values := []string{"-1", "0", "1"}
		if action.name == "placement" {
			values = []string{"0", "1"}
		}
		if len(args) != 2 || !plan.pinTarget(args[0]) || !slices.Contains(values, args[1]) {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks pin target or rank is not admitted")
		}
		action.target, action.value = args[0], args[1]
	case "pin-restore":
		if len(args) != 1 || !plan.pinTarget(args[0]) {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks pin restoration target is not admitted")
		}
		action.target = args[0]
	case "takeover-arm", "takeover-fire", "takeover-status", "takeover-adopt":
		if len(args) != 0 {
			return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks takeover actions take no caller argument")
		}
	default:
		return 0, mdsCephFSRanksAction{}, errors.New("MDS CephFS ranks action is not admitted")
	}
	return id, action, nil
}

type mdsCephFSRanksResult struct {
	Source          string                      `json:"source_commit"`
	Token           string                      `json:"token"`
	RequestID       int64                       `json:"request_id"`
	Action          string                      `json:"action"`
	Target          string                      `json:"target,omitempty"`
	Value           string                      `json:"value,omitempty"`
	GlobalID        uint64                      `json:"global_id,omitempty"`
	FSID            string                      `json:"fsid"`
	FSCID           int64                       `json:"fscid"`
	FSName          string                      `json:"fs_name"`
	Topology        *mdsCephFSRanksTopology     `json:"topology,omitempty"`
	IdentityChecked bool                        `json:"identity_checked"`
	Views           []mdsCephFSRanksView        `json:"views,omitempty"`
	Blocklist       []mdsSessionsBlock          `json:"blocklist,omitempty"`
	Native          *mdsCephFSRanksNative       `json:"native,omitempty"`
	Processes       []*ceph.NativeCommandResult `json:"native_processes,omitempty"`
	Pin             *mdsCephFSRanksPin          `json:"pin,omitempty"`
	Placement       *mdsCephFSRanksPlacement    `json:"placement,omitempty"`
	Takeover        *mdsCephFSRanksTakeover     `json:"takeover,omitempty"`
	Status          *mdsCephFSRanksStatus       `json:"status,omitempty"`
	Complete        bool                        `json:"complete"`
	Error           string                      `json:"error,omitempty"`
	ElapsedMillis   int64                       `json:"elapsed_ms"`
	ObservedUTC     string                      `json:"observed_utc"`
}

func (c *fixtureCluster) mdsCephFSRanksJournal() error {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	identity, native := "", ""
	if x.goIntent {
		identity = "client.mds-cephfs-ranks-" + s.token
	}
	if x.nativeIntent {
		native = "client.mds-cephfs-ranks-native-" + s.token
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-ranks-owned.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name, "fscid": s.oracle.FSCID,
		"client_identity": identity, "native_identity": native, "plan": x.plan, "baseline_blocklist": x.baselineBlocklist,
		"startup_topology": x.startup, "current_topology": x.current, "pins": x.pins, "pin_baseline": x.pinBaseline, "takeover": x.takeover,
		"owned_session_addresses": x.owned,
	})
}

func (c *fixtureCluster) mdsCephFSRankedOwned() func(string) bool {
	owned := map[string]bool{}
	for _, daemon := range c.mdsFresh.fs.MDSs() {
		if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" {
			owned[daemon.ID] = true
		}
	}
	return func(name string) bool { return owned[name] }
}

func (c *fixtureCluster) mdsCephFSRanksTopology(ctx context.Context) (mdsCephFSRanksTopology, error) {
	data, err := c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return mdsCephFSRanksTopology{}, err
	}
	return parseMDSCephFSRanksTopology(data, c.mdsFresh.oracle, c.profile.ipFamily, c.mdsCephFSRankedOwned())
}

// mdsCephFSRanksTell addresses one proven daemon by name.
func (c *fixtureCluster) mdsCephFSRanksTell(ctx context.Context, d *mdsCephFSRanksDaemon, args ...string) ([]byte, error) {
	return c.cluster.Ceph(ctx, append([]string{"tell", "mds." + d.Name}, args...)...)
}

// mdsCephFSRanksProve reads the native map and requires the stable topology
// with the current owned daemons. With daemons, it also requires each rank's
// own status to report that rank, GID, up:active and at least the map epoch.
func (c *fixtureCluster) mdsCephFSRanksProve(ctx context.Context, daemons bool) (mdsCephFSRanksTopology, error) {
	x := c.mdsCephFSRanks
	t, err := c.mdsCephFSRanksTopology(ctx)
	if err != nil {
		return t, err
	}
	if err := t.stable(c.mdsCephFSRankedOwned()); err != nil {
		return t, err
	}
	if !t.sameAs(x.current) {
		return t, errors.New("MDS CephFS ranks daemons changed during the owned action")
	}
	if daemons {
		for rank, d := range t.Ranks {
			data, err := c.mdsCephFSRanksTell(ctx, d, "status", "--format", "json")
			if err != nil {
				return t, err
			}
			whoami, gid, state, epoch, err := parseMDSCephFSRanksDaemonStatus(data)
			if err != nil {
				return t, err
			}
			if int(whoami) != rank || gid != d.GID || state != "up:active" || epoch < t.MDSMapEpoch {
				return t, errors.New("MDS CephFS ranks daemon status differs from the proven rank")
			}
		}
	}
	return t, nil
}

// mdsCephFSRanksViews observes both ranks of the proven topology.
func (c *fixtureCluster) mdsCephFSRanksViews(ctx context.Context, t mdsCephFSRanksTopology, sessions bool) ([]mdsCephFSRanksView, error) {
	x := c.mdsCephFSRanks
	var views []mdsCephFSRanksView
	for rank, d := range t.Ranks {
		v := mdsCephFSRanksView{Rank: int32(rank), Name: d.Name, GID: d.GID}
		data, err := c.mdsCephFSRanksTell(ctx, d, "get", "subtrees")
		if err != nil {
			return nil, err
		}
		if v.Subtrees, v.SubtreeCount, err = parseMDSCephFSRanksSubtrees(data, "/"+x.plan.Directory); err != nil {
			return nil, err
		}
		if data, err = c.mdsCephFSRanksTell(ctx, d, "perf", "dump", "mds"); err != nil {
			return nil, err
		}
		if v.Counters, err = parseMDSCephFSRanksCounters(data); err != nil {
			return nil, err
		}
		if sessions {
			if data, err = c.mdsCephFSRanksTell(ctx, d, "session", "ls", "--format", "json"); err != nil {
				return nil, err
			}
			if v.Sessions, err = parseMDSSessionsList(data); err != nil {
				return nil, err
			}
		}
		if data, err = c.mdsCephFSRanksTell(ctx, d, "status", "--format", "json"); err != nil {
			return nil, err
		}
		whoami, gid, state, epoch, err := parseMDSCephFSRanksDaemonStatus(data)
		if err != nil || int(whoami) != rank || gid != d.GID || state != "up:active" {
			return nil, errors.Join(err, errors.New("MDS CephFS ranks observed daemon differs from the proven rank"))
		}
		v.DaemonEpoch = epoch
		views = append(views, v)
	}
	return views, nil
}

func (c *fixtureCluster) prepareMDSCephFSRanks(ctx context.Context, out string) error {
	p := c.profile
	if p.mdsCephFSRanks != "1" || p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.osdProfile != "legacy-v6" || c.mdsFresh == nil || c.mdsFresh.fs == nil || c.mdsCephFSRanks != nil {
		return errors.New("invalid MDS CephFS ranks preparation scope")
	}
	s := c.mdsFresh
	x := &mdsCephFSRanksScope{out: out, plan: mdsCephFSRanksPlanFor(s.token), pinBaseline: map[string]string{}, pinApplied: map[string]string{}, pinInode: map[string]uint64{}, owned: map[string]uint64{}}
	c.mdsCephFSRanks = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	t, err := c.mdsCephFSRanksTopology(ctx)
	if err != nil {
		return err
	}
	if err := t.stable(c.mdsCephFSRankedOwned()); err != nil {
		return err
	}
	if r0 := t.Ranks[0]; r0.Name != s.oracle.MDS.Name || r0.GID != s.oracle.MDS.GID || r0.Endpoint != s.oracle.MDS.Endpoint || r0.Nonce != s.oracle.MDS.Nonce || r0.Incarnation != s.oracle.MDS.Incarnation {
		return errors.New("MDS CephFS ranks rank 0 differs from the fresh oracle")
	}
	x.startup, x.current = t, t
	if x.baselineBlocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	views, err := c.mdsCephFSRanksViews(ctx, t, true)
	if err != nil {
		return err
	}
	for _, v := range views {
		if len(v.Sessions) != 0 {
			return errors.New("MDS CephFS ranks acceptance requires an owned filesystem without client sessions")
		}
	}
	x.baselineReady = true
	x.goIntent = true
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return err
	}
	if x.goClient, err = c.cluster.CreateClient(ctx, "mds-cephfs-ranks-"+s.token, mdsCephFSRanksGoCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated MDS CephFS ranks identity: %w", err)
	}
	_, keyring, err := x.goClient.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-ranks.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	x.nativeIntent = true
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return err
	}
	if x.native, err = c.cluster.CreateClient(ctx, "mds-cephfs-ranks-native-"+s.token, mdsCephFSRanksNativeCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated native CephFS ranks oracle identity: %w", err)
	}
	_, nativeKeyring, err := x.native.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(nativeKeyring); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-ranks-native.keyring"), nativeKeyring, 0600); err != nil {
		return err
	}
	// Every owned daemon is a relay peer: rank 1 now, the standby after the
	// takeover. The relay admits an MDS by its container IP.
	var peers map[string]string
	if json.Unmarshal([]byte(c.profile.peers), &peers) != nil || peers == nil {
		return errors.New("MDS CephFS ranks relay peer inventory missing")
	}
	for _, d := range append(slices.Clone(t.Ranks), &t.Standbys[0]) {
		peers["mds."+d.Name] = d.Endpoint
	}
	encoded, _ := json.Marshal(peers)
	c.profile.peers, c.profile.mdsCephFSRanksIdentity = string(encoded), "client.mds-cephfs-ranks-"+s.token
	if err := os.WriteFile(filepath.Join(out, "peers.json"), encoded, 0600); err != nil {
		return err
	}
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return err
	}
	goCaps, nativeCaps := mdsCephFSRanksGoCaps(s.name), mdsCephFSRanksNativeCaps(s.name)
	return writeMDSFreshJSON(out, "mds-cephfs-ranks-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name,
		"client_identity": c.profile.mdsCephFSRanksIdentity, "native_identity": "client.mds-cephfs-ranks-native-" + s.token,
		"client_caps": map[string]string{"mon": goCaps.Mon, "mds": goCaps.MDS, "osd": goCaps.OSD},
		"native_caps": map[string]string{"mon": nativeCaps.Mon, "mds": nativeCaps.MDS, "osd": nativeCaps.OSD},
		"plan":        x.plan, "baseline_blocklist": x.baselineBlocklist, "topology": t, "views": views,
	})
}

// mdsCephFSRanksRunNative runs the fixed native driver once. A failed, killed
// or truncated process is an error: its output is never trusted partially.
func (c *fixtureCluster) mdsCephFSRanksRunNative(ctx context.Context, action, path string, extra ...string) (*ceph.NativeCommandResult, mdsCephFSRanksNative, error) {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	argv := append([]string{"python3", "-c", mdsCephFSRanksNativePython, "client.mds-cephfs-ranks-native-" + s.token, "/out/mds-cephfs-ranks-native.keyring", s.name, x.plan.Directory, action, path}, extra...)
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 30 * time.Second, MaxStdout: mdsCephFSRanksJSONLimit, MaxStderr: 16 << 10, Label: "owned native CephFS ranks " + action})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, mdsCephFSRanksNative{}, errors.Join(err, fmt.Errorf("native CephFS ranks %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeMDSCephFSRanksNative(action, path, extra, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

func (c *fixtureCluster) mdsCephFSRanksControl(ctx context.Context, out string, fields []string) (resultErr error) {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	if c.profile.mdsCephFSRanks != "1" || c.profile.mdsFresh != "1" || s == nil || s.fs == nil || x == nil || s.out != out || x.out != out {
		return errors.New("invalid owned MDS CephFS ranks control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 110*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS CephFS ranks control requested after cleanup began")
	}
	id, action, err := parseMDSCephFSRanksRequest(fields, s.token, x.plan, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := mdsCephFSRanksResult{Source: mdsFreshSource, Token: s.token, RequestID: id, Action: action.name, Target: action.target, Value: action.value, GlobalID: action.gid,
		FSID: s.oracle.FSID, FSCID: s.oracle.FSCID, FSName: s.name}
	defer func() {
		result.ElapsedMillis = time.Since(started).Milliseconds()
		result.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if resultErr != nil {
			result.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-cephfs-ranks-result.json", result))
	}()
	switch action.name {
	case "takeover-fire", "takeover-status":
		// Fire is time-critical and scoped by the armed GID; status reads a
		// transitional map. Neither can prove the stable topology.
		if err := c.mdsCephFSRanksTakeoverAct(ctx, id, action, &result); err != nil {
			return err
		}
		result.Complete = true
		return nil
	case "takeover-adopt":
		// Adoption proves the new topology itself, then the ordinary proof
		// runs against it.
		if err := c.mdsCephFSRanksTakeoverAct(ctx, id, action, &result); err != nil {
			return err
		}
	default:
		if x.takeover.pending() {
			return errors.New("MDS CephFS ranks action refused while the owned takeover is not adopted")
		}
		if _, err := c.mdsCephFSRanksProve(ctx, false); err != nil {
			return err
		}
		if err := c.mdsCephFSRanksAct(ctx, id, action, &result); err != nil {
			return err
		}
	}
	t, err := c.mdsCephFSRanksProve(ctx, true)
	if err != nil {
		return err
	}
	result.Topology, result.IdentityChecked, result.Complete = &t, true, true
	return nil
}

func (c *fixtureCluster) mdsCephFSRanksAct(ctx context.Context, id int64, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	switch action.name {
	case "observe":
		views, err := c.mdsCephFSRanksViews(ctx, x.current, true)
		if err != nil {
			return err
		}
		result.Views = views
		changed := false
		for _, v := range views {
			for _, session := range v.Sessions {
				if session.ID == action.gid && session.AuthID == "mds-cephfs-ranks-"+s.token && x.owned[session.Address] == 0 {
					// The test named this owned identity's GlobalID: its address is
					// owned evidence for cleanup of a later blocklist entry.
					x.owned[session.Address], changed = session.ID, true
				}
			}
		}
		if changed {
			if err := c.mdsCephFSRanksJournal(); err != nil {
				return err
			}
		}
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	case "stat":
		path, err := x.plan.path(action.target)
		if err != nil {
			return err
		}
		process, native, err := c.mdsCephFSRanksRunNative(ctx, "stat", path)
		result.Processes = append(result.Processes, process)
		if err != nil {
			return err
		}
		result.Native = &native
		return nil
	case "pin", "pin-restore":
		return c.mdsCephFSRanksPin(ctx, id, action, result)
	case "placement":
		return c.mdsCephFSRanksPlacement(ctx, action, result)
	case "takeover-arm":
		return c.mdsCephFSRanksTakeoverAct(ctx, id, action, result)
	}
	return errors.New("MDS CephFS ranks action is not admitted")
}

// mdsCephFSRanksPin changes one owned directory's export pin, or restores the
// value read before its first owned change. The prior value is read with a
// fresh mount, the change is made by another mount that then shuts down, and
// a third fresh mount reads it back (#48: the setting mount's own readback
// is not evidence). The change is one-shot and never reissued.
func (c *fixtureCluster) mdsCephFSRanksPin(ctx context.Context, id int64, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	x := c.mdsCephFSRanks
	path, err := x.plan.path(action.target)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(x.pins, func(p *mdsCephFSRanksPin) bool {
		return p.Target == action.target && (p.State == "intended" || p.State == "uncertain")
	}) {
		return errors.New("MDS CephFS ranks pin of this directory has an uncertain outcome; refusing another change")
	}
	process, prior, err := c.mdsCephFSRanksRunNative(ctx, "pin-get", path)
	result.Processes = append(result.Processes, process)
	if err != nil {
		return err
	}
	if !prior.Stat.Exists {
		return errors.New("MDS CephFS ranks pin target does not exist")
	}
	previous := *prior.Pin
	if inode, ok := x.pinInode[action.target]; ok && inode != prior.Stat.Ino {
		return errors.New("MDS CephFS ranks pin target inode was replaced; refusing mutation")
	}
	if applied, ok := x.pinApplied[action.target]; ok && applied != previous {
		return errors.New("MDS CephFS ranks pin changed outside the fixture; refusing mutation")
	}
	value := action.value
	if action.name == "pin-restore" {
		baseline, ok := x.pinBaseline[action.target]
		if !ok {
			return errors.New("MDS CephFS ranks pin restoration without an owned change")
		}
		value = baseline
	} else if _, ok := x.pinBaseline[action.target]; !ok {
		x.pinBaseline[action.target], x.pinInode[action.target] = previous, prior.Stat.Ino
	}
	pin := &mdsCephFSRanksPin{Kind: action.name, RequestID: id, Target: action.target, Path: path, Inode: prior.Stat.Ino, Previous: previous, Value: value, State: "intended"}
	result.Pin = pin
	if previous == value {
		pin.State, pin.Readback, pin.ReaderGlobalID = "unchanged", previous, prior.NativeGlobalID
		x.pins = append(x.pins, pin)
		x.pinApplied[action.target] = value
		return c.mdsCephFSRanksJournal()
	}
	x.pins = append(x.pins, pin)
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return err
	}
	process, set, err := c.mdsCephFSRanksRunNative(ctx, "pin-set", path, value)
	result.Processes = append(result.Processes, process)
	if err != nil {
		pin.State, pin.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSRanksJournal())
	}
	pin.SetterGlobalID = set.NativeGlobalID
	process, readback, err := c.mdsCephFSRanksRunNative(ctx, "pin-get", path)
	result.Processes = append(result.Processes, process)
	if err == nil && (!readback.Stat.Exists || readback.Stat.Ino != pin.Inode || *readback.Pin != value || readback.NativeGlobalID == set.NativeGlobalID) {
		err = errors.New("MDS CephFS ranks pin readback differs from the requested policy")
	}
	if err != nil {
		pin.State, pin.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSRanksJournal())
	}
	pin.State, pin.Readback, pin.ReaderGlobalID = "applied", *readback.Pin, readback.NativeGlobalID
	x.pinApplied[action.target] = value
	result.Native = &readback
	return c.mdsCephFSRanksJournal()
}

// mdsCephFSRanksPlacement waits, bounded, until both ranks agree that the
// owned directory is a subtree with a single authority on the given rank. A
// mixed or ambiguous snapshot is observed again; the bound returns the last
// views with Converged false rather than an error.
func (c *fixtureCluster) mdsCephFSRanksPlacement(ctx context.Context, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	x := c.mdsCephFSRanks
	path, err := x.plan.path(action.target)
	if err != nil {
		return err
	}
	rank, _ := strconv.Atoi(action.value)
	placement := &mdsCephFSRanksPlacement{Target: action.target, Path: path, Rank: int32(rank)}
	result.Placement = placement
	started := time.Now()
	deadline := started.Add(60 * time.Second)
	for {
		placement.Polls++
		views, err := c.mdsCephFSRanksViews(ctx, x.current, false)
		if err != nil {
			return err
		}
		result.Views = views
		if mdsCephFSRanksPlaced(views, path, int32(rank)) {
			placement.Converged = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	placement.WaitedMillis = time.Since(started).Milliseconds()
	// The sessions of the converged state, for per-rank capability counts.
	views, err := c.mdsCephFSRanksViews(ctx, x.current, true)
	if err != nil {
		return err
	}
	if placement.Converged && !mdsCephFSRanksPlaced(views, path, int32(rank)) {
		return errors.New("MDS CephFS ranks placement changed right after it converged")
	}
	result.Views = views
	return nil
}

func (c *fixtureCluster) mdsCephFSRanksStatus(ctx context.Context) (mdsCephFSRanksStatus, error) {
	x := c.mdsCephFSRanks
	t, err := c.mdsCephFSRanksTopology(ctx)
	if err != nil {
		return mdsCephFSRanksStatus{}, err
	}
	status := mdsCephFSRanksStatus{Topology: t, ArmedBlocklist: []mdsSessionsBlock{}}
	if x.takeover == nil {
		return status, nil
	}
	for _, d := range append(slices.Clone(t.Daemons), t.Standbys...) {
		status.ArmedPresent = status.ArmedPresent || d.GID == x.takeover.Armed.GID
	}
	blocks, err := c.mdsSessionsBlocklist(ctx)
	if err != nil {
		return status, err
	}
	for _, address := range x.takeover.ArmedAddresses {
		if entry := mdsSessionsFindBlock(blocks, address); entry != nil {
			status.ArmedBlocklist = append(status.ArmedBlocklist, *entry)
		}
	}
	return status, nil
}

// mdsCephFSRanksTakeoverAct runs one takeover step. arm and adopt run inside
// the ordinary proof; fire and status do not.
func (c *fixtureCluster) mdsCephFSRanksTakeoverAct(ctx context.Context, id int64, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	x := c.mdsCephFSRanks
	switch action.name {
	case "takeover-arm":
		if x.takeover != nil {
			return errors.New("MDS CephFS ranks takeover was already armed")
		}
		t := x.current
		r := &mdsCephFSRanksTakeover{State: "armed", ArmRequestID: id, Armed: *t.Ranks[0], Standby: t.Standbys[0], Rank1: *t.Ranks[1], ArmedAddresses: []string{}}
		for _, address := range r.Armed.Addresses {
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
				return errors.New("MDS CephFS ranks takeover target address is already blocklisted")
			}
		}
		x.takeover, result.Takeover = r, r
		return c.mdsCephFSRanksJournal()
	case "takeover-fire":
		r := x.takeover
		if r == nil || r.State != "armed" {
			return errors.New("MDS CephFS ranks takeover fire requires one armed, unfired takeover")
		}
		// Journal the intent first; the outcome is recorded once and the
		// command is never reissued, whatever it returns.
		r.State, r.FireRequestID, r.FireStartedUTC = "intended", id, time.Now().UTC().Format(time.RFC3339Nano)
		result.Takeover = r
		if err := c.mdsCephFSRanksJournal(); err != nil {
			return err
		}
		// The armed GID names exactly the proven rank-0 daemon incarnation.
		output, err := c.cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(r.Armed.GID, 10))
		r.FiredUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if len(output) != 0 {
			r.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
		}
		if err != nil {
			r.State, r.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSRanksJournal())
		}
		r.State = "applied"
		return c.mdsCephFSRanksJournal()
	case "takeover-status":
		r := x.takeover
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS ranks takeover status requires a fired, unadopted takeover")
		}
		status, err := c.mdsCephFSRanksStatus(ctx)
		if err != nil {
			return err
		}
		result.Takeover, result.Status = r, &status
		return nil
	case "takeover-adopt":
		r := x.takeover
		if r == nil || (r.State != "applied" && r.State != "uncertain") {
			return errors.New("MDS CephFS ranks takeover adoption requires a fired, unadopted takeover")
		}
		// Full native proof, waiting (bounded) for the failed daemon to return
		// as the owned standby: the former standby holds rank 0 up:active with
		// its own GID and a new incarnation, rank 1 is unchanged, the armed GID
		// is gone and the failed daemon registered again with a new GID.
		owned := c.mdsCephFSRankedOwned()
		deadline := time.Now().Add(60 * time.Second)
		var status mdsCephFSRanksStatus
		var proofErr error
		for {
			r.AdoptPolls++
			var err error
			if status, err = c.mdsCephFSRanksStatus(ctx); err != nil {
				return err
			}
			t := status.Topology
			proofErr = t.stable(owned)
			if proofErr == nil {
				n0, n1, sb := t.Ranks[0], t.Ranks[1], t.Standbys[0]
				switch {
				case n0.Name != r.Standby.Name || n0.GID != r.Standby.GID || n0.Incarnation == r.Armed.Incarnation:
					proofErr = errors.New("rank 0 is not held by the armed standby in a new incarnation")
				case !n1.same(&r.Rank1):
					proofErr = errors.New("rank 1 changed during the takeover")
				case status.ArmedPresent:
					proofErr = errors.New("the armed daemon incarnation is still mapped")
				case sb.Name != r.Armed.Name || sb.GID == r.Armed.GID:
					proofErr = errors.New("the failed daemon has not returned as the owned standby")
				}
			}
			if proofErr == nil || time.Now().After(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		result.Takeover, result.Status = r, &status
		if proofErr != nil {
			return fmt.Errorf("MDS CephFS ranks takeover adoption could not prove the new topology: %w", proofErr)
		}
		adopted := status.Topology
		r.OldFencing = status.ArmedBlocklist
		r.Adopted, r.AdoptRequestID, r.AdoptedUTC, r.State = &adopted, id, time.Now().UTC().Format(time.RFC3339Nano), "adopted"
		x.current = adopted
		return c.mdsCephFSRanksJournal()
	}
	return errors.New("MDS CephFS ranks takeover action is not admitted")
}

type mdsCephFSRanksPinCleanup struct {
	Target   string `json:"target"`
	Baseline string `json:"baseline"`
	State    string `json:"state"` // restored, unchanged, directory-absent, skipped, failed
	Readback string `json:"readback,omitempty"`
	Error    string `json:"error,omitempty"`
}

type mdsCephFSRanksCleanup struct {
	Source                string                     `json:"source_commit"`
	Token                 string                     `json:"token"`
	FSName                string                     `json:"fs_name"`
	Attempted             bool                       `json:"attempted"`
	Pins                  []*mdsCephFSRanksPin       `json:"pins"`
	PinCleanup            []mdsCephFSRanksPinCleanup `json:"pin_cleanup"`
	PinsRestored          bool                       `json:"pins_restored"`
	Takeover              *mdsCephFSRanksTakeover    `json:"takeover,omitempty"`
	Topology              *mdsCephFSRanksTopology    `json:"topology,omitempty"`
	TopologyRestored      bool                       `json:"topology_restored"`
	TopologyNote          string                     `json:"topology_note,omitempty"`
	ResidualSessions      []mdsSessionsNative        `json:"residual_owned_sessions"`
	NamespaceChecked      bool                       `json:"namespace_checked"`
	NamespaceRestored     bool                       `json:"namespace_restored"`
	NamespaceNote         string                     `json:"namespace_note,omitempty"`
	Namespace             *mdsCephFSRanksNative      `json:"namespace,omitempty"`
	NewBlocklist          []mdsSessionsBlock         `json:"new_blocklist"`
	RemovedBlocklist      []mdsCephFSRemovedBlock    `json:"removed_blocklist"`
	KeptBlocklist         []mdsSessionsBlock         `json:"kept_new_blocklist"`
	FinalBlocklist        []mdsSessionsBlock         `json:"final_blocklist,omitempty"`
	BlocklistRestored     bool                       `json:"blocklist_restored"`
	ClientIdentityRemoved bool                       `json:"client_identity_removed"`
	NativeIdentityRemoved bool                       `json:"native_identity_removed"`
	Completed             bool                       `json:"completed"`
	Errors                []string                   `json:"errors,omitempty"`
}

// finishMDSCephFSRanks runs once, inside the cached MDS fresh cleanup and
// before the owned filesystem is removed. It waits for the stable topology
// (the failed daemon returns as the standby by itself; nothing is restarted),
// restores every owned pin whose directory still exists, reads the namespace
// back, removes only blocklist entries attributable to the takeover or an
// observed owned Go session, and removes its two identities. Residual
// namespace entries are reported and discarded by the filesystem removal; no
// session is evicted.
func (c *fixtureCluster) finishMDSCephFSRanks(ctx context.Context) error {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	if s == nil || x == nil {
		return nil
	}
	ctx, stop := context.WithTimeout(ctx, 100*time.Second)
	defer stop()
	receipt := mdsCephFSRanksCleanup{Source: mdsFreshSource, Token: s.token, FSName: s.name, Attempted: true, Pins: x.pins, Takeover: x.takeover, PinCleanup: []mdsCephFSRanksPinCleanup{},
		ResidualSessions: []mdsSessionsNative{}, NewBlocklist: []mdsSessionsBlock{}, RemovedBlocklist: []mdsCephFSRemovedBlock{}, KeptBlocklist: []mdsSessionsBlock{}}
	if err := writeMDSFreshJSON(x.out, "mds-cephfs-ranks-cleanup.json", receipt); err != nil {
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	nativeName := "mds-cephfs-ranks-native-" + s.token
	if x.baselineReady {
		var t mdsCephFSRanksTopology
		var err error
		owned := c.mdsCephFSRankedOwned()
		deadline := time.Now().Add(60 * time.Second)
		for {
			t, err = c.mdsCephFSRanksTopology(ctx)
			if err == nil {
				err = t.stable(owned)
			}
			if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		receipt.Topology = &t
		receipt.TopologyRestored = err == nil
		if err != nil {
			receipt.TopologyNote = "the two-active and one-standby topology did not return; native mounts are skipped and the filesystem removal discards the namespace"
			fail(err)
		}
		goPresent := false
		if receipt.TopologyRestored {
			for _, d := range t.Ranks {
				data, err := c.mdsCephFSRanksTell(ctx, d, "session", "ls", "--format", "json")
				fail(err)
				if err != nil {
					goPresent = true
					continue
				}
				sessions, err := parseMDSSessionsList(data)
				fail(err)
				for _, session := range sessions {
					if session.AuthID == "mds-cephfs-ranks-"+s.token || session.AuthID == nativeName {
						receipt.ResidualSessions = append(receipt.ResidualSessions, session)
						goPresent = goPresent || session.AuthID == "mds-cephfs-ranks-"+s.token
					}
				}
				goPresent = goPresent || err != nil
			}
		}
		receipt.PinsRestored = true
		switch {
		case !receipt.TopologyRestored:
			receipt.NamespaceNote = "topology not stable; pins and namespace are discarded with the owned filesystem"
			receipt.PinsRestored = len(x.pinBaseline) == 0
		case x.native == nil:
			receipt.NamespaceNote = "native identity was never created; no pin or namespace mutation was possible"
			receipt.NamespaceChecked, receipt.NamespaceRestored = true, true
		case goPresent:
			// A native mount could wait on capability revocations from a Go
			// session whose process is gone. The filesystem removal discards it.
			receipt.NamespaceNote = "Go session still present; pin restoration and namespace readback skipped, both discarded with the owned filesystem"
			receipt.PinsRestored = len(x.pinBaseline) == 0
		default:
			for _, target := range []string{x.plan.Forward, x.plan.Migrate} {
				baseline, changed := x.pinBaseline[target]
				if !changed {
					continue
				}
				entry := mdsCephFSRanksPinCleanup{Target: target, Baseline: baseline}
				entry.State, entry.Readback, err = c.mdsCephFSRanksRestorePin(ctx, target, baseline)
				if err != nil {
					entry.Error = err.Error()
					receipt.PinsRestored = false
					fail(err)
				}
				receipt.PinCleanup = append(receipt.PinCleanup, entry)
			}
			path, _ := x.plan.path("dir")
			_, native, err := c.mdsCephFSRanksRunNative(ctx, "stat", path)
			fail(err)
			if err == nil {
				receipt.Namespace, receipt.NamespaceChecked, receipt.NamespaceRestored = &native, true, !native.Stat.Exists
				if native.Stat.Exists {
					receipt.NamespaceNote = "the owned directory remained; it is discarded with the owned filesystem"
				}
			}
		}
		blocks, err := c.mdsSessionsBlocklist(ctx)
		fail(err)
		if err == nil {
			for _, entry := range blocks {
				if !slices.ContainsFunc(x.baselineBlocklist, func(b mdsSessionsBlock) bool { return b.Address == entry.Address && b.Range == entry.Range }) {
					receipt.NewBlocklist = append(receipt.NewBlocklist, entry)
				}
			}
			c.mdsCephFSRanksRestoreBlocklist(ctx, &receipt, fail)
		}
	} else {
		receipt.NamespaceChecked, receipt.NamespaceRestored, receipt.BlocklistRestored, receipt.PinsRestored, receipt.TopologyRestored = true, true, true, true, true
		receipt.NamespaceNote = "preparation ended before any owned mutation was possible"
	}
	remove := func(config *ceph.ClientConfig, intent bool, label string) bool {
		if config == nil {
			if intent {
				fail(fmt.Errorf("MDS CephFS ranks %s identity creation outcome is unknown", label))
				return false
			}
			return true
		}
		err := c.cluster.DeleteClient(ctx, config)
		fail(err)
		return err == nil
	}
	receipt.ClientIdentityRemoved = remove(x.goClient, x.goIntent, "client")
	receipt.NativeIdentityRemoved = remove(x.native, x.nativeIntent, "native oracle")
	for _, err := range errs {
		receipt.Errors = append(receipt.Errors, err.Error())
	}
	receipt.Completed = len(errs) == 0 && receipt.NamespaceChecked && receipt.BlocklistRestored && receipt.PinsRestored && receipt.TopologyRestored &&
		receipt.ClientIdentityRemoved && receipt.NativeIdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("MDS CephFS ranks cleanup did not verify every check"))
	}
	return errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "mds-cephfs-ranks-cleanup.json", receipt))
}

// mdsCephFSRanksRestorePin returns one owned directory to its baseline pin
// during cleanup with the same three-mount recipe, unless the directory is
// gone (its pin went with it) or already at the baseline.
func (c *fixtureCluster) mdsCephFSRanksRestorePin(ctx context.Context, target, baseline string) (string, string, error) {
	x := c.mdsCephFSRanks
	// The current value must be one the fixture applied, or the value of a
	// change whose outcome it recorded as uncertain; anything else changed
	// outside the fixture.
	known := []string{x.pinApplied[target]}
	for _, p := range x.pins {
		if p.Target == target && (p.State == "intended" || p.State == "uncertain") {
			known = append(known, p.Value)
		}
	}
	path, err := x.plan.path(target)
	if err != nil {
		return "failed", "", err
	}
	_, prior, err := c.mdsCephFSRanksRunNative(ctx, "pin-get", path)
	if err != nil {
		return "failed", "", err
	}
	if !prior.Stat.Exists {
		return "directory-absent", "", nil
	}
	if inode := x.pinInode[target]; inode != 0 && inode != prior.Stat.Ino {
		return "skipped", *prior.Pin, errors.New("owned pin directory was replaced; refusing restoration")
	}
	if *prior.Pin == baseline {
		return "unchanged", *prior.Pin, nil
	}
	if !slices.Contains(known, *prior.Pin) {
		return "skipped", *prior.Pin, errors.New("owned pin changed outside the fixture; refusing restoration")
	}
	pin := &mdsCephFSRanksPin{Kind: "cleanup-restore", Target: target, Path: path, Inode: prior.Stat.Ino, Previous: *prior.Pin, Value: baseline, State: "intended"}
	x.pins = append(x.pins, pin)
	if err := c.mdsCephFSRanksJournal(); err != nil {
		return "failed", "", err
	}
	_, set, err := c.mdsCephFSRanksRunNative(ctx, "pin-set", path, baseline)
	if err != nil {
		pin.State, pin.Error = "uncertain", err.Error()
		return "failed", "", errors.Join(err, c.mdsCephFSRanksJournal())
	}
	pin.SetterGlobalID = set.NativeGlobalID
	_, readback, err := c.mdsCephFSRanksRunNative(ctx, "pin-get", path)
	if err == nil && (!readback.Stat.Exists || *readback.Pin != baseline) {
		err = errors.New("owned pin restoration readback differs from the baseline")
	}
	if err != nil {
		pin.State, pin.Error = "uncertain", err.Error()
		return "failed", "", errors.Join(err, c.mdsCephFSRanksJournal())
	}
	pin.State, pin.Readback, pin.ReaderGlobalID = "applied", *readback.Pin, readback.NativeGlobalID
	x.pinApplied[target] = baseline
	return "restored", *readback.Pin, c.mdsCephFSRanksJournal()
}

// mdsCephFSRanksRestoreBlocklist removes only new entries attributable to
// owned mutations: the failed rank-0 incarnation's addresses (after its GID
// left the FSMap) and addresses of owned Go sessions observed by the test.
func (c *fixtureCluster) mdsCephFSRanksRestoreBlocklist(ctx context.Context, receipt *mdsCephFSRanksCleanup, fail func(error)) {
	x := c.mdsCephFSRanks
	receipt.BlocklistRestored = true
	armedGone := false
	if x.takeover != nil {
		status, err := c.mdsCephFSRanksStatus(ctx)
		fail(err)
		armedGone = err == nil && !status.ArmedPresent
	}
	for _, entry := range receipt.NewBlocklist {
		kind := ""
		switch {
		case entry.Range:
		case x.takeover != nil && slices.Contains(x.takeover.ArmedAddresses, entry.Address):
			if armedGone {
				kind = "mds-takeover"
			} else {
				fail(errors.New("armed MDS incarnation is still mapped; its fencing entry is kept"))
			}
		case x.owned[entry.Address] != 0:
			kind = "owned-session"
		}
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
		fail(errors.New("MDS CephFS ranks acceptance kept new blocklist entries it cannot attribute to an owned mutation"))
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
