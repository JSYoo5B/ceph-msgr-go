package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS push acceptance extends the CephFS profile with a third owned
// directory and a fourth identity, the push oracle (MDS "allow rwps"), to
// provoke MDS pushes natively: a rename that revokes a Go dentry lease, a
// snapshot created and removed on one owned directory, a quota set and
// cleared on another, a temporary lowering of the owned daemon's
// mds_min_caps_per_client and mds_max_caps_per_client (RECALL_STATE from the
// upkeep thread) and one `cache drop` (RECALL_STATE followed by FLUSHMSG).
// Every mutation is journaled before it is issued and never reissued. The two
// configuration entries are owned provider overrides on the daemon's own
// section, restored by the test and again (idempotently, verified natively)
// during cleanup. The filesystem already allows snapshots (verified at
// preparation); its flags are never changed. Native behavior relied upon:
// mds/Locker.cc (issue_client_lease, revoke_client_leases,
// handle_client_lease), mds/Server.cc (handle_client_mksnap, _mksnap_finish,
// _rmsnap_finish, handle_set_vxattr quota, recall_client_state,
// flush_session), mds/MDCache.cc (broadcast_quota_to_client,
// do_realm_invalidate_and_update_notify, notify_global_snaprealm_update,
// upkeep_main), mds/MDSRank.cc (cache drop), mds/MDSAuthCaps.cc and
// pybind/cephfs/cephfs.pyx at mdsFreshSource (LGPL-2.1 references; no code
// copied).

const (
	mdsCephFSPushRecallFiles = 24
	// RECALL_STATE asks for newlim = mds_min_caps_per_client while a session
	// holds fewer than mds_recall_max_caps; the upkeep thread recalls only
	// sessions above mds_max_caps_per_client.
	mdsCephFSPushMinCaps  = 8
	mdsCephFSPushMaxCaps  = 12
	mdsCephFSPushMinName  = "mds_min_caps_per_client"
	mdsCephFSPushMaxName  = "mds_max_caps_per_client"
	mdsCephFSPushDropSecs = 10
)

// mdsCephFSPushPlan is the fixed push namespace of one run.
type mdsCephFSPushPlan struct {
	Directory     string   `json:"directory"`
	LeaseDir      string   `json:"lease_dir"`
	Victim        string   `json:"victim"`
	Renamed       string   `json:"renamed"`
	SnapDir       string   `json:"snap_dir"`
	SnapFile      string   `json:"snap_file"`
	Snapshot      string   `json:"snapshot"`
	QuotaDir      string   `json:"quota_dir"`
	QuotaFile     string   `json:"quota_file"`
	QuotaMaxBytes int64    `json:"quota_max_bytes"`
	RecallDir     string   `json:"recall_dir"`
	RecallFiles   []string `json:"recall_files"`
	MinCaps       uint64   `json:"recall_min_caps"`
	MaxCaps       uint64   `json:"recall_max_caps"`
	DropTimeout   int      `json:"cache_drop_timeout_s"`
}

func mdsCephFSPushPlanFor(token string) mdsCephFSPushPlan {
	raw, _ := hex.DecodeString(token)
	var seed uint16
	if len(raw) >= 2 {
		seed = binary.BigEndian.Uint16(raw)
	}
	files := make([]string, mdsCephFSPushRecallFiles)
	for i := range files {
		files[i] = fmt.Sprintf("recall-%02d", i)
	}
	snapshot := "ceph-msgr-snap"
	if len(token) >= 8 {
		snapshot += "-" + token[:8]
	}
	// A token-bound quota between 16 MiB and 272 MiB, in 4 KiB steps.
	return mdsCephFSPushPlan{Directory: "ceph-msgr-cephfs-" + token + "-push", LeaseDir: "lease", Victim: "victim", Renamed: "renamed", SnapDir: "snap", SnapFile: "snap-file",
		Snapshot: snapshot, QuotaDir: "quota", QuotaFile: "quota-file", QuotaMaxBytes: 1<<24 + 4096*int64(seed), RecallDir: "recall", RecallFiles: files,
		MinCaps: mdsCephFSPushMinCaps, MaxCaps: mdsCephFSPushMaxCaps, DropTimeout: mdsCephFSPushDropSecs}
}

// mdsCephFSPushTargets are the closed names a push action may address.
var mdsCephFSPushTargets = []string{"dir", "lease", "victim", "renamed", "snap", "snap-file", "snapshot", "quota", "quota-file", "recall"}

// path maps a closed push target to its absolute path. The directory keeps
// the "ceph-msgr-cephfs-" prefix and carries the "-push" suffix that the
// push driver admits.
func (p mdsCephFSPushPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-cephfs-") || !strings.HasSuffix(p.Directory, "-push") || strings.ContainsAny(p.Directory, "/ \x00") {
		return "", errors.New("MDS CephFS push directory is not owned")
	}
	root := "/" + p.Directory
	switch target {
	case "dir":
		return root, nil
	case "lease":
		return root + "/" + p.LeaseDir, nil
	case "victim":
		return root + "/" + p.LeaseDir + "/" + p.Victim, nil
	case "renamed":
		return root + "/" + p.LeaseDir + "/" + p.Renamed, nil
	case "snap":
		return root + "/" + p.SnapDir, nil
	case "snap-file":
		return root + "/" + p.SnapDir + "/" + p.SnapFile, nil
	case "snapshot":
		return root + "/" + p.SnapDir + "/.snap/" + p.Snapshot, nil
	case "quota":
		return root + "/" + p.QuotaDir, nil
	case "quota-file":
		return root + "/" + p.QuotaDir + "/" + p.QuotaFile, nil
	case "recall":
		return root + "/" + p.RecallDir, nil
	}
	return "", errors.New("MDS CephFS push target is not admitted")
}

func mdsCephFSPushNativeCaps(fsName string) ceph.ClientCaps {
	// Metadata only: snapshots need the MDS "s" flag (MAY_SNAPSHOT) and
	// ceph.quota vxattrs the "p" flag (MAY_SET_VXATTR). No OSD or MGR grant.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rwps fsname=" + fsName}
}

// mdsCephFSPushBaseline is the native state the push acceptance may change
// and must restore: the owned daemon's effective recall settings with no
// central entry for either option, and a filesystem that allows snapshots.
type mdsCephFSPushBaseline struct {
	AllowSnaps    bool   `json:"allow_snaps"`
	Section       string `json:"config_section"`
	DaemonMinCaps uint64 `json:"daemon_mds_min_caps_per_client"`
	DaemonMaxCaps uint64 `json:"daemon_mds_max_caps_per_client"`
	CentralCount  int    `json:"central_entries"`
}

// mdsCephFSPushConfig is a native readback of both recall settings.
type mdsCephFSPushConfig struct {
	Section        string `json:"section"`
	DaemonMinCaps  uint64 `json:"daemon_mds_min_caps_per_client"`
	DaemonMaxCaps  uint64 `json:"daemon_mds_max_caps_per_client"`
	CentralMinCaps string `json:"central_mds_min_caps_per_client,omitempty"`
	CentralMaxCaps string `json:"central_mds_max_caps_per_client,omitempty"`
	CentralCount   int    `json:"central_entries"`
}

// mdsCephFSCacheDrop is the native `cache drop` result, reduced.
type mdsCephFSCacheDrop struct {
	RecallReturnCode int64   `json:"client_recall_return_code"`
	RecallMessage    string  `json:"client_recall_message"`
	Recalled         int64   `json:"client_recall_recalled"`
	FlushReturnCode  int64   `json:"flush_journal_return_code"`
	FlushMessage     string  `json:"flush_journal_message"`
	Trimmed          int64   `json:"trim_cache_trimmed"`
	Duration         float64 `json:"duration_s"`
	StartedUTC       string  `json:"started_utc"`
	FinishedUTC      string  `json:"finished_utc"`
}

func parseMDSCephFSCacheDrop(data []byte) (mdsCephFSCacheDrop, error) {
	var result mdsCephFSCacheDrop
	if len(data) == 0 || len(data) > mdsFreshJSONLimit || !json.Valid(data) {
		return result, errors.New("MDS cache drop output exceeds its bound or is invalid")
	}
	type section struct {
		ReturnCode *int64  `json:"return_code"`
		Message    *string `json:"message"`
		Recalled   *int64  `json:"recalled"`
		Trimmed    *int64  `json:"trimmed"`
	}
	type output struct {
		Recall   *section `json:"client_recall"`
		Flush    *section `json:"flush_journal"`
		Trim     *section `json:"trim_cache"`
		Duration *float64 `json:"duration"`
		Error    *string  `json:"error"`
	}
	var native struct {
		output
		Result *output `json:"result"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return result, errors.New("MDS cache drop output shape differs")
	}
	o := native.output
	if native.Result != nil {
		o = *native.Result
	}
	if o.Error != nil {
		return result, fmt.Errorf("MDS cache drop failed: %s", *o.Error)
	}
	if o.Recall == nil || o.Recall.ReturnCode == nil || o.Recall.Message == nil || o.Recall.Recalled == nil || o.Flush == nil || o.Flush.ReturnCode == nil || o.Flush.Message == nil ||
		o.Trim == nil || o.Trim.Trimmed == nil || o.Duration == nil || *o.Duration < 0 {
		return result, errors.New("MDS cache drop output lacks a recall, journal flush, trim or duration section")
	}
	return mdsCephFSCacheDrop{RecallReturnCode: *o.Recall.ReturnCode, RecallMessage: *o.Recall.Message, Recalled: *o.Recall.Recalled, FlushReturnCode: *o.Flush.ReturnCode,
		FlushMessage: *o.Flush.Message, Trimmed: *o.Trim.Trimmed, Duration: *o.Duration}, nil
}

// parseMDSCephFSDaemonUint reads one unsigned `config get` value.
func parseMDSCephFSDaemonUint(data []byte, name string) (uint64, error) {
	var values map[string]string
	if len(data) == 0 || len(data) > 4096 || json.Unmarshal(data, &values) != nil || len(values) != 1 {
		return 0, errors.New("MDS daemon configuration readback differs")
	}
	raw, ok := values[name]
	value, err := strconv.ParseUint(raw, 10, 64)
	if !ok || err != nil || strconv.FormatUint(value, 10) != raw {
		return 0, errors.New("MDS daemon configuration value is not a canonical unsigned integer")
	}
	return value, nil
}

// parseMDSCephFSAllowSnaps reads the filesystem's allow_snaps flag from a
// native `fs get`.
func parseMDSCephFSAllowSnaps(data []byte, fscid int64, name string) (bool, error) {
	var native struct {
		ID  *int64 `json:"id"`
		Map struct {
			Name  string `json:"fs_name"`
			Flags *struct {
				AllowSnaps *bool `json:"allow_snaps"`
			} `json:"flags_state"`
		} `json:"mdsmap"`
	}
	if len(data) == 0 || len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &native) != nil || native.ID == nil || *native.ID != fscid || native.Map.Name != name ||
		native.Map.Flags == nil || native.Map.Flags.AllowSnaps == nil {
		return false, errors.New("MDS CephFS push filesystem flags differ")
	}
	return *native.Map.Flags.AllowSnaps, nil
}

func (c *fixtureCluster) mdsCephFSDaemonUint(ctx context.Context, name string) (uint64, error) {
	data, err := c.mdsSessionsDaemon(ctx, "config", "get", name, "--format", "json")
	if err != nil {
		return 0, err
	}
	return parseMDSCephFSDaemonUint(data, name)
}

// mdsCephFSPushReadConfig reads both recall settings from the daemon and the
// central database (entries for either name in any section are counted).
func (c *fixtureCluster) mdsCephFSPushReadConfig(ctx context.Context) (mdsCephFSPushConfig, error) {
	x := c.mdsCephFS
	state := mdsCephFSPushConfig{Section: "mds." + x.current.Name}
	var err error
	if state.DaemonMinCaps, err = c.mdsCephFSDaemonUint(ctx, mdsCephFSPushMinName); err != nil {
		return state, err
	}
	if state.DaemonMaxCaps, err = c.mdsCephFSDaemonUint(ctx, mdsCephFSPushMaxName); err != nil {
		return state, err
	}
	entries, err := c.cluster.Configuration(ctx)
	if err != nil {
		return state, err
	}
	for _, entry := range entries {
		if entry.Name != mdsCephFSPushMinName && entry.Name != mdsCephFSPushMaxName {
			continue
		}
		state.CentralCount++
		if entry.Section == state.Section && entry.Mask == "" {
			if entry.Name == mdsCephFSPushMinName {
				state.CentralMinCaps = entry.Value
			} else {
				state.CentralMaxCaps = entry.Value
			}
		}
	}
	return state, nil
}

// mdsCephFSPushWaitConfig waits until the daemon applies both values.
func (c *fixtureCluster) mdsCephFSPushWaitConfig(ctx context.Context, min, max uint64) (mdsCephFSPushConfig, error) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		state, err := c.mdsCephFSPushReadConfig(ctx)
		if err != nil || state.DaemonMinCaps == min && state.DaemonMaxCaps == max {
			return state, err
		}
		select {
		case <-ctx.Done():
			return state, fmt.Errorf("MDS daemon recall settings did not reach %d/%d: %w", min, max, ctx.Err())
		case <-tick.C:
		}
	}
}

// preparePush records the push baseline and creates the push oracle
// identity. The filesystem must already allow snapshots and neither recall
// option may have a central entry, so restoration is exact.
func (c *fixtureCluster) prepareMDSCephFSPush(ctx context.Context, out string) error {
	s, x := c.mdsFresh, c.mdsCephFS
	data, err := c.cluster.Ceph(ctx, "fs", "get", s.name, "--format", "json")
	if err != nil {
		return err
	}
	allow, err := parseMDSCephFSAllowSnaps(data, s.oracle.FSCID, s.name)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("MDS CephFS push acceptance requires a filesystem that already allows snapshots; it never changes filesystem flags")
	}
	state, err := c.mdsCephFSPushReadConfig(ctx)
	if err != nil {
		return err
	}
	if state.CentralCount != 0 || state.DaemonMinCaps <= mdsCephFSPushMinCaps || state.DaemonMaxCaps <= mdsCephFSPushMaxCaps {
		return errors.New("MDS CephFS push acceptance refuses to adopt existing recall settings")
	}
	x.pushBaseline = &mdsCephFSPushBaseline{AllowSnaps: allow, Section: state.Section, DaemonMinCaps: state.DaemonMinCaps, DaemonMaxCaps: state.DaemonMaxCaps}
	x.pushIntent = true
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	x.pushNative, err = c.cluster.CreateClient(ctx, "mds-cephfs-push-native-"+s.token, mdsCephFSPushNativeCaps(s.name))
	if err != nil {
		return fmt.Errorf("create dedicated native CephFS push identity: %w", err)
	}
	_, keyring, err := x.pushNative.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(keyring); err != nil {
		return err
	}
	// The key reaches each push worker on its standard input only.
	return nil
}

// The push driver is a separate fixed LibCephFS program bound to the push
// directory. It admits the directory, its children and grandchildren, and
// <dir>/<child>/.snap/<name> for snapshot actions; extra arguments are only a
// rename destination (same rules) or a decimal quota. It always shuts down.
// Source: pybind/cephfs/cephfs.pyx at mdsFreshSource; independent driver.
const mdsCephFSPushNativePython = `
import cephfs, json, os, sys, time
identity, fsname, directory, action, path = sys.argv[1:6]
extra = sys.argv[6:]
def owned(p):
    parts = p.split('/')
    if not directory.startswith('ceph-msgr-cephfs-') or not directory.endswith('-push') or '/' in directory:
        return False
    if len(parts) < 2 or len(parts) > 5 or parts[0] != '' or parts[1] != directory:
        return False
    if any(c in ('', '.', '..') for c in parts[1:]):
        return False
    if '.snap' in parts[1:]:
        return len(parts) == 5 and parts[3] == '.snap' and parts.count('.snap') == 1
    return len(parts) <= 4
snapshot = len(path.split('/')) == 5
if not owned(path) or (action == 'rename' and (len(extra) != 1 or not owned(extra[0]) or snapshot or len(extra[0].split('/')) == 5)):
    raise ValueError('path outside the owned directory')
MASK = 0x30f
fs = cephfs.LibCephFS(conffile='/tmp/ceph-msgr-test/ceph.conf', auth_id=identity.removeprefix('client.'), conf={'key': NATIVE_KEY})
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
    elif action == 'rename' and len(extra) == 1:
        start = time.monotonic_ns()
        fs.rename(path, extra[0])
        out['timings_ns'] = {'start': 0, 'renamed': time.monotonic_ns() - start}
        out['source_stat'] = stat(path)
        out['stat'] = stat(extra[0])
    elif action in ('mksnap', 'rmsnap') and snapshot and not extra:
        start = time.monotonic_ns()
        if action == 'mksnap':
            fs.mkdir(path, 0o755)
        else:
            fs.rmdir(path)
        out['timings_ns'] = {'start': 0, 'done': time.monotonic_ns() - start}
        out['stat'] = stat(path)
    elif action == 'setquota' and not snapshot and len(extra) == 1 and extra[0].isdigit() and extra[0].isascii():
        fs.setxattr(path, 'ceph.quota.max_bytes', extra[0].encode(), 0)
        try:
            out['value_hex'] = fs.getxattr(path, 'ceph.quota.max_bytes', 64).hex()
        except cephfs.NoData:
            out['quota_absent'] = True
        out['stat'] = stat(path)
    else:
        raise ValueError('native CephFS push action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSPushNative accepts exactly the shape of one push action.
func decodeMDSCephFSPushNative(action, path string, extra []string, data []byte) (mdsCephFSNative, error) {
	result, err := decodeMDSCephFSNativeOutput(action, path, data)
	if err != nil {
		return result, err
	}
	timed := func(last string) bool {
		start, ok := result.TimingsNS["start"]
		end, done := result.TimingsNS[last]
		return len(result.TimingsNS) == 2 && ok && done && start == 0 && end >= 0
	}
	ok := false
	switch action {
	case "stat":
		ok = result.Stat != nil && validMDSCephFSStat(*result.Stat)
	case "rename":
		ok = result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.SourceStat != nil && !result.SourceStat.Exists && validMDSCephFSStat(*result.SourceStat) && timed("renamed")
	case "mksnap":
		ok = result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && timed("done")
	case "rmsnap":
		ok = result.Stat != nil && !result.Stat.Exists && validMDSCephFSStat(*result.Stat) && timed("done")
	case "setquota":
		// The readback is evidence, not a precondition: the setxattr already
		// succeeded. It is either a canonical decimal or reported absent
		// (ENODATA); the test compares it with the requested value.
		value, err := hex.DecodeString(result.ValueHex)
		decimal, parseErr := strconv.ParseUint(string(value), 10, 64)
		readback := err == nil && parseErr == nil && strconv.FormatUint(decimal, 10) == string(value) && !result.QuotaAbsent
		readback = readback || result.QuotaAbsent && result.ValueHex == ""
		ok = len(extra) == 1 && readback && result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.Stat.Mode&0o170000 == 0o040000
	}
	if !ok || result.TimingsNS != nil && action != "rename" && action != "mksnap" && action != "rmsnap" {
		return mdsCephFSNative{}, fmt.Errorf("native CephFS push %s output shape differs", action)
	}
	return result, nil
}

// mdsCephFSRunPushNative runs the push driver once as the push identity.
func (c *fixtureCluster) mdsCephFSRunPushNative(ctx context.Context, action, path string, extra ...string) (*ceph.NativeCommandResult, mdsCephFSNative, error) {
	s, x := c.mdsFresh, c.mdsCephFS
	if x.pushNative == nil {
		return nil, mdsCephFSNative{}, errors.New("native CephFS push identity is unavailable")
	}
	timeout := 30 * time.Second
	if action != "stat" {
		timeout = 45 * time.Second
	}
	argv := append([]string{"python3", "-c", mdsCephFSPushNativePython, "client.mds-cephfs-push-native-" + s.token, s.name, x.push.Directory, action, path}, extra...)
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: mdsCephFSJSONLimit, MaxStderr: 16 << 10, Label: "owned native CephFS push " + action}, x.pushNative)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, mdsCephFSNative{}, errors.Join(err, fmt.Errorf("native CephFS push %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeMDSCephFSPushNative(action, path, extra, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// pushMutation finds the newest journal record of one kind.
func (x *mdsCephFSScope) pushMutation(kind string) *mdsCephFSMutation {
	for _, m := range slices.Backward(x.mutations) {
		if m.Kind == kind {
			return m
		}
	}
	return nil
}

// mdsCephFSPushAct runs one push action. Native mutations are one-shot; the
// removal of a snapshot or quota requires its applied creation, and the
// recall settings move only between their baseline and the owned values.
func (c *fixtureCluster) mdsCephFSPushAct(ctx context.Context, id int64, action mdsCephFSAction, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	record := func(kind, target string) (*mdsCephFSMutation, error) {
		if x.mutated(kind) {
			return nil, fmt.Errorf("MDS CephFS %s was already issued", kind)
		}
		m := &mdsCephFSMutation{Kind: kind, RequestID: id, Target: target, State: "intended"}
		x.mutations = append(x.mutations, m)
		result.Mutation = m
		return m, c.mdsCephFSJournal()
	}
	requireApplied := func(kind string) error {
		if m := x.pushMutation(kind); m == nil || m.State != "applied" {
			return fmt.Errorf("MDS CephFS push action requires an applied %s", kind)
		}
		return nil
	}
	native := func(m *mdsCephFSMutation, driver, target string, extra ...string) error {
		path, err := x.push.path(target)
		if err != nil {
			return err
		}
		for i, arg := range extra {
			if slices.Contains(mdsCephFSPushTargets, arg) {
				if extra[i], err = x.push.path(arg); err != nil {
					return err
				}
			}
		}
		process, output, err := c.mdsCephFSRunPushNative(ctx, driver, path, extra...)
		result.Process = process
		if err != nil {
			if m != nil {
				m.State, m.Error = "uncertain", err.Error()
				err = errors.Join(err, c.mdsCephFSJournal())
			}
			return err
		}
		result.Native = &output
		if m != nil {
			m.State, m.NativeGlobalID = "applied", output.NativeGlobalID
			return c.mdsCephFSJournal()
		}
		return nil
	}
	quota := strconv.FormatInt(x.push.QuotaMaxBytes, 10)
	switch action.name {
	case "push-stat":
		return native(nil, "stat", action.target)
	case "push-rename":
		m, err := record("push-rename", x.push.Victim+"->"+x.push.Renamed)
		if err != nil {
			return err
		}
		return native(m, "rename", "victim", "renamed")
	case "push-snapshot-create":
		m, err := record("push-snapshot-create", x.push.SnapDir+"/.snap/"+x.push.Snapshot)
		if err != nil {
			return err
		}
		return native(m, "mksnap", "snapshot")
	case "push-snapshot-remove":
		if err := requireApplied("push-snapshot-create"); err != nil {
			return err
		}
		m, err := record("push-snapshot-remove", x.push.SnapDir+"/.snap/"+x.push.Snapshot)
		if err != nil {
			return err
		}
		return native(m, "rmsnap", "snapshot")
	case "push-quota-set":
		m, err := record("push-quota-set", x.push.QuotaDir+" ceph.quota.max_bytes="+quota)
		if err != nil {
			return err
		}
		return native(m, "setquota", "quota", quota)
	case "push-quota-clear":
		if err := requireApplied("push-quota-set"); err != nil {
			return err
		}
		m, err := record("push-quota-clear", x.push.QuotaDir+" ceph.quota.max_bytes=0")
		if err != nil {
			return err
		}
		return native(m, "setquota", "quota", "0")
	case "push-recall-on":
		return c.mdsCephFSPushRecallOn(ctx, id, result)
	case "push-recall-max-off", "push-recall-off":
		return c.mdsCephFSPushRecallOff(ctx, action.name == "push-recall-off", result)
	case "push-cache-drop":
		if err := requireApplied("push-config-min"); err != nil {
			return err
		}
		m, err := record("push-cache-drop", fmt.Sprintf("tell mds.%s:0 cache drop %d", c.mdsFresh.name, x.push.DropTimeout))
		if err != nil {
			return err
		}
		started := time.Now().UTC()
		data, err := c.mdsSessionsDaemon(ctx, "cache", "drop", strconv.Itoa(x.push.DropTimeout), "--format", "json")
		finished := time.Now().UTC()
		if len(data) != 0 {
			m.Output = strings.TrimSpace(string(data[:min(len(data), 4096)]))
		}
		var drop mdsCephFSCacheDrop
		if err == nil {
			drop, err = parseMDSCephFSCacheDrop(data)
		}
		if err != nil {
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSJournal())
		}
		drop.StartedUTC, drop.FinishedUTC = started.Format(time.RFC3339Nano), finished.Format(time.RFC3339Nano)
		m.State = "applied"
		result.CacheDrop = &drop
		return c.mdsCephFSJournal()
	}
	return errors.New("MDS CephFS push action is not admitted")
}

// mdsCephFSPushRecallOn applies mds_min_caps_per_client, then
// mds_max_caps_per_client, on the owned daemon's section, and waits until the
// daemon reports both values.
func (c *fixtureCluster) mdsCephFSPushRecallOn(ctx context.Context, id int64, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	if x.pushBaseline == nil || x.pushMin != nil || x.pushMax != nil || x.mutated("push-config-min") || x.mutated("push-config-max") {
		return errors.New("MDS CephFS push recall settings are not at their owned baseline")
	}
	section := x.pushBaseline.Section
	if section != "mds."+x.current.Name {
		return errors.New("MDS CephFS push recall section does not name the owned daemon")
	}
	current, err := c.mdsCephFSPushReadConfig(ctx)
	if err != nil {
		return err
	}
	if current.CentralCount != 0 || current.DaemonMinCaps != x.pushBaseline.DaemonMinCaps || current.DaemonMaxCaps != x.pushBaseline.DaemonMaxCaps {
		return errors.New("MDS CephFS push recall settings changed outside the fixture")
	}
	for _, step := range []struct {
		kind, name string
		value      uint64
		handle     **ceph.ConfigOverride
	}{{"push-config-min", mdsCephFSPushMinName, x.push.MinCaps, &x.pushMin}, {"push-config-max", mdsCephFSPushMaxName, x.push.MaxCaps, &x.pushMax}} {
		m := &mdsCephFSMutation{Kind: step.kind, RequestID: id, Target: section + " " + step.name + "=" + strconv.FormatUint(step.value, 10), State: "intended"}
		x.mutations = append(x.mutations, m)
		result.Mutation = m
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
		// The provider keeps a handle for Restore even after an uncertain reply.
		*step.handle, err = c.cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: section, Name: step.name, Value: strconv.FormatUint(step.value, 10)})
		if err != nil {
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSJournal())
		}
		m.State = "applied"
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
	}
	state, err := c.mdsCephFSPushWaitConfig(ctx, x.push.MinCaps, x.push.MaxCaps)
	result.PushConfig = &state
	return err
}

// mdsCephFSPushRecallOff restores mds_max_caps_per_client and, when all is
// set, mds_min_caps_per_client, then waits for the daemon's baseline values.
func (c *fixtureCluster) mdsCephFSPushRecallOff(ctx context.Context, all bool, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	restore := func(kind string, handle *ceph.ConfigOverride) error {
		m := x.pushMutation(kind)
		if m == nil || handle == nil {
			return fmt.Errorf("MDS CephFS push %s restore requested without an owned override", kind)
		}
		if m.State == "restored" {
			return nil
		}
		if m.State != "applied" {
			return fmt.Errorf("MDS CephFS push %s is %s, not applied", kind, m.State)
		}
		m.State = "restore-intended"
		result.Mutation = m
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
		if err := handle.Restore(ctx); err != nil {
			m.State, m.Error = "restore-uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSJournal())
		}
		m.State = "restored"
		return c.mdsCephFSJournal()
	}
	if err := restore("push-config-max", x.pushMax); err != nil {
		return err
	}
	if err := requireAppliedOrRestored(x, "push-config-min"); err != nil {
		return err
	}
	min := x.push.MinCaps
	if all {
		if err := restore("push-config-min", x.pushMin); err != nil {
			return err
		}
		min = x.pushBaseline.DaemonMinCaps
	}
	state, err := c.mdsCephFSPushWaitConfig(ctx, min, x.pushBaseline.DaemonMaxCaps)
	result.PushConfig = &state
	if err == nil && all && state.CentralCount != 0 {
		err = errors.New("MDS CephFS push recall settings left a central entry after restoration")
	}
	return err
}

func requireAppliedOrRestored(x *mdsCephFSScope, kind string) error {
	if m := x.pushMutation(kind); m == nil || m.State != "applied" && m.State != "restored" {
		return fmt.Errorf("MDS CephFS push %s is not owned", kind)
	}
	return nil
}

// finishMDSCephFSPushConfig restores both owned overrides (a confirmed
// restore is a no-op; the provider reconciles an interrupted one and refuses
// outside edits) and verifies the baseline natively. It reports whether the
// recall settings equal their baseline.
func (c *fixtureCluster) finishMDSCephFSPushConfig(ctx context.Context, receipt *mdsCephFSCleanup, fail func(error)) {
	x := c.mdsCephFS
	if x.pushBaseline == nil {
		receipt.PushConfigRestored = x.pushMin == nil && x.pushMax == nil
		return
	}
	var restoreErr error
	for _, step := range []struct {
		kind   string
		handle *ceph.ConfigOverride
	}{{"push-config-max", x.pushMax}, {"push-config-min", x.pushMin}} {
		if step.handle == nil {
			continue
		}
		err := step.handle.Restore(ctx)
		if m := x.pushMutation(step.kind); m != nil && err == nil && m.State != "restored" {
			m.State = "restored"
		}
		restoreErr = errors.Join(restoreErr, err)
	}
	fail(restoreErr)
	fail(c.mdsCephFSJournal())
	wait, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	state, err := c.mdsCephFSPushWaitConfig(wait, x.pushBaseline.DaemonMinCaps, x.pushBaseline.DaemonMaxCaps)
	fail(err)
	receipt.PushConfig = &state
	receipt.PushConfigRestored = restoreErr == nil && err == nil && state.CentralCount == 0
	if err == nil && state.CentralCount != 0 {
		fail(errors.New("MDS CephFS push recall settings left a central entry"))
	}
}
