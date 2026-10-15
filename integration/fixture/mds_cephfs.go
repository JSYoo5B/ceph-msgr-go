package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// The CephFS metadata acceptance drives a closed set of native LibCephFS
// operations on the owned filesystem of the MDS fresh fixture: stat, getxattr,
// list, read, and two one-shot mutations (create three files, write one file).
// Every mutation is journaled before it is issued and is never replayed after
// an uncertain outcome. The Go test removes the namespace with its own
// requests; cleanup independently reads it back, checks residual sessions and
// the blocklist, and removes the two identities. Native behavior relied upon:
// client/Client.cc, pybind/cephfs/cephfs.pyx, mds/Server.cc and
// mds/SessionMap.cc at mdsFreshSource (LGPL-2.1 references; no code copied).

const mdsCephFSJSONLimit = 64 << 10
const mdsCephFSXattrName = "user.ceph-msgr"

// mdsCephFSPlan is the fixed namespace of one run. Every native path is
// derived from it; callers select only a closed target name.
type mdsCephFSPlan struct {
	Directory     string   `json:"directory"`
	GoFile        string   `json:"go_file"`
	NativeFiles   []string `json:"native_files"`
	Missing       string   `json:"missing"`
	XattrName     string   `json:"xattr_name"`
	XattrValueHex string   `json:"xattr_value_hex"`
	PayloadHex    string   `json:"payload_hex"`
}

func mdsCephFSPlanFor(token string) mdsCephFSPlan {
	raw, _ := hex.DecodeString(token)
	// A binary value: NUL and 0xff bytes around ASCII and the raw token.
	value := append([]byte{0x00, 0xff}, "ceph-msgr"...)
	value = append(append(value, 0x00), raw...)
	sum := sha256.Sum256([]byte("ceph-msgr-cephfs-payload:" + token))
	payload := append(sum[:], sum[:]...)
	return mdsCephFSPlan{Directory: "ceph-msgr-cephfs-" + token, GoFile: "go-file", NativeFiles: []string{"native-0", "native-1", "native-2"},
		Missing: "missing", XattrName: mdsCephFSXattrName, XattrValueHex: hex.EncodeToString(value), PayloadHex: hex.EncodeToString(payload)}
}

// path maps a closed target name to its absolute path in the owned filesystem.
func (p mdsCephFSPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-cephfs-") || strings.ContainsAny(p.Directory, "/ \x00") {
		return "", errors.New("MDS CephFS plan directory is not owned")
	}
	root := "/" + p.Directory
	switch {
	case target == "dir":
		return root, nil
	case target == p.GoFile, target == p.Missing, slices.Contains(p.NativeFiles, target):
		return root + "/" + target, nil
	}
	return "", errors.New("MDS CephFS target is not admitted")
}

func mdsCephFSGoCaps(fsName string) ceph.ClientCaps {
	// Metadata only: MON read for authentication and a read-write MDS grant
	// bound to the owned filesystem. No OSD, MGR or path restriction.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName}
}

func mdsCephFSNativeCaps(fsName string) ceph.ClientCaps {
	// The native oracle mounts, writes file data and reads it back, so it
	// also holds the CephFS data-pool tag grant of the owned filesystem.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName, OSD: "allow rw tag cephfs data=" + fsName}
}

type mdsCephFSMutation struct {
	Kind           string `json:"kind"`
	RequestID      int64  `json:"request_id"`
	Target         string `json:"target"`
	State          string `json:"state"`
	NativeGlobalID uint64 `json:"native_global_id,omitempty"`
	GlobalID       uint64 `json:"global_id,omitempty"`
	Address        string `json:"address,omitempty"`
	Until          string `json:"blocklist_until,omitempty"`
	Output         string `json:"native_output,omitempty"`
	Error          string `json:"error,omitempty"`
}

type mdsCephFSScope struct {
	out                    string
	plan                   mdsCephFSPlan
	recovery               mdsCephFSRecoveryPlan
	goClient, native       *ceph.ClientConfig
	recClient              *ceph.ClientConfig
	goIntent, nativeIntent bool
	recIntent              bool
	baselineReady          bool
	baselineBlocklist      []mdsSessionsBlock
	lastRequestID          int64
	mutations              []*mdsCephFSMutation
	// current is the owned rank-0 incarnation every action proves: the
	// startup daemon until an owned restart is adopted.
	current mdsFreshDaemon
	restart *mdsCephFSRestart
	// owned maps session addresses of owned Go identities, observed through
	// observe or eviction, to their GlobalID.
	owned map[string]uint64
}

func (x *mdsCephFSScope) mutated(kind string) bool {
	return slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == kind })
}

type mdsCephFSAction struct {
	name, target string
	gid          uint64
}

// The request is <id> <ownership token> <action> [argument]. Only fixed
// actions and target names exist; paths and values come from the plan.
func parseMDSCephFSRequest(fields []string, token string, previousID int64) (int64, mdsCephFSAction, error) {
	var action mdsCephFSAction
	if len(fields) < 3 || len(fields) > 4 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned MDS CephFS request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, action, errors.New("invalid owned MDS CephFS request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid MDS CephFS request identity")
	}
	action.name = fields[2]
	arg := ""
	if len(fields) == 4 {
		arg = fields[3]
	}
	files := []string{"go-file", "native-0", "native-1", "native-2", "missing"}
	switch action.name {
	case "observe", "evict", "recovery-ops":
		gid, err := strconv.ParseUint(arg, 10, 64)
		if err != nil || gid == 0 || strconv.FormatUint(gid, 10) != arg {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS session-scoped actions require one canonical GlobalID")
		}
		action.gid = gid
	case "recovery-stat":
		if arg != "dir" && !slices.Contains(mdsCephFSRecoveryNames, arg) {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS recovery stat target is not admitted")
		}
		action.target = arg
	case "recovery-list", "recovery-write", "restart-arm", "restart-fire", "restart-status", "restart-adopt":
		if arg != "" {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS recovery and restart actions take no caller argument")
		}
		switch action.name {
		case "recovery-list":
			action.target = "dir"
		case "recovery-write":
			action.target = "fenced"
		}
	case "stat":
		if arg != "dir" && !slices.Contains(files, arg) {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS stat target is not admitted")
		}
		action.target = arg
	case "getxattr", "write", "read":
		if arg != "go-file" {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS file action admits only the Go-created file")
		}
		action.target = arg
	case "list":
		if arg != "dir" {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS listing admits only the owned directory")
		}
		action.target = arg
	case "create-native":
		if arg != "" {
			return 0, mdsCephFSAction{}, errors.New("MDS CephFS native creation takes no caller argument")
		}
		action.target = "dir"
	default:
		return 0, mdsCephFSAction{}, errors.New("MDS CephFS action is not admitted")
	}
	return id, action, nil
}

// One fixed native LibCephFS driver per action. The fixture passes only plan
// paths and values; the driver re-checks that every path is inside an owned
// directory (both plan directories share the ceph-msgr-cephfs- prefix). It
// mounts the owned filesystem by name and always shuts down, which closes its
// own CephFS session. Source: pybind/cephfs/cephfs.pyx at mdsFreshSource;
// independent driver, no code copied.
const mdsCephFSNativePython = `
import cephfs, json, os, sys, time
identity, keyring, fsname, directory, action, path = sys.argv[1:7]
extra = sys.argv[7:]
prefix = '/' + directory
if not directory.startswith('ceph-msgr-cephfs-') or '/' in directory or not (path == prefix or (path.startswith(prefix + '/') and path.count('/') == 2)):
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
    elif action == 'getxattr' and len(extra) == 1:
        out['value_hex'] = fs.getxattr(path, extra[0], 65536).hex()
    elif action == 'create' and path == prefix and len(extra) == 3:
        out['created'] = []
        for name in extra:
            fd = fs.open(path + '/' + name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            fs.close(fd)
            out['created'].append({'name': name, 'stat': stat(path + '/' + name)})
    elif action == 'list' and path == prefix and not extra:
        out['entries'] = []
        handle = fs.opendir(path)
        try:
            while True:
                entry = fs.readdir(handle)
                if entry is None:
                    break
                name = entry.d_name.decode('utf-8')
                if name in ('.', '..'):
                    continue
                out['entries'].append({'name': name, 'd_ino': entry.d_ino, 'stat': stat(path + '/' + name)})
        finally:
            fs.closedir(handle)
    elif action == 'write' and len(extra) == 1:
        data = bytes.fromhex(extra[0])
        clock = [('start', time.monotonic_ns())]
        fd = fs.open(path, os.O_WRONLY, 0)
        clock.append(('opened', time.monotonic_ns()))
        out['written'] = fs.write(fd, data, 0)
        clock.append(('written', time.monotonic_ns()))
        fs.fsync(fd, 0)
        clock.append(('synced', time.monotonic_ns()))
        fs.close(fd)
        clock.append(('closed', time.monotonic_ns()))
        out['timings_ns'] = {name: value - clock[0][1] for name, value in clock}
        out['stat'] = stat(path)
    elif action == 'create-write' and len(extra) == 1:
        data = bytes.fromhex(extra[0])
        fd = fs.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        out['written'] = fs.write(fd, data, 0)
        fs.fsync(fd, 0)
        fs.close(fd)
        out['stat'] = stat(path)
    elif action == 'read' and len(extra) == 1:
        fd = fs.open(path, os.O_RDONLY, 0)
        try:
            out['data_hex'] = fs.read(fd, 0, int(extra[0]) + 1).hex()
        finally:
            fs.close(fd)
        out['stat'] = stat(path)
    else:
        raise ValueError('native CephFS action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

type mdsCephFSStat struct {
	Exists bool   `json:"exists"`
	Errno  int    `json:"errno"`
	Ino    uint64 `json:"ino"`
	Mode   uint32 `json:"mode"`
	UID    uint32 `json:"uid"`
	GID    uint32 `json:"gid"`
	NLink  uint32 `json:"nlink"`
	Size   uint64 `json:"size"`
}

type mdsCephFSEntry struct {
	Name string        `json:"name"`
	DIno uint64        `json:"d_ino,omitempty"`
	Stat mdsCephFSStat `json:"stat"`
}

type mdsCephFSNative struct {
	Action         string           `json:"action"`
	Path           string           `json:"path"`
	NativeGlobalID uint64           `json:"native_global_id"`
	Stat           *mdsCephFSStat   `json:"stat,omitempty"`
	ValueHex       string           `json:"value_hex,omitempty"`
	Created        []mdsCephFSEntry `json:"created,omitempty"`
	Entries        []mdsCephFSEntry `json:"entries,omitempty"`
	Written        int              `json:"written,omitempty"`
	TimingsNS      map[string]int64 `json:"timings_ns,omitempty"`
	DataHex        string           `json:"data_hex,omitempty"`
}

func validMDSCephFSStat(s mdsCephFSStat) bool {
	if !s.Exists {
		// Only ENOENT is an observation; any other failure aborts the driver.
		return s.Errno == 2 && s.Ino == 0
	}
	return s.Errno == 0 && s.Ino != 0 && s.Mode != 0
}

// decodeMDSCephFSNativeOutput bounds and decodes one driver output with
// unknown fields refused; callers then require the per-action shape.
func decodeMDSCephFSNativeOutput(action, path string, data []byte) (mdsCephFSNative, error) {
	var result mdsCephFSNative
	if len(data) == 0 || len(data) > mdsCephFSJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSNative{}, errors.New("native CephFS output identity differs")
	}
	return result, nil
}

// decodeMDSCephFSNative accepts exactly the shape of one admitted action.
func decodeMDSCephFSNative(action, path string, plan mdsCephFSPlan, data []byte) (mdsCephFSNative, error) {
	result, err := decodeMDSCephFSNativeOutput(action, path, data)
	if err != nil {
		return result, err
	}
	ok := false
	switch action {
	case "stat":
		ok = result.Stat != nil && validMDSCephFSStat(*result.Stat)
	case "getxattr":
		_, err := hex.DecodeString(result.ValueHex)
		ok = err == nil && result.ValueHex != ""
	case "create":
		ok = len(result.Created) == len(plan.NativeFiles)
		for i, entry := range result.Created {
			ok = ok && entry.Name == plan.NativeFiles[i] && entry.Stat.Exists && validMDSCephFSStat(entry.Stat)
		}
	case "list":
		ok = len(result.Entries) <= 64
		seen := map[string]bool{}
		for _, entry := range result.Entries {
			ok = ok && entry.Name != "" && !seen[entry.Name] && entry.DIno != 0 && validMDSCephFSStat(entry.Stat)
			seen[entry.Name] = true
		}
	case "write":
		ok = result.Written == len(plan.PayloadHex)/2 && len(result.TimingsNS) == 5 && result.Stat != nil && validMDSCephFSStat(*result.Stat)
		previous := int64(0)
		for _, name := range []string{"start", "opened", "written", "synced", "closed"} {
			value, present := result.TimingsNS[name]
			ok = ok && present && value >= previous
			previous = value
		}
	case "read":
		_, err := hex.DecodeString(result.DataHex)
		ok = err == nil && result.Stat != nil && validMDSCephFSStat(*result.Stat)
	}
	if !ok {
		return mdsCephFSNative{}, fmt.Errorf("native CephFS %s output shape differs", action)
	}
	return result, nil
}

type mdsCephFSResult struct {
	Source          string                    `json:"source_commit"`
	Token           string                    `json:"token"`
	RequestID       int64                     `json:"request_id"`
	Action          string                    `json:"action"`
	Target          string                    `json:"target,omitempty"`
	GlobalID        uint64                    `json:"global_id,omitempty"`
	FSID            string                    `json:"fsid"`
	FSCID           int64                     `json:"fscid"`
	FSName          string                    `json:"fs_name"`
	MDSGID          uint64                    `json:"mds_gid"`
	MDSEndpoint     string                    `json:"mds_endpoint"`
	MDSNonce        uint32                    `json:"mds_nonce"`
	MDSIncarnation  int32                     `json:"mds_incarnation"`
	Rank            int32                     `json:"rank"`
	IdentityChecked bool                      `json:"identity_checked"`
	Map             *mdsSessionsMap           `json:"map,omitempty"`
	DaemonEpoch     uint32                    `json:"daemon_mdsmap_epoch,omitempty"`
	Sessions        []mdsSessionsNative       `json:"sessions"`
	Blocklist       []mdsSessionsBlock        `json:"blocklist,omitempty"`
	Native          *mdsCephFSNative          `json:"native,omitempty"`
	Process         *ceph.NativeCommandResult `json:"native_process,omitempty"`
	Mutation        *mdsCephFSMutation        `json:"mutation,omitempty"`
	Restart         *mdsCephFSRestart         `json:"restart,omitempty"`
	RankStatus      *mdsCephFSRankStatus      `json:"rank_status,omitempty"`
	HistoricOps     []mdsCephFSHistoricOp     `json:"historic_ops,omitempty"`
	Complete        bool                      `json:"complete"`
	Error           string                    `json:"error,omitempty"`
	ElapsedMillis   int64                     `json:"elapsed_ms"`
	ObservedUTC     string                    `json:"observed_utc"`
}

func (c *fixtureCluster) mdsCephFSJournal() error {
	s, x := c.mdsFresh, c.mdsCephFS
	identity, native, recovery := "", "", ""
	if x.goIntent {
		identity = "client.mds-cephfs-" + s.token
	}
	if x.nativeIntent {
		native = "client.mds-cephfs-native-" + s.token
	}
	if x.recIntent {
		recovery = "client.mds-cephfs-recovery-" + s.token
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-owned.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name, "fscid": s.oracle.FSCID,
		"client_identity": identity, "native_identity": native, "recovery_identity": recovery, "plan": x.plan, "recovery_plan": x.recovery,
		"baseline_blocklist": x.baselineBlocklist, "mutations": x.mutations, "restart": x.restart, "current_mds": x.current, "owned_session_addresses": x.owned,
	})
}

func (c *fixtureCluster) prepareMDSCephFS(ctx context.Context, out string) error {
	p := c.profile
	if p.mdsCephFS != "1" || p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.osdProfile != "legacy-v6" || c.mdsFresh == nil || c.mdsFresh.fs == nil || c.mdsCephFS != nil {
		return errors.New("invalid MDS CephFS preparation scope")
	}
	s := c.mdsFresh
	x := &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(s.token), recovery: mdsCephFSRecoveryPlanFor(s.token), current: s.oracle.MDS, owned: map[string]uint64{}}
	c.mdsCephFS = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	var err error
	if x.baselineBlocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	sessions, _, err := c.mdsSessionsList(ctx)
	if err != nil {
		return err
	}
	if len(sessions) != 0 {
		return errors.New("MDS CephFS acceptance requires an owned filesystem without client sessions")
	}
	x.baselineReady = true
	// Record each identity name before the provider mutation creates it.
	x.goIntent = true
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	x.goClient, err = c.cluster.CreateClient(ctx, "mds-cephfs-"+s.token, mdsCephFSGoCaps(s.name))
	if err != nil {
		return fmt.Errorf("create dedicated MDS CephFS identity: %w", err)
	}
	_, keyring, err := x.goClient.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs.keyring"), keyring, 0600); err != nil {
		return err
	}
	x.nativeIntent = true
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	x.native, err = c.cluster.CreateClient(ctx, "mds-cephfs-native-"+s.token, mdsCephFSNativeCaps(s.name))
	if err != nil {
		return fmt.Errorf("create dedicated native CephFS oracle identity: %w", err)
	}
	_, nativeKeyring, err := x.native.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(nativeKeyring); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-native.keyring"), nativeKeyring, 0600); err != nil {
		return err
	}
	c.profile.mdsCephFSIdentity = "client.mds-cephfs-" + s.token
	x.recIntent = true
	if err := c.mdsCephFSJournal(); err != nil {
		return err
	}
	x.recClient, err = c.cluster.CreateClient(ctx, "mds-cephfs-recovery-"+s.token, mdsCephFSRecoveryCaps(s.name))
	if err != nil {
		return fmt.Errorf("create dedicated CephFS recovery identity: %w", err)
	}
	_, recoveryKeyring, err := x.recClient.ConnectionConfig()
	if err != nil {
		return err
	}
	recoveryKey, err := mdsSessionsKeyringSecret(recoveryKeyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-recovery.key"), []byte(recoveryKey+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-recovery.keyring"), recoveryKeyring, 0600); err != nil {
		return err
	}
	c.profile.mdsCephFSRecoveryIdentity = "client.mds-cephfs-recovery-" + s.token
	if err := errors.Join(c.mdsCephFSJournal(), c.mdsCephFSWriteTarget()); err != nil {
		return err
	}
	goCaps, nativeCaps, recoveryCaps := mdsCephFSGoCaps(s.name), mdsCephFSNativeCaps(s.name), mdsCephFSRecoveryCaps(s.name)
	return writeMDSFreshJSON(out, "mds-cephfs-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name,
		"mds_gid": s.oracle.MDS.GID, "client_identity": c.profile.mdsCephFSIdentity, "native_identity": "client.mds-cephfs-native-" + s.token,
		"recovery_identity": c.profile.mdsCephFSRecoveryIdentity,
		"client_caps":       map[string]string{"mon": goCaps.Mon, "mds": goCaps.MDS, "osd": goCaps.OSD},
		"native_caps":       map[string]string{"mon": nativeCaps.Mon, "mds": nativeCaps.MDS, "osd": nativeCaps.OSD},
		"recovery_caps":     map[string]string{"mon": recoveryCaps.Mon, "mds": recoveryCaps.MDS, "osd": recoveryCaps.OSD},
		"plan":              x.plan, "recovery_plan": x.recovery, "baseline_blocklist": x.baselineBlocklist,
		"data_pool": s.oracle.DataPools[0], "data_pool_name": s.name + "-data",
	})
}

// mdsCephFSRunNative runs the fixed native driver once. A failed, killed or
// truncated process is an error: its output is never trusted partially.
func (c *fixtureCluster) mdsCephFSRunNative(ctx context.Context, directory, action, path string, extra ...string) (*ceph.NativeCommandResult, mdsCephFSNative, error) {
	s, x := c.mdsFresh, c.mdsCephFS
	if directory != x.plan.Directory && directory != x.recovery.Directory {
		return nil, mdsCephFSNative{}, errors.New("native CephFS directory is not owned")
	}
	timeout := 30 * time.Second
	if action == "write" || action == "create-write" {
		timeout = 45 * time.Second
	}
	argv := append([]string{"python3", "-c", mdsCephFSNativePython, "client.mds-cephfs-native-" + s.token, "/out/mds-cephfs-native.keyring", s.name, directory, action, path}, extra...)
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: mdsCephFSJSONLimit, MaxStderr: 16 << 10, Label: "owned native CephFS " + action})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, mdsCephFSNative{}, errors.Join(err, fmt.Errorf("native CephFS %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	stdout := bytes.TrimSpace(process.Stdout)
	if directory == x.recovery.Directory {
		native, err := decodeMDSCephFSNativeOutput(action, path, stdout)
		if err == nil {
			native, err = decodeMDSCephFSRecoveryNative(action, path, x.recovery, native)
		}
		return &process, native, err
	}
	native, err := decodeMDSCephFSNative(action, path, x.plan, stdout)
	return &process, native, err
}

func (c *fixtureCluster) mdsCephFSControl(ctx context.Context, out string, fields []string) (resultErr error) {
	s, x := c.mdsFresh, c.mdsCephFS
	if c.profile.mdsCephFS != "1" || c.profile.mdsFresh != "1" || s == nil || s.fs == nil || x == nil || s.out != out || x.out != out {
		return errors.New("invalid owned MDS CephFS control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS CephFS control requested after cleanup began")
	}
	id, action, err := parseMDSCephFSRequest(fields, s.token, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := mdsCephFSResult{Source: mdsFreshSource, Token: s.token, RequestID: id, Action: action.name, Target: action.target, GlobalID: action.gid, FSID: s.oracle.FSID, FSCID: s.oracle.FSCID, FSName: s.name,
		MDSGID: x.current.GID, MDSEndpoint: x.current.Endpoint, MDSNonce: x.current.Nonce, MDSIncarnation: x.current.Incarnation}
	defer func() {
		result.ElapsedMillis = time.Since(started).Milliseconds()
		result.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if resultErr != nil {
			result.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-cephfs-result.json", result))
	}()
	checkIdentity := func() error {
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		owned := x.current
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MDS.GID != owned.GID || current.MDS.Endpoint != owned.Endpoint || current.MDS.Nonce != owned.Nonce ||
			current.MDS.Incarnation != owned.Incarnation || current.MDS.Name != owned.Name || current.MDS.Rank != 0 || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
			return errors.New("MDS CephFS native target changed during the owned action")
		}
		return nil
	}
	switch action.name {
	case "restart-fire", "restart-status":
		// Fire is time-critical and scoped by the armed GID; status reads a
		// transitional map. Neither can prove an active owned daemon.
		if err := c.mdsCephFSRestartAct(ctx, id, action, &result); err != nil {
			return err
		}
		result.Complete = true
		return nil
	case "restart-adopt":
		// Adoption proves the new incarnation itself, then the ordinary
		// post-action proof runs against it.
		if err := c.mdsCephFSRestartAct(ctx, id, action, &result); err != nil {
			return err
		}
	default:
		if x.restart.pending() {
			return errors.New("MDS CephFS action refused while the owned MDS restart is not adopted")
		}
		// Every action first proves the owned target, then repeats the proof.
		if err := checkIdentity(); err != nil {
			return err
		}
		if err := c.mdsCephFSAct(ctx, id, action, &result); err != nil {
			return err
		}
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	result.IdentityChecked = true
	current, err := c.mdsSessionsReadMap(ctx)
	if err != nil {
		return err
	}
	result.Map = &current
	data, err := c.mdsSessionsDaemon(ctx, "status", "--format", "json")
	if err != nil {
		return err
	}
	if result.DaemonEpoch, err = parseMDSSessionsDaemonEpoch(data); err != nil {
		return err
	}
	result.Complete = true
	return nil
}

func (c *fixtureCluster) mdsCephFSAct(ctx context.Context, id int64, action mdsCephFSAction, result *mdsCephFSResult) error {
	s, x := c.mdsFresh, c.mdsCephFS
	switch action.name {
	case "observe":
		sessions, _, err := c.mdsSessionsList(ctx)
		if err != nil {
			return err
		}
		result.Sessions = sessions
		for _, session := range sessions {
			if session.ID == action.gid && ownedGoAuthID(s.token, session.AuthID) && x.owned[session.Address] == 0 {
				// The test named this owned identity's GlobalID: its address
				// is owned evidence for cleanup of a later blocklist entry.
				x.owned[session.Address] = session.ID
				if err := c.mdsCephFSJournal(); err != nil {
					return err
				}
			}
		}
		// Observations also report every blocklist entry for the test.
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	case "evict":
		if err := c.mdsCephFSEvict(ctx, id, action.gid, result); err != nil {
			return err
		}
		var err error
		if result.Sessions, _, err = c.mdsSessionsList(ctx); err != nil {
			return err
		}
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	case "restart-arm":
		return c.mdsCephFSRestartAct(ctx, id, action, result)
	case "recovery-ops":
		return c.mdsCephFSHistoricOps(ctx, action.gid, result)
	case "recovery-stat", "recovery-list", "recovery-write":
		return c.mdsCephFSRecoveryNative(ctx, id, action, result)
	}
	path, err := x.plan.path(action.target)
	if err != nil {
		return err
	}
	native, extra := action.name, []string(nil)
	var mutation *mdsCephFSMutation
	switch action.name {
	case "getxattr":
		extra = []string{x.plan.XattrName}
	case "read":
		extra = []string{strconv.Itoa(len(x.plan.PayloadHex) / 2)}
	case "create-native", "write":
		kind, target := "native-create", strings.Join(x.plan.NativeFiles, ",")
		native, extra = "create", x.plan.NativeFiles
		if action.name == "write" {
			kind, target, native, extra = "native-write", x.plan.GoFile, "write", []string{x.plan.PayloadHex}
		}
		// One-shot: a mutation is never issued twice, whatever its outcome.
		if x.mutated(kind) {
			return fmt.Errorf("MDS CephFS %s was already issued", kind)
		}
		mutation = &mdsCephFSMutation{Kind: kind, RequestID: id, Target: target, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSJournal(); err != nil {
			return err
		}
	}
	process, output, err := c.mdsCephFSRunNative(ctx, x.plan.Directory, native, path, extra...)
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

type mdsCephFSCleanup struct {
	Source                  string                  `json:"source_commit"`
	Token                   string                  `json:"token"`
	FSName                  string                  `json:"fs_name"`
	Attempted               bool                    `json:"attempted"`
	Mutations               []*mdsCephFSMutation    `json:"mutations"`
	Restart                 *mdsCephFSRestart       `json:"restart,omitempty"`
	ResidualSessions        []mdsSessionsNative     `json:"residual_owned_sessions"`
	NamespaceChecked        bool                    `json:"namespace_checked"`
	NamespaceRestored       bool                    `json:"namespace_restored"`
	NamespaceNote           string                  `json:"namespace_note,omitempty"`
	Namespace               *mdsCephFSNative        `json:"namespace,omitempty"`
	RecoveryNamespace       *mdsCephFSNative        `json:"recovery_namespace,omitempty"`
	NewBlocklist            []mdsSessionsBlock      `json:"new_blocklist"`
	RemovedBlocklist        []mdsCephFSRemovedBlock `json:"removed_blocklist"`
	KeptBlocklist           []mdsSessionsBlock      `json:"kept_new_blocklist"`
	FinalBlocklist          []mdsSessionsBlock      `json:"final_blocklist,omitempty"`
	BlocklistRestored       bool                    `json:"blocklist_restored"`
	ClientIdentityRemoved   bool                    `json:"client_identity_removed"`
	NativeIdentityRemoved   bool                    `json:"native_identity_removed"`
	RecoveryIdentityRemoved bool                    `json:"recovery_identity_removed"`
	Completed               bool                    `json:"completed"`
	Errors                  []string                `json:"errors,omitempty"`
}

// finishMDSCephFS runs once, inside the cached MDS fresh cleanup and before
// the owned filesystem is removed. It mutates nothing but owned state: the
// three identities and new blocklist entries attributable to the owned
// eviction, the owned MDS restart or an observed owned Go session address.
// Residual namespace entries are reported and then discarded by the verified
// filesystem removal; no session is evicted.
func (c *fixtureCluster) finishMDSCephFS(ctx context.Context) error {
	s, x := c.mdsFresh, c.mdsCephFS
	if s == nil || x == nil {
		return nil
	}
	ctx, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	receipt := mdsCephFSCleanup{Source: mdsFreshSource, Token: s.token, FSName: s.name, Attempted: true, Mutations: x.mutations, Restart: x.restart, ResidualSessions: []mdsSessionsNative{},
		NewBlocklist: []mdsSessionsBlock{}, RemovedBlocklist: []mdsCephFSRemovedBlock{}, KeptBlocklist: []mdsSessionsBlock{}}
	if err := writeMDSFreshJSON(x.out, "mds-cephfs-cleanup.json", receipt); err != nil {
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	nativeName := "mds-cephfs-native-" + s.token
	if x.baselineReady {
		sessions, _, err := c.mdsSessionsList(ctx)
		fail(err)
		goPresent := false
		for _, session := range sessions {
			if ownedGoAuthID(s.token, session.AuthID) || session.AuthID == nativeName {
				receipt.ResidualSessions = append(receipt.ResidualSessions, session)
				goPresent = goPresent || ownedGoAuthID(s.token, session.AuthID)
			}
		}
		switch {
		case err != nil:
			receipt.NamespaceNote = "native session listing failed; namespace readback skipped"
		case x.native == nil:
			receipt.NamespaceNote = "native identity was never created; no native namespace mutation was possible"
			receipt.NamespaceChecked, receipt.NamespaceRestored = true, len(x.mutations) == 0
		case goPresent:
			// A native mount could wait on capability revocations from a Go
			// session whose process is gone. The filesystem removal discards it.
			receipt.NamespaceNote = "Go session still present; namespace readback skipped and residual entries are discarded with the owned filesystem"
		default:
			path, _ := x.plan.path("dir")
			_, native, err := c.mdsCephFSRunNative(ctx, x.plan.Directory, "stat", path)
			fail(err)
			recoveryPath, _ := x.recovery.path("dir")
			_, recovery, recoveryErr := c.mdsCephFSRunNative(ctx, x.recovery.Directory, "stat", recoveryPath)
			fail(recoveryErr)
			if err == nil && recoveryErr == nil {
				receipt.Namespace, receipt.RecoveryNamespace, receipt.NamespaceChecked = &native, &recovery, true
				receipt.NamespaceRestored = !native.Stat.Exists && !recovery.Stat.Exists
				if !receipt.NamespaceRestored {
					receipt.NamespaceNote = "an owned directory remained; it is discarded with the owned filesystem"
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
			c.mdsCephFSRestoreBlocklist(ctx, &receipt, fail)
		}
	} else {
		receipt.NamespaceChecked, receipt.NamespaceRestored, receipt.BlocklistRestored = true, true, true
		receipt.NamespaceNote = "preparation ended before any owned namespace mutation was possible"
	}
	remove := func(config *ceph.ClientConfig, intent bool, label string) bool {
		if config == nil {
			if intent {
				fail(fmt.Errorf("MDS CephFS %s identity creation outcome is unknown", label))
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
	receipt.RecoveryIdentityRemoved = remove(x.recClient, x.recIntent, "recovery")
	for _, err := range errs {
		receipt.Errors = append(receipt.Errors, err.Error())
	}
	receipt.Completed = len(errs) == 0 && receipt.NamespaceChecked && receipt.BlocklistRestored && receipt.ClientIdentityRemoved && receipt.NativeIdentityRemoved && receipt.RecoveryIdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("MDS CephFS cleanup did not verify every check"))
	}
	return errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "mds-cephfs-cleanup.json", receipt))
}
