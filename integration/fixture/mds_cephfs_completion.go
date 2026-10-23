package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// The CephFS completion acceptance drives the request-completion and
// reconnect edge cases of one owned rank-0 MDS: a lost reply of a delivered
// request, a REPLAY answered from the completed-request record, a denied
// reconnect, a missed reconnect window, a forced read-only filesystem, a
// dentry lease RENEW and a cap snap flush. It owns one directory, two
// identities, runtime-only daemon settings changed through the MDS admin
// socket (ms_blackhole_client, mds_early_reply, mds_log_pause, debug levels),
// two central settings of the owned daemon's section (mds_deny_all_reconnect,
// mds_reconnect_timeout), at most four restarts of the owned daemon, one
// native create+write, one snapshot and evictions of owned Go sessions.
// Every MDS daemon command goes through the admin socket in the daemon's own
// container: ms_blackhole_client also drops MCommand traffic of `ceph tell`,
// which is a client connection. Native behavior relied upon:
// msg/Connection.cc (is_blackhole), msg/async/ProtocolV2.cc and
// AsyncConnection.cc (blackholed receive and send), mds/MDLog.cc
// (mds_log_pause), mds/Server.cc (early_reply, journal_and_reply,
// handle_client_request completed-request path, dispatch during reconnect,
// handle_client_reconnect, reconnect_tick, force_clients_readonly),
// mds/MDCache.cc (force_readonly), mds/Locker.cc (handle_client_lease,
// handle_client_caps FLUSHSNAP), mds/MDSRank.cc (admin socket commands,
// evict_client), mds/SessionMap.cc (Session::dump completed_requests),
// mon/MDSMonitor.cc (fail_mds_gid) and mds/MDSDaemon.cc (respawn) at
// mdsFreshSource (LGPL-2.1 references; no code copied).

const (
	mdsCephFSCompletionJSONLimit    = 4 << 20
	mdsCephFSCompletionMaxRestarts  = 4
	mdsCephFSCompletionFencedBytes  = 5000
	mdsCephFSCompletionLogLines     = 600
	mdsCephFSCompletionLogLineBytes = 2048
	mdsCephFSCompletionLogBytes     = 256 << 20
	mdsCephFSCompletionWindow       = "5"
)

// mdsCephFSCompletionChildren are the closed names directly below the owned
// directory; the grandchildren are lease/leased and snap/snap-file.
var mdsCephFSCompletionChildren = []string{"lease", "replayed", "lost-file", "lost-dir", "fenced", "ro-file", "ro-dir", "snap"}
var mdsCephFSCompletionGrandchildren = []string{"lease/leased", "snap/snap-file"}

// mdsCephFSCompletionConfigNames is the closed set of daemon options read back
// through the admin socket.
var mdsCephFSCompletionConfigNames = []string{"ms_blackhole_client", "mds_early_reply", "mds_log_pause", "debug_mds", "debug_ms", "mds_deny_all_reconnect",
	"mds_reconnect_timeout", "mds_session_blocklist_on_timeout", "mds_session_blocklist_on_evict"}

// mdsCephFSCompletionRuntimeKinds maps each runtime-only change to its daemon
// settings and the action that restores it.
var mdsCephFSCompletionRuntimeKinds = map[string]struct {
	settings map[string]string
	restore  string
}{
	"log-pause":       {map[string]string{"mds_log_pause": "true"}, "log-resume"},
	"early-reply-off": {map[string]string{"mds_early_reply": "false"}, "early-reply-on"},
	"blackhole-on":    {map[string]string{"ms_blackhole_client": "true"}, "blackhole-off"},
	"debug-on":        {map[string]string{"debug_mds": "10/10", "debug_ms": "1/5"}, "debug-off"},
}

// mdsCephFSCompletionSeconds reads a float option value ("45.000000"); an
// unreadable value is 0.
func mdsCephFSCompletionSeconds(value string) float64 {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return seconds
}

func mdsCephFSCompletionRestoreOf(action string) string {
	for kind, spec := range mdsCephFSCompletionRuntimeKinds {
		if spec.restore == action {
			return kind
		}
	}
	return ""
}

type mdsCephFSCompletionPlan struct {
	Directory        string   `json:"directory"`
	Children         []string `json:"children"`
	Grandchildren    []string `json:"grandchildren"`
	Snapshot         string   `json:"snapshot"`
	FencedPayloadHex string   `json:"fenced_payload_hex"`
	ReconnectTimeout string   `json:"reconnect_timeout"`
	SplitFiles       []string `json:"split_files"`
}

func mdsCephFSCompletionPlanFor(token string) mdsCephFSCompletionPlan {
	var payload []byte
	block := sha256.Sum256([]byte("ceph-msgr-cephfs-completion-fenced:" + token))
	for len(payload) < mdsCephFSCompletionFencedBytes {
		payload = append(payload, block[:]...)
		block = sha256.Sum256(block[:])
	}
	return mdsCephFSCompletionPlan{Directory: "ceph-msgr-completion-" + token, Children: slices.Clone(mdsCephFSCompletionChildren), Grandchildren: slices.Clone(mdsCephFSCompletionGrandchildren),
		Snapshot: "completion-" + token[:8], FencedPayloadHex: hex.EncodeToString(payload[:mdsCephFSCompletionFencedBytes]), ReconnectTimeout: mdsCephFSCompletionWindow, SplitFiles: mdsCephFSMultiReconnectNames()}
}

// mdsCephFSMultiReconnectNames are actual owned files whose retained cap state
// exceeds an 8 KiB fragment budget. Long legal names exercise real short paths; no fake
// inode, cap or lock data is submitted to inflate reconnect state.
func mdsCephFSMultiReconnectNames() []string {
	names := make([]string, 32)
	for i := range names {
		names[i] = fmt.Sprintf("split-%03d-%s", i, strings.Repeat("s", 215))
	}
	return names
}

// targets lists every closed stat target.
func (p mdsCephFSCompletionPlan) targets() []string {
	return append(append(append(append([]string{"dir"}, p.Children...), p.SplitFiles...), p.Grandchildren...), "snapshot")
}

// path maps a closed target to its absolute path in the owned filesystem.
func (p mdsCephFSCompletionPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-completion-") || strings.ContainsAny(p.Directory, "/. \x00") || len(p.Snapshot) == 0 || strings.ContainsAny(p.Snapshot, "/. \x00") {
		return "", errors.New("MDS CephFS completion directory is not owned")
	}
	root := "/" + p.Directory
	switch {
	case target == "dir":
		return root, nil
	case target == "snapshot":
		return root + "/snap/.snap/" + p.Snapshot, nil
	case slices.Contains(p.Children, target), slices.Contains(p.SplitFiles, target), slices.Contains(p.Grandchildren, target):
		return root + "/" + target, nil
	}
	return "", errors.New("MDS CephFS completion target is not admitted")
}

func mdsCephFSCompletionGoCaps(fsName string) ceph.ClientCaps {
	// CephFS sessions and explicit OSD reads of the owned data pool; no
	// object writes and no MGR.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName, OSD: "allow r tag cephfs data=" + fsName}
}

func mdsCephFSCompletionNativeCaps(fsName string) ceph.ClientCaps {
	// The native oracle writes one file's data and creates and removes one
	// snapshot (`s`, MAY_SNAPSHOT).
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rws fsname=" + fsName, OSD: "allow rw tag cephfs data=" + fsName}
}

// One fixed native LibCephFS driver. It admits the owned directory, its
// children and grandchildren, and the one planned snapshot of `snap`.
// Source: pybind/cephfs/cephfs.pyx at mdsFreshSource; independent driver.
const mdsCephFSCompletionNativePython = `
import cephfs, json, os, sys
identity, fsname, directory, snapshot, action, path = sys.argv[1:7]
extra = sys.argv[7:]
def owned(p):
    parts = p.split('/')
    if not directory.startswith('ceph-msgr-completion-') or any(c in directory for c in '/. '):
        return False
    if len(parts) < 2 or parts[0] != '' or parts[1] != directory or any(c in ('', '.', '..') for c in parts[1:]):
        return False
    if '.snap' in parts:
        return parts == ['', directory, 'snap', '.snap', snapshot]
    return len(parts) <= 4
if not owned(path):
    raise ValueError('path outside the owned directory')
snap = '.snap' in path.split('/')
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
    elif action == 'list' and path == '/' + directory and not extra:
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
    elif action == 'create-write' and not snap and len(extra) == 1:
        data = bytes.fromhex(extra[0])
        fd = fs.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        out['written'] = fs.write(fd, data, 0)
        fs.fsync(fd, 0)
        fs.close(fd)
        out['stat'] = stat(path)
    elif action in ('mksnap', 'rmsnap') and snap and not extra:
        if action == 'mksnap':
            fs.mkdir(path, 0o755)
        else:
            fs.rmdir(path)
        out['stat'] = stat(path)
    else:
        raise ValueError('native CephFS completion action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSCompletionNative accepts exactly the shape of one action.
func decodeMDSCephFSCompletionNative(action, path string, plan mdsCephFSCompletionPlan, data []byte) (mdsCephFSNative, error) {
	result, err := decodeMDSCephFSNativeOutput(action, path, data)
	if err != nil {
		return result, err
	}
	ok := false
	switch action {
	case "stat":
		ok = result.Stat != nil && validMDSCephFSStat(*result.Stat) && result.Entries == nil && result.Written == 0
	case "list":
		ok = len(result.Entries) <= len(plan.Children)+len(plan.SplitFiles) && result.Stat == nil
		seen := map[string]bool{}
		for _, entry := range result.Entries {
			ok = ok && (slices.Contains(plan.Children, entry.Name) || slices.Contains(plan.SplitFiles, entry.Name)) && !seen[entry.Name] && entry.DIno != 0 && validMDSCephFSStat(entry.Stat) && entry.Stat.Exists
			seen[entry.Name] = true
		}
	case "create-write":
		ok = result.Written == len(plan.FencedPayloadHex)/2 && result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.Stat.Size == uint64(result.Written)
	case "mksnap":
		ok = result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat)
	case "rmsnap":
		ok = result.Stat != nil && !result.Stat.Exists && validMDSCephFSStat(*result.Stat)
	}
	if !ok {
		return mdsCephFSNative{}, fmt.Errorf("native CephFS completion %s output shape differs", action)
	}
	return result, nil
}

// mdsCephFSCompletionRuntime is one runtime-only change of the daemon,
// applied through its admin socket. It never reaches the central database
// and does not survive a daemon restart.
type mdsCephFSCompletionRuntime struct {
	Kind             string            `json:"kind"`
	Settings         map[string]string `json:"settings"`
	Baseline         map[string]string `json:"baseline"`
	IncarnationGID   uint64            `json:"incarnation_gid"`
	RequestID        int64             `json:"request_id"`
	State            string            `json:"state"` // intended, applied, uncertain, restore-intended, restored, restore-uncertain, discarded-by-restart
	Readback         map[string]string `json:"readback,omitempty"`
	RestoreRequestID int64             `json:"restore_request_id,omitempty"`
	RestoreReadback  map[string]string `json:"restore_readback,omitempty"`
	Error            string            `json:"error,omitempty"`
}

func (r *mdsCephFSCompletionRuntime) active() bool {
	return r.State == "intended" || r.State == "applied" || r.State == "uncertain" || r.State == "restore-intended" || r.State == "restore-uncertain"
}

// mdsCephFSCompletionCentral is one owned central-database override of the
// owned daemon's section; it is applied and restored once.
type mdsCephFSCompletionCentral struct {
	Kind      string `json:"kind"`
	Section   string `json:"section"`
	Name      string `json:"name"`
	Value     string `json:"value"`
	RequestID int64  `json:"request_id"`
	// State: intended, applied, uncertain, restore-intended, restored, restore-uncertain.
	State            string `json:"state"`
	DaemonValue      string `json:"daemon_value,omitempty"`
	RestoreRequestID int64  `json:"restore_request_id,omitempty"`
	RestoredDaemon   string `json:"restored_daemon_value,omitempty"`
	Error            string `json:"error,omitempty"`
	handle           *ceph.ConfigOverride
}

var mdsCephFSCompletionCentralKinds = map[string]struct{ kind, name, value string }{
	"deny-on":   {"deny", "mds_deny_all_reconnect", "true"},
	"window-on": {"window", "mds_reconnect_timeout", mdsCephFSCompletionWindow},
}

type mdsCephFSCompletionDaemonStatus struct {
	GID         uint64 `json:"gid"`
	Rank        int32  `json:"whoami"`
	State       string `json:"state"`
	MDSMapEpoch uint32 `json:"mdsmap_epoch"`
	FSName      string `json:"fs_name"`
}

func parseMDSCephFSCompletionDaemonStatus(data []byte) (mdsCephFSCompletionDaemonStatus, error) {
	var native struct {
		ID     *uint64 `json:"id"`
		WhoAmI *int32  `json:"whoami"`
		State  string  `json:"state"`
		Epoch  *uint32 `json:"mdsmap_epoch"`
		FSName string  `json:"fs_name"`
	}
	if len(data) == 0 || len(data) > 64<<10 || json.Unmarshal(data, &native) != nil || native.ID == nil || *native.ID == 0 || native.WhoAmI == nil || native.Epoch == nil ||
		!strings.HasPrefix(native.State, "up:") {
		return mdsCephFSCompletionDaemonStatus{}, errors.New("MDS completion daemon status differs")
	}
	return mdsCephFSCompletionDaemonStatus{GID: *native.ID, Rank: *native.WhoAmI, State: native.State, MDSMapEpoch: *native.Epoch, FSName: native.FSName}, nil
}

func parseMDSCephFSCompletionConfig(data []byte, name string) (string, error) {
	var values map[string]string
	if len(data) == 0 || len(data) > 4096 || json.Unmarshal(data, &values) != nil || len(values) != 1 {
		return "", errors.New("MDS completion daemon configuration readback differs")
	}
	value, ok := values[name]
	if !ok || value == "" || len(value) > 64 {
		return "", errors.New("MDS completion daemon configuration value differs")
	}
	return value, nil
}

// mdsCephFSCompletionCompleted is the completed-request record of one owned
// session as `session ls` dumps it (Session::dump / session_info_t::dump).
type mdsCephFSCompletionCompleted struct {
	GlobalID         uint64                       `json:"global_id"`
	State            string                       `json:"state"`
	Reconnecting     bool                         `json:"reconnecting"`
	RequestsInFlight uint64                       `json:"requests_in_flight"`
	NumCompleted     uint64                       `json:"num_completed_requests"`
	Requests         []mdsCephFSCompletionRequest `json:"completed_requests"`
}

type mdsCephFSCompletionRequest struct {
	TID        uint64 `json:"tid"`
	CreatedIno string `json:"created_ino"`
}

// parseMDSCephFSCompletionSessions keeps the completed-request records of the
// named GlobalIDs only.
func parseMDSCephFSCompletionSessions(data []byte, gids []uint64) ([]mdsCephFSCompletionCompleted, error) {
	var native []struct {
		ID               *uint64 `json:"id"`
		State            string  `json:"state"`
		Reconnecting     bool    `json:"reconnecting"`
		RequestsInFlight uint64  `json:"requests_in_flight"`
		NumCompleted     *uint64 `json:"num_completed_requests"`
		Completed        []struct {
			TID     *uint64 `json:"tid"`
			Created string  `json:"created_ino"`
		} `json:"completed_requests"`
	}
	if len(data) == 0 || len(data) > mdsCephFSCompletionJSONLimit || json.Unmarshal(data, &native) != nil || native == nil {
		return nil, errors.New("MDS completion session listing shape differs")
	}
	result := []mdsCephFSCompletionCompleted{}
	for _, n := range native {
		if n.ID == nil || !slices.Contains(gids, *n.ID) {
			continue
		}
		if n.NumCompleted == nil || len(n.Completed) > 4096 || uint64(len(n.Completed)) != *n.NumCompleted {
			return nil, errors.New("MDS completion completed-request record differs from its count")
		}
		c := mdsCephFSCompletionCompleted{GlobalID: *n.ID, State: n.State, Reconnecting: n.Reconnecting, RequestsInFlight: n.RequestsInFlight, NumCompleted: *n.NumCompleted,
			Requests: []mdsCephFSCompletionRequest{}}
		for _, r := range n.Completed {
			if r.TID == nil || *r.TID == 0 || !strings.HasPrefix(r.Created, "0x") || len(r.Created) > 18 {
				return nil, errors.New("MDS completion completed-request entry differs")
			}
			c.Requests = append(c.Requests, mdsCephFSCompletionRequest{TID: *r.TID, CreatedIno: r.Created})
		}
		result = append(result, c)
	}
	return result, nil
}

// mdsCephFSCompletionLogMarkers select daemon log lines that carry evidence
// for the acceptance but not the client GlobalID or address nonce.
var mdsCephFSCompletionLogMarkers = []string{
	"convert it to lookup request", "queuing replayed op", "queuing completed op",
	"force file system read-only", "force_clients_readonly", "flushsnap",
	"mds_deny_all_reconnect was set", "no longer in reconnect state", "reconnect timed out", "reconnect gives up on",
	"reconnect will complete once clients are evicted", "denied reconnect attempt", "evicting unresponsive client",
	"ignoring msg from not-open session", "ignoring sessionless msg",
}

// filterMDSCephFSCompletionLog keeps the lines of one client: its GlobalID,
// its address nonce on a client message, a blackholed client message or a
// closed marker. Lines are truncated; the result is bounded.
func filterMDSCephFSCompletionLog(lines []string, gid uint64, nonce uint32) ([]string, int) {
	client := "client." + strconv.FormatUint(gid, 10)
	address := ":0/" + strconv.FormatUint(uint64(nonce), 10) + " "
	messages := []string{"client_reply(", "client_caps(", "client_lease(", "client_session(", "client_snap(", "client_request(", "client_reconnect("}
	kept, dropped := []string{}, 0
	for _, line := range lines {
		match := false
		if i := strings.Index(line, client); i >= 0 {
			rest := line[i+len(client):]
			match = rest == "" || strings.ContainsRune(":) ,]", rune(rest[0]))
		}
		hasMessage := slices.ContainsFunc(messages, func(m string) bool { return strings.Contains(line, m) })
		match = match || strings.Contains(line, " blackhole ") && hasMessage || strings.Contains(line, address) && hasMessage ||
			slices.ContainsFunc(mdsCephFSCompletionLogMarkers, func(m string) bool { return strings.Contains(line, m) })
		if !match {
			continue
		}
		if len(kept) == mdsCephFSCompletionLogLines {
			dropped++
			continue
		}
		if len(line) > mdsCephFSCompletionLogLineBytes {
			line = line[:mdsCephFSCompletionLogLineBytes]
		}
		kept = append(kept, line)
	}
	return kept, dropped
}

// filterMDSCephFSReconnectHandlers preserves source order and exact client
// attribution. Handler lines directly record native MClientReconnect.more.
func filterMDSCephFSReconnectHandlers(lines []string, gid uint64) []string {
	prefix := "handle_client_reconnect client." + strconv.FormatUint(gid, 10)
	var result []string
	for _, line := range lines {
		at := strings.Index(line, prefix)
		if at < 0 {
			continue
		}
		tail := strings.TrimSpace(line[at+len(prefix):])
		if tail == "" || tail == "(more)" {
			result = append(result, line)
		}
	}
	return result
}

type mdsCephFSCompletionLog struct {
	Mark     int      `json:"mark"`
	Total    int      `json:"total_lines"`
	Lines    []string `json:"lines"`
	Dropped  int      `json:"dropped"`
	GlobalID uint64   `json:"global_id"`
	Nonce    uint32   `json:"nonce"`
}

type mdsCephFSCompletionScope struct {
	out                    string
	plan                   mdsCephFSCompletionPlan
	goClient, native       *ceph.ClientConfig
	goIntent, nativeIntent bool
	baselineReady          bool
	baselineBlocklist      []mdsSessionsBlock
	baselineConfig         map[string]string
	lastRequestID          int64
	mutations              []*mdsCephFSMutation
	runtime                []*mdsCephFSCompletionRuntime
	central                []*mdsCephFSCompletionCentral
	restarts               []*mdsCephFSRestart
	current                mdsFreshDaemon
	owned                  map[string]uint64
	logMark                int
}

func (x *mdsCephFSCompletionScope) mutated(kind string) bool {
	return slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == kind })
}

func (x *mdsCephFSCompletionScope) pending() *mdsCephFSRestart {
	if n := len(x.restarts); n != 0 && x.restarts[n-1].pending() {
		return x.restarts[n-1]
	}
	return nil
}

// activeRuntime returns the runtime change of one kind that may still be in
// effect on the daemon incarnation gid.
func (x *mdsCephFSCompletionScope) activeRuntime(kind string, gid uint64) *mdsCephFSCompletionRuntime {
	for _, r := range x.runtime {
		if r.Kind == kind && r.IncarnationGID == gid && r.active() {
			return r
		}
	}
	return nil
}

// quiesced reports whether native clients and journaled session changes can
// proceed: no blackhole and no paused journal may be in effect.
func (x *mdsCephFSCompletionScope) quiesced(gid uint64) error {
	if x.activeRuntime("blackhole-on", gid) != nil || x.activeRuntime("log-pause", gid) != nil {
		return errors.New("MDS CephFS completion native or journaled action refused while client messages are blackholed or the journal is paused")
	}
	return nil
}

type mdsCephFSCompletionAction struct {
	name, target string
	gid          uint64
	nonce        uint32
}

// parseMDSCephFSCompletionRequest admits <id> <token> <action> [arguments].
func parseMDSCephFSCompletionRequest(fields []string, token string, plan mdsCephFSCompletionPlan, previousID int64) (int64, mdsCephFSCompletionAction, error) {
	var action mdsCephFSCompletionAction
	if len(fields) < 3 || len(fields) > 5 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned MDS CephFS completion request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, action, errors.New("invalid owned MDS CephFS completion request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid MDS CephFS completion request identity")
	}
	action.name = fields[2]
	args := fields[3:]
	gid := func(arg string) (uint64, error) {
		value, err := strconv.ParseUint(arg, 10, 64)
		if err != nil || value == 0 || strconv.FormatUint(value, 10) != arg {
			return 0, errors.New("MDS CephFS completion action requires one canonical GlobalID")
		}
		return value, nil
	}
	switch action.name {
	case "observe", "evict", "ops-in-flight", "historic-ops":
		if len(args) != 1 {
			return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion session action takes one GlobalID")
		}
		if action.gid, err = gid(args[0]); err != nil {
			return 0, mdsCephFSCompletionAction{}, err
		}
	case "mds-log", "reconnect-log":
		if len(args) != 2 {
			return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion log action takes a GlobalID and an address nonce")
		}
		if action.gid, err = gid(args[0]); err != nil {
			return 0, mdsCephFSCompletionAction{}, err
		}
		nonce, err := strconv.ParseUint(args[1], 10, 32)
		if err != nil || nonce == 0 || strconv.FormatUint(nonce, 10) != args[1] {
			return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion log nonce is not canonical")
		}
		action.nonce = uint32(nonce)
	case "stat":
		if len(args) != 1 || !slices.Contains(plan.targets(), args[0]) {
			return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion stat target is not admitted")
		}
		action.target = args[0]
	case "list", "write-fenced", "snapshot-create", "snapshot-remove", "mds-log-mark", "config-read", "flush-journal", "force-readonly",
		"log-pause", "log-resume", "early-reply-off", "early-reply-on", "blackhole-on", "blackhole-off", "debug-on", "debug-off",
		"deny-on", "deny-off", "window-on", "window-off", "restart-arm", "restart-fire", "restart-status", "restart-adopt":
		if len(args) != 0 {
			return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion action takes no caller argument")
		}
		switch action.name {
		case "list":
			action.target = "dir"
		case "write-fenced":
			action.target = "fenced"
		case "snapshot-create", "snapshot-remove":
			action.target = "snapshot"
		}
	default:
		return 0, mdsCephFSCompletionAction{}, errors.New("MDS CephFS completion action is not admitted")
	}
	return id, action, nil
}

type mdsCephFSCompletionFlush struct {
	ReturnCode int64  `json:"return_code"`
	Message    string `json:"message"`
}

type mdsCephFSCompletionResult struct {
	Source          string                           `json:"source_commit"`
	Token           string                           `json:"token"`
	RequestID       int64                            `json:"request_id"`
	Action          string                           `json:"action"`
	Target          string                           `json:"target,omitempty"`
	GlobalID        uint64                           `json:"global_id,omitempty"`
	FSID            string                           `json:"fsid"`
	FSCID           int64                            `json:"fscid"`
	FSName          string                           `json:"fs_name"`
	MDSGID          uint64                           `json:"mds_gid"`
	MDSEndpoint     string                           `json:"mds_endpoint"`
	MDSNonce        uint32                           `json:"mds_nonce"`
	MDSIncarnation  int32                            `json:"mds_incarnation"`
	Rank            int32                            `json:"rank"`
	IdentityChecked bool                             `json:"identity_checked"`
	Map             *mdsSessionsMap                  `json:"map,omitempty"`
	Daemon          *mdsCephFSCompletionDaemonStatus `json:"daemon,omitempty"`
	Sessions        []mdsSessionsNative              `json:"sessions"`
	Completed       []mdsCephFSCompletionCompleted   `json:"completed"`
	Blocklist       []mdsSessionsBlock               `json:"blocklist,omitempty"`
	Native          *mdsCephFSNative                 `json:"native,omitempty"`
	Process         *ceph.NativeCommandResult        `json:"native_process,omitempty"`
	Mutation        *mdsCephFSMutation               `json:"mutation,omitempty"`
	Runtime         *mdsCephFSCompletionRuntime      `json:"runtime,omitempty"`
	Central         *mdsCephFSCompletionCentral      `json:"central,omitempty"`
	Config          map[string]string                `json:"config,omitempty"`
	Restart         *mdsCephFSRestart                `json:"restart,omitempty"`
	RankStatus      *mdsCephFSRankStatus             `json:"rank_status,omitempty"`
	Ops             []mdsCephFSHistoricOp            `json:"ops,omitempty"`
	FlushJournal    *mdsCephFSCompletionFlush        `json:"flush_journal,omitempty"`
	Log             *mdsCephFSCompletionLog          `json:"log,omitempty"`
	Complete        bool                             `json:"complete"`
	Error           string                           `json:"error,omitempty"`
	ElapsedMillis   int64                            `json:"elapsed_ms"`
	ObservedUTC     string                           `json:"observed_utc"`
}

func (c *fixtureCluster) mdsCephFSCompletionJournal() error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	identity, native := "", ""
	if x.goIntent {
		identity = "client.mds-cephfs-completion-" + s.token
	}
	if x.nativeIntent {
		native = "client.mds-cephfs-completion-native-" + s.token
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-completion-owned.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name, "fscid": s.oracle.FSCID,
		"client_identity": identity, "native_identity": native, "plan": x.plan, "baseline_blocklist": x.baselineBlocklist, "baseline_config": x.baselineConfig,
		"mutations": x.mutations, "runtime": x.runtime, "central": x.central, "restarts": x.restarts, "current_mds": x.current, "owned_session_addresses": x.owned,
	})
}

func (c *fixtureCluster) mdsCephFSCompletionWriteTarget() error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	adopted := 0
	for _, r := range x.restarts {
		if r.State == "adopted" {
			adopted++
		}
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-completion-target.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name, "mds": x.current, "adopted_restarts": adopted,
	})
}

// mdsCephFSCompletionDaemon returns the owned daemon's container. Its name
// stays the same across restarts: the daemon respawns in place.
func (c *fixtureCluster) mdsCephFSCompletionDaemon() (*cephfs.MDS, error) {
	x := c.mdsCephFSCompletion
	for _, daemon := range c.mdsFresh.fs.MDSs() {
		if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" && daemon.ID == x.current.Name {
			return daemon, nil
		}
	}
	return nil, errors.New("owned MDS CephFS completion daemon container is unavailable")
}

// mdsCephFSCompletionAsok runs one admin socket command in the owned daemon's
// container. It does not use the Messenger.
func (c *fixtureCluster) mdsCephFSCompletionAsok(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	daemon, err := c.mdsCephFSCompletionDaemon()
	if err != nil {
		return nil, err
	}
	argv := append([]string{"ceph", "--admin-daemon", "/var/run/ceph/ceph-mds." + daemon.ID + ".asok"}, args...)
	process, err := c.cluster.RunNativeCommand(ctx, daemon.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: mdsCephFSCompletionJSONLimit, MaxStderr: 16 << 10,
		Label: "owned MDS admin socket " + args[0]})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return nil, errors.Join(err, fmt.Errorf("owned MDS admin socket %s did not complete (exit %d, timed out %v)", args[0], process.ExitCode, process.TimedOut))
	}
	return bytes.TrimSpace(process.Stdout), nil
}

func (c *fixtureCluster) mdsCephFSCompletionStatus(ctx context.Context) (mdsCephFSCompletionDaemonStatus, error) {
	data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "status")
	if err != nil {
		return mdsCephFSCompletionDaemonStatus{}, err
	}
	return parseMDSCephFSCompletionDaemonStatus(data)
}

func (c *fixtureCluster) mdsCephFSCompletionConfig(ctx context.Context) (map[string]string, error) {
	values := map[string]string{}
	for _, name := range mdsCephFSCompletionConfigNames {
		data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "config", "get", name)
		if err != nil {
			return values, err
		}
		if values[name], err = parseMDSCephFSCompletionConfig(data, name); err != nil {
			return values, err
		}
	}
	return values, nil
}

func (c *fixtureCluster) mdsCephFSCompletionSessions(ctx context.Context, gids ...uint64) ([]mdsSessionsNative, []mdsCephFSCompletionCompleted, error) {
	data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "session", "ls")
	if err != nil {
		return nil, nil, err
	}
	sessions, err := parseMDSSessionsList(data)
	if err != nil {
		return nil, nil, err
	}
	completed, err := parseMDSCephFSCompletionSessions(data, gids)
	return sessions, completed, err
}

// mdsCephFSCompletionLogLines reads the owned daemon's container output (its
// log) line by line, bounded.
func (c *fixtureCluster) mdsCephFSCompletionLogLines(ctx context.Context) ([]string, error) {
	daemon, err := c.mdsCephFSCompletionDaemon()
	if err != nil {
		return nil, err
	}
	reader, err := daemon.Logs(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	buffered := bufio.NewReaderSize(io.LimitReader(reader, mdsCephFSCompletionLogBytes), 64<<10)
	var lines []string
	for {
		line, err := buffered.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if len(line) > 4*mdsCephFSCompletionLogLineBytes {
				line = line[:4*mdsCephFSCompletionLogLineBytes]
			}
			lines = append(lines, line)
		}
		if errors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (c *fixtureCluster) mdsCephFSCompletionRankStatus(ctx context.Context, armed uint64) (mdsCephFSRankStatus, error) {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
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
	status, err := parseMDSCephFSRankStatus(data, s.oracle, c.profile.ipFamily, func(name string) bool { return owned[name] }, armed)
	if err != nil {
		return status, err
	}
	status.ArmedBlocklist = []mdsSessionsBlock{}
	if r := x.pending(); r != nil {
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return status, err
		}
		for _, address := range r.ArmedAddresses {
			if entry := mdsSessionsFindBlock(blocks, address); entry != nil {
				status.ArmedBlocklist = append(status.ArmedBlocklist, *entry)
			}
		}
	}
	return status, nil
}

func (c *fixtureCluster) prepareMDSCephFSCompletion(ctx context.Context, out string) error {
	p := c.profile
	if p.mdsCephFSCompletion != "1" || p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSRanks == "1" || p.osdProfile != "legacy-v6" ||
		c.mdsFresh == nil || c.mdsFresh.fs == nil || c.mdsCephFSCompletion != nil {
		return errors.New("invalid MDS CephFS completion preparation scope")
	}
	s := c.mdsFresh
	x := &mdsCephFSCompletionScope{out: out, plan: mdsCephFSCompletionPlanFor(s.token), current: s.oracle.MDS, owned: map[string]uint64{}}
	c.mdsCephFSCompletion = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	var err error
	if x.baselineBlocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	sessions, _, err := c.mdsCephFSCompletionSessions(ctx)
	if err != nil {
		return err
	}
	if len(sessions) != 0 {
		return errors.New("MDS CephFS completion acceptance requires an owned filesystem without client sessions")
	}
	if x.baselineConfig, err = c.mdsCephFSCompletionConfig(ctx); err != nil {
		return err
	}
	b := x.baselineConfig
	if b["ms_blackhole_client"] != "false" || b["mds_early_reply"] != "true" || b["mds_log_pause"] != "false" || b["mds_deny_all_reconnect"] != "false" ||
		b["mds_session_blocklist_on_timeout"] != "true" || b["mds_session_blocklist_on_evict"] != "true" || mdsCephFSCompletionSeconds(b["mds_reconnect_timeout"]) <= mdsCephFSCompletionSeconds(mdsCephFSCompletionWindow) {
		return errors.New("MDS CephFS completion acceptance refuses to adopt non-default daemon settings")
	}
	entries, err := c.cluster.Configuration(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name == "mds_deny_all_reconnect" || entry.Name == "mds_reconnect_timeout" || entry.Name == "ms_blackhole_client" || entry.Name == "mds_log_pause" || entry.Name == "mds_early_reply" {
			return errors.New("MDS CephFS completion acceptance refuses to adopt an existing central entry of a setting it changes")
		}
	}
	data, err := c.cluster.Ceph(ctx, "fs", "get", s.name, "--format", "json")
	if err != nil {
		return err
	}
	if allow, err := parseMDSCephFSAllowSnaps(data, s.oracle.FSCID, s.name); err != nil || !allow {
		return errors.Join(err, errors.New("MDS CephFS completion acceptance requires a filesystem that already allows snapshots"))
	}
	x.baselineReady = true
	x.goIntent = true
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	if x.goClient, err = c.cluster.CreateClient(ctx, "mds-cephfs-completion-"+s.token, mdsCephFSCompletionGoCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated MDS CephFS completion identity: %w", err)
	}
	_, keyring, err := x.goClient.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-completion.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	x.nativeIntent = true
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	if x.native, err = c.cluster.CreateClient(ctx, "mds-cephfs-completion-native-"+s.token, mdsCephFSCompletionNativeCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated native CephFS completion identity: %w", err)
	}
	_, nativeKeyring, err := x.native.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(nativeKeyring); err != nil {
		return err
	}
	// The native-only key stays in memory and reaches each worker on its
	// standard input (runKeyedNative); no host keyring file is written.
	c.profile.mdsCephFSCompletionIdentity = "client.mds-cephfs-completion-" + s.token
	if err := errors.Join(c.mdsCephFSCompletionJournal(), c.mdsCephFSCompletionWriteTarget()); err != nil {
		return err
	}
	goCaps, nativeCaps := mdsCephFSCompletionGoCaps(s.name), mdsCephFSCompletionNativeCaps(s.name)
	return writeMDSFreshJSON(out, "mds-cephfs-completion-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name, "mds_gid": s.oracle.MDS.GID,
		"client_identity": c.profile.mdsCephFSCompletionIdentity, "native_identity": "client.mds-cephfs-completion-native-" + s.token,
		"client_caps": map[string]string{"mon": goCaps.Mon, "mds": goCaps.MDS, "osd": goCaps.OSD}, "native_caps": map[string]string{"mon": nativeCaps.Mon, "mds": nativeCaps.MDS, "osd": nativeCaps.OSD},
		"plan": x.plan, "baseline_blocklist": x.baselineBlocklist, "baseline_config": x.baselineConfig, "data_pool": s.oracle.DataPools[0], "data_pool_name": s.name + "-data",
	})
}

// mdsCephFSCompletionRunNative runs the fixed native driver once.
func (c *fixtureCluster) mdsCephFSCompletionRunNative(ctx context.Context, action, path string, extra ...string) (*ceph.NativeCommandResult, mdsCephFSNative, error) {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	timeout := 30 * time.Second
	if action == "create-write" || action == "mksnap" || action == "rmsnap" {
		timeout = 45 * time.Second
	}
	argv := append([]string{"python3", "-c", mdsCephFSCompletionNativePython, "client.mds-cephfs-completion-native-" + s.token, s.name,
		x.plan.Directory, x.plan.Snapshot, action, path}, extra...)
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: mdsCephFSJSONLimit, MaxStderr: 16 << 10, Label: "owned native CephFS completion " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, mdsCephFSNative{}, errors.Join(err, fmt.Errorf("native CephFS completion %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeMDSCephFSCompletionNative(action, path, x.plan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

func (c *fixtureCluster) mdsCephFSCompletionControl(ctx context.Context, out string, fields []string) (resultErr error) {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	if c.profile.mdsCephFSCompletion != "1" || c.profile.mdsFresh != "1" || s == nil || s.fs == nil || x == nil || s.out != out || x.out != out {
		return errors.New("invalid owned MDS CephFS completion control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 110*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS CephFS completion control requested after cleanup began")
	}
	id, action, err := parseMDSCephFSCompletionRequest(fields, s.token, x.plan, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := mdsCephFSCompletionResult{Source: mdsFreshSource, Token: s.token, RequestID: id, Action: action.name, Target: action.target, GlobalID: action.gid, FSID: s.oracle.FSID,
		FSCID: s.oracle.FSCID, FSName: s.name, MDSGID: x.current.GID, MDSEndpoint: x.current.Endpoint, MDSNonce: x.current.Nonce, MDSIncarnation: x.current.Incarnation}
	defer func() {
		result.ElapsedMillis = time.Since(started).Milliseconds()
		result.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if resultErr != nil {
			result.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-cephfs-completion-result.json", result))
	}()
	checkIdentity := func() error {
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		owned := x.current
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MDS.GID != owned.GID || current.MDS.Endpoint != owned.Endpoint || current.MDS.Nonce != owned.Nonce ||
			current.MDS.Incarnation != owned.Incarnation || current.MDS.Name != owned.Name || current.MDS.Rank != 0 || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
			return errors.New("MDS CephFS completion native target changed during the owned action")
		}
		return nil
	}
	if r := x.pending(); r != nil {
		// Between fire and adopt only transitional reads and daemon-local
		// diagnostics of the respawned owned daemon are admitted.
		switch action.name {
		case "restart-fire", "restart-status", "restart-adopt":
		case "config-read", "debug-on", "debug-off", "mds-log-mark", "mds-log":
			status, err := c.mdsCephFSCompletionRankStatus(ctx, r.Armed.GID)
			if err != nil {
				return err
			}
			daemon, err := c.mdsCephFSCompletionStatus(ctx)
			if err != nil {
				return err
			}
			result.RankStatus, result.Daemon = &status, &daemon
			if status.Rank0 == nil || status.Rank0.Name != x.current.Name || status.Rank0.GID != daemon.GID || daemon.GID == r.Armed.GID || daemon.Rank != 0 {
				return errors.New("MDS CephFS completion respawned daemon is not the owned rank 0 incarnation")
			}
			if err := c.mdsCephFSCompletionDaemonAct(ctx, id, action, daemon.GID, &result); err != nil {
				return err
			}
			result.Complete = true
			return nil
		default:
			return errors.New("MDS CephFS completion action refused while an owned MDS restart is not adopted")
		}
	}
	switch action.name {
	case "restart-fire", "restart-status":
		if err := c.mdsCephFSCompletionRestartAct(ctx, id, action, &result); err != nil {
			return err
		}
		result.Complete = true
		return nil
	case "restart-adopt":
		if err := c.mdsCephFSCompletionRestartAct(ctx, id, action, &result); err != nil {
			return err
		}
	default:
		if err := checkIdentity(); err != nil {
			return err
		}
		if err := c.mdsCephFSCompletionAct(ctx, id, action, &result); err != nil {
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
	daemon, err := c.mdsCephFSCompletionStatus(ctx)
	if err != nil {
		return err
	}
	result.Daemon = &daemon
	if daemon.GID != x.current.GID || daemon.Rank != 0 || daemon.State != "up:active" || daemon.MDSMapEpoch < current.Epoch {
		return errors.New("MDS CephFS completion daemon status does not prove the owned active incarnation")
	}
	result.Complete = true
	return nil
}

func (c *fixtureCluster) mdsCephFSCompletionAct(ctx context.Context, id int64, action mdsCephFSCompletionAction, result *mdsCephFSCompletionResult) error {
	x := c.mdsCephFSCompletion
	switch action.name {
	case "observe":
		sessions, completed, err := c.mdsCephFSCompletionSessions(ctx, action.gid)
		if err != nil {
			return err
		}
		result.Sessions, result.Completed = sessions, completed
		for _, session := range sessions {
			if session.ID == action.gid && session.AuthID == "mds-cephfs-completion-"+c.mdsFresh.token && x.owned[session.Address] == 0 {
				x.owned[session.Address] = session.ID
				if err := c.mdsCephFSCompletionJournal(); err != nil {
					return err
				}
			}
		}
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	case "evict":
		if err := x.quiesced(x.current.GID); err != nil {
			return err
		}
		if err := c.mdsCephFSCompletionEvict(ctx, id, action.gid, result); err != nil {
			return err
		}
		var err error
		if result.Sessions, _, err = c.mdsCephFSCompletionSessions(ctx); err != nil {
			return err
		}
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	case "ops-in-flight", "historic-ops":
		if !slices.Contains(slices.Collect(maps.Values(x.owned)), action.gid) {
			return errors.New("MDS op tracker reads are admitted only for an observed owned Go session")
		}
		command := "dump_ops_in_flight"
		if action.name == "historic-ops" {
			command = "dump_historic_ops"
		}
		data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, command)
		if err != nil {
			return err
		}
		result.Ops, err = parseMDSCephFSHistoricOps(data, action.gid)
		return err
	case "stat", "list", "write-fenced", "snapshot-create", "snapshot-remove":
		return c.mdsCephFSCompletionNative(ctx, id, action, result)
	case "flush-journal":
		data, err := c.mdsCephFSCompletionAsok(ctx, 60*time.Second, "flush", "journal")
		if err != nil {
			return err
		}
		var flush struct {
			ReturnCode *int64 `json:"return_code"`
			Message    string `json:"message"`
		}
		if json.Unmarshal(data, &flush) != nil || flush.ReturnCode == nil {
			return errors.New("MDS completion journal flush output differs")
		}
		result.FlushJournal = &mdsCephFSCompletionFlush{ReturnCode: *flush.ReturnCode, Message: flush.Message}
		if *flush.ReturnCode != 0 {
			return fmt.Errorf("MDS completion journal flush returned %d", *flush.ReturnCode)
		}
		return nil
	case "force-readonly":
		if x.mutated("force-readonly") {
			return errors.New("MDS CephFS completion force_readonly was already issued")
		}
		m := &mdsCephFSMutation{Kind: "force-readonly", RequestID: id, Target: fmt.Sprintf("mds.%s gid %d", x.current.Name, x.current.GID), State: "intended"}
		x.mutations = append(x.mutations, m)
		result.Mutation = m
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
		output, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "force_readonly")
		if len(output) != 0 {
			m.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
		}
		if err != nil {
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		m.State = "applied"
		return c.mdsCephFSCompletionJournal()
	case "deny-on", "window-on", "deny-off", "window-off":
		return c.mdsCephFSCompletionCentralAct(ctx, id, action.name, result)
	case "restart-arm":
		return c.mdsCephFSCompletionRestartAct(ctx, id, action, result)
	}
	return c.mdsCephFSCompletionDaemonAct(ctx, id, action, x.current.GID, result)
}

// mdsCephFSCompletionDaemonAct runs the daemon-local actions: configuration
// readback, runtime changes, log marks and log reads. gid is the incarnation
// the admin socket answered for.
func (c *fixtureCluster) mdsCephFSCompletionDaemonAct(ctx context.Context, id int64, action mdsCephFSCompletionAction, gid uint64, result *mdsCephFSCompletionResult) error {
	x := c.mdsCephFSCompletion
	switch action.name {
	case "config-read":
		values, err := c.mdsCephFSCompletionConfig(ctx)
		result.Config = values
		return err
	case "mds-log-mark":
		lines, err := c.mdsCephFSCompletionLogLines(ctx)
		if err != nil {
			return err
		}
		x.logMark = len(lines)
		result.Log = &mdsCephFSCompletionLog{Mark: x.logMark, Total: len(lines), Lines: []string{}}
		return nil
	case "mds-log", "reconnect-log":
		lines, err := c.mdsCephFSCompletionLogLines(ctx)
		if err != nil {
			return err
		}
		mark := min(x.logMark, len(lines))
		selected := lines[mark:]
		if action.name == "reconnect-log" {
			// This read-only oracle keeps only this client's fragment handler
			// lines before the general bounded filter. Large retained cap sets
			// otherwise fill that filter with unrelated cap recovery details.
			selected = filterMDSCephFSReconnectHandlers(selected, action.gid)
		}
		kept, dropped := filterMDSCephFSCompletionLog(selected, action.gid, action.nonce)
		result.Log = &mdsCephFSCompletionLog{Mark: mark, Total: len(lines), Lines: kept, Dropped: dropped, GlobalID: action.gid, Nonce: action.nonce}
		return nil
	}
	kind, restore := action.name, false
	if k := mdsCephFSCompletionRestoreOf(action.name); k != "" {
		kind, restore = k, true
	}
	spec, ok := mdsCephFSCompletionRuntimeKinds[kind]
	if !ok {
		return errors.New("MDS CephFS completion daemon action is not admitted")
	}
	if !restore {
		if x.activeRuntime(kind, gid) != nil {
			return fmt.Errorf("MDS CephFS completion %s is already in effect", kind)
		}
		if kind != "debug-on" && x.pending() != nil {
			return errors.New("MDS CephFS completion runtime change refused during a restart")
		}
		r := &mdsCephFSCompletionRuntime{Kind: kind, Settings: maps.Clone(spec.settings), Baseline: map[string]string{}, IncarnationGID: gid, RequestID: id, State: "intended"}
		for name := range spec.settings {
			r.Baseline[name] = x.baselineConfig[name]
		}
		x.runtime = append(x.runtime, r)
		result.Runtime = r
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
		err := c.mdsCephFSCompletionApplyRuntime(ctx, r.Settings, &r.Readback)
		r.State = "applied"
		if err != nil {
			r.State, r.Error = "uncertain", err.Error()
		}
		return errors.Join(err, c.mdsCephFSCompletionJournal())
	}
	r := x.activeRuntime(kind, gid)
	if r == nil {
		return fmt.Errorf("MDS CephFS completion %s has no owned change to restore", action.name)
	}
	r.State, r.RestoreRequestID = "restore-intended", id
	result.Runtime = r
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	err := c.mdsCephFSCompletionApplyRuntime(ctx, r.Baseline, &r.RestoreReadback)
	r.State = "restored"
	if err != nil {
		r.State, r.Error = "restore-uncertain", err.Error()
	}
	return errors.Join(err, c.mdsCephFSCompletionJournal())
}

// mdsCephFSCompletionApplyRuntime sets each value through the admin socket
// and requires the daemon to report it.
func (c *fixtureCluster) mdsCephFSCompletionApplyRuntime(ctx context.Context, settings map[string]string, readback *map[string]string) error {
	*readback = map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(settings)) {
		if _, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "config", "set", name, settings[name]); err != nil {
			return err
		}
		data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "config", "get", name)
		if err != nil {
			return err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return err
		}
		(*readback)[name] = value
		if value != settings[name] {
			return fmt.Errorf("MDS daemon reports %s=%s after setting %s", name, value, settings[name])
		}
	}
	return nil
}

// mdsCephFSCompletionCentralAct applies or restores one owned central
// override of the owned daemon's section through the provider.
func (c *fixtureCluster) mdsCephFSCompletionCentralAct(ctx context.Context, id int64, name string, result *mdsCephFSCompletionResult) error {
	x := c.mdsCephFSCompletion
	if spec, ok := mdsCephFSCompletionCentralKinds[name]; ok {
		if slices.ContainsFunc(x.central, func(o *mdsCephFSCompletionCentral) bool { return o.Kind == spec.kind }) {
			return fmt.Errorf("MDS CephFS completion central %s was already applied once", spec.kind)
		}
		if slices.ContainsFunc(x.central, func(o *mdsCephFSCompletionCentral) bool { return o.State != "restored" }) {
			return errors.New("MDS CephFS completion admits one unrestored central override at a time")
		}
		o := &mdsCephFSCompletionCentral{Kind: spec.kind, Section: "mds." + x.current.Name, Name: spec.name, Value: spec.value, RequestID: id, State: "intended"}
		x.central = append(x.central, o)
		result.Central = o
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
		var err error
		o.handle, err = c.cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: o.Section, Name: o.Name, Value: o.Value})
		if err != nil {
			o.State, o.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		o.State = "applied"
		if spec.kind == "deny" {
			// mds_deny_all_reconnect is a runtime option: the running daemon
			// reports it. mds_reconnect_timeout is read at the next startup.
			o.DaemonValue, err = c.mdsCephFSCompletionWaitConfig(ctx, o.Name, o.Value)
		} else {
			o.DaemonValue, err = c.mdsCephFSCompletionReadConfig(ctx, o.Name)
		}
		return errors.Join(err, c.mdsCephFSCompletionJournal())
	}
	kind := strings.TrimSuffix(name, "-off")
	var o *mdsCephFSCompletionCentral
	for _, candidate := range x.central {
		if candidate.Kind == kind {
			o = candidate
		}
	}
	if o == nil || o.handle == nil || o.State == "restored" {
		return fmt.Errorf("MDS CephFS completion central %s has no owned override to restore", kind)
	}
	o.State, o.RestoreRequestID = "restore-intended", id
	result.Central = o
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	if err := o.handle.Restore(ctx); err != nil {
		o.State, o.Error = "restore-uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSCompletionJournal())
	}
	o.State = "restored"
	var err error
	if kind == "deny" {
		o.RestoredDaemon, err = c.mdsCephFSCompletionWaitConfig(ctx, o.Name, x.baselineConfig[o.Name])
	} else {
		o.RestoredDaemon, err = c.mdsCephFSCompletionReadConfig(ctx, o.Name)
	}
	if err == nil {
		err = c.mdsCephFSCompletionNoCentral(ctx)
	}
	return errors.Join(err, c.mdsCephFSCompletionJournal())
}

func (c *fixtureCluster) mdsCephFSCompletionReadConfig(ctx context.Context, name string) (string, error) {
	data, err := c.mdsCephFSCompletionAsok(ctx, 15*time.Second, "config", "get", name)
	if err != nil {
		return "", err
	}
	return parseMDSCephFSCompletionConfig(data, name)
}

func (c *fixtureCluster) mdsCephFSCompletionWaitConfig(ctx context.Context, name, want string) (string, error) {
	wait, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		value, err := c.mdsCephFSCompletionReadConfig(wait, name)
		if err != nil || value == want {
			return value, err
		}
		select {
		case <-wait.Done():
			return value, fmt.Errorf("MDS daemon %s did not reach %s: %w", name, want, wait.Err())
		case <-tick.C:
		}
	}
}

// mdsCephFSCompletionNoCentral requires that no central entry of an owned
// setting remains in any section.
func (c *fixtureCluster) mdsCephFSCompletionNoCentral(ctx context.Context) error {
	entries, err := c.cluster.Configuration(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		for _, spec := range mdsCephFSCompletionCentralKinds {
			if entry.Name == spec.name {
				return fmt.Errorf("central entry %s/%s remains", entry.Section, entry.Name)
			}
		}
	}
	return nil
}

// mdsCephFSCompletionNative runs one native driver action. Mutations are
// one-shot and journaled before the process starts.
func (c *fixtureCluster) mdsCephFSCompletionNative(ctx context.Context, id int64, action mdsCephFSCompletionAction, result *mdsCephFSCompletionResult) error {
	x := c.mdsCephFSCompletion
	if err := x.quiesced(x.current.GID); err != nil {
		return err
	}
	path, err := x.plan.path(action.target)
	if err != nil {
		return err
	}
	native, extra, kind := action.name, []string(nil), ""
	switch action.name {
	case "write-fenced":
		native, extra, kind = "create-write", []string{x.plan.FencedPayloadHex}, "native-create-write"
	case "snapshot-create":
		native, kind = "mksnap", "native-snapshot-create"
	case "snapshot-remove":
		native, kind = "rmsnap", "native-snapshot-remove"
		if m := slices.IndexFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == "native-snapshot-create" && m.State == "applied" }); m < 0 {
			return errors.New("MDS CephFS completion snapshot removal requires an applied creation")
		}
	}
	var mutation *mdsCephFSMutation
	if kind != "" {
		if x.mutated(kind) {
			return fmt.Errorf("MDS CephFS completion %s was already issued", kind)
		}
		mutation = &mdsCephFSMutation{Kind: kind, RequestID: id, Target: action.target, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
	}
	process, output, err := c.mdsCephFSCompletionRunNative(ctx, native, path, extra...)
	result.Process = process
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		return err
	}
	result.Native = &output
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", output.NativeGlobalID
		return c.mdsCephFSCompletionJournal()
	}
	return nil
}

// mdsCephFSCompletionEvict evicts one open session of the owned Go identity
// through the admin socket and waits until it is gone and its exact address
// is blocklisted. It is used for a Go session the client can no longer reach.
func (c *fixtureCluster) mdsCephFSCompletionEvict(ctx context.Context, id int64, gid uint64, result *mdsCephFSCompletionResult) error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	sessions, _, err := c.mdsCephFSCompletionSessions(ctx)
	if err != nil {
		return err
	}
	var victim *mdsSessionsNative
	for i := range sessions {
		if sessions[i].ID == gid {
			victim = &sessions[i]
		}
	}
	if victim == nil || victim.State != "open" && victim.State != "stale" || victim.AuthID != "mds-cephfs-completion-"+s.token {
		return errors.New("MDS CephFS completion eviction target is not a session of the owned Go identity")
	}
	if slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == "eviction" && m.GlobalID == gid }) {
		return errors.New("MDS CephFS completion eviction of this GlobalID was already issued")
	}
	before, err := c.mdsSessionsBlocklist(ctx)
	if err != nil {
		return err
	}
	if mdsSessionsFindBlock(before, victim.Address) != nil || mdsSessionsFindBlock(x.baselineBlocklist, victim.Address) != nil {
		return errors.New("MDS CephFS completion eviction target address is already blocklisted")
	}
	x.owned[victim.Address] = gid
	m := &mdsCephFSMutation{Kind: "eviction", RequestID: id, Target: strconv.FormatUint(gid, 10), State: "intended", GlobalID: gid, Address: victim.Address}
	x.mutations = append(x.mutations, m)
	result.Mutation = m
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	output, err := c.mdsCephFSCompletionAsok(ctx, 30*time.Second, "session", "evict", "id="+strconv.FormatUint(gid, 10))
	if len(output) != 0 {
		m.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
	}
	if err != nil {
		m.State, m.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSCompletionJournal())
	}
	m.State = "requested"
	if err := c.mdsCephFSCompletionJournal(); err != nil {
		return err
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		sessions, _, err := c.mdsCephFSCompletionSessions(ctx)
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
			return c.mdsCephFSCompletionJournal()
		}
		select {
		case <-ctx.Done():
			err := fmt.Errorf("MDS CephFS completion eviction did not remove the session and blocklist its address: %w", ctx.Err())
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSCompletionJournal())
		case <-tick.C:
		}
	}
}

// mdsCephFSCompletionRestartAct runs one step of an owned restart. Restarts
// are sequential and bounded; each fails exactly the armed GID once.
func (c *fixtureCluster) mdsCephFSCompletionRestartAct(ctx context.Context, id int64, action mdsCephFSCompletionAction, result *mdsCephFSCompletionResult) error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	var r *mdsCephFSRestart
	if n := len(x.restarts); n != 0 {
		r = x.restarts[n-1]
	}
	switch action.name {
	case "restart-arm":
		if r != nil && r.State != "adopted" {
			return errors.New("MDS CephFS completion restart refused before the previous one was adopted")
		}
		if len(x.restarts) == mdsCephFSCompletionMaxRestarts {
			return errors.New("MDS CephFS completion restart budget is exhausted")
		}
		armed := x.current
		r = &mdsCephFSRestart{State: "armed", ArmRequestID: id, Armed: armed, ArmedAddresses: []string{}}
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
				return errors.New("MDS CephFS completion restart target address is already blocklisted")
			}
		}
		x.restarts = append(x.restarts, r)
		result.Restart = r
		return c.mdsCephFSCompletionJournal()
	case "restart-fire":
		if r == nil || r.State != "armed" {
			return errors.New("MDS CephFS completion restart fire requires one armed, unfired restart")
		}
		r.State, r.FireRequestID, r.FireStartedUTC = "intended", id, time.Now().UTC().Format(time.RFC3339Nano)
		result.Restart = r
		// Runtime changes end with the armed process, whatever the outcome.
		for _, rt := range x.runtime {
			if rt.IncarnationGID == r.Armed.GID && rt.active() {
				rt.State = "discarded-by-restart"
			}
		}
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
		output, err := c.cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(r.Armed.GID, 10))
		r.FiredUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if len(output) != 0 {
			r.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
		}
		if err != nil {
			r.State, r.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		r.State = "applied"
		return c.mdsCephFSCompletionJournal()
	case "restart-status":
		if r == nil || !r.pending() || r.State == "intended" {
			return errors.New("MDS CephFS completion restart status requires a fired, unadopted restart")
		}
		status, err := c.mdsCephFSCompletionRankStatus(ctx, r.Armed.GID)
		if err != nil {
			return err
		}
		result.Restart, result.RankStatus = r, &status
		return nil
	case "restart-adopt":
		if r == nil || !r.pending() || r.State == "intended" {
			return errors.New("MDS CephFS completion restart adoption requires a fired, unadopted restart")
		}
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		a, n := r.Armed, current.MDS
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) ||
			n.Name != a.Name || n.Rank != 0 || n.State != "up:active" || n.GID == a.GID || n.Nonce == a.Nonce || n.Nonce == 0 || n.Incarnation == a.Incarnation {
			return errors.New("MDS CephFS completion restart adoption could not prove a new incarnation of the owned daemon")
		}
		status, err := c.mdsCephFSCompletionRankStatus(ctx, r.Armed.GID)
		if err != nil {
			return err
		}
		if status.ArmedPresent || status.Rank0 == nil || status.Rank0.GID != n.GID {
			return errors.New("MDS CephFS completion restart adoption found the armed daemon still mapped")
		}
		r.OldFencing = status.ArmedBlocklist
		adopted := n
		r.Adopted, r.AdoptRequestID, r.AdoptedUTC, r.State = &adopted, id, time.Now().UTC().Format(time.RFC3339Nano), "adopted"
		x.current = n
		result.Restart, result.RankStatus = r, &status
		result.MDSGID, result.MDSEndpoint, result.MDSNonce, result.MDSIncarnation = n.GID, n.Endpoint, n.Nonce, n.Incarnation
		return errors.Join(c.mdsCephFSCompletionJournal(), c.mdsCephFSCompletionWriteTarget())
	}
	return errors.New("MDS CephFS completion restart action is not admitted")
}

// blockOwner attributes one new blocklist entry to an owned mutation, or
// returns "" when it is not attributable. gone reports whether an armed GID
// left the FSMap.
func (x *mdsCephFSCompletionScope) blockOwner(entry mdsSessionsBlock, gone func(uint64) bool) (string, error) {
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
	}
	for _, r := range x.restarts {
		if slices.Contains(r.ArmedAddresses, entry.Address) {
			if !gone(r.Armed.GID) {
				return "", errors.New("an armed MDS incarnation is still mapped; its fencing entry is kept")
			}
			return "mds-restart", nil
		}
	}
	if x.owned[entry.Address] != 0 {
		return "owned-session", nil
	}
	return "", nil
}

type mdsCephFSCompletionCleanup struct {
	Source                string                        `json:"source_commit"`
	Token                 string                        `json:"token"`
	FSName                string                        `json:"fs_name"`
	Attempted             bool                          `json:"attempted"`
	Mutations             []*mdsCephFSMutation          `json:"mutations"`
	Runtime               []*mdsCephFSCompletionRuntime `json:"runtime"`
	Central               []*mdsCephFSCompletionCentral `json:"central"`
	Restarts              []*mdsCephFSRestart           `json:"restarts"`
	PendingRestartActive  bool                          `json:"pending_restart_active,omitempty"`
	Config                map[string]string             `json:"config,omitempty"`
	ConfigRestored        bool                          `json:"config_restored"`
	ResidualSessions      []mdsSessionsNative           `json:"residual_owned_sessions"`
	NamespaceChecked      bool                          `json:"namespace_checked"`
	NamespaceRestored     bool                          `json:"namespace_restored"`
	NamespaceNote         string                        `json:"namespace_note,omitempty"`
	Namespace             *mdsCephFSNative              `json:"namespace,omitempty"`
	NewBlocklist          []mdsSessionsBlock            `json:"new_blocklist"`
	RemovedBlocklist      []mdsCephFSRemovedBlock       `json:"removed_blocklist"`
	KeptBlocklist         []mdsSessionsBlock            `json:"kept_new_blocklist"`
	FinalBlocklist        []mdsSessionsBlock            `json:"final_blocklist,omitempty"`
	BlocklistRestored     bool                          `json:"blocklist_restored"`
	ClientIdentityRemoved bool                          `json:"client_identity_removed"`
	NativeIdentityRemoved bool                          `json:"native_identity_removed"`
	Completed             bool                          `json:"completed"`
	Errors                []string                      `json:"errors,omitempty"`
}

// finishMDSCephFSCompletion runs once, inside the cached MDS fresh cleanup and
// before the owned filesystem is removed. It restores runtime and central
// settings first (a blackholed or paused daemon would stall every later
// step), then reports residual sessions, reads the namespace back natively,
// removes attributable blocklist entries and deletes both identities. It
// never evicts.
func (c *fixtureCluster) finishMDSCephFSCompletion(ctx context.Context) error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	if s == nil || x == nil {
		return nil
	}
	ctx, stop := context.WithTimeout(ctx, 100*time.Second)
	defer stop()
	receipt := mdsCephFSCompletionCleanup{Source: mdsFreshSource, Token: s.token, FSName: s.name, Attempted: true, Mutations: x.mutations, Runtime: x.runtime, Central: x.central,
		Restarts: x.restarts, ResidualSessions: []mdsSessionsNative{}, NewBlocklist: []mdsSessionsBlock{}, RemovedBlocklist: []mdsCephFSRemovedBlock{}, KeptBlocklist: []mdsSessionsBlock{}}
	if err := writeMDSFreshJSON(x.out, "mds-cephfs-completion-cleanup.json", receipt); err != nil {
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if !x.baselineReady {
		receipt.ConfigRestored, receipt.NamespaceChecked, receipt.NamespaceRestored, receipt.BlocklistRestored = true, true, true, true
		receipt.NamespaceNote = "preparation ended before any owned mutation was possible"
	} else {
		// A pending restart: wait for the respawned daemon to become active
		// (it does so by itself once its reconnect window closes).
		if r := x.pending(); r != nil {
			wait, stopWait := context.WithTimeout(ctx, 60*time.Second)
			for {
				status, err := c.mdsCephFSCompletionRankStatus(wait, r.Armed.GID)
				if err == nil && status.Rank0 != nil && status.Rank0.State == "up:active" && status.Rank0.GID != r.Armed.GID {
					receipt.PendingRestartActive = true
					break
				}
				if wait.Err() != nil {
					fail(errors.New("a fired owned restart did not reach an active incarnation"))
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			stopWait()
		}
		gid := x.current.GID
		if status, err := c.mdsCephFSCompletionStatus(ctx); err == nil {
			gid = status.GID
		} else {
			fail(err)
		}
		for _, r := range x.runtime {
			if !r.active() {
				continue
			}
			if r.IncarnationGID != gid {
				r.State = "discarded-by-restart"
				continue
			}
			err := c.mdsCephFSCompletionApplyRuntime(ctx, r.Baseline, &r.RestoreReadback)
			r.State = "restored"
			if err != nil {
				r.State, r.Error = "restore-uncertain", err.Error()
			}
			fail(err)
		}
		for _, o := range x.central {
			if o.handle == nil || o.State == "restored" {
				continue
			}
			err := o.handle.Restore(ctx)
			if err == nil {
				o.State = "restored"
			}
			fail(err)
		}
		fail(c.mdsCephFSCompletionJournal())
		config, err := c.mdsCephFSCompletionConfig(ctx)
		fail(err)
		receipt.Config = config
		centralErr := c.mdsCephFSCompletionNoCentral(ctx)
		fail(centralErr)
		receipt.ConfigRestored = err == nil && centralErr == nil
		for _, name := range []string{"ms_blackhole_client", "mds_early_reply", "mds_log_pause", "debug_mds", "debug_ms", "mds_deny_all_reconnect"} {
			if config[name] != x.baselineConfig[name] {
				receipt.ConfigRestored = false
				fail(fmt.Errorf("MDS daemon %s=%s differs from baseline %s", name, config[name], x.baselineConfig[name]))
			}
		}
		sessions, _, err := c.mdsCephFSCompletionSessions(ctx)
		fail(err)
		goPresent := false
		for _, session := range sessions {
			if session.AuthID == "mds-cephfs-completion-"+s.token || session.AuthID == "mds-cephfs-completion-native-"+s.token {
				receipt.ResidualSessions = append(receipt.ResidualSessions, session)
				goPresent = goPresent || session.AuthID == "mds-cephfs-completion-"+s.token
			}
		}
		quiet := x.quiesced(gid) == nil && receipt.ConfigRestored
		switch {
		case err != nil:
			receipt.NamespaceNote = "native session listing failed; namespace readback skipped"
		case x.native == nil:
			receipt.NamespaceNote = "native identity was never created; no native namespace mutation was possible"
			receipt.NamespaceChecked, receipt.NamespaceRestored = true, len(x.mutations) == 0
		case goPresent:
			receipt.NamespaceNote = "Go session still present; namespace readback skipped and residual entries are discarded with the owned filesystem"
		case !quiet:
			receipt.NamespaceNote = "daemon settings were not restored; native readback skipped"
		default:
			path, _ := x.plan.path("dir")
			_, native, err := c.mdsCephFSCompletionRunNative(ctx, "stat", path)
			fail(err)
			if err == nil {
				receipt.Namespace, receipt.NamespaceChecked, receipt.NamespaceRestored = &native, true, !native.Stat.Exists
				if !receipt.NamespaceRestored {
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
			c.mdsCephFSCompletionRestoreBlocklist(ctx, &receipt, fail)
		}
	}
	remove := func(config *ceph.ClientConfig, intent bool, label string) bool {
		if config == nil {
			if intent {
				fail(fmt.Errorf("MDS CephFS completion %s identity creation outcome is unknown", label))
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
	receipt.Completed = len(errs) == 0 && receipt.ConfigRestored && receipt.NamespaceChecked && receipt.BlocklistRestored && receipt.ClientIdentityRemoved && receipt.NativeIdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("MDS CephFS completion cleanup did not verify every check"))
	}
	return errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "mds-cephfs-completion-cleanup.json", receipt))
}

// mdsCephFSCompletionRestoreBlocklist removes only new entries attributable to
// owned mutations and verifies them absent.
func (c *fixtureCluster) mdsCephFSCompletionRestoreBlocklist(ctx context.Context, receipt *mdsCephFSCompletionCleanup, fail func(error)) {
	x := c.mdsCephFSCompletion
	receipt.BlocklistRestored = true
	present := map[uint64]bool{}
	statusErr := error(nil)
	if len(x.restarts) != 0 {
		var status mdsCephFSRankStatus
		status, statusErr = c.mdsCephFSCompletionRankStatus(ctx, 0)
		fail(statusErr)
		for _, d := range append(status.Daemons, status.Standbys...) {
			present[d.GID] = true
		}
	}
	gone := func(gid uint64) bool { return statusErr == nil && !present[gid] }
	for _, entry := range receipt.NewBlocklist {
		kind, err := x.blockOwner(entry, gone)
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
		fail(errors.New("MDS CephFS completion acceptance kept new blocklist entries it cannot attribute to an owned mutation"))
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
